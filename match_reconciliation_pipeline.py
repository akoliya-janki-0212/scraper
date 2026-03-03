#!/usr/bin/env python3
"""
Robust reconciliation pipeline for competitor scrape + system + existing CM map.

Goals:
- Keep existing correct URL mappings.
- Replace wrong mappings only with safe high/medium confidence candidates.
- If existing mapping is wrong and no safe replacement exists, separate it as
  wrong-no-replacement (do not force weak auto-match).
- Detect crawl misses and track consecutive miss counts.
- Produce action files for downstream import/review.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import zipfile
import os
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() == "nan":
        return ""
    return text


def norm_id(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", clean_text(value).lower())


def norm_numeric_id(value: Any) -> str:
    token = norm_id(value)
    if token.isdigit():
        return token.lstrip("0") or "0"
    return token


def norm_brand(value: Any) -> str:
    token = clean_text(value).lower()
    token = re.sub(r"[^a-z0-9]+", "-", token)
    return token.strip("-")


def split_multi_values(value: Any) -> list[str]:
    raw = clean_text(value)
    if not raw:
        return []
    parts = [clean_text(p) for p in re.split(r"[,;|]+", raw)]
    parts = [p for p in parts if p]
    if parts:
        return parts
    return [raw]


def id_tokens(value: Any) -> list[str]:
    tokens: list[str] = []
    seen: set[str] = set()
    for part in split_multi_values(value):
        token = norm_id(part)
        if token and token not in seen:
            seen.add(token)
            tokens.append(token)
    return tokens


def numeric_tokens(value: Any) -> list[str]:
    tokens: list[str] = []
    seen: set[str] = set()
    for part in split_multi_values(value):
        token = norm_numeric_id(part)
        if token and token not in seen:
            seen.add(token)
            tokens.append(token)
    return tokens


def extract_domain(url: str) -> str:
    raw = clean_text(url)
    if not raw:
        return ""
    try:
        host = urlparse(raw).hostname or ""
    except ValueError:
        return ""
    host = host.lower()
    return re.sub(r"^www\.", "", host)


def url_fingerprint(url: str) -> str:
    raw = clean_text(url)
    if not raw:
        return ""
    raw = re.sub(r"^https?://(www\.)?", "", raw, flags=re.IGNORECASE)
    raw = re.sub(r"\?.*$", "", raw)
    raw = raw.rstrip("/")
    if not raw:
        return ""
    segments = raw.split("/")
    return "/".join(segments[:3])


def path_key(url: str) -> str:
    raw = clean_text(url)
    if not raw:
        return ""
    try:
        path = urlparse(raw).path or ""
    except ValueError:
        return ""
    if not path or path == "/":
        return ""
    return path.strip("/")


def url_slug(url: str) -> str:
    key = path_key(url)
    if not key:
        return ""
    return key.split("/")[-1]


def token_set(value: Any) -> set[str]:
    return {t for t in re.findall(r"[a-z0-9]+", clean_text(value).lower()) if t}


def name_similarity(left: str, right: str) -> float:
    a = token_set(left)
    b = token_set(right)
    if not a or not b:
        return 0.0
    common = len(a & b)
    return common / len(a | b) * 100.0


def brand_core_tokens(value: str) -> set[str]:
    stop = {
        "inc",
        "llc",
        "ltd",
        "co",
        "corp",
        "company",
        "official",
        "store",
        "shop",
        "home",
        "furniture",
    }
    return {t for t in token_set(value) if t not in stop}


def brand_relation(system_brand: str, comp_brand: str) -> str:
    left = norm_brand(system_brand)
    right = norm_brand(comp_brand)
    if not left or not right:
        return "UNKNOWN"
    if left == right:
        return "EXACT"
    if left in right or right in left:
        return "CLONE"
    a = brand_core_tokens(system_brand)
    b = brand_core_tokens(comp_brand)
    if a and b:
        overlap = len(a & b) / max(len(a), len(b))
        if overlap >= 0.6:
            return "CLONE"
    return "MISMATCH"


def token_fragments(token: str) -> set[str]:
    token = clean_text(token)
    out: set[str] = set()
    if not token:
        return out
    if len(token) >= 6:
        out.add(token[:6])
        out.add(token[-6:])
    if len(token) >= 8:
        out.add(token[:8])
        out.add(token[-8:])
    return out


def mpn_core_token(token: str) -> str:
    normalized = norm_id(token)
    if not normalized:
        return ""
    match = re.search(r"\d", normalized)
    if not match:
        return normalized
    return normalized[match.start() :]


def parse_mpn_core_parts(token: str) -> tuple[str, str] | None:
    core = mpn_core_token(token)
    if not core:
        return None
    m = re.match(r"^(\d+)([a-z][a-z0-9]*)?$", core)
    if not m:
        return None
    num = m.group(1) or ""
    suffix = m.group(2) or ""
    return num, suffix


def parse_mpn_token_parts(token: str) -> tuple[str, str, str] | None:
    normalized = norm_id(token)
    if not normalized:
        return None
    m = re.match(r"^([a-z]*)(\d+)([a-z0-9]*)$", normalized)
    if not m:
        return None
    prefix = m.group(1) or ""
    number = m.group(2) or ""
    suffix = m.group(3) or ""
    return prefix, number, suffix


def mpn_family_key(token: str) -> str:
    parts = parse_mpn_core_parts(token)
    if not parts:
        return ""
    num, suffix = parts
    if not suffix:
        return ""
    prefix = suffix[:2]
    return f"{num}|{prefix}"


def partial_token_match(left: str, right: str) -> bool:
    left_n = norm_id(left)
    right_n = norm_id(right)
    if not left_n or not right_n:
        return False
    if left_n == right_n:
        return True

    # If neither token contains digits, allow strong substring/suffix match
    # (e.g. SAUBREYCHMARINE vs JOFAUBREYCHMARINE).
    if not re.search(r"\d", left_n) and not re.search(r"\d", right_n):
        # Direct containment
        if left_n in right_n or right_n in left_n:
            return True

        # Strong common suffix (>= 6 chars)
        common_suffix = os.path.commonprefix([left_n[::-1], right_n[::-1]])[::-1]
        if len(common_suffix) >= 6:
            return True

        return False

    left_parts = parse_mpn_token_parts(left_n)
    right_parts = parse_mpn_token_parts(right_n)
    if not left_parts or not right_parts:
        return False

    lpre, lnum, lsuf = left_parts
    rpre, rnum, rsuf = right_parts
    if lnum != rnum:
        return False

    # Do not treat plain-numeric and prefixed-numeric identifiers as partial
    # equivalents (e.g. 04166 vs DN04166).
    if ((not lpre and rpre) or (lpre and not rpre)) and not lsuf and not rsuf:
        return False

    # If both tokens do NOT contain numeric cores, allow strong suffix match
    # even when prefixes differ (e.g. SAUBREYCHMARINE vs JOFAUBREYCHMARINE).
    if not lnum and not rnum:
        # Allow match if one full normalized token ends with the other
        # and shared suffix length is reasonably strong (>= 6 chars).
        if left_n.endswith(right_n) or right_n.endswith(left_n):
            return True
        common_suffix = os.path.commonprefix([left_n[::-1], right_n[::-1]])[::-1]
        if len(common_suffix) >= 6:
            return True
        return False

    # Keep prefix-aware partials to avoid unrelated numeric model collisions
    # (e.g. CML100STE vs CKS100STE should not match).
    if lpre and rpre and lpre != rpre and not (lpre.endswith(rpre) or rpre.endswith(lpre)):
        return False

    if not lsuf and not rsuf:
        return True
    if not lsuf or not rsuf:
        return False
    short, long = (lsuf, rsuf) if len(lsuf) <= len(rsuf) else (rsuf, lsuf)

    # Allow 1-character suffix partials (e.g. 9227L vs 9227LO)
    # but still require strict numeric core equality and bounded suffix expansion.
    return (
        len(short) >= 1
        and long.startswith(short)
        and (len(long) - len(short) <= 2)
    )


def all_tokens_exact(left_tokens: list[str], right_tokens: list[str] | set[str]) -> bool:
    if not left_tokens or not right_tokens:
        return False
    right_list = list(right_tokens)

    def equivalent(a: str, b: str) -> bool:
        if a == b:
            return True
        if a.isdigit() and b.isdigit():
            return (a.lstrip("0") or "0") == (b.lstrip("0") or "0")
        return False

    for token in left_tokens:
        if not any(equivalent(token, candidate) for candidate in right_list):
            return False
    return True


def all_tokens_partial(left_tokens: list[str], right_tokens: list[str]) -> bool:
    if not left_tokens or not right_tokens:
        return False
    for token in left_tokens:
        if not any(partial_token_match(token, s) for s in right_tokens):
            return False
    return True


def all_tokens_match_strict(comp_tokens: list[str], system_tokens: list[str], partial: bool = False) -> bool:
    if not comp_tokens or not system_tokens:
        return False
    if partial:
        forward = all_tokens_partial(comp_tokens, system_tokens)
    else:
        forward = all_tokens_exact(comp_tokens, system_tokens)
    if not forward:
        return False
    if partial:
        return all_tokens_partial(system_tokens, comp_tokens)
    return all_tokens_exact(system_tokens, comp_tokens)


def url_matches_scrape_params(cm_url: str, scrape_url: str) -> bool:
    cm_raw = clean_text(cm_url)
    scrape_raw = clean_text(scrape_url)
    if not cm_raw or not scrape_raw:
        return False
    try:
        cm_parsed = urlparse(cm_raw)
        scrape_parsed = urlparse(scrape_raw)
    except ValueError:
        return False

    cm_key = norm_id(url_slug(cm_raw))
    scrape_key = norm_id(url_slug(scrape_raw))
    if not cm_key or not scrape_key or cm_key != scrape_key:
        return False

    cm_params = parse_qs(cm_parsed.query, keep_blank_values=True)
    scrape_params = parse_qs(scrape_parsed.query, keep_blank_values=True)
    for key, values in scrape_params.items():
        if key not in cm_params:
            return False
        scrape_values = {clean_text(v).lower() for v in values}
        cm_values = {clean_text(v).lower() for v in cm_params.get(key, [])}
        if scrape_values and cm_values and scrape_values.isdisjoint(cm_values):
            return False
    return True


def wrong_reason(reason: str) -> bool:
    return "wrong match" in clean_text(reason).lower()


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=True)


@dataclass
class CandidateResult:
    idx: int
    signal: str
    score: int
    confidence: str
    remark: str
    name_similarity: float
    reasons: list[str]
    flags: dict[str, bool]


class ReconciliationPipeline:
    def __init__(
        self,
        scrape_file: Path,
        system_file: Path,
        cm_file: Path,
        output_dir: Path,
        history_file: Path,
        limit: int | None = None,
        min_confidence: str = "AUTO",
    ):
        self.scrape_file = scrape_file
        self.system_file = system_file
        self.cm_file = cm_file
        self.output_dir = output_dir
        self.history_file = history_file
        self.limit = limit
        self.min_confidence = min_confidence.upper()

        self.system: dict[str, dict[str, Any]] = {}
        self.system_gtin_token_counts: defaultdict[str, int] = defaultdict(int)
        self.scrape_rows: list[dict[str, Any]] = []
        self.scrape_headers: list[str] = []
        self.scrape_brand_col = "Ref Brand Name"
        self.scrape_indexes: dict[str, dict[str, list[int]]] = {
            "gtin": defaultdict(list),
            "mpn": defaultdict(list),
            "mpn_core": defaultdict(list),
            "mpn_family": defaultdict(list),
            "handle": defaultdict(list),
            "url_fp": defaultdict(list),
            "path_key": defaultdict(list),
            "brand_mpn": defaultdict(list),
        }
        self.scrape_domain: str = ""

        self.cm_by_product: dict[str, dict[str, Any]] = {}
        self.cm_competitor_id: str = ""

        self.used_scrape_indices: set[int] = set()
        self.allocated_ref_urls: set[str] = set()
        self.decision_by_product: dict[str, str] = {}
        self.unmatched_scrape_rows: list[dict[str, Any]] = []
        self.unmatch_matched_with_cm_rows: list[dict[str, Any]] = []

        self.report_rows: list[dict[str, Any]] = []
        self.new_update_rows: list[dict[str, Any]] = []
        self.approve_rows: list[dict[str, Any]] = []
        self.wrong_no_replacement_rows: list[dict[str, Any]] = []
        self.manual_review_rows: list[dict[str, Any]] = []
        self.crawl_retry_rows: list[dict[str, Any]] = []

        self.summary: dict[str, Any] = {}

    def run(self) -> dict[str, Any]:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        print("[PIPELINE] Starting reconciliation pipeline...")
        print(f"[PIPELINE] Loading system file: {self.system_file}")
        self.load_system()
        print(f"[PIPELINE] System products loaded: {len(self.system)}")
        print(f"[PIPELINE] Loading scrape file: {self.scrape_file}")
        self.load_scrape()
        print(f"[PIPELINE] Scrape rows loaded: {len(self.scrape_rows)}")
        print(f"[PIPELINE] Loading CM file: {self.cm_file}")
        self.load_cm()
        print(f"[PIPELINE] CM rows loaded: {len(self.cm_by_product)}")
        print("[PIPELINE] Evaluating products...")

        history = self.load_history()
        history_out: dict[str, int] = {}

        crawl_quality = self.crawl_quality_state()
        required_conf = self.required_confidence(crawl_quality)

        total_products = len(self.system)
        for idx, (product_id, sys_row) in enumerate(self.ordered_system_items(), start=1):
            if idx % 1000 == 0 or idx == 1 or idx == total_products:
                print(f"[PIPELINE] Processing product {idx}/{total_products} (product_id={product_id})")
            cm_row = self.cm_by_product.get(product_id)
            decision = self.evaluate_product(sys_row, cm_row, required_conf, history, history_out)
            self.decision_by_product[product_id] = decision

        print("[PIPELINE] Building unmatched scrape rows...")
        self.build_unmatched_scrape_rows()
        print("[PIPELINE] Building unmatched-with-CM comparison...")
        self.build_unmatch_matched_with_cm()
        print("[PIPELINE] Writing output files...")
        self.write_outputs(crawl_quality, required_conf)
        print("[PIPELINE] Saving history...")
        self.save_history(history_out)
        print("[PIPELINE] Reconciliation completed.")
        return self.summary

    def ordered_system_items(self) -> list[tuple[str, dict[str, Any]]]:
        def key(item: tuple[str, dict[str, Any]]) -> tuple[int, int, int, int, int]:
            pid, row = item
            cm = self.cm_by_product.get(pid)
            has_cm = 1 if cm else 0
            cm_reason = clean_text(cm.get("reason", "") if cm else "").lower()
            cm_wrongish = 1 if (cm and (wrong_reason(cm_reason) or "url not found" in cm_reason)) else 0
            has_gtin = 1 if row.get("_gtin_is_unique") else 0
            id_specificity = -len(row.get("_id_tokens", []))  # fewer IDs first
            try:
                pid_num = -int(pid)
            except ValueError:
                pid_num = 0
            return (has_cm, cm_wrongish, has_gtin, id_specificity, pid_num)

        items = list(self.system.items())
        items.sort(key=key, reverse=True)
        return items

    def load_system(self) -> None:
        required = {
            "product_id",
            "sku",
            "web_id",
            "gtin",
            "mpn",
            "brand_label",
            "cat",
            "part_number",
            "osb_url",
        }
        with self.system_file.open("r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames or not required.issubset(set(reader.fieldnames)):
                missing = sorted(required - set(reader.fieldnames or []))
                raise ValueError(f"{self.system_file} missing required columns: {', '.join(missing)}")

            for row in reader:
                product_id = clean_text(row.get("product_id"))
                if not product_id or product_id in self.system:
                    continue

                mpn_tokens = id_tokens(row.get("mpn"))
                sku_tokens = id_tokens(row.get("sku"))
                part_tokens = id_tokens(row.get("part_number"))
                id_union = list(dict.fromkeys(mpn_tokens + sku_tokens + part_tokens))
                search_id_tokens = list(dict.fromkeys((mpn_tokens + sku_tokens) if (mpn_tokens or sku_tokens) else part_tokens))
                gtin_values = numeric_tokens(row.get("gtin"))
                osb_url = clean_text(row.get("osb_url"))
                sys_row = {
                    "product_id": product_id,
                    "product_name": clean_text(row.get("product_name")),
                    "sku": clean_text(row.get("sku")),
                    "web_id": clean_text(row.get("web_id")),
                    "gtin": clean_text(row.get("gtin")),
                    "mpn": clean_text(row.get("mpn")),
                    "brand_label": clean_text(row.get("brand_label")),
                    "cat": clean_text(row.get("cat")),
                    "part_number": clean_text(row.get("part_number")),
                    "osb_url": osb_url,
                    "_sku": norm_id(row.get("sku")),  # legacy single-value key
                    "_web": norm_id(row.get("web_id")),  # legacy single-value key
                    "_gtin": norm_numeric_id(row.get("gtin")),  # legacy single-value key
                    "_mpn": norm_id(row.get("mpn")),  # legacy single-value key
                    "_brand": norm_brand(row.get("brand_label")),
                    "_part": norm_id(row.get("part_number")),  # legacy single-value key
                    "_url_key": norm_id(row.get("osb_url")),  # legacy single-value key
                    "_url_slug": norm_id(url_slug(osb_url)),
                    "_mpn_tokens": mpn_tokens,
                    "_sku_tokens": sku_tokens,
                    "_part_tokens": part_tokens,
                    "_id_tokens": id_union,
                    "_search_id_tokens": search_id_tokens,
                    "_id_token_set": set(id_union),
                    "_gtin_tokens": gtin_values,
                    "_gtin_set": set(gtin_values),
                    "_gtin_is_unique": False,
                }
                self.system[product_id] = sys_row
                for token in gtin_values:
                    self.system_gtin_token_counts[token] += 1

        for sys_row in self.system.values():
            gtin_tokens = sys_row.get("_gtin_tokens", [])
            sys_row["_gtin_is_unique"] = bool(
                gtin_tokens and all(self.system_gtin_token_counts.get(token, 0) == 1 for token in gtin_tokens)
            )

    def load_scrape(self) -> None:
        required = {"Ref Product URL", "Ref MPN", "Ref Product Name", "Ref GTIN"}
        brand_candidates = ["Ref Brand Name", "Ref brand_label Name"]
        with self.scrape_file.open("r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames or not required.issubset(set(reader.fieldnames)):
                missing = sorted(required - set(reader.fieldnames or []))
                raise ValueError(f"{self.scrape_file} missing required columns: {', '.join(missing)}")
            self.scrape_brand_col = ""
            fieldnames = set(reader.fieldnames or [])
            for candidate in brand_candidates:
                if candidate in fieldnames:
                    self.scrape_brand_col = candidate
                    break
            if not self.scrape_brand_col:
                raise ValueError(
                    f"{self.scrape_file} missing brand column. Expected one of: {', '.join(brand_candidates)}"
                )
            self.scrape_headers = list(reader.fieldnames)

            for idx, row in enumerate(reader):
                if self.limit is not None and idx >= self.limit:
                    break

                clean = {k: clean_text(v) for k, v in row.items()}
                url = clean.get("Ref Product URL", "")
                # Derive handle from URL slug instead of CSV column
                derived_handle = url_slug(url)
                # Prefer Ref MPN, fallback to Item Number (spec extraction), then Ref SKU
                ref_mpn = (
                    clean.get("Ref MPN", "")
                    or clean.get("Item Number", "")
                    or clean.get("Ref SKU", "")
                )
                ref_gtin = clean.get("Ref GTIN", "")
                brand_label = clean.get(self.scrape_brand_col, "")
                mpn_tokens = id_tokens(ref_mpn)
                gtin_values = numeric_tokens(ref_gtin)

                parsed = {
                    "raw": clean,
                    "_url_fp": url_fingerprint(url),
                    "_path_key": path_key(url),
                    "_handle": norm_id(derived_handle),
                    "_mpn": norm_id(ref_mpn),  # legacy single-value key
                    "_gtin": norm_numeric_id(ref_gtin),  # legacy single-value key
                    "_brand": norm_brand(brand_label),
                    "_mpn_tokens": mpn_tokens,
                    "_mpn_token_set": set(mpn_tokens),
                    "_gtin_tokens": gtin_values,
                    "_gtin_set": set(gtin_values),
                }
                self.scrape_rows.append(parsed)

                row_index = len(self.scrape_rows) - 1
                if parsed["_url_fp"]:
                    self.scrape_indexes["url_fp"][parsed["_url_fp"]].append(row_index)
                if parsed["_path_key"]:
                    self.scrape_indexes["path_key"][parsed["_path_key"]].append(row_index)
                if parsed["_handle"]:
                    self.scrape_indexes["handle"][parsed["_handle"]].append(row_index)
                for token in parsed["_mpn_tokens"]:
                    self.scrape_indexes["mpn"][token].append(row_index)
                    core = mpn_core_token(token)
                    if core:
                        self.scrape_indexes["mpn_core"][core].append(row_index)
                    family = mpn_family_key(token)
                    if family:
                        self.scrape_indexes["mpn_family"][family].append(row_index)
                    if parsed["_brand"]:
                        self.scrape_indexes["brand_mpn"][f"{parsed['_brand']}|{token}"].append(row_index)
                for token in parsed["_gtin_tokens"]:
                    self.scrape_indexes["gtin"][token].append(row_index)

                if not self.scrape_domain:
                    self.scrape_domain = extract_domain(url)

    def load_cm(self) -> None:
        if not self.cm_file.exists():
            self.cm_by_product = {}
            self.cm_competitor_id = ""
            return

        domain_comp_id_counter: Counter[str] = Counter()
        with self.cm_file.open("r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cm_url = clean_text(row.get("competitor_url"))
                cm_domain = extract_domain(cm_url)
                if not cm_domain or not self.scrape_domain or self.scrape_domain not in cm_domain:
                    continue
                comp_id = clean_text(row.get("competitor_id"))
                if comp_id:
                    domain_comp_id_counter[comp_id] += 1
        self.cm_competitor_id = domain_comp_id_counter.most_common(1)[0][0] if domain_comp_id_counter else ""

        system_ids = set(self.system.keys())
        with self.cm_file.open("r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                product_id = clean_text(row.get("product_id"))
                if not product_id or product_id not in system_ids:
                    continue

                row_comp_id = clean_text(row.get("competitor_id"))
                row_url = clean_text(row.get("competitor_url"))
                row_domain = extract_domain(row_url)

                if self.cm_competitor_id:
                    if row_comp_id != self.cm_competitor_id:
                        continue
                elif self.scrape_domain and row_domain and self.scrape_domain not in row_domain:
                    continue

                cm = {
                    "product_id": product_id,
                    "competitor_id": row_comp_id,
                    "competitor_url": row_url,
                    "reason": clean_text(row.get("reason")),
                    "other_reason": clean_text(row.get("other_reason")),
                    "cm_received_sku": clean_text(row.get("cm_received_sku")),
                    "competitor_name": clean_text(row.get("competitor_name")),
                    "last_update_date": clean_text(row.get("last_update_date")),
                    "_url_fp": url_fingerprint(row_url),
                    "_path_key": path_key(row_url),
                }

                old = self.cm_by_product.get(product_id)
                if old is None:
                    self.cm_by_product[product_id] = cm
                    continue

                # Prefer rows with URL, then newer timestamp.
                old_url = bool(old.get("competitor_url"))
                new_url = bool(cm.get("competitor_url"))
                if new_url and not old_url:
                    self.cm_by_product[product_id] = cm
                    continue
                if cm.get("last_update_date", "") > old.get("last_update_date", ""):
                    self.cm_by_product[product_id] = cm

    def crawl_quality_state(self) -> str:
        total = len(self.scrape_rows)
        if total == 0:
            return "POOR"

        unique_urls = len(self.scrape_indexes["url_fp"])
        unique_url_ratio = unique_urls / total
        missing_mpn = sum(1 for r in self.scrape_rows if not r.get("_mpn"))
        missing_gtin = sum(1 for r in self.scrape_rows if not r.get("_gtin"))
        missing_mpn_ratio = missing_mpn / total
        missing_gtin_ratio = missing_gtin / total

        if unique_url_ratio < 0.60 or missing_mpn_ratio > 0.35:
            return "POOR"
        if unique_url_ratio < 0.80 or missing_mpn_ratio > 0.15 or missing_gtin_ratio > 0.40:
            return "FAIR"
        return "GOOD"

    def required_confidence(self, crawl_quality: str) -> str:
        if self.min_confidence in {"HIGH", "MEDIUM"}:
            return self.min_confidence
        if crawl_quality == "GOOD":
            return "MEDIUM"
        return "HIGH"

    @staticmethod
    def confidence_rank(value: str) -> int:
        return {"LOW": 1, "MEDIUM": 2, "HIGH": 3}.get(value, 0)

    @staticmethod
    def signal_rank(value: str) -> int:
        return {
            "MPN_GTIN": 6,
            "MPN": 5,
            "MPN_PARTIAL_GTIN": 4,
            "MPN_PARTIAL": 3,
            "GTIN": 2,
            "URL_HANDLE": 1,
            "NONE": 0,
        }.get(value, 0)

    def score_candidate(self, sys_row: dict[str, Any], scrape_idx: int) -> CandidateResult:
        row = self.scrape_rows[scrape_idx]
        raw = row["raw"]
        reasons: list[str] = []
        flags = {
            "gtin_match": False,
            "mpn_exact_all": False,
            "mpn_partial_all": False,
            "mpn_any": False,
            "url_key_match": False,
            "brand_exact": False,
            "brand_clone": False,
            "brand_conflict": False,
        }
        signal = "NONE"
        remark = ""
        score = 0.0

        if sys_row.get("_gtin_is_unique") and all_tokens_match_strict(row["_gtin_tokens"], sys_row["_gtin_tokens"], partial=False):
            flags["gtin_match"] = True
            reasons.append("GTIN exact (all values)")

        comp_mpn_tokens = row["_mpn_tokens"]
        mpn_tokens = sys_row["_mpn_tokens"]
        sku_tokens = sys_row["_sku_tokens"]
        part_tokens = sys_row["_part_tokens"]

        if comp_mpn_tokens:
            token_sources: list[tuple[str, list[str]]] = []
            if len(mpn_tokens) > 1:
                # For multi-token system MPN, require full coverage against system MPN only.
                token_sources = [("MPN", mpn_tokens)]
            else:
                if mpn_tokens:
                    token_sources.append(("MPN", mpn_tokens))
                if sku_tokens:
                    token_sources.append(("SKU", sku_tokens))
                if part_tokens:
                    token_sources.append(("PART", part_tokens))

            exact_sources: list[str] = []
            partial_sources: list[str] = []
            for label, system_tokens in token_sources:
                if all_tokens_match_strict(comp_mpn_tokens, system_tokens, partial=False):
                    exact_sources.append(label)
                if all_tokens_match_strict(comp_mpn_tokens, system_tokens, partial=True):
                    partial_sources.append(label)

            primary_available = bool(mpn_tokens or sku_tokens)
            exact_primary = any(source in {"MPN", "SKU"} for source in exact_sources)
            partial_primary = any(source in {"MPN", "SKU"} for source in partial_sources)
            flags["mpn_exact_all"] = bool(exact_sources) and (exact_primary or not primary_available)
            flags["mpn_partial_all"] = bool(partial_sources) and (partial_primary or not primary_available)
            flags["mpn_any"] = flags["mpn_exact_all"] or flags["mpn_partial_all"]
            if exact_sources:
                reasons.append(f"MPN exact full-token match on {', '.join(exact_sources)}")
            elif partial_sources:
                reasons.append(f"MPN partial full-token match on {', '.join(partial_sources)}")

        if flags["mpn_exact_all"] and flags["gtin_match"]:
            signal = "MPN_GTIN"
            score = 1000
            reasons.append("MPN exact + GTIN exact")
        elif flags["mpn_exact_all"]:
            signal = "MPN"
            score = 900
            reasons.append("MPN exact (system MPN/SKU/PART coverage)")
        elif flags["mpn_partial_all"] and flags["gtin_match"]:
            signal = "MPN_PARTIAL_GTIN"
            score = 840
            reasons.append("MPN partial + GTIN exact")
        elif flags["mpn_partial_all"]:
            signal = "MPN_PARTIAL"
            score = 760
            reasons.append("MPN partial (prefix/suffix)")
        elif flags["gtin_match"]:
            signal = "GTIN"
            score = 700
            reasons.append("GTIN exact")

        if sys_row["_url_slug"] and row["_handle"] and sys_row["_url_slug"] == row["_handle"]:
            flags["url_key_match"] = True
            if signal == "NONE":
                signal = "URL_HANDLE"
                score = 450
                reasons.append("URL slug == scrape handle")

        relation = brand_relation(sys_row.get("brand_label", ""), raw.get(self.scrape_brand_col, ""))
        if relation == "EXACT":
            flags["brand_exact"] = True
            score += 120
            reasons.append("Brand exact")
        elif relation == "CLONE":
            flags["brand_clone"] = True
            score += 65
            remark = "Clone brand match"
            reasons.append("Brand clone/synonym")
        elif relation == "MISMATCH":
            flags["brand_conflict"] = True
            score -= 120
            reasons.append("Brand mismatch")

        similarity = name_similarity(sys_row.get("product_name", ""), raw.get("Ref Product Name", ""))
        if similarity >= 70:
            score += 90
            reasons.append(f"Name similarity {similarity:.1f}% (high)")
        elif similarity >= 45:
            score += 55
            reasons.append(f"Name similarity {similarity:.1f}% (medium)")
        elif similarity >= 25:
            score += 20
            reasons.append(f"Name similarity {similarity:.1f}% (low)")

        if signal in {"MPN_GTIN", "MPN"}:
            confidence = "HIGH"
        elif signal in {"MPN_PARTIAL_GTIN", "MPN_PARTIAL", "GTIN"}:
            confidence = "MEDIUM"
        else:
            confidence = "LOW"

        if flags["brand_conflict"] and signal not in {"MPN_GTIN", "MPN", "GTIN"}:
            confidence = "LOW"

        return CandidateResult(
            idx=scrape_idx,
            signal=signal,
            score=int(round(score)),
            confidence=confidence,
            remark=remark,
            name_similarity=similarity,
            reasons=reasons,
            flags=flags,
        )

    def collect_candidate_indices(self, sys_row: dict[str, Any]) -> set[int]:
        candidates: set[int] = set()
        brand = sys_row["_brand"]

        if sys_row.get("_gtin_is_unique"):
            for token in sys_row["_gtin_tokens"]:
                candidates.update(self.scrape_indexes["gtin"].get(token, []))

        for token in sys_row["_search_id_tokens"]:
            candidates.update(self.scrape_indexes["mpn"].get(token, []))
            core = mpn_core_token(token)
            if core:
                candidates.update(self.scrape_indexes["mpn_core"].get(core, []))
            family = mpn_family_key(token)
            if family:
                candidates.update(self.scrape_indexes["mpn_family"].get(family, []))
            if brand:
                candidates.update(self.scrape_indexes["brand_mpn"].get(f"{brand}|{token}", []))

        if sys_row["_url_slug"]:
            candidates.update(self.scrape_indexes["handle"].get(sys_row["_url_slug"], []))

        return candidates

    def collect_cm_received_candidate_indices(self, cm_row: dict[str, Any] | None) -> set[int]:
        if cm_row is None:
            return set()

        candidates: set[int] = set()
        for token in id_tokens(cm_row.get("cm_received_sku", "")):
            candidates.update(self.scrape_indexes["mpn"].get(token, []))
            core = mpn_core_token(token)
            if core:
                candidates.update(self.scrape_indexes["mpn_core"].get(core, []))
            family = mpn_family_key(token)
            if family:
                candidates.update(self.scrape_indexes["mpn_family"].get(family, []))
        for token in numeric_tokens(cm_row.get("cm_received_sku", "")):
            candidates.update(self.scrape_indexes["gtin"].get(token, []))
        return candidates

    def best_candidate(
        self,
        sys_row: dict[str, Any],
        candidate_indices: set[int],
        allow_used: bool = False,
    ) -> tuple[CandidateResult | None, bool, list[CandidateResult]]:
        scored: list[CandidateResult] = []
        for idx in candidate_indices:
            if not allow_used and idx in self.used_scrape_indices:
                continue
            scored.append(self.score_candidate(sys_row, idx))

        if not scored:
            return None, False, []

        scored.sort(
            key=lambda c: (
                -c.score,
                -self.signal_rank(c.signal),
                -int(c.flags["brand_exact"]),
                -int(c.flags["brand_clone"]),
                -c.name_similarity,
                c.idx,
            )
        )
        top = scored[0]
        if top.signal == "NONE" or top.score < 300:
            return None, False, scored[:5]

        ambiguous = False
        if len(scored) > 1:
            second = scored[1]
            if abs(top.score - second.score) <= 20 and top.signal == second.signal:
                ambiguous = True
        return top, ambiguous, scored[:5]

    def evaluate_existing_url(
        self,
        sys_row: dict[str, Any],
        cm_row: dict[str, Any] | None,
    ) -> tuple[str, CandidateResult | None]:
        if cm_row is None:
            return "NO_CM", None

        pkey = cm_row.get("_path_key", "")
        cm_url = cm_row.get("competitor_url", "")
        idxs: set[int] = set()
        if pkey:
            idxs.update(self.scrape_indexes["path_key"].get(pkey, []))
        else:
            fp = cm_row.get("_url_fp", "")
            if fp:
                idxs.update(self.scrape_indexes["url_fp"].get(fp, []))

        if not idxs:
            return "CM_URL_NOT_IN_SCRAPE", None

        if cm_url:
            param_matched = {
                idx
                for idx in idxs
                if url_matches_scrape_params(cm_url, self.scrape_rows[idx]["raw"].get("Ref Product URL", ""))
            }
            if param_matched:
                anchor_idx = min(param_matched)
                anchor = self.score_candidate(sys_row, anchor_idx)
                anchor.reasons.insert(0, "CM URL key+params matched scrape")
                strong_signal = anchor.signal in {"MPN_GTIN", "MPN", "MPN_PARTIAL_GTIN", "MPN_PARTIAL", "GTIN"}
                if strong_signal:
                    return "CM_URL_MATCH_CORRECT", anchor
                return "CM_URL_MATCH_WEAK", anchor
            elif pkey:
                anchor, _, _ = self.best_candidate(sys_row, idxs, allow_used=True)
                if anchor is None and idxs:
                    anchor_idx = min(idxs)
                    anchor = self.score_candidate(sys_row, anchor_idx)
                if anchor is not None:
                    anchor.reasons.insert(0, "CM URL path matched scrape (params differ/missing)")
                    return "CM_URL_MATCH_WEAK", anchor
                return "CM_URL_MATCH_WRONG", None

        top, _, _ = self.best_candidate(sys_row, idxs, allow_used=True)
        if top is None:
            return "CM_URL_MATCH_WRONG", None

        strong = top.signal in {"MPN_GTIN", "MPN", "MPN_PARTIAL_GTIN", "MPN_PARTIAL", "GTIN"}
        if strong and (not top.flags["brand_conflict"] or top.signal == "MPN_GTIN"):
            return "CM_URL_MATCH_CORRECT", top
        return "CM_URL_MATCH_WRONG", top

    def evaluate_product(
        self,
        sys_row: dict[str, Any],
        cm_row: dict[str, Any] | None,
        required_confidence: str,
        history: dict[str, int],
        history_out: dict[str, int],
    ) -> str:
        pid = sys_row["product_id"]
        existing_state, existing_hit = self.evaluate_existing_url(sys_row, cm_row)
        candidates = self.collect_candidate_indices(sys_row)
        candidates.update(self.collect_cm_received_candidate_indices(cm_row))
        best, ambiguous, top5 = self.best_candidate(sys_row, candidates, allow_used=False)
        best_blocked_by_used = False
        if best is None:
            fallback_best, fallback_ambiguous, fallback_top5 = self.best_candidate(sys_row, candidates, allow_used=True)
            if fallback_best is not None and fallback_best.idx in self.used_scrape_indices:
                best = fallback_best
                ambiguous = fallback_ambiguous
                top5 = fallback_top5
                best_blocked_by_used = True

        best_url = ""
        best_name = ""
        best_ref_sku = ""
        best_score = ""
        best_conf = ""
        best_signal = ""
        best_name_similarity = ""
        best_remark = ""
        best_reasons = ""
        top_candidates = ""

        if best is not None:
            raw = self.scrape_rows[best.idx]["raw"]
            best_url = raw.get("Ref Product URL", "")
            best_url_norm = url_fingerprint(best_url)
            best_name = raw.get("Ref Product Name", "")
            best_ref_sku = raw.get("Ref MPN", "") or raw.get("Ref SKU", "")
            best_score = str(best.score)
            best_conf = best.confidence
            best_signal = best.signal
            best_name_similarity = f"{best.name_similarity:.1f}"
            best_remark = best.remark
            best_reasons = "; ".join(best.reasons[:8])
            if best.flags.get("brand_conflict") and best.signal in {"MPN_GTIN", "MPN", "GTIN"}:
                if best_remark:
                    best_remark = f"{best_remark}; Brand mismatch overridden by exact key"
                else:
                    best_remark = "Brand mismatch overridden by exact key"
        if top5:
            parts = []
            for cand in top5:
                raw = self.scrape_rows[cand.idx]["raw"]
                parts.append(f"{raw.get('Ref Product URL','')}#{cand.signal}:{cand.score}")
            top_candidates = " | ".join(parts)

        required_rank = self.confidence_rank(required_confidence)
        best_rank = self.confidence_rank(best.confidence) if best else 0
        exact_reuse_allowed = bool(
            best is not None
            and best_blocked_by_used
            and best.signal in {"MPN_GTIN", "MPN", "GTIN"}
            and not ambiguous
        )
        brand_override_allowed = bool(
            best is not None
            and best.flags["brand_conflict"]
            and best.signal in {"MPN_GTIN", "MPN", "GTIN"}
            and existing_state in {"CM_URL_MATCH_WRONG", "CM_URL_NOT_IN_SCRAPE"}
        )
        best_safe = bool(
            best is not None
            and best.signal != "NONE"
            and best_rank >= required_rank
            and not ambiguous
            and (not best_blocked_by_used or exact_reuse_allowed)
            and (not best.flags["brand_conflict"] or brand_override_allowed)
        )

        # Prevent duplicate URL allocation across add/replace categories
        if best_safe and best_url:
            best_url_norm = url_fingerprint(best_url)
            if best_url_norm in self.allocated_ref_urls:
                best_safe = False

        decision = "NO_MATCH"
        decision_reason = ""

        cm_url = cm_row.get("competitor_url", "") if cm_row else ""
        cm_reason = cm_row.get("reason", "") if cm_row else ""
        cm_competitor_id = cm_row.get("competitor_id", "") if cm_row else ""
        if not cm_competitor_id:
            cm_competitor_id = self.cm_competitor_id

        if existing_state == "CM_URL_MATCH_CORRECT":
            decision = "KEEP_EXISTING"
            decision_reason = "Existing CM URL validated from scrape"
            if existing_hit is not None:
                existing_raw = self.scrape_rows[existing_hit.idx]["raw"]
                best_url = existing_raw.get("Ref Product URL", "")
                best_name = existing_raw.get("Ref Product Name", "")
                best_ref_sku = existing_raw.get("Ref MPN", "") or existing_raw.get("Ref SKU", "")
                best_score = str(existing_hit.score)
                best_conf = existing_hit.confidence
                best_signal = existing_hit.signal
                best_name_similarity = f"{existing_hit.name_similarity:.1f}"
                best_remark = existing_hit.remark
                best_reasons = "; ".join(existing_hit.reasons[:8])
                self.used_scrape_indices.add(existing_hit.idx)

            if cm_row and wrong_reason(cm_reason):
                self.approve_rows.append(
                    {
                        "product_id": pid,
                        "competitor_id": cm_competitor_id,
                        "type": "update",
                        "source": "CM",
                        "is_issue": "Approved",
                    }
                )
            history_out[pid] = 0

        elif existing_state == "CM_URL_MATCH_WEAK":
            replace_needed = bool(best_url and (not cm_url or not url_matches_scrape_params(cm_url, best_url)))
            prefer_existing_brand = bool(
                existing_hit is not None
                and (existing_hit.flags.get("brand_exact") or existing_hit.flags.get("brand_clone"))
                and best is not None
                and best.flags.get("brand_conflict")
            )
            if prefer_existing_brand:
                decision = "KEEP_EXISTING"
                decision_reason = "Existing URL path is brand-aligned; cross-brand candidate ignored"
                existing_raw = self.scrape_rows[existing_hit.idx]["raw"]
                best_url = existing_raw.get("Ref Product URL", "")
                best_name = existing_raw.get("Ref Product Name", "")
                best_ref_sku = existing_raw.get("Ref MPN", "") or existing_raw.get("Ref SKU", "")
                best_score = str(existing_hit.score)
                best_conf = existing_hit.confidence
                best_signal = existing_hit.signal
                best_name_similarity = f"{existing_hit.name_similarity:.1f}"
                best_remark = existing_hit.remark
                best_reasons = "; ".join(existing_hit.reasons[:8])
                self.used_scrape_indices.add(existing_hit.idx)
            elif best_safe and replace_needed:
                decision = "REPLACE_WRONG"
                decision_reason = "Existing URL found but weak identity; stronger replacement found"
                self.new_update_rows.append(
                    {
                        "product_id": pid,
                        "competitor_id": cm_competitor_id,
                        "sku": sys_row.get("sku", ""),
                        "ref_sku": best_ref_sku,
                        "ref_url": best_url,
                        "ref_name": best_name,
                        "send_in_feed": 1,
                        "action": "replace_wrong_match",
                        "confidence": best_conf,
                        "score": best_score,
                        "remark": best_remark,
                        "existing_url": cm_url,
                    }
                )
                self.used_scrape_indices.add(best.idx)
            elif existing_hit is not None:
                decision = "KEEP_EXISTING"
                decision_reason = "Existing URL validated by key+params; no stronger safe replacement"
                existing_raw = self.scrape_rows[existing_hit.idx]["raw"]
                best_url = existing_raw.get("Ref Product URL", "")
                best_name = existing_raw.get("Ref Product Name", "")
                best_ref_sku = existing_raw.get("Ref MPN", "") or existing_raw.get("Ref SKU", "")
                best_score = str(existing_hit.score)
                best_conf = existing_hit.confidence
                best_signal = existing_hit.signal
                best_name_similarity = f"{existing_hit.name_similarity:.1f}"
                best_remark = existing_hit.remark
                best_reasons = "; ".join(existing_hit.reasons[:8])
                self.used_scrape_indices.add(existing_hit.idx)
            else:
                decision = "NO_MATCH"
                decision_reason = "Weak existing URL match and no safe candidate"

            if best_signal and best_signal == "NONE":
                decision = "WRONG_NO_REPLACEMENT"
                decision_reason = "Existing mapping wrong and no valid candidate signal"
                self.wrong_no_replacement_rows.append(
                    {
                        "competitor_id": cm_competitor_id,
                        "product_id": pid,
                        "sku": sys_row.get("sku", ""),
                        "brand_label": sys_row.get("brand_label", ""),
                        "cm_url": cm_url,
                        "cm_reason": cm_reason,
                        "best_candidate_url": "",
                        "best_candidate_score": "",
                        "best_candidate_confidence": "",
                    }
                )
            history_out[pid] = 0

        elif existing_state == "CM_URL_MATCH_WRONG":
            # If no valid signal candidate exists, treat as wrong with no replacement
            if best is None or best.signal == "NONE":
                decision = "WRONG_NO_REPLACEMENT"
                decision_reason = "Existing mapping wrong and no valid candidate signal"
                self.wrong_no_replacement_rows.append(
                    {
                        "competitor_id": cm_competitor_id,
                        "product_id": pid,
                        "sku": sys_row.get("sku", ""),
                        "brand_label": sys_row.get("brand_label", ""),
                        "cm_url": cm_url,
                        "cm_reason": cm_reason,
                        "best_candidate_url": "",
                        "best_candidate_score": "",
                        "best_candidate_confidence": "",
                    }
                )
                history_out[pid] = 0
            else:
                replace_needed = bool(best_url and (not cm_url or not url_matches_scrape_params(cm_url, best_url)))
                if best_safe and replace_needed:
                    decision = "REPLACE_WRONG"
                    decision_reason = "Existing mapping wrong; safe replacement found"
                    self.new_update_rows.append(
                        {
                            "product_id": pid,
                            "competitor_id": cm_competitor_id,
                            "sku": sys_row.get("sku", ""),
                            "ref_sku": best_ref_sku,
                            "ref_url": best_url,
                            "ref_name": best_name,
                            "send_in_feed": 1,
                            "action": "replace_wrong_match",
                            "confidence": best_conf,
                            "score": best_score,
                            "remark": best_remark,
                            "existing_url": cm_url,
                        }
                    )
                    self.used_scrape_indices.add(best.idx)
                    if best_url:
                        self.allocated_ref_urls.add(url_fingerprint(best_url))
                    history_out[pid] = 0
                elif ambiguous:
                    decision = "MANUAL_REVIEW"
                    decision_reason = "Multiple close candidates"
                    self.manual_review_rows.append(
                        {
                            "competitor_id": cm_competitor_id,
                            "product_id": pid,
                            "sku": sys_row.get("sku", ""),
                            "cm_url": cm_url,
                            "cm_reason": cm_reason,
                            "top_candidates": top_candidates,
                        }
                    )
                    history_out[pid] = 0
                elif best_blocked_by_used and best is not None and best.signal in {"MPN_PARTIAL", "MPN_PARTIAL_GTIN"}:
                    decision = "WRONG_NO_REPLACEMENT"
                    decision_reason = "Partial candidate already allocated to stronger match"
                    self.wrong_no_replacement_rows.append(
                        {
                            "competitor_id": cm_competitor_id,
                            "product_id": pid,
                            "sku": sys_row.get("sku", ""),
                            "brand_label": sys_row.get("brand_label", ""),
                            "cm_url": cm_url,
                            "cm_reason": cm_reason,
                            "best_candidate_url": best_url,
                            "best_candidate_score": best_score,
                            "best_candidate_confidence": best_conf,
                        }
                    )
                    history_out[pid] = 0
                elif best_blocked_by_used and best is not None:
                    decision = "MANUAL_REVIEW"
                    decision_reason = "Strong candidate exists but already allocated to another product"
                    self.manual_review_rows.append(
                        {
                            "competitor_id": cm_competitor_id,
                            "product_id": pid,
                            "sku": sys_row.get("sku", ""),
                            "cm_url": cm_url,
                            "cm_reason": cm_reason,
                            "top_candidates": top_candidates,
                        }
                    )
                    history_out[pid] = 0
                else:
                    decision = "WRONG_NO_REPLACEMENT"
                    decision_reason = "No safe replacement candidate in scrape"
                    self.wrong_no_replacement_rows.append(
                        {
                            "competitor_id": cm_competitor_id,
                            "product_id": pid,
                            "sku": sys_row.get("sku", ""),
                            "brand_label": sys_row.get("brand_label", ""),
                            "cm_url": cm_url,
                            "cm_reason": cm_reason,
                            "best_candidate_url": best_url,
                            "best_candidate_score": best_score,
                            "best_candidate_confidence": best_conf,
                        }
                    )
                    history_out[pid] = 0

        elif existing_state == "CM_URL_NOT_IN_SCRAPE":
            miss_count = int(history.get(pid, 0)) + 1
            history_out[pid] = miss_count

            if best_safe:
                decision = "REPLACE_MISSING_URL"
                decision_reason = "Existing URL missing in scrape; replacement found"
                self.new_update_rows.append(
                    {
                        "product_id": pid,
                        "competitor_id": cm_competitor_id,
                        "sku": sys_row.get("sku", ""),
                        "ref_sku": best_ref_sku,
                        "ref_url": best_url,
                        "ref_name": best_name,
                        "send_in_feed": 1,
                        "action": "replace_missing_url",
                        "confidence": best_conf,
                        "score": best_score,
                        "remark": best_remark,
                        "existing_url": cm_url,
                    }
                )
                self.used_scrape_indices.add(best.idx)
                if best_url:
                    self.allocated_ref_urls.add(url_fingerprint(best_url))
                history_out[pid] = 0
            else:
                decision = "CRAWL_MISS_STALE" if miss_count >= 3 else "CRAWL_MISS_PENDING"
                decision_reason = "Existing URL not found in scrape and no safe replacement"
                self.crawl_retry_rows.append(
                    {
                        "competitor_id": cm_competitor_id,
                        "product_id": pid,
                        "sku": sys_row.get("sku", ""),
                        "mpn": sys_row.get("mpn", ""),
                        "gtin": sys_row.get("gtin", ""),
                        "brand_label": sys_row.get("brand_label", ""),
                        "existing_url": cm_url,
                        "retry_query": f"{sys_row.get('brand_label','')} {sys_row.get('mpn','')}".strip(),
                        "miss_count": miss_count,
                        "status": decision,
                    }
                )

        else:
            # NO_CM
            # If no valid signal candidate exists, treat as wrong with no replacement
            if best is not None and best.signal == "NONE":
                decision = "WRONG_NO_REPLACEMENT"
                decision_reason = "No matching candidate signal"
                self.wrong_no_replacement_rows.append(
                    {
                        "competitor_id": cm_competitor_id,
                        "product_id": pid,
                        "sku": sys_row.get("sku", ""),
                        "brand_label": sys_row.get("brand_label", ""),
                        "cm_url": cm_url,
                        "cm_reason": "",
                        "best_candidate_url": "",
                        "best_candidate_score": "",
                        "best_candidate_confidence": "",
                    }
                )
            elif best_safe:
                decision = "ADD_NEW_MATCH"
                decision_reason = "No existing CM mapping; safe candidate found"
                self.new_update_rows.append(
                    {
                        "product_id": pid,
                        "competitor_id": cm_competitor_id,
                        "sku": sys_row.get("sku", ""),
                        "ref_sku": best_ref_sku,
                        "ref_url": best_url,
                        "ref_name": best_name,
                        "send_in_feed": 1,
                        "action": "add_new_match",
                        "confidence": best_conf,
                        "score": best_score,
                        "remark": best_remark,
                        "existing_url": "",
                    }
                )
                self.used_scrape_indices.add(best.idx)
                if best_url:
                    self.allocated_ref_urls.add(url_fingerprint(best_url))
            elif best is not None and best.flags["brand_conflict"]:
                decision = "MANUAL_REVIEW"
                decision_reason = "Brand mismatch for no-CM candidate"
                self.manual_review_rows.append(
                    {
                        "competitor_id": cm_competitor_id,
                        "product_id": pid,
                        "sku": sys_row.get("sku", ""),
                        "cm_url": "",
                        "cm_reason": "",
                        "top_candidates": top_candidates,
                    }
                )
            elif ambiguous:
                decision = "MANUAL_REVIEW"
                decision_reason = "No CM; ambiguous candidate set"
                self.manual_review_rows.append(
                    {
                        "competitor_id": cm_competitor_id,
                        "product_id": pid,
                        "sku": sys_row.get("sku", ""),
                        "cm_url": "",
                        "cm_reason": "",
                        "top_candidates": top_candidates,
                    }
                )
            elif best_blocked_by_used and best is not None:
                decision = "MANUAL_REVIEW"
                decision_reason = "Strong candidate exists but already allocated to another product"
                self.manual_review_rows.append(
                    {
                        "competitor_id": cm_competitor_id,
                        "product_id": pid,
                        "sku": sys_row.get("sku", ""),
                        "cm_url": "",
                        "cm_reason": "",
                        "top_candidates": top_candidates,
                    }
                )
            else:
                decision = "NO_MATCH"
                decision_reason = "No safe candidate"
            history_out[pid] = 0

        self.report_rows.append(
            {
                "competitor_id": cm_competitor_id,
                "product_id": pid,
                "sku": sys_row.get("sku", ""),
                "our_mpn": sys_row.get("mpn", ""),
                "our_gtin": sys_row.get("gtin", ""),
                "our_brand": sys_row.get("brand_label", ""),
                "our_category": sys_row.get("cat", ""),
                "existing_competitor_url": cm_url,
                "existing_reason": cm_reason,
                "existing_state": existing_state,
                "best_candidate_url": best_url,
                "best_candidate_name": best_name,
                "best_candidate_ref_sku": best_ref_sku,
                "best_candidate_score": best_score,
                "best_candidate_confidence": best_conf,
                "best_candidate_signal": best_signal,
                "best_candidate_name_similarity": best_name_similarity,
                "best_candidate_remark": best_remark,
                "best_candidate_reasons": best_reasons,
                "top_candidates": top_candidates,
                "decision": decision,
                "decision_reason": decision_reason,
            }
        )
        return decision

    def build_unmatched_scrape_rows(self) -> None:
        for idx, row in enumerate(self.scrape_rows):
            if idx not in self.used_scrape_indices:
                self.unmatched_scrape_rows.append(row["raw"])

    def build_unmatch_matched_with_cm(self) -> None:
        unresolved_states = {
            "WRONG_NO_REPLACEMENT",
            "CRAWL_MISS_PENDING",
            "CRAWL_MISS_STALE",
            "MANUAL_REVIEW",
            "NO_MATCH",
        }
        cm_path_lookup: dict[str, dict[str, Any]] = {}
        for pid, cm in self.cm_by_product.items():
            state = self.decision_by_product.get(pid, "")
            if state not in unresolved_states:
                continue
            pkey = cm.get("_path_key", "")
            if not pkey:
                continue
            cm_path_lookup[pkey] = {
                "product_id": pid,
                "data": cm,
                "system_data": self.system.get(pid, {}),
            }

        all_headers = self.dynamic_merge_headers(cm_path_lookup, self.unmatched_scrape_rows)
        matched_rows: list[dict[str, Any]] = []
        matched_unmatch_indices: set[int] = set()

        for idx, row in enumerate(self.unmatched_scrape_rows):
            pkey = path_key(row.get("Ref Product URL", ""))
            if not pkey:
                continue
            if pkey not in cm_path_lookup:
                continue

            cm_info = cm_path_lookup[pkey]
            matched_rows.append(
                self.dynamic_merge_row(
                    cm_info["data"],
                    cm_info["system_data"],
                    row,
                    pkey,
                    "MATCHED",
                    all_headers,
                )
            )
            matched_unmatch_indices.add(idx)
            del cm_path_lookup[pkey]

        for pkey, cm_info in cm_path_lookup.items():
            matched_rows.append(
                self.dynamic_merge_row(
                    cm_info["data"],
                    cm_info["system_data"],
                    {},
                    pkey,
                    "NO_MATCH",
                    all_headers,
                )
            )

        if matched_unmatch_indices:
            self.unmatched_scrape_rows = [
                row for idx, row in enumerate(self.unmatched_scrape_rows) if idx not in matched_unmatch_indices
            ]

        self.unmatch_matched_with_cm_rows = matched_rows

    @staticmethod
    def dynamic_merge_headers(cm_lookup: dict[str, dict[str, Any]], unmatch_rows: list[dict[str, Any]]) -> list[str]:
        headers: dict[str, bool] = {"match_path_key": True, "match_status": True}
        if cm_lookup:
            sample = next(iter(cm_lookup.values()))
            for key in sample.get("data", {}).keys():
                if key.startswith("_"):
                    continue
                headers[f"cm_{key}"] = True
            for key in sample.get("system_data", {}).keys():
                if key.startswith("_"):
                    continue
                headers[f"osb_{key}"] = True
        if unmatch_rows:
            for key in unmatch_rows[0].keys():
                clean_key = key.replace("Ref ", "").replace(" ", "_").lower().strip()
                headers[f"competitor_{clean_key}"] = True
        return list(headers.keys())

    @staticmethod
    def dynamic_merge_row(
        cm_row: dict[str, Any],
        system_row: dict[str, Any],
        comp_row: dict[str, Any],
        pkey: str,
        status: str,
        headers: list[str],
    ) -> dict[str, Any]:
        out = {h: "" for h in headers}
        for key, value in cm_row.items():
            if key.startswith("_"):
                continue
            out[f"cm_{key}"] = clean_text(value)
        for key, value in system_row.items():
            if key.startswith("_"):
                continue
            out[f"osb_{key}"] = clean_text(value)
        for key, value in comp_row.items():
            clean_key = key.replace("Ref ", "").replace(" ", "_").lower().strip()
            out[f"competitor_{clean_key}"] = clean_text(value)
        out["match_path_key"] = pkey
        out["match_status"] = status
        return out

    def load_history(self) -> dict[str, int]:
        if not self.history_file.exists():
            return {}
        try:
            payload = json.loads(self.history_file.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                return {}
            result: dict[str, int] = {}
            for key, value in payload.items():
                try:
                    result[str(key)] = int(value)
                except (TypeError, ValueError):
                    continue
            return result
        except (json.JSONDecodeError, OSError):
            return {}

    def save_history(self, history: dict[str, int]) -> None:
        cleaned = {k: int(v) for k, v in history.items() if int(v) > 0}
        write_json(self.history_file, cleaned)

    def write_outputs(self, crawl_quality: str, required_conf: str) -> None:
        # Ensure ref_url appears only once across key decision types
        # Count occurrences of best_candidate_url across decisions:
        # KEEP_EXISTING, ADD_NEW_MATCH, REPLACE_WRONG, REPLACE_MISSING_URL
        decision_url_counter: dict[str, int] = {}

        allowed_decisions = {
            "KEEP_EXISTING",
            "ADD_NEW_MATCH",
            "REPLACE_WRONG",
            "REPLACE_MISSING_URL",
        }

        for row in self.report_rows:
            decision = clean_text(row.get("decision", ""))
            url = clean_text(row.get("best_candidate_url", ""))
            if not url or decision not in allowed_decisions:
                continue
            key = url_fingerprint(url)
            decision_url_counter[key] = decision_url_counter.get(key, 0) + 1

        # Keep only URLs that appear exactly once across these decisions
        filtered_new_update_rows: list[dict[str, Any]] = []
        for row in self.new_update_rows:
            ref_url = clean_text(row.get("ref_url", ""))
            if not ref_url:
                continue
            key = url_fingerprint(ref_url)

            # If URL assigned more than once anywhere, skip entirely
            if decision_url_counter.get(key, 0) == 1:
                filtered_new_update_rows.append(row)

        self.new_update_rows = filtered_new_update_rows

        match_report_headers = [
            "product_id",
            "competitor_id",
            "sku",
            "our_mpn",
            "our_gtin",
            "our_brand",
            "our_category",
            "existing_competitor_url",
            "existing_reason",
            "existing_state",
            "best_candidate_url",
            "best_candidate_name",
            "best_candidate_ref_sku",
            "best_candidate_score",
            "best_candidate_confidence",
            "best_candidate_signal",
            "best_candidate_name_similarity",
            "best_candidate_remark",
            "best_candidate_reasons",
            "top_candidates",
            "decision",
            "decision_reason",
        ]
        new_update_headers = [
            "product_id",
            "competitor_id",
            "sku",
            "ref_sku",
            "ref_url",
            "ref_name",
            "send_in_feed",
            "action",
            "confidence",
            "score",
            "remark",
            "existing_url",
        ]
        approve_headers = ["product_id", "competitor_id", "type", "source", "is_issue"]
        wrong_headers = [
            "product_id",
            "competitor_id",
            "sku",
            "brand_label",
            "cm_url",
            "cm_reason",
            "best_candidate_url",
            "best_candidate_score",
            "best_candidate_confidence",
        ]
        manual_headers = ["product_id", "competitor_id", "sku", "cm_url", "cm_reason", "top_candidates"]
        retry_headers = [
            "product_id",
            "competitor_id",
            "sku",
            "mpn",
            "gtin",
            "brand_label",
            "existing_url",
            "retry_query",
            "miss_count",
            "status",
        ]

        outputs = {
            "match_product_report.csv": (self.report_rows, match_report_headers),
            "new_update_matches.csv": (self.new_update_rows, new_update_headers),
            "approve_mark_products.csv": (self.approve_rows, approve_headers),
            "wrong_no_replacement.csv": (self.wrong_no_replacement_rows, wrong_headers),
            "manual_review.csv": (self.manual_review_rows, manual_headers),
            "crawl_retry_queue.csv": (self.crawl_retry_rows, retry_headers),
            "unmatch_products.csv": (self.unmatched_scrape_rows, self.scrape_headers),
        }

        for filename, (rows, headers) in outputs.items():
            write_csv(self.output_dir / filename, rows, headers)

        # dynamic file
        if self.unmatch_matched_with_cm_rows:
            dynamic_headers = list(self.unmatch_matched_with_cm_rows[0].keys())
        else:
            dynamic_headers = ["match_path_key", "match_status"]
        write_csv(
            self.output_dir / "unmatch_matched_with_cm.csv",
            self.unmatch_matched_with_cm_rows,
            dynamic_headers,
        )

        zip_path = self.output_dir / f"{self.output_dir.name}.zip"
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for filename in [
                "match_product_report.csv",
                "new_update_matches.csv",
                "approve_mark_products.csv",
                "wrong_no_replacement.csv",
                # "manual_review.csv",
                # "crawl_retry_queue.csv",
                # "unmatch_products.csv",
                # "unmatch_matched_with_cm.csv",
            ]:
                file_path = self.output_dir / filename
                if file_path.exists():
                    zf.write(file_path, arcname=filename)

        self.summary = {
            "scrape_file": str(self.scrape_file),
            "system_file": str(self.system_file),
            "cm_file": str(self.cm_file),
            "scrape_domain": self.scrape_domain,
            "cm_competitor_id": self.cm_competitor_id,
            "crawl_quality": crawl_quality,
            "required_confidence": required_conf,
            "products_evaluated": len(self.report_rows),
            "new_update_matches": len(self.new_update_rows),
            "approve_rows": len(self.approve_rows),
            "wrong_no_replacement": len(self.wrong_no_replacement_rows),
            "manual_review": len(self.manual_review_rows),
            "crawl_retry_queue": len(self.crawl_retry_rows),
            "unmatch_products": len(self.unmatched_scrape_rows),
            "url_matched_unmatch_with_cm": sum(
                1 for row in self.unmatch_matched_with_cm_rows if clean_text(row.get("match_status")) == "MATCHED"
            ),
            "cm_rows_loaded": len(self.cm_by_product),
            "zip_file": str(zip_path),
        }
        write_json(self.output_dir / "reconcile_summary.json", self.summary)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Robust competitor match reconciliation pipeline.")
    parser.add_argument("scrape_file", nargs="?", default="afastore.csv")
    parser.add_argument("system_file", nargs="?", default="system.csv")
    parser.add_argument("cm_file", nargs="?", default="competitor-full.csv")
    parser.add_argument("--output-dir", default="reconcile_output")
    parser.add_argument("--history-file", default="match_missing_history.json")
    parser.add_argument("--limit", type=int, default=None, help="Optional scrape row limit for testing.")
    parser.add_argument(
        "--min-confidence",
        choices=["AUTO", "HIGH", "MEDIUM"],
        default="AUTO",
        help="Minimum confidence to allow auto add/replace decisions.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    pipeline = ReconciliationPipeline(
        scrape_file=Path(args.scrape_file),
        system_file=Path(args.system_file),
        cm_file=Path(args.cm_file),
        output_dir=Path(args.output_dir),
        history_file=Path(args.history_file),
        limit=args.limit,
        min_confidence=args.min_confidence,
    )
    summary = pipeline.run()
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
