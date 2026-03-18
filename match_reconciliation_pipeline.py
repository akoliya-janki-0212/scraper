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
from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

# Matching controls inspired by legacy PHP validation logic
STOP_WORDS: set[str] = {
    "by",
    "in",
    "the",
    "and",
    "collection",
    "is",
    "set",
    "of",
    "furniture",
    "home",
    "with",
    "small",
    "products",
    "product",
    "htm",
    "html",
}

EXCLUDE_CATEGORIES: set[str] = {
    "Dining Sets",
    "Home Bar Sets",
    "Bedroom Sets",
    "Living Room Sets",
    "Coffee Table Sets",
    "Home Office Sets",
    "Game Table Sets",
    "Bedding and Comforter Sets",
    "Outdoor Conversation Sets",
}

SYNONYMS: dict[str, list[str]] = {
    "gray": ["grey"],
    "grey": ["gray", "greystone"],
    "washedgray": ["washedgrey"],
    "greystone": ["grey"],
    "darkbrown": ["slate"],
    "slate": ["darkbrown"],
    "lightbrown": ["sand"],
    "sand": ["lightbrown"],
    "darkgray": ["darkgrey", "stormgray"],
    "darkgrey": ["darkgray"],
    "stormgray": ["darkgray"],
    "wardrobe": ["storage", "unit", "storageunit"],
    "storage": ["wardrobe"],
    "phillipe": ["philippe"],
    "philippe": ["phillipe"],
    "unit": ["wardrobe"],
    "californiaking": ["calking", "cking"],
    "calking": ["californiaking"],
    "cking": ["californiaking"],
    "philips": ["ps"],
    "caribbean": ["carribean"],
    "carribean": ["caribbean"],
    "blacksilver": ["black", "silver"],
}

EXCLUDE_SYNONYMS: dict[str, list[str]] = {
    "king": ["calking", "californiaking", "cking"],
}

BED_PART_TOKENS: set[str] = {"headboard", "footboard", "rails"}


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
    parts = [clean_text(p) for p in re.split(r"[,_;|]+", raw)]
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


@lru_cache(maxsize=50000)
def normalize_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", clean_text(value).lower())


@lru_cache(maxsize=50000)
def tokenize_text(value: str) -> tuple[str, ...]:
    tokens = [t for t in re.findall(r"[a-z0-9]+", clean_text(value).lower()) if t]
    filtered: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        if token in STOP_WORDS:
            continue
        if token not in seen:
            seen.add(token)
            filtered.append(token)
    return tuple(filtered)


@lru_cache(maxsize=20000)
def token_variants(token: str) -> tuple[str, ...]:
    base = normalize_text(token)
    if not base:
        return tuple()
    variants: set[str] = {base}
    variants.add(base + "s")
    variants.add(base + "es")
    variants.add(base.rstrip("s"))
    for syn in SYNONYMS.get(base, []):
        syn_norm = normalize_text(syn)
        if syn_norm:
            variants.add(syn_norm)
            variants.add(syn_norm + "s")
            variants.add(syn_norm.rstrip("s"))
    return tuple(v for v in variants if v)


def levenshtein_with_cutoff(left: str, right: str, max_dist: int) -> int:
    if left == right:
        return 0
    if abs(len(left) - len(right)) > max_dist:
        return max_dist + 1
    # Ensure left is the shorter string
    if len(left) > len(right):
        left, right = right, left
    previous = list(range(len(left) + 1))
    for i, rc in enumerate(right, start=1):
        current = [i]
        min_row = i
        for j, lc in enumerate(left, start=1):
            insertions = previous[j] + 1
            deletions = current[j - 1] + 1
            substitutions = previous[j - 1] + (lc != rc)
            val = min(insertions, deletions, substitutions)
            current.append(val)
            if val < min_row:
                min_row = val
        if min_row > max_dist:
            return max_dist + 1
        previous = current
    return previous[-1]


def fuzzy_token_match(token: str, haystack: list[str]) -> bool:
    if not token or not haystack:
        return False
    variants = token_variants(token)
    if not variants:
        return False
    needle = normalize_text(token)
    needle_len = len(needle)
    for candidate in haystack:
        cand_norm = normalize_text(candidate)
        if not cand_norm:
            continue
        if cand_norm in variants:
            return True
        if needle_len > 3 and abs(len(cand_norm) - needle_len) <= 2:
            for variant in variants:
                max_dist = int(max(1, len(variant) * 0.2))
                if levenshtein_with_cutoff(variant, cand_norm, max_dist) <= max_dist:
                    return True
    return False


def extract_url_tokens(url: str, include_query: bool = True) -> tuple[list[str], list[str]]:
    raw = clean_text(url).lower()
    if not raw:
        return [], []
    try:
        parsed = urlparse(raw)
    except ValueError:
        parsed = None
    path = parsed.path if parsed else raw
    query = parsed.query if (parsed and include_query) else ""
    text = f"{path} {query}".strip() if query else path
    text = re.sub(r"\.(html?|php|aspx?)$", "", text)
    tokens = [t for t in re.findall(r"[a-z0-9]+", text) if t]

    name_tokens: list[str] = []
    id_tokens: list[str] = []
    seen_name: set[str] = set()
    seen_id: set[str] = set()
    for token in tokens:
        if token not in seen_id:
            seen_id.add(token)
            id_tokens.append(token)
        if token in STOP_WORDS:
            continue
        if len(token) <= 1:
            continue
        if len(token) >= 6 and any(c.isdigit() for c in token) and any(c.isalpha() for c in token):
            # Likely SKU/MPN-like; skip for name matching
            continue
        if token not in seen_name:
            seen_name.add(token)
            name_tokens.append(token)
    return name_tokens, id_tokens


def extract_osb_tokens(osb_url: str, brand_label: str, collection: str) -> list[str]:
    raw = clean_text(osb_url).lower()
    if not raw:
        return []
    try:
        parsed = urlparse(raw)
        path = parsed.path or ""
    except ValueError:
        path = raw
    path = path.strip("/")
    if not path:
        return []
    tokens = [t for t in re.findall(r"[a-z0-9]+", path) if t]
    brand_tokens = [t for t in re.findall(r"[a-z0-9]+", clean_text(brand_label).lower()) if t]
    collection_clean = clean_text(collection).lower().replace("collection", "")
    collection_tokens = [t for t in re.findall(r"[a-z0-9]+", collection_clean) if t]
    filtered = [t for t in tokens if t not in brand_tokens and t not in collection_tokens]
    return filtered


def url_has_set_token(text: str) -> bool:
    if not text:
        return False
    return re.search(r"(^|[^a-z0-9])set(?!-of)([^a-z0-9]|$)", text.lower()) is not None


def name_url_match_percent(name_tokens: list[str], url_tokens: list[str]) -> tuple[float, set[str]]:
    if not name_tokens or not url_tokens:
        return 0.0, set()
    matched = 0
    matched_tokens: set[str] = set()
    for token in name_tokens:
        if fuzzy_token_match(token, url_tokens):
            matched += 1
            matched_tokens.add(token)
    return matched / max(1, len(name_tokens)) * 100.0, matched_tokens


def is_set_from_text(tokens: list[str], raw_text: str) -> bool:
    if "set" in tokens or "sets" in tokens:
        return True
    match = re.search(r"\b(\d+)\s*piece", clean_text(raw_text), re.IGNORECASE)
    if match:
        try:
            return int(match.group(1)) > 1
        except ValueError:
            return False
    return False

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


def merge_mpn(value: str) -> str:
    parts = [clean_text(p).lower() for p in clean_text(value).split(";") if clean_text(p)]
    if len(parts) < 2:
        return clean_text(value).lower()
    parts.sort()
    first = parts[0]
    prefix = first.split("-", 1)[0]
    merged = first
    for part in parts[1:]:
        if part.startswith(prefix + "-"):
            part = part[len(prefix) + 1 :]
        merged = f"{merged}-{part}"
    return merged


def is_strong_id_token(token: str) -> bool:
    token = normalize_text(token)
    if len(token) < 5:
        return False
    return any(c.isdigit() for c in token)


def partial_token_match(left: str, right: str) -> bool:
    # Normalize by removing all non-alphanumeric characters on both sides
    left_n = re.sub(r"[^a-z0-9]+", "", clean_text(left).lower())
    right_n = re.sub(r"[^a-z0-9]+", "", clean_text(right).lower())

    if not left_n or not right_n:
        return False

    # Exact match after normalization
    if left_n == right_n:
        return True

    # Bi-directional partial containment (no min length restriction)
    # Example:
    # ABCXYZ vs XYZ
    # PQA vs MNPPQA
    if left_n in right_n or right_n in left_n:
        return True

    return False


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

        self.brand_id_token_map: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))

        self.cm_by_product: dict[str, dict[str, Any]] = {}
        self.cm_competitor_id: str = ""
        self.cm_repricer_id: str = ""

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
                mpn_merged = merge_mpn(row.get("mpn", ""))
                mpn_merged_token = norm_id(mpn_merged)
                if mpn_merged_token and mpn_merged_token not in mpn_tokens:
                    mpn_tokens.append(mpn_merged_token)
                id_union = list(dict.fromkeys(mpn_tokens + sku_tokens + part_tokens))
                search_id_tokens = list(dict.fromkeys((mpn_tokens + sku_tokens) if (mpn_tokens or sku_tokens) else part_tokens))
                gtin_values = numeric_tokens(row.get("gtin"))
                osb_url = clean_text(row.get("osb_url"))
                product_name = clean_text(row.get("product_name"))
                collection = clean_text(row.get("collection"))
                name_tokens = list(tokenize_text(product_name))
                osb_tokens = extract_osb_tokens(osb_url, row.get("brand_label", ""), collection)
                _, osb_id_tokens = extract_url_tokens(osb_url, include_query=False)
                sys_row = {
                    "product_id": product_id,
                    "product_name": product_name,
                    "sku": clean_text(row.get("sku")),
                    "90 days Sales": clean_text(row.get("90 days Sales")),
                    "web_id": clean_text(row.get("web_id")),
                    "gtin": clean_text(row.get("gtin")),
                    "mpn": clean_text(row.get("mpn")),
                    "brand_label": clean_text(row.get("brand_label")),
                    "collection": collection,
                    "cat": clean_text(row.get("cat")),
                    "type": clean_text(row.get("type")),
                    "part_number": clean_text(row.get("part_number")),
                    "osb_url": osb_url,
                    "system_status": clean_text(row.get("status") or row.get("data_status")),
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
                    "_category_tokens": token_set(row.get("cat")),
                    "_name_tokens": name_tokens,
                    "_is_set": is_set_from_text(name_tokens, product_name),
                    "_osb_tokens": osb_tokens,
                    "_osb_id_tokens": osb_id_tokens,
                }
                self.system[product_id] = sys_row
                for token in gtin_values:
                    self.system_gtin_token_counts[token] += 1

                brand_key = sys_row.get("_brand", "")
                if brand_key:
                    for token in id_union:
                        if is_strong_id_token(token):
                            self.brand_id_token_map[brand_key][token].add(product_id)

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
                url_tokens, url_id_tokens = extract_url_tokens(url)
                url_tokens_path, url_id_tokens_path = extract_url_tokens(url, include_query=False)
                try:
                    url_path = (urlparse(url).path or "").lower()
                except ValueError:
                    url_path = ""
                # Prefer Ref MPN, fallback to Item Number (spec extraction), then Ref SKU
                ref_mpn = (
                    clean.get("Ref MPN", "")
                    or clean.get("Item Number", "")
                    or clean.get("Ref SKU", "")
                )
                ref_gtin = clean.get("Ref GTIN", "")
                brand_label = clean.get(self.scrape_brand_col, "")
                ref_category = clean.get("Ref Category", "")
                ref_name = clean.get("Ref Product Name", "")
                mpn_tokens = id_tokens(ref_mpn)
                gtin_values = numeric_tokens(ref_gtin)

                parsed = {
                    "raw": clean,
                    "_url_fp": url_fingerprint(url),
                    "_path_key": path_key(url),
                    "_handle": norm_id(derived_handle),
                    "_url_tokens": url_tokens,
                    "_url_id_tokens": url_id_tokens,
                    "_url_tokens_path": url_tokens_path,
                    "_url_id_tokens_path": url_id_tokens_path,
                    "_url_has_set": url_has_set_token(url_path),
                    "_url_contains_with": any(t in {"with", "w", "bench"} for t in url_tokens),
                    "_mpn": norm_id(ref_mpn),  # legacy single-value key
                    "_gtin": norm_numeric_id(ref_gtin),  # legacy single-value key
                    "_brand": norm_brand(brand_label),
                    "_category_raw": ref_category,
                    "_category_tokens": token_set(ref_category),
                    "_name_tokens": list(tokenize_text(ref_name)),
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
            self.cm_repricer_id = ""
            return

        domain_comp_id_counter: Counter[str] = Counter()
        domain_repricer_counter: Counter[str] = Counter()
        with self.cm_file.open("r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cm_url = clean_text(row.get("competitor_url"))
                cm_domain = extract_domain(cm_url)
                if not cm_domain or not self.scrape_domain or self.scrape_domain not in cm_domain:
                    continue
                comp_id = clean_text(row.get("competitor_id"))
                repricer_id = clean_text(row.get("repricer_id"))
                if comp_id:
                    domain_comp_id_counter[comp_id] += 1
                if repricer_id:
                    domain_repricer_counter[repricer_id] += 1
        self.cm_competitor_id = domain_comp_id_counter.most_common(1)[0][0] if domain_comp_id_counter else ""
        self.cm_repricer_id = domain_repricer_counter.most_common(1)[0][0] if domain_repricer_counter else ""

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
                repricer_id = clean_text(row.get("repricer_id"))

                if self.cm_competitor_id:
                    if row_comp_id != self.cm_competitor_id:
                        continue
                elif self.scrape_domain and row_domain and self.scrape_domain not in row_domain:
                    continue

                cm = {
                    "product_id": product_id,
                    "competitor_id": row_comp_id,
                    "repricer_id": repricer_id,
                    "competitor_url": row_url,
                    "reason": clean_text(row.get("reason")),
                    "other_reason": clean_text(row.get("other_reason")),
                    "approval_status": clean_text(row.get("approval_status")),
                    "reviewed_by_user": clean_text(row.get("reviewed_by_user")),
                    "cm_received_sku": clean_text(row.get("cm_received_sku")),
                    "competitor_name": clean_text(row.get("competitor_name")),
                    "last_update_date": clean_text(row.get("last_update_date")),
                    "sku_mismatch": clean_text(row.get("sku_mismatch") or row.get("SKU Mismatch")),
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
            "URL_ID": 3,
            "GTIN": 2,
            "CONTENT_STRONG": 1,
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
            "url_id_match": False,
            "url_key_match": False,
            "brand_exact": False,
            "brand_clone": False,
            "brand_conflict": False,
            "category_exact": False,
            "category_partial": False,
            "name_url_full": False,
            "name_url_high": False,
            "name_url_partial": False,
            "osb_url_full": False,
            "osb_url_high": False,
            "osb_url_partial": False,
            "set_mismatch": False,
            "bed_part_mismatch": False,
            "other_product_id_conflict": False,
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
            flags["mpn_exact_all"] = False
            flags["mpn_partial_all"] = False
            flags["mpn_any"] = False

            def any_match(left_tokens, right_tokens, partial=False):
                if not left_tokens or not right_tokens:
                    return False

                left_norm = [norm_id(t) for t in left_tokens if norm_id(t)]
                right_norm = [norm_id(t) for t in right_tokens if norm_id(t)]

                if not left_norm or not right_norm:
                    return False

                for l in left_norm:
                    for r in right_norm:
                        if not partial:
                            if l == r:
                                return True
                        else:
                            if l in r or r in l:
                                return True
                return False

            # Priority 1: MPN
            if mpn_tokens:
                if any_match(comp_mpn_tokens, mpn_tokens, partial=False):
                    flags["mpn_exact_all"] = True
                    reasons.append("MPN exact match on MPN")
                elif any_match(comp_mpn_tokens, mpn_tokens, partial=True):
                    flags["mpn_partial_all"] = True
                    reasons.append("MPN partial match on MPN")

            # Priority 2: SKU
            if not flags["mpn_exact_all"] and not flags["mpn_partial_all"] and sku_tokens:
                if any_match(comp_mpn_tokens, sku_tokens, partial=False):
                    flags["mpn_exact_all"] = True
                    reasons.append("MPN exact match on SKU")
                elif any_match(comp_mpn_tokens, sku_tokens, partial=True):
                    flags["mpn_partial_all"] = True
                    reasons.append("MPN partial match on SKU")

            # Priority 3: PART NUMBER
            if not flags["mpn_exact_all"] and not flags["mpn_partial_all"] and part_tokens:
                if any_match(comp_mpn_tokens, part_tokens, partial=False):
                    flags["mpn_exact_all"] = True
                    reasons.append("MPN exact match on PART")
                elif any_match(comp_mpn_tokens, part_tokens, partial=True):
                    flags["mpn_partial_all"] = True
                    reasons.append("MPN partial match on PART")

            flags["mpn_any"] = flags["mpn_exact_all"] or flags["mpn_partial_all"]

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

        url_tokens = row.get("_url_tokens", [])
        url_id_tokens = row.get("_url_id_tokens", [])
        url_tokens_path = row.get("_url_tokens_path", url_tokens)
        url_id_tokens_path = row.get("_url_id_tokens_path", url_id_tokens)

        # URL ID token match (MPN/SKU/PART appearing in URL)
        for token in sys_row.get("_search_id_tokens", []):
            token_norm = normalize_text(token)
            if token_norm and token_norm in url_id_tokens:
                flags["url_id_match"] = True
                reasons.append("ID token found in URL")
                score = max(score, 720)
                if signal == "NONE":
                    signal = "URL_ID"
                break

        # Name vs URL tokens (legacy-style matching)
        name_tokens = sys_row.get("_name_tokens", [])
        name_url_percent, matched_name_tokens = name_url_match_percent(name_tokens, url_tokens)
        if name_url_percent >= 100:
            flags["name_url_full"] = True
            score += 70
            reasons.append("Full name tokens in URL")
        elif name_url_percent >= 90:
            flags["name_url_high"] = True
            score += 60
            reasons.append("High name tokens in URL")
        elif name_url_percent >= 50:
            flags["name_url_partial"] = True
            score += 25
            reasons.append("Partial name tokens in URL")

        # OSB URL tokens vs competitor URL
        osb_tokens = sys_row.get("_osb_tokens", [])
        osb_percent, matched_osb_tokens = name_url_match_percent(osb_tokens, url_tokens)
        if name_url_percent == 0 and osb_tokens:
            osb_percent_path, matched_osb_tokens_path = name_url_match_percent(osb_tokens, url_tokens_path)
            if osb_percent_path > osb_percent:
                osb_percent = osb_percent_path
                matched_osb_tokens = matched_osb_tokens_path
        if osb_percent >= 100:
            flags["osb_url_full"] = True
            score += 70
            reasons.append("Full OSB URL tokens in URL")
        elif osb_percent >= 90:
            flags["osb_url_high"] = True
            score += 60
            reasons.append("High OSB URL tokens in URL")
        elif osb_percent >= 50:
            flags["osb_url_partial"] = True
            score += 25
            reasons.append("Partial OSB URL tokens in URL")

        # Pending URL tokens (for extra confidence boost)
        pending_tokens = [
            t
            for t in url_tokens
            if t not in matched_name_tokens
            and t not in matched_osb_tokens
            and t not in STOP_WORDS
        ]
        if not pending_tokens and (flags["name_url_full"] or flags["osb_url_full"] or flags["url_id_match"]):
            score += 60
            reasons.append("No pending URL tokens")

        if (
            signal == "NONE"
            and (flags["name_url_high"] or flags["name_url_full"])
            and (flags["osb_url_high"] or flags["osb_url_full"])
        ):
            signal = "CONTENT_STRONG"
            score = max(score, 520)
            reasons.append("Name + OSB URL alignment")

        # Set mismatch checks (URL indicates set, system name not set)
        url_is_set = bool(row.get("_url_has_set")) or is_set_from_text(
            url_tokens, raw.get("Ref Product Name", "") + " " + raw.get("Ref Product URL", "")
        )
        sys_is_set = bool(sys_row.get("_is_set"))
        sys_type = clean_text(sys_row.get("type", "")) if "type" in sys_row else ""
        if (
            url_is_set
            and not sys_is_set
            and sys_row.get("cat", "") not in EXCLUDE_CATEGORIES
            and (not sys_type or sys_type == "simple")
        ):
            flags["set_mismatch"] = True
            score -= 200
            reasons.append("Set mismatch (URL suggests set)")

        # Bed part mismatch (headboard/footboard/rails)
        has_with_token = bool(row.get("_url_contains_with")) or any(t in {"with", "w", "bench"} for t in url_tokens)
        pending_bed_parts = any(t in BED_PART_TOKENS for t in pending_tokens)
        part_tokens = [normalize_text(t) for t in sys_row.get("_part_tokens", []) if normalize_text(t)]
        parts_all_in_url = bool(part_tokens) and all(t in url_id_tokens_path for t in part_tokens)
        if (
            pending_bed_parts
            and not has_with_token
            and not parts_all_in_url
            and sys_row.get("cat", "") != "Bed Frames & Headboards"
        ):
            flags["bed_part_mismatch"] = True
            score -= 150
            reasons.append("URL suggests bed parts for non-bed category")

        # Other product ID conflict within same brand
        brand_key = sys_row.get("_brand", "")
        if brand_key:
            token_map = self.brand_id_token_map.get(brand_key, {})
            conflict = False
            candidate_tokens = set(url_id_tokens) | set(row.get("_mpn_tokens", []))
            for token in candidate_tokens:
                token_norm = normalize_text(token)
                if not is_strong_id_token(token_norm):
                    continue
                pids = token_map.get(token_norm, set())
                if pids and (sys_row.get("product_id") not in pids or len(pids) > 1):
                    conflict = True
                    break
            if conflict:
                flags["other_product_id_conflict"] = True
                score -= 100
                reasons.append("URL/MPN aligns to other product ID in same brand")

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

        # Category match logic
        sys_cat_tokens = sys_row.get("_category_tokens", set())
        scrape_cat_tokens = row.get("_category_tokens", set())

        if sys_cat_tokens and scrape_cat_tokens:
            if sys_cat_tokens == scrape_cat_tokens:
                flags["category_exact"] = True
                score += 80
                reasons.append("Category exact")
            else:
                intersection = len(sys_cat_tokens & scrape_cat_tokens)
                ratio = intersection / max(len(sys_cat_tokens), len(scrape_cat_tokens))
                if ratio >= 0.6:
                    flags["category_partial"] = True
                    score += 40
                    reasons.append("Category partial")

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
        elif signal in {"MPN_PARTIAL_GTIN", "MPN_PARTIAL", "GTIN", "URL_ID"}:
            confidence = "MEDIUM"
        elif signal == "CONTENT_STRONG":
            confidence = "LOW"
        else:
            confidence = "LOW"

        # Upgrade partial MPN to HIGH if brand is exact
        if signal == "MPN_PARTIAL" and flags.get("brand_exact"):
            confidence = "HIGH"
        if signal == "URL_ID" and flags.get("brand_exact") and (flags.get("name_url_high") or flags.get("name_url_full")):
            confidence = "HIGH"
        if signal == "CONTENT_STRONG" and flags.get("brand_exact") and flags.get("name_url_full"):
            confidence = "MEDIUM"

        # Category cannot override strong mismatch
        if flags["category_partial"] and flags["brand_conflict"] and signal not in {"MPN_GTIN", "MPN", "GTIN"}:
            confidence = "LOW"
        if flags["brand_conflict"] and signal not in {"MPN_GTIN", "MPN", "GTIN"}:
            confidence = "LOW"

        # Hard mismatch flags should suppress auto confidence unless exact keys
        hard_mismatch = flags["set_mismatch"] or flags["bed_part_mismatch"] or flags["other_product_id_conflict"]
        if hard_mismatch and signal != "MPN_GTIN":
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
                -int(c.flags.get("category_exact", False)),
                -int(c.flags.get("category_partial", False)),
                int(c.flags.get("set_mismatch", False)),
                int(c.flags.get("bed_part_mismatch", False)),
                int(c.flags.get("other_product_id_conflict", False)),
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
                strong_signal = anchor.signal in {
                    "MPN_GTIN",
                    "MPN",
                    "MPN_PARTIAL_GTIN",
                    "MPN_PARTIAL",
                    "GTIN",
                    "URL_ID",
                }
                hard_mismatch = (
                    anchor.flags.get("set_mismatch")
                    or anchor.flags.get("bed_part_mismatch")
                    or anchor.flags.get("other_product_id_conflict")
                )
                if strong_signal and (not hard_mismatch or anchor.signal == "MPN_GTIN"):
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

        strong = top.signal in {"MPN_GTIN", "MPN", "MPN_PARTIAL_GTIN", "MPN_PARTIAL", "GTIN", "URL_ID"}
        hard_mismatch = top.flags.get("set_mismatch") or top.flags.get("bed_part_mismatch") or top.flags.get(
            "other_product_id_conflict"
        )
        if strong and not hard_mismatch and (not top.flags["brand_conflict"] or top.signal == "MPN_GTIN"):
            return "CM_URL_MATCH_CORRECT", top
        if hard_mismatch:
            return "CM_URL_MATCH_WEAK", top
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
        hard_mismatch = bool(
            best is not None
            and (
                best.flags.get("set_mismatch")
                or best.flags.get("bed_part_mismatch")
                or best.flags.get("other_product_id_conflict")
            )
        )
        best_safe = bool(
            best is not None
            and best.signal != "NONE"
            and best_rank >= required_rank
            and not ambiguous
            and (not best_blocked_by_used or exact_reuse_allowed)
            and (not best.flags["brand_conflict"] or brand_override_allowed)
            and (not hard_mismatch or best.signal == "MPN_GTIN")
        )
        name_url_gate = bool(
            best is not None
            and (
                best.name_similarity >= 50
                or best.flags.get("name_url_partial")
                or best.flags.get("name_url_high")
                or best.flags.get("name_url_full")
            )
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
        cm_repricer_id = cm_row.get("repricer_id", "") if cm_row else ""
        cm_sku_mismatch = cm_row.get("sku_mismatch", "") if cm_row else ""
        cm_other_reason = cm_row.get("other_reason", "") if cm_row else ""
        cm_approval_status = cm_row.get("approval_status", "") if cm_row else ""
        cm_reviewed_by_user = cm_row.get("reviewed_by_user", "") if cm_row else ""
        cm_wrong_flag = wrong_reason(cm_reason) or wrong_reason(cm_other_reason)
        if not cm_competitor_id:
            cm_competitor_id = self.cm_competitor_id
        if not cm_repricer_id:
            cm_repricer_id = self.cm_repricer_id

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
                if best_url:
                    self.allocated_ref_urls.add(url_fingerprint(best_url))

            # Approve only when CM was marked wrong but we validated it as correct
            if cm_row and cm_wrong_flag:
                self.approve_rows.append(
                    {
                        "product_id": pid,
                        "competitor_id": cm_competitor_id,
                        "repricer_id": cm_repricer_id,
                        "sku": sys_row.get("sku", ""),
                        "our_mpn": sys_row.get("mpn", ""),
                        "our_status": sys_row.get("system_status", ""),
                        "brand_label": sys_row.get("brand_label", ""),
                        "osb_url": sys_row.get("osb_url", ""),
                        "90 days Sales": sys_row.get("90 days Sales", ""),
                        "existing_competitor_url": cm_url,
                        "existing_reason": cm_reason,
                        "approval_status": cm_approval_status,
                        "reviewed_by_user": cm_reviewed_by_user,
                        "sku_mismatch": cm_sku_mismatch,
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
                if best_url:
                    self.allocated_ref_urls.add(url_fingerprint(best_url))

            elif best_safe and replace_needed:
                decision = "REPLACE_WRONG"
                decision_reason = "Existing URL found but weak identity; stronger replacement found"
                if cm_wrong_flag:
                    decision = "MANUAL_REVIEW"
                    decision_reason = "CM marked wrong match; skip auto update"
                    self.manual_review_rows.append(
                        {
                            "competitor_id": cm_competitor_id,
                            "repricer_id": cm_repricer_id,
                            "product_id": pid,
                            "sku": sys_row.get("sku", ""),
                            "cm_url": cm_url,
                            "cm_reason": cm_reason,
                            "top_candidates": top_candidates,
                        }
                    )
                else:
                    self.new_update_rows.append(
                        {
                            "product_id": pid,
                            "competitor_id": cm_competitor_id,
                            "repricer_id": cm_repricer_id,
                            "sku": sys_row.get("sku", ""),
                            "our_mpn": sys_row.get("mpn", ""),
                            "our_status": sys_row.get("system_status", ""),
                            "brand_label": sys_row.get("brand_label", ""),
                            "osb_url": sys_row.get("osb_url", ""),
                            "90 days Sales": sys_row.get("90 days Sales", ""),
                            "ref_sku": best_ref_sku,
                            "ref_url": best_url,
                            "ref_name": best_name,
                            "send_in_feed": 1,
                            "action": "replace_wrong_match",
                            "confidence": best_conf,
                            "score": best_score,
                            "remark": best_remark,
                            "existing_url": cm_url,
                            "existing_reason": cm_reason,
                            "approval_status": cm_approval_status,
                            "reviewed_by_user": cm_reviewed_by_user,
                            "sku_mismatch": cm_sku_mismatch,
                        }
                    )
                    self.used_scrape_indices.add(best.idx)
                    if best_url:
                        self.allocated_ref_urls.add(url_fingerprint(best_url))
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
                if best_url:
                    self.allocated_ref_urls.add(url_fingerprint(best_url))
            else:
                decision = "NO_MATCH"
                decision_reason = "Weak existing URL match and no safe candidate"

            history_out[pid] = 0

        elif existing_state == "CM_URL_MATCH_WRONG":
            # If no valid signal candidate exists, treat as wrong with no replacement
            if best is None or best.signal == "NONE":
                decision = "WRONG_NO_REPLACEMENT"
                decision_reason = "Existing mapping wrong and no valid candidate signal"
                self.wrong_no_replacement_rows.append(
                    {
                        "competitor_id": cm_competitor_id,
                        "repricer_id": cm_repricer_id,
                        "product_id": pid,
                        "sku": sys_row.get("sku", ""),
                        "brand_label": sys_row.get("brand_label", ""),
                        "system_status": sys_row.get("system_status", ""),
                        "sku_mismatch": cm_sku_mismatch,
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
                    if cm_wrong_flag:
                        decision = "MANUAL_REVIEW"
                        decision_reason = "CM marked wrong match; skip auto update"
                        self.manual_review_rows.append(
                            {
                                "competitor_id": cm_competitor_id,
                                "repricer_id": cm_repricer_id,
                                "product_id": pid,
                                "sku": sys_row.get("sku", ""),
                                "cm_url": cm_url,
                                "cm_reason": cm_reason,
                                "top_candidates": top_candidates,
                            }
                        )
                    else:
                        self.new_update_rows.append(
                            {
                                "product_id": pid,
                                "competitor_id": cm_competitor_id,
                                "repricer_id": cm_repricer_id,
                                "sku": sys_row.get("sku", ""),
                                "our_mpn": sys_row.get("mpn", ""),
                                "our_status": sys_row.get("system_status", ""),
                                "brand_label": sys_row.get("brand_label", ""),
                                "osb_url": sys_row.get("osb_url", ""),
                                "90 days Sales": sys_row.get("90 days Sales", ""),
                                "ref_sku": best_ref_sku,
                                "ref_url": best_url,
                                "ref_name": best_name,
                                "send_in_feed": 1,
                                "action": "replace_wrong_match",
                                "confidence": best_conf,
                                "score": best_score,
                                "remark": best_remark,
                                "existing_url": cm_url,
                                "existing_reason": cm_reason,
                                "approval_status": cm_approval_status,
                                "reviewed_by_user": cm_reviewed_by_user,
                                "sku_mismatch": cm_sku_mismatch,
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
                            "repricer_id": cm_repricer_id,
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
                            "repricer_id": cm_repricer_id,
                            "product_id": pid,
                            "sku": sys_row.get("sku", ""),
                            "brand_label": sys_row.get("brand_label", ""),
                            "system_status": sys_row.get("system_status", ""),
                            "sku_mismatch": cm_sku_mismatch,
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
                            "repricer_id": cm_repricer_id,
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
                            "repricer_id": cm_repricer_id,
                            "product_id": pid,
                            "sku": sys_row.get("sku", ""),
                            "brand_label": sys_row.get("brand_label", ""),
                            "system_status": sys_row.get("system_status", ""),
                            "sku_mismatch": cm_sku_mismatch,
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
                if cm_wrong_flag:
                    decision = "MANUAL_REVIEW"
                    decision_reason = "CM marked wrong match; skip auto update"
                    self.manual_review_rows.append(
                        {
                            "competitor_id": cm_competitor_id,
                            "repricer_id": cm_repricer_id,
                            "product_id": pid,
                            "sku": sys_row.get("sku", ""),
                            "cm_url": cm_url,
                            "cm_reason": cm_reason,
                            "top_candidates": top_candidates,
                        }
                    )
                else:
                    self.new_update_rows.append(
                        {
                            "product_id": pid,
                            "competitor_id": cm_competitor_id,
                            "repricer_id": cm_repricer_id,
                            "sku": sys_row.get("sku", ""),
                            "our_mpn": sys_row.get("mpn", ""),
                            "our_status": sys_row.get("system_status", ""),
                            "brand_label": sys_row.get("brand_label", ""),
                            "osb_url": sys_row.get("osb_url", ""),
                            "90 days Sales": sys_row.get("90 days Sales", ""),
                            "ref_sku": best_ref_sku,
                            "ref_url": best_url,
                            "ref_name": best_name,
                            "send_in_feed": 1,
                            "action": "replace_missing_url",
                            "confidence": best_conf,
                            "score": best_score,
                            "remark": best_remark,
                            "existing_url": cm_url,
                            "existing_reason": cm_reason,
                            "approval_status": cm_approval_status,
                            "reviewed_by_user": cm_reviewed_by_user,
                            "sku_mismatch": cm_sku_mismatch,
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
                        "repricer_id": cm_repricer_id,
                        "product_id": pid,
                        "sku": sys_row.get("sku", ""),
                        "mpn": sys_row.get("mpn", ""),
                        "gtin": sys_row.get("gtin", ""),
                        "brand_label": sys_row.get("brand_label", ""),
                        "system_status": sys_row.get("system_status", ""),
                        "sku_mismatch": cm_sku_mismatch,
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
                        "repricer_id": cm_repricer_id,
                        "product_id": pid,
                        "sku": sys_row.get("sku", ""),
                        "brand_label": sys_row.get("brand_label", ""),
                        "system_status": sys_row.get("system_status", ""),
                        "sku_mismatch": cm_sku_mismatch,
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
                if not name_url_gate:
                    decision = "MANUAL_REVIEW"
                    decision_reason = "Name/URL match below 50% for add_new_match"
                    self.manual_review_rows.append(
                        {
                            "competitor_id": cm_competitor_id,
                            "repricer_id": cm_repricer_id,
                            "product_id": pid,
                            "sku": sys_row.get("sku", ""),
                            "cm_url": "",
                            "cm_reason": "",
                            "top_candidates": top_candidates,
                        }
                    )
                elif cm_wrong_flag:
                    decision = "MANUAL_REVIEW"
                    decision_reason = "CM marked wrong match; skip auto update"
                    self.manual_review_rows.append(
                        {
                            "competitor_id": cm_competitor_id,
                            "repricer_id": cm_repricer_id,
                            "product_id": pid,
                            "sku": sys_row.get("sku", ""),
                            "cm_url": "",
                            "cm_reason": "",
                            "top_candidates": top_candidates,
                        }
                    )
                else:
                    self.new_update_rows.append(
                        {
                            "product_id": pid,
                            "competitor_id": cm_competitor_id,
                            "repricer_id": cm_repricer_id,
                            "sku": sys_row.get("sku", ""),
                            "our_mpn": sys_row.get("mpn", ""),
                            "our_status": sys_row.get("system_status", ""),
                            "brand_label": sys_row.get("brand_label", ""),
                            "osb_url": sys_row.get("osb_url", ""),
                            "90 days Sales": sys_row.get("90 days Sales", ""),
                            "ref_sku": best_ref_sku,
                            "ref_url": best_url,
                            "ref_name": best_name,
                            "send_in_feed": 1,
                            "action": "add_new_match",
                            "confidence": best_conf,
                            "score": best_score,
                            "remark": best_remark,
                            "existing_url": "",
                            "existing_reason": "",
                            "approval_status": cm_approval_status,
                            "reviewed_by_user": cm_reviewed_by_user,
                            "sku_mismatch": cm_sku_mismatch,
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
                        "repricer_id": cm_repricer_id,
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
                        "repricer_id": cm_repricer_id,
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
                        "repricer_id": cm_repricer_id,
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
                "repricer_id": cm_repricer_id,
                "product_id": pid,
                "sku": sys_row.get("sku", ""),
                "our_mpn": sys_row.get("mpn", ""),
                "our_status": sys_row.get("system_status", ""),
                "our_gtin": sys_row.get("gtin", ""),
                "our_brand": sys_row.get("brand_label", ""),
                "our_category": sys_row.get("cat", ""),
                "brand_label": sys_row.get("brand_label", ""),
                "osb_url": sys_row.get("osb_url", ""),
                "90 days Sales": sys_row.get("90 days Sales", ""),
                "system_status": sys_row.get("system_status", ""),
                "existing_competitor_url": cm_url,
                "existing_reason": cm_reason,
                "approval_status": cm_approval_status,
                "reviewed_by_user": cm_reviewed_by_user,
                "sku_mismatch": cm_sku_mismatch,
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

        match_report_headers = [
            "product_id",
            "competitor_id",
            "repricer_id",
            "sku",
            "our_mpn",
            "our_status",
            "our_gtin",
            "our_brand",
            "our_category",
            "brand_label",
            "osb_url",
            "90 days Sales",
            "system_status",
            "existing_competitor_url",
            "existing_reason",
            "approval_status",
            "reviewed_by_user",
            "sku_mismatch",
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
            "repricer_id",
            "sku",
            "our_mpn",
            "our_status",
            "brand_label",
            "osb_url",
            "90 days Sales",
            "ref_sku",
            "ref_url",
            "ref_name",
            "send_in_feed",
            "action",
            "confidence",
            "score",
            "remark",
            "existing_url",
            "existing_reason",
            "approval_status",
            "reviewed_by_user",
            "sku_mismatch",
        ]
        approve_headers = [
            "product_id",
            "competitor_id",
            "repricer_id",
            "sku",
            "our_mpn",
            "our_status",
            "brand_label",
            "osb_url",
            "90 days Sales",
            "existing_competitor_url",
            "existing_reason",
            "approval_status",
            "reviewed_by_user",
            "sku_mismatch",
            "type",
            "source",
            "is_issue",
        ]
        wrong_headers = [
            "product_id",
            "competitor_id",
            "repricer_id",
            "sku",
            "brand_label",
            "system_status",
            "cm_url",
            "cm_reason",
            "sku_mismatch",
            "best_candidate_url",
            "best_candidate_score",
            "best_candidate_confidence",
        ]
        manual_headers = ["product_id", "competitor_id", "repricer_id", "sku", "cm_url", "cm_reason", "top_candidates"]
        retry_headers = [
            "product_id",
            "competitor_id",
            "repricer_id",
            "sku",
            "mpn",
            "gtin",
            "brand_label",
            "system_status",
            "sku_mismatch",
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
