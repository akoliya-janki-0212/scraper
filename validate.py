"""
Competitor URL Validation Engine
Python port of the PHP Validate class
"""

import csv
import os
import re
import json
import time
import copy
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs, urlencode


# Defaults for UI/config integrations
DEFAULT_SCORE_CONFIG = {
    'exact_mpn_match': 270,
    'full_name_match': 70,
    'high_name_match': 60,
    'partial_name_match': 25,
    'full_url_match': 70,
    'high_url_match': 60,
    'partial_url_match': 25,
    'full_config_match': 70,
    'config_match': 60,
    'price_valid': 20,
    'no_pending_parts': 60,
    'add_set_missmatch_score': 140,
    'set_mismatch': -200,
    'same_brand_wrong_match': -100,
    'attribute_mismatch': -70,
    'same_group_wrong_match': -80,
    'min_confidence_score': 60,
    'name_match_threshold_high': 90,
    'name_match_threshold_partial': 50,
    'url_match_threshold_high': 90,
    'url_match_threshold_partial': 50,
    'price_range_percent': 15,
    'wrong_match_threshold': 60,
    'fuzzy_match_threshold': 80,
    'manual_score_buffer': 10,
    'comp_headboard_osb_diff_product': -150,
}

DEFAULT_FILTER_CONFIG = {
    'allowed_reasons': ['Active', 'Not available', 'Out of Stock', 'Ignored'],
    'exclude_competitors': ['Over Stock', 'Overstock.com'],
    'disallowed_visibility': ['Not Visible Individually'],
    'required_type': 'simple',
    'require_competitor_sku': True,
    'include_competitors': [],
    'apply_row_filters': False,
}

DEFAULT_EXCLUDE_CATEGORY = [
    'Dining Sets', 'Home Bar Sets', 'Bedroom Sets', 'Living Room Sets',
    'Coffee Table Sets', 'Home Office Sets', 'Game Table Sets',
    'Bedding and Comforter Sets', 'Outdoor Conversation Sets',
]


class Validate:
    def __init__(
        self,
        mode='cm',
        output_type='combined',
        input_files: dict | None = None,
        output_dir: str | None = None,
        timestamp: str | None = None,
        filter_config: dict | None = None,
        exclude_category: list | None = None,
    ):
        self._is_cm_or_pr = mode
        self._output_type = output_type
        self._files = {}
        self._timestamp = timestamp or datetime.now().strftime("%Y_%m_%d_%H_%M")
        self._competitor_files = {}
        self._correct_file = None
        self._wrong_file = None
        self._manual_file = None

        # Centralized Scoring Configuration
        self._score_config = copy.deepcopy(DEFAULT_SCORE_CONFIG)

        # Data storage
        self._system_data = {}
        self._brand_mpn_list = {}
        self._primary_ids = {}
        self._attr_options = {}
        self._competitor_names = {}
        self._scraped_data = {}

        self._filter_config = copy.deepcopy(DEFAULT_FILTER_CONFIG)
        if filter_config:
            self.update_filter_config(filter_config)

        self._exclude_category = list(exclude_category) if exclude_category is not None else list(DEFAULT_EXCLUDE_CATEGORY)

        # Caches
        self._token_cache = {}
        self._normalization_cache = {}
        self._stop_words = ['by', 'in', 'the', 'and', 'collection', 'is', 'set', 'of',
                            'furniture', 'home', 'with', 'small', 'products', 'product', 'htm', 'html']
        self._stop_words_map = {w: True for w in self._stop_words}

        self._synonyms = {
            'gray': ['grey'],
            'grey': ['gray', 'greystone'],
            'washedgray': ['washedgrey'],
            'greystone': ['grey'],
            'darkbrown': ['slate'],
            'slate': ['darkbrown'],
            'lightbrown': ['sand'],
            'sand': ['lightbrown'],
            'darkgray': ['darkgrey', 'stormgray'],
            'darkgrey': ['darkgray'],
            'stormgray': ['darkgray'],
            'wardrobe': ['storage', 'unit', 'storageunit'],
            'storage': ['wardrobe'],
            'phillipe': ['philippe'],
            'philippe': ['phillipe'],
            'unit': ['wardrobe'],
            'californiaking': ['calking', 'cking'],
            'calking': ['californiaking'],
            'cking': ['californiaking'],
            'philips': ['ps'],
            'caribbean': ['carribean'],
            'carribean': ['caribbean'],
            'blacksilver': ['black', 'silver'],
        }

        self._exclude_synonyms = {
            'king': ['calking', 'californiaking', 'cking']
        }

        self._group_attr_label_map = {
            'color': 'color',
            'bed size': 'bed_size_measure',
            'size': 'size',
            'fireplace option': 'fireplace_option',
            'layout': 'layout_icon',
            'rug size': 'rug_size',
            'mattress size': 'mattress_size',
            'comfort level': 'comfort_level',
            'mattress thickness': 'mattress_thickness',
            'dimensions': 'dimension_text',
            'reclining type': 'power_option',
        }

        self._comp_url_params = {}
        if mode == 'cm':
            self._comp_url_params = {
                'Furniture Cart': ['items'],
                'Furniture Pick': ['items', 'size'],
                'Bed Bath & Beyond': ['option'],
                'English Elm': ['variant'],
                'France & Son': ['variant'],
                'Grayson Living': ['variant'],
                'Over Stock': ['option'],
                'Amazon': [],
            }
        else:
            self._comp_url_params = {
                'Furniture Cart': ['items'],
                'Furniture Pick': ['items', 'size'],
                'Bed Bath & Beyond': ['option'],
                'English Elm': ['variant'],
                'France & Son': ['variant'],
                'Grayson Living': ['variant'],
                'Overstock.com': ['option'],
            }

        # Regex patterns
        self._set_regex = re.compile(r'(^|[^a-z0-9])set(?!-of)([^a-z0-9]|$)', re.IGNORECASE)
        self._alphanumeric_re = re.compile(r'[^a-z0-9]')
        self._word_split_re = re.compile(r'[^a-z0-9]+')
        self._extension_re = re.compile(r'\.(html?|php|aspx?)$')

        self._allowed_headers = [
            'repricer_id', 'other_repricer_id', 'product_id', 'brand_id', 'collection',
            'brand_label', 'web_id', 'visibility', 'category', 'mpn', 'sku', 'part_number',
            'osb_url', 'competitor_id', 'competitor_name', 'competitor_price', 'competitor_url',
            'other_url', 'other_last_update_date', 'reason', 'other_reason', 'last_update_date',
            'cm_pr_mismatch_url', 'type', 'competitor_sku', 'competitor_product_name',
            'competitor_sku1', 'competitor_product_name1', 'scraped_sku', 'scraped_name',
            'config_or_mpn_match', 'productname_word_match_percent', 'osb_url_word_match_percent',
            'pending_url', 'match_reasons', 'wrong_match_reason', 'confidence_score',
            'allowed_filter', 'remark', 'product_status', '90 days Sales', 'approval_status',
            'reviewed_by_user'
        ]

        out_dir = output_dir or f"validationScore/{mode}/{self._timestamp}"
        os.makedirs(out_dir, exist_ok=True)

        base_dir = "inputValidateFiles"
        default_inputs = {
            'comp': f"{base_dir}/competitor-full.csv",
            'sys': f"{base_dir}/system.csv",
            'scraped': f"{base_dir}/scraped.csv",
        }
        if input_files:
            default_inputs.update({k: v for k, v in input_files.items() if v})

        self._files = {
            'comp': default_inputs['comp'],
            'sys': default_inputs['sys'],
            'scraped': default_inputs['scraped'],
            'detail': f"{out_dir}/details.csv",
            'summary': f"{out_dir}/summary.csv",
            'count': f"{out_dir}/count_summary.csv",
            'all_invalid': f"{out_dir}/all_invalid.csv",
        }

        if output_type == 'competitor_wise':
            self._files['competitor_dir'] = f"{out_dir}/competitor_wise/"
        elif output_type == 'valid_invalid':
            self._files['correct'] = f"{out_dir}/correct_matches.csv"
            self._files['wrong'] = f"{out_dir}/wrong_matches.csv"
            self._files['manual'] = f"{out_dir}/manual_check_required.csv"

        self._fuzzy_variant_cache = {}

    # ─────────────────────────────────────────────
    # Score config helpers
    # ─────────────────────────────────────────────

    def get_score_config(self, key=None):
        if key is None:
            return self._score_config
        return self._score_config.get(key)

    def set_score_config(self, key, value):
        self._score_config[key] = value

    def update_score_config(self, config: dict):
        self._score_config.update(config)

    def get_filter_config(self):
        return self._filter_config

    def update_filter_config(self, config: dict):
        if not isinstance(config, dict):
            return
        self._filter_config.update(config)

    # ─────────────────────────────────────────────
    # String helpers
    # ─────────────────────────────────────────────

    def normalize(self, s: str) -> str:
        if not s or not s.strip():
            return ''
        if s in self._normalization_cache:
            return self._normalization_cache[s]
        low = s.lower()
        words = self._word_split_re.split(low)
        words = list(dict.fromkeys(w for w in words if w))
        result = self._alphanumeric_re.sub('', ' '.join(words))
        self._normalization_cache[s] = result
        return result

    def split_values_for_synonyms(self, value: str) -> list:
        if not value or not value.strip():
            return []
        value = value.lower()
        value = re.sub(r'[^a-z0-9]+', ' ', value)
        result = {}
        for w in value.split():
            if w and w not in self._stop_words_map:
                result[w] = True
        return list(result.keys())

    def config_contains_with_synonyms(self, norm_config: str, value: str) -> bool:
        if not value or not norm_config:
            return False
        options = self.split_values_for_synonyms(value)
        value_norm = self.normalize(value)
        if len(options) > 1:
            if value_norm not in self._synonyms:
                self._synonyms[value_norm] = []
            for opt in options:
                opt_norm = self.normalize(opt)
                if opt_norm:
                    self._synonyms[value_norm].append(opt_norm)
            self._synonyms[value_norm] = list(dict.fromkeys(self._synonyms[value_norm]))

        exclusive_map = self._exclude_synonyms
        if value_norm in exclusive_map:
            for blocked in exclusive_map[value_norm]:
                blocked_norm = self.normalize(blocked)
                if blocked_norm and blocked_norm in norm_config:
                    return False

        if value_norm in norm_config:
            return True

        if value_norm not in self._synonyms:
            return False

        for syn in self._synonyms[value_norm]:
            if isinstance(syn, list):
                for val in syn:
                    if val and val in norm_config:
                        return True
            else:
                syn_norm = self.normalize(syn)
                if syn_norm and syn_norm in norm_config:
                    return True
        return False

    def tokenize(self, s: str) -> list:
        if s in self._token_cache:
            return self._token_cache[s]
        clean = s.lower()
        clean = clean.replace('http://', '').replace('https://', '')
        clean = clean.split('?')[0]
        clean = self._extension_re.sub('', clean)
        tokens = self._word_split_re.split(clean)
        filtered = []
        for token in tokens:
            if len(token) <= 1:
                continue
            if (len(token) >= 6 and re.search(r'[0-9]', token) and re.search(r'[a-z]', token)):
                continue
            filtered.append(token)
        result = list(dict.fromkeys(filtered))
        self._token_cache[s] = result
        return result

    def fuzzy_match(self, needle: str, haystack_tokens: list) -> int:
        if not needle:
            return 0
        options = self.split_values_for_synonyms(needle)
        needle_low = needle.lower()
        if len(options) > 1:
            if needle_low not in self._synonyms:
                self._synonyms[needle_low] = []
            for opt in options:
                opt_norm = self.normalize(opt)
                if opt_norm:
                    self._synonyms[needle_low].append(opt_norm)
            self._synonyms[needle_low] = list(dict.fromkeys(self._synonyms[needle_low]))

        fuzzy_threshold = self.get_score_config('fuzzy_match_threshold')
        cache_key = needle_low
        if cache_key not in self._fuzzy_variant_cache:
            variants = [needle_low, needle_low + 's', needle_low + 'es', needle_low.rstrip('s')]
            if needle_low in self._synonyms:
                for syn in self._synonyms[needle_low]:
                    if isinstance(syn, list):
                        for val in syn:
                            syn_norm = self.normalize(val)
                            variants += [syn_norm, syn_norm + 's', syn_norm.rstrip('s')]
                    else:
                        syn_norm = self.normalize(syn)
                        variants += [syn_norm, syn_norm + 's', syn_norm.rstrip('s')]
            self._fuzzy_variant_cache[cache_key] = list(dict.fromkeys(variants))

        needle_variants = self._fuzzy_variant_cache[cache_key]
        needle_len = len(needle_low)

        for token in haystack_tokens:
            if token in needle_variants:
                return 100
            if needle_len > 3 and abs(len(token) - needle_len) <= 2:
                for variant in needle_variants:
                    max_distance = int(len(variant) * 0.2)
                    if self._levenshtein(variant, token) <= max_distance:
                        return fuzzy_threshold
        return 0

    @staticmethod
    def _levenshtein(s1: str, s2: str) -> int:
        if s1 == s2:
            return 0
        if len(s1) < len(s2):
            s1, s2 = s2, s1
        if not s2:
            return len(s1)
        prev = list(range(len(s2) + 1))
        for i, c1 in enumerate(s1):
            curr = [i + 1]
            for j, c2 in enumerate(s2):
                curr.append(min(prev[j + 1] + 1, curr[j] + 1, prev[j] + (c1 != c2)))
            prev = curr
        return prev[-1]

    def merge_mpn(self, value: str) -> str:
        parts = [p for p in value.split(';') if p.strip()]
        if len(parts) < 2:
            return value.lower()
        parts.sort()
        first = parts[0].lower().strip()
        result = first
        prefix = first.split('-')[0]
        for i in range(1, len(parts)):
            mpn = parts[i].lower().strip()
            if mpn.startswith(prefix + '-'):
                mpn = mpn[len(prefix) + 1:]
            result += '-' + mpn
        return result

    def remove_brand_collection(self, url_clean: str, row: dict) -> str:
        brand_lower = (row.get('brand_label') or '').lower()
        url_clean = url_clean.replace(brand_lower, '')
        coll = row.get('collection') or ''
        if coll:
            coll = re.sub(r'collection', '', coll, flags=re.IGNORECASE).strip().lower()
            if coll:
                url_clean = url_clean.replace(coll, '')
        return url_clean

    def get_url_key_with_params(self, url: str, comp: str = None) -> str:
        try:
            parts = urlparse(url)
        except Exception:
            return ''
        if not parts or not parts.path:
            return ''
        key = parts.path.lstrip('/').lower()
        if comp is None:
            if parts.query:
                key += '?' + parts.query.lower()
            return key
        allowed_map = {k.lower(): v for k, v in self._comp_url_params.items()}
        comp_key = comp.strip().lower()
        if comp_key not in allowed_map:
            return key
        allowed_params = allowed_map[comp_key]
        if not allowed_params:
            return key
        if parts.query:
            query_dict = parse_qs(parts.query, keep_blank_values=False)
            filtered = {}
            for param in allowed_params:
                if param in query_dict and query_dict[param]:
                    filtered[param] = query_dict[param][0]
            if filtered:
                key += '?' + urlencode(filtered)
        return key

    def get_filter_condition(self, row: dict) -> bool:
        comp_name = row.get('competitor_name', '')
        comp_sku = row.get('competitor_sku', '')
        reason = row.get('reason', '')
        visibility = row.get('visibility', '')
        rtype = row.get('type', '')
        cfg = self._filter_config
        required_type = cfg.get('required_type')
        if required_type and rtype != required_type:
            return False
        allowed_reasons = cfg.get('allowed_reasons')
        if allowed_reasons is not None and reason not in allowed_reasons:
            return False
        disallowed_visibility = cfg.get('disallowed_visibility', [])
        if visibility in disallowed_visibility:
            return False
        if cfg.get('require_competitor_sku', True):
            if not comp_sku or comp_sku == 'Not available':
                return False
        exclude_comps = cfg.get('exclude_competitors', [])
        if comp_name in exclude_comps:
            return False
        include_comps = cfg.get('include_competitors') or []
        if include_comps and comp_name not in include_comps:
            return False
        return True

    # ─────────────────────────────────────────────
    # Data loading
    # ─────────────────────────────────────────────

    def prepare_system_product_data(self):
        print("Loading System Data...")
        self._system_data = {}
        self._brand_mpn_list = {}
        self._primary_ids = {}
        filepath = self._files.get('sys', '')
        if not filepath or not os.path.exists(filepath):
            print(f"System file not found: {filepath}")
            return
        with open(filepath, newline='', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                pid = row.get('product_id', '')
                bid = row.get('brand_id', '')
                self._system_data[pid] = {
                    'n': (row.get('product_name') or '').lower(),
                    'p': row.get('primary_id') or None,
                    'web_id': row.get('web_id') or None,
                    'gtin': row.get('gtin') or None,
                    'fcv': (row.get('Group Attr 1 Value') or '').strip(),
                    'type': row.get('type') or None,
                    'sku': row.get('sku') or None,
                    'status': row.get('status') or None,
                    'part_number': row.get('part_number') or None,
                    'visibility': row.get('Visibility') or None,
                    'cat': row.get('cat') or None,
                    'brand_id': bid,
                    'collection': row.get('collection') or None,
                    'brand_label': row.get('brand_label') or None,
                    'mpn': row.get('mpn') or None,
                    'osb_url': row.get('osb_url') or None,
                    'product_name': row.get('product_name') or None,
                    'our_price': row.get('our_price') or None,
                    'map_price': row.get('map_price') or None,
                    '90 days Sales': row.get('90 days Sales') or None,
                    'scv': (row.get('Group Attr 2 Value') or '').strip(),
                }
                mpn = row.get('mpn', '')
                if mpn and bid:
                    clean_mpn = self.normalize(mpn)
                    clean_mpn2 = self.normalize(self.merge_mpn(mpn))
                    self._brand_mpn_list.setdefault(bid, {})[clean_mpn] = pid
                    if clean_mpn != clean_mpn2:
                        self._brand_mpn_list[bid][clean_mpn2] = pid
                primary_id = row.get('primary_id', '')
                if primary_id:
                    self._primary_ids.setdefault(primary_id, []).append(pid)

        # Sort brand MPN lists by key length descending
        for bid in self._brand_mpn_list:
            self._brand_mpn_list[bid] = dict(
                sorted(self._brand_mpn_list[bid].items(), key=lambda x: len(x[0]), reverse=True)
            )
        print(f"Loaded {len(self._system_data)} system records.")

    def prepare_scraped_data(self):
        print("Loading Scraped Data...")
        self._scraped_data = {}
        filepath = self._files.get('scraped', '')
        if not filepath or not os.path.exists(filepath):
            print(f"Scraped file not found: {filepath}")
            return
        with open(filepath, newline='', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                url_key = self.get_url_key_with_params(row.get('Ref Product URL', ''))
                if url_key in self._scraped_data:
                    continue
                ref_mpn = row.get('Ref MPN', '')
                ref_sku = row.get('Ref SKU', '')
                self._scraped_data[url_key] = {
                    'sku': (ref_mpn if ref_mpn else ref_sku).lower(),
                    'name': (row.get('Ref Product Name') or '').lower(),
                }
        print(f"Loaded {len(self._scraped_data)} scraped records.")

    # ─────────────────────────────────────────────
    # Output file management
    # ─────────────────────────────────────────────

    def _initialize_output_files(self, output_headers: list):
        if self._output_type == 'competitor_wise':
            comp_dir = self._files.get('competitor_dir', '')
            os.makedirs(comp_dir, exist_ok=True)
        elif self._output_type == 'valid_invalid':
            self._correct_file = open(self._files['correct'], 'w', newline='', encoding='utf-8')
            self._wrong_file = open(self._files['wrong'], 'w', newline='', encoding='utf-8')
            self._manual_file = open(self._files['manual'], 'w', newline='', encoding='utf-8')
            for f in [self._correct_file, self._wrong_file, self._manual_file]:
                csv.writer(f).writerow(output_headers)
        else:
            self._files['detail_file'] = open(self._files['detail'], 'w', newline='', encoding='utf-8')
            csv.writer(self._files['detail_file']).writerow(output_headers)

    def _write_output_row(self, output_headers: list, row: dict, category: str = 'combined'):
        output_data = [row.get(col, '') for col in output_headers]
        if self._output_type == 'competitor_wise':
            comp_id = row.get('competitor_id', '')
            comp_name = row.get('competitor_name', '')
            safe_name = re.sub(r'[^a-z0-9]', '_', comp_name, flags=re.IGNORECASE)
            filename = self._files['competitor_dir'] + f"competitor_{comp_id}_{safe_name}.csv"
            if comp_id not in self._competitor_files:
                self._competitor_files[comp_id] = open(filename, 'w', newline='', encoding='utf-8')
                csv.writer(self._competitor_files[comp_id]).writerow(output_headers)
            csv.writer(self._competitor_files[comp_id]).writerow(output_data)
        elif self._output_type == 'valid_invalid':
            target = self._wrong_file
            if category == 'correct':
                target = self._correct_file
            elif category == 'manual':
                target = self._manual_file
            csv.writer(target).writerow(output_data)
        else:
            csv.writer(self._files['detail_file']).writerow(output_data)

    def _close_output_files(self):
        if self._output_type == 'competitor_wise':
            for f in self._competitor_files.values():
                f.close()
        elif self._output_type == 'valid_invalid':
            for f in [self._correct_file, self._wrong_file, self._manual_file]:
                if f:
                    f.close()
        else:
            df = self._files.get('detail_file')
            if df:
                df.close()

    # ─────────────────────────────────────────────
    # Main processing
    # ─────────────────────────────────────────────

    def prepare_details_csv(self):
        self.prepare_system_product_data()
        self.prepare_scraped_data()
        print("Processing Competitor Data...")

        comp_file = self._files.get('comp', '')
        if not comp_file or not os.path.exists(comp_file):
            raise RuntimeError(f"Competitor file not found: {comp_file}")

        output_headers = self._allowed_headers
        self._initialize_output_files(output_headers)

        row_count = 0
        total_rows = 0
        correct_count = 0
        wrong_count = 0
        manual_count = 0

        min_confidence = self.get_score_config('min_confidence_score')
        product_validity = {}
        invalid_rows_by_product = {}

        with open(comp_file, newline='', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)

            for row in reader:
                row_count += 1
                total_rows += 1
                if row_count % 1000 == 0:
                    print(f"Processed {row_count} rows...", end='\r')

                remarks = []

                include_comps = self._filter_config.get('include_competitors') or []
                if include_comps and row.get('competitor_name') not in include_comps:
                    continue

                if row.get('competitor_id') == '29' and self._is_cm_or_pr == 'cm':
                    continue
                if row.get('product_id') not in self._system_data:
                    continue

                # sys_sales = self._system_data[row['product_id']].get('90 days Sales')
                # try:
                #     if not (float(sys_sales or 0) >= 1):
                #         continue
                # except (ValueError, TypeError):
                #     continue

                # Initialize new columns
                new_cols = [
                    'type', 'sku', 'part_number', 'visibility', 'category',
                    'mpn_exist', 'mpn_exist_wo_specialchar', 'wrong_match_mpn', 'match_with_config',
                    'productname_word_match_percent', 'osb_url_word_match_percent', 'pending_url',
                    'valid', 'match_reasons', 'wrong_match_reason', 'confidence_score',
                    'sku_match_with_url', 'sku_match_with_cm', 'remark',
                    'brand_id', 'collection', 'brand_label', 'mpn', 'osb_url',
                    'product_name', 'our_price', 'map_price', 'web_id', 'first_config',
                    'second_config', 'primary_id', '90 days Sales', 'product_status',
                    'competitor_sku1', 'competitor_product_name1', 'scraped_sku', 'scraped_name',
                    'allowed_filter', 'config_or_mpn_match', 'cm_pr_mismatch_url',
                ]
                for col in new_cols:
                    if col not in row:
                        row[col] = ''

                sys_data = self._system_data.get(row['product_id'], {})

                scrap_url_key = self.get_url_key_with_params(
                    row.get('competitor_url', ''), row.get('competitor_name', '')
                )
                scraped_data = self._scraped_data.get(scrap_url_key, {})

                url_raw = (row.get('competitor_url') or '').lower()
                other_url_raw = (row.get('other_url') or '').lower()
                url_clean = urlparse(url_raw).path.strip('/')
                product_type = sys_data.get('type') or 'simple'

                if product_type != 'simple':
                    continue

                cm_pr_sku_match = self._is_cm_or_pr == 'cm'
                if (not cm_pr_sku_match and row.get('cm_pr_mismatch_url') == '2'):
                    comp_name = row.get('competitor_name', '')
                    if comp_name in self._comp_url_params:
                        required_params = self._comp_url_params[comp_name]
                        cm_params = parse_qs(urlparse(url_raw).query or '')
                        pr_params = parse_qs(urlparse(other_url_raw).query or '')
                        for param in required_params:
                            if (param in pr_params and param in cm_params and
                                    pr_params[param] == cm_params[param]):
                                cm_pr_sku_match = True
                                remarks.append('CM PR params are matched')
                                break
                    else:
                        cm_pr_sku_match = True

                cm_sku = ''
                cm_name_value = ''
                if cm_pr_sku_match:
                    cm_sku = (row.get('competitor_sku') or '').strip().lower()
                    cm_name_value = (row.get('competitor_product_name') or '').strip().lower()

                row['competitor_sku1'] = cm_sku
                row['competitor_product_name1'] = cm_name_value
                row['competitor_sku'] = scraped_data.get('sku') or cm_sku
                row['competitor_product_name'] = scraped_data.get('name') or cm_name_value
                cm_sku = row['competitor_sku']
                cm_name_value = row['competitor_product_name']
                cm_name = ('-' + cm_name_value) if cm_name_value else ''

                row['scraped_sku'] = scraped_data.get('sku', '')
                row['scraped_name'] = scraped_data.get('name', '')
                row['brand_id'] = sys_data.get('brand_id') or ''
                row['collection'] = sys_data.get('collection') or ''
                row['product_status'] = 'Enable' if str(sys_data.get('status', '')) == '1' else 'Disable'
                row['brand_label'] = sys_data.get('brand_label') or ''

                mpn_raw = sys_data.get('mpn') or ''
                if (sys_data.get('brand_label') or '') == 'Monarch Specialties':
                    mpn_raw = re.sub(r'^I\s+|\s+', ' ', mpn_raw.strip()).strip()
                else:
                    mpn_raw = mpn_raw or ''
                row['mpn'] = mpn_raw.lstrip('0')
                row['sku'] = (sys_data.get('sku') or '').lstrip()
                row['part_number'] = (sys_data.get('part_number') or '').lstrip()

                row['osb_url'] = sys_data.get('osb_url') or ''
                first_att_v = sys_data.get('fcv', '')
                second_att_v = sys_data.get('scv', '')
                row['product_name'] = f"{sys_data.get('product_name', '')} {first_att_v} {second_att_v}".strip()
                row['our_price'] = sys_data.get('our_price') or ''
                row['map_price'] = sys_data.get('map_price') or ''
                row['web_id'] = sys_data.get('web_id') or ''
                row['primary_id'] = sys_data.get('p') or ''
                row['90 days Sales'] = sys_data.get('90 days Sales') or ''
                row['type'] = product_type
                row['category'] = sys_data.get('cat') or ''
                row['visibility'] = sys_data.get('visibility') or ''

                cat_type = sys_data.get('cat', '')
                sku_raw = row['sku']
                part_raw = row['part_number']
                mpn_norm = self.normalize(mpn_raw)
                sku_norm = self.normalize(sku_raw)
                part_norm = self.normalize(part_raw)
                cm_sku_norm = self.normalize(cm_sku)

                if row.get('competitor_name') in self._comp_url_params:
                    url_clean = cm_name if cm_name else url_clean
                url_clean += '-' + cm_name
                url_clean = self.remove_brand_collection(url_clean, row)

                is_range_string = '~' in cm_sku
                is_contains_with = any(x in url_clean.lower() for x in ['with', 'w/', 'bench'])
                url_norm = self.normalize(url_clean)
                url_norm += self.normalize(cm_sku)
                url_tokens = self.tokenize(url_clean)

                mpn_extra = ''
                if row.get('brand_id') == '13863':
                    mpn_extra = re.sub(r'(?<=\d)[A-Za-z]$', '', mpn_raw)

                osb_url_raw = (row.get('osb_url') or '').lower()
                osb_url_raw = self.remove_brand_collection(osb_url_raw, row)
                osb_url_parts = [p.strip() for p in osb_url_raw.split('-') if p.strip()]

                score = 0
                reasons = []
                wrong_reasons = []
                replace_words = []
                mpn_match_for_set = False
                url_match_for_set = False
                name_match_for_set = False
                is_set_miss_match = False
                valid_url = True

                # URL not found remark
                if (self._is_cm_or_pr == 'cm' and
                        row.get('competitor_name') in self._comp_url_params):
                    required_params = self._comp_url_params[row['competitor_name']]
                    cm_params_q = parse_qs(urlparse(url_raw).query or '')
                    pr_params_q = parse_qs(urlparse(other_url_raw).query or '')
                    pr_param_present = False
                    for param in required_params:
                        if param in pr_params_q and param not in cm_params_q:
                            pr_param_present = True
                            break
                    if pr_param_present:
                        remarks.append('PR URL has Params But Not in CM')

                if (self._is_cm_or_pr == 'cm' and
                        row.get('reason') == 'URL not found' and
                        row.get('other_last_update_date')):
                    try:
                        other_updated_at = datetime.strptime(
                            row['other_last_update_date'], '%Y-%m-%d %H:%M:%S'
                        )
                        if other_updated_at >= datetime.now() - timedelta(days=1):
                            valid_url = False
                            remarks.append('CM Reason- URL not found and PR Last updated on Last 2 days')
                    except Exception:
                        pass

                matching_values = list(dict.fromkeys(
                    v for v in [mpn_raw, sku_raw, part_raw, mpn_extra] if v
                ))

                # MPN matching
                mpn_matched = self._match_mpn_part_sku(
                    matching_values, url_raw, url_norm, row, score, reasons,
                    replace_words, mpn_match_for_set
                )
                score = mpn_matched['score']
                reasons = mpn_matched['reasons']
                replace_words = mpn_matched['replace_words']
                mpn_match_for_set = mpn_matched['mpn_match_for_set']
                row['sku_match_with_url'] = 1 if mpn_matched['matched'] else ''

                # CM SKU matching
                cm_matched = self._match_mpn_part_sku(
                    matching_values, cm_sku, cm_sku_norm, row, score, reasons,
                    replace_words, mpn_match_for_set
                )
                score = cm_matched['score']
                reasons = cm_matched['reasons']
                replace_words = cm_matched['replace_words']
                mpn_match_for_set = cm_matched['mpn_match_for_set']
                row['sku_match_with_cm'] = 1 if cm_matched['matched'] else ''

                is_set = bool(self._set_regex.search(url_clean))

                # Name matching
                name_result = {'percent': 0}
                if valid_url:
                    name_result = self._match_name(
                        row, url_tokens, is_set, product_type, cat_type,
                        score, reasons, wrong_reasons, replace_words,
                        name_match_for_set, is_set_miss_match
                    )
                    score = name_result['score']
                    reasons = name_result['reasons']
                    wrong_reasons = name_result['wrong_reasons']
                    replace_words = name_result['replace_words']
                    name_match_for_set = name_result['name_match_for_set']
                    is_set_miss_match = name_result['is_set_miss_match']
                name_match_percent = name_result.get('percent', 0)

                # OSB URL matching
                if osb_url_parts and name_match_percent > 14:
                    osb_result = self._match_osb_url(
                        osb_url_parts, url_tokens, score, reasons, replace_words, url_match_for_set, row
                    )
                    score = osb_result['score']
                    reasons = osb_result['reasons']
                    replace_words = osb_result['replace_words']
                    url_match_for_set = osb_result['url_match_for_set']

                # Config matching
                config_result = {'is_match': False, 'family_ids': []}
                if valid_url:
                    config_result = self._match_config(row, url_norm, score, reasons, wrong_reasons)
                    score = config_result['score']
                    reasons = config_result['reasons']
                    wrong_reasons = config_result['wrong_reasons']
                is_config_match = config_result['is_match']
                family_ids = config_result.get('family_ids', [])

                # Wrong match detection
                if (not mpn_match_for_set and not is_config_match and
                        not row.get('match_with_config') and
                        row.get('brand_id') in self._brand_mpn_list and valid_url):
                    wr_result = self._detect_wrong_matches(
                        row, url_norm, url_tokens, is_set, family_ids, score, wrong_reasons, is_range_string
                    )
                    score = wr_result['score']
                    wrong_reasons = wr_result['wrong_reasons']

                # Price validation
                pr_result = self._validate_price(row, score, reasons)
                score = pr_result['score']
                reasons = pr_result['reasons']

                # Pending URL
                pending_part = self._calculate_pending_url(url_tokens, replace_words, row)
                row['pending_url'] = ' | '.join(pending_part)
                pending_url_low = row['pending_url'].lower()

                if (('headboard' in pending_url_low or
                     'footboard' in pending_url_low or
                     'rails' in pending_url_low) and
                        sys_data.get('cat') != 'Bed Frames & Headboards' and
                        row.get('sku_mismatch') != 'No' and
                        not is_contains_with):
                    score += self.get_score_config('comp_headboard_osb_diff_product')
                    wrong_reasons.append('Diff product Not Headboard/Footboard or rails')

                # CM product name matching
                if cm_pr_sku_match:
                    cm_pn_result = self._match_cm_product_name(row, score, reasons, name_match_for_set)
                    score = cm_pn_result['score']
                    reasons = cm_pn_result['reasons']
                    name_match_for_set = cm_pn_result['name_match_for_set']

                # Set mismatch scoring
                score = self._apply_set_mismatch_scoring(
                    reasons, mpn_match_for_set, url_match_for_set,
                    name_match_for_set, is_set_miss_match, score
                )

                # No pending bonus
                if not pending_part and not row.get('config_or_mpn_match'):
                    score += self.get_score_config('no_pending_parts')

                row['confidence_score'] = score
                row['match_reasons'] = '|'.join(list(dict.fromkeys(reasons)))
                row['wrong_match_reason'] = '|'.join(list(dict.fromkeys(wrong_reasons)))
                row['cm_pr_mismatch_url'] = 'MissMatch' if row.get('cm_pr_mismatch_url') == '1' else 'Same'
                row['allowed_filter'] = self.get_filter_condition(row)
                if self._filter_config.get('apply_row_filters') and not row['allowed_filter']:
                    continue
                if row['product_id'] not in product_validity:
                    product_validity[row['product_id']] = 0
                    invalid_rows_by_product[row['product_id']] = {}
                match_valid = self._is_match_valid(row, score, min_confidence, valid_url, remarks)
                row['remark'] = '|'.join(list(dict.fromkeys(remarks)))
                row['valid'] = 1 if match_valid else 0

                if row['valid'] == 1:
                    product_validity[row['product_id']] = 1
                else:
                    invalid_rows_by_product[row['product_id']] = sys_data

                category = self._determine_validation_category(match_valid, score, min_confidence, valid_url, row)
                if category == 'correct':
                    correct_count += 1
                elif category == 'wrong':
                    wrong_count += 1
                else:
                    manual_count += 1

                self._write_output_row(output_headers, row, category)

        self._close_output_files()

        # Write all invalid
        invalid_header = ['product_name', 'web_id', 'mpn', 'sku', 'gtin', 'brand_label', 'cat']
        with open(self._files['all_invalid'], 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(invalid_header)
            for pid, has_valid in product_validity.items():
                if has_valid == 0:
                    r = invalid_rows_by_product.get(pid, {})
                    writer.writerow([r.get(col, '') for col in invalid_header])

        print(f"\nSuccess. Files generated.")
        print("--------------------------------------")
        print(f"Total Rows Processed : {total_rows}")
        print(f"Correct Matches      : {correct_count}")
        print(f"Wrong Matches        : {wrong_count}")
        print(f"Manual Check         : {manual_count}")
        print("--------------------------------------")

        if self._output_type == 'combined':
            self.generate_summaries()

    # ─────────────────────────────────────────────
    # Private helpers (match methods)
    # ─────────────────────────────────────────────

    def _is_match_valid(self, row, score, min_confidence, valid_url, remarks):
        return (score >= min_confidence) and valid_url

    def _determine_validation_category(self, is_valid, score, min_confidence, valid_url, row):
        if not row.get('competitor_sku') and not row.get('competitor_product_name'):
            return 'manual'
        if (not row.get('scraped_sku') and
                row.get('competitor_name') in ['Bed Bath & Beyond', 'Over Stock']):
            return 'manual'
        match_reasons = row.get('match_reasons', '')
        wrong_reason = row.get('wrong_match_reason', '')
        if wrong_reason and 'Exact SKU MATCH' not in match_reasons and 'Full Name' not in match_reasons:
            return 'wrong'
        if wrong_reason and 'Exact SKU MATCH' in match_reasons:
            return 'manual'
        if is_valid:
            return 'correct'
        buffer = self.get_score_config('manual_score_buffer') or 0
        if score >= (min_confidence - buffer):
            return 'manual'
        if not valid_url and score >= (min_confidence * 0.75):
            return 'manual'
        if row.get('pending_url'):
            return 'manual'
        return 'manual'

    def _match_mpn_part_sku(self, mpn_sku_part_list, url_raw, url_norm, row,
                             score, reasons, replace_words, mpn_match_for_set):
        result = {
            'matched': False, 'score': score, 'reasons': reasons,
            'replace_words': replace_words, 'mpn_match_for_set': mpn_match_for_set
        }
        if mpn_match_for_set or not mpn_sku_part_list:
            return result
        for value in mpn_sku_part_list:
            r = self._match_single_value(value, url_raw, url_norm, row, score, reasons, replace_words, mpn_match_for_set)
            score = r['score']
            reasons = r['reasons']
            replace_words = r['replace_words']
            mpn_match_for_set = r['mpn_match_for_set']
            if r['matched']:
                result.update({'matched': True, 'score': score, 'reasons': reasons,
                                'replace_words': replace_words, 'mpn_match_for_set': mpn_match_for_set})
                return result
        result.update({'score': score, 'reasons': reasons,
                       'replace_words': replace_words, 'mpn_match_for_set': mpn_match_for_set})
        return result

    def _match_single_value(self, value, url_raw, url_norm, row, score, reasons, replace_words, mpn_match_for_set):
        result = {
            'matched': False, 'score': score, 'reasons': reasons,
            'replace_words': replace_words, 'mpn_match_for_set': mpn_match_for_set
        }
        value = value.strip().lower()
        if not value:
            return result
        merged = self.merge_mpn(value)
        variants = list(dict.fromkeys([
            value, merged, self.normalize(value), self.normalize(merged)
        ]))
        for v in variants:
            if not v:
                continue
            if v in url_raw or v in url_norm:
                score += self.get_score_config('exact_mpn_match')
                row['mpn_exist'] = 1
                reasons.append('Exact SKU MATCH')
                replace_words.append(v)
                mpn_match_for_set = True
                result.update({'matched': True, 'score': score, 'reasons': reasons,
                                'replace_words': replace_words, 'mpn_match_for_set': mpn_match_for_set})
                return result
        # Multi-part match
        parts = [p.strip() for p in value.split(';') if p.strip()]
        if len(parts) <= 1:
            return result
        all_match = True
        for p in parts:
            p = p.lower().lstrip('0')
            if not p:
                all_match = False
                break
            p_norm = self.normalize(p)
            if p not in url_raw and (not p_norm or p_norm not in url_norm):
                all_match = False
                break
        if all_match:
            score += self.get_score_config('exact_mpn_match')
            row['mpn_exist'] = 1
            reasons.append('Exact SKU MATCH')
            replace_words.append(value)
            mpn_match_for_set = True
            result.update({'matched': True, 'score': score, 'reasons': reasons,
                            'replace_words': replace_words, 'mpn_match_for_set': mpn_match_for_set})
        return result

    def _match_name(self, row, url_tokens, is_set, product_type, cat_type,
                    score, reasons, wrong_reasons, replace_words, name_match_for_set, is_set_miss_match):
        prod_name_raw = (row.get('product_name') or '').lower()
        name_clean = self.remove_brand_collection(prod_name_raw, row)
        name_tokens = self.tokenize(name_clean)
        name_tokens_filtered = [t for t in name_tokens if t not in self._stop_words_map]

        piece_count_m = re.search(r'\b(\d+)\s*Piece\b', name_clean, re.IGNORECASE)
        piece_count = int(piece_count_m.group(1)) if piece_count_m else 0
        is_name_set = ('set' in name_tokens or 'sets' in name_tokens or
                       (('piece' in name_tokens or 'pieces' in name_tokens) and piece_count > 1))

        if (row.get('sku_mismatch') != 'No' and is_set and not is_name_set and
                product_type == 'simple' and cat_type not in self._exclude_category):
            score += self.get_score_config('set_mismatch')
            wrong_reasons.append('Match with Set Product')
            is_set_miss_match = True

        total_words = len(name_tokens_filtered)
        name_match_percent = 0
        if total_words > 0:
            matched_count = 0
            fuzzy_threshold = self.get_score_config('fuzzy_match_threshold')
            for word in name_tokens_filtered:
                if self.fuzzy_match(word, url_tokens) >= fuzzy_threshold:
                    matched_count += 1
                    replace_words.append(word)
            name_match_percent = (matched_count / total_words) * 100
            row['productname_word_match_percent'] = round(name_match_percent, 2)
            if name_match_percent >= 100:
                score += self.get_score_config('full_name_match')
                reasons.append('Full Name')
                name_match_for_set = True
            elif name_match_percent >= self.get_score_config('name_match_threshold_high'):
                score += self.get_score_config('high_name_match')
                reasons.append('High Name')
                name_match_for_set = True
            elif name_match_percent >= self.get_score_config('name_match_threshold_partial'):
                score += self.get_score_config('partial_name_match')
                reasons.append('Partial Name')

        return {
            'percent': name_match_percent, 'score': score, 'reasons': reasons,
            'wrong_reasons': wrong_reasons, 'replace_words': replace_words,
            'name_match_for_set': name_match_for_set, 'is_set_miss_match': is_set_miss_match
        }

    def _match_osb_url(self, osb_url_parts, url_tokens, score, reasons, replace_words, url_match_for_set, row):
        matched_count = 0
        total = len(osb_url_parts)
        fuzzy_threshold = self.get_score_config('fuzzy_match_threshold')
        for word in osb_url_parts:
            if self.fuzzy_match(word, url_tokens) >= fuzzy_threshold:
                matched_count += 1
                replace_words.append(word)
        if total > 0:
            percent = (matched_count / total) * 100
            row['osb_url_word_match_percent'] = round(percent, 2)
            if percent >= 100:
                score += self.get_score_config('full_url_match')
                reasons.append('Full URL')
                url_match_for_set = True
            elif percent >= self.get_score_config('url_match_threshold_high'):
                score += self.get_score_config('high_url_match')
                reasons.append('High URL')
                url_match_for_set = True
            elif percent >= self.get_score_config('url_match_threshold_partial'):
                score += self.get_score_config('partial_url_match')
                reasons.append('Partial URL')
        return {'score': score, 'reasons': reasons, 'replace_words': replace_words, 'url_match_for_set': url_match_for_set}

    def _match_all_tokens(self, url_norm: str, value: str, min_match: int = 2) -> bool:
        if not value:
            return False
        value = value.lower()
        tokens = re.split(
            r'(?:\s*\b(?:and|or)\b\s*|\s*[,\/]\s*|\s+(?=\S)|(?<![A-Za-z0-9])-(?![A-Za-z0-9]))',
            value
        )
        tokens = [t.strip() for t in tokens if t.strip()]
        if not tokens:
            return False
        match_count = 0
        threshold = min(len(tokens), min_match)
        for token in tokens:
            if self.config_contains_with_synonyms(url_norm, token):
                match_count += 1
                if match_count >= threshold:
                    return True
        return False

    def _match_config(self, row, url_norm, score, reasons, wrong_reasons):
        product_id = row.get('product_id', '')
        sys = self._system_data.get(product_id)
        is_config_match = False
        family_ids = []
        last_fam = None

        if sys and sys.get('p') and sys['p'] in self._primary_ids:
            cur_val1 = sys.get('fcv', '')
            cur_val2 = sys.get('scv', '')
            has_config1 = bool(cur_val1 and self._match_all_tokens(url_norm, cur_val1))
            has_config2 = bool(cur_val2 and self._match_all_tokens(url_norm, cur_val2))

            if (has_config1 and has_config2) or (has_config1 and not cur_val2):
                is_config_match = True
                score += self.get_score_config('config_match')
                reasons.append('Full Config Match' if (has_config1 and has_config2) else 'Config Match')
            else:
                family = []
                for fam_pid in self._primary_ids[sys['p']]:
                    cfg = self._system_data.get(fam_pid, {})
                    if 'fcv' not in cfg:
                        continue
                    val1 = cfg.get('fcv', '')
                    val2 = cfg.get('scv', '')
                    priority = max(len(val1), len(val2))
                    family_ids.append(fam_pid)
                    family.append({'pid': fam_pid, 'val1': val1, 'val2': val2, 'priority': priority})

                family.sort(key=lambda x: x['priority'], reverse=True)

                for fam in family:
                    last_fam = fam
                    has_fam1 = bool(fam['val1'] and self._match_all_tokens(url_norm, fam['val1']))
                    has_fam2 = bool(fam['val2'] and self._match_all_tokens(url_norm, fam['val2']))
                    if (has_fam1 and has_fam2) or (has_fam1 and not fam['val2']):
                        row['match_with_config'] = self._system_data.get(fam['pid'], {}).get('web_id') or fam['pid']
                        row['config_or_mpn_match'] = row['match_with_config']
                        if fam['pid'] == product_id:
                            is_config_match = True
                            score += self.get_score_config('config_match')
                            reasons.append('Full Config Match' if (has_fam1 and has_fam2) else 'Config Match')
                        break

                if row.get('match_with_config') and not is_config_match and row.get('sku_mismatch') != 'No':
                    if 'Exact SKU MATCH' in reasons and last_fam and self._system_data.get(last_fam['pid'], {}).get('web_id'):
                        fam_sys = self._system_data.get(last_fam['pid'], {})
                        sku_n = self.normalize(fam_sys.get('sku', ''))
                        mpn_n = self.normalize(fam_sys.get('mpn', ''))
                        part_n = self.normalize(fam_sys.get('part_number', ''))
                        match_part = any(p and p in url_norm for p in dict.fromkeys([mpn_n, sku_n, part_n]) if p)
                        if match_part:
                            score += self.get_score_config('same_group_wrong_match')
                            wrong_reasons.append('Match with Same Group Product3')
                    else:
                        score += self.get_score_config('same_group_wrong_match')
                        wrong_reasons.append('Match with Same Group Product1')

        elif sys and not sys.get('p'):
            cur_val1 = sys.get('bed_size_measure', '') or sys.get('fcv', '')
            cur_val2 = sys.get('color', '') or sys.get('scv', '')
            has_config1 = bool(cur_val1 and self._match_all_tokens(url_norm, cur_val1))
            has_config2 = bool(cur_val2 and self._match_all_tokens(url_norm, cur_val2))
            if has_config1 or has_config2:
                is_config_match = True
                if self._system_data.get(product_id, {}).get('web_id') != row.get('web_id'):
                    row['match_with_config'] = self._system_data.get(product_id, {}).get('web_id') or product_id
                    row['config_or_mpn_match'] = row['match_with_config']
                score += self.get_score_config('config_match')
                reasons.append('Full Config Match W/G' if (has_config1 and has_config2) else 'Config Match W/G')

        return {
            'is_match': is_config_match, 'family_ids': family_ids,
            'score': score, 'reasons': reasons, 'wrong_reasons': wrong_reasons
        }

    def _detect_wrong_matches(self, row, url_norm, url_tokens, is_set, family_ids, score, wrong_reasons, is_range_string):
        brand_id = row.get('brand_id', '')
        brand_label_lower = (row.get('brand_label') or '').lower()
        for other_mpn_norm, other_pid in self._brand_mpn_list.get(brand_id, {}).items():
            if other_mpn_norm not in url_norm or is_range_string:
                continue
            sys_d = self._system_data.get(other_pid)
            if not sys_d:
                continue
            name_clean = sys_d.get('n', '').replace(brand_label_lower, '')
            first_att_v = sys_d.get('fcv', '')
            second_att_v = sys_d.get('scv', '')
            name_clean = f"{name_clean} {first_att_v} {second_att_v}".strip()
            name_tokens = self.tokenize(name_clean)
            if is_set and 'set' not in name_tokens and sys_d.get('type') == 'simple' and sys_d.get('cat') not in self._exclude_category:
                continue
            if other_pid in family_ids and row.get('sku_mismatch') != 'No':
                if other_pid == row.get('product_id'):
                    break
                score += self.get_score_config('same_group_wrong_match')
                wrong_reasons.append('Match with Same Group Product2')
                row['wrong_match_mpn'] = self._system_data.get(other_pid, {}).get('web_id') or other_pid
                row['config_or_mpn_match'] = row['wrong_match_mpn']
                break
            other_tokens = [t for t in name_tokens if t not in self._stop_words_map]
            total_words = len(other_tokens)
            if total_words > 0:
                matched_count = 0
                fuzzy_threshold = self.get_score_config('fuzzy_match_threshold')
                for word in other_tokens:
                    if self.fuzzy_match(word, url_tokens) >= fuzzy_threshold:
                        matched_count += 1
                if row.get('sku_mismatch') != 'No' and (matched_count / total_words) > (self.get_score_config('wrong_match_threshold') / 100):
                    if other_pid == row.get('product_id'):
                        break
                    if other_pid in family_ids:
                        score += self.get_score_config('same_group_wrong_match')
                        wrong_reasons.append('Match with Same Group Product3')
                    else:
                        score += self.get_score_config('same_brand_wrong_match')
                        wrong_reasons.append('Match with Same Brand Product')
                    row['wrong_match_mpn'] = self._system_data.get(other_pid, {}).get('web_id') or other_pid
                    row['config_or_mpn_match'] = row['wrong_match_mpn']
                    break
        return {'score': score, 'wrong_reasons': wrong_reasons}

    def _validate_price(self, row, score, reasons):
        try:
            our_price = float(row.get('our_price') or 0)
            comp_price = float(row.get('competitor_price') or 0)
        except (ValueError, TypeError):
            return {'score': score, 'reasons': reasons}
        if our_price > 0 and comp_price > 0:
            range_pct = self.get_score_config('price_range_percent') / 100
            min_p = our_price * (1 - range_pct)
            max_p = our_price * (1 + range_pct)
            if min_p <= comp_price <= max_p:
                score += self.get_score_config('price_valid')
                reasons.append('Price Valid')
        return {'score': score, 'reasons': reasons}

    def _calculate_pending_url(self, url_tokens, replace_words, row):
        if not replace_words:
            pending_part = list(url_tokens)
        else:
            pending_part = [t for t in url_tokens if t not in replace_words]
        pending_part = [w for w in pending_part if w not in self._stop_words_map]
        brand_tokens = self._word_split_re.split((row.get('brand_label') or '').lower())
        pending_part = [w for w in pending_part if w not in brand_tokens]
        coll = row.get('collection') or ''
        if coll:
            coll = re.sub(r'collection', '', coll, flags=re.IGNORECASE).strip().lower()
            coll_tokens = self._word_split_re.split(coll)
            pending_part = [w for w in pending_part if w not in coll_tokens]
        return pending_part

    def _match_cm_product_name(self, row, score, reasons, name_match_for_set):
        cm_product_raw = (row.get('competitor_product_name') or '').strip()
        if not cm_product_raw or not (self._is_cm_or_pr == 'cm' or row.get('cm_pr_mismatch_url') == '2'):
            return {'score': score, 'reasons': reasons, 'name_match_for_set': name_match_for_set}
        prod_name_raw = (row.get('product_name') or '').lower()
        name_clean = self.remove_brand_collection(prod_name_raw, row)
        name_tokens = self.tokenize(name_clean)
        name_tokens_filtered = [t for t in name_tokens if t not in self._stop_words_map]
        total_words = len(name_tokens_filtered)
        if total_words > 0:
            cm_product_name = cm_product_raw.lower().replace(' ', '-')
            cm_product_tokens = self.tokenize(cm_product_name)
            matched_count = 0
            fuzzy_threshold = self.get_score_config('fuzzy_match_threshold')
            for word in name_tokens_filtered:
                if self.fuzzy_match(word, cm_product_tokens) >= fuzzy_threshold:
                    matched_count += 1
            pct = (matched_count / total_words) * 100
            if pct >= 100:
                score += self.get_score_config('full_name_match')
                reasons.append('Full CM Product Name')
                name_match_for_set = True
            elif pct >= self.get_score_config('name_match_threshold_high'):
                score += self.get_score_config('high_name_match')
                reasons.append('High CM Product Name')
                name_match_for_set = True
            elif pct >= self.get_score_config('name_match_threshold_partial'):
                score += self.get_score_config('partial_name_match')
                reasons.append('Partial CM Product Name')
        return {'score': score, 'reasons': reasons, 'name_match_for_set': name_match_for_set}

    def _apply_set_mismatch_scoring(self, reasons, mpn_match_for_set, url_match_for_set, name_match_for_set, is_set_miss_match, score):
        if ((len(reasons) >= 3 and mpn_match_for_set and url_match_for_set and name_match_for_set and is_set_miss_match) or
                len(reasons) >= 7):
            score += self.get_score_config('add_set_missmatch_score')
        return score

    # ─────────────────────────────────────────────
    # Summaries
    # ─────────────────────────────────────────────

    def generate_summaries(self):
        print("Generating Summaries...")
        if self._output_type != 'combined':
            return
        products = {}
        comp_stats = {}
        with open(self._files['detail'], newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                pid = row.get('product_id', '')
                if pid not in products:
                    products[pid] = {
                        'data': [pid, row.get('web_id'), row.get('product_name'), row.get('mpn'),
                                 row.get('brand_label'), row.get('our_price')],
                        'total': 0, 'valid': 0, 'active_valid': 0
                    }
                products[pid]['total'] += 1
                if str(row.get('valid')) == '1':
                    products[pid]['valid'] += 1
                    if row.get('reason') != 'Ignored':
                        products[pid]['active_valid'] += 1
                comp_name = row.get('competitor_name', '')
                if comp_name not in comp_stats:
                    comp_stats[comp_name] = {'name': comp_name, 'total': 0, 'valid': 0, 'invalid': 0}
                comp_stats[comp_name]['total'] += 1
                if str(row.get('valid')) == '1':
                    comp_stats[comp_name]['valid'] += 1
                else:
                    comp_stats[comp_name]['invalid'] += 1

        with open(self._files['summary'], 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['product_id', 'web_id', 'product_name', 'mpn', 'brand_label', 'our_price', 'total_comp', 'valid_matches', 'active_valid_matches'])
            for p in products.values():
                writer.writerow(p['data'] + [p['total'], p['valid'], p['active_valid']])

        sorted_stats = sorted(comp_stats.values(), key=lambda x: x['valid'], reverse=True)
        with open(self._files['count'], 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Competitor Name', 'Total', 'Valid', 'Invalid'])
            for s in sorted_stats:
                writer.writerow([s['name'], s['total'], s['valid'], s['invalid']])


if __name__ == '__main__':
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else 'cm'
    output_type = sys.argv[2] if len(sys.argv) > 2 else 'valid_invalid'
    obj = Validate(mode, output_type)
    obj.prepare_details_csv()
