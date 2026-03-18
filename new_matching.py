#!/usr/bin/env python3
"""
Unified Competitor Match Reconciliation Pipeline with Comprehensive Data
Handles 5 cases with full data integration:
1. Wrong Match - Mark existing match as wrong
2. New Match - Add new competitor match  
3. Approve Match - Validate previously wrong matches
4. Keep Existing - Retain valid existing matches
5. Manual Review - Flag ambiguous cases for human review

Includes sales data, MFR counts, and all available system/competitor attributes
"""

import argparse
import csv
import json
import re
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from urllib.parse import parse_qs, urlparse


# ============================================================================
# Data Cleaning & Normalization Functions
# ============================================================================

def clean_text(value: Any) -> str:
    """Clean and normalize text values"""
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in ["nan", "null", "none", ""]:
        return ""
    return text


def clean_float(value: Any) -> float:
    """Convert to float safely"""
    if value is None:
        return 0.0
    try:
        return float(str(value).replace(',', '').strip())
    except (ValueError, TypeError):
        return 0.0


def clean_int(value: Any) -> int:
    """Convert to int safely"""
    if value is None:
        return 0
    try:
        return int(float(str(value).replace(',', '').strip()))
    except (ValueError, TypeError):
        return 0


def norm_id(value: Any) -> str:
    """Normalize ID (MPN, SKU, etc.) - remove special chars, lowercase"""
    return re.sub(r"[^a-z0-9]+", "", clean_text(value).lower())


def norm_numeric_id(value: Any) -> str:
    """Normalize numeric ID, strip leading zeros"""
    token = norm_id(value)
    if token.isdigit():
        return token.lstrip("0") or "0"
    return token


def norm_brand(value: Any) -> str:
    """Normalize brand name for matching"""
    token = clean_text(value).lower()
    token = re.sub(r"[^a-z0-9]+", "-", token)
    return token.strip("-")


def tokenize(value: Any) -> List[str]:
    """Split text into tokens (words)"""
    text = clean_text(value).lower()
    # Remove URLs prefixes
    text = re.sub(r"https?://(www\.)?", "", text)
    # Split on non-alphanumeric
    tokens = re.findall(r"[a-z0-9]+", text)
    # Filter out single chars and barcode-like tokens
    filtered = []
    for t in tokens:
        if len(t) <= 1:
            continue
        # Skip if looks like barcode (mixed letters/numbers, length >= 8)
        if len(t) >= 8 and re.search(r"\d", t) and re.search(r"[a-z]", t):
            # But keep if it's likely an MPN (shorter)
            if len(t) > 12:
                continue
        filtered.append(t)
    return filtered


def token_set(value: Any) -> Set[str]:
    """Get unique token set"""
    return set(tokenize(value))


def split_multi_values(value: Any, separators: str = "[,_;|]+") -> List[str]:
    """Split multi-value fields (like multiple MPNs)"""
    raw = clean_text(value)
    if not raw:
        return []
    parts = [clean_text(p) for p in re.split(separators, raw)]
    return [p for p in parts if p]


def id_tokens(value: Any) -> List[str]:
    """Extract unique ID tokens from multi-value field"""
    tokens = []
    seen = set()
    for part in split_multi_values(value):
        token = norm_id(part)
        if token and token not in seen:
            seen.add(token)
            tokens.append(token)
    return tokens


def numeric_tokens(value: Any) -> List[str]:
    """Extract numeric tokens (GTIN)"""
    tokens = []
    seen = set()
    for part in split_multi_values(value):
        token = norm_numeric_id(part)
        if token and token not in seen:
            seen.add(token)
            tokens.append(token)
    return tokens


# ============================================================================
# URL Processing Functions
# ============================================================================

def extract_domain(url: str) -> str:
    """Extract domain from URL"""
    raw = clean_text(url)
    if not raw:
        return ""
    try:
        host = urlparse(raw).hostname or ""
    except ValueError:
        return ""
    host = host.lower()
    return re.sub(r"^www\.", "", host)


def path_key(url: str) -> str:
    """Extract path key from URL (path without trailing slash)"""
    raw = clean_text(url)
    if not raw:
        return ""
    try:
        path = urlparse(raw).path or ""
    except ValueError:
        return ""
    path = path.rstrip("/")
    if not path or path == "/":
        return ""
    return path


def url_slug(url: str) -> str:
    """Extract last segment of URL path"""
    path = path_key(url)
    if not path:
        return ""
    return path.split("/")[-1]


def url_fingerprint(url: str) -> str:
    """Create URL fingerprint (domain + first 2 path segments)"""
    raw = clean_text(url)
    if not raw:
        return ""
    try:
        parsed = urlparse(raw)
        domain = re.sub(r"^www\.", "", parsed.hostname or "")
        path = parsed.path.strip("/")
        segments = path.split("/")[:2]  # First 2 path segments
        return f"{domain}/{'/'.join(segments)}"
    except:
        return ""


def url_matches_with_params(cm_url: str, scrape_url: str, required_params: List[str] = None) -> bool:
    """Check if URLs match with parameter validation"""
    if not cm_url or not scrape_url:
        return False
    
    try:
        cm_parsed = urlparse(cm_url)
        scrape_parsed = urlparse(scrape_url)
    except:
        return False
    
    # Check if slugs match
    cm_slug = norm_id(url_slug(cm_url))
    scrape_slug = norm_id(url_slug(scrape_url))
    if cm_slug and scrape_slug and cm_slug != scrape_slug:
        return False
    
    # Parameter validation
    if required_params:
        cm_params = parse_qs(cm_parsed.query)
        scrape_params = parse_qs(scrape_parsed.query)
        
        for param in required_params:
            if param not in scrape_params:
                continue
            if param not in cm_params:
                return False
            # Check if values overlap
            cm_values = {clean_text(v).lower() for v in cm_params.get(param, [])}
            scrape_values = {clean_text(v).lower() for v in scrape_params.get(param, [])}
            if cm_values and scrape_values and cm_values.isdisjoint(scrape_values):
                return False
    
    return True


# ============================================================================
# Matching & Scoring Functions (PHP logic port)
# ============================================================================

# Stop words (from PHP)
STOP_WORDS = {
    'by', 'in', 'the', 'and', 'collection', 'is', 'set', 'of', 'furniture',
    'home', 'with', 'small', 'products', 'product', 'htm', 'html', 'inc',
    'llc', 'ltd', 'co', 'corp', 'company', 'official', 'store', 'shop'
}

# Synonyms (from PHP)
SYNONYMS = {
    'gray': ['grey'],
    'grey': ['gray'],
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

# Exclude synonyms (from PHP)
EXCLUDE_SYNONYMS = {
    'king': ['calking', 'californiaking', 'cking']
}

# Set categories (from PHP)
SET_CATEGORIES = {
    'Dining Sets', 'Home Bar Sets', 'Bedroom Sets', 'Living Room Sets',
    'Coffee Table Sets', 'Home Office Sets', 'Game Table Sets',
    'Bedding and Comforter Sets', 'Outdoor Conversation Sets'
}

# Competitor URL parameters (from PHP)
COMP_URL_PARAMS = {
    'cm': {
        'Furniture Cart': ['items'],
        'Furniture Pick': ['items', 'size'],
        'Bed Bath & Beyond': ['option'],
        'English Elm': ['variant'],
        'France & Son': ['variant'],
        'Grayson Living': ['variant'],
        'Over Stock': ['option'],
    },
    'pr': {
        'Furniture Cart': ['items'],
        'Furniture Pick': ['items', 'size'],
        'English Elm': ['variant'],
        'France & Son': ['variant'],
        'Grayson Living': ['variant'],
        'Overstock.com': ['option'],
    }
}


@dataclass
class ScoreConfig:
    """Scoring configuration (ported from PHP)"""
    # MPN Matching
    exact_mpn_match: int = 270
    
    # Name Matching
    full_name_match: int = 70
    high_name_match: int = 60
    partial_name_match: int = 25
    
    # URL Matching
    full_url_match: int = 70
    high_url_match: int = 60
    partial_url_match: int = 25
    
    # Configuration Matching
    full_config_match: int = 70
    config_match: int = 60
    
    # Price Validation
    price_valid: int = 20
    
    # Positive Adjustments
    no_pending_parts: int = 60
    add_set_mismatch_score: int = 140
    
    # Penalties
    set_mismatch: int = -200
    same_brand_wrong_match: int = -100
    attribute_mismatch: int = -70
    same_group_wrong_match: int = -80
    
    # Thresholds
    min_confidence_score: int = 60
    name_match_threshold_high: int = 90
    name_match_threshold_partial: int = 50
    url_match_threshold_high: int = 90
    url_match_threshold_partial: int = 50
    price_range_percent: int = 15
    wrong_match_threshold: int = 70
    fuzzy_match_threshold: int = 80


class PHPValidator:
    """Port of PHP validation logic to Python"""
    
    def __init__(self, mode: str = 'cm'):
        self.mode = mode  # 'cm' or 'pr'
        self.score_config = ScoreConfig()
        self.stop_words = STOP_WORDS
        self.synonyms = SYNONYMS.copy()
        self.exclude_synonyms = EXCLUDE_SYNONYMS
        self.set_categories = SET_CATEGORIES
        self.comp_url_params = COMP_URL_PARAMS.get(mode, {})
        
        # Cache for normalized strings
        self.normalization_cache = {}
        self.token_cache = {}
        self.variant_cache = {}
    
    def normalize(self, text: str) -> str:
        """Normalize string (port of PHP normalize)"""
        if not text or not text.strip():
            return ""
        
        cache_key = text
        if cache_key in self.normalization_cache:
            return self.normalization_cache[cache_key]
        
        # Clear cache if too large
        if len(self.normalization_cache) > 50000:
            self.normalization_cache = {}
        
        text = text.lower()
        words = re.findall(r"[a-z0-9]+", text)
        if not words:
            self.normalization_cache[cache_key] = ""
            return ""
        
        # Remove duplicates while preserving order
        seen = set()
        unique_words = []
        for w in words:
            if w not in seen:
                seen.add(w)
                unique_words.append(w)
        
        result = " ".join(unique_words)
        self.normalization_cache[cache_key] = result
        return result
    
    def tokenize(self, text: str) -> List[str]:
        """Tokenize string (port of PHP tokenize)"""
        if not text:
            return []
        
        cache_key = text
        if cache_key in self.token_cache:
            return self.token_cache[cache_key]
        
        clean = text.lower()
        clean = re.sub(r"https?://(www\.)?", "", clean)
        clean = clean.split("?")[0] if "?" in clean else clean
        clean = re.sub(r"\.(html?|php|aspx?)$", "", clean)
        
        tokens = re.findall(r"[a-z0-9]+", clean)
        
        # Filter tokens
        filtered = []
        for t in tokens:
            if len(t) <= 1:
                continue
            # Skip if looks like barcode (mixed letters/numbers, length >= 6)
            if len(t) >= 6 and re.search(r"\d", t) and re.search(r"[a-z]", t):
                # But keep if it's likely an MPN (shorter)
                if len(t) > 8:
                    continue
            filtered.append(t)
        
        # Remove duplicates
        result = list(dict.fromkeys(filtered))
        self.token_cache[cache_key] = result
        return result
    
    def fuzzy_match(self, needle: str, haystack_tokens: List[str]) -> int:
        """Fuzzy match with synonyms (port of PHP fuzzyMatch)"""
        if not needle or not haystack_tokens:
            return 0
        
        # Split value for synonyms
        options = self.split_values_for_synonyms(needle)
        needle_lower = needle.lower()
        
        # Update synonyms if needed
        if len(options) > 1:
            if needle_lower not in self.synonyms:
                self.synonyms[needle_lower] = []
            for opt in options:
                opt_norm = self.normalize(opt)
                if opt_norm:
                    self.synonyms[needle_lower].append(opt_norm)
            self.synonyms[needle_lower] = list(dict.fromkeys(self.synonyms[needle_lower]))
        
        # Get variants
        cache_key = needle_lower
        if cache_key not in self.variant_cache:
            variants = [
                needle_lower,
                needle_lower + 's',
                needle_lower + 'es',
                re.sub(r's$', '', needle_lower)
            ]
            
            if needle_lower in self.synonyms:
                for syn in self.synonyms[needle_lower]:
                    if isinstance(syn, list):
                        for s in syn:
                            s_norm = self.normalize(s)
                            variants.extend([s_norm, s_norm + 's', re.sub(r's$', '', s_norm)])
                    else:
                        s_norm = self.normalize(syn)
                        variants.extend([s_norm, s_norm + 's', re.sub(r's$', '', s_norm)])
            
            self.variant_cache[cache_key] = list(dict.fromkeys(variants))
        
        needle_variants = self.variant_cache[cache_key]
        needle_len = len(needle_lower)
        threshold = self.score_config.fuzzy_match_threshold
        
        for token in haystack_tokens:
            # Exact/plural/synonym match
            if token in needle_variants:
                return 100
            
            # Fuzzy match for longer words
            if needle_len > 3 and abs(len(token) - needle_len) <= 2:
                for variant in needle_variants:
                    max_distance = int(len(variant) * 0.2)
                    if self.levenshtein(variant, token) <= max_distance:
                        return threshold
        
        return 0
    
    def levenshtein(self, s1: str, s2: str) -> int:
        """Levenshtein distance"""
        if len(s1) < len(s2):
            return self.levenshtein(s2, s1)
        if len(s2) == 0:
            return len(s1)
        
        previous_row = range(len(s2) + 1)
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row
        
        return previous_row[-1]
    
    def split_values_for_synonyms(self, value: str) -> List[str]:
        """Split value for synonym processing"""
        if not value or not value.strip():
            return []
        
        value = value.lower()
        value = re.sub(r"[^a-z0-9]+", " ", value)
        result = []
        for w in value.split():
            if w and w not in self.stop_words:
                result.append(w)
        return result
    
    def config_contains_with_synonyms(self, norm_config: str, value: str) -> bool:
        """Check if config contains value with synonyms"""
        if not value or not norm_config:
            return False
        
        options = self.split_values_for_synonyms(value)
        value_norm = self.normalize(value)
        
        # Update synonyms if needed
        if len(options) > 1:
            if value_norm not in self.synonyms:
                self.synonyms[value_norm] = []
            for opt in options:
                opt_norm = self.normalize(opt)
                if opt_norm:
                    self.synonyms[value_norm].append(opt_norm)
            self.synonyms[value_norm] = list(dict.fromkeys(self.synonyms[value_norm]))
        
        # Check exclusions
        if value_norm in EXCLUDE_SYNONYMS:
            for blocked in EXCLUDE_SYNONYMS[value_norm]:
                blocked_norm = self.normalize(blocked)
                if blocked_norm and blocked_norm in norm_config:
                    return False
        
        # Direct match
        if value_norm in norm_config:
            return True
        
        # Synonym match
        if value_norm not in self.synonyms:
            return False
        
        for syn in self.synonyms[value_norm]:
            if isinstance(syn, list):
                for s in syn:
                    s_norm = self.normalize(s)
                    if s_norm and s_norm in norm_config:
                        return True
            else:
                s_norm = self.normalize(syn)
                if s_norm and s_norm in norm_config:
                    return True
        
        return False
    
    def merge_mpn(self, value: str) -> str:
        """Merge multiple MPNs (port of PHP mergeMpn)"""
        parts = [p for p in value.split(';') if p.strip()]
        
        if len(parts) < 2:
            return value.lower()
        
        parts.sort()
        first = parts[0].lower().strip()
        result = first
        
        # Extract prefix from first MPN
        prefix = first.split('-')[0] if '-' in first else first
        
        for i in range(1, len(parts)):
            mpn = parts[i].lower().strip()
            
            # Remove prefix if present
            if mpn.startswith(prefix + '-'):
                mpn = mpn[len(prefix) + 1:]
            
            result += '-' + mpn
        
        return result
    
    def remove_brand_collection(self, text: str, brand: str, collection: str = "") -> str:
        """Remove brand and collection from text"""
        if brand:
            text = text.replace(brand.lower(), '')
        if collection:
            coll = collection.lower().replace('collection', '').strip()
            if coll:
                text = text.replace(coll, '')
        return text
    
    def is_set_product(self, url: str, name_tokens: List[str], category: str) -> bool:
        """Check if product is a set"""
        # Check URL for 'set'
        has_set_in_url = bool(re.search(r'(^|[^a-z0-9])set(?!-of)([^a-z0-9]|$)', url.lower()))
        
        # Check if category is in set categories
        is_set_category = category in self.set_categories
        
        # Check if 'set' in name tokens
        has_set_in_name = 'set' in name_tokens
        
        return has_set_in_url or is_set_category or has_set_in_name
    
    def calculate_score(self, 
                        system_data: Dict[str, Any],
                        competitor_data: Dict[str, Any],
                        url_tokens: List[str],
                        url_norm: str,
                        matched_tokens: List[str] = None) -> Tuple[int, List[str], List[str]]:
        """
        Calculate confidence score (port of PHP scoring logic)
        Returns: (score, reasons, wrong_reasons)
        """
        score = 0
        reasons = []
        wrong_reasons = []
        
        if matched_tokens is None:
            matched_tokens = []
        
        # Extract data
        product_name = system_data.get('product_name', '')
        brand = system_data.get('brand_label', '')
        category = system_data.get('cat', '')
        mpn = system_data.get('mpn', '')
        sku = system_data.get('sku', '')
        part_number = system_data.get('part_number', '')
        our_price = clean_float(system_data.get('our_price', 0))
        comp_price = clean_float(competitor_data.get('competitor_price', 0))
        
        # MPN/SKU/PART matching
        mpn_values = [v for v in [mpn, sku, part_number] if v]
        mpn_matched = False
        
        for value in mpn_values:
            value_norm = self.normalize(value)
            if value_norm and (value_norm in url_norm or value_norm in competitor_data.get('competitor_url', '')):
                score += self.score_config.exact_mpn_match
                reasons.append(f"Exact MPN/SKU match: {value}")
                matched_tokens.extend(tokenize(value))
                mpn_matched = True
                break
        
        # Name matching
        name_tokens = tokenize(self.remove_brand_collection(product_name, brand))
        name_tokens = [t for t in name_tokens if t not in self.stop_words]
        
        if name_tokens:
            matched_count = 0
            for token in name_tokens:
                if self.fuzzy_match(token, url_tokens) >= self.score_config.fuzzy_match_threshold:
                    matched_count += 1
                    matched_tokens.append(token)
            
            name_match_percent = (matched_count / len(name_tokens)) * 100 if name_tokens else 0
            
            if name_match_percent >= 100:
                score += self.score_config.full_name_match
                reasons.append("Full Name Match")
            elif name_match_percent >= self.score_config.name_match_threshold_high:
                score += self.score_config.high_name_match
                reasons.append("High Name Match")
            elif name_match_percent >= self.score_config.name_match_threshold_partial:
                score += self.score_config.partial_name_match
                reasons.append("Partial Name Match")
        
        # Set product detection and penalty
        is_set = self.is_set_product(competitor_data.get('competitor_url', ''), name_tokens, category)
        if is_set and 'set' not in name_tokens and system_data.get('type') == 'simple' and category not in self.set_categories:
            score += self.score_config.set_mismatch
            wrong_reasons.append("Match with Set Product")
        
        # Price validation
        if our_price > 0 and comp_price > 0:
            price_range = self.score_config.price_range_percent / 100
            min_price = our_price * (1 - price_range)
            max_price = our_price * (1 + price_range)
            
            if min_price <= comp_price <= max_price:
                score += self.score_config.price_valid
                reasons.append("Price Valid")
        
        return score, reasons, wrong_reasons


# ============================================================================
# Data Classes for Rich Information
# ============================================================================

@dataclass
class SystemProduct:
    """Complete system product data"""
    product_id: str = ""
    product_name: str = ""
    sku: str = ""
    web_id: str = ""
    gtin: str = ""
    mpn: str = ""
    brand_id: str = ""
    brand_label: str = ""
    collection: str = ""
    cat: str = ""
    type: str = "simple"
    status: str = ""
    visibility: str = ""
    part_number: str = ""
    osb_url: str = ""
    our_price: float = 0.0
    map_price: float = 0.0
    primary_id: str = ""
    first_config: str = ""
    second_config: str = ""
    
    # Sales data
    sales_90_days: int = 0
    mfr_sales_30_days: int = 0
    
    # Attribute values
    color: str = ""
    bed_size_measure: str = ""
    size: str = ""
    fireplace_option: str = ""
    layout_icon: str = ""
    rug_size: str = ""
    mattress_size: str = ""
    power_option: str = ""
    dimension_text: str = ""
    comfort_level: str = ""
    mattress_thickness: str = ""
    
    # Normalized fields (populated later)
    _mpn_tokens: List[str] = field(default_factory=list)
    _sku_tokens: List[str] = field(default_factory=list)
    _part_tokens: List[str] = field(default_factory=list)
    _gtin_tokens: List[str] = field(default_factory=list)
    _id_tokens: List[str] = field(default_factory=list)
    _brand_norm: str = ""
    _url_slug: str = ""
    _config_values: Dict[str, str] = field(default_factory=dict)
    
    def normalize(self):
        """Populate normalized fields"""
        self._mpn_tokens = id_tokens(self.mpn)
        self._sku_tokens = id_tokens(self.sku)
        self._part_tokens = id_tokens(self.part_number)
        self._gtin_tokens = numeric_tokens(self.gtin)
        self._id_tokens = list(dict.fromkeys(
            self._mpn_tokens + self._sku_tokens + self._part_tokens
        ))
        self._brand_norm = norm_brand(self.brand_label)
        self._url_slug = norm_id(url_slug(self.osb_url))
        
        # Collect config values
        self._config_values = {
            'color': norm_id(self.color),
            'bed_size_measure': norm_id(self.bed_size_measure),
            'size': norm_id(self.size),
            'fireplace_option': norm_id(self.fireplace_option),
            'layout_icon': norm_id(self.layout_icon),
            'rug_size': norm_id(self.rug_size),
            'mattress_size': norm_id(self.mattress_size),
            'power_option': norm_id(self.power_option),
            'dimension_text': norm_id(self.dimension_text),
            'comfort_level': norm_id(self.comfort_level),
            'mattress_thickness': norm_id(self.mattress_thickness),
        }


@dataclass
class ScrapeProduct:
    """Complete scraped competitor product data"""
    raw: Dict[str, Any] = field(default_factory=dict)
    
    # Extracted fields
    url: str = ""
    product_id: str = ""
    variant_id: str = ""
    category: str = ""
    category_url: str = ""
    brand: str = ""
    product_name: str = ""
    sku: str = ""
    mpn: str = ""
    gtin: str = ""
    price: float = 0.0
    main_image: str = ""
    quantity: int = 0
    group_attr_1: str = ""
    group_attr_2: str = ""
    status: str = ""
    date_scrapped: str = ""
    
    # Normalized fields
    _url_fp: str = ""
    _path_key: str = ""
    _handle: str = ""
    _mpn: str = ""
    _mpn_tokens: List[str] = field(default_factory=list)
    _gtin: str = ""
    _gtin_tokens: List[str] = field(default_factory=list)
    _brand: str = ""
    _domain: str = ""
    _price_float: float = 0.0
    
    def extract(self, row: Dict[str, str]):
        """Extract data from raw CSV row"""
        self.raw = row
        
        self.url = clean_text(row.get("Ref Product URL", ""))
        self.product_id = clean_text(row.get("Ref Product ID", ""))
        self.variant_id = clean_text(row.get("Ref Varient ID", ""))
        self.category = clean_text(row.get("Ref Category", ""))
        self.category_url = clean_text(row.get("Ref Category URL", ""))
        self.brand = clean_text(row.get("Ref Brand Name", ""))
        self.product_name = clean_text(row.get("Ref Product Name", ""))
        self.sku = clean_text(row.get("Ref SKU", ""))
        self.mpn = clean_text(row.get("Ref MPN", "")) or self.sku
        self.gtin = clean_text(row.get("Ref GTIN", ""))
        self.price = clean_float(row.get("Ref Price", 0))
        self.main_image = clean_text(row.get("Ref Main Image", ""))
        self.quantity = clean_int(row.get("Ref Quantity", 0))
        self.group_attr_1 = clean_text(row.get("Ref Group Attr 1", ""))
        self.group_attr_2 = clean_text(row.get("Ref Group Attr 2", ""))
        self.status = clean_text(row.get("Ref Status", ""))
        self.date_scrapped = clean_text(row.get("Date Scrapped", ""))
        
        # Normalize
        self._url_fp = url_fingerprint(self.url)
        self._path_key = path_key(self.url)
        self._handle = norm_id(url_slug(self.url))
        self._mpn = norm_id(self.mpn)
        self._mpn_tokens = id_tokens(self.mpn)
        self._gtin = norm_numeric_id(self.gtin)
        self._gtin_tokens = numeric_tokens(self.gtin)
        self._brand = norm_brand(self.brand)
        self._domain = extract_domain(self.url)
        self._price_float = self.price


@dataclass
class CompetitorMatch:
    """Existing competitor match from CM/PR"""
    product_id: str = ""
    competitor_id: str = ""
    repricer_id: str = ""
    competitor_url: str = ""
    competitor_name: str = ""
    reason: str = ""
    other_reason: str = ""
    competitor_price: float = 0.0
    last_update_date: str = ""
    sku_mismatch: str = ""
    cm_received_sku: str = ""
    cm_received_name: str = ""
    source: str = ""  # 'cm' or 'pr'
    
    # Normalized fields
    _url_fp: str = ""
    _path_key: str = ""
    _url_slug: str = ""
    _price_float: float = 0.0
    
    def normalize(self):
        """Populate normalized fields"""
        self._url_fp = url_fingerprint(self.competitor_url)
        self._path_key = path_key(self.competitor_url)
        self._url_slug = norm_id(url_slug(self.competitor_url))
        self._price_float = clean_float(self.competitor_price)


@dataclass
class CandidateResult:
    """Match candidate result with rich data"""
    idx: int
    signal: str  # MPN_GTIN, MPN, GTIN, URL_HANDLE, etc.
    score: int
    confidence: str  # HIGH, MEDIUM, LOW
    remark: str
    name_similarity: float
    reasons: List[str]
    flags: Dict[str, bool] = field(default_factory=dict)
    matched_tokens: List[str] = field(default_factory=dict)
    
    # Rich data
    scrape_product: Optional[ScrapeProduct] = None
    system_product: Optional[SystemProduct] = None
    
    # Match details
    price_diff_percent: float = 0.0
    price_diff_abs: float = 0.0
    brand_match_type: str = ""
    category_match_percent: float = 0.0
    attribute_matches: Dict[str, bool] = field(default_factory=dict)
    attribute_values: Dict[str, Tuple[str, str]] = field(default_factory=dict)


# ============================================================================
# Main Reconciliation Pipeline
# ============================================================================

class UnifiedReconciliationPipeline:
    """
    Unified pipeline for competitor match reconciliation
    Handles 5 cases with comprehensive data integration:
    - Wrong Match
    - New Match
    - Approve Match
    - Keep Existing
    - Manual Review
    """
    
    def __init__(
        self,
        scrape_file: Path,
        system_file: Path,
        cm_file: Path,
        output_dir: Path,
        mode: str = 'cm',  # 'cm' or 'pr'
        limit: Optional[int] = None,
        min_confidence: str = "AUTO"
    ):
        self.scrape_file = scrape_file
        self.system_file = system_file
        self.cm_file = cm_file
        self.output_dir = output_dir
        self.mode = mode
        self.limit = limit
        self.min_confidence = min_confidence.upper()
        
        # Initialize PHP validator
        self.validator = PHPValidator(mode)
        
        # Data storage
        self.system_products: Dict[str, SystemProduct] = {}
        self.system_by_mpn: Dict[str, List[str]] = defaultdict(list)
        self.system_by_sku: Dict[str, List[str]] = defaultdict(list)
        self.system_by_gtin: Dict[str, List[str]] = defaultdict(list)
        self.system_by_url_slug: Dict[str, List[str]] = defaultdict(list)
        self.system_primary_groups: Dict[str, List[str]] = defaultdict(list)
        
        self.scrape_products: List[ScrapeProduct] = []
        self.scrape_headers: List[str] = []
        self.scrape_indexes: Dict[str, Dict[str, List[int]]] = {
            "url_fp": defaultdict(list),
            "path_key": defaultdict(list),
            "handle": defaultdict(list),
            "mpn": defaultdict(list),
            "gtin": defaultdict(list),
            "brand_mpn": defaultdict(list),
        }
        
        self.competitor_matches: Dict[str, CompetitorMatch] = {}
        self.cm_competitor_id: str = ""
        self.cm_repricer_id: str = ""
        
        # State tracking
        self.used_scrape_indices: Set[int] = set()
        self.allocated_ref_urls: Set[str] = set()
        self.decision_by_product: Dict[str, str] = {}
        
        # Output collections for 5 cases
        self.report_rows: List[Dict[str, Any]] = []
        self.wrong_match_rows: List[Dict[str, Any]] = []      # Case 1
        self.new_match_rows: List[Dict[str, Any]] = []        # Case 2
        self.approve_match_rows: List[Dict[str, Any]] = []    # Case 3
        self.keep_existing_rows: List[Dict[str, Any]] = []    # Case 4
        self.manual_review_rows: List[Dict[str, Any]] = []    # Case 5
        
        # Unmatched data
        self.unmatched_scrape_products: List[ScrapeProduct] = []
        
        # Summary
        self.summary: Dict[str, Any] = {}
    
    def run(self) -> Dict[str, Any]:
        """Execute the reconciliation pipeline"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.output_dir = self.output_dir / f"{self.mode}_{timestamp}"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"[{self.mode.upper()}] Starting reconciliation pipeline...")
        print(f"[{self.mode.upper()}] Loading system data: {self.system_file}")
        self.load_system()
        print(f"[{self.mode.upper()}] System products loaded: {len(self.system_products)}")
        
        print(f"[{self.mode.upper()}] Loading scrape data: {self.scrape_file}")
        self.load_scrape()
        print(f"[{self.mode.upper()}] Scrape products loaded: {len(self.scrape_products)}")
        
        print(f"[{self.mode.upper()}] Loading competitor data: {self.cm_file}")
        self.load_competitor_matches()
        print(f"[{self.mode.upper()}] Competitor matches loaded: {len(self.competitor_matches)}")
        
        print(f"[{self.mode.upper()}] Evaluating products...")
        self.evaluate_products()
        
        print(f"[{self.mode.upper()}] Building unmatched products...")
        self.build_unmatched()
        
        print(f"[{self.mode.upper()}] Generating reports...")
        self.generate_reports()
        
        print(f"[{self.mode.upper()}] Writing output files...")
        self.write_outputs()
        
        print(f"[{self.mode.upper()}] Reconciliation completed.")
        return self.summary
    
    def load_system(self) -> None:
        """Load system product data with all attributes"""
        required = {"product_id", "sku", "mpn", "brand_label"}
        
        with self.system_file.open("r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                raise ValueError("System file has no headers")
            
            # Check required columns
            field_set = set(reader.fieldnames)
            missing = required - field_set
            if missing:
                print(f"Warning: Missing columns: {missing}")
            
            for row in reader:
                pid = clean_text(row.get("product_id", ""))
                if not pid or pid in self.system_products:
                    continue
                
                # Create system product with all available fields
                product = SystemProduct(
                    product_id=pid,
                    product_name=clean_text(row.get("product_name", "")),
                    sku=clean_text(row.get("sku", "")),
                    web_id=clean_text(row.get("web_id", "")),
                    gtin=clean_text(row.get("gtin", "")),
                    mpn=clean_text(row.get("mpn", "")),
                    brand_id=clean_text(row.get("brand_id", "")),
                    brand_label=clean_text(row.get("brand_label", "")),
                    collection=clean_text(row.get("collection", "")),
                    cat=clean_text(row.get("cat", "")),
                    type=clean_text(row.get("type", "simple")),
                    status=clean_text(row.get("status", "")),
                    visibility=clean_text(row.get("visibility", "")),
                    part_number=clean_text(row.get("part_number", "")),
                    osb_url=clean_text(row.get("osb_url", "")),
                    our_price=clean_float(row.get("our_price", 0)),
                    map_price=clean_float(row.get("map_price", 0)),
                    primary_id=clean_text(row.get("primary_id", "")),
                    first_config=clean_text(row.get("first_config", "")),
                    second_config=clean_text(row.get("second_config", "")),
                    
                    # Sales data
                    sales_90_days=clean_int(row.get("90 days Sales", 0)),
                    mfr_sales_30_days=clean_int(row.get("30 days MFR Sales", 0)),
                    
                    # Attribute values
                    color=clean_text(row.get("color", "")),
                    bed_size_measure=clean_text(row.get("bed_size_measure", "")),
                    size=clean_text(row.get("size", "")),
                    fireplace_option=clean_text(row.get("fireplace_option", "")),
                    layout_icon=clean_text(row.get("layout_icon", "")),
                    rug_size=clean_text(row.get("rug_size", "")),
                    mattress_size=clean_text(row.get("mattress_size", "")),
                    power_option=clean_text(row.get("power_option", "")),
                    dimension_text=clean_text(row.get("dimension_text", "")),
                    comfort_level=clean_text(row.get("comfort_level", "")),
                    mattress_thickness=clean_text(row.get("mattress_thickness", "")),
                )
                
                product.normalize()
                self.system_products[pid] = product
                
                # Index for fast lookup
                for token in product._mpn_tokens:
                    self.system_by_mpn[token].append(pid)
                for token in product._sku_tokens:
                    self.system_by_sku[token].append(pid)
                for token in product._gtin_tokens:
                    self.system_by_gtin[token].append(pid)
                
                if product._url_slug:
                    self.system_by_url_slug[product._url_slug].append(pid)
                
                if product.primary_id:
                    self.system_primary_groups[product.primary_id].append(pid)
    
    def load_scrape(self) -> None:
        """Load scraped competitor data"""
        required = {"Ref Product URL", "Ref MPN", "Ref Product Name"}
        
        with self.scrape_file.open("r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                raise ValueError("Scrape file has no headers")
            
            self.scrape_headers = list(reader.fieldnames)
            
            for idx, row in enumerate(reader):
                if self.limit and idx >= self.limit:
                    break
                
                # Create scrape product
                product = ScrapeProduct()
                product.extract(row)
                
                self.scrape_products.append(product)
                
                # Index for fast lookup
                if product._url_fp:
                    self.scrape_indexes["url_fp"][product._url_fp].append(idx)
                if product._path_key:
                    self.scrape_indexes["path_key"][product._path_key].append(idx)
                if product._handle:
                    self.scrape_indexes["handle"][product._handle].append(idx)
                
                for token in product._mpn_tokens:
                    self.scrape_indexes["mpn"][token].append(idx)
                    if product._brand:
                        self.scrape_indexes["brand_mpn"][f"{product._brand}|{token}"].append(idx)
                
                for token in product._gtin_tokens:
                    self.scrape_indexes["gtin"][token].append(idx)
    
    def load_competitor_matches(self) -> None:
        """Load existing competitor matches from CM/PR"""
        if not self.cm_file.exists():
            return
        
        # First pass: identify competitor ID for this domain
        domain_counts = Counter()
        repricer_counts = Counter()
        
        with self.cm_file.open("r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                comp_url = clean_text(row.get("competitor_url", ""))
                comp_domain = extract_domain(comp_url)
                if comp_domain:
                    domain_counts[comp_domain] += 1
                    comp_id = clean_text(row.get("competitor_id", ""))
                    if comp_id:
                        domain_counts[f"id:{comp_id}"] += 1
                    repricer_id = clean_text(row.get("repricer_id", ""))
                    if repricer_id:
                        repricer_counts[repricer_id] += 1
        
        # Most common competitor ID
        self.cm_competitor_id = ""
        self.cm_repricer_id = ""
        
        for key, count in domain_counts.most_common():
            if key.startswith("id:"):
                self.cm_competitor_id = key[3:]
                break
        
        if repricer_counts:
            self.cm_repricer_id = repricer_counts.most_common(1)[0][0]
        
        # Second pass: load matches
        with self.cm_file.open("r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                pid = clean_text(row.get("product_id", ""))
                if not pid or pid not in self.system_products:
                    continue
                
                # Filter by competitor
                comp_id = clean_text(row.get("competitor_id", ""))
                if self.cm_competitor_id and comp_id != self.cm_competitor_id:
                    continue
                
                # Determine source
                source = "cm" if row.get("source") == "CM" else "pr"
                
                match = CompetitorMatch(
                    product_id=pid,
                    competitor_id=comp_id,
                    repricer_id=clean_text(row.get("repricer_id", "")),
                    competitor_url=clean_text(row.get("competitor_url", "")),
                    competitor_name=clean_text(row.get("competitor_name", "")),
                    reason=clean_text(row.get("reason", "")),
                    other_reason=clean_text(row.get("other_reason", "")),
                    competitor_price=clean_float(row.get("competitor_price", 0)),
                    last_update_date=clean_text(row.get("last_update_date", "")),
                    sku_mismatch=clean_text(row.get("sku_mismatch", "")),
                    cm_received_sku=clean_text(row.get("cm_received_sku", "")),
                    cm_received_name=clean_text(row.get("competitor_product_name", "")),
                    source=source
                )
                
                match.normalize()
                
                # Keep the most recent match
                if pid not in self.competitor_matches:
                    self.competitor_matches[pid] = match
                else:
                    existing_date = self.competitor_matches[pid].last_update_date
                    new_date = match.last_update_date
                    if new_date > existing_date:
                        self.competitor_matches[pid] = match
    
    def evaluate_products(self) -> None:
        """Evaluate all products and make decisions for 5 cases"""
        total = len(self.system_products)
        
        for idx, (pid, sys_product) in enumerate(self.system_products.items(), 1):
            if idx % 1000 == 0 or idx == 1 or idx == total:
                print(f"[{self.mode.upper()}] Processing {idx}/{total}")
            
            match = self.competitor_matches.get(pid)
            self.evaluate_product(pid, sys_product, match)
    
    def evaluate_product(self, pid: str, sys_product: SystemProduct, 
                        match: Optional[CompetitorMatch]) -> None:
        """Evaluate a single product and determine case"""
        
        # Find candidate matches from scrape data
        candidates = self.find_candidates(sys_product)
        
        # Score candidates
        scored_candidates = []
        for idx in candidates:
            candidate = self.score_candidate(sys_product, idx)
            if candidate and candidate.score >= 300:  # Minimum threshold
                scored_candidates.append(candidate)
        
        # Sort by score
        scored_candidates.sort(key=lambda c: (-c.score, -self.signal_rank(c.signal)))
        
        # Determine existing match status
        existing_status, existing_candidate = self.evaluate_existing_match(sys_product, match)
        
        # Make decision based on cases
        decision, decision_reason = self.make_decision(
            pid, sys_product, match,
            existing_status, existing_candidate,
            scored_candidates
        )
        
        # Record decision with rich data
        self.record_decision(
            pid, sys_product, match,
            existing_status, existing_candidate,
            scored_candidates,
            decision, decision_reason
        )
    
    def find_candidates(self, sys_product: SystemProduct) -> Set[int]:
        """Find candidate scrape indices for this product"""
        candidates = set()
        
        # Match by MPN/SKU tokens
        for token in sys_product._id_tokens:
            candidates.update(self.scrape_indexes["mpn"].get(token, []))
            
            # Brand+MPN combination
            if sys_product._brand_norm:
                candidates.update(self.scrape_indexes["brand_mpn"].get(
                    f"{sys_product._brand_norm}|{token}", []
                ))
        
        # Match by GTIN
        for token in sys_product._gtin_tokens:
            candidates.update(self.scrape_indexes["gtin"].get(token, []))
        
        # Match by URL slug
        if sys_product._url_slug:
            candidates.update(self.scrape_indexes["handle"].get(sys_product._url_slug, []))
        
        return candidates
    
    def score_candidate(self, sys_product: SystemProduct, scrape_idx: int) -> Optional[CandidateResult]:
        """Score a candidate match using all available data"""
        if scrape_idx >= len(self.scrape_products):
            return None
        
        scrape = self.scrape_products[scrape_idx]
        
        # Use PHP validator to calculate base score
        url_tokens = tokenize(scrape.url)
        url_norm = self.validator.normalize(scrape.url)
        matched_tokens = []
        
        # Prepare competitor data for validator
        comp_data = {
            "competitor_url": scrape.url,
            "competitor_price": scrape.price
        }
        
        score, reasons, wrong_reasons = self.validator.calculate_score(
            asdict(sys_product),
            comp_data,
            url_tokens,
            url_norm,
            matched_tokens
        )
        
        # Calculate additional metrics
        name_similarity = self.name_similarity(
            sys_product.product_name,
            scrape.product_name
        )
        
        # Brand matching
        brand_relation = self.brand_relation(
            sys_product.brand_label,
            scrape.brand
        )
        
        flags = {
            "brand_exact": brand_relation == "EXACT",
            "brand_clone": brand_relation == "CLONE",
            "brand_conflict": brand_relation == "MISMATCH",
        }
        
        # Brand score adjustments
        if brand_relation == "EXACT":
            score += 120
            reasons.append("Brand exact")
        elif brand_relation == "CLONE":
            score += 65
            reasons.append("Brand clone/synonym")
        elif brand_relation == "MISMATCH":
            score -= 120
            reasons.append("Brand mismatch")
        
        # Category matching
        category_match_percent = 0.0
        if sys_product.cat and scrape.category:
            sys_tokens = set(tokenize(sys_product.cat))
            scrape_tokens = set(tokenize(scrape.category))
            if sys_tokens and scrape_tokens:
                common = len(sys_tokens & scrape_tokens)
                category_match_percent = (common / max(len(sys_tokens), len(scrape_tokens))) * 100
                
                if category_match_percent >= 80:
                    score += 80
                    reasons.append(f"Category exact ({category_match_percent:.1f}%)")
                elif category_match_percent >= 50:
                    score += 40
                    reasons.append(f"Category partial ({category_match_percent:.1f}%)")
        
        # Name similarity
        if name_similarity >= 70:
            score += 90
            reasons.append(f"Name similarity {name_similarity:.1f}%")
        elif name_similarity >= 45:
            score += 55
            reasons.append(f"Name similarity {name_similarity:.1f}%")
        elif name_similarity >= 25:
            score += 20
            reasons.append(f"Name similarity {name_similarity:.1f}%")
        
        # Price comparison
        price_diff_percent = 0.0
        price_diff_abs = 0.0
        if sys_product.our_price > 0 and scrape.price > 0:
            price_diff_abs = abs(sys_product.our_price - scrape.price)
            price_diff_percent = (price_diff_abs / sys_product.our_price) * 100
        
        # Attribute matching (config values)
        attribute_matches = {}
        attribute_values = {}
        for attr, sys_val in sys_product._config_values.items():
            if sys_val and sys_val in url_norm:
                attribute_matches[attr] = True
                attribute_values[attr] = (sys_val, sys_val)
        
        # Determine signal and confidence
        signal, confidence = self.determine_signal_and_confidence(
            sys_product, scrape, score, flags
        )
        
        return CandidateResult(
            idx=scrape_idx,
            signal=signal,
            score=int(round(score)),
            confidence=confidence,
            remark="",
            name_similarity=name_similarity,
            reasons=reasons,
            flags=flags,
            matched_tokens=matched_tokens,
            scrape_product=scrape,
            system_product=sys_product,
            price_diff_percent=price_diff_percent,
            price_diff_abs=price_diff_abs,
            brand_match_type=brand_relation,
            category_match_percent=category_match_percent,
            attribute_matches=attribute_matches,
            attribute_values=attribute_values
        )
    
    def name_similarity(self, name1: str, name2: str) -> float:
        """Calculate name similarity percentage"""
        tokens1 = set(tokenize(name1))
        tokens2 = set(tokenize(name2))
        
        if not tokens1 or not tokens2:
            return 0.0
        
        common = len(tokens1 & tokens2)
        return (common / len(tokens1 | tokens2)) * 100.0
    
    def brand_relation(self, sys_brand: str, comp_brand: str) -> str:
        """Determine brand relationship"""
        if not sys_brand or not comp_brand:
            return "UNKNOWN"
        
        sys_norm = norm_brand(sys_brand)
        comp_norm = norm_brand(comp_brand)
        
        if sys_norm == comp_norm:
            return "EXACT"
        
        if sys_norm in comp_norm or comp_norm in sys_norm:
            return "CLONE"
        
        # Check core tokens
        sys_tokens = {t for t in tokenize(sys_brand) if t not in STOP_WORDS}
        comp_tokens = {t for t in tokenize(comp_brand) if t not in STOP_WORDS}
        
        if sys_tokens and comp_tokens:
            overlap = len(sys_tokens & comp_tokens) / max(len(sys_tokens), len(comp_tokens))
            if overlap >= 0.6:
                return "CLONE"
        
        return "MISMATCH"
    
    def determine_signal_and_confidence(self, sys_product: SystemProduct, 
                                       scrape: ScrapeProduct,
                                       score: int, flags: Dict[str, bool]) -> Tuple[str, str]:
        """Determine match signal and confidence level"""
        
        # Check for MPN+GTIN combination
        mpn_exact = any(t in scrape._mpn_tokens for t in sys_product._mpn_tokens) if sys_product._mpn_tokens else False
        gtin_exact = any(t in scrape._gtin_tokens for t in sys_product._gtin_tokens) if sys_product._gtin_tokens else False
        
        if mpn_exact and gtin_exact:
            return "MPN_GTIN", "HIGH"
        elif mpn_exact:
            return "MPN", "HIGH"
        elif gtin_exact:
            return "GTIN", "MEDIUM"
        elif sys_product._url_slug and sys_product._url_slug == scrape._handle:
            return "URL_HANDLE", "MEDIUM"
        else:
            # Based on score
            if score >= 700:
                return "HIGH_SCORE", "HIGH"
            elif score >= 500:
                return "MEDIUM_SCORE", "MEDIUM"
            else:
                return "LOW_SCORE", "LOW"
    
    def evaluate_existing_match(self, sys_product: SystemProduct, 
                               match: Optional[CompetitorMatch]) -> Tuple[str, Optional[CandidateResult]]:
        """Evaluate existing CM match against scrape data"""
        if not match or not match.competitor_url:
            return "NO_EXISTING", None
        
        # Find scrape rows matching this URL
        candidates = set()
        if match._path_key:
            candidates.update(self.scrape_indexes["path_key"].get(match._path_key, []))
        if match._url_fp:
            candidates.update(self.scrape_indexes["url_fp"].get(match._url_fp, []))
        
        if not candidates:
            return "URL_NOT_IN_SCRAPE", None
        
        # Score candidates
        best_candidate = None
        for idx in candidates:
            candidate = self.score_candidate(sys_product, idx)
            if candidate:
                if not best_candidate or candidate.score > best_candidate.score:
                    best_candidate = candidate
        
        if not best_candidate:
            return "MATCH_WRONG", None
        
        # Check if match is correct
        if best_candidate.signal in ["MPN_GTIN", "MPN", "GTIN"] and best_candidate.score >= 500:
            if not best_candidate.flags.get("brand_conflict", False):
                return "MATCH_CORRECT", best_candidate
        
        return "MATCH_WRONG", best_candidate
    
    def make_decision(self, pid: str, sys_product: SystemProduct, 
                     match: Optional[CompetitorMatch],
                     existing_status: str, existing_candidate: Optional[CandidateResult],
                     candidates: List[CandidateResult]) -> Tuple[str, str]:
        """
        Make decision for 5 cases:
        1. Wrong Match
        2. New Match
        3. Approve Match
        4. Keep Existing
        5. Manual Review
        """
        
        # Check if there's a valid candidate
        best_candidate = candidates[0] if candidates else None
        
        # Determine required confidence based on mode
        required_confidence = "HIGH" if self.mode == 'pr' else "MEDIUM"
        
        # Case 3: Approve Match (existing match marked as wrong but actually valid)
        if match and self.is_wrong_match(match) and existing_status == "MATCH_CORRECT":
            return "APPROVE_MATCH", "Previously wrong match now validated"
        
        # Case 4: Keep Existing (valid existing match)
        if existing_status == "MATCH_CORRECT":
            return "KEEP_EXISTING", "Existing match is valid"
        
        # Case 1: Wrong Match (existing match is wrong, no good replacement)
        if existing_status in ["MATCH_WRONG", "URL_NOT_IN_SCRAPE"]:
            # If no good candidate, mark as wrong
            if not best_candidate or best_candidate.confidence not in ["HIGH", "MEDIUM"]:
                return "WRONG_MATCH", "Existing match is wrong, no good replacement"
        
        # Case 2: New Match (no existing match, good candidate found)
        if not match and best_candidate:
            if best_candidate.confidence == "HIGH":
                return "NEW_MATCH", "High confidence new match found"
            elif best_candidate.confidence == "MEDIUM" and required_confidence == "MEDIUM":
                return "NEW_MATCH", "Medium confidence new match found"
        
        # Case 5: Manual Review (ambiguous or conflicting)
        if len(candidates) >= 2:
            # Check if top candidates are close in score
            if abs(candidates[0].score - candidates[1].score) <= 50:
                return "MANUAL_REVIEW", "Multiple close candidates"
        
        if best_candidate and best_candidate.flags.get("brand_conflict", False):
            if best_candidate.signal not in ["MPN_GTIN", "MPN"]:
                return "MANUAL_REVIEW", "Brand mismatch without strong ID match"
        
        # Default: no match
        if not best_candidate:
            if existing_status == "NO_EXISTING":
                return "NO_MATCH", "No candidate found"
            else:
                return "WRONG_MATCH", "Existing match invalid, no replacement"
        
        # If existing match is wrong and there's a good candidate, it could be new match
        if existing_status in ["MATCH_WRONG", "URL_NOT_IN_SCRAPE"] and best_candidate:
            if best_candidate.confidence == "HIGH":
                return "NEW_MATCH", "Replacing wrong match with high confidence candidate"
            elif best_candidate.confidence == "MEDIUM" and required_confidence == "MEDIUM":
                return "NEW_MATCH", "Replacing wrong match with medium confidence candidate"
        
        return "MANUAL_REVIEW", "Ambiguous case - requires review"
    
    def is_wrong_match(self, match: CompetitorMatch) -> bool:
        """Check if existing match is marked as wrong"""
        reason = match.reason.lower()
        return "wrong match" in reason or "wrong" in reason
    
    def signal_rank(self, signal: str) -> int:
        """Rank signal strength for sorting"""
        ranks = {
            "MPN_GTIN": 6,
            "MPN": 5,
            "GTIN": 4,
            "HIGH_SCORE": 3,
            "URL_HANDLE": 2,
            "MEDIUM_SCORE": 1,
            "LOW_SCORE": 0
        }
        return ranks.get(signal, 0)
    
    def record_decision(self, pid: str, sys_product: SystemProduct, 
                       match: Optional[CompetitorMatch],
                       existing_status: str, existing_candidate: Optional[CandidateResult],
                       candidates: List[CandidateResult],
                       decision: str, decision_reason: str) -> None:
        """Record decision with rich data for reporting"""
        
        best = candidates[0] if candidates else None
        existing_url = match.competitor_url if match else ""
        existing_reason = match.reason if match else ""
        existing_sku_mismatch = match.sku_mismatch if match else ""
        
        # Build comprehensive report row
        report_row = {
            # Product identifiers
            "product_id": pid,
            "sku": sys_product.sku,
            "web_id": sys_product.web_id,
            "product_name": sys_product.product_name,
            "brand_label": sys_product.brand_label,
            "brand_id": sys_product.brand_id,
            "category": sys_product.cat,
            "collection": sys_product.collection,
            "type": sys_product.type,
            "status": sys_product.status,
            "visibility": sys_product.visibility,
            
            # Product identifiers
            "mpn": sys_product.mpn,
            "gtin": sys_product.gtin,
            "part_number": sys_product.part_number,
            "osb_url": sys_product.osb_url,
            
            # Pricing
            "our_price": sys_product.our_price,
            "map_price": sys_product.map_price,
            
            # Sales data
            "sales_90_days": sys_product.sales_90_days,
            "mfr_sales_30_days": sys_product.mfr_sales_30_days,
            
            # Attributes
            "color": sys_product.color,
            "bed_size_measure": sys_product.bed_size_measure,
            "size": sys_product.size,
            "fireplace_option": sys_product.fireplace_option,
            "layout_icon": sys_product.layout_icon,
            "rug_size": sys_product.rug_size,
            "mattress_size": sys_product.mattress_size,
            "power_option": sys_product.power_option,
            "dimension_text": sys_product.dimension_text,
            "comfort_level": sys_product.comfort_level,
            "mattress_thickness": sys_product.mattress_thickness,
            
            # Existing match info
            "existing_status": existing_status,
            "existing_url": existing_url,
            "existing_reason": existing_reason,
            "existing_sku_mismatch": existing_sku_mismatch,
            "existing_price": match.competitor_price if match else "",
            "existing_last_update": match.last_update_date if match else "",
            "existing_source": match.source if match else "",
            
            # Decision
            "decision": decision,
            "decision_reason": decision_reason,
        }
        
        # Add best candidate info
        if best and best.scrape_product:
            scrape = best.scrape_product
            report_row.update({
                # Candidate identifiers
                "best_candidate_url": scrape.url,
                "best_candidate_product_id": scrape.product_id,
                "best_candidate_variant_id": scrape.variant_id,
                "best_candidate_name": scrape.product_name,
                "best_candidate_brand": scrape.brand,
                "best_candidate_category": scrape.category,
                "best_candidate_mpn": scrape.mpn,
                "best_candidate_sku": scrape.sku,
                "best_candidate_gtin": scrape.gtin,
                "best_candidate_price": scrape.price,
                "best_candidate_quantity": scrape.quantity,
                "best_candidate_group_attr_1": scrape.group_attr_1,
                "best_candidate_group_attr_2": scrape.group_attr_2,
                "best_candidate_status": scrape.status,
                "best_candidate_date_scrapped": scrape.date_scrapped,
                
                # Match metrics
                "best_candidate_score": best.score,
                "best_candidate_confidence": best.confidence,
                "best_candidate_signal": best.signal,
                "best_candidate_name_similarity": f"{best.name_similarity:.1f}",
                "best_candidate_price_diff_percent": f"{best.price_diff_percent:.1f}",
                "best_candidate_price_diff_abs": f"{best.price_diff_abs:.2f}",
                "best_candidate_brand_match": best.brand_match_type,
                "best_candidate_category_match": f"{best.category_match_percent:.1f}",
                "best_candidate_reasons": "; ".join(best.reasons[:5]),
                "best_candidate_matched_tokens": ", ".join(best.matched_tokens[:10]),
                
                # Attribute matches
                "best_candidate_attribute_matches": "; ".join([
                    attr for attr, matched in best.attribute_matches.items() if matched
                ]),
            })
        
        # Add candidate summary
        if candidates:
            report_row["candidates_count"] = len(candidates)
            report_row["top_candidates"] = " | ".join([
                f"{c.scrape_product.url if c.scrape_product else 'N/A'}#{c.signal}:{c.score}"
                for c in candidates[:3]
            ])
        
        self.report_rows.append(report_row)
        
        # Add to specific case collections
        case_row = {
            "product_id": pid,
            "sku": sys_product.sku,
            "web_id": sys_product.web_id,
            "product_name": sys_product.product_name,
            "brand_label": sys_product.brand_label,
            "category": sys_product.cat,
            "mpn": sys_product.mpn,
            "gtin": sys_product.gtin,
            "sales_90_days": sys_product.sales_90_days,
            "mfr_sales_30_days": sys_product.mfr_sales_30_days,
            "existing_url": existing_url,
            "existing_reason": existing_reason,
            "decision_reason": decision_reason,
            
            # Best candidate info
            "best_candidate_url": report_row.get("best_candidate_url", ""),
            "best_candidate_score": report_row.get("best_candidate_score", ""),
            "best_candidate_confidence": report_row.get("best_candidate_confidence", ""),
            "best_candidate_signal": report_row.get("best_candidate_signal", ""),
        }
        
        if decision == "WRONG_MATCH":
            self.wrong_match_rows.append(case_row)
        elif decision == "NEW_MATCH":
            self.new_match_rows.append(case_row)
            if best:
                self.used_scrape_indices.add(best.idx)
                if best.scrape_product:
                    self.allocated_ref_urls.add(url_fingerprint(best.scrape_product.url))
        elif decision == "APPROVE_MATCH":
            self.approve_match_rows.append(case_row)
        elif decision == "KEEP_EXISTING":
            self.keep_existing_rows.append(case_row)
        elif decision == "MANUAL_REVIEW":
            self.manual_review_rows.append(case_row)
        
        self.decision_by_product[pid] = decision
    
    def build_unmatched(self) -> None:
        """Build list of unmatched scrape products"""
        for idx, product in enumerate(self.scrape_products):
            if idx not in self.used_scrape_indices:
                self.unmatched_scrape_products.append(product)
    
    def generate_reports(self) -> None:
        """Generate summary statistics and reports"""
        
        # Case counts
        case_counts = Counter(self.decision_by_product.values())
        
        # Competitor statistics
        comp_stats = defaultdict(lambda: {"total": 0, "wrong": 0, "new": 0, "keep": 0})
        for pid, decision in self.decision_by_product.items():
            match = self.competitor_matches.get(pid)
            if match and match.competitor_name:
                comp = match.competitor_name
            else:
                comp = "NO_MATCH"
            
            comp_stats[comp]["total"] += 1
            if decision == "WRONG_MATCH":
                comp_stats[comp]["wrong"] += 1
            elif decision == "NEW_MATCH":
                comp_stats[comp]["new"] += 1
            elif decision == "KEEP_EXISTING":
                comp_stats[comp]["keep"] += 1
        
        # Sales impact analysis
        total_sales = sum(p.sales_90_days for p in self.system_products.values())
        matched_sales = sum(
            self.system_products[pid].sales_90_days 
            for pid in self.decision_by_product.keys()
            if self.decision_by_product[pid] in ["KEEP_EXISTING", "NEW_MATCH", "APPROVE_MATCH"]
        )
        
        self.summary = {
            "mode": self.mode,
            "timestamp": datetime.now().isoformat(),
            "products_evaluated": len(self.report_rows),
            "case_counts": dict(case_counts),
            "wrong_match": len(self.wrong_match_rows),
            "new_match": len(self.new_match_rows),
            "approve_match": len(self.approve_match_rows),
            "keep_existing": len(self.keep_existing_rows),
            "manual_review": len(self.manual_review_rows),
            "unmatched_scrape": len(self.unmatched_scrape_products),
            "competitor_statistics": dict(comp_stats),
            "sales_impact": {
                "total_sales_90_days": total_sales,
                "matched_sales_90_days": matched_sales,
                "coverage_percent": round((matched_sales / total_sales * 100) if total_sales > 0 else 0, 2)
            }
        }
    
    def write_outputs(self) -> None:
        """Write all output files"""
        
        # Write full report with all columns
        if self.report_rows:
            headers = list(self.report_rows[0].keys())
            self.write_csv(self.output_dir / "01_full_report.csv", self.report_rows, headers)
        
        # Write case-specific files
        case_headers = [
            "product_id", "sku", "web_id", "product_name", "brand_label",
            "category", "mpn", "gtin", "sales_90_days", "mfr_sales_30_days",
            "existing_url", "existing_reason", "decision_reason",
            "best_candidate_url", "best_candidate_score", 
            "best_candidate_confidence", "best_candidate_signal"
        ]
        
        self.write_csv(self.output_dir / "02_wrong_match.csv", self.wrong_match_rows, case_headers)
        self.write_csv(self.output_dir / "03_new_match.csv", self.new_match_rows, case_headers)
        self.write_csv(self.output_dir / "04_approve_match.csv", self.approve_match_rows, case_headers)
        self.write_csv(self.output_dir / "05_keep_existing.csv", self.keep_existing_rows, case_headers)
        self.write_csv(self.output_dir / "06_manual_review.csv", self.manual_review_rows, case_headers)
        
        # Write unmatched scrape data with all columns
        if self.unmatched_scrape_products:
            unmatched_rows = [p.raw for p in self.unmatched_scrape_products]
            self.write_csv(self.output_dir / "07_unmatched_scrape.csv", unmatched_rows, self.scrape_headers)
        
        # Write summary
        self.write_json(self.output_dir / "summary.json", self.summary)
        
        # Create competitor analysis report
        self.write_competitor_report()
        
        # Create sales impact report
        self.write_sales_report()
        
        # Create zip archive
        self.create_zip()
    
    def write_competitor_report(self) -> None:
        """Write competitor-wise analysis report"""
        comp_rows = []
        
        for comp_name, stats in self.summary.get("competitor_statistics", {}).items():
            row = {
                "competitor": comp_name,
                "total_products": stats["total"],
                "wrong_match": stats["wrong"],
                "new_match": stats["new"],
                "keep_existing": stats["keep"],
                "match_rate": round((stats["keep"] + stats["new"]) / stats["total"] * 100, 2) if stats["total"] > 0 else 0
            }
            comp_rows.append(row)
        
        if comp_rows:
            headers = ["competitor", "total_products", "wrong_match", "new_match", "keep_existing", "match_rate"]
            self.write_csv(self.output_dir / "08_competitor_analysis.csv", comp_rows, headers)
    
    def write_sales_report(self) -> None:
        """Write sales impact report"""
        sales_rows = []
        
        for decision in ["KEEP_EXISTING", "NEW_MATCH", "APPROVE_MATCH", "WRONG_MATCH", "MANUAL_REVIEW"]:
            products = [
                self.system_products[pid] 
                for pid, dec in self.decision_by_product.items() 
                if dec == decision
            ]
            
            if products:
                total_sales = sum(p.sales_90_days for p in products)
                avg_sales = total_sales / len(products) if products else 0
                total_mfr = sum(p.mfr_sales_30_days for p in products)
                
                sales_rows.append({
                    "decision": decision,
                    "product_count": len(products),
                    "total_sales_90_days": total_sales,
                    "avg_sales_90_days": round(avg_sales, 2),
                    "total_mfr_sales_30_days": total_mfr,
                    "avg_mfr_sales_30_days": round(total_mfr / len(products) if products else 0, 2)
                })
        
        if sales_rows:
            headers = ["decision", "product_count", "total_sales_90_days", "avg_sales_90_days",
                      "total_mfr_sales_30_days", "avg_mfr_sales_30_days"]
            self.write_csv(self.output_dir / "09_sales_impact.csv", sales_rows, headers)
    
    def write_csv(self, path: Path, rows: List[Dict[str, Any]], headers: List[str]) -> None:
        """Write rows to CSV file"""
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            for row in rows:
                # Ensure all headers are present
                clean_row = {}
                for h in headers:
                    val = row.get(h, "")
                    # Convert None to empty string
                    if val is None:
                        val = ""
                    clean_row[h] = val
                writer.writerow(clean_row)
    
    def write_json(self, path: Path, data: Any) -> None:
        """Write JSON file"""
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, default=str)
    
    def create_zip(self) -> None:
        """Create zip archive of output files"""
        zip_path = self.output_dir / f"{self.mode}_reconciliation.zip"
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for file in self.output_dir.glob("*.csv"):
                if file.name != "07_unmatched_scrape.csv":  # Optionally exclude large file
                    zf.write(file, arcname=file.name)


# ============================================================================
# Command Line Interface
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Unified Competitor Match Reconciliation with Comprehensive Data"
    )
    
    parser.add_argument("scrape_file", help="Scraped competitor data CSV")
    parser.add_argument("system_file", help="System product data CSV")
    parser.add_argument("cm_file", help="Existing competitor mappings CSV")
    
    parser.add_argument("--mode", "-m", choices=["cm", "pr"], default="cm",
                       help="Mode: cm (URL-based) or pr (name-based)")
    parser.add_argument("--output-dir", "-o", default="reconcile_output",
                       help="Output directory")
    parser.add_argument("--min-confidence", choices=["AUTO", "HIGH", "MEDIUM"],
                       default="AUTO", help="Minimum confidence for auto decisions")
    parser.add_argument("--limit", type=int, help="Limit number of products to process")
    
    args = parser.parse_args()
    
    pipeline = UnifiedReconciliationPipeline(
        scrape_file=Path(args.scrape_file),
        system_file=Path(args.system_file),
        cm_file=Path(args.cm_file),
        output_dir=Path(args.output_dir),
        mode=args.mode,
        limit=args.limit,
        min_confidence=args.min_confidence
    )
    
    summary = pipeline.run()
    
    print("\n" + "="*60)
    print("RECONCILIATION SUMMARY")
    print("="*60)
    for key, value in summary.items():
        if key not in ["competitor_statistics"]:
            print(f"{key:25}: {value}")
    
    print("\nCase Counts:")
    for case, count in summary.get("case_counts", {}).items():
        print(f"  {case:20}: {count}")
    
    if "sales_impact" in summary:
        print("\nSales Impact:")
        sales = summary["sales_impact"]
        print(f"  Total Sales (90 days)   : {sales['total_sales_90_days']}")
        print(f"  Matched Sales (90 days) : {sales['matched_sales_90_days']}")
        print(f"  Coverage                : {sales['coverage_percent']}%")
    
    print("="*60)
    print(f"Output directory: {pipeline.output_dir}")
    print(f"Zip archive     : {pipeline.output_dir / f'{args.mode}_reconciliation.zip'}")


if __name__ == "__main__":
    main()