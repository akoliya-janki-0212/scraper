#!/usr/bin/env python3
"""
Multi-Competitor Reconciliation Pipeline
Matches each product with ALL relevant competitors for comprehensive price comparison
Handles 5 cases with complete competitive landscape
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
from typing import Any, Dict, List, Optional, Set, Tuple
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


def extract_domain_from_competitor(competitor: str) -> str:
    """Extract likely domain from competitor name"""
    name = competitor.lower()
    # Remove common words
    name = re.sub(r'\s+(inc|llc|ltd|co|corp|company|official|store|shop)$', '', name)
    # Convert to domain format
    domain = re.sub(r'[^a-z0-9]+', '', name)
    return domain


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
        segments = path.split("/")[:2]
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
    'Furniture Cart': ['items'],
    'Furniture Pick': ['items', 'size'],
    'Bed Bath & Beyond': ['option'],
    'English Elm': ['variant'],
    'France & Son': ['variant'],
    'Grayson Living': ['variant'],
    'Over Stock': ['option'],
    'Overstock.com': ['option'],
}


@dataclass
class ScoreConfig:
    """Scoring configuration (ported from PHP)"""
    exact_mpn_match: int = 270
    full_name_match: int = 70
    high_name_match: int = 60
    partial_name_match: int = 25
    full_url_match: int = 70
    high_url_match: int = 60
    partial_url_match: int = 25
    full_config_match: int = 70
    config_match: int = 60
    price_valid: int = 20
    no_pending_parts: int = 60
    add_set_mismatch_score: int = 140
    set_mismatch: int = -200
    same_brand_wrong_match: int = -100
    attribute_mismatch: int = -70
    same_group_wrong_match: int = -80
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
    
    def __init__(self):
        self.score_config = ScoreConfig()
        self.stop_words = STOP_WORDS
        self.synonyms = SYNONYMS.copy()
        self.exclude_synonyms = EXCLUDE_SYNONYMS
        self.set_categories = SET_CATEGORIES
        self.comp_url_params = COMP_URL_PARAMS
        
        # Caches
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
        
        if len(self.normalization_cache) > 50000:
            self.normalization_cache = {}
        
        text = text.lower()
        words = re.findall(r"[a-z0-9]+", text)
        if not words:
            self.normalization_cache[cache_key] = ""
            return ""
        
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
        
        filtered = []
        for t in tokens:
            if len(t) <= 1:
                continue
            if len(t) >= 6 and re.search(r"\d", t) and re.search(r"[a-z]", t):
                if len(t) > 8:
                    continue
            filtered.append(t)
        
        result = list(dict.fromkeys(filtered))
        self.token_cache[cache_key] = result
        return result
    
    def fuzzy_match(self, needle: str, haystack_tokens: List[str]) -> int:
        """Fuzzy match with synonyms"""
        if not needle or not haystack_tokens:
            return 0
        
        options = self.split_values_for_synonyms(needle)
        needle_lower = needle.lower()
        
        if len(options) > 1:
            if needle_lower not in self.synonyms:
                self.synonyms[needle_lower] = []
            for opt in options:
                opt_norm = self.normalize(opt)
                if opt_norm:
                    self.synonyms[needle_lower].append(opt_norm)
            self.synonyms[needle_lower] = list(dict.fromkeys(self.synonyms[needle_lower]))
        
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
            if token in needle_variants:
                return 100
            
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
        has_set_in_url = bool(re.search(r'(^|[^a-z0-9])set(?!-of)([^a-z0-9]|$)', url.lower()))
        is_set_category = category in self.set_categories
        has_set_in_name = 'set' in name_tokens
        return has_set_in_url or is_set_category or has_set_in_name
    
    def merge_mpn(self, value: str) -> str:
        """Merge multiple MPNs"""
        parts = [p for p in value.split(';') if p.strip()]
        
        if len(parts) < 2:
            return value.lower()
        
        parts.sort()
        first = parts[0].lower().strip()
        result = first
        
        prefix = first.split('-')[0] if '-' in first else first
        
        for i in range(1, len(parts)):
            mpn = parts[i].lower().strip()
            if mpn.startswith(prefix + '-'):
                mpn = mpn[len(prefix) + 1:]
            result += '-' + mpn
        
        return result
    
    def calculate_score(self, system_data: Dict[str, Any], competitor_data: Dict[str, Any], 
                       url_tokens: List[str], url_norm: str, matched_tokens: List[str]) -> Tuple[int, List[str], List[str]]:
        """
        Calculate confidence score based on PHP logic
        Returns: (score, reasons, wrong_reasons)
        """
        score = 0
        reasons = []
        wrong_reasons = []
        
        # Extract data
        product_name = system_data.get('product_name', '')
        brand = system_data.get('brand_label', '')
        category = system_data.get('cat', '')
        mpn = system_data.get('mpn', '')
        sku = system_data.get('sku', '')
        part_number = system_data.get('part_number', '')
        our_price = float(system_data.get('our_price', 0) or 0)
        comp_price = float(competitor_data.get('competitor_price', 0) or 0)
        
        # MPN/SKU/PART matching
        mpn_values = [v for v in [mpn, sku, part_number] if v]
        
        for value in mpn_values:
            value_norm = self.normalize(value)
            if value_norm and (value_norm in url_norm or value_norm in competitor_data.get('competitor_url', '')):
                score += self.score_config.exact_mpn_match
                reasons.append(f"Exact MPN/SKU match: {value}")
                matched_tokens.extend(tokenize(value))
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
# Data Classes
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
    
    # Normalized fields
    _mpn_tokens: List[str] = field(default_factory=list)
    _sku_tokens: List[str] = field(default_factory=list)
    _part_tokens: List[str] = field(default_factory=list)
    _gtin_tokens: List[str] = field(default_factory=list)
    _id_tokens: List[str] = field(default_factory=list)
    _brand_norm: str = ""
    _url_slug: str = ""
    
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
    
    # Competitor identification
    competitor_name: str = ""
    competitor_id: str = ""
    
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
    _competitor_domain: str = ""
    
    def extract(self, row: Dict[str, str]):
        """Extract data from raw CSV row"""
        self.raw = row
        
        # Basic fields
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
        
        # Competitor identification - handle both naming conventions
        self.competitor_name = clean_text(row.get("Competitor Name", "")) or \
                               clean_text(row.get("competitor_name", "")) or \
                               clean_text(row.get("Competitor", "")) or \
                               clean_text(row.get("Retailer", ""))
        self.competitor_id = clean_text(row.get("Competitor ID", "")) or \
                             clean_text(row.get("competitor_id", ""))
        
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
        self._competitor_domain = extract_domain_from_competitor(self.competitor_name)


@dataclass
class ExistingMatch:
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
    _competitor_domain: str = ""
    
    def normalize(self):
        """Populate normalized fields"""
        self._url_fp = url_fingerprint(self.competitor_url)
        self._path_key = path_key(self.competitor_url)
        self._url_slug = norm_id(url_slug(self.competitor_url))
        self._competitor_domain = extract_domain_from_competitor(self.competitor_name)


@dataclass
class MatchResult:
    """Complete match result for a product-competitor pair"""
    product_id: str
    competitor_name: str
    competitor_id: str
    scrape_idx: int
    signal: str
    score: int
    confidence: str
    name_similarity: float
    reasons: List[str]
    flags: Dict[str, bool] = field(default_factory=dict)
    
    # Rich data
    scrape_product: Optional[ScrapeProduct] = None
    system_product: Optional[SystemProduct] = None
    
    # Match details
    price_diff_percent: float = 0.0
    price_diff_abs: float = 0.0
    brand_match_type: str = ""
    category_match_percent: float = 0.0
    
    # Decision
    decision: str = "PENDING"
    decision_reason: str = ""


# ============================================================================
# Main Reconciliation Pipeline
# ============================================================================

class MultiCompetitorPipeline:
    """
    Pipeline for reconciling multiple competitors from a single scrap file
    Matches each product with ALL relevant competitors for complete price comparison
    """
    
    def __init__(
        self,
        scrape_file: Path,
        system_file: Path,
        cm_file: Path,
        output_dir: Path,
        limit: Optional[int] = None
    ):
        self.scrape_file = scrape_file
        self.system_file = system_file
        self.cm_file = cm_file
        self.output_dir = output_dir
        self.limit = limit
        
        # Initialize validator
        self.validator = PHPValidator()
        
        # Data storage
        self.system_products: Dict[str, SystemProduct] = {}
        self.system_by_mpn: Dict[str, List[str]] = defaultdict(list)
        self.system_by_sku: Dict[str, List[str]] = defaultdict(list)
        self.system_by_gtin: Dict[str, List[str]] = defaultdict(list)
        self.system_by_url_slug: Dict[str, List[str]] = defaultdict(list)
        
        # Scrape data organized by competitor
        self.competitors: Set[str] = set()
        self.scrape_by_competitor: Dict[str, List[ScrapeProduct]] = defaultdict(list)
        self.scrape_indexes: Dict[str, Dict[str, Dict[str, List[int]]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(list))
        )
        
        # Existing matches
        self.existing_matches: Dict[str, Dict[str, ExistingMatch]] = defaultdict(dict)  # [product_id][competitor_name]
        
        # Match results - store ALL matches for each product-competitor pair
        self.all_matches: Dict[str, Dict[str, List[MatchResult]]] = defaultdict(lambda: defaultdict(list))  # [product_id][competitor_name]
        self.best_matches: Dict[str, Dict[str, MatchResult]] = defaultdict(dict)  # [product_id][competitor_name] - best per competitor
        
        # Summary
        self.summary: Dict[str, Any] = {}
    
    def run(self) -> Dict[str, Any]:
        """Execute the reconciliation pipeline"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.output_dir = self.output_dir / f"multi_competitor_{timestamp}"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        print("="*60)
        print("MULTI-COMPETITOR RECONCILIATION PIPELINE")
        print("="*60)
        print("Matching each product with ALL relevant competitors")
        
        print(f"\n[1/5] Loading system data: {self.system_file}")
        self.load_system()
        print(f"  → {len(self.system_products)} products loaded")
        
        print(f"\n[2/5] Loading scrape data: {self.scrape_file}")
        self.load_scrape()
        print(f"  → {len(self.competitors)} competitors found")
        for comp in sorted(self.competitors)[:10]:  # Show first 10
            count = len(self.scrape_by_competitor[comp])
            print(f"     - {comp}: {count} products")
        if len(self.competitors) > 10:
            print(f"     ... and {len(self.competitors) - 10} more")
        
        print(f"\n[3/5] Loading existing matches: {self.cm_file}")
        self.load_existing_matches()
        match_count = sum(len(matches) for matches in self.existing_matches.values())
        print(f"  → {match_count} existing matches loaded")
        
        print(f"\n[4/5] Finding matches for each competitor...")
        self.find_all_matches()
        
        print(f"\n[5/5] Evaluating match quality...")
        self.evaluate_matches()
        
        print(f"\nGenerating comprehensive reports...")
        self.generate_reports()
        self.write_outputs()
        
        print(f"\n✓ Reconciliation completed!")
        print(f"  Output directory: {self.output_dir}")
        
        return self.summary
    
    def load_system(self) -> None:
        """Load system product data"""
        with self.system_file.open("r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                raise ValueError("System file has no headers")
            
            for row in reader:
                pid = clean_text(row.get("product_id", ""))
                if not pid or pid in self.system_products:
                    continue
                
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
                    sales_90_days=clean_int(row.get("90 days Sales", 0)),
                    mfr_sales_30_days=clean_int(row.get("30 days MFR Sales", 0)),
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
    
    def load_scrape(self) -> None:
        """Load scraped data and organize by competitor"""
        with self.scrape_file.open("r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                raise ValueError("Scrape file has no headers")
            
            for idx, row in enumerate(reader):
                if self.limit and idx >= self.limit:
                    break
                
                product = ScrapeProduct()
                product.extract(row)
                
                if not product.competitor_name:
                    continue
                
                self.competitors.add(product.competitor_name)
                comp_list = self.scrape_by_competitor[product.competitor_name]
                comp_idx = len(comp_list)
                comp_list.append(product)
                
                # Index for this competitor
                idx_map = self.scrape_indexes[product.competitor_name]
                if product._url_fp:
                    idx_map["url_fp"][product._url_fp].append(comp_idx)
                if product._path_key:
                    idx_map["path_key"][product._path_key].append(comp_idx)
                if product._handle:
                    idx_map["handle"][product._handle].append(comp_idx)
                for token in product._mpn_tokens:
                    idx_map["mpn"][token].append(comp_idx)
                for token in product._gtin_tokens:
                    idx_map["gtin"][token].append(comp_idx)
    
    def load_existing_matches(self) -> None:
        """Load existing competitor matches"""
        if not self.cm_file.exists():
            return
        
        with self.cm_file.open("r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                pid = clean_text(row.get("product_id", ""))
                if not pid or pid not in self.system_products:
                    continue
                
                comp_name = clean_text(row.get("competitor_name", "")) or \
                           clean_text(row.get("Competitor", ""))
                if not comp_name:
                    continue
                
                source = "cm" if row.get("source") == "CM" else "pr"
                
                match = ExistingMatch(
                    product_id=pid,
                    competitor_id=clean_text(row.get("competitor_id", "")),
                    repricer_id=clean_text(row.get("repricer_id", "")),
                    competitor_url=clean_text(row.get("competitor_url", "")),
                    competitor_name=comp_name,
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
                self.existing_matches[pid][comp_name] = match
    
    def find_all_matches(self) -> None:
        """Find ALL potential matches for each product across all competitors"""
        total_products = len(self.system_products)
        comp_list = sorted(self.competitors)
        
        # Track match counts for reporting
        match_counts = defaultdict(int)
        
        for comp_idx, competitor in enumerate(comp_list, 1):
            print(f"\r  Processing competitor {comp_idx}/{len(comp_list)}: {competitor[:50]}...", end="")
            
            if competitor not in self.scrape_by_competitor:
                continue
            
            # For each product, find all matching scrape entries for this competitor
            product_count = 0
            matches_found = 0
            
            for prod_idx, (pid, sys_product) in enumerate(self.system_products.items()):
                if prod_idx % 1000 == 0:
                    print(f"\r  {competitor[:30]}: scanning product {prod_idx}/{total_products}...", end="")
                
                # Find all candidate indices for this product-competitor pair
                candidate_indices = self.find_candidates_for_competitor(sys_product, competitor)
                
                if candidate_indices:
                    matches_found += len(candidate_indices)
                    
                    # Score each candidate
                    for idx in candidate_indices:
                        match = self.score_match(sys_product, competitor, idx)
                        if match and match.score >= 300:  # Minimum threshold
                            self.all_matches[pid][competitor].append(match)
                    
                    # Find the best match for this competitor
                    if self.all_matches[pid][competitor]:
                        best = max(self.all_matches[pid][competitor], key=lambda m: m.score)
                        self.best_matches[pid][competitor] = best
                        
                        match_counts[competitor] += 1
                
                product_count += 1
            
            print(f"\r  {competitor[:30]}: found {matches_found} matches for {product_count} products")
        
        print(f"\n  Total matches found: {sum(len(matches) for comp in self.all_matches.values() for matches in comp.values())}")
    
    def find_candidates_for_competitor(self, sys_product: SystemProduct, competitor: str) -> Set[int]:
        """Find candidate scrape indices for this product from a specific competitor"""
        if competitor not in self.scrape_indexes:
            return set()
        
        idx_map = self.scrape_indexes[competitor]
        candidates = set()
        
        # Match by MPN/SKU tokens
        for token in sys_product._id_tokens:
            candidates.update(idx_map["mpn"].get(token, []))
        
        # Match by GTIN
        for token in sys_product._gtin_tokens:
            candidates.update(idx_map["gtin"].get(token, []))
        
        # Match by URL slug
        if sys_product._url_slug:
            candidates.update(idx_map["handle"].get(sys_product._url_slug, []))
        
        return candidates
    
    def score_match(self, sys_product: SystemProduct, competitor: str, idx: int) -> Optional[MatchResult]:
        """Score a potential match"""
        if competitor not in self.scrape_by_competitor or idx >= len(self.scrape_by_competitor[competitor]):
            return None
        
        scrape = self.scrape_by_competitor[competitor][idx]
        
        # Calculate base score
        url_tokens = tokenize(scrape.url)
        url_norm = self.validator.normalize(scrape.url)
        matched_tokens = []
        
        comp_data = {
            "competitor_url": scrape.url,
            "competitor_price": scrape.price
        }
        
        # Convert system product to dict for validator
        sys_dict = asdict(sys_product)
        
        score, reasons, wrong_reasons = self.validator.calculate_score(
            sys_dict,
            comp_data,
            url_tokens,
            url_norm,
            matched_tokens
        )
        
        # Additional metrics
        name_similarity = self.name_similarity(
            sys_product.product_name,
            scrape.product_name
        )
        
        brand_relation = self.brand_relation(
            sys_product.brand_label,
            scrape.brand
        )
        
        flags = {
            "brand_exact": brand_relation == "EXACT",
            "brand_clone": brand_relation == "CLONE",
            "brand_conflict": brand_relation == "MISMATCH",
        }
        
        # Brand adjustments
        if brand_relation == "EXACT":
            score += 120
            reasons.append("Brand exact")
        elif brand_relation == "CLONE":
            score += 65
            reasons.append("Brand clone")
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
                if category_match_percent >= 50:
                    score += 40
                    reasons.append(f"Category match {category_match_percent:.0f}%")
        
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
        
        # Determine signal and confidence
        signal, confidence = self.determine_signal(sys_product, scrape, score)
        
        return MatchResult(
            product_id=sys_product.product_id,
            competitor_name=competitor,
            competitor_id=scrape.competitor_id,
            scrape_idx=idx,
            signal=signal,
            score=int(round(score)),
            confidence=confidence,
            name_similarity=name_similarity,
            reasons=reasons,
            flags=flags,
            scrape_product=scrape,
            system_product=sys_product,
            price_diff_percent=price_diff_percent,
            price_diff_abs=price_diff_abs,
            brand_match_type=brand_relation,
            category_match_percent=category_match_percent
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
        
        sys_tokens = {t for t in tokenize(sys_brand) if t not in STOP_WORDS}
        comp_tokens = {t for t in tokenize(comp_brand) if t not in STOP_WORDS}
        
        if sys_tokens and comp_tokens:
            overlap = len(sys_tokens & comp_tokens) / max(len(sys_tokens), len(comp_tokens))
            if overlap >= 0.6:
                return "CLONE"
        
        return "MISMATCH"
    
    def determine_signal(self, sys_product: SystemProduct, scrape: ScrapeProduct, score: int) -> Tuple[str, str]:
        """Determine match signal and confidence"""
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
        elif score >= 700:
            return "HIGH_SCORE", "HIGH"
        elif score >= 500:
            return "MEDIUM_SCORE", "MEDIUM"
        else:
            return "LOW_SCORE", "LOW"
    
    def evaluate_matches(self) -> None:
        """Evaluate match quality and determine decisions"""
        print("  Evaluating match quality...")
        
        for pid, competitor_matches in self.all_matches.items():
            for competitor, matches in competitor_matches.items():
                for match in matches:
                    # Get existing match if any
                    existing = self.existing_matches.get(pid, {}).get(competitor)
                    
                    # Determine decision for this match
                    match.decision, match.decision_reason = self.determine_match_decision(
                        pid, match, existing
                    )
        
        # Count matches by decision
        decision_counts = defaultdict(int)
        for pid, competitor_matches in self.all_matches.items():
            for competitor, matches in competitor_matches.items():
                for match in matches:
                    decision_counts[match.decision] += 1
        
        print(f"  Match decisions:")
        for decision, count in sorted(decision_counts.items()):
            print(f"    {decision}: {count}")
    
    def determine_match_decision(self, pid: str, match: MatchResult, 
                                existing: Optional[ExistingMatch]) -> Tuple[str, str]:
        """Determine decision for a specific match"""
        
        # Case 3: Approve Match (previously wrong but now valid)
        if existing and self.is_wrong_match(existing):
            if match.signal in ["MPN_GTIN", "MPN", "GTIN"] and match.score >= 500:
                return "APPROVE_MATCH", "Previously wrong match now validated"
        
        # Case 4: Keep Existing (valid existing match)
        if existing and not self.is_wrong_match(existing):
            # Check if this match corresponds to the existing URL
            if existing._url_fp and match.scrape_product and existing._url_fp == match.scrape_product._url_fp:
                return "KEEP_EXISTING", "Valid existing match"
        
        # Case 2: New Match (good quality match with no existing)
        if not existing:
            if match.confidence == "HIGH":
                return "NEW_MATCH", "High confidence new match"
            elif match.confidence == "MEDIUM":
                return "NEW_MATCH", "Medium confidence new match"
        
        # Case 1: Wrong Match (existing match is wrong, no good alternative)
        if existing and self.is_wrong_match(existing):
            return "WRONG_MATCH", "Existing match is wrong"
        
        # Default: needs review
        return "REVIEW", "Needs manual review"
    
    def is_wrong_match(self, match: ExistingMatch) -> bool:
        """Check if match is marked as wrong"""
        reason = match.reason.lower()
        return "wrong match" in reason or "wrong" in reason
    
    def signal_rank(self, signal: str) -> int:
        """Rank signal strength"""
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
    
    def generate_reports(self) -> None:
        """Generate summary statistics"""
        
        # Decision counts
        decision_counts = defaultdict(int)
        competitor_stats = defaultdict(lambda: {
            "total_matches": 0,
            "high_confidence": 0,
            "avg_score": 0,
            "score_sum": 0
        })
        
        # Collect all matches for reporting
        all_match_rows = []
        
        for pid, competitor_matches in self.all_matches.items():
            sys_product = self.system_products[pid]
            
            for competitor, matches in competitor_matches.items():
                for match in matches:
                    # Update counts
                    decision_counts[match.decision] += 1
                    
                    # Update competitor stats
                    comp_stat = competitor_stats[competitor]
                    comp_stat["total_matches"] += 1
                    comp_stat["score_sum"] += match.score
                    if match.confidence == "HIGH":
                        comp_stat["high_confidence"] += 1
                    
                    # Create report row
                    scrape = match.scrape_product
                    existing = self.existing_matches.get(pid, {}).get(competitor)
                    
                    row = {
                        # Product info
                        "product_id": pid,
                        "sku": sys_product.sku,
                        "web_id": sys_product.web_id,
                        "product_name": sys_product.product_name,
                        "brand_label": sys_product.brand_label,
                        "category": sys_product.cat,
                        "mpn": sys_product.mpn,
                        "gtin": sys_product.gtin,
                        "our_price": sys_product.our_price,
                        "sales_90_days": sys_product.sales_90_days,
                        "mfr_sales_30_days": sys_product.mfr_sales_30_days,
                        
                        # Competitor info
                        "competitor": competitor,
                        "competitor_id": match.competitor_id,
                        "competitor_url": scrape.url if scrape else "",
                        "competitor_price": scrape.price if scrape else 0,
                        "competitor_product_name": scrape.product_name if scrape else "",
                        "competitor_mpn": scrape.mpn if scrape else "",
                        
                        # Match quality
                        "match_score": match.score,
                        "match_confidence": match.confidence,
                        "match_signal": match.signal,
                        "name_similarity": f"{match.name_similarity:.1f}",
                        "brand_match": match.brand_match_type,
                        "category_match": f"{match.category_match_percent:.1f}",
                        "price_diff_percent": f"{match.price_diff_percent:.1f}",
                        "price_diff_abs": f"{match.price_diff_abs:.2f}",
                        "match_reasons": "; ".join(match.reasons[:3]),
                        
                        # Decision
                        "decision": match.decision,
                        "decision_reason": match.decision_reason,
                        
                        # Existing match info
                        "existing_url": existing.competitor_url if existing else "",
                        "existing_reason": existing.reason if existing else "",
                        "existing_sku_mismatch": existing.sku_mismatch if existing else "",
                    }
                    
                    all_match_rows.append(row)
        
        # Calculate average scores
        for comp, stats in competitor_stats.items():
            if stats["total_matches"] > 0:
                stats["avg_score"] = round(stats["score_sum"] / stats["total_matches"], 2)
            del stats["score_sum"]
        
        # Sales impact
        products_with_matches = set()
        for pid in self.all_matches.keys():
            if any(self.all_matches[pid].values()):
                products_with_matches.add(pid)
        
        total_sales = sum(p.sales_90_days for p in self.system_products.values())
        matched_sales = sum(self.system_products[pid].sales_90_days for pid in products_with_matches)
        
        self.summary = {
            "timestamp": datetime.now().isoformat(),
            "products_evaluated": len(self.system_products),
            "products_with_matches": len(products_with_matches),
            "product_coverage_percent": round(len(products_with_matches) / len(self.system_products) * 100, 2),
            "total_matches_found": len(all_match_rows),
            "avg_matches_per_product": round(len(all_match_rows) / max(len(products_with_matches), 1), 2),
            "competitors_found": len(self.competitors),
            "decision_counts": dict(decision_counts),
            "competitor_statistics": competitor_stats,
            "sales_impact": {
                "total_sales_90_days": total_sales,
                "matched_sales_90_days": matched_sales,
                "coverage_percent": round((matched_sales / total_sales * 100) if total_sales > 0 else 0, 2)
            }
        }
        
        # Store for output
        self.all_match_rows = all_match_rows
        self.all_match_rows.sort(key=lambda r: (r["product_id"], r["competitor"]))
    
    def write_outputs(self) -> None:
        """Write all output files"""
        
        # Main report - ALL matches
        if hasattr(self, 'all_match_rows') and self.all_match_rows:
            headers = list(self.all_match_rows[0].keys())
            self.write_csv(self.output_dir / "01_all_matches.csv", self.all_match_rows, headers)
            
            # Also create a pivot-style report with one row per product showing all competitors
            self.create_competitor_matrix()
        
        # Reports by competitor
        if hasattr(self, 'all_match_rows') and self.all_match_rows:
            comp_dir = self.output_dir / "by_competitor"
            comp_dir.mkdir(exist_ok=True)
            
            for comp in self.competitors:
                comp_rows = [r for r in self.all_match_rows if r["competitor"] == comp]
                if comp_rows:
                    safe_name = re.sub(r'[^a-z0-9]+', '_', comp.lower())
                    filename = comp_dir / f"{safe_name}.csv"
                    headers = list(comp_rows[0].keys())
                    self.write_csv(filename, comp_rows, headers)
        
        # Decision-based reports
        if hasattr(self, 'all_match_rows') and self.all_match_rows:
            decision_map = {
                "NEW_MATCH": "02_new_matches.csv",
                "KEEP_EXISTING": "03_keep_existing.csv",
                "APPROVE_MATCH": "04_approve_matches.csv",
                "WRONG_MATCH": "05_wrong_matches.csv",
                "REVIEW": "06_review_needed.csv",
            }
            
            for decision, filename in decision_map.items():
                rows = [r for r in self.all_match_rows if r["decision"] == decision]
                if rows:
                    headers = list(rows[0].keys())
                    self.write_csv(self.output_dir / filename, rows, headers)
        
        # Summary
        self.write_json(self.output_dir / "summary.json", self.summary)
        
        # Create zip
        self.create_zip()
    
    def create_competitor_matrix(self) -> None:
        """Create a matrix report with one row per product showing all competitors"""
        matrix_rows = []
        
        # Group by product
        product_groups = defaultdict(list)
        for row in self.all_match_rows:
            product_groups[row["product_id"]].append(row)
        
        for pid, rows in product_groups.items():
            # Get base product info from first row
            base_row = rows[0]
            matrix_row = {
                "product_id": pid,
                "sku": base_row["sku"],
                "web_id": base_row["web_id"],
                "product_name": base_row["product_name"],
                "brand_label": base_row["brand_label"],
                "category": base_row["category"],
                "mpn": base_row["mpn"],
                "gtin": base_row["gtin"],
                "our_price": base_row["our_price"],
                "sales_90_days": base_row["sales_90_days"],
                "mfr_sales_30_days": base_row["mfr_sales_30_days"],
            }
            
            # Add competitor columns
            for row in rows:
                comp = row["competitor"]
                matrix_row[f"{comp}_url"] = row["competitor_url"]
                matrix_row[f"{comp}_price"] = row["competitor_price"]
                matrix_row[f"{comp}_score"] = row["match_score"]
                matrix_row[f"{comp}_confidence"] = row["match_confidence"]
                matrix_row[f"{comp}_decision"] = row["decision"]
            
            matrix_rows.append(matrix_row)
        
        if matrix_rows:
            headers = list(matrix_rows[0].keys())
            self.write_csv(self.output_dir / "competitor_matrix.csv", matrix_rows, headers)
    
    def write_csv(self, path: Path, rows: List[Dict[str, Any]], headers: List[str]) -> None:
        """Write rows to CSV"""
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            for row in rows:
                clean_row = {h: row.get(h, "") for h in headers}
                # Convert None to empty string
                clean_row = {k: v if v is not None else "" for k, v in clean_row.items()}
                writer.writerow(clean_row)
    
    def write_json(self, path: Path, data: Any) -> None:
        """Write JSON file"""
        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, default=str)
    
    def create_zip(self) -> None:
        """Create zip archive"""
        zip_path = self.output_dir / "reconciliation_report.zip"
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for file in self.output_dir.glob("*.csv"):
                zf.write(file, arcname=file.name)
            for file in (self.output_dir / "by_competitor").glob("*.csv"):
                zf.write(file, arcname=f"by_competitor/{file.name}")


# ============================================================================
# Command Line Interface
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Multi-Competitor Reconciliation - Matches each product with ALL relevant competitors"
    )
    
    parser.add_argument("scrape_file", help="Scraped data CSV with all competitors")
    parser.add_argument("system_file", help="System product data CSV")
    parser.add_argument("cm_file", help="Existing competitor mappings CSV")
    
    parser.add_argument("--output-dir", "-o", default="reconcile_output",
                       help="Output directory")
    parser.add_argument("--limit", type=int, help="Limit number of products to process")
    
    args = parser.parse_args()
    
    pipeline = MultiCompetitorPipeline(
        scrape_file=Path(args.scrape_file),
        system_file=Path(args.system_file),
        cm_file=Path(args.cm_file),
        output_dir=Path(args.output_dir),
        limit=args.limit
    )
    
    summary = pipeline.run()
    
    print("\n" + "="*60)
    print("FINAL SUMMARY")
    print("="*60)
    print(f"Products Evaluated:     {summary['products_evaluated']}")
    print(f"Products with Matches:  {summary['products_with_matches']} ({summary['product_coverage_percent']}%)")
    print(f"Total Matches Found:    {summary['total_matches_found']}")
    print(f"Avg Matches per Product: {summary['avg_matches_per_product']}")
    print(f"Competitors Found:      {summary['competitors_found']}")
    
    print("\nDecision Breakdown:")
    for decision, count in summary.get("decision_counts", {}).items():
        print(f"  {decision:20}: {count}")
    
    if "sales_impact" in summary:
        print("\nSales Impact:")
        sales = summary["sales_impact"]
        print(f"  Total Sales (90 days)   : {sales['total_sales_90_days']}")
        print(f"  Matched Sales (90 days) : {sales['matched_sales_90_days']}")
        print(f"  Coverage                : {sales['coverage_percent']}%")
    
    print("="*60)
    print(f"Output: {pipeline.output_dir}")
    print(f"Zip:    {pipeline.output_dir / 'reconciliation_report.zip'}")


if __name__ == "__main__":
    main()