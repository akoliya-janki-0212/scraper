import os
import csv
import time
import sys
import gc
import threading
import requests
import re
import json
from datetime import datetime, timezone
from xml.etree import ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
import random
import cloudscraper
from curl_cffi import requests as cc_requests
from typing import Optional, Tuple

# ================= ENV =================

CURR_URL = os.getenv("CURR_URL", "https://www.homedepot.com").rstrip("/")
SITEMAP_INDEX = os.getenv("SITEMAP_INDEX", "").strip()
GRAPHQL_URL = os.getenv("GRAPHQL_URL", "").strip()
STORE_ID = os.getenv("STORE_ID", "1710")
ZIP_CODE = os.getenv("ZIP_CODE", "96913")
REQUEST_DELAY_BASE = float(os.getenv("REQUEST_DELAY", "1.0"))

SITEMAP_OFFSET = int(os.getenv("SITEMAP_OFFSET", "0"))
MAX_SITEMAPS = int(os.getenv("MAX_SITEMAPS", "0"))
MAX_URLS_PER_SITEMAP = int(os.getenv("MAX_URLS_PER_SITEMAP", "0"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "4"))
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "1.0"))
SITEMAP_NAME_MATCH = os.getenv("SITEMAP_NAME_MATCH", "PIPs.xml").strip().lower()

OUTPUT_CSV = f"products_chunk_{SITEMAP_OFFSET}.csv"
SCRAPED_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

# ================= LOGGER =================

def log(msg: str, level: str = "INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sys.stderr.write(f"[{timestamp}] [{level}] {msg}\n")
    sys.stderr.flush()


class RequestManager:
    def __init__(self):
        # Initialize cloudscraper with browser-like headers
        self.scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'mobile': False
            },
            delay=10  # Cloudflare challenge delay
        )
        
        # Enhanced headers for both scrapers
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9",
            # "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
            "Referer": CURR_URL + "/",
        }
        
        self.scraper.headers.update(self.headers)
        self.retry_delays = [1, 2, 4, 8, 16]  # Exponential backoff
        self.request_count = 0
        self.last_request_time = 0
        
    def _respect_rate_limit(self, crawl_delay=None):
        """Add random delay between requests"""
        current_time = time.time()
        if self.request_count > 0:
            elapsed = current_time - self.last_request_time
            # Use crawl_delay from robots.txt if provided, otherwise use base delay
            base_delay = crawl_delay if crawl_delay else REQUEST_DELAY_BASE
            min_delay = base_delay * 0.8  # 80% of base delay
            max_delay = base_delay * 1.5  # 150% of base delay
            target_delay = random.uniform(min_delay, max_delay)
            
            if elapsed < target_delay:
                sleep_time = target_delay - elapsed
                time.sleep(sleep_time)
        
        self.last_request_time = time.time()
        self.request_count += 1
        
        # Occasionally longer pause
        if self.request_count % 20 == 0:
            long_pause = random.uniform(8, 15)
            log(f"Taking longer pause after {self.request_count} requests: {long_pause:.1f}s")
            time.sleep(long_pause)
    
    def _fetch_with_cloudscraper(self, url: str, crawl_delay=None) -> Optional[Tuple[str, int]]:
        """Use cloudscraper for Cloudflare-protected pages"""
        try:
            self._respect_rate_limit(crawl_delay)
            response = self.scraper.get(url, timeout=45)
            if response.status_code == 200:
                return response.text, response.status_code
            return None, response.status_code
        except Exception as e:
            log(f"Cloudscraper error for {url}: {e}")
            return None, 0
    
    def _fetch_with_curl_cffi(self, url: str, crawl_delay=None) -> Optional[Tuple[str, int]]:
        """Use curl_cffi for JavaScript-heavy pages"""
        try:
            self._respect_rate_limit(crawl_delay)
            # Use impersonate to mimic real browser TLS fingerprint
            response = cc_requests.get(
                url, 
                headers=self.headers,
                timeout=45,
                impersonate="chrome110"  # Mimic Chrome 110
            )
            if response.status_code == 200:
                return response.text, response.status_code
            return None, response.status_code
        except Exception as e:
            log(f"Curl_cffi error for {url}: {e}")
            return None, 0
    
    def fetch(self, url: str, retry_count: int = 0, crawl_delay=None) -> Optional[str]:
        """Intelligent fetching with fallback strategies"""
        if retry_count >= len(self.retry_delays):
            log(f"Max retries exceeded for {url}")
            return None
        
        # Choose strategy based on retry count
        if retry_count == 0:
            # First try: cloudscraper (best for Cloudflare)
            content, status = self._fetch_with_cloudscraper(url, crawl_delay)
        elif retry_count % 2 == 1:
            # Odd retries: curl_cffi
            content, status = self._fetch_with_curl_cffi(url, crawl_delay)
        else:
            # Even retries: cloudscraper again
            content, status = self._fetch_with_cloudscraper(url, crawl_delay)
        
        if content:
            return content
        
        # Handle specific status codes
        if status in [403, 429, 503]:
            delay = self.retry_delays[retry_count] + random.uniform(0, 1)
            log(f"HTTP {status} for {url}, retry {retry_count+1} in {delay:.1f}s")
            time.sleep(delay)
            return self.fetch(url, retry_count + 1, crawl_delay)
        elif status == 404:
            log(f"URL not found: {url}")
            return None
        
        # For other errors, retry with delay
        if status != 200:
            delay = self.retry_delays[retry_count]
            log(f"Retry {retry_count+1} for {url} in {delay}s")
            time.sleep(delay)
            return self.fetch(url, retry_count + 1, crawl_delay)
        
        return None

# Initialize global request manager
request_manager = RequestManager()
# ================= HTTP =================

session = requests.Session()
session.headers.update(
    {
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Content-Type": "application/json",
        "Origin": CURR_URL,
        "Referer": CURR_URL + "/",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    }
)


def http_get(url: str, crawl_delay=None) -> Optional[str]:
    """Wrapper for request manager"""
    return request_manager.fetch(url, crawl_delay=crawl_delay)

def load_xml(url: str, crawl_delay=None) -> Optional[ET.Element]:
    data = http_get(url, crawl_delay)
    if not data:
        return None
    try:
        return ET.fromstring(data)
    except ET.ParseError as e:
        log(f"XML parse error for {url}: {e}")
        return None
    

def get_sitemap_from_robots_txt() -> Optional[str]:
    try:
        robots_url = f"{CURR_URL}/robots.txt"
        response = http_get(robots_url)
        for line in response.split("\n"):
            if line.lower().startswith("sitemap:"):
                sitemap_url = line.split(":", 1)[1].strip()
                if sitemap_url:
                    log(f"Extracted sitemap index from robots.txt: {sitemap_url}")
                    return sitemap_url
    except Exception as e:
        log(f"Error fetching robots.txt: {e}", "WARNING")
    return None


# def http_get(url: str) -> Optional[str]:
#     for attempt in range(3):
#         try:
#             r = session.get(url, timeout=30)
#             if r.status_code == 200:
#                 return r.text
#             log(f"Status {r.status_code} for {url}", "WARNING")
#         except Exception as e:
#             log(f"GET attempt {attempt + 1} failed for {url}: {e}", "WARNING")
#         time.sleep(1 + attempt)
#     return None


# def load_xml(url: str) -> Optional[ET.Element]:
#     data = http_get(url)
#     if not data:
#         return None
#     try:
#         return ET.fromstring(data)
#     except ET.ParseError as e:
#         log(f"XML parsing failed for {url}: {e}", "ERROR")
#         return None


# ================= GRAPHQL =================

GRAPHQL_QUERY = """
query productClientOnlyProduct(
  $itemId: String!,
  $storeId: String,
  $zipCode: String,
  $skipSpecificationGroup: Boolean = false,
  $isBrandPricingPolicyCompliant: Boolean = false
) {
  product(itemId: $itemId) {
    itemId
    identifiers {
      canonicalUrl
      brandName
      itemId
      modelNumber
      productLabel
      storeSkuNumber
      upcGtin13
      upc
    }
    specificationGroup @skip(if: $skipSpecificationGroup) {
      specTitle
      specifications {
        specName
        specValue
      }
    }
    availabilityType {
      status
      type
      buyable
      discontinued
    }
    details {
      highlights
      description
    }
    media {
      images {
        url
      }
    }
    pricing(storeId: $storeId, isBrandPricingPolicyCompliant: $isBrandPricingPolicyCompliant) {
      value
      original
      unitOfMeasure
    }
    taxonomy {
      breadCrumbs {
        label
        url
      }
    }
  }
}
"""


def extract_item_id_from_url(product_url: str) -> Optional[str]:
    # Home Depot PDP pattern usually ends with /<numeric_item_id>
    m = re.search(r"/(\d+)(?:[/?#]|$)", product_url)
    return m.group(1) if m else None


def fetch_product_graphql(product_url: str, item_id: str) -> Optional[dict]:
    parsed = urlparse(product_url)
    path = parsed.path if parsed.path else "/"

    payload = {
        "operationName": "productClientOnlyProduct",
        "variables": {
            "itemId": item_id,
            "storeId": STORE_ID,
            "zipCode": ZIP_CODE,
            "skipSpecificationGroup": False,
            "skipPaintDetails": True,
            "skipInstallServices": True,
            "skipKPF": False,
            "skipSubscribeAndSave": False,
            "skipFavoriteCount": False,
            "isBrandPricingPolicyCompliant": False,
        },
        "query": GRAPHQL_QUERY,
    }

    x_user_id = (
        session.cookies.get("thda.u")
        or session.cookies.get("x-user-id")
        or ""
    )
    x_api_cookies = json.dumps({"x-user-id": x_user_id}) if x_user_id else "{}"

    headers = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/json",
        "origin": CURR_URL,
        "referer": CURR_URL + "/",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "x-api-cookies": x_api_cookies,
        "x-current-url": path,
        "x-experience-name": "fusion-gm-pip-desktop",
        "x-debug": "false",
        "x-hd-dc": "beta",
    }

    def parse_graphql_payload(resp_text: str, status_code: int) -> Optional[dict]:
        try:
            data = json.loads(resp_text)
        except Exception:
            return None

        # Accept valid GraphQL payload even when upstream replies 206.
        if isinstance(data, dict) and data.get("data", {}).get("product"):
            if status_code == 206:
                log(f"GraphQL status 206 for item {item_id}, accepted (data.product present)", "DEBUG")
            if data.get("errors"):
                log(f"GraphQL errors for item {item_id}: {data['errors']}", "WARNING")
            return data
        return None

    for attempt in range(5):
        try:
            r = session.post(GRAPHQL_URL, headers=headers, json=payload, timeout=35)
            if r.status_code in (200, 206):
                parsed_payload = parse_graphql_payload(r.text, r.status_code)
                if parsed_payload is not None:
                    return parsed_payload

            # Fallback: impersonated TLS/client fingerprint via curl_cffi.
            try:
                cr = cc_requests.post(
                    GRAPHQL_URL,
                    headers=headers,
                    json=payload,
                    timeout=35,
                    impersonate="chrome120",
                )
                if cr.status_code in (200, 206):
                    parsed_payload = parse_graphql_payload(cr.text, cr.status_code)
                    if parsed_payload is not None:
                        return parsed_payload
                log(f"GraphQL fallback status {cr.status_code} for item {item_id}", "WARNING")
            except Exception as ce:
                log(f"GraphQL fallback attempt {attempt + 1} failed for item {item_id}: {ce}", "WARNING")

            log(f"GraphQL status {r.status_code} for item {item_id}", "WARNING")
        except Exception as e:
            log(f"GraphQL attempt {attempt + 1} failed for item {item_id}: {e}", "WARNING")
        time.sleep(1 + attempt)
    return None


def extract_product_data(gql_data: dict, product_url: str, item_id: str) -> Optional[dict]:
    product = (gql_data or {}).get("data", {}).get("product")
    if not product:
        return None

    identifiers = product.get("identifiers") or {}
    pricing = product.get("pricing") or {}
    taxonomy = product.get("taxonomy") or {}
    breadcrumbs = taxonomy.get("breadCrumbs") or []
    availability = product.get("availabilityType") or {}
    images = (product.get("media") or {}).get("images") or []

    brand = identifiers.get("brandName", "")
    name = identifiers.get("productLabel", "")
    sku = identifiers.get("storeSkuNumber", "")
    mpn = identifiers.get("modelNumber", "")
    gtin = identifiers.get("upcGtin13", "") or identifiers.get("upc", "") or ""
    price = pricing.get("value", "")
    main_image = images[0].get("url", "") if images else ""

    category = ""
    category_url = ""
    if breadcrumbs:
        non_home = [b for b in breadcrumbs if b.get("label", "").strip().lower() != "home"]
        if non_home:
            category = non_home[-1].get("label", "")
            category_url = non_home[-1].get("url", "")

    status_raw = str(availability.get("status", ""))
    status = "SELLABLE" if "active" in status_raw.lower() or availability.get("buyable") else "OUT_OF_STOCK"

    spec_groups = product.get("specificationGroup") or []
    additional_data = {
        "description": (product.get("details") or {}).get("description", ""),
        "highlights": (product.get("details") or {}).get("highlights", []),
        "original_price": pricing.get("original", ""),
        "unit_of_measure": pricing.get("unitOfMeasure", ""),
        "canonical_url": identifiers.get("canonicalUrl", ""),
        "specification_group": spec_groups,
    }

    return {
        "product_id": item_id,
        "variation_id": sku or item_id,
        "category": category,
        "category_url": category_url,
        "brand": brand,
        "name": name,
        "sku": sku,
        "mpn": mpn,
        "gtin": gtin,
        "price": price,
        "main_image": main_image,
        "quantity": 1,
        "group_attr_1": "",
        "group_attr_2": "",
        "status": status,
        "additional_data": json.dumps(additional_data, ensure_ascii=False),
        "product_url": product_url,
    }


# ================= PRODUCT PROCESSING =================

csv_lock = threading.Lock()
seen_lock = threading.Lock()
stats_lock = threading.Lock()


def process_product_data(product_url: str, writer, seen: set, stats: dict):
    item_id = extract_item_id_from_url(product_url)
    if not item_id:
        with stats_lock:
            stats["errors"] += 1
        log(f"Could not extract item ID from URL: {product_url}", "WARNING")
        return

    key = f"{item_id}|{product_url}"
    with seen_lock:
        if key in seen:
            return
        seen.add(key)

    gql_data = fetch_product_graphql(product_url, item_id)
    if not gql_data:
        with stats_lock:
            stats["errors"] += 1
        return

    product_info = extract_product_data(gql_data, product_url, item_id)
    if not product_info:
        with stats_lock:
            stats["errors"] += 1
        log(f"No product data for item {item_id}", "WARNING")
        return

    row = [
        product_info["product_url"],
        product_info["product_id"],
        product_info["variation_id"],
        product_info["category"],
        product_info["category_url"],
        product_info["brand"],
        product_info["name"],
        product_info["sku"],
        product_info["mpn"],
        product_info["gtin"],
        product_info["price"],
        product_info["main_image"],
        product_info["quantity"],
        product_info["group_attr_1"],
        product_info["group_attr_2"],
        product_info["status"],
        product_info["additional_data"],
        SCRAPED_DATE,
    ]

    with csv_lock:
        writer.writerow(row)

    with stats_lock:
        stats["products_fetched"] += 1
        stats["urls_processed"] += 1

    if REQUEST_DELAY > 0:
        time.sleep(REQUEST_DELAY)


# ================= MAIN =================

def main():
    global GRAPHQL_URL
    if not GRAPHQL_URL:
        GRAPHQL_URL = "https://apionline.homedepot.com/federation-gateway/graphql?opname=productClientOnlyProduct"

    sitemap = "https://www.homedepot.com/sitemap/P/PIPs.xml"

    log("=" * 60)
    log("Home Depot GraphQL Scraper")
    log(f"Timestamp: {SCRAPED_DATE}")
    log(f"Base URL: {CURR_URL}")
    log(f"GraphQL URL: {GRAPHQL_URL}")
    log(f"Sitemap Index: {sitemap}")
    log(f"Sitemap Offset: {SITEMAP_OFFSET}")
    log(f"Max Sitemaps: {MAX_SITEMAPS if MAX_SITEMAPS > 0 else 'All'}")
    log(f"Max URLs per Sitemap: {MAX_URLS_PER_SITEMAP if MAX_URLS_PER_SITEMAP > 0 else 'All'}")
    log(f"Max Workers: {MAX_WORKERS}")
    log(f"Request Delay: {REQUEST_DELAY}s")
    log("=" * 60)

    index = load_xml(sitemap)
    if index is None:
        log("Failed to load sitemap index", "ERROR")
        sys.exit(1)

    ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    all_locs = []
    for path in [".//ns:sitemap/ns:loc", ".//sitemap/loc", ".//loc"]:
        elements = index.findall(path, ns) if "ns:" in path else index.findall(path)
        if elements:
            all_locs = [e.text.strip() for e in elements if e.text]
            if all_locs:
                break

    if not all_locs:
        log("No sitemap locations found in index", "ERROR")
        sys.exit(1)

    # Primary selection: only sitemap URLs containing PIPs.xml (case-insensitive)
    sitemaps = [
        loc for loc in all_locs
        if loc.lower().endswith(".xml") and SITEMAP_NAME_MATCH in loc.lower()
    ]

    # Fallback: use any XML sitemap URLs if no PIPs.xml matches are present
    if not sitemaps:
        sitemaps = [loc for loc in all_locs if loc.lower().endswith(".xml")]
        # log(
        #     f"No sitemap matched '{SITEMAP_NAME_MATCH}'. Falling back to all XML sitemap entries: {len(sitemaps)}",
        #     "WARNING",
        # )
    else:
        log(f"Matched {len(sitemaps)} sitemap URLs using pattern '{SITEMAP_NAME_MATCH}'")

    if SITEMAP_OFFSET >= len(sitemaps):
        log(f"Offset {SITEMAP_OFFSET} exceeds total sitemaps ({len(sitemaps)})", "WARNING")
        sys.exit(0)

    end_index = SITEMAP_OFFSET + MAX_SITEMAPS if MAX_SITEMAPS > 0 else len(sitemaps)
    sitemaps_to_process = sitemaps[SITEMAP_OFFSET:end_index]

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "Ref Product URL",
                "Ref Product ID",
                "Ref Varient ID",
                "Ref Category",
                "Ref Category URL",
                "Ref Brand Name",
                "Ref Product Name",
                "Ref SKU",
                "Ref MPN",
                "Ref GTIN",
                "Ref Price",
                "Ref Main Image",
                "Ref Quantity",
                "Ref Group Attr 1",
                "Ref Group Attr 2",
                "Ref Status",
                "Additional Product Data",
                "Date Scrapped",
            ]
        )

        seen = set()
        stats = {
            "sitemaps_processed": 0,
            "urls_processed": 0,
            "products_fetched": 0,
            "errors": 0,
        }

        for sitemap_url in sitemaps_to_process:
            stats["sitemaps_processed"] += 1
            log(f"Processing sitemap {stats['sitemaps_processed']}/{len(sitemaps_to_process)}: {sitemap_url}")

            xml = load_xml(sitemap_url)
            if not xml:
                log(f"Failed to load sitemap: {sitemap_url}", "ERROR")
                continue

            urls = []
            for path in [".//ns:url/ns:loc", ".//url/loc", ".//loc"]:
                elements = xml.findall(path, ns) if "ns:" in path else xml.findall(path)
                if elements:
                    urls = [
                        e.text.strip()
                        for e in elements
                        if e.text
                        and "/p/" in e.text
                        and extract_item_id_from_url(e.text)
                    ]
                    if urls:
                        break

            if not urls:
                log(f"No product URLs found in sitemap: {sitemap_url}", "WARNING")
                continue

            if MAX_URLS_PER_SITEMAP > 0:
                original_count = len(urls)
                urls = urls[:MAX_URLS_PER_SITEMAP]
                log(f"Limited to {len(urls)} out of {original_count} URLs")
            else:
                log(f"Found {len(urls)} product URLs in this sitemap")

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [executor.submit(process_product_data, url, writer, seen, stats) for url in urls]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        log(f"Error in thread execution: {e}", "ERROR")
                        with stats_lock:
                            stats["errors"] += 1

            gc.collect()

    log("=" * 60)
    log("SCRAPING STATISTICS")
    log("=" * 60)
    log(f"Sitemaps processed: {stats['sitemaps_processed']}")
    log(f"URLs processed: {stats['urls_processed']}")
    log(f"Products successfully fetched: {stats['products_fetched']}")
    log(f"Errors encountered: {stats['errors']}")
    if stats["urls_processed"] > 0:
        success_rate = (stats["products_fetched"] / stats["urls_processed"]) * 100
        log(f"Success rate: {success_rate:.1f}%")
    log("=" * 60)
    log(f"Completed: {OUTPUT_CSV}")
    log("=" * 60)


if __name__ == "__main__":
    import urllib3

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    if not CURR_URL:
        log("Error: CURR_URL environment variable is required", "ERROR")
        sys.exit(1)

    main()
