import os
import csv
import time
import sys
import gc
import threading
import requests
import re
import json
from typing import Optional, List, Dict
from datetime import datetime, timezone
from xml.etree import ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= ENV =================

CURR_URL = os.getenv("CURR_URL", "https://www.overstock.com").rstrip("/")
SITEMAP_INDEX = os.getenv("SITEMAP_INDEX", "https://api.overstock.com/sitemaps/overstock-v3/us/sitemap.xml")
API_BASE_URL = os.getenv("API_BASE_URL", "https://www.overstock.com/api/product")
SITEMAP_OFFSET = int(os.getenv("SITEMAP_OFFSET", "0"))
MAX_SITEMAPS = int(os.getenv("MAX_SITEMAPS", "0"))
MAX_URLS_PER_SITEMAP = int(os.getenv("MAX_URLS_PER_SITEMAP", "0"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "4"))
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "1.0"))

OUTPUT_CSV = f"products_chunk_{SITEMAP_OFFSET}.csv"
SCRAPED_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

# ================= LOGGER =================

def log(msg: str, level: str = "INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sys.stderr.write(f"[{timestamp}] [{level}] {msg}\n")
    sys.stderr.flush()

# ================= HTTP SESSION =================

session = requests.Session()

def http_get(url: str, is_json: bool = False) -> Optional[str]:
    """HTTP GET request with different headers for sitemap vs API requests"""
    headers = {}
    
    if is_json:
        # Headers for API/JSON requests only
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": f"{CURR_URL}/",
            "X-Requested-With": "XMLHttpRequest",
            "Connection": "keep-alive",
        }
    else:
        # Minimal headers for sitemap requests
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    
    for attempt in range(3):
        try:
            r = session.get(url, headers=headers, timeout=15, verify=True)
            if r.status_code == 200:
                log(f"Success fetching {url}", "DEBUG")
                return r.text
            else:
                log(f"Status {r.status_code} for {url}", "WARNING")
                if r.status_code == 429:  # Rate limited
                    time.sleep(5)
        except requests.exceptions.Timeout:
            log(f"Timeout on attempt {attempt+1} for {url}", "WARNING")
            time.sleep(2)
        except Exception as e:
            log(f"Attempt {attempt+1} failed for {url}: {type(e).__name__}", "WARNING")
            time.sleep(1)
    return None

def fetch_json(url: str) -> Optional[dict]:
    """Fetch JSON data with proper headers"""
    try:
        # Headers specifically for JSON/API requests
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "en-US,en;q=0.9",
            # "Accept-Encoding": "gzip, deflate, br",
            "Referer": f"{CURR_URL}/",
            "X-Requested-With": "XMLHttpRequest",
            "Connection": "keep-alive",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
        }
        
        r = session.get(url, headers=headers, timeout=15, verify=True)
        # print(url);
        # print(r.content);
        if r.status_code == 200:
            return r.json()
        else:
            log(f"JSON fetch failed: {r.status_code} for {url}", "WARNING")
            return None
    except json.JSONDecodeError as e:
        log(f"JSON decode error for {url}: {e}", "ERROR")
        return None
    except Exception as e:
        log(f"Error fetching JSON from {url}: {e}", "ERROR")
        return None

# ================= SITEMAP PROCESSING =================

def load_xml(url: str) -> Optional[ET.Element]:
    """Load XML with minimal headers (no API headers)"""
    data = http_get(url, is_json=False)  # is_json=False for sitemap
    if not data:
        log(f"Failed to load XML from {url}", "ERROR")
        return None
    try:
        # Clean XML if needed
        if "<?xml" not in data[:100]:
            data = '<?xml version="1.0" encoding="UTF-8"?>\n' + data
        return ET.fromstring(data)
    except ET.ParseError as e:
        log(f"XML parsing failed for {url}: {e}", "ERROR")
        # Try to extract URLs with regex
        try:
            # Create a dummy element
            root = ET.Element("urlset")
            urls = re.findall(r'<loc>(https?://[^<]+)</loc>', data)
            for url_text in urls:
                url_elem = ET.SubElement(root, "url")
                loc_elem = ET.SubElement(url_elem, "loc")
                loc_elem.text = url_text
            return root
        except Exception as e2:
            log(f"Regex extraction also failed: {e2}", "ERROR")
            return None

def extract_product_id(product_url: str) -> Optional[str]:
    """Extract product ID from Overstock URL"""
    patterns = [
        r'/(\d+)/product\.html',      
        r'/product/(\d+)/',
        r'/catalog/(\d+)/',
        r'/[\w-]+/(\d+)\.html',
        r'/(\d+)\.html',
        r'[?&]IID=(\d+)',
    ]

    for pattern in patterns:
        match = re.search(pattern, product_url)
        if match:
            product_id = match.group(1)
            log(f"Extracted product ID {product_id} from {product_url}", "DEBUG")
            return product_id

    log(f"No product ID found in URL: {product_url}", "WARNING")
    return None

# ================= PRODUCT PROCESSING =================

csv_lock = threading.Lock()

def normalize_image_url(url: str) -> str:
    """Normalize image URL for Overstock"""
    if not url:
        return ""
    
    if url.startswith("//"):
        return "https:" + url
    elif url.startswith("/"):
        return f"{CURR_URL}{url}"
    elif not url.startswith("http"):
        return f"https://ak1.ostkcdn.com{url}" if 'ostkcdn.com' not in url else f"https://{url}"
    
    return url

from typing import Dict

def extract_overstock_data(product_data: dict) -> Dict:
    """
    Extract data from Overstock product API response
    (schema-aligned to current Overstock JSON)
    """
    try:
        product = product_data  # API already returns product root

        # ---------- Basic ----------
        product_id = str(product.get('productId', ''))
        name = product.get('name', '').strip()

        brand_obj = product.get('brand', {})
        brand = brand_obj.get('name', '') if isinstance(brand_obj, dict) else str(brand_obj)

        # ---------- SKU / MPN ----------
        details = product.get('details', {})
        sku = details.get('sku', '')

        specs = product.get('specifications', {})
        mpn = specs.get('Model Number', [''])[0]

        # ---------- Category ----------
        breadcrumbs = product.get('breadcrumbs', [])
        category = ''
        category_url = ''

        if breadcrumbs:
            last = breadcrumbs[-1]
            category = last.get('label', '')
            url = last.get('url', '')
            category_url = f"{CURR_URL}{url}" if url.startswith('/') else url

        # ---------- Images ----------
        image_data = product.get('imageData', {})
        main_image = image_data.get('productImageUrl', '')

        # ---------- Variations (pick first SELLABLE) ----------
        variations = product.get('variations', [])

        variation_id = ''
        quantity = 0
        status = 'OUT_OF_STOCK'
        price = ''
        group_attr_1 = ''
        group_attr_2 = ''

        selected_variation = None
        for v in variations:
            if v.get('status') == 'SELLABLE':
                selected_variation = v
                break

        if not selected_variation and variations:
            selected_variation = variations[0]

        if selected_variation:
            variation_id = selected_variation.get('variationId')

            inventory = selected_variation.get('inventory', {})
            quantity = inventory.get('quantityAvailable', 0)

            status = selected_variation.get('status', '')

            prices = selected_variation.get('prices', {})
            sale = prices.get('salePrice', {})
            base = prices.get('basePrice', {})

            price = (
                sale.get('amount')
                or base.get('amount')
                or ''
            )

            # Variation name usually carries size
            group_attr_1 = selected_variation.get('description', '')
            group_attr_2 = selected_variation.get('fullSku', '')

        return {
            'product_id': product_id,
            'name': name,
            'brand': brand,
            'price': price,
            'main_image': main_image,
            'sku': sku,
            'mpn': mpn,
            'category': category,
            'category_url': category_url,
            'quantity': quantity,
            'status': status,
            'variation_id': variation_id,
            'group_attr_1': group_attr_1,
            'group_attr_2': group_attr_2
        }

    except Exception as e:
        log(f"Error extracting Overstock data: {e}", "ERROR")
        return {}

def process_product_data(product_url: str, writer, seen: set, stats: dict):
    """Process a single Overstock product URL"""
    if product_url in seen:
        return
    seen.add(product_url)
    
    log(f"Processing product URL: {product_url}", "DEBUG")
    
    # Extract product ID
    product_id = extract_product_id(product_url)
    if not product_id:
        stats['errors'] += 1
        log(f"No product ID found for URL: {product_url}", "ERROR")
        return
    
    # Fetch product data from API - Overstock may have different endpoints
    # Try multiple possible API endpoints
    api_endpoints = [
        f"https://www.overstock.com/api/product/{product_id}",
        # f"https://www.overstock.com/api/products/{product_id}",
        # f"https://www.overstock.com/api/catalog/product/{product_id}",
    ]
    
    data = None
    for api_url in api_endpoints:
        log(f"Trying API endpoint: {api_url}", "DEBUG")
        data = fetch_json(api_url)  # This uses JSON-specific headers
        if data:
            break
        time.sleep(0.5)
    
    if not data:
        # Try direct product page scraping as fallback
        log(f"API failed, trying direct page for {product_id}", "WARNING")
        page_content = http_get(product_url, is_json=False)  # Minimal headers for HTML page
        if page_content:
            # Look for JSON-LD or product data in page
            json_ld_pattern = r'<script type="application/ld\+json">(.*?)</script>'
            matches = re.findall(json_ld_pattern, page_content, re.DOTALL)
            if matches:
                try:
                    data = json.loads(matches[0])
                except:
                    pass
    
    if not data:
        stats['errors'] += 1
        log(f"No data found for product {product_id}", "ERROR")
        return
    
    # Extract data from response
    product_info = extract_overstock_data(data)
    if not product_info.get('product_id'):
        stats['errors'] += 1
        log(f"Invalid data for product {product_id}", "ERROR")
        return
    
    try:
        # Prepare row data
        row = [
            product_url + "?option=" + str(product_info['variation_id']),
            product_info['product_id'],  # Ref Product ID
            product_info['variation_id'],  # Ref Varient ID
            product_info['category'],  # Ref Category
            product_info['category_url'],  # Ref Category URL
            product_info['brand'],  # Ref Brand Name
            product_info['name'],  # Ref Product Name
            product_info['sku'],  # Ref SKU
            product_info['mpn'],  # Ref MPN
            '',  # Ref GTIN (empty for now)
            product_info['price'],  # Ref Price
            normalize_image_url(product_info['main_image']),  # Ref Main Image
            product_info['quantity'],  # Ref Quantity
            product_info['group_attr_1'],  # Ref Group Attr 1
            product_info['group_attr_2'],  # Ref Group Attr 2
            product_info['status'],  # Ref Status
            SCRAPED_DATE  # Date Scrapped
        ]
        
        with csv_lock:
            writer.writerow(row)
        
        stats['products_fetched'] += 1
        log(f"Fetched product {product_info['product_id']}: {product_info['name'][:50]}...", "INFO")
        
    except Exception as e:
        log(f"Error creating row for product {product_id}: {e}", "ERROR")
        stats['errors'] += 1
    
    # Respect request delay
    time.sleep(REQUEST_DELAY)
    stats['urls_processed'] += 1

# ================= MAIN =================

def main():
    log("=" * 60)
    log("Overstock Parallel Scraper")
    log(f"Timestamp: {SCRAPED_DATE}")
    log(f"Base URL: {CURR_URL}")
    log(f"API Base URL: {API_BASE_URL}")
    log(f"Sitemap Index: {SITEMAP_INDEX}")
    log(f"Sitemap Offset: {SITEMAP_OFFSET}")
    log(f"Max Sitemaps: {MAX_SITEMAPS if MAX_SITEMAPS > 0 else 'All'}")
    log(f"Max URLs per Sitemap: {MAX_URLS_PER_SITEMAP if MAX_URLS_PER_SITEMAP > 0 else 'All'}")
    log(f"Max Workers: {MAX_WORKERS}")
    log(f"Request Delay: {REQUEST_DELAY}s")
    log("=" * 60)
    
    # Load sitemap index - NO HEADERS for sitemap
    log(f"Loading sitemap index from {SITEMAP_INDEX}")
    index = load_xml(SITEMAP_INDEX)
    if index is None:
        log("Failed to load sitemap index", "ERROR")
        sys.exit(1)
    
    # Extract sitemap URLs
    ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    sitemaps = []
    
    # Try different XML structures
    for path in [".//ns:sitemap/ns:loc", ".//sitemap/loc", ".//loc"]:
        elements = index.findall(path, ns) if "ns:" in path else index.findall(path)
        if elements:
            sitemaps = [e.text.strip() for e in elements if e.text]
            break
    
    # If still no sitemaps, try regex
    if not sitemaps:
        log("No sitemaps found with XML parsing, trying regex", "WARNING")
        # Try common Overstock sitemap patterns
        sitemaps = [
            "https://www.overstock.com/sitemap_products_1.xml",
            "https://www.overstock.com/sitemap_products_2.xml",
            "https://www.overstock.com/sitemap.xml",
        ]
    
    # Apply offset and limit
    if SITEMAP_OFFSET >= len(sitemaps):
        log(f"Offset {SITEMAP_OFFSET} exceeds total sitemaps ({len(sitemaps)})", "WARNING")
        sys.exit(0)
    
    end_index = SITEMAP_OFFSET + MAX_SITEMAPS if MAX_SITEMAPS > 0 else len(sitemaps)
    sitemaps_to_process = sitemaps[SITEMAP_OFFSET:end_index]
    
    log(f"Total sitemaps found: {len(sitemaps)}")
    log(f"Sitemaps to process: {len(sitemaps_to_process)}")
    
    # Initialize CSV
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        
        # Write header
        writer.writerow([
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
            "Date Scrapped"
        ])
        
        # Initialize tracking
        seen = set()
        stats = {
            'sitemaps_processed': 0,
            'urls_processed': 0,
            'products_fetched': 0,
            'errors': 0
        }
        
        # Process each sitemap
        for sitemap_url in sitemaps_to_process:
            stats['sitemaps_processed'] += 1
            log(f"Processing sitemap {stats['sitemaps_processed']}/{len(sitemaps_to_process)}: {sitemap_url}")
            
            # Load product sitemap - NO HEADERS for sitemap
            xml = load_xml(sitemap_url)
            if not xml:
                log(f"Failed to load sitemap: {sitemap_url}", "ERROR")
                continue
            
            # Extract product URLs - Overstock product URLs typically contain /product/ or /catalog/
            urls = []
            for path in [".//ns:url/ns:loc", ".//url/loc", ".//loc"]:
                elements = xml.findall(path, ns) if "ns:" in path else xml.findall(path)
                if elements:
                    urls = [
                        e.text.strip()
                        for e in elements
                        if e.text
                        and (
                            '/product/' in e.text
                            or '/product.html' in e.text
                            or '/catalog/' in e.text
                            or 'IID=' in e.text
                        )
                    ]
                    if urls:
                        break
            
            if not urls:
                log(f"No product URLs found in sitemap: {sitemap_url}", "WARNING")
                continue
            
            # Apply URL limit
            if MAX_URLS_PER_SITEMAP > 0:
                original_count = len(urls)
                urls = urls[:MAX_URLS_PER_SITEMAP]
                log(f"Limited to {len(urls)} out of {original_count} URLs")
            else:
                log(f"Found {len(urls)} product URLs in this sitemap")
            
            # Process URLs in parallel
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [
                    executor.submit(process_product_data, url, writer, seen, stats)
                    for url in urls
                ]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        log(f"Error in thread execution: {e}", "ERROR")
                        stats['errors'] += 1
            
            # Clean up memory
            gc.collect()
    
    # Print statistics
    log("=" * 60)
    log("SCRAPING STATISTICS")
    log("=" * 60)
    log(f"Sitemaps processed: {stats['sitemaps_processed']}")
    log(f"URLs processed: {stats['urls_processed']}")
    log(f"Products successfully fetched: {stats['products_fetched']}")
    log(f"Errors encountered: {stats['errors']}")
    if stats['urls_processed'] > 0:
        success_rate = (stats['products_fetched'] / stats['urls_processed']) * 100
        log(f"Success rate: {success_rate:.1f}%")
    log("=" * 60)
    log(f"Completed: {OUTPUT_CSV}")
    log("=" * 60)

if __name__ == "__main__":
    # Suppress SSL warnings
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    # Validate environment variables
    if not CURR_URL:
        log("Error: CURR_URL environment variable is required", "ERROR")
        sys.exit(1)
    
    main()