import os
import csv
import time
import sys
import gc
import threading
import requests
import re
import json
import html
import ast
from typing import Optional, List, Dict
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from xml.etree import ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= ENV =================

CURR_URL = os.getenv("CURR_URL", "https://www.discountlivingrooms.com").rstrip("/")
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
# Add default headers to session for all requests
session.headers.update({
    # "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    # "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
})
def get_sitemap_from_robots_txt():
    try:
        # Construct robots.txt URL
        robots_url = f"{CURR_URL}/robots.txt"
        
        # Fetch the robots.txt content
        response = requests.get(robots_url, timeout=10)
        response.raise_for_status()
     
        # Extract Sitemap URL
        sitemap_url = None
        for line in response.text.split('\n'):
            if line.lower().startswith('sitemap:'):
                sitemap_url = line.split(':', 1)[1].strip()
                break
        
        if sitemap_url:
            print(f"Extracted Sitemap URL: {sitemap_url}")
            return sitemap_url
        else:
            print("No Sitemap directive found in robots.txt")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Error fetching robots.txt: {e}")
        return None

def http_get(url: str, is_json: bool = False) -> Optional[str]:
    """HTTP GET request with different headers for sitemap vs API requests"""
    for attempt in range(3):
        try:
            if is_json:
                # For API/JSON requests, override with JSON-specific headers
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Accept": "application/json, text/javascript, */*; q=0.01",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Referer": f"{CURR_URL}/",
                    "X-Requested-With": "XMLHttpRequest",
                    "Sec-Fetch-Dest": "empty",
                    "Sec-Fetch-Mode": "cors",
                    "Sec-Fetch-Site": "same-origin",
                }
                r = session.get(url, headers=headers, timeout=15, verify=True)
            else:
                # For sitemap/XML requests, use default session headers (already set)
                r = session.get(url, timeout=15, verify=True)
                
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


def _clean_strings(obj):
    """Recursively clean JSON-escaped strings like \\/"""
    if isinstance(obj, dict):
        return {k: _clean_strings(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [_clean_strings(v) for v in obj]

    if isinstance(obj, str):
        # JSON escape cleanup (safe + no warnings)
        return obj.replace('\\/', '/')

    return obj


def extract_datalayer(html_text):
    match = re.search(r'dataLayer\s*=\s*(\[[\s\S]*?\]);', html_text)
    if not match:
        return None

    raw = html.unescape(match.group(1))

    # JS → Python literal compatibility
    raw = raw.replace(':true', ':True') \
             .replace(':false', ':False') \
             .replace(':null', ':None')

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        data = ast.literal_eval(raw)

    # 🔥 Clean ALL strings at source
    return _clean_strings(data)

def extract_additional_product_info(html_text):
    try:
        soup = BeautifulSoup(html_text, 'html.parser')
        
        container = soup.find('div', class_='Product__additional-container')
        
        if not container:
            container = soup.find('div', class_='data-table')
            if not container:
                return json.dumps({})
        
        additional_info = {}

        labels = container.find_all('div', class_='label')
        
        for label in labels:
            label_text = label.get_text(strip=True)
            
            data_div = label.find_next_sibling('div', class_='data')
            
            if data_div:
                data_text = data_div.get_text(strip=True)
                if label_text and data_text:
                    json_key = re.sub(r'[^a-zA-Z0-9_]', '_', label_text.lower().replace(' ', '_'))
                    additional_info[json_key] = data_text

        return json.dumps(additional_info, ensure_ascii=False)
    except Exception as e:
        print(f"Error while processing additional Data: {e}")
        return json.dumps({})

def fetch_json(url: str) -> Optional[dict]:
    """Fetch JSON data with proper headers"""
    try:
        # Headers specifically for JSON/API requests
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(url, headers=headers)
        # Look for dataLayer
        html = response.text
        data_layer = extract_datalayer(html)
        additional_info = extract_additional_product_info(html)

        if not data_layer:
            print("No dataLayer found")
            return

        product_data = data_layer[0]
        product_data["additional_product_info_html"] = additional_info
        product_data["_raw_html"] = html  # pass raw HTML for fallback parsing

        return product_data
    except Exception as e:
        print(f"Error fetching JSON: {e}")
        return None

# ================= SITEMAP PROCESSING =================

def load_xml(url: str) -> Optional[ET.Element]:
    """Load XML with appropriate headers"""
    # For GitHub Actions, we might need longer timeout for sitemap
    data = None
    for attempt in range(3):
        try:
            # Use http_get with is_json=False for sitemap requests
            data = http_get(url, is_json=False)
            if data:
                break
        except Exception as e:
            log(f"Attempt {attempt+1} for sitemap failed: {e}", "WARNING")
            time.sleep(2)
    
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

def extract_product_data(product_data: dict) -> dict:
    """
    Extract product data from Discount Living Rooms JSON structure
    """
    try:
        # ---------- Basic ----------
        product_id = str(product_data.get('magentoProductId', ''))
        name = product_data.get('magentoProductName', '').strip()
        sku = product_data.get('magentoProductSku', '')
        
        # ---------- Brand ----------
        # Extract brand from ecommerce items
        ecommerce_items = product_data.get('ecommerce', {}).get('items', [])
        brand = ''
        quantity = 0
        if ecommerce_items:
            brand = ecommerce_items[0].get('item_brand', '')
            quantity = ecommerce_items[0].get('quantity', '')
        
        # ---------- Price ----------
        price = product_data.get('magentoProductPrice', '')
        
        # ---------- Main Image ----------
        main_image = product_data.get('magentoProductImage1', '')

        # ---------- Additional Info ----------
        additional_data = product_data.get('additional_product_info_html', '')
        raw_html = product_data.get('_raw_html', '')
        mpn = sku  # default fallback
        category = ''

        try:
            additional_info_dict = json.loads(additional_data) if additional_data else {}

            # 1️⃣ Prefer Item Number from parsed JSON
            item_number = additional_info_dict.get('item_number', '').strip()
            if item_number:
                mpn = item_number

            category = additional_info_dict.get('product_type', '').strip()

            # 2️⃣ Fallback: Extract directly from HTML if still empty
            if not item_number and raw_html:
                soup = BeautifulSoup(raw_html, "html.parser")
                for label in soup.select("div.label"):
                    if label.get_text(strip=True).lower() == "item number":
                        data_div = label.find_next_sibling("div", class_="data")
                        if data_div:
                            html_item_number = data_div.get_text(strip=True)
                            if html_item_number:
                                mpn = html_item_number
                                break

        except Exception as e:
            print(f"Error setting mpn or category : {e}")
        
        
        # ---------- Category ----------
        category_url = ''
        
        if ecommerce_items and not category:
            # Use the first available category field
            category_fields = [
                'item_category', 'item_category2', 'item_category3',
                'item_category4', 'item_category5', 'item_category6',
                'item_category7', 'item_category8', 'item_category9'
            ]
            categories = []
            for field in category_fields:
                cat_value = ecommerce_items[0].get(field, '')
                if cat_value:  # Only add non-empty values
                    categories.append(cat_value)
            
            # Join all categories with | separator
            if categories:
                category = ' | '.join(categories)
        
        # ---------- Stock Status ----------
        availability = product_data.get('magentoProductAvailability', '')
        status = 'OUT_OF_STOCK'
        
        if availability == 'InStock':
            status = 'SELLABLE'

        
        # ---------- Variation ID ----------
        # Use product ID as variation ID since no separate variation field
        variation_id = ''
        
        # ---------- Additional Attributes ----------
        group_attr_1 = ''
        group_attr_2 = ''
        
        # Could use some product name parts or other fields if needed
        # For now leaving empty as not specified in JSON
        
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
            'group_attr_2': group_attr_2,
            'additional_data': additional_data,
        }
        
    except Exception as e:
        print(f"Error extracting product data: {e}")
        return {}

def process_product_data(product_url: str, writer, seen: set, stats: dict):
    """Process a single Overstock product URL"""
    if product_url in seen:
        return
    seen.add(product_url)
    
    log(f"Processing product URL: {product_url}", "DEBUG")
    
    data = fetch_json(product_url)  # This uses JSON-specific headers

    if not data:
        stats['errors'] += 1
        log(f"No data found for product {product_url}", "ERROR")
        return
    
    # Extract data from response
    product_info = extract_product_data(data)
    if not product_info.get('product_id'):
        stats['errors'] += 1
        log(f"Invalid data for product {product_id}", "ERROR")
        return
    
    try:
        # Prepare row data
        row = [
            product_url,
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
            product_info['additional_data'],  #additional Data
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
    sitemap = get_sitemap_from_robots_txt()
    log("=" * 60)
    log("Overstock Parallel Scraper")
    log(f"Timestamp: {SCRAPED_DATE}")
    log(f"Base URL: {CURR_URL}")
    log(f"API Base URL: {API_BASE_URL}")
    log(f"Sitemap Index: {sitemap}")
    log(f"Sitemap Offset: {SITEMAP_OFFSET}")
    log(f"Max Sitemaps: {MAX_SITEMAPS if MAX_SITEMAPS > 0 else 'All'}")
    log(f"Max URLs per Sitemap: {MAX_URLS_PER_SITEMAP if MAX_URLS_PER_SITEMAP > 0 else 'All'}")
    log(f"Max Workers: {MAX_WORKERS}")
    log(f"Request Delay: {REQUEST_DELAY}s")
    log("=" * 60)
    
    # Load sitemap index - NO HEADERS for sitemap
    log(f"Loading sitemap index from {sitemap}")
    index = load_xml(sitemap)
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
            sitemaps = [e.text.strip() for e in elements 
            if e.text and "product" in e.text.lower()]
            break
    
    # If still no sitemaps, try regex
    if not sitemaps:
        log("No sitemaps found with XML parsing, trying regex", "WARNING")
    
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
            "Additional Product Data",
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
                        and not any(ext in e.text for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.svg'])
                        and (
                            '.html' in e.text
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