import os
import csv
import time
import sys
import gc
import threading
import requests
import random
import re
import json
import html
import ast
from typing import Optional, List, Dict, Tuple
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from xml.etree import ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

# ================= ENV =================

CURR_URL = os.getenv("CURR_URL", "https://www.furniturecart.com").rstrip("/")
SITEMAP_INDEX = f"{CURR_URL}/sitemap.xml"
SITEMAP_OFFSET = int(os.getenv("SITEMAP_OFFSET", "0"))
MAX_SITEMAPS = int(os.getenv("MAX_SITEMAPS", "0"))
MAX_URLS_PER_SITEMAP = int(os.getenv("MAX_URLS_PER_SITEMAP", "0"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "4"))
REQUEST_DELAY_BASE = float(os.getenv("REQUEST_DELAY", "1.0"))
SAMPLE_SIZE = int(os.getenv("SAMPLE_SIZE", "5"))

# FlareSolverr configuration
FLARESOLVERR_URL = os.getenv("FLARESOLVERR_URL", "http://localhost:8191/v1")
FLARESOLVERR_TIMEOUT = int(os.getenv("FLARESOLVERR_TIMEOUT", "60"))

# ---------- NEW: chunked single‑sitemap mode ----------
SITEMAP_URL = os.getenv("SITEMAP_URL", "")          # process exactly this sitemap
URL_OFFSET   = int(os.getenv("URL_OFFSET", "0"))    # start index inside the sitemap
CHUNK_ID     = os.getenv("CHUNK_ID", str(SITEMAP_OFFSET))  # unique chunk identifier

# Output file name: use CHUNK_ID when in chunk mode, else fallback to offset
if SITEMAP_URL:
    OUTPUT_CSV = f"products_chunk_{CHUNK_ID}.csv"
else:
    OUTPUT_CSV = f"products_chunk_{SITEMAP_OFFSET}.csv"

SCRAPED_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

# ================= LOGGER =================

def log(msg: str, level: str = "INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sys.stderr.write(f"[{timestamp}] [{level}] {msg}\n")
    sys.stderr.flush()

# ================= FLARESOLVERR SESSION =================

class FlareSolverrSession:
    def __init__(self):
        self.session = requests.Session()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9",
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

    def flaresolverr_request(self, url: str, max_retries: int = 3) -> Optional[Tuple[str, int]]:
        """Make request through FlareSolverr to bypass Cloudflare"""
        for attempt in range(max_retries):
            try:
                payload = {
                    "cmd": "request.get",
                    "url": url,
                    "maxTimeout": 60000,
                    "session": None,  # Create new session
                    "headers": self.headers
                }
                
                response = self.session.post(
                    FLARESOLVERR_URL,
                    json=payload,
                    timeout=FLARESOLVERR_TIMEOUT
                )
                
                if response.status_code == 200:
                    result = response.json()
                    
                    if result.get("status") == "ok":
                        solution = result.get("solution", {})
                        content = solution.get("response", "")
                        
                        # Extract cookies for potential future requests
                        cookies = solution.get("cookies", [])
                        for cookie in cookies:
                            self.session.cookies.set(
                                cookie.get("name"),
                                cookie.get("value"),
                                domain=cookie.get("domain")
                            )
                        
                        # Update headers from response
                        if "headers" in solution:
                            for key, value in solution["headers"].items():
                                if key.lower() not in ["content-length", "content-encoding", "transfer-encoding"]:
                                    self.headers[key] = value
                        
                        return content, 200
                
                log(f"FlareSolverr attempt {attempt + 1} failed for {url}: {response.status_code}")
                
            except requests.exceptions.Timeout:
                log(f"FlareSolverr timeout on attempt {attempt + 1} for {url}")
            except requests.exceptions.ConnectionError:
                log(f"FlareSolverr connection error on attempt {attempt + 1} for {url}")
            except Exception as e:
                log(f"FlareSolverr error on attempt {attempt + 1} for {url}: {e}")
            
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
        
        return None, 0

    def fetch(self, url: str) -> Optional[Tuple[str, int]]:
        """Fetch URL through FlareSolverr"""
        return self.flaresolverr_request(url)

flaresolverr_session = FlareSolverrSession()

def get_sitemap_from_robots_txt():
    try:
        robots_url = f"{CURR_URL}/robots.txt"
        content, status = flaresolverr_session.fetch(robots_url)
        
        if content and status == 200:
            sitemap_url = None
            for line in content.split('\n'):
                if line.lower().startswith('sitemap:'):
                    sitemap_url = line.split(':', 1)[1].strip()
                    break
            
            if sitemap_url:
                print(f"Extracted Sitemap URL: {sitemap_url}")
                return sitemap_url
            else:
                print("No Sitemap directive found in robots.txt")
                return None
        else:
            print(f"Error fetching robots.txt: Status {status}")
            return None
            
    except Exception as e:
        print(f"Error fetching robots.txt: {e}")
        return None

def check_robots_txt():
    """Check robots.txt for crawl delays and sitemap location"""
    robots_url = f"{CURR_URL}/robots.txt"
    log(f"Checking robots.txt: {robots_url}")
    
    content, status = flaresolverr_session.fetch(robots_url)
    if content and status == 200:
        lines = content.split('\n')
        crawl_delay = None
        sitemap_url = None
        
        for line in lines:
            line = line.strip()
            if line.lower().startswith('sitemap:'):
                parts = line.split(':', 1)
                if len(parts) > 1:
                    potential_url = parts[1].strip()
                    if potential_url.startswith('http'):
                        sitemap_url = potential_url
                        log(f"Found valid sitemap in robots.txt: {sitemap_url}")
            elif line.lower().startswith('crawl-delay:'):
                try:
                    parts = line.split(':', 1)
                    if len(parts) > 1:
                        crawl_delay = float(parts[1].strip())
                        log(f"Found Crawl-delay: {crawl_delay} seconds")
                except (ValueError, IndexError) as e:
                    log(f"Error parsing crawl-delay: {e}")
        
        return crawl_delay, sitemap_url
    
    log("No robots.txt found or couldn't fetch it")
    return None, None

class RequestManager:
    def __init__(self):
        self.request_count = 0
        self.last_request_time = 0
        self.retry_delays = [1, 2, 4]
        
    def _respect_rate_limit(self, crawl_delay=None):
        current_time = time.time()
        if self.request_count > 0:
            elapsed = current_time - self.last_request_time
            base_delay = crawl_delay if crawl_delay else REQUEST_DELAY_BASE
            min_delay = base_delay * 0.8
            max_delay = base_delay * 1.5
            target_delay = random.uniform(0, 1)
            
            if elapsed < target_delay:
                sleep_time = target_delay - elapsed
                time.sleep(sleep_time)
        
        self.last_request_time = time.time()
        self.request_count += 1
        
        if self.request_count % 20 == 0:
            long_pause = random.uniform(0, 1)
            log(f"Taking longer pause after {self.request_count} requests: {long_pause:.1f}s")
            time.sleep(long_pause)
    
    def fetch(self, url: str, retry_count: int = 0, crawl_delay=None) -> Optional[str]:
        if retry_count >= len(self.retry_delays):
            log(f"Max retries exceeded for {url}")
            return None
        
        self._respect_rate_limit(crawl_delay)
        content, status = flaresolverr_session.fetch(url)
        
        if content and status == 200:
            return content
        
        if status in [403, 429, 503]:
            delay = self.retry_delays[retry_count] + random.uniform(0, 1)
            log(f"HTTP {status} for {url} , retry {retry_count+1} in {delay:.1f}s")
            time.sleep(delay)
            return self.fetch(url, retry_count + 1, crawl_delay)
        elif status == 404:
            log(f"URL not found: {url}")
            return None
        
        if status != 200 and status != 0:
            delay = self.retry_delays[retry_count]
            log(f"Retry {retry_count+1} for {url} in {delay}s (status: {status})")
            time.sleep(delay)
            return self.fetch(url, retry_count + 1, crawl_delay)
        
        return None

request_manager = RequestManager()

def http_get(url: str, crawl_delay=None) -> Optional[str]:
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

csv_lock = threading.Lock()

def normalize_image_url(url: str) -> str:
    if not url:
        return ""
    
    if url.startswith("//"):
        return "https:" + url
    elif url.startswith("/"):
        return f"{CURR_URL}{url}"
    elif not url.startswith("http"):
        return f"https://ak1.ostkcdn.com{url}" if 'ostkcdn.com' not in url else f"https://{url}"
    
    return url

def extract_product_info_from_html(html: str, product_url: str) -> dict:
    """
    Parse product HTML and return a dictionary with all required fields.
    """
    soup = BeautifulSoup(html, 'html.parser')
    info = {}

    # --- product_id ---
    prod_input = soup.find('input', {'name': 'product'})
    info['product_id'] = prod_input.get('value', '') if prod_input else ''

    # --- sku & variation_id ---
    sku_meta = soup.find('meta', {'itemprop': 'sku'})
    info['sku'] = sku_meta.get('content', '') if sku_meta else ''
    # Use the SKU as the variation ID for this bundle configuration
    info['variation_id'] = info['sku']

    # --- mpn ---
    mpn_meta = soup.find('meta', {'itemprop': 'mpn'})
    info['mpn'] = mpn_meta.get('content', '') if mpn_meta else ''

    # --- name ---
    name_h1 = soup.find('h1', {'itemprop': 'name'})
    info['name'] = name_h1.get_text(strip=True) if name_h1 else ''

    # --- brand ---
    brand_meta = soup.find('meta', {'itemprop': 'brand'})
    if brand_meta:
        info['brand'] = brand_meta.get('content', '')
    else:
        brand_link = soup.find('a', href=lambda h: h and '/brand/' in h)
        info['brand'] = brand_link.get_text(strip=True) if brand_link else ''

    # --- category & category_url (from breadcrumbs) ---
    info['category'] = ''
    info['category_url'] = ''
    breadcrumbs = soup.find('div', class_='breadcrumbs')
    if breadcrumbs:
        crumbs = breadcrumbs.find_all('li')
        # Home (0), Bedroom (1), Bedroom Furniture (2), Bedroom Sets (3)
        if len(crumbs) >= 4:
            cat_li = crumbs[3]
            cat_link = cat_li.find('a')
            if cat_link:
                info['category'] = cat_link.find('span').get_text(strip=True)
                info['category_url'] = cat_link.get('href', '')

    # --- price ---
    price_meta = soup.find('meta', {'itemprop': 'price'})
    if price_meta:
        info['price'] = price_meta.get('content', '').strip()
    else:
        price_span = soup.find('span', {'class': 'price', 'id': re.compile(r'product-price-\d+')})
        if price_span:
            raw = price_span.get_text(strip=True).replace('$', '').replace(',', '')
            info['price'] = raw.strip()
        else:
            info['price'] = ''

    # --- main_image (full size) ---
    img_meta = soup.find('meta', {'itemprop': 'image'})
    if img_meta:
        info['main_image'] = img_meta.get('content', '')
    else:
        img_main = soup.find('img', {'id': 'image-main'})
        info['main_image'] = img_main.get('src', '') if img_main else ''

    # --- quantity (global) ---
    qty_input = soup.find('input', {'id': 'qty-input'})
    info['quantity'] = qty_input.get('value', '1') if qty_input else '1'

    # --- group_attr_1: selected bed size ---
    bed_size = ''
    # Look for the active Queen bed option (adjust class if King is selected)
    active_bed = soup.find('li', class_='option-item-209551 selection-item-263524 active')
    if active_bed:
        text = active_bed.get_text()
        match = re.search(r'\(([^)]+)\)', text)
        if match:
            bed_size = match.group(1)
    info['group_attr_1'] = bed_size

    # --- group_attr_2: color ---
    color = ''
    # Try from the "Additional Information" panel first
    add_info = soup.find('div', class_='product-details')
    if add_info:
        for li in add_info.find_all('li', class_='clearer'):
            title_div = li.find('div', class_='title')
            if title_div and 'Color' in title_div.get_text():
                desc_div = li.find('div', class_='description')
                if desc_div:
                    color = desc_div.get_text(strip=True)
                    break
    if not color:
        # Fallback: look in the dimension/attribute list
        color_li = soup.find('li', class_='clearer')
        while color_li:
            title = color_li.find('div', class_='title')
            if title and 'Color' in title.get_text():
                desc = color_li.find('div', class_='description')
                if desc:
                    color = desc.get_text(strip=True)
                    break
            color_li = color_li.find_next_sibling('li', class_='clearer')
    info['group_attr_2'] = color

    # --- status (availability) ---
    status = ''
    avail_link = soup.find('link', {'itemprop': 'availability'})
    if avail_link:
        href = avail_link.get('href', '')
        if 'InStock' in href:
            status = 'In Stock'
        elif 'OutOfStock' in href:
            status = 'Out of Stock'
    if not status:
        # Fallback from product details
        status_li = soup.find('li', class_='clearer')
        while status_li:
            title = status_li.find('div', class_='title')
            if title and 'Availability' in title.get_text():
                desc = status_li.find('div', class_='description')
                if desc:
                    status = desc.get_text(strip=True)
                    break
            status_li = status_li.find_next_sibling('li', class_='clearer')
    info['status'] = status

    # --- additional_data: JSON with extra info (collection, dimensions, features) ---
    additional = {}

    # Collection
    collection = ''
    coll_link = soup.find('a', href=lambda h: h and '/collection/' in h)
    if coll_link:
        collection = coll_link.get_text(strip=True)
    else:
        coll_li = soup.find('li', class_='clearer')
        while coll_li:
            title = coll_li.find('div', class_='title')
            if title and 'Collection' in title.get_text():
                desc = coll_li.find('div', class_='description')
                if desc:
                    collection = desc.get_text(strip=True)
                    break
            coll_li = coll_li.find_next_sibling('li', class_='clearer')
    additional['collection'] = collection

    # Dimensions (extract from the dimensions tab)
    dims = {}
    dims_section = soup.find('div', class_='product-dimensions')
    if dims_section:
        for row in dims_section.find_all('li', class_='clearer'):
            title_div = row.find('div', class_='title')
            dims_div = row.find('div', class_='dimensions')
            if title_div and dims_div:
                piece = title_div.get_text(strip=True)
                dims[piece] = dims_div.get_text(strip=True)
    additional['dimensions'] = dims

    # Features (from the Details tab)
    features = []
    details_section = soup.find('div', class_='product-details')
    if details_section:
        for li in details_section.find_all('li', class_='clearer'):
            title_div = li.find('div', class_='title')
            if title_div and 'Features' in title_div.get_text():
                desc_div = li.find('div', class_='description')
                if desc_div:
                    raw = desc_div.get_text(separator='\n').strip()
                    features = [f.strip() for f in raw.split('\n') if f.strip()]
                    break
    additional['features'] = features

    info['additional_data'] = json.dumps(additional, ensure_ascii=False)

    return info


def getBundleData(html):
  
    soup = BeautifulSoup(html, 'html.parser')
    
    # Find script tags containing Product.Bundle initialization
    script_pattern = re.compile(r'var bundle = new Product\.Bundle\(({.*?})\);', re.DOTALL)
    
    # Look in all script tags
    for script in soup.find_all('script'):
        if script.string:
            # Search for Product.Bundle pattern
            match = script_pattern.search(script.string)
            if match:
                # Return the raw JSON string from the JavaScript object
                return match.group(1).strip()
    
    return None

def process_product_data(product_url: str, writer, seen: set, stats: dict, crawl_delay=None):
    if product_url in seen:
        return
    seen.add(product_url)

    log(f"Processing product URL: {product_url}", "DEBUG")

    # Fetch the original product page
    html = http_get(product_url, crawl_delay)
    soup = BeautifulSoup(html, 'html.parser')

    # --- Extract bundle data from JavaScript ---
    bundleId = None
    bundle_option_id = None
    max_selections_length = 0
    bundleJson = getBundleData(html)
    if bundleJson:
        try:
            bundleData = json.loads(bundleJson)
            bundleId = bundleData.get('bundleId')
            if bundleId:
                options = bundleData.get('options', {})
                for opt_id, opt_data in options.items():
                    selections = opt_data.get('selections', {})
                    sel_len = len(selections)
                    if sel_len > max_selections_length:
                        max_selections_length = sel_len
                        bundle_option_id = opt_id
        except Exception as e:
            log(f"Error parsing bundle data for {product_url}: {e}", "WARNING")

    is_bundle = bool(bundleId and bundle_option_id and max_selections_length > 0)
    variation_rows_written = 0

    if is_bundle:
        log(f"Bundle detected (ID: {bundleId}, option: {bundle_option_id}, max selections: {max_selections_length}).", "DEBUG")

        bundle_set = soup.find('div', class_='bundle-set')
        if not bundle_set:
            log("No <div class='bundle-set'> found – cannot process variations. Writing original row as fallback.", "WARNING")
        else:
            selection_items = bundle_set.select("ul > li[class*='selection-item-']")
            if not selection_items:
                log("No selection items found in bundle-set – cannot process variations. Writing original row as fallback.", "WARNING")
            else:
                # Build set of expected selection names
                expected_names = set()
                selection_id_to_name = {}
                active_original_name = None
                for li in selection_items:
                    sel_id = None
                    for cls in li.get('class', []):
                        if cls.startswith('selection-item-'):
                            sel_id = cls.replace('selection-item-', '')
                            break
                    name = li.get('data-item-set-name', li.get_text(strip=True))
                    if sel_id:
                        selection_id_to_name[sel_id] = name
                    expected_names.add(name)
                    if 'active' in li.get('class', []):
                        active_original_name = name

                log(f"Expected names: {expected_names} | Active on original: {active_original_name}", "DEBUG")

                processed_names = set()

                for i in range(1, max_selections_length + 1):
                    items_value = bundleId - i
                    parsed = urlparse(product_url)
                    query_dict = parse_qs(parsed.query, keep_blank_values=True)

                    # Remove any existing items parameters
                    keys_to_remove = [k for k in query_dict if k.startswith('items')]
                    for k in keys_to_remove:
                        del query_dict[k]

                    param_key = f'items'
                    query_dict[param_key] = [str(items_value)]
                    new_query = urlencode(query_dict, doseq=True)
                    new_parsed = parsed._replace(query=new_query)
                    variation_url = urlunparse(new_parsed)

                    log(f"Fetching variation: {variation_url}", "DEBUG")
                    variation_html = http_get(variation_url, crawl_delay)
                    if not variation_html:
                        continue

                    variation_soup = BeautifulSoup(variation_html, 'html.parser')
                    var_bundle_set = variation_soup.find('div', class_='bundle-set')
                    if not var_bundle_set:
                        continue

                    var_active_li = var_bundle_set.find('li', class_='active')
                    if not var_active_li:
                        continue

                    active_name = var_active_li.get('data-item-set-name', var_active_li.get_text(strip=True))

                    if active_name not in expected_names:
                        log(f"Active name '{active_name}' not in expected set, skipping.", "DEBUG")
                        continue
                    if active_name in processed_names:
                        log(f"Already processed '{active_name}', skipping duplicate.", "DEBUG")
                        continue

                    # --- Valid variation – extract product data ---
                    try:
                        var_product_info = extract_product_info_from_html(variation_html, variation_url)
                    except Exception as e:
                        log(f"Failed to extract product info from variation: {e}", "ERROR")
                        stats['errors'] += 1
                        continue

                    # Write CSV row
                    try:
                        row = [
                            variation_url,
                            var_product_info.get('product_id', ''),
                            var_product_info.get('variation_id', ''),
                            var_product_info.get('category', ''),
                            var_product_info.get('category_url', ''),
                            var_product_info.get('brand', ''),
                            var_product_info.get('name', ''),
                            active_name,
                            var_product_info.get('sku', ''),
                            var_product_info.get('mpn', ''),
                            '',  # empty column
                            var_product_info.get('price', ''),
                            normalize_image_url(var_product_info.get('main_image', '')),
                            var_product_info.get('quantity', ''),
                            var_product_info.get('group_attr_1', ''),
                            var_product_info.get('group_attr_2', ''),
                            var_product_info.get('status', ''),
                            var_product_info.get('additional_data', ''),
                            SCRAPED_DATE
                        ]
                        with csv_lock:
                            writer.writerow(row)
                        stats['products_fetched'] += 1
                        variation_rows_written += 1
                        processed_names.add(active_name)
                        log(f"Fetched bundle variation: {active_name}", "INFO")
                    except Exception as e:
                        log(f"Error writing row for variation: {e}", "ERROR")
                        stats['errors'] += 1

                # After loop: warn about missing variations
                missing = expected_names - processed_names
                if missing:
                    log(f"WARNING: Missing variations: {missing}", "WARNING")

    # --- If we are not a bundle, OR we are a bundle but failed to write any variation row, write the original product row ---
    if not is_bundle or variation_rows_written == 0:
        try:
            product_info = extract_product_info_from_html(html, product_url)
            row = [
                product_url,
                product_info.get('product_id', ''),
                product_info.get('variation_id', ''),
                product_info.get('category', ''),
                product_info.get('category_url', ''),
                product_info.get('brand', ''),
                product_info.get('name', ''),
                '',#set includes name
                product_info.get('sku', ''),
                product_info.get('mpn', ''),
                '',  # empty column
                product_info.get('price', ''),
                normalize_image_url(product_info.get('main_image', '')),
                product_info.get('quantity', ''),
                product_info.get('group_attr_1', ''),
                product_info.get('group_attr_2', ''),
                product_info.get('status', ''),
                product_info.get('additional_data', ''),
                SCRAPED_DATE
            ]
            with csv_lock:
                writer.writerow(row)
            stats['products_fetched'] += 1
            log(f"Fetched original product {product_info.get('sku', '')}: {product_info.get('name', '')[:50]}...", "INFO")
        except Exception as e:
            log(f"Failed to extract/write original product info: {e}", "ERROR")
            stats['errors'] += 1
    stats['urls_processed'] += 1

# ================= MAIN =================

def main():
    crawl_delay, robots_sitemap = check_robots_txt()
    crawl_delay = 0  # Override for this site (adjust if needed)
    
    # ------------------------------------------------------------------
    # MODE 1: Process a SINGLE SITEMAP with offset + limit (chunk mode)
    # ------------------------------------------------------------------
    if SITEMAP_URL:
        log("=" * 60)
        log("SCRAPER STARTED – CHUNK MODE (single sitemap with offset/limit)")
        log(f"SITEMAP_URL: {SITEMAP_URL}")
        log(f"URL_OFFSET: {URL_OFFSET}")
        log(f"MAX_URLS_PER_SITEMAP (limit): {MAX_URLS_PER_SITEMAP}")
        log(f"CHUNK_ID: {CHUNK_ID}")
        log(f"OUTPUT_CSV: {OUTPUT_CSV}")
        log("=" * 60)

        # Load the sitemap
        xml = load_xml(SITEMAP_URL, crawl_delay)
        if not xml:
            log(f"Failed to load sitemap: {SITEMAP_URL}", "ERROR")
            sys.exit(1)

        # Extract all product URLs (same filtering as before)
        ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        urls = []
        for path in [".//ns:url/ns:loc", ".//url/loc", ".//loc"]:
            elements = xml.findall(path, ns) if "ns:" in path else xml.findall(path)
            if elements:
                urls = [
                    e.text.strip()
                    for e in elements
                    if e.text
                    and not any(ext in e.text for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.svg'])
                    and ('.html' in e.text)
                ]
                if urls:
                    break

        if not urls:
            log(f"No product URLs found in sitemap: {SITEMAP_URL}", "WARNING")
            sys.exit(0)

        total_urls = len(urls)
        start = URL_OFFSET
        # limit = 0 means "all remaining"
        end = start + MAX_URLS_PER_SITEMAP if MAX_URLS_PER_SITEMAP > 0 else total_urls
        urls_to_process = urls[start:end]

        log(f"Sitemap contains {total_urls} product URLs. Processing {len(urls_to_process)} URLs (offset {start})")
        if not urls_to_process:
            log("No URLs to process in this chunk – exiting.")
            sys.exit(0)

        # Initialize CSV and write header
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "Ref Product URL",
                "Ref Product ID",
                "Ref Varient ID",
                "Ref Category",
                "Ref Category URL",
                "Ref Brand Name",
                "Ref Product Name",
                "Set Includes Name",
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

            seen = set()
            stats = {
                'sitemaps_processed': 1,
                'urls_processed': 0,
                'products_fetched': 0,
                'errors': 0
            }

            # Process URLs with ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [
                    executor.submit(process_product_data, url, writer, seen, stats, crawl_delay)
                    for url in urls_to_process
                ]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        log(f"Error in thread execution: {e}", "ERROR")
                        stats['errors'] += 1

            gc.collect()

        # Statistics for this chunk
        log("=" * 60)
        log("CHUNK SCRAPING STATISTICS")
        log("=" * 60)
        log(f"Chunk ID:           {CHUNK_ID}")
        log(f"Sitemap:            {SITEMAP_URL}")
        log(f"URLs processed:     {stats['urls_processed']}")
        log(f"Products fetched:   {stats['products_fetched']}")
        log(f"Errors:             {stats['errors']}")
        if stats['urls_processed'] > 0:
            success_rate = (stats['products_fetched'] / stats['urls_processed']) * 100
            log(f"Success rate:       {success_rate:.1f}%")
        log("=" * 60)
        log(f"Chunk output saved: {OUTPUT_CSV}")
        log("=" * 60)
        return

    # ------------------------------------------------------------------
    # MODE 2: Process a SITEMAP INDEX (original behaviour)
    # ------------------------------------------------------------------
    log("=" * 60)
    log("SCRAPER STARTED – SITEMAP INDEX MODE")
    log(f"FlareSolverr URL: {FLARESOLVERR_URL}")
    log(f"Timestamp: {SCRAPED_DATE}")
    log(f"Base URL: {CURR_URL}")
    log(f"Sitemap Index: {SITEMAP_INDEX}")
    log(f"Sitemap Offset: {SITEMAP_OFFSET}")
    log(f"Max Sitemaps: {MAX_SITEMAPS if MAX_SITEMAPS > 0 else 'All'}")
    log(f"Max URLs per Sitemap: {MAX_URLS_PER_SITEMAP if MAX_URLS_PER_SITEMAP > 0 else 'All'}")
    log(f"Max Workers: {MAX_WORKERS}")
    log(f"Request Delay: {REQUEST_DELAY_BASE}s")
    log(f"Sample Size for Checking: {SAMPLE_SIZE}")
    log("=" * 60)

    sitemap = SITEMAP_INDEX
    if robots_sitemap and robots_sitemap.startswith('http'):
        sitemap = robots_sitemap
        log(f"Using sitemap from robots.txt: {sitemap}")
    else:
        if robots_sitemap:
            log(f"Invalid sitemap URL in robots.txt: '{robots_sitemap}', using default")
        else:
            log(f"No valid sitemap in robots.txt, using default: {sitemap}")

    log(f"Loading sitemap index from {sitemap}")
    index = load_xml(sitemap, crawl_delay)
    if index is None:
        log("Failed to load sitemap index", "ERROR")
        sys.exit(1)

    ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    sitemaps = []
    for path in [".//ns:sitemap/ns:loc", ".//sitemap/loc", ".//loc"]:
        elements = index.findall(path, ns) if "ns:" in path else index.findall(path)
        if elements:
            sitemaps = [e.text.strip() for e in elements if e.text]
            break

    if not sitemaps:
        log("No sitemaps found with XML parsing, trying regex", "WARNING")
        # (regex fallback could be added, but we assume XML works)

    if SITEMAP_OFFSET >= len(sitemaps):
        log(f"Offset {SITEMAP_OFFSET} exceeds total sitemaps ({len(sitemaps)})", "WARNING")
        sys.exit(0)

    end_index = SITEMAP_OFFSET + MAX_SITEMAPS if MAX_SITEMAPS > 0 else len(sitemaps)
    sitemaps_to_process = sitemaps[SITEMAP_OFFSET:end_index]

    log(f"Total sitemaps found: {len(sitemaps)}")
    log(f"Sitemaps to process: {len(sitemaps_to_process)}")

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Ref Product URL",
            "Ref Product ID",
            "Ref Varient ID",
            "Ref Category",
            "Ref Category URL",
            "Ref Brand Name",
            "Ref Product Name",
            "Set Includes Name",
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

        seen = set()
        stats = {
            'sitemaps_processed': 0,
            'urls_processed': 0,
            'products_fetched': 0,
            'errors': 0
        }

        for sitemap_url in sitemaps_to_process:
            stats['sitemaps_processed'] += 1
            log(f"Processing sitemap {stats['sitemaps_processed']}/{len(sitemaps_to_process)}: {sitemap_url}")

            xml = load_xml(sitemap_url, crawl_delay)
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
                        and not any(ext in e.text for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.svg'])
                        and ('.html' in e.text)
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
                futures = [
                    executor.submit(process_product_data, url, writer, seen, stats, crawl_delay)
                    for url in urls
                ]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        log(f"Error in thread execution: {e}", "ERROR")
                        stats['errors'] += 1

            gc.collect()

    # Statistics
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
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    if not CURR_URL:
        log("Error: CURR_URL environment variable is required", "ERROR")
        sys.exit(1)

    # Test FlareSolverr connection (only warn on failure)
    if FLARESOLVERR_URL:
        log(f"Testing FlareSolverr connection at {FLARESOLVERR_URL}")
        try:
            test_response = requests.post(FLARESOLVERR_URL, json={"cmd": "sessions.list"}, timeout=10)
            if test_response.status_code == 200:
                log("✓ FlareSolverr connection successful")
            else:
                log(f"⚠ FlareSolverr returned status {test_response.status_code}")
        except Exception as e:
            log(f"⚠ FlareSolverr connection failed: {e}")
            log("Continuing anyway, but requests may fail...")
    else:
        log("⚠ FLARESOLVERR_URL not set, requests will use direct HTTP (may fail behind Cloudflare)")

    main()