import os
import csv
import time
import sys
import random
import cloudscraper
import requests
from curl_cffi import requests as cc_requests
from bs4 import BeautifulSoup
import re
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import gc
from urllib.parse import urljoin, urlparse
from typing import Optional, Tuple, List
from xml.etree import ElementTree as ET
import json

# ================= CONFIG =================
CURR_URL = os.getenv("CURR_URL", "https://www.cymax.com").rstrip("/")
SITEMAP_OFFSET = int(os.getenv("SITEMAP_OFFSET", "0"))
MAX_PRODUCTS = int(os.getenv("MAX_PRODUCTS", "0"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10"))
REQUEST_DELAY_BASE = float(os.getenv("REQUEST_DELAY", "0.2"))
FLARESOLVERR_URL = os.getenv("FLARESOLVERR_URL", "").strip()
FLARESOLVERR_URLS_ENV = os.getenv("FLARESOLVERR_URLS", "").strip()
COLLECT_URLS_ONLY = os.getenv("COLLECT_URLS_ONLY", "0") == "1"
MASTER_URLS_FILE = os.getenv("MASTER_URLS_FILE", "cymaxv2_all_urls.txt").strip()
PRODUCT_URLS_FILE = os.getenv("PRODUCT_URLS_FILE", "").strip()
URL_OFFSET = int(os.getenv("URL_OFFSET", str(SITEMAP_OFFSET)))
URL_LIMIT = int(os.getenv("URL_LIMIT", str(MAX_PRODUCTS)))
CHUNK_ID = os.getenv("CHUNK_ID", str(URL_OFFSET))

if PRODUCT_URLS_FILE:
    OUTPUT_CSV = f"cymax_products_{CHUNK_ID}.csv"
else:
    OUTPUT_CSV = f"cymax_products_{SITEMAP_OFFSET}.csv"
SCRAPED_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# ================= LOGGER =================
def log(msg: str):
    sys.stderr.write(f"[{time.strftime('%H:%M:%S')}] {msg}\n")
    sys.stderr.flush()

def parse_flaresolverr_urls() -> List[str]:
    urls = [u.strip() for u in FLARESOLVERR_URLS_ENV.split(",") if u.strip()]
    if urls:
        return urls
    if FLARESOLVERR_URL:
        return [FLARESOLVERR_URL]
    return []

# ================= ENHANCED REQUEST MANAGER =================
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
        # self.headers = {
        #     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        # }
        
        # self.scraper.headers.update(self.headers)
        self.retry_delays = [1, 2, 4, 8, 16]  # Exponential backoff
        self.request_count = 0
        self.last_request_time = 0
        self.lock = threading.Lock()
        self.fs_endpoints = parse_flaresolverr_urls()
        self.fs_endpoint_index = 0
        self.fs_lock = threading.Lock()

    def _ordered_fs_endpoints(self) -> List[str]:
        if not self.fs_endpoints:
            return []
        with self.fs_lock:
            start = self.fs_endpoint_index % len(self.fs_endpoints)
            self.fs_endpoint_index += 1
        return self.fs_endpoints[start:] + self.fs_endpoints[:start]
        
    def _respect_rate_limit(self, crawl_delay=None):
        """Add random delay between requests"""
        with self.lock:
            current_time = time.time()
            if self.request_count > 0:
                elapsed = current_time - self.last_request_time
                # Use crawl_delay if provided, otherwise use base delay
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
            if self.request_count % 500 == 0:
                long_pause = random.uniform(8, 15)
                log(f"Taking longer pause after {self.request_count} requests: {long_pause:.1f}s")
                time.sleep(long_pause)
    
    def _fetch_with_cloudscraper(self, url: str, crawl_delay=None) -> Optional[Tuple[str, int]]:
        """Use cloudscraper for Cloudflare-protected pages"""
        try:
            self._respect_rate_limit(crawl_delay)
            response = self.scraper.get(url, timeout=45)
            if response.status_code == 200:
                # Check for Cloudflare block
                if "Cloudflare" in response.text and "Attention Required" in response.text:
                    log(f"Cloudflare block detected on {url}")
                    return None, 403
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
                # headers=self.headers,
                timeout=45,
                impersonate="chrome110",  # Mimic Chrome 110
                allow_redirects=True
            )
            if response.status_code == 200:
                return response.text, response.status_code
            return None, response.status_code
        except Exception as e:
            log(f"Curl_cffi error for {url}: {e}")
            return None, 0

    def _fetch_with_flaresolverr(self, url: str, crawl_delay=None) -> Optional[Tuple[str, int]]:
        """Use FlareSolverr as fallback when direct methods are blocked"""
        if not self.fs_endpoints:
            return None, 0
        try:
            self._respect_rate_limit(crawl_delay)
            payload = {"cmd": "request.get", "url": url, "maxTimeout": 120000}
            last_status = 0
            for endpoint in self._ordered_fs_endpoints():
                try:
                    response = requests.post(
                        endpoint,
                        json=payload,
                        timeout=90,
                    )
                    if response.status_code != 200:
                        last_status = response.status_code
                        continue
                    result = response.json()
                    if result.get("status") != "ok":
                        continue
                    solution = result.get("solution", {})
                    content = solution.get("response", "")
                    status = int(solution.get("status", 200))
                    if content:
                        return content, status
                    last_status = status
                except Exception as e:
                    log(f"FlareSolverr endpoint error ({endpoint}): {e}")
            return None, last_status
        except Exception as e:
            log(f"FlareSolverr error for {url}: {e}")
            return None, 0
    
    def fetch(self, url: str, retry_count: int = 0, crawl_delay=None) -> Optional[str]:
        """Intelligent fetching with fallback strategies"""
        if retry_count >= len(self.retry_delays):
            log(f"Max retries exceeded for {url}")
            return None
        # retry_count = 1;
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

        # If blocked and FlareSolverr is configured, try it immediately.
        if status in [403, 429, 503] and self.fs_endpoints:
            fs_content, fs_status = self._fetch_with_flaresolverr(url, crawl_delay=crawl_delay)
            if fs_content:
                log(f"Recovered via FlareSolverr for {url}")
                return fs_content
            status = fs_status or status
        
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
    
    def get(self, url, max_retries=3):
        """Wrapper for fetch method (compatibility with old code)"""
        return self.fetch(url, retry_count=0)

# ================= ROBOTS.TXT CHECK =================
def check_robots_txt(scraper):
    """Check robots.txt for crawl delays and sitemap location"""
    robots_url = f"{CURR_URL}/robots.txt"
    log(f"Checking robots.txt: {robots_url}")
    
    robots_content = scraper.fetch(robots_url)
    if robots_content:
        lines = robots_content.split('\n')
        crawl_delay = None
        sitemap_url = None
        
        for line in lines:
            line = line.strip()
            # Handle sitemap entries
            if line.lower().startswith('sitemap:'):
                parts = line.split(':', 1)
                if len(parts) > 1:
                    potential_url = parts[1].strip()
                    if potential_url.startswith('http'):
                        sitemap_url = potential_url
                        log(f"Found valid sitemap in robots.txt: {sitemap_url}")
            # Handle crawl-delay entries
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

# ================= SITEMAP PARSING =================
def load_xml(url: str, scraper, crawl_delay=None) -> Optional[ET.Element]:
    """Load and parse XML from URL"""
    data = scraper.fetch(url, crawl_delay=crawl_delay)
    if not data:
        return None
    try:
        return ET.fromstring(data)
    except ET.ParseError as e:
        log(f"XML parse error for {url}: {e}")
        return None

def extract_urls_from_sitemap(content):
    """Extract URLs from sitemap content"""
    urls = []
    
    # Method 1: XML parsing
    try:
        root = ET.fromstring(content)
        # Handle different namespace variations
        for elem in root.iter():
            if elem.tag.endswith('loc') and elem.text:
                urls.append(elem.text)
    except:
        # Method 2: Regex extraction
        url_patterns = [
            r'<loc>\s*(https?://[^<]+)\s*</loc>',
            r'https?://[^\s<>"]+\.htm',
        ]
        
        for pattern in url_patterns:
            matches = re.findall(pattern, content)
            urls.extend(matches)
    
    return list(set(urls))  # Remove duplicates

# ================= SITEMAP DISCOVERY =================
def discover_product_urls(scraper, crawl_delay=None):
    """Discover product URLs from sitemaps and category pages"""
    log("Starting URL discovery...")
    all_urls = set()
    
    # First try to get sitemap from robots.txt
    _, robots_sitemap = check_robots_txt(scraper)
    
    sitemap_urls = []
    if robots_sitemap and robots_sitemap.startswith('http'):
        sitemap_urls.append(robots_sitemap)
        log(f"Using sitemap from robots.txt: {robots_sitemap}")

    # Always include common fallbacks (robots sitemap can be blocked from some IP ranges)
    fallback_sitemaps = [
        f"{CURR_URL}/sitemap.xml",
        f"{CURR_URL}/sitemap_index.xml",
        f"{CURR_URL}/sitemap/products.xml",
        f"{CURR_URL}/sitemap_products_1.xml",
    ]
    # Include www/non-www variants for better compatibility
    if "://www." in CURR_URL:
        bare = CURR_URL.replace("://www.", "://", 1)
        fallback_sitemaps.extend([
            f"{bare}/sitemap.xml",
            f"{bare}/sitemap_index.xml",
        ])
    else:
        www = CURR_URL.replace("://", "://www.", 1)
        fallback_sitemaps.extend([
            f"{www}/sitemap.xml",
            f"{www}/sitemap_index.xml",
        ])

    # De-duplicate while preserving order
    sitemap_urls.extend([u for u in fallback_sitemaps if u])
    sitemap_urls = list(dict.fromkeys(sitemap_urls))
    
    # Process sitemaps
    for sitemap_url in sitemap_urls:
        log(f"Trying sitemap: {sitemap_url}")
        content = scraper.fetch(sitemap_url, crawl_delay=crawl_delay)
        if content:
            urls = extract_urls_from_sitemap(content)
            
            # Check if this is a sitemap index (contains other sitemaps)
            if any('sitemap' in url.lower() for url in urls[:10]):
                log(f"Found sitemap index with {len(urls)} child sitemaps")
                # Process each child sitemap
                for child_sitemap in urls[:13]:  # Limit to first 10 to avoid overwhelming
                    log(f"  Loading child sitemap: {child_sitemap}")
                    child_content = scraper.fetch(child_sitemap, crawl_delay=crawl_delay)
                    if child_content:
                        child_urls = extract_urls_from_sitemap(child_content)
                        all_urls.update(child_urls)
                        log(f"  Found {len(child_urls)} URLs from child sitemap")
            else:
                all_urls.update(urls)
                log(f"Found {len(urls)} URLs in {sitemap_url}")
            
            # If we got URLs, we can stop
            if len(all_urls) > 0:
                break
    
    # Filter to product URLs (ending with .htm)
    product_urls = []
    base_host = urlparse(CURR_URL).netloc.lower().replace("www.", "")
    for url in all_urls:
        host = urlparse(url).netloc.lower().replace("www.", "")
        if '.htm' in url and host == base_host:
            # Filter out non-product URLs
            if not any(x in url for x in ['--C', '--PC', 'robots', 'sitemap', '/c-', '/category-']):
                product_urls.append(url)
    
    # Remove duplicates while preserving order
    product_urls = list(dict.fromkeys(product_urls))
    
    log(f"Discovered {len(product_urls)} product URLs")
    return product_urls

# ================= PRODUCT PARSER =================
def parse_product_page(html, url):
    try:
        soup = BeautifulSoup(html, "html.parser")

        def abs_url(src):
            return urljoin(url, src) if src else ""

        product = {
            "url": url,
            "product_id": "",
            "title": "",
            "price": "",
            "original_price": "",
            "brand": "",
            "category": "",
            "category_full": "",
            "sku": "",
            "mpn": "",
            "description": "",
            "images": [],
            "dimensions": "",
            "weight": "",
            "availability": "Unknown",
            "shipping_info": "",
            "variants": [],
            "specifications": {},
        }

        # ---------- PRODUCT ID ----------
        id_span = soup.find("span", class_="product-id-label")
        if id_span:
            product["product_id"] = id_span.get_text(strip=True)

        if not product["product_id"]:
            m = re.search(r'"productId":\s*"(\d+)"', html)
            if m:
                product["product_id"] = m.group(1)

        if not product["product_id"]:
            m = re.search(r"Item:\s*(\d+)", html)
            if m:
                product["product_id"] = m.group(1)

        if not product["product_id"]:
            m = re.search(r"/(\d+)[-A-Z]*\.htm", url)
            if m:
                product["product_id"] = m.group(1)

        # ---------- TITLE ----------
        h1 = soup.find("h1", itemprop="name")
        if h1:
            product["title"] = h1.get_text(strip=True)
        elif soup.title:
            product["title"] = soup.title.get_text(strip=True)

        # ---------- PRICE ----------
        price_el = soup.find(id="product-main-price")
        if price_el:
            clean = price_el.get_text().replace("$", "").replace(",", "")
            m = re.search(r"\d+(\.\d+)?", clean)
            if m:
                product["price"] = m.group(0)

        # OG fallback
        if not product["price"]:
            og = soup.find("meta", property="og:price:amount")
            if og:
                product["price"] = og.get("content", "")

        # ---------- ORIGINAL PRICE ----------
        lp = soup.find("span", class_="list-price")
        if lp:
            clean = lp.get_text().replace("$", "").replace(",", "")
            m = re.search(r"\d+(\.\d+)?", clean)
            if m:
                product["original_price"] = m.group(0)

        # ---------- BRAND ----------
        meta_brand = soup.find("meta", itemprop="brand")
        if meta_brand:
            product["brand"] = meta_brand.get("content", "")

        if not product["brand"]:
            m = re.search(r'"brandName":\s*"([^"]+)"', html)
            if m:
                product["brand"] = m.group(1)

        # ---------- CATEGORY ----------
        crumbs = soup.select(".breadcrumb li a")
        cats = [c.get_text(strip=True) for c in crumbs if c.get_text(strip=True) != "Home"]
        if cats:
            product["category_full"] = " > ".join(cats)
            product["category"] = cats[-1]

        # ---------- SKU / MPN ----------
        m = re.search(r'"manufacturerPartNumbers":\s*\["([^"]+)"\]', html)
        if m:
            product["sku"] = m.group(1)
            product["mpn"] = m.group(1)

        # ---------- DESCRIPTION ----------
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc:
            product["description"] = meta_desc.get("content", "")

        # ---------- IMAGES ----------
        main_img = soup.find("img", id="product-main-image")
        if main_img and main_img.get("src"):
            product["images"].append(abs_url(main_img["src"]))

        for img in soup.select("#gallery-slider-area img"):
            src = img.get("data-src") or img.get("src")
            if src:
                full = abs_url(src)
                if full not in product["images"]:
                    product["images"].append(full)

        # ---------- DIMENSIONS / WEIGHT ----------
        dims = soup.find(id="product-dimensions")
        if dims:
            text = dims.get_text(" ", strip=True)

            m = re.search(r"Dimensions:\s*([^P]+)", text)
            if m:
                product["dimensions"] = m.group(1).strip()

            m = re.search(r"Product Weight:\s*([^\s]+.*)", text)
            if m:
                product["weight"] = m.group(1).strip()

        # ---------- SHIPPING ----------
        ship = soup.find(id="product-shipping-info")
        if ship:
            txt = ship.get_text(" ", strip=True)
            product["shipping_info"] = txt
            if "Ships" in txt:
                product["availability"] = "Available"

        # ---------- VARIANTS ----------
        product["variants"] = [{
            "type": "default",
            "title": "Default",
            "price": product["price"],
            "url": url,
            "image": product["images"][0] if product["images"] else "",
            "dimensions": product["dimensions"],
        }]

        # ---------- SPECS ----------
        specs = {}
        table = soup.find("table", class_="table-striped")
        if table:
            for tr in table.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) >= 2:
                    k = tds[0].get_text(strip=True)
                    v = tds[1].get_text(strip=True)
                    if k and v:
                        specs[k] = v

        product["specifications"] = specs

        return product

    except Exception as e:
        log(f"Parse error for {url}: {e}")
        return None

# ================= PRODUCT PROCESSING =================
csv_lock = threading.Lock()

def process_product(url, writer, scraper, seen, results, crawl_delay=None):
    """Process a single product and write to CSV"""
    if url in seen:
        return
    
    seen.add(url)
    
    try:
        html = scraper.fetch(url, crawl_delay=crawl_delay)
        if not html:
            log(f"Failed to fetch: {url}")
            return
        
        product = parse_product_page(html, url)
        if not product:
            return
        
        # Process each variant
        for idx, variant in enumerate(product['variants']):
            variant_id = f"{product['product_id']}_{idx+1}" if idx > 0 else product['product_id']
            
            # Prepare CSV row
            row = [
                variant['url'],  # Ref Product URL
                product['product_id'],  # Ref Product ID
                variant_id,  # Ref Variant ID
                product['category'],  # Ref Category
                "",  # Ref Category URL
                product['brand'],  # Ref Brand Name
                f"{product['title']} - {variant['title']}" if variant['title'] != 'Default' else product['title'],  # Ref Product Name
                product['sku'],  # Ref SKU
                product['mpn'],  # Ref MPN
                "",  # Ref GTIN
                variant['price'] or product['price'],  # Ref Price
                variant['image'] or (product['images'][0] if product['images'] else ""),  # Ref Main Image
                1,  # Ref Quantity (assume 1)
                variant['title'],  # Ref Group Attr 1
                variant['type'],  # Ref Group Attr 2
                product['availability'],  # Ref Status
                SCRAPED_DATE  # Date Scraped
            ]
            
            with csv_lock:
                writer.writerow(row)
        
        results.append(product['product_id'])
        log(f"✓ Processed {len(product['variants'])} variants for {product['product_id']}")
        
        # Variable delay between products
        base_delay = crawl_delay if crawl_delay else REQUEST_DELAY_BASE
        delay = random.uniform(base_delay * 0.8, base_delay * 1.5)
        time.sleep(delay)
        
    except Exception as e:
        log(f"Error processing {url}: {e}")

def process_urls(product_urls, scraper, crawl_delay):
    log(f"Processing {len(product_urls)} URLs in this chunk")
    if not product_urls:
        log("No URLs to process for this chunk")
        return

    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            "Ref Product URL",
            "Ref Product ID",
            "Ref Variant ID",
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
            "Date Scraped"
        ])

        seen = set()
        results = []

        log(f"Starting processing with {MAX_WORKERS} workers...")
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            for url in product_urls:
                future = executor.submit(
                    process_product,
                    url, writer, scraper, seen, results, crawl_delay
                )
                futures.append(future)

            completed = 0
            total = len(futures)
            for future in as_completed(futures):
                completed += 1
                if completed % 5 == 0 or completed == total:
                    log(f"Progress: {completed}/{total} products processed")
                try:
                    future.result()
                except Exception as e:
                    log(f"Future error: {e}")
                if completed % 20 == 0:
                    gc.collect()

    log("\n" + "=" * 60)
    log("SCRAPING COMPLETE")
    log("=" * 60)
    log(f"Output file: {OUTPUT_CSV}")
    log(f"Unique products processed: {len(results)}")
    log(f"Total requests made: {scraper.request_count}")

def read_urls_from_file(path: str):
    urls = []
    if path.lower().endswith(".csv"):
        with open(path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames and "url" in reader.fieldnames:
                urls = [str(row.get("url", "")).strip() for row in reader if str(row.get("url", "")).strip()]
            else:
                f.seek(0)
                raw = list(csv.reader(f))
                urls = [r[0].strip() for r in raw if r and r[0].strip()]
    else:
        with open(path, "r", encoding="utf-8") as f:
            urls = [line.strip() for line in f if line.strip()]
    return list(dict.fromkeys(urls))

def write_urls_to_file(urls, path: str):
    with open(path, "w", encoding="utf-8") as f:
        for url in urls:
            f.write(url + "\n")

# ================= MAIN =================
def main():
    log("=" * 60)
    log("CYMAX.COM PRODUCT SCRAPER (Enhanced Cloudflare Resistance)")
    log("=" * 60)
    
    # Initialize enhanced scraper
    scraper = RequestManager()
    
    # Check robots.txt for crawl delays
    crawl_delay, robots_sitemap = check_robots_txt(scraper)
    
    # If crawl_delay is found in robots.txt, use it (but cap it)
    if crawl_delay:
        if crawl_delay > 30:  # Cap at 30 seconds max
            log(f"Crawl-delay {crawl_delay}s is too high, capping at 30s")
            crawl_delay = 30
        log(f"Respecting crawl-delay: {crawl_delay} seconds between requests")
    else:
        log(f"Using default request delay: {REQUEST_DELAY_BASE} seconds")
    
    if COLLECT_URLS_ONLY:
        all_product_urls = discover_product_urls(scraper, crawl_delay)
        if not all_product_urls:
            log("ERROR: No product URLs found")
            sys.exit(1)
        write_urls_to_file(all_product_urls, MASTER_URLS_FILE)
        log(f"Collected {len(all_product_urls)} URLs into {MASTER_URLS_FILE}")
        return

    if PRODUCT_URLS_FILE:
        log(f"Loading URLs from file: {PRODUCT_URLS_FILE}")
        try:
            all_product_urls = read_urls_from_file(PRODUCT_URLS_FILE)
        except Exception as e:
            log(f"ERROR: Failed to read PRODUCT_URLS_FILE: {e}")
            sys.exit(1)

        start = max(0, URL_OFFSET)
        end = start + URL_LIMIT if URL_LIMIT > 0 else len(all_product_urls)
        product_urls = all_product_urls[start:end]
        log(
            f"URL file has {len(all_product_urls)} unique URLs. "
            f"Processing {len(product_urls)} (offset={start}, limit={URL_LIMIT if URL_LIMIT > 0 else 'all'})"
        )
        process_urls(product_urls, scraper, crawl_delay)
        return

    all_product_urls = discover_product_urls(scraper, crawl_delay)
    if not all_product_urls:
        log("ERROR: No product URLs found")
        sys.exit(1)

    log(f"Total product URLs discovered: {len(all_product_urls)}")
    if MAX_PRODUCTS > 0:
        product_urls = all_product_urls[SITEMAP_OFFSET:SITEMAP_OFFSET + MAX_PRODUCTS]
    else:
        product_urls = all_product_urls[SITEMAP_OFFSET:]

    process_urls(product_urls, scraper, crawl_delay)

if __name__ == "__main__":
    main()