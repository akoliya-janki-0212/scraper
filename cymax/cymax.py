import os
import csv
import time
import sys
import threading
import requests
import random
import re
import json
from typing import Optional, List, Dict, Tuple
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from xml.etree import ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse, urljoin

# ================= ENV =================

CURR_URL = os.getenv("CURR_URL", "https://www.cymax.com").rstrip("/")
SITEMAP_INDEX = f"{CURR_URL}/sitemap.xml"
SITEMAP_OFFSET = int(os.getenv("SITEMAP_OFFSET", "0"))
MAX_SITEMAPS = int(os.getenv("MAX_SITEMAPS", "0"))
MAX_URLS_PER_SITEMAP = int(os.getenv("MAX_URLS_PER_SITEMAP", "0"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "4"))
REQUEST_DELAY_BASE = float(os.getenv("REQUEST_DELAY", "1.0"))
PRODUCT_ID_URL_RE = re.compile(r"/(\d+)\.htm")
PRODUCT_ID_JSON_RE = re.compile(r'"productId":\s*"(\d+)"')
SKU_RE = re.compile(r'"manufacturerPartNumbers":\s*\["([^"]+)"\]')

# ===== HIGH SCALE TUNING (500k+ URLs) =====
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
MAX_QUEUE_SIZE = int(os.getenv("MAX_QUEUE_SIZE", "10000"))

# FlareSolverr configuration
FLARESOLVERR_URL = os.getenv("FLARESOLVERR_URL", "http://localhost:8191/v1")
FLARESOLVERR_URLS_ENV = os.getenv("FLARESOLVERR_URLS", "")
FLARESOLVERR_INSTANCES = int(os.getenv("FLARESOLVERR_INSTANCES", "0"))
FLARESOLVERR_TIMEOUT = int(os.getenv("FLARESOLVERR_TIMEOUT", "60"))
FLARESOLVERR_MAX_RETRIES = int(os.getenv("FLARESOLVERR_MAX_RETRIES", "5"))
FLARESOLVERR_REQUEST_TIMEOUT_MS = int(os.getenv("FLARESOLVERR_REQUEST_TIMEOUT_MS", "90000"))

# ---------- NEW: chunked single‑sitemap mode ----------
SITEMAP_URL = os.getenv("SITEMAP_URL", "")          # process exactly this sitemap
URL_OFFSET   = int(os.getenv("URL_OFFSET", "0"))    # start index inside the sitemap
URL_LIMIT    = int(os.getenv("URL_LIMIT", "0"))     # max urls in chunk mode
CHUNK_ID     = os.getenv("CHUNK_ID", str(SITEMAP_OFFSET))  # unique chunk identifier
PRODUCT_URLS_FILE = os.getenv("PRODUCT_URLS_FILE", "").strip()
# ----------- URL COLLECTION MODE ENV VARS -----------
COLLECT_URLS_ONLY = os.getenv("COLLECT_URLS_ONLY", "0") == "1"
MASTER_URLS_FILE = os.getenv("MASTER_URLS_FILE", "cymax_all_urls.txt").strip()

# Output file name: use CHUNK_ID when in chunk/file mode, else fallback to offset
if PRODUCT_URLS_FILE or SITEMAP_URL:
    OUTPUT_CSV = f"cymax_products_{CHUNK_ID}.csv"
else:
    OUTPUT_CSV = f"cymax_products_{SITEMAP_OFFSET}.csv"

SCRAPED_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

# ================= LOGGER =================

def log(msg: str, level: str = "INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sys.stderr.write(f"[{timestamp}] [{level}] {msg}\n")
    sys.stderr.flush()

def sanitize_url_text(text: str) -> str:
    clean = re.sub(r"<[^>]+>", " ", text or "")
    m = re.search(r"https?://[^\s\"'<>]+", clean)
    return m.group(0).strip() if m else ""

def build_flaresolverr_pool_from_base(base_url: str, instances: int) -> List[str]:
    if instances < 1 or not base_url:
        return []
    parsed = urlparse(base_url)
    if not parsed.scheme or not parsed.hostname:
        return []
    base_port = parsed.port or (443 if parsed.scheme == "https" else 80)
    path = parsed.path or "/v1"
    return [
        f"{parsed.scheme}://{parsed.hostname}:{base_port + i}{path}"
        for i in range(instances)
    ]

def parse_flaresolverr_urls() -> List[str]:
    urls = [u.strip() for u in FLARESOLVERR_URLS_ENV.split(",") if u.strip()]
    if urls:
        return urls
    if FLARESOLVERR_INSTANCES > 0 and FLARESOLVERR_URL:
        pooled = build_flaresolverr_pool_from_base(FLARESOLVERR_URL, FLARESOLVERR_INSTANCES)
        if pooled:
            return pooled
    if FLARESOLVERR_URL:
        return [FLARESOLVERR_URL]
    return []

def align_flaresolverr_hosts_with_workers(endpoints: List[str], workers: int) -> Tuple[List[str], int]:
    configured_workers = max(1, workers)
    if not endpoints:
        return endpoints, configured_workers
    if len(endpoints) >= configured_workers:
        return endpoints[:configured_workers], configured_workers
    return endpoints, len(endpoints)

FLARESOLVERR_URLS = parse_flaresolverr_urls()
FLARESOLVERR_URLS, EFFECTIVE_MAX_WORKERS = align_flaresolverr_hosts_with_workers(
    FLARESOLVERR_URLS,
    MAX_WORKERS,
)
thread_local = threading.local()
fs_assign_lock = threading.Lock()
fs_assign_count = 0

# ================= FLARESOLVERR SESSION =================

class FlareSolverrSession:
    def __init__(self, endpoint: str):
        self.endpoint = endpoint
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
                    "maxTimeout": FLARESOLVERR_REQUEST_TIMEOUT_MS,
                    "session": None,  # Create new session
                    "headers": self.headers
                }
                
                response = self.session.post(
                    self.endpoint,
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
                
                if response.status_code >= 500:
                    self._rotate_to_next_endpoint()
                log(f"FlareSolverr attempt {attempt + 1} failed for {url}: {response.status_code}")
                
            except requests.exceptions.Timeout:
                self._rotate_to_next_endpoint()
                log(f"FlareSolverr timeout on attempt {attempt + 1} for {url}")
            except requests.exceptions.ConnectionError:
                self._rotate_to_next_endpoint()
                log(f"FlareSolverr connection error on attempt {attempt + 1} for {url}")
            except Exception as e:
                log(f"FlareSolverr error on attempt {attempt + 1} for {url}: {e}")
            
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
        
        return None, 0

    def _rotate_to_next_endpoint(self):
        global fs_assign_count
        if len(FLARESOLVERR_URLS) <= 1:
            return
        with fs_assign_lock:
            endpoint = FLARESOLVERR_URLS[fs_assign_count % len(FLARESOLVERR_URLS)]
            fs_assign_count += 1
        if endpoint != self.endpoint:
            log(f"Switching FlareSolverr endpoint from {self.endpoint} to {endpoint}", "DEBUG")
            self.endpoint = endpoint

    def fetch(self, url: str) -> Optional[Tuple[str, int]]:
        """Fetch URL through FlareSolverr"""
        return self.flaresolverr_request(url, max_retries=FLARESOLVERR_MAX_RETRIES)

def get_thread_flaresolverr_session() -> FlareSolverrSession:
    global fs_assign_count
    if not hasattr(thread_local, "fs_session"):
        if not FLARESOLVERR_URLS:
            raise RuntimeError("FLARESOLVERR_URL or FLARESOLVERR_URLS is required")
        with fs_assign_lock:
            endpoint = FLARESOLVERR_URLS[fs_assign_count % len(FLARESOLVERR_URLS)]
            fs_assign_count += 1
        thread_local.fs_session = FlareSolverrSession(endpoint)
        log(f"Thread {threading.get_ident()} assigned FlareSolverr endpoint {endpoint}", "DEBUG")
    return thread_local.fs_session

def get_sitemap_from_robots_txt():
    try:
        robots_url = f"{CURR_URL}/robots.txt"
        content, status = get_thread_flaresolverr_session().fetch(robots_url)
        
        if content and status == 200:
            sitemap_url = None
            for line in content.split('\n'):
                if line.lower().startswith('sitemap:'):
                    sitemap_url = sanitize_url_text(line.split(':', 1)[1].strip())
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
    
    content, status = get_thread_flaresolverr_session().fetch(robots_url)
    if content and status == 200:
        lines = content.split('\n')
        crawl_delay = None
        sitemap_url = None
        
        for line in lines:
            line = line.strip()
            if line.lower().startswith('sitemap:'):
                parts = line.split(':', 1)
                if len(parts) > 1:
                    potential_url = sanitize_url_text(parts[1].strip())
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
        self.retry_delays = [1, 2, 4, 8]
        
    def _respect_rate_limit(self, crawl_delay=None):
        current_time = time.time()
        base_delay = crawl_delay if crawl_delay is not None else REQUEST_DELAY_BASE
        
        if self.request_count > 0:
            elapsed = current_time - self.last_request_time
            if base_delay > 0:
                min_delay = base_delay * 0.8
                max_delay = base_delay * 1.5
                target_delay = random.uniform(min_delay, max_delay)
                
                if elapsed < target_delay:
                    sleep_time = target_delay - elapsed
                    time.sleep(sleep_time)
        
        self.last_request_time = time.time()
        self.request_count += 1
        
        if base_delay > 0 and self.request_count % 20 == 0:
            long_pause = random.uniform(base_delay * 2, base_delay * 3)
            log(f"Taking longer pause after {self.request_count} requests: {long_pause:.1f}s")
            time.sleep(long_pause)
    
    def fetch(self, url: str, retry_count: int = 0, crawl_delay=None) -> Optional[str]:
        if retry_count >= len(self.retry_delays):
            log(f"Max retries exceeded for {url}")
            return None
        
        self._respect_rate_limit(crawl_delay)
        content, status = get_thread_flaresolverr_session().fetch(url)
        
        if content and status == 200:
            return content
        
        if status in [0, 403, 429, 500, 502, 503, 504]:
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

def get_thread_request_manager() -> RequestManager:
    if not hasattr(thread_local, "request_manager"):
        thread_local.request_manager = RequestManager()
    return thread_local.request_manager

def http_get(url: str, crawl_delay=None) -> Optional[str]:
    return get_thread_request_manager().fetch(url, crawl_delay=crawl_delay)

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



def parse_product_page(html, url):
    """Extract product data from HTML"""
    try:
        soup = BeautifulSoup(html, 'lxml')

        def abs_url(src):
            return urljoin(url, src) if src else ""

        product = {
            "product_id": "",
            "title": "",
            "price": "",
            "brand": "",
            "category": "",
            "sku": "",
            "mpn": "",
            "image": "",
            "availability": "Unknown",
        }

        # PRODUCT ID - from multiple sources
        # 1. From URL (fastest)
        url_match = PRODUCT_ID_URL_RE.search(url)
        if url_match:
            product["product_id"] = url_match.group(1)

        # 2. From product-id-label span
        if not product["product_id"]:
            id_span = soup.find("span", class_="product-id-label")
            if id_span:
                product["product_id"] = id_span.get_text(strip=True)

        # 3. From JSON in script
        if not product["product_id"]:
            id_match = PRODUCT_ID_JSON_RE.search(html)
            if id_match:
                product["product_id"] = id_match.group(1)

        # TITLE
        h1 = soup.find("h1", itemprop="name")
        if h1:
            product["title"] = h1.get_text(strip=True)[:200]
        elif soup.title:
            product["title"] = soup.title.get_text(strip=True)[:200]

        # PRICE
        price_el = soup.find(id="product-main-price")
        if price_el:
            price_text = price_el.get_text()
            price_match = re.search(r"([\d,]+\.?\d*)", price_text.replace("$", "").replace(",", ""))
            if price_match:
                product["price"] = price_match.group(1)

        # BRAND
        brand_meta = soup.find("meta", itemprop="brand")
        if brand_meta:
            product["brand"] = brand_meta.get("content", "")[:100]

        if not product["brand"]:
            brand_match = re.search(r'"brandName":\s*"([^"]+)"', html)
            if brand_match:
                product["brand"] = brand_match.group(1)[:100]

        # CATEGORY
        crumbs = soup.select(".breadcrumb a")
        if len(crumbs) >= 3:
            product["category"] = crumbs[-2].get_text(strip=True)[:100]

        # SKU / MPN
        sku_match = SKU_RE.search(html)
        if sku_match:
            product["sku"] = sku_match.group(1)[:100]
            product["mpn"] = product["sku"]

        # IMAGE
        main_img = soup.find("img", id="product-main-image")
        if main_img and main_img.get("src"):
            product["image"] = abs_url(main_img["src"])

        # AVAILABILITY
        if "Ships between" in html[:2000]:
            product["availability"] = "Available"

        return product

    except Exception as e:
        log(f"Parse error: {e}")
        return None
    
seen_lock = threading.Lock()
stats_lock = threading.Lock()

def process_product_data(product_url: str, writer, seen: set, stats: dict, crawl_delay=None):
    """FP-FC style processing flow with thread-safe seen/stats handling."""
    if product_url in seen:
        return
    with seen_lock:
        seen.add(product_url)

    try:
        html = http_get(product_url)
        if not html:
            with stats_lock:
                stats["errors"] += 1
                stats["urls_processed"] += 1
            return

        product = parse_product_page(html, product_url)
        if not product or not product.get("product_id"):
            with stats_lock:
                stats["errors"] += 1
                stats["urls_processed"] += 1
            return

        # Write to CSV
        row = [
            product_url,                            # Ref Product URL
            product["product_id"],                  # Ref Product ID
            product["product_id"],                  # Ref Variant ID
            product["category"],                    # Ref Category
            "",                                     # Ref Category URL
            product["brand"],                       # Ref Brand Name
            product["title"],                       # Ref Product Name
            product["sku"],                         # Ref SKU
            product["mpn"],                         # Ref MPN
            "",                                     # Ref GTIN
            product["price"],                       # Ref Price
            product["image"],                       # Ref Main Image
            1,                                      # Ref Quantity
            "Default",                             # Ref Group Attr 1
            "default",                             # Ref Group Attr 2
            product["availability"],               # Ref Status
            SCRAPED_DATE                           # Date Scraped
        ]

        with csv_lock:
            writer.writerow(row)

        with stats_lock:
            stats["products_fetched"] += 1
            stats["urls_processed"] += 1

            if stats["urls_processed"] % 1000 == 0:
                elapsed = time.time() - start_time if 'start_time' in globals() else 1
                rate = stats["products_fetched"] / elapsed if elapsed > 0 else 0
                log(
                    f"✓ Processed: {stats['products_fetched']} | "
                    f"Failed: {stats['errors']} | Rate: {rate:.1f}/s"
                )

    except Exception as e:
        with stats_lock:
            stats["errors"] += 1
            stats["urls_processed"] += 1
        log(f"Error: {product_url[-50:]}... - {str(e)[:50]}")
CSV_HEADER = [
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
]

# ================= MAIN =================

def main():
    crawl_delay, robots_sitemap = check_robots_txt()
    crawl_delay = 0  # Override for this site (adjust if needed)

    # ------------------------------------------------------------------
    # MODE 0: Process URL list file with offset + limit (workflow chunk mode)
    # ------------------------------------------------------------------
    if PRODUCT_URLS_FILE:
        log("=" * 60)
        log("SCRAPER STARTED – URL FILE CHUNK MODE")
        log(f"PRODUCT_URLS_FILE: {PRODUCT_URLS_FILE}")
        log(f"URL_OFFSET: {URL_OFFSET}")
        log(f"URL_LIMIT: {URL_LIMIT}")
        log(f"CHUNK_ID: {CHUNK_ID}")
        log(f"OUTPUT_CSV: {OUTPUT_CSV}")
        log("=" * 60)

        try:
            if PRODUCT_URLS_FILE.lower().endswith(".csv"):
                with open(PRODUCT_URLS_FILE, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    if reader.fieldnames and "url" in [h.strip().lower() for h in reader.fieldnames]:
                        all_urls = [str(row.get("url", "")).strip() for row in reader if str(row.get("url", "")).strip()]
                    else:
                        f.seek(0)
                        raw = list(csv.reader(f))
                        all_urls = [r[0].strip() for r in raw if r and r[0].strip() and r[0].strip().startswith("http")]
            else:
                with open(PRODUCT_URLS_FILE, "r", encoding="utf-8") as f:
                    all_urls = [line.strip() for line in f if line.strip()]
        except Exception as e:
            log(f"❌ Failed to read PRODUCT_URLS_FILE: {e}", "ERROR")
            sys.exit(1)

        # Unique while preserving order
        all_product_urls = list(dict.fromkeys(all_urls))
        start = max(URL_OFFSET, 0)
        end = start + URL_LIMIT if URL_LIMIT > 0 else len(all_product_urls)
        urls_to_process = all_product_urls[start:end]

        log(
            f"URL file has {len(all_product_urls)} unique URLs. "
            f"Processing {len(urls_to_process)} (offset={start}, limit={URL_LIMIT if URL_LIMIT > 0 else 'all'})"
        )
        if not urls_to_process:
            log("No URLs to process in this chunk – exiting.")
            sys.exit(0)

        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(CSV_HEADER)

            seen = set()
            stats = {
                'sitemaps_processed': 0,
                'urls_processed': 0,
                'products_fetched': 0,
                'errors': 0
            }

            with ThreadPoolExecutor(max_workers=EFFECTIVE_MAX_WORKERS) as executor:
                semaphore = threading.Semaphore(MAX_QUEUE_SIZE)

                def submit_task(u):
                    semaphore.acquire()
                    future = executor.submit(process_product_data, u, writer, seen, stats, crawl_delay)
                    future.add_done_callback(lambda f: semaphore.release())

                for url in urls_to_process:
                    submit_task(url)

        log("=" * 60)
        log("URL FILE CHUNK STATISTICS")
        log("=" * 60)
        log(f"Chunk ID:           {CHUNK_ID}")
        log(f"URLs processed:     {stats['urls_processed']}")
        log(f"Products fetched:   {stats['products_fetched']}")
        log(f"Errors:             {stats['errors']}")
        if stats['urls_processed'] > 0:
            success_rate = (stats['products_fetched'] / stats['urls_processed']) * 100
            log(f"Success rate:       {success_rate:.1f}%")
        log(f"Chunk output saved: {OUTPUT_CSV}")
        log("=" * 60)
        return
    
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
                    and ('.htm' in e.text)
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
            writer.writerow(CSV_HEADER)

            seen = set()
            stats = {
                'sitemaps_processed': 1,
                'urls_processed': 0,
                'products_fetched': 0,
                'errors': 0
            }

            # Process URLs with ThreadPoolExecutor and bounded queue
            with ThreadPoolExecutor(max_workers=EFFECTIVE_MAX_WORKERS) as executor:
                semaphore = threading.Semaphore(MAX_QUEUE_SIZE)

                def submit_task(u):
                    semaphore.acquire()
                    future = executor.submit(process_product_data, u, writer, seen, stats, crawl_delay)
                    future.add_done_callback(lambda f: semaphore.release())

                for url in urls_to_process:
                    submit_task(url)

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
    # MODE X: Collect ALL product URLs from sitemap index (no scraping)
    # ------------------------------------------------------------------
    if COLLECT_URLS_ONLY:
        log("=" * 60)
        log("URL COLLECTION MODE – Collecting all product URLs only")
        log(f"Base URL: {CURR_URL}")
        log(f"Sitemap Index: {SITEMAP_INDEX}")
        log(f"Output file: {MASTER_URLS_FILE}")
        log("=" * 60)

        sitemap = SITEMAP_INDEX
        if robots_sitemap and robots_sitemap.startswith('http'):
            sitemap = robots_sitemap

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
            log("No sitemaps found in index", "ERROR")
            sys.exit(1)

        all_urls = []

        for i, sitemap_url in enumerate(sitemaps, 1):
            log(f"Collecting from sitemap {i}/{len(sitemaps)}: {sitemap_url}")
            xml = load_xml(sitemap_url, crawl_delay)
            if not xml:
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
                        and ('.htm' in e.text)
                    ]
                    if urls:
                        break

            all_urls.extend(urls)

        # Deduplicate while preserving order
        unique_urls = list(dict.fromkeys(all_urls))

        with open(MASTER_URLS_FILE, "w", encoding="utf-8") as f:
            for url in unique_urls:
                f.write(url + "\n")

        log("=" * 60)
        log(f"Total URLs collected: {len(unique_urls)}")
        log(f"Saved to: {MASTER_URLS_FILE}")
        log("=" * 60)

        sys.exit(0)
    # ------------------------------------------------------------------
    # MODE 2: Process a SITEMAP INDEX (original behaviour)
    # ------------------------------------------------------------------
    log("=" * 60)
    log("SCRAPER STARTED – SITEMAP INDEX MODE")
    log(f"FlareSolverr URLs: {', '.join(FLARESOLVERR_URLS) if FLARESOLVERR_URLS else 'none'}")
    log(f"Timestamp: {SCRAPED_DATE}")
    log(f"Base URL: {CURR_URL}")
    log(f"Sitemap Index: {SITEMAP_INDEX}")
    log(f"Sitemap Offset: {SITEMAP_OFFSET}")
    log(f"Max Sitemaps: {MAX_SITEMAPS if MAX_SITEMAPS > 0 else 'All'}")
    log(f"Max URLs per Sitemap: {MAX_URLS_PER_SITEMAP if MAX_URLS_PER_SITEMAP > 0 else 'All'}")
    log(f"Max Workers (configured): {MAX_WORKERS}")
    log(f"Max Workers (effective): {EFFECTIVE_MAX_WORKERS}")
    log(f"Request Delay: {REQUEST_DELAY_BASE}s")
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
        writer.writerow(CSV_HEADER)

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
                        and ('.htm' in e.text)
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

            with ThreadPoolExecutor(max_workers=EFFECTIVE_MAX_WORKERS) as executor:
                semaphore = threading.Semaphore(MAX_QUEUE_SIZE)

                def submit_task(u):
                    semaphore.acquire()
                    future = executor.submit(process_product_data, u, writer, seen, stats, crawl_delay)
                    future.add_done_callback(lambda f: semaphore.release())

                for url in urls:
                    submit_task(url)

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
    if FLARESOLVERR_URLS:
        for endpoint in FLARESOLVERR_URLS:
            log(f"Testing FlareSolverr connection at {endpoint}")
            try:
                test_response = requests.post(endpoint, json={"cmd": "sessions.list"}, timeout=10)
                if test_response.status_code == 200:
                    log(f"✓ FlareSolverr connection successful: {endpoint}")
                else:
                    log(f"⚠ FlareSolverr returned status {test_response.status_code} for {endpoint}")
            except Exception as e:
                log(f"⚠ FlareSolverr connection failed for {endpoint}: {e}")
                log("Continuing anyway, but requests may fail...")
    else:
        log("⚠ FLARESOLVERR_URL/FLARESOLVERR_URLS not set, requests will fail behind Cloudflare")

    main()