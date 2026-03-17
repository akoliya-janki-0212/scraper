import os
import csv
import time
import sys
import gc
import gzip
import threading
import requests
import re
import json
import urllib3
from typing import Optional, List, Dict, Any, Set
from datetime import datetime, timezone
from xml.etree import ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class BloomingDalesScraper:
    """Class-based scraper for bloomingdales.com"""
    
    def __init__(self):
        """Initialize the scraper with configuration from environment variables"""
        # Environment variables
        self.curr_url = os.getenv("CURR_URL", "https://www.bloomingdales.com/").rstrip("/")
        self.api_base_url = os.getenv("API_BASE_URL", "https://www.bloomingdales.com/xapi/digital/v1/product/").rstrip("/")
        self.sitemap_offset = int(os.getenv("SITEMAP_OFFSET", "0"))
        self.max_sitemaps = int(os.getenv("MAX_SITEMAPS", "0"))
        self.max_urls_per_sitemap = int(os.getenv("MAX_URLS_PER_SITEMAP", "0"))
        self.max_workers = int(os.getenv("MAX_WORKERS", "4"))
        self.request_delay = float(os.getenv("REQUEST_DELAY", "2.0"))
        
        # Optional: hardcoded sitemap URLs passed as env var (comma-separated)
        # If set, robots.txt fetch is skipped entirely
        self.sitemap_urls_override = os.getenv("SITEMAP_URLS_OVERRIDE", "")
        
        # Output file
        self.output_csv = f"bloomingdales_products_chunk_{self.sitemap_offset}.csv"
        self.scraped_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        
        # Statistics
        self.stats = {
            'sitemaps_processed': 0,
            'urls_processed': 0,
            'products_fetched': 0,
            'errors': 0
        }
        
        # Thread lock for CSV writing
        self.csv_lock = threading.Lock()
        
        # User-Agent can be overridden per job to avoid fingerprinting
        user_agent = os.getenv(
            "USER_AGENT",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        
        # Session for HTTP requests
        self.session = requests.Session()
        self.session.headers.update({
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'cache-control': 'max-age=0',
            'priority': 'u=0, i',
            'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36'
        })
        
        self.log("=" * 60)
        self.log("bloomingdales.com Scraper Initialized")
        self.log(f"Timestamp: {self.scraped_date}")
        self.log(f"Base URL: {self.curr_url}")
        self.log(f"API Base URL: {self.api_base_url}")
        self.log(f"Sitemap Offset: {self.sitemap_offset}")
        self.log(f"Max Sitemaps: {self.max_sitemaps if self.max_sitemaps > 0 else 'All'}")
        self.log(f"Max URLs per Sitemap: {self.max_urls_per_sitemap if self.max_urls_per_sitemap > 0 else 'All'}")
        self.log(f"Max Workers: {self.max_workers}")
        self.log(f"Request Delay: {self.request_delay}s")
        self.log(f"Sitemap Override: {'Yes' if self.sitemap_urls_override else 'No (will fetch robots.txt)'}")
        self.log("=" * 60)
    
    def log(self, msg: str, level: str = "INFO"):
        """Log message to stderr"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sys.stderr.write(f"[{timestamp}] [{level}] {msg}\n")
        sys.stderr.flush()
    
    def http_get(self, url: str, is_json: bool = False) -> Optional[str]:
        """HTTP GET request with retry logic and aggressive backoff for 429"""
        for attempt in range(3):
            try:
                # For API/JSON requests, override with JSON-specific headers
                headers = {
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
                    'cache-control': 'max-age=0',
                    'priority': 'u=0, i',
                    'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"macOS"',
                    'sec-fetch-dest': 'document',
                    'sec-fetch-mode': 'navigate',
                    'sec-fetch-site': 'none',
                    'sec-fetch-user': '?1',
                    'upgrade-insecure-requests': '1',
                    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                    "Referer": f"{self.curr_url}/",
                }
                r = self.session.get(url, headers=headers, timeout=15, verify=True)
                if r.status_code == 200:
                    self.log(f"Success fetching {url}", "DEBUG")
                    return r.text
                else:
                    self.log(f"Status {r.status_code} for {url}", "WARNING")
                    if r.status_code == 429:  # Rate limited
                        time.sleep(5)
            except requests.exceptions.Timeout:
                self.log(f"Timeout on attempt {attempt+1} for {url}", "WARNING")
                time.sleep(2)
            except Exception as e:
                self.log(f"Attempt {attempt+1} failed for {url}: {type(e).__name__}", "WARNING")
                time.sleep(1)
        return None
    
    def fetch_json(self, url: str) -> Optional[dict]:
        """Fetch JSON data from API with retry and backoff"""
        for attempt in range(5):
            try:
                headers = {
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
                    'cache-control': 'max-age=0',
                    'priority': 'u=0, i',
                    'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"macOS"',
                    'sec-fetch-dest': 'document',
                    'sec-fetch-mode': 'navigate',
                    'sec-fetch-site': 'none',
                    'sec-fetch-user': '?1',
                    'upgrade-insecure-requests': '1',
                    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                    "Referer": f"{self.curr_url}/",
                }
                response = self.session.get(url, headers=headers, timeout=15, verify=True)
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
                    wait_time = 30 * (attempt + 1)
                    self.log(f"Rate limited (429) on API {url}, waiting {wait_time}s (attempt {attempt+1}/5)", "WARNING")
                    time.sleep(wait_time)
                elif response.status_code in (503, 502, 504):
                    wait_time = 10 * (attempt + 1)
                    self.log(f"Server error {response.status_code} on API {url}, waiting {wait_time}s", "WARNING")
                    time.sleep(wait_time)
                else:
                    self.log(f"API fetch failed: {response.status_code} for {url}", "WARNING")
                    return None
            except Exception as e:
                self.log(f"Error fetching JSON from {url} (attempt {attempt+1}): {e}", "ERROR")
                time.sleep(5 * (attempt + 1))
        
        self.log(f"All JSON fetch attempts exhausted for {url}", "ERROR")
        return None
    
    def load_xml(self, url: str) -> Optional[ET.Element]:
        """Load XML from a URL, transparently decompressing .gz content."""
        raw: Optional[bytes] = None
        for attempt in range(3):
            try:
                raw = self.http_get_bytes(url)
                if raw:
                    break
            except Exception as e:
                self.log(f"Attempt {attempt + 1} for sitemap failed: {e}", "WARNING")
                time.sleep(2)

        if not raw:
            self.log(f"Failed to load XML from {url}", "ERROR")
            return None

        # ---- Decompress if gzip ----
        if raw[:2] == b'\x1f\x8b':
            self.log(f"Detected gzip content, decompressing: {url}", "DEBUG")
            try:
                raw = gzip.decompress(raw)
            except Exception as e:
                self.log(f"gzip decompression failed for {url}: {e}", "ERROR")
                return None

        # ---- Decode bytes → str ----
        try:
            data = raw.decode("utf-8")
        except UnicodeDecodeError:
            data = raw.decode("latin-1")

        # ---- Strip CDATA wrappers so ET can parse cleanly ----
        data = re.sub(r'<!\[CDATA\[(.*?)]]>', lambda m: m.group(1), data, flags=re.DOTALL)

        # ---- Ensure XML declaration ----
        if "<?xml" not in data[:100]:
            data = '<?xml version="1.0" encoding="UTF-8"?>\n' + data

        try:
            return ET.fromstring(data)
        except ET.ParseError as e:
            self.log(f"XML parsing failed for {url}: {e}", "ERROR")
            # Regex fallback — also handles any residual CDATA
            try:
                root = ET.Element("urlset")
                pattern = r'<loc>(?:<!\[CDATA\[)?(https?://[^<\]]+?)(?:]]>)?</loc>'
                for url_text in re.findall(pattern, data):
                    url_elem = ET.SubElement(root, "url")
                    loc_elem = ET.SubElement(url_elem, "loc")
                    loc_elem.text = url_text.strip()
                self.log(f"Regex fallback found {len(root)} URLs in {url}", "WARNING")
                return root
            except Exception as e2:
                self.log(f"Regex extraction also failed for {url}: {e2}", "ERROR")
                return None

    def http_get_bytes(self, url: str) -> Optional[bytes]:
        """GET raw bytes with up to 3 retries. Always appends ?bo=0.
        Used for sitemap fetching so .gz files can be decompressed before decoding."""
        for attempt in range(3):
            try:
                r = self.session.get(url, timeout=30, verify=True)
                if r.status_code == 200:
                    self.log(f"Success (bytes): {url}", "DEBUG")
                    return r.content
                self.log(f"Status {r.status_code} for {url}", "WARNING")
                if r.status_code == 429:
                    time.sleep(5)
            except requests.exceptions.Timeout:
                self.log(f"Timeout attempt {attempt + 1} for {url}", "WARNING")
                time.sleep(2)
            except Exception as e:
                self.log(f"Attempt {attempt + 1} failed for {url}: {type(e).__name__}", "WARNING")
                time.sleep(1)
        return None
    
    def get_sitemap_urls_from_robots(self) -> List[str]:
        """
        Fetch robots.txt and extract sitemap URLs for product sitemaps.
        Retries up to 5 times with progressive backoff on failure/rate-limit.
        """
        robots_url = f"{self.curr_url}/robots.txt"
        self.log(f"Fetching robots.txt from {robots_url}")
        
        content = None
        for attempt in range(5):
            content = self.http_get(robots_url)
            if content:
                break
            wait = 20 * (attempt + 1)  # 20s, 40s, 60s, 80s, 100s
            self.log(f"robots.txt fetch failed, retrying in {wait}s (attempt {attempt+1}/5)", "WARNING")
            time.sleep(wait)
        
        if not content:
            self.log("Failed to fetch robots.txt after all retries", "ERROR")
            return []
        
        sitemap_urls = []
        for line in content.split('\n'):
            line = line.strip()
            if line.lower().startswith('sitemap:'):
                print(line)
                sitemap_urls = line.split(':', 1)[1].strip()
        self.log(f"Found {len(sitemap_urls)} product sitemaps")
        return sitemap_urls
    
    def extract_product_urls_from_sitemap(self, sitemap_url: str) -> List[str]:
        """Extract product URLs from a sitemap. Converts .gz to .xml automatically."""
        xml_url = sitemap_url
        self.log(f"Loading sitemap from: {xml_url}", "DEBUG")
        
        xml = self.load_xml(xml_url)
        if not xml:
            return []
        
        ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        urls = []
        
        for path in [".//ns:url/ns:loc", ".//url/loc", ".//loc"]:
            if "ns:" in path:
                elements = xml.findall(path, ns)
            else:
                elements = xml.findall(path)
            
            if elements:
                for elem in elements:
                    if elem.text and '/product/' in elem.text:
                        urls.append(elem.text.strip())
                if urls:
                    break
        
        self.log(f"Found {len(urls)} product URLs in {sitemap_url}")
        return urls
    
    def extract_product_identifier(self, product_url: str) -> Optional[str]:
        """
        Extract product identifier from URL.
        Example: https://www.bloomingdales.com/shop/product/lamarque-lace-maxi-skirt?ID=5827118
        Returns: 5827118
        """
        parsed = urlparse(product_url)
        query_params = parse_qs(parsed.query)
        product_id = query_params.get("ID")
        if product_id and product_id[0]:
            return product_id[0].strip()
    
    def get_all_product_info(self, product):
        # -------- SAFE PRODUCT EXTRACTION -------- #

        product_id = product.get('id', '')

        # --- Basic Info ---
        detail = product.get('detail', {})
        division_data = product.get('division', {})
        department_data = product.get('department', {})

        name = detail.get('name', '')
        complete_name = detail.get('completeName', '')

        brand_data = detail.get('brand', {})
        brand = brand_data.get('name', '')
        brand_id = brand_data.get('id', '')

        division = division_data.get('name', '')
        department = department_data.get('departmentName', '')

        top_category = (
            product.get('relationships', {})
                .get('taxonomy', {})
                .get('categories', [{}])[0]
                .get('name', '')
        )

        product_url = product.get('identifier', {}).get('productUrl', '')

        # --- Pricing ---
        pricing = product.get('pricing', {}).get('price', {})
        tiered_price = pricing.get('tieredPrice', [{}])[0]
        price_values = tiered_price.get('values', [{}])[0]

        price = price_values.get('value', '')
        formatted_price = price_values.get('formattedValue', '')

        # --- Color / Variant ---
        color_data = product.get('traits', {}).get('colors', {})
        color_id = color_data.get('selectedColor', '')

        color_map = color_data.get('colorMap', {})
        selected_color_data = color_map.get(color_id, {})

        color = selected_color_data.get('name', '')

        # --- UPC / SKU (dynamic) ---
        upcs = product.get('relationships', {}).get('upcs', {})
        first_upc = next(iter(upcs.values()), {})

        upc_id = first_upc.get('id', '')
        upc_number = first_upc.get('identifier', {}).get('upcNumber', '')
        sku = first_upc.get('markStyleCode', '')

        # --- Dimensions / Materials / Features ---
        dimensions = detail.get('dimensionsBulletText', [])
        materials = detail.get('materialsAndCare', [])
        features = detail.get('bulletText', [])

        # --- Image ---
        image = (
            product.get('imagery', {})
                .get('images', [{}])[0]
                .get('filePath', '')
        )

        # -------- FINAL STRUCTURED DICTIONARY -------- #

        extracted_product = {
            "product_id": product_id,
            "name": name,
            "complete_name": complete_name,
            "brand": brand,
            "brand_id": brand_id,
            "division": division,
            "department": department,
            "top_category": top_category,
            "product_url": product_url,
            "price": price,
            "formatted_price": formatted_price,
            "color": color,
            "color_id": color_id,
            "upc_id": upc_id,
            "upc_number": upc_number,
            "sku": sku,
            "dimensions": dimensions,
            "materials": materials,
            "features": features,
            "image": image
        }
        
        return extracted_product
    
    def extract_product_data(self, api_data: dict, product_url: str) -> List[Dict]:
        """Extract product data from API response."""
        try:
            if not api_data or not isinstance(api_data, dict):
                self.log(f"Invalid API data for {product_url}", "ERROR")
                return []
            
            product = next(iter(api_data.get('product')), None)

            product_info = self.get_all_product_info(product)            
            return [product_info]
            
        except Exception as e:
            self.log(f"Error extracting product data for {product_url}: {e}", "ERROR")
            return []
    
    def process_product(self, product_url: str, seen: Set[str], writer) -> None:
        """Process a single product URL."""
        if product_url in seen:
            return
        seen.add(product_url)
        
        self.log(f"Processing: {product_url}", "DEBUG")
        
        identifier = self.extract_product_identifier(product_url)
        if not identifier:
            self.stats['errors'] += 1
            self.log(f"No identifier found for {product_url}", "ERROR")
            return
        
        api_url = f"{self.api_base_url}/{identifier.upper()}"
        self.log(f"API URL: {api_url}", "DEBUG")
        
        api_data = self.fetch_json(api_url)
        if not api_data:
            self.stats['errors'] += 1
            self.log(f"No API data for {identifier}", "ERROR")
            return
        
        products = self.extract_product_data(api_data, product_url)
        
        for product in products:
            if not product.get('product_id'):
                continue
            
            try:
                row = [
                    product["product_id"],
                    product["name"],
                    product["complete_name"],
                    product["brand"],
                    product["brand_id"],
                    product["division"],
                    product["department"],
                    product["top_category"],
                    product["product_url"],
                    product["price"],
                    product["formatted_price"],
                    product["color"],
                    product["color_id"],
                    product["upc_id"],
                    product["upc_number"],
                    product["sku"],
                    product["dimensions"],
                    product["materials"],
                    product["features"],
                    self.normalize_image_url(product["image"]),
                    self.scraped_date,
                ]
                
                with self.csv_lock:
                    writer.writerow(row)
                
                self.stats['products_fetched'] += 1
                self.log(f"Fetched: {str(product['name'])[:50]}", "INFO")
                
            except Exception as e:
                self.log(f"Error creating row for {product_url}: {e}", "ERROR")
                self.stats['errors'] += 1
        
        time.sleep(self.request_delay)
        self.stats['urls_processed'] += 1
    
    def normalize_image_url(self, url: str) -> str:
        """Normalize image URL to absolute form."""
        if not url:
            return ""
        if url.startswith("//"):
            return "https:" + url
        elif url.startswith("/"):
            return f"{self.curr_url}{url}"
        elif not url.startswith("http"):
            return f"https://{url}"
        return url
    
    def run(self):
        """Main execution method."""
        try:
            # Cold-start delay: stagger parallel jobs to avoid simultaneous robot fetches
            cold_start_delay = float(os.getenv("COLD_START_DELAY", "0"))
            if cold_start_delay > 0:
                self.log(f"Cold-start delay: sleeping {cold_start_delay}s before starting...")
                time.sleep(cold_start_delay)
            
            # Step 1: Get sitemap URLs
            sitema_index = self.get_sitemap_urls_from_robots()
            index = self.load_xml(sitema_index)
            
            if index is None:
                self.log("Failed to load sitemap index", "ERROR")
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
            
            sitemaps = [url for url in sitemaps if "_pdp_" in url]
            # If still no sitemaps, try regex
            if not sitemaps:
                self.log("No sitemaps found with XML parsing, trying regex", "WARNING")
                return None
            
            # Apply offset and limit
            if self.sitemap_offset >= len(sitemaps):
                self.log(f"Offset {self.sitemap_offset} exceeds total sitemaps ({len(sitemaps)})", "WARNING")
                sys.exit(0)
            
            end_index = self.sitemap_offset + self.max_sitemaps if self.max_sitemaps > 0 else len(sitemaps)
            sitemaps_to_process = sitemaps[self.sitemap_offset:end_index]
            
            self.log(f"Total sitemaps found: {len(sitemaps)}")
            self.log(f"Sitemaps to process: {len(sitemaps_to_process)}")
                        
            # Step 2: Open CSV for writing
            with open(self.output_csv, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "product_id",
                    "name",
                    "complete_name",
                    "brand",
                    "brand_id",
                    "division",
                    "department",
                    "top_category",
                    "product_url",
                    "price",
                    "formatted_price",
                    "color",
                    "color_id",
                    "upc_id",
                    "upc_number",
                    "sku",
                    "dimensions",
                    "materials",
                    "features",
                    "image",
                    "scraped_date",
                ])
                
                seen = set()
                
                # Step 3: Process each sitemap
                for sitemap_url in sitemaps_to_process:
                    self.stats['sitemaps_processed'] += 1
                    self.log(
                        f"Processing sitemap {self.stats['sitemaps_processed']}/{len(sitemaps_to_process)}: {sitemap_url}"
                    )
                    
                    urls = self.extract_product_urls_from_sitemap(sitemap_url)
                    
                    if self.max_urls_per_sitemap > 0:
                        original_count = len(urls)
                        urls = urls[:self.max_urls_per_sitemap]
                        self.log(f"Limited to {len(urls)}/{original_count} URLs")
                    else:
                        self.log(f"Found {len(urls)} product URLs")
                    
                    if not urls:
                        self.log(f"No product URLs found in {sitemap_url}", "WARNING")
                        continue
                    
                    with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                        futures = [
                            executor.submit(self.process_product, url, seen, writer)
                            for url in urls
                        ]
                        for future in as_completed(futures):
                            try:
                                future.result()
                            except Exception as e:
                                self.log(f"Error in thread: {e}", "ERROR")
                                self.stats['errors'] += 1
                    
                    gc.collect()
            
            self.print_statistics()
            
        except KeyboardInterrupt:
            self.log("Scraping interrupted by user", "WARNING")
            self.print_statistics()
        except Exception as e:
            self.log(f"Fatal error: {e}", "ERROR")
            self.print_statistics()
    
    def print_statistics(self):
        """Print scraping statistics."""
        self.log("=" * 60)
        self.log("SCRAPING STATISTICS")
        self.log("=" * 60)
        self.log(f"Sitemaps processed: {self.stats['sitemaps_processed']}")
        self.log(f"URLs processed: {self.stats['urls_processed']}")
        self.log(f"Products fetched: {self.stats['products_fetched']}")
        self.log(f"Errors: {self.stats['errors']}")
        if self.stats['urls_processed'] > 0:
            success_rate = (self.stats['products_fetched'] / self.stats['urls_processed']) * 100
            self.log(f"Success rate: {success_rate:.1f}%")
        self.log("=" * 60)
        self.log(f"Output saved to: {self.output_csv}")
        self.log("=" * 60)


def main():
    """Entry point"""
    if not os.getenv("CURR_URL"):
        print("Error: CURR_URL environment variable is required", file=sys.stderr)
        sys.exit(1)
    
    scraper = BloomingDalesScraper()
    scraper.run()


if __name__ == "__main__":
    main()