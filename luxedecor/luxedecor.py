import os
import csv
import time
import sys
import gc
import threading
import requests
import re
import json
import urllib3
from typing import Optional, List, Dict, Any, Set
from datetime import datetime, timezone
from xml.etree import ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class LuxeDecorScraper:
    """Class-based scraper for luxedecor.com"""
    
    def __init__(self):
        """Initialize the scraper with configuration from environment variables"""
        # Environment variables
        self.curr_url = os.getenv("CURR_URL", "https://www.luxedecor.com").rstrip("/")
        self.api_base_url = os.getenv("API_BASE_URL", "https://www.luxedecor.com/api/product").rstrip("/")
        self.sitemap_offset = int(os.getenv("SITEMAP_OFFSET", "0"))
        self.max_sitemaps = int(os.getenv("MAX_SITEMAPS", "0"))
        self.max_urls_per_sitemap = int(os.getenv("MAX_URLS_PER_SITEMAP", "0"))
        self.max_workers = int(os.getenv("MAX_WORKERS", "4"))
        self.request_delay = float(os.getenv("REQUEST_DELAY", "2.0"))
        
        # Optional: hardcoded sitemap URLs passed as env var (comma-separated)
        # If set, robots.txt fetch is skipped entirely
        self.sitemap_urls_override = os.getenv("SITEMAP_URLS_OVERRIDE", "")
        
        # Output file
        self.output_csv = f"luxedecor_products_chunk_{self.sitemap_offset}.csv"
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
        self.log("LuxeDecor.com Scraper Initialized")
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
        for attempt in range(3):  # increased from 3 to 5
            try:
                if is_json:
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
                else:
                    response = self.session.get(url, timeout=15, verify=True)
                
                if response.status_code == 200:
                    return response.text
                elif response.status_code == 429:
                    # Progressive backoff: 30s, 60s, 90s, 120s, 150s
                    wait_time = 30 * (attempt + 1)
                    self.log(f"Rate limited (429) on {url}, waiting {wait_time}s (attempt {attempt+1}/5)", "WARNING")
                    time.sleep(wait_time)
                elif response.status_code in (503, 502, 504):
                    wait_time = 3 * (attempt + 1)
                    self.log(f"Server error {response.status_code} on {url}, waiting {wait_time}s", "WARNING")
                    time.sleep(wait_time)
                else:
                    self.log(f"Status {response.status_code} for {url}", "WARNING")
                    time.sleep(2)
            except requests.exceptions.Timeout:
                self.log(f"Timeout on attempt {attempt+1} for {url}", "WARNING")
                time.sleep(5 * (attempt + 1))
            except Exception as e:
                self.log(f"Attempt {attempt+1} failed for {url}: {type(e).__name__}: {e}", "WARNING")
                time.sleep(2)
        
        self.log(f"All attempts exhausted for {url}", "ERROR")
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
        """Load and parse XML from URL"""
        data = None
        for attempt in range(5):
            try:
                data = self.http_get(url, is_json=False)
                if data:
                    break
                time.sleep(5 * (attempt + 1))
            except Exception as e:
                self.log(f"Attempt {attempt+1} for sitemap failed: {e}", "WARNING")
                time.sleep(5)
        
        if not data:
            self.log(f"Failed to load XML from {url}", "ERROR")
            return None
        
        try:
            if "<?xml" not in data[:100]:
                data = '<?xml version="1.0" encoding="UTF-8"?>\n' + data
            return ET.fromstring(data)
        except ET.ParseError as e:
            self.log(f"XML parsing failed for {url}: {e}", "ERROR")
            try:
                root = ET.Element("urlset")
                urls = re.findall(r'<loc>(https?://[^<]+)</loc>', data)
                for url_text in urls:
                    url_elem = ET.SubElement(root, "url")
                    loc_elem = ET.SubElement(url_elem, "loc")
                    loc_elem.text = url_text
                return root
            except Exception as e2:
                self.log(f"Regex extraction also failed: {e2}", "ERROR")
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
            if line.lower().startswith('sitemap:'):
                sitemap_url = line.split(':', 1)[1].strip()
                if '/sitemap-products-' in sitemap_url:
                    sitemap_urls.append(sitemap_url)
                    self.log(f"Found product sitemap: {sitemap_url}", "DEBUG")
        
        self.log(f"Found {len(sitemap_urls)} product sitemaps")
        return sitemap_urls
    
    def get_sitemap_urls(self) -> List[str]:
        """
        Get sitemap URLs either from the SITEMAP_URLS_OVERRIDE env var
        (comma-separated list, skips robots.txt entirely) or by fetching robots.txt.
        """
        if self.sitemap_urls_override:
            urls = [u.strip() for u in self.sitemap_urls_override.split(',') if u.strip()]
            self.log(f"Using {len(urls)} sitemap URLs from SITEMAP_URLS_OVERRIDE (skipping robots.txt)")
            return urls
        
        return self.get_sitemap_urls_from_robots()
    
    def convert_gz_to_xml_url(self, gz_url: str) -> str:
        """Convert .gz sitemap URL to .xml URL"""
        if gz_url.endswith('.gz'):
            return gz_url.replace('.gz', '.xml')
        return gz_url
    
    def extract_product_urls_from_sitemap(self, sitemap_url: str) -> List[str]:
        """Extract product URLs from a sitemap. Converts .gz to .xml automatically."""
        xml_url = self.convert_gz_to_xml_url(sitemap_url)
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
        Example: https://www.luxedecor.com/product/acme-furniture-bertie-end-table-casual-side-acf82842?phash=eff584
        Returns: ACF82842
        """
        parsed = urlparse(product_url)
        path = parsed.path
        last_segment = path.split('/')[-1]
        
        match = re.search(r'-([a-z0-9]+)$', last_segment, re.IGNORECASE)
        if match:
            return match.group(1).upper()
        
        if re.match(r'^[a-z0-9]+$', last_segment, re.IGNORECASE):
            return last_segment.upper()
        
        self.log(f"Could not extract identifier from URL: {product_url}", "WARNING")
        return None
    
    def get_group_attr_details(self, additional_data, identifier, value_fetcher):
        finish_value = None
        for spec in additional_data.get("specifications", []):
            if spec["name"] == identifier:
                values = spec.get("values", [])
                if values and len(values) == 1:
                    finish_value = values[0].get(value_fetcher)
                break
        return finish_value
    
    def fetch_product_additional_data(self, product_url: str) -> Optional[dict]:
        """Fetch additional (overview) data for a product."""
        identifier = self.extract_product_identifier(product_url)
        if not identifier:
            self.stats['errors'] += 1
            self.log(f"No identifier found for {product_url}", "ERROR")
            return None
        
        api_url = f"{self.api_base_url}/{identifier.upper()}/overview-data"
        self.log(f"Overview API URL: {api_url}", "DEBUG")
        
        api_data = self.fetch_json(api_url)
        if not api_data:
            self.stats['errors'] += 1
            self.log(f"No overview API data for {identifier}", "ERROR")
            return None
        
        return api_data
    
    def extract_product_data(self, api_data: dict, product_url: str) -> List[Dict]:
        """Extract product data from API response."""
        try:
            if not api_data or not isinstance(api_data, dict):
                self.log(f"Invalid API data for {product_url}", "ERROR")
                return []
            
            additional_data = self.fetch_product_additional_data(product_url)
            
            product_id   = api_data.get('itemProperties', {}).get('itemId', '')
            name         = api_data.get('itemProperties', {}).get('description', '')
            sku          = api_data.get('itemProperties', {}).get('sku', '')
            brand        = api_data.get('vendor', {}).get('name', '')
            price        = api_data.get('pricingProperties', {}).get('retailPrice', '')
            main_image   = ""
            category     = (
                api_data.get('mainCategory', {}).get('name', '') +
                " / " +
                api_data.get('subCategory', {}).get('name', '')
            )
            category_url = (
                api_data.get('mainCategory', {}).get('link', '') +
                " / " +
                api_data.get('subCategory', {}).get('link', '')
            )
            description   = additional_data.get('featureDescription', '') if additional_data else ""
            quantity      = api_data.get('stockProperties', {}).get('stockQty', '')
            status        = ""
            dimension_str = additional_data.get('dimension', '') if additional_data else ""
            group_attr_1  = self.get_group_attr_details(additional_data, 'finish', 'name') if additional_data else ""
            group_attr_2  = ""
            
            product_info = {
                'product_url':  product_url,
                'product_id':   product_id,
                'name':         name,
                'brand':        brand,
                'sku':          sku,
                'price':        price,
                'main_image':   main_image,
                'category':     category,
                'category_url': category_url,
                'description':  description,
                'dimensions':   dimension_str,
                'quantity':     quantity,
                'status':       status,
                'group_attr_1': group_attr_1,
                'group_attr_2': group_attr_2,
            }
            
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
                    product['product_url'],
                    product['product_id'],
                    product['category'],
                    product['category_url'],
                    product['brand'],
                    product['name'],
                    product['sku'],
                    '',  # MPN
                    '',  # GTIN
                    product['price'],
                    self.normalize_image_url(product['main_image']),
                    product['quantity'],
                    product['group_attr_1'],
                    product['group_attr_2'],
                    product['status'],
                    product['description'],
                    product['dimensions'],
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
            sitemap_urls = self.get_sitemap_urls()
            if not sitemap_urls:
                self.log("No product sitemaps found", "ERROR")
                return
            
            # Apply offset and limit
            if self.sitemap_offset >= len(sitemap_urls):
                self.log(f"Offset {self.sitemap_offset} exceeds total sitemaps ({len(sitemap_urls)})", "ERROR")
                return
            
            end_index = self.sitemap_offset + self.max_sitemaps if self.max_sitemaps > 0 else len(sitemap_urls)
            sitemaps_to_process = sitemap_urls[self.sitemap_offset:end_index]
            
            self.log(f"Total sitemaps available: {len(sitemap_urls)}")
            self.log(f"Processing {len(sitemaps_to_process)} sitemaps: {sitemaps_to_process}")
            
            # Step 2: Open CSV for writing
            with open(self.output_csv, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "Product URL",
                    "Product ID",
                    "Category",
                    "Category URL",
                    "Brand",
                    "Product Name",
                    "SKU",
                    "MPN",
                    "GTIN",
                    "Price",
                    "Main Image",
                    "Quantity",
                    "group_attr_1",
                    "group_attr_2",
                    "Status",
                    "Description",
                    "Dimensions",
                    "Date Scraped",
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
    
    scraper = LuxeDecorScraper()
    scraper.run()


if __name__ == "__main__":
    main()