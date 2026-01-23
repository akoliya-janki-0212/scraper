#!/usr/bin/env python3
"""
Parallel Shopify Sitemap Scraper
Supports Cloudflare-protected sites with automatic bypass
"""

import os
import sys
import time
import json
import csv
import re
import random
from datetime import datetime
from typing import Optional, List, Dict, Any, Set
from urllib.parse import urlparse, urljoin
import xml.etree.ElementTree as ET
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor

# Cloudflare bypass imports
import cloudscraper
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import undetected_chromedriver as uc
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

# ================= CONFIGURATION =================

# Environment variables with defaults
CURR_URL = os.getenv('CURR_URL', '').rstrip('/')
SITEMAP_OFFSET = int(os.getenv('SITEMAP_OFFSET') or 0)
MAX_SITEMAPS = int(os.getenv('MAX_SITEMAPS') or 0)
MAX_URLS_PER_SITEMAP = int(os.getenv('MAX_URLS_PER_SITEMAP') or 0)
USE_SELENIUM = os.getenv('USE_SELENIUM', 'false').lower() == 'true'
MAX_WORKERS = int(os.getenv('MAX_WORKERS') or 5)
REQUEST_TIMEOUT = 30

# Derived constants
SITEMAP_INDEX = f"{CURR_URL}/sitemap.xml" if CURR_URL else ""
OUTPUT_CSV = f"products_chunk_{SITEMAP_OFFSET}.csv"

# ================= LOGGER =================

def log_msg(msg: str) -> None:
    """Thread-safe logging to stderr"""
    timestamp = datetime.now().strftime('%H:%M:%S')
    sys.stderr.write(f"[{timestamp}] {msg}\n")
    sys.stderr.flush()

# ================= HTTP CLIENT WITH CLOUDFLARE BYPASS =================

class CloudflareBypassClient:
    """Handles HTTP requests with Cloudflare bypass capabilities"""
    
    def __init__(self):
        self.session = None
        self.driver = None
        self.use_selenium = USE_SELENIUM
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]
        self._init_session()
    
    def _init_session(self) -> None:
        """Initialize HTTP session with retry logic"""
        if self.use_selenium:
            log_msg("Using Selenium for Cloudflare bypass")
            options = uc.ChromeOptions()
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)
            
            self.driver = uc.Chrome(options=options)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        else:
            # Use cloudscraper for JavaScript challenge solving
            log_msg("Using cloudscraper for Cloudflare bypass")
            self.session = cloudscraper.create_scraper(
                browser={
                    'browser': 'chrome',
                    'platform': 'windows',
                    'mobile': False
                },
                delay=10,
                interpreter='nodejs'
            )
            
            # Add retry logic
            retry_strategy = Retry(
                total=3,
                backoff_factor=1,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["GET", "POST"]
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            self.session.mount("http://", adapter)
            self.session.mount("https://", adapter)
            
            # Set headers
            self.session.headers.update({
                'User-Agent': random.choice(self.user_agents),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            })
    
    def http_get(self, url: str, max_retries: int = 3) -> Optional[str]:
        """Fetch URL content with Cloudflare bypass"""
        for attempt in range(max_retries):
            try:
                if self.use_selenium and self.driver:
                    log_msg(f"Selenium GET: {url}")
                    self.driver.get(url)
                    
                    # Wait for page to load (check for common Cloudflare elements)
                    try:
                        WebDriverWait(self.driver, 10).until(
                            EC.presence_of_element_located((By.TAG_NAME, "body"))
                        )
                        
                        # Check if we're on a Cloudflare challenge page
                        page_source = self.driver.page_source
                        if "challenge" in page_source.lower() or "cloudflare" in page_source.lower():
                            log_msg("Cloudflare challenge detected, waiting...")
                            time.sleep(5)
                            continue
                            
                        return page_source
                    except Exception as e:
                        log_msg(f"Selenium wait error: {e}")
                        continue
                
                elif self.session:
                    log_msg(f"HTTP GET: {url}")
                    response = self.session.get(
                        url,
                        timeout=REQUEST_TIMEOUT,
                        allow_redirects=True
                    )
                    
                    # Check for Cloudflare challenges
                    if response.status_code == 403 or "cloudflare" in response.text.lower():
                        log_msg(f"Cloudflare challenge detected (status: {response.status_code})")
                        if attempt < max_retries - 1:
                            time.sleep(5 * (attempt + 1))
                            continue
                    
                    response.raise_for_status()
                    return response.text
                
            except Exception as e:
                log_msg(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 * (attempt + 1))
        
        log_msg(f"Failed to fetch URL after {max_retries} attempts: {url}")
        return None
    
    def close(self) -> None:
        """Clean up resources"""
        if self.driver:
            self.driver.quit()
        if self.session:
            self.session.close()

# Global HTTP client instance
http_client = CloudflareBypassClient()

# ================= XML/JSON PARSING =================

def load_xml(url: str) -> Optional[ET.Element]:
    """Load and parse XML from URL"""
    content = http_client.http_get(url)
    if not content:
        return None
    
    try:
        # Remove XML declaration if present to help parsing
        if content.startswith('<?xml'):
            content = content[content.find('?>') + 2:]
        
        # Parse XML
        root = ET.fromstring(content)
        
        # Handle namespaces
        for elem in root.iter():
            if '}' in elem.tag:
                elem.tag = elem.tag.split('}', 1)[1]
        
        return root
    except Exception as e:
        log_msg(f"Failed to parse XML from {url}: {e}")
        return None

def fetch_json(url: str) -> Optional[Dict[str, Any]]:
    """Fetch and parse JSON from URL"""
    content = http_client.http_get(url)
    if not content:
        return None
    
    try:
        return json.loads(content)
    except Exception as e:
        log_msg(f"Failed to parse JSON from {url}: {e}")
        return None

def normalize_image_url(url: str) -> str:
    """Normalize image URL"""
    if not url:
        return ""
    
    if url.startswith('//'):
        return f"https:{url}"
    elif url.startswith('/'):
        return f"{CURR_URL}{url}"
    
    return url

# ================= PRODUCT PROCESSING =================

def process_product(url: str, csv_writer, seen_urls: Set[str], lock=None) -> None:
    """Process a single product URL"""
    # Thread-safe check for seen URLs
    if lock:
        with lock:
            if url in seen_urls:
                return
            seen_urls.add(url)
    elif url in seen_urls:
        return
    
    log_msg(f"Product: {url}")
    
    # Fetch product JSON (Shopify stores use .js extension)
    product_url = url.rstrip('/') + '.js'
    product = fetch_json(product_url)
    
    if not product or 'variants' not in product or not product['variants']:
        log_msg(f"Invalid product JSON or no variants: {url}")
        return
    
    # Extract product data
    product_id = str(product.get('id', ''))
    product_title = product.get('title', '').strip()
    vendor = product.get('vendor', '').strip()
    product_type = product.get('type', '').strip()
    handle = product.get('handle', '').strip()
    
    # Get options
    options = product.get('options', [])
    option_names = [opt.get('name', '').strip() for opt in options]
    
    # Get main image
    images = product.get('featured_image', '')
    if images:
        images = normalize_image_url(images)
    
    log_msg(f"Variants found: {len(product['variants'])}")
    
    # Process each variant
    for variant in product['variants']:
        variant_id = str(variant.get('id', ''))
        variant_title = variant.get('title', '').strip()
        sku = variant.get('sku', '').strip()
        barcode = variant.get('barcode', '').strip()
        
        # Extract option values
        option_values = [
            variant.get('option1', ''),
            variant.get('option2', ''),
            variant.get('option3', '')
        ]
        
        variant_price = variant.get('price', '0.00')
        available = '1' if variant.get('available', False) else '0'
        variant_url = f"{url.rstrip('/')}?variant={variant_id}"
        
        # Write CSV row
        row = [
            product_id, product_title, vendor, product_type, handle,
            variant_id, variant_title, sku, barcode,
            option_names[0] if len(option_names) > 0 else '',
            option_values[0] if len(option_values) > 0 else '',
            option_names[1] if len(option_names) > 1 else '',
            option_values[1] if len(option_values) > 1 else '',
            option_names[2] if len(option_names) > 2 else '',
            option_values[2] if len(option_values) > 2 else '',
            variant_price, available, variant_url, images
        ]
        
        if lock:
            with lock:
                csv_writer.writerow(row)
        else:
            csv_writer.writerow(row)
    
    # Respectful delay between requests
    time.sleep(0.15)

# ================= SITEMAP PROCESSING =================

def get_sitemap_urls(sitemap_url: str) -> List[str]:
    """Extract URLs from a sitemap XML"""
    root = load_xml(sitemap_url)
    if not root:
        return []
    
    # Find all URL locations
    urls = []
    namespace = ''
    
    # Handle namespaces
    for elem in root.iter():
        if '}' in elem.tag:
            namespace = '{' + elem.tag.split('}', 1)[0] + '}'
            break
    
    # Find all loc elements
    for elem in root.iter(f'{namespace}loc'):
        urls.append(elem.text.strip())
    
    return urls

def process_sitemap(sitemap_url: str, csv_writer, seen_urls: Set[str], 
                   max_urls: int = 0) -> int:
    """Process a single sitemap"""
    log_msg(f"Loading sitemap: {sitemap_url}")
    
    urls = get_sitemap_urls(sitemap_url)
    if not urls:
        log_msg(f"No URLs found in sitemap: {sitemap_url}")
        return 0
    
    if max_urls > 0:
        urls = urls[:max_urls]
    
    log_msg(f"URLs in sitemap: {len(urls)}")
    
    # Process URLs with thread pool
    processed = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for url in urls:
            future = executor.submit(process_product, url, csv_writer, seen_urls)
            futures.append(future)
        
        # Wait for all tasks to complete
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
                processed += 1
            except Exception as e:
                log_msg(f"Error processing product: {e}")
    
    return processed

# ================= MAIN FUNCTION =================

def main() -> None:
    """Main scraping function"""
    log_msg("Scraper started")
    log_msg(f"Base URL: {CURR_URL}")
    log_msg(f"Sitemap offset: {SITEMAP_OFFSET}")
    log_msg(f"Max sitemaps: {MAX_SITEMAPS if MAX_SITEMAPS > 0 else 'ALL'}")
    log_msg(f"Max URLs per sitemap: {MAX_URLS_PER_SITEMAP if MAX_URLS_PER_SITEMAP > 0 else 'ALL'}")
    log_msg(f"Max workers: {MAX_WORKERS}")
    log_msg(f"Using Selenium: {USE_SELENIUM}")
    
    if not CURR_URL:
        log_msg("ERROR: CURR_URL environment variable not set")
        sys.exit(1)
    
    # Load sitemap index
    log_msg(f"Loading sitemap index: {SITEMAP_INDEX}")
    index_root = load_xml(SITEMAP_INDEX)
    
    if not index_root:
        log_msg("Failed to load sitemap index")
        sys.exit(1)
    
    # Extract sitemap URLs
    sitemap_urls = []
    namespace = ''
    
    # Find namespace
    for elem in index_root.iter():
        if '}' in elem.tag:
            namespace = '{' + elem.tag.split('}', 1)[0] + '}'
            break
    
    # Find all sitemap loc elements
    for elem in index_root.iter(f'{namespace}loc'):
        sitemap_urls.append(elem.text.strip())
    
    # Apply offset and limit
    if SITEMAP_OFFSET > 0:
        sitemap_urls = sitemap_urls[SITEMAP_OFFSET:]
    
    if MAX_SITEMAPS > 0:
        sitemap_urls = sitemap_urls[:MAX_SITEMAPS]
    
    log_msg(f"Sitemaps to process: {len(sitemap_urls)}")
    
    # Open CSV file
    csv_file = open(OUTPUT_CSV, 'w', newline='', encoding='utf-8')
    csv_writer = csv.writer(csv_file)
    
    # Write header
    csv_writer.writerow([
        'product_id', 'product_title', 'vendor', 'type', 'handle',
        'variant_id', 'variant_title', 'sku', 'barcode',
        'option_1_name', 'option_1_value',
        'option_2_name', 'option_2_value',
        'option_3_name', 'option_3_value',
        'variant_price', 'available', 'variant_url', 'image_url'
    ])
    
    # Track seen URLs
    seen_urls = set()
    total_processed = 0
    
    try:
        # Process each sitemap
        for sitemap_url in sitemap_urls:
            processed = process_sitemap(
                sitemap_url,
                csv_writer,
                seen_urls,
                MAX_URLS_PER_SITEMAP if MAX_URLS_PER_SITEMAP > 0 else 0
            )
            total_processed += processed
            log_msg(f"Processed {processed} URLs from sitemap")
            
            # Force garbage collection
            import gc
            gc.collect()
    
    except KeyboardInterrupt:
        log_msg("Interrupted by user")
    except Exception as e:
        log_msg(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Cleanup
        csv_file.close()
        http_client.close()
        
        log_msg(f"Chunk completed: {OUTPUT_CSV}")
        log_msg(f"Total URLs processed: {total_processed}")

if __name__ == "__main__":
    main()