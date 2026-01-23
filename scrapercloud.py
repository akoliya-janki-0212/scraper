import json
import csv
import time
import random
import sys
from typing import Optional, Dict, List, Any
from datetime import datetime
from urllib.parse import urljoin, urlparse
import xml.etree.ElementTree as ET
import re
import os
from io import StringIO

# Enhanced Cloudflare bypass imports
import cloudscraper
import requests
from bs4 import BeautifulSoup

# ================= ENV =================

CURR_URL = os.getenv('CURR_URL', '').rstrip('/')
SITEMAP_OFFSET = int(os.getenv('SITEMAP_OFFSET', '0'))
MAX_SITEMAPS = int(os.getenv('MAX_SITEMAPS', '0'))
MAX_URLS_PER_SITEMAP = int(os.getenv('MAX_URLS_PER_SITEMAP', '0'))

SITEMAP_INDEX = f"{CURR_URL}/sitemap.xml" if CURR_URL else ""
OUTPUT_CSV = f'products_chunk_{SITEMAP_OFFSET}.csv'

# ================= LOGGER =================

def log_msg(msg: str) -> None:
    timestamp = datetime.now().strftime('%H:%M:%S')
    print(f"[{timestamp}] {msg}", file=sys.stderr)
    sys.stderr.flush()

# ================= HTTP with Enhanced Cloudflare Bypass =================

class CloudflareBypassSession:
    def __init__(self):
        # Try multiple user agents and approaches
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15"
        ]
        
        # Create multiple scraper configurations
        self.scrapers = []
        
        # Configuration 1: Standard chrome
        try:
            scraper1 = cloudscraper.create_scraper(
                browser={
                    'browser': 'chrome',
                    'platform': 'windows',
                    'mobile': False,
                    'desktop': True
                },
                delay=15,
                interpreter='nodejs'
            )
            scraper1.headers.update({
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                # "Accept-Encoding": "gzip, deflate, br",
                "DNT": "1",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "Cache-Control": "max-age=0",
            })
            self.scrapers.append(scraper1)
        except Exception as e:
            log_msg(f"Failed to create scraper1: {e}")
        
        # Configuration 2: Firefox
        try:
            scraper2 = cloudscraper.create_scraper(
                browser={
                    'browser': 'firefox',
                    'platform': 'windows',
                    'mobile': False
                },
                delay=10
            )
            scraper2.headers.update({
                "Accept": "application/xml,text/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "DNT": "1",
                "Connection": "keep-alive",
            })
            self.scrapers.append(scraper2)
        except Exception as e:
            log_msg(f"Failed to create scraper2: {e}")
        
        # Fallback to requests session if cloudscraper fails
        self.fallback_session = requests.Session()
        self.fallback_session.headers.update({
            "User-Agent": random.choice(self.user_agents),
            "Accept": "application/xml,text/xml;q=0.9,*/*;q=0.8",
        })
        
        self.current_scraper_index = 0
        
    def rotate_user_agent(self, session):
        """Rotate user agent to avoid detection"""
        session.headers.update({
            "User-Agent": random.choice(self.user_agents)
        })
        
    def get(self, url: str, retries: int = 5) -> Optional[str]:
        """Get URL with multiple bypass attempts"""
        
        for attempt in range(retries):
            try:
                log_msg(f"Fetching: {url} (attempt {attempt + 1}/{retries})")
                
                # Try cloudscraper first
                if self.scrapers:
                    scraper = self.scrapers[self.current_scraper_index % len(self.scrapers)]
                    self.rotate_user_agent(scraper)
                    
                    # Add random delay to mimic human behavior
                    time.sleep(random.uniform(1, 3))
                    
                    response = scraper.get(url, timeout=30)
                    
                    # Check for Cloudflare challenges
                    if response.status_code == 403:
                        log_msg("Cloudflare 403 Forbidden detected")
                        self.current_scraper_index += 1
                        continue
                        
                    if "cf-browser-verification" in response.text.lower() or \
                       "cloudflare" in response.text.lower() and "challenge" in response.text.lower():
                        log_msg("Cloudflare challenge page detected, rotating scraper...")
                        self.current_scraper_index += 1
                        time.sleep(3)
                        continue
                    
                    response.raise_for_status()
                    
                    # Validate it's actually XML, not a challenge page
                    content_type = response.headers.get("Content-Type", "").lower()
                    text = response.text
                    
                    if len(text) < 100 and ("<html" in text.lower() or "<!doctype" in text.lower()):
                        log_msg("Short HTML response (likely challenge), retrying...")
                        continue
                    
                    if text.strip().startswith("<?xml") or "<urlset" in text or "<sitemapindex" in text:
                        log_msg("Successfully fetched XML content")
                        return text
                    else:
                        log_msg("Response is not XML, retrying...")
                        continue
                        
                else:
                    # Fallback to requests
                    time.sleep(random.uniform(2, 4))
                    response = self.fallback_session.get(url, timeout=30)
                    response.raise_for_status()
                    
                    # Check for Cloudflare
                    if "cloudflare" in response.text.lower():
                        log_msg("Cloudflare detected in fallback session")
                        self.rotate_user_agent(self.fallback_session)
                        continue
                    
                    return response.text
                    
            except Exception as e:
                log_msg(f"HTTP error on attempt {attempt + 1}: {str(e)[:100]}")
                if attempt < retries - 1:
                    wait_time = 2 ** attempt + random.uniform(1, 3)
                    log_msg(f"Waiting {wait_time:.1f} seconds before retry...")
                    time.sleep(wait_time)
                    
                    # Rotate to next scraper for next attempt
                    if self.scrapers:
                        self.current_scraper_index += 1
        
        # All retries failed, try one last attempt with different approach
        log_msg("All normal attempts failed, trying alternative approach...")
        return self._try_alternative_get(url)
        
    def _try_alternative_get(self, url: str) -> Optional[str]:
        """Alternative method using different libraries/approaches"""
        try:
            # Try with undetected-chromedriver if available
            try:
                import undetected_chromedriver as uc
                log_msg("Trying undetected_chromedriver...")
                
                options = uc.ChromeOptions()
                options.add_argument('--headless=new')
                options.add_argument('--no-sandbox')
                options.add_argument('--disable-dev-shm-usage')
                options.add_argument('--disable-blink-features=AutomationControlled')
                options.add_experimental_option("excludeSwitches", ["enable-automation"])
                options.add_experimental_option('useAutomationExtension', False)
                
                driver = uc.Chrome(options=options, version_main=120)
                driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                
                driver.get(url)
                time.sleep(5)  # Wait for page to load
                content = driver.page_source
                driver.quit()
                
                if content and len(content) > 100:
                    return content
                    
            except ImportError:
                log_msg("undetected_chromedriver not available")
            except Exception as e:
                log_msg(f"undetected_chromedriver failed: {e}")
                
        except Exception as e:
            log_msg(f"Alternative approach failed: {e}")
            
        return None

    def get_json(self, url: str) -> Optional[Dict]:
        text = self.get(url)
        if text:
            try:
                return json.loads(text)
            except json.JSONDecodeError as e:
                log_msg(f"JSON decode error: {e}")
        return None

# Initialize session
session = CloudflareBypassSession()

def normalize_image(url: str) -> str:
    """Normalize image URL."""
    if not url:
        return ""
    if url.startswith('//'):
        return f'https:{url}'
    elif url.startswith('/'):
        return urljoin(CURR_URL, url)
    return url

def extract_json_from_script(html: str) -> Optional[Dict]:
    """Extract JSON data from script tags (fallback method)."""
    soup = BeautifulSoup(html, 'html.parser')
    
    # Method 1: Look for product JSON in script tags
    script_patterns = [
        r'var\s+product\s*=\s*({.*?});',
        r'window\.product\s*=\s*({.*?});',
        r'Product\s*=\s*({.*?});',
        r'product: ({.*?}),',
        r'"product":({.*?}),',
        r'productData\s*=\s*({.*?});',
        r'item: ({.*?})'
    ]
    
    for script in soup.find_all('script'):
        if script.string:
            content = script.string.strip()
            for pattern in script_patterns:
                match = re.search(pattern, content, re.DOTALL)
                if match:
                    try:
                        return json.loads(match.group(1))
                    except json.JSONDecodeError:
                        pass
    
    # Method 2: Look for JSON-LD data
    for script in soup.find_all('script', type='application/ld+json'):
        try:
            data = json.loads(script.string)
            if isinstance(data, dict) and '@type' in data:
                if data.get('@type') == 'Product':
                    return convert_ldjson_to_product(data)
        except:
            pass
    
    return None

def convert_ldjson_to_product(ld_data: Dict) -> Dict:
    """Convert JSON-LD to our product format."""
    product = {
        'id': ld_data.get('sku') or ld_data.get('productID') or '',
        'title': ld_data.get('name') or '',
        'vendor': ld_data.get('brand', {}).get('name') if isinstance(ld_data.get('brand'), dict) else ld_data.get('brand') or '',
        'type': ld_data.get('category') or '',
        'handle': '',
        'options': [],
        'variants': [],
        'featured_image': ld_data.get('image') or ''
    }
    
    # Handle offers
    offers = ld_data.get('offers', {})
    if isinstance(offers, list):
        offers = offers[0] if offers else {}
    
    variant = {
        'id': product['id'],
        'title': product['title'],
        'sku': product['id'],
        'barcode': '',
        'option1': '',
        'option2': '',
        'option3': '',
        'price': offers.get('price') if isinstance(offers, dict) else '',
        'available': offers.get('availability') == 'https://schema.org/InStock' if isinstance(offers, dict) else True
    }
    
    product['variants'].append(variant)
    return product

# ================= PRODUCT PROCESSING =================

def process_product(product_url: str, csv_writer, seen_urls: set) -> None:
    """Process a single product page."""
    if product_url in seen_urls:
        return
    seen_urls.add(product_url)
    
    log_msg(f"Product: {product_url}")
    
    # Try JSON endpoint first (Shopify style)
    json_url = f"{product_url.rstrip('/')}.json"
    product_data = session.get_json(json_url)
    
    # If JSON endpoint fails, try scraping HTML
    if not product_data or 'product' not in product_data:
        log_msg(f"JSON endpoint failed, trying HTML scrape for: {product_url}")
        html = session.get(product_url)
        if html:
            product_data = extract_json_from_script(html)
            if product_data:
                product_data = {'product': product_data}
    
    if not product_data or 'product' not in product_data:
        log_msg("Failed to extract product data")
        return
    
    product = product_data['product']
    
    if not product.get('variants'):
        log_msg("No variants found")
        return
    
    log_msg(f"Variants found: {len(product['variants'])}")
    
    options = product.get('options', [])
    images = normalize_image(product.get('featured_image', ''))
    
    for variant in product['variants']:
        csv_writer.writerow([
            product.get('id', ''),
            product.get('title', '').strip(),
            product.get('vendor', '').strip(),
            product.get('product_type', product.get('type', '')).strip(),
            product.get('handle', '').strip(),
            variant.get('id', ''),
            variant.get('title', '').strip(),
            variant.get('sku', ''),
            variant.get('barcode', ''),
            options[0].get('name', '') if len(options) > 0 else '',
            variant.get('option1', ''),
            options[1].get('name', '') if len(options) > 1 else '',
            variant.get('option2', ''),
            options[2].get('name', '') if len(options) > 2 else '',
            variant.get('option3', ''),
            variant.get('price', ''),
            '1' if variant.get('available', False) else '0',
            f"{product_url.rstrip('/')}?variant={variant.get('id', '')}",
            images
        ])
    
    # Respectful delay
    time.sleep(0.5 + random.uniform(0, 0.5))

# ================= SITEMAP PARSING =================

def parse_sitemap(xml_content: str) -> List[str]:
    """Parse sitemap XML and extract URLs."""
    urls = []
    
    try:
        # Try parsing with namespaces
        root = ET.fromstring(xml_content)
        
        # Namespace handling
        namespaces = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        
        # Try with namespace first
        for url in root.findall('.//ns:url', namespaces):
            loc = url.find('ns:loc', namespaces)
            if loc is not None and loc.text:
                urls.append(loc.text)
        
        # If no URLs found, try without namespace
        if not urls:
            for url in root.findall('.//url'):
                loc = url.find('loc')
                if loc is not None and loc.text:
                    urls.append(loc.text)
        
        # Alternative: direct loc elements
        if not urls:
            for loc in root.findall('.//loc'):
                if loc.text:
                    urls.append(loc.text)
                    
    except Exception as e:
        log_msg(f"Error parsing sitemap: {e}")
        # Try simple regex fallback
        urls = re.findall(r'<loc>(.*?)</loc>', xml_content)
    
    return urls

# ================= MAIN =================

def main():
    log_msg("Scraper started")
    log_msg(f"Base URL: {CURR_URL}")
    log_msg(f"Sitemap offset: {SITEMAP_OFFSET}")
    log_msg(f"Max sitemaps: {MAX_SITEMAPS if MAX_SITEMAPS else 'ALL'}")
    log_msg(f"Max URLs per sitemap: {MAX_URLS_PER_SITEMAP if MAX_URLS_PER_SITEMAP else 'ALL'}")
    
    # Load sitemap index with retry
    max_retries = 3
    for retry in range(max_retries):
        sitemap_index_content = session.get(SITEMAP_INDEX)
        if sitemap_index_content:
            break
        log_msg(f"Failed to load sitemap index, retry {retry + 1}/{max_retries}")
        time.sleep(5)
    
    if not sitemap_index_content:
        log_msg("Failed to load sitemap index after all retries")
        
        # Try alternative sitemap paths
        alternative_paths = [
            '/sitemap_index.xml',
            '/sitemap-index.xml',
            '/sitemap/sitemap_index.xml',
            '/sitemap/sitemap-index.xml'
        ]
        
        for path in alternative_paths:
            alternative_url = urljoin(CURR_URL, path)
            log_msg(f"Trying alternative: {alternative_url}")
            sitemap_index_content = session.get(alternative_url)
            if sitemap_index_content:
                break
        
        if not sitemap_index_content:
            log_msg("All sitemap attempts failed")
            # Create empty CSV file to avoid breaking the pipeline
            with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow([
                    'product_id', 'product_title', 'vendor', 'type', 'handle',
                    'variant_id', 'variant_title', 'sku', 'barcode',
                    'option_1_name', 'option_1_value',
                    'option_2_name', 'option_2_value',
                    'option_3_name', 'option_3_value',
                    'variant_price', 'available', 'variant_url', 'image_url'
                ])
            log_msg(f"Created empty CSV: {OUTPUT_CSV}")
            return
    
    # Parse sitemap index
    try:
        # Clean XML if needed
        if not sitemap_index_content.strip().startswith('<?xml'):
            sitemap_index_content = '<?xml version="1.0" encoding="UTF-8"?>' + sitemap_index_content
        
        urls = parse_sitemap(sitemap_index_content)
        
        # If we got URLs directly (not a sitemap index), use them
        if urls and any('/sitemap' in url.lower() for url in urls[:3]):
            sitemap_urls = urls
        else:
            # Assume it's a sitemap index with sitemap entries
            sitemap_urls = []
            # Try to extract sitemap URLs from the content
            for line in sitemap_index_content.split('\n'):
                if 'sitemap' in line.lower() and '.xml' in line:
                    match = re.search(r'<loc>(.*?)</loc>', line)
                    if match:
                        sitemap_urls.append(match.group(1))
        
        if not sitemap_urls:
            # Maybe it's a direct sitemap with product URLs
            sitemap_urls = [SITEMAP_INDEX]
            
    except Exception as e:
        log_msg(f"Error parsing sitemap index: {e}")
        # Fallback: use the main sitemap directly
        sitemap_urls = [SITEMAP_INDEX]
    
    # Apply offset and limit
    if sitemap_urls and len(sitemap_urls) > 0:
        start_idx = SITEMAP_OFFSET
        if MAX_SITEMAPS > 0:
            sitemap_urls = sitemap_urls[start_idx:start_idx + MAX_SITEMAPS]
        else:
            sitemap_urls = sitemap_urls[start_idx:]
    
    log_msg(f"Sitemaps to process: {len(sitemap_urls) if sitemap_urls else 0}")
    
    # Open CSV file
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        
        # Write header
        writer.writerow([
            'product_id', 'product_title', 'vendor', 'type', 'handle',
            'variant_id', 'variant_title', 'sku', 'barcode',
            'option_1_name', 'option_1_value',
            'option_2_name', 'option_2_value',
            'option_3_name', 'option_3_value',
            'variant_price', 'available', 'variant_url', 'image_url'
        ])
        
        seen_urls = set()
        
        if sitemap_urls:
            for sitemap_url in sitemap_urls:
                log_msg(f"Loading sitemap: {sitemap_url}")
                
                sitemap_content = session.get(sitemap_url)
                if not sitemap_content:
                    log_msg("Failed to load sitemap")
                    continue
                
                # Parse sitemap
                urls = parse_sitemap(sitemap_content)
                
                # Apply limit if specified
                if MAX_URLS_PER_SITEMAP > 0:
                    urls = urls[:MAX_URLS_PER_SITEMAP]
                
                log_msg(f"URLs in sitemap: {len(urls)}")
                
                # Process each URL
                for url in urls:
                    if url and '/products/' in url:  # Only process product pages
                        process_product(url, writer, seen_urls)
                
                # Clean up
                del sitemap_content
                import gc
                gc.collect()
                time.sleep(1)  # Delay between sitemaps
    
    log_msg(f"Chunk completed: {OUTPUT_CSV}")
    log_msg(f"Total products processed: {len(seen_urls)}")

if __name__ == "__main__":
    main()