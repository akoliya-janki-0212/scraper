import json
import csv
import time
import random
import sys
import os
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from io import StringIO
from typing import Optional, Dict, List
from urllib.parse import urljoin

# Replacement for cloudscraper
from curl_cffi import requests
from bs4 import BeautifulSoup

# ================= ENV =================
CURR_URL = os.getenv('CURR_URL', '').rstrip('/')
SITEMAP_OFFSET = int(os.getenv('SITEMAP_OFFSET', '0'))
MAX_SITEMAPS = int(os.getenv('MAX_SITEMAPS', '0'))
MAX_URLS_PER_SITEMAP = int(os.getenv('MAX_URLS_PER_SITEMAP', '0'))

SITEMAP_INDEX = f"{CURR_URL}/sitemap.xml" if CURR_URL else ""
OUTPUT_CSV = f'products_chunk_{SITEMAP_OFFSET}.csv'

def log_msg(msg: str) -> None:
    timestamp = datetime.now().strftime('%H:%M:%S')
    print(f"[{timestamp}] {msg}", file=sys.stderr)
    sys.stderr.flush()

# ================= HTTP with TLS Impersonation =================

class CloudflareBypassSession:
    def __init__(self):
        # Impersonate Chrome 110 TLS fingerprint
        self.impersonate = "chrome110"
        self.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
        }

    def get(self, url: str, retries: int = 3) -> Optional[str]:
        for attempt in range(retries):
            try:
                log_msg(f"Fetching: {url} (attempt {attempt + 1})")
                resp = requests.get(url, impersonate=self.impersonate, headers=self.headers, timeout=30)
                
                if resp.status_code == 403:
                    log_msg("Blocked (403). Retrying with delay...")
                    time.sleep(10)
                    continue
                
                resp.raise_for_status()
                return resp.text
            except Exception as e:
                log_msg(f"Request failed: {e}")
                time.sleep(5)
        return None

session = CloudflareBypassSession()

# ================= DATA EXTRACTION =================

def extract_product_data(html: str) -> Optional[Dict]:
    soup = BeautifulSoup(html, 'html.parser')
    
    # Primary Method: JSON-LD (Works for afastores and most modern sites)
    for script in soup.find_all('script', type='application/ld+json'):
        try:
            data = json.loads(script.string)
            # Handle list of objects or single object
            items = data if isinstance(data, list) else [data]
            for item in items:
                if item.get('@type') == 'Product' or 'name' in item:
                    return {
                        'id': item.get('sku') or item.get('productID') or '',
                        'title': item.get('name', ''),
                        'vendor': item.get('brand', {}).get('name') if isinstance(item.get('brand'), dict) else item.get('brand', ''),
                        'type': item.get('category', ''),
                        'variants': [{
                            'id': item.get('sku', '1'),
                            'price': item.get('offers', {}).get('price') if isinstance(item.get('offers'), dict) else '',
                            'available': 'InStock' in str(item.get('offers', {}))
                        }],
                        'images': item.get('image', [])
                    }
        except:
            continue
    return None

def process_product(product_url: str, csv_writer, seen_urls: set):
    if product_url in seen_urls: return
    seen_urls.add(product_url)

    html = session.get(product_url)
    if not html: return

    data = extract_product_data(html)
    if not data:
        log_msg(f"No Product Schema found at {product_url}")
        return

    # Normalize images
    img = data['images'][0] if isinstance(data['images'], list) and data['images'] else data['images']
    
    for v in data['variants']:
        csv_writer.writerow([
            data['id'], data['title'], data['vendor'], data['type'], '',
            v['id'], '', data['id'], '', '', '', '', '', '', '',
            v['price'], '1' if v['available'] else '0', product_url, img
        ])
    
    time.sleep(random.uniform(1.5, 3.0)) # Be more human

# ================= SITEMAP PARSING =================

def get_urls_from_xml(xml_content: str) -> List[str]:
    urls = []
    try:
        # Use regex to find <loc> content to avoid namespace issues in old XML parsers
        urls = re.findall(r'<loc>(.*?)</loc>', xml_content)
    except Exception as e:
        log_msg(f"XML Parse error: {e}")
    return urls

def main():
    sitemap_index = session.get(SITEMAP_INDEX)
    if not sitemap_index: sys.exit(1)

    all_sitemaps = get_urls_from_xml(sitemap_index)
    
    # Slice based on matrix offset
    target_sitemaps = all_sitemaps[SITEMAP_OFFSET : SITEMAP_OFFSET + MAX_SITEMAPS]
    log_msg(f"Processing {len(target_sitemaps)} sitemaps")

    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['product_id', 'product_title', 'vendor', 'type', 'handle', 'variant_id', 'variant_title', 'sku', 'barcode', 'opt1_n', 'opt1_v', 'opt2_n', 'opt2_v', 'opt3_n', 'opt3_v', 'price', 'available', 'url', 'image'])
        
        seen_urls = set()
        for s_url in target_sitemaps:
            s_content = session.get(s_url)
            if not s_content: continue
            
            p_urls = get_urls_from_xml(s_content)
            if MAX_URLS_PER_SITEMAP > 0: p_urls = p_urls[:MAX_URLS_PER_SITEMAP]
            
            for p_url in p_urls:
                # Basic filter to ensure we hit product pages
                if any(x in p_url.lower() for x in ['/product', '/item', '-p/']):
                    process_product(p_url, writer, seen_urls)

if __name__ == "__main__":
    main()