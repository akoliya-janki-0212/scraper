import os
import csv
import time
import sys
import gc
import threading
import random
import re
import json
import html
import ast
from typing import Optional, List, Dict
from datetime import datetime, timezone
from xml.etree import ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

import cloudscraper
import requests
from bs4 import BeautifulSoup

# ================= ENV =================
CURR_URL = os.getenv("CURR_URL", "https://www.emmamason.com").rstrip("/")
SITEMAP_INDEX = f"{CURR_URL}/sitemap.xml"
SITEMAP_OFFSET = int(os.getenv("SITEMAP_OFFSET", "0"))
MAX_SITEMAPS = int(os.getenv("MAX_SITEMAPS", "0"))
MAX_URLS_PER_SITEMAP = int(os.getenv("MAX_URLS_PER_SITEMAP", "0"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "4"))
REQUEST_DELAY_BASE = float(os.getenv("REQUEST_DELAY", "1.0"))
SAMPLE_SIZE = int(os.getenv("SAMPLE_SIZE", "5"))

OUTPUT_CSV = f"products_chunk_{SITEMAP_OFFSET}.csv"
SCRAPED_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

# ================= LOGGER =================
def log(msg: str, level: str = "INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sys.stderr.write(f"[{timestamp}] [{level}] {msg}\n")
    sys.stderr.flush()

# ================= CLOUDSCRAPER SESSION =================
scraper = cloudscraper.create_scraper(
    browser={'browser': 'chrome', 'platform': 'linux', 'desktop': True},
    delay=1
)

def http_get(url: str, retries: int = 3) -> Optional[str]:
    for attempt in range(retries):
        try:
            r = scraper.get(url, timeout=30)
            if r.status_code == 200:
                return r.text
            elif r.status_code in (403, 429, 503):
                delay = (2 ** attempt) + random.uniform(0, 1)
                log(f"[{r.status_code}] {url} → retry {attempt+1} in {delay:.1f}s")
                time.sleep(delay)
                continue
            else:
                log(f"HTTP {r.status_code} for {url}")
                return None
        except Exception as e:
            log(f"Request error (attempt {attempt+1}): {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return None

def load_xml(url: str) -> Optional[ET.Element]:
    data = http_get(url)
    if not data:
        return None
    try:
        return ET.fromstring(data)
    except ET.ParseError as e:
        log(f"XML parse error for {url}: {e}")
        return None

# ================= DATA EXTRACTION (unchanged) =================
def _clean_strings(obj):
    if isinstance(obj, dict):
        return {k: _clean_strings(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_clean_strings(v) for v in obj]
    if isinstance(obj, str):
        return obj.replace('\\/', '/')
    return obj

def extract_datalayer(html_text):
    patterns = [r'dataLayer\.push\s*\(\s*(\{[\s\S]*?\})\s*\);']
    for pattern in patterns:
        match = re.search(pattern, html_text)
        if match:
            raw = html.unescape(match.group(1))
            raw = raw.replace(':true', ':True').replace(':false', ':False').replace(':null', ':None')
            try:
                data = json.loads(raw)
                return _clean_strings(data)
            except:
                try:
                    return _clean_strings(ast.literal_eval(raw))
                except:
                    pass
    return None

def extract_additional_product_info(html_text):
    try:
        soup = BeautifulSoup(html_text, 'html.parser')
        table = soup.find('table', id='product-attribute-specs-table') or soup.find('table', class_='additional-attributes')
        if not table:
            return json.dumps({})
        
        additional_info = {}
        for row in table.find_all('tr'):
            th = row.find('th')
            td = row.find('td')
            if th and td:
                label = th.get_text(strip=True)
                value = td.get_text(strip=True)
                if label and value:
                    key = re.sub(r'[^a-zA-Z0-9_]', '_', label.lower().replace(' ', '_')).strip('_')
                    additional_info[key] = value
        return json.dumps(additional_info, ensure_ascii=False)
    except:
        return json.dumps({})

def fetch_json(url: str, check_is_pdp_only: bool = False) -> Optional[dict]:
    data = http_get(url)
    if not data:
        return None
    try:
        data_layer = extract_datalayer(data)
        if not data_layer:
            return None
        product_data = data_layer[0] if isinstance(data_layer, list) else data_layer
        is_pdp = product_data.get("ecommerce", {}).get("isPDP", None)
        
        if check_is_pdp_only:
            return {"isPDP": is_pdp}
        if is_pdp == 0:
            return None
            
        additional_info = extract_additional_product_info(data)
        product_data["additional_product_info_html"] = additional_info
        return product_data
    except Exception as e:
        log(f"Error in fetch_json {url}: {e}")
        return None

def check_sitemap_contains_products(sitemap_url: str) -> bool:
    log(f"Checking sitemap: {sitemap_url}")
    xml = load_xml(sitemap_url)
    if not xml:
        return False
    
    ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    urls = []
    for path in [".//ns:url/ns:loc", ".//url/loc", ".//loc"]:
        elements = xml.findall(path, ns) if "ns:" in path else xml.findall(path)
        if elements:
            urls = [e.text.strip() for e in elements if e.text and '.html' in e.text and not any(ext in e.text for ext in ['.jpg','.jpeg','.png'])]
            break
    
    sample_size = min(SAMPLE_SIZE, len(urls))
    sample_urls = random.sample(urls, sample_size) if len(urls) > sample_size else urls
    
    products_found = 0
    for url in sample_urls:
        data = fetch_json(url, check_is_pdp_only=True)
        if data and data.get("isPDP", 0) != 0:
            products_found += 1
        time.sleep(0.5)
    
    return products_found > 0

# ================= PRODUCT PROCESSING =================
csv_lock = threading.Lock()

def normalize_image_url(url: str) -> str:
    if not url:
        return ""
    if url.startswith("//"):
        return "https:" + url
    if url.startswith("/"):
        return f"{CURR_URL}{url}"
    return url

def extract_product_data(product_data: dict) -> dict:
    try:
        ecommerce = product_data.get('ecommerce', {})
        items = ecommerce.get('items', [{}])[0]
        
        product_id = str(product_data.get('ecomm_prodid', [''])[0] if isinstance(product_data.get('ecomm_prodid'), list) else '')
        name = items.get('item_name') or product_data.get('product', {}).get('name', '')
        sku = product_data.get('ecomm_prodsku') or product_data.get('product', {}).get('sku', '')
        brand = items.get('item_brand', '')
        price = str(items.get('price') or ecommerce.get('value', ''))
        
        additional_data = product_data.get('additional_product_info_html', '{}')
        try:
            add_dict = json.loads(additional_data)
            mpn = add_dict.get('item_number', '')
            category = add_dict.get('product_type', '')
        except:
            mpn = sku
            category = ''
        
        if not category and items:
            cats = [items.get(f'item_category{i}', '') for i in range(10) if items.get(f'item_category{i}')]
            category = ' | '.join(filter(None, cats))
        
        availability = ecommerce.get('magentoProductAvailability', '')
        status = 'SELLABLE' if availability == 'InStock' else 'OUT_OF_STOCK'
        
        return {
            'product_id': product_id,
            'name': name,
            'brand': brand,
            'price': price,
            'sku': sku,
            'mpn': mpn,
            'category': category,
            'status': status,
            'additional_data': additional_data,
            'quantity': items.get('quantity', 0),
            'main_image': ''  # can be extended later
        }
    except Exception as e:
        log(f"Extract error: {e}")
        return {}

def process_product_data(product_url: str, writer, seen: set, stats: dict):
    if product_url in seen:
        return
    seen.add(product_url)
    
    data = fetch_json(product_url)
    if not data:
        stats['errors'] += 1
        return
    
    info = extract_product_data(data)
    if not info.get('product_id'):
        stats['errors'] += 1
        return
    
    row = [
        product_url, info['product_id'], '', info['category'], '',
        info['brand'], info['name'], info['sku'], info['mpn'], '',
        info['price'], normalize_image_url(info.get('main_image','')),
        info['quantity'], '', '', info['status'], info['additional_data'], SCRAPED_DATE
    ]
    
    with csv_lock:
        writer.writerow(row)
    
    stats['products_fetched'] += 1
    log(f"✓ {info['product_id']} | {info['name'][:60]}...")
    
    time.sleep(REQUEST_DELAY_BASE)
    stats['urls_processed'] += 1

# ================= MAIN =================
def main():
    log("=" * 70)
    log("Emma Mason Scraper → GitHub Actions Edition (cloudscraper)")
    log(f"Base URL     : {CURR_URL}")
    log(f"Sitemap      : {SITEMAP_INDEX}")
    log(f"Workers      : {MAX_WORKERS}")
    log(f"Delay        : {REQUEST_DELAY_BASE}s")
    log("=" * 70)

    index = load_xml(SITEMAP_INDEX)
    if not index:
        log("Failed to load sitemap index", "ERROR")
        sys.exit(1)

    ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    sitemaps = [e.text.strip() for e in index.findall(".//ns:sitemap/ns:loc", ns) if e.text]

    log(f"Found {len(sitemaps)} sub-sitemaps")

    if SITEMAP_OFFSET >= len(sitemaps):
        log("Offset too high", "WARNING")
        sys.exit(0)

    end = SITEMAP_OFFSET + MAX_SITEMAPS if MAX_SITEMAPS > 0 else len(sitemaps)
    sitemaps_to_process = sitemaps[SITEMAP_OFFSET:end]

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Ref Product URL","Ref Product ID","Ref Varient ID","Ref Category","Ref Category URL",
            "Ref Brand Name","Ref Product Name","Ref SKU","Ref MPN","Ref GTIN","Ref Price",
            "Ref Main Image","Ref Quantity","Ref Group Attr 1","Ref Group Attr 2",
            "Ref Status","Additional Product Data","Date Scrapped"
        ])

        seen = set()
        stats = {'sitemaps_processed': 0, 'urls_processed': 0, 'products_fetched': 0, 'errors': 0}

        for sitemap_url in sitemaps_to_process:
            stats['sitemaps_processed'] += 1
            log(f"[{stats['sitemaps_processed']}/{len(sitemaps_to_process)}] {sitemap_url}")

            xml = load_xml(sitemap_url)
            if not xml:
                continue

            urls = [e.text.strip() for e in xml.findall(".//ns:url/ns:loc", ns) if e.text and '.html' in e.text]
            
            if MAX_URLS_PER_SITEMAP:
                urls = urls[:MAX_URLS_PER_SITEMAP]

            log(f"→ {len(urls)} URLs found")

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [executor.submit(process_product_data, u, writer, seen, stats) for u in urls]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        log(f"Thread error: {e}", "ERROR")

            gc.collect()

    log("=" * 70)
    log("FINAL STATS")
    log(f"Sitemaps processed : {stats['sitemaps_processed']}")
    log(f"URLs processed     : {stats['urls_processed']}")
    log(f"Products fetched   : {stats['products_fetched']}")
    log(f"Errors             : {stats['errors']}")
    if stats['urls_processed']:
        log(f"Success rate       : {stats['products_fetched']/stats['urls_processed']*100:.1f}%")
    log(f"Output file        : {OUTPUT_CSV}")
    log("=" * 70)

if __name__ == "__main__":
    if not CURR_URL:
        log("CURR_URL env var missing", "ERROR")
        sys.exit(1)
    main()