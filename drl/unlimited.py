import os
import csv
import time
import sys
import gc
import threading
import re
import json
from typing import Optional, Dict, List
from datetime import datetime
from xml.etree import ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from bs4 import BeautifulSoup

# ================= ENV =================

CURR_URL = os.getenv("CURR_URL", "https://www.unlimitedfurnituregroup.com/").rstrip("/")
SITEMAP_OFFSET = int(os.getenv("SITEMAP_OFFSET", "0"))
MAX_SITEMAPS = int(os.getenv("MAX_SITEMAPS", "0"))
MAX_URLS_PER_SITEMAP = int(os.getenv("MAX_URLS_PER_SITEMAP", "0"))

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "8"))
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "0.15"))

SITEMAP_INDEX = f"{CURR_URL}/sitemap.xml"
OUTPUT_CSV = f"products_chunk_{SITEMAP_OFFSET}.csv"

SCRAPED_DATE = datetime.utcnow().strftime("%Y-%m-%d")

# ================= LOGGER =================

def log(msg: str):
    sys.stderr.write(f"[{time.strftime('%H:%M:%S')}] {msg}\n")
    sys.stderr.flush()

# ================= HTTP =================

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
})

def http_get(url: str) -> Optional[str]:
    for attempt in range(3):
        try:
            r = session.get(url, timeout=30)
            if r.ok:
                return r.text
            log(f"HTTP {r.status_code} for {url}")
        except Exception as e:
            log(f"Attempt {attempt + 1} failed for {url}: {str(e)}")
            time.sleep(1)
    return None

def load_xml(url: str) -> Optional[ET.Element]:
    data = http_get(url)
    if not data:
        return None
    try:
        return ET.fromstring(data)
    except ET.ParseError as e:
        log(f"XML Parse error for {url}: {str(e)}")
        return None

def extract_json_ld(html: str) -> Optional[Dict]:
    """Extract JSON-LD structured data from HTML"""
    soup = BeautifulSoup(html, 'html.parser')
    
    # Find all script tags with type="application/ld+json"
    scripts = soup.find_all('script', type='application/ld+json')
    
    for script in scripts:
        try:
            data = json.loads(script.string)
            # Look for Product schema
            if isinstance(data, dict) and data.get('@type') == 'Product':
                return data
            elif isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and item.get('@type') == 'Product':
                        return item
        except:
            continue
    
    return None

def extract_meta_tags(soup: BeautifulSoup) -> Dict:
    """Extract product data from meta tags"""
    meta_data = {}
    
    # Open Graph meta tags
    og_title = soup.find('meta', property='og:title')
    if og_title:
        meta_data['title'] = og_title.get('content', '')
    
    og_image = soup.find('meta', property='og:image')
    if og_image:
        meta_data['image'] = og_image.get('content', '')
    
    og_url = soup.find('meta', property='og:url')
    if og_url:
        meta_data['url'] = og_url.get('content', '')
    
    # Product meta tags
    price = soup.find('meta', property='product:price:amount')
    if price:
        meta_data['price'] = price.get('content', '')
    
    currency = soup.find('meta', property='product:price:currency')
    if currency:
        meta_data['currency'] = currency.get('content', '')
    
    retailer_id = soup.find('meta', property='product:retailer_item_id')
    if retailer_id:
        meta_data['sku'] = retailer_id.get('content', '')
    
    availability = soup.find('meta', property='product:availability')
    if availability:
        meta_data['availability'] = availability.get('content', '')
    
    return meta_data

def parse_product_data(html: str, url: str) -> Optional[Dict]:
    """Parse product data from HTML page"""
    soup = BeautifulSoup(html, 'html.parser')
    product_data = {}
    
    # Try to get data from JSON-LD first (most reliable)
    json_ld = extract_json_ld(html)
    if json_ld:
        product_data = json_ld
    
    # Supplement with meta tags
    meta_data = extract_meta_tags(soup)
    
    # Extract from page elements if needed
    if not product_data.get('name'):
        title_tag = soup.find('h1', class_='page-title')
        if title_tag:
            product_data['name'] = title_tag.get_text(strip=True)
    
    if not product_data.get('sku'):
        sku_element = soup.find('div', class_='product attribute sku')
        if sku_element:
            value = sku_element.find('div', class_='value')
            if value:
                product_data['sku'] = value.get_text(strip=True)
    
    # Get description
    if not product_data.get('description'):
        desc_element = soup.find('div', class_='product attribute description')
        if desc_element:
            value = desc_element.find('div', class_='value')
            if value:
                product_data['description'] = value.get_text(strip=True)
    
    # Merge meta data
    if meta_data:
        if 'sku' in meta_data and not product_data.get('sku'):
            product_data['sku'] = meta_data['sku']
        if 'price' in meta_data and not product_data.get('offers'):
            product_data['offers'] = {
                'price': meta_data['price'],
                'priceCurrency': meta_data.get('currency', 'USD'),
                'availability': meta_data.get('availability', '')
            }
        if 'image' in meta_data and not product_data.get('image'):
            product_data['image'] = meta_data['image']
    
    return product_data

# ================= PRODUCT =================

csv_lock = threading.Lock()

def process_product(url: str, writer, seen: set):
    """Process a single product URL"""
    if url in seen:
        return
    seen.add(url)
    
    log(f"Processing: {url}")
    html = http_get(url)
    if not html:
        return
    
    product_data = parse_product_data(html, url)
    if not product_data:
        return
    
    # Extract brand from URL or product data
    brand = "Unknown"
    if "brands/" in url:
        brand_match = re.search(r'brands/([^/]+)', url)
        if brand_match:
            brand = brand_match.group(1).replace('-', ' ').title()
    elif product_data.get('brand'):
        if isinstance(product_data['brand'], dict):
            brand = product_data['brand'].get('name', '')
        else:
            brand = product_data['brand']
    
    # Extract category
    category = product_data.get('category', '')
    if not category and 'breadcrumb' in str(html).lower():
        # Try to extract from breadcrumbs
        soup = BeautifulSoup(html, 'html.parser')
        breadcrumb_items = soup.find_all('script', type='application/ld+json')
        for item in breadcrumb_items:
            try:
                data = json.loads(item.string)
                if isinstance(data, dict) and data.get('@type') == 'BreadcrumbList':
                    items = data.get('itemListElement', [])
                    if len(items) >= 2:
                        category = items[1].get('item', {}).get('name', '')
                    break
            except:
                continue
    
    # Get price and availability
    price = ""
    status = "inactive"
    if product_data.get('offers'):
        offers = product_data['offers']
        if isinstance(offers, dict):
            price = offers.get('price', '')
            availability = offers.get('availability', '')
            if 'InStock' in availability or 'instock' in availability.lower():
                status = "active"
    
    # Get images
    main_image = ""
    if product_data.get('image'):
        if isinstance(product_data['image'], list):
            main_image = product_data['image'][0] if product_data['image'] else ""
        else:
            main_image = product_data['image']
    
    # Normalize image URL
    if main_image and main_image.startswith('//'):
        main_image = 'https:' + main_image
    
    # Build the row matching your CSV structure
    row = [
        url,                          # Ref Product URL
        product_data.get('sku', ''),   # Ref Product ID (using SKU as ID)
        "",                            # Ref Varient ID (Magento may have simple products without variants)
        category,                       # Ref Category
        "",                            # Ref Category URL
        brand,                          # Ref Brand Name
        product_data.get('name', ''),   # Ref Product Name
        product_data.get('sku', ''),    # Ref SKU
        product_data.get('sku', ''),    # Ref MPN
        product_data.get('gtin13', ''), # Ref GTIN
        price,                          # Ref Price
        main_image,                     # Ref Main Image
        "1" if status == "active" else "0",  # Ref Quantity (simplified)
        product_data.get('description', '')[:100],  # Ref Group Attr 1 (first part of description)
        "",                            # Ref Group Attr 2
        status,                         # Ref Status
        SCRAPED_DATE                    # Date Scrapped
    ]
    
    with csv_lock:
        writer.writerow(row)
        log(f"✓ Added: {product_data.get('name', 'Unknown')[:50]}...")
    
    time.sleep(REQUEST_DELAY)

# ================= MAIN =================

log("Magento scraper started")
log(f"Base URL: {CURR_URL}")

index = load_xml(SITEMAP_INDEX)
if not index:
    log("Failed to load sitemap index")
    sys.exit(1)

ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
sitemaps = [e.text for e in index.findall(".//ns:sitemap/ns:loc", ns)]

# Filter for product sitemaps (Magento often has separate sitemaps for products)
product_sitemaps = [s for s in sitemaps if 'product' in s.lower()]
if product_sitemaps:
    sitemaps = product_sitemaps
    log(f"Found {len(product_sitemaps)} product sitemaps")

sitemaps = sitemaps[
    SITEMAP_OFFSET : SITEMAP_OFFSET + MAX_SITEMAPS if MAX_SITEMAPS else None
]

log(f"Sitemaps to process: {len(sitemaps)}")

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
    ])

    seen = set()
    total_products = 0

    for sitemap_url in sitemaps:
        log(f"Loading sitemap: {sitemap_url}")
        xml = load_xml(sitemap_url)
        if not xml:
            continue

        urls = [e.text for e in xml.findall(".//ns:url/ns:loc", ns)]
        if MAX_URLS_PER_SITEMAP:
            urls = urls[:MAX_URLS_PER_SITEMAP]
        
        log(f"Found {len(urls)} URLs in sitemap")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [
                executor.submit(process_product, u, writer, seen)
                for u in urls
            ]
            for ftr in as_completed(futures):
                ftr.result()
                total_products += 1

        gc.collect()

log(f"Completed: {OUTPUT_CSV} with {total_products} products")