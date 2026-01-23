import os
import csv
import time
import sys
import gc
import threading
import requests
from typing import Optional
from datetime import datetime
from xml.etree import ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= ENV =================

CURR_URL = os.getenv("CURR_URL", "").rstrip("/")
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
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
})

def http_get(url: str) -> Optional[str]:
    for _ in range(3):
        try:
            r = session.get(url, timeout=30)
            if r.ok:
                return r.text
        except Exception:
            time.sleep(0.3)
    return None

def load_xml(url: str) -> Optional[ET.Element]:
    data = http_get(url)
    if not data:
        return None
    try:
        return ET.fromstring(data)
    except ET.ParseError:
        return None

def fetch_json(url: str) -> Optional[dict]:
    try:
        r = session.get(url, timeout=30)
        return r.json() if r.ok else None
    except Exception:
        return None

def normalize_image(url: str) -> str:
    return "https:" + url if url and url.startswith("//") else (url or "")

# ================= PRODUCT =================

csv_lock = threading.Lock()

def extract_category(tags: list):
    for t in tags:
        if t.startswith("collection_"):
            return t.replace("collection_", ""), ""
    return "", ""

def process_product(url: str, writer, seen: set):
    if url in seen:
        return
    seen.add(url)

    product = fetch_json(url.rstrip("/") + ".js")
    if not product or not product.get("variants"):
        return

    tags = product.get("tags", [])
    category, category_url = extract_category(tags)

    brand = product.get("vendor", "")
    product_name = product.get("title", "")
    product_id = product.get("id", "")
    main_image = normalize_image(product.get("featured_image"))
    product_url = f"{CURR_URL}{product.get('url', '')}"

    for v in product["variants"]:
        row = [
            product_url,                        # Ref Product URL
            product_id,                         # Ref Product ID
            v.get("id", ""),                    # Ref Varient ID
            category,                           # Ref Category
            category_url,                       # Ref Category URL
            brand,                              # Ref Brand Name
            product_name,                       # Ref Product Name
            v.get("sku", ""),                   # Ref SKU
            v.get("sku", ""),                   # Ref MPN
            v.get("barcode", ""),               # Ref GTIN
            v.get("price", ""),                 # Ref Price
            main_image,                         # Ref Main Image
            1 if v.get("available") else 0,     # Ref Quantity
            v.get("option1", ""),               # Ref Group Attr 1
            v.get("option2", ""),               # Ref Group Attr 2
            "active" if v.get("available") else "inactive",  # Ref Status
            SCRAPED_DATE                        # Date Scrapped
        ]

        with csv_lock:
            writer.writerow(row)

    time.sleep(REQUEST_DELAY)

# ================= MAIN =================

log("Scraper started")
log(f"Base URL: {CURR_URL}")

index = load_xml(SITEMAP_INDEX)
if not index:
    log("Failed to load sitemap index")
    sys.exit(1)

ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
sitemaps = [e.text for e in index.findall(".//ns:sitemap/ns:loc", ns)]

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

    for sitemap_url in sitemaps:
        log(f"Loading sitemap: {sitemap_url}")
        xml = load_xml(sitemap_url)
        if not xml:
            continue

        urls = [e.text for e in xml.findall(".//ns:url/ns:loc", ns)]
        if MAX_URLS_PER_SITEMAP:
            urls = urls[:MAX_URLS_PER_SITEMAP]

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [
                executor.submit(process_product, u, writer, seen)
                for u in urls
            ]
            for ftr in as_completed(futures):
                ftr.result()

        gc.collect()

log(f"Completed: {OUTPUT_CSV}")