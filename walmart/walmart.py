import os
import csv
import time
import sys
import gc
import threading
import requests
import re
import json
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
from xml.etree import ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class WalmartScraper:

    # ============================================================
    #  Init — all config + shared state
    # ============================================================

    def __init__(self):
        self.curr_url             = os.getenv("CURR_URL", "https://www.walmart.com").rstrip("/")
        self.api_base_url         = os.getenv("API_BASE_URL", "").rstrip("/")
        self.sitemap_offset       = int(os.getenv("SITEMAP_OFFSET", "0"))
        self.max_sitemaps         = int(os.getenv("MAX_SITEMAPS", "0"))
        self.max_urls_per_sitemap = int(os.getenv("MAX_URLS_PER_SITEMAP", "0"))
        self.max_workers          = int(os.getenv("MAX_WORKERS", "4"))
        self.request_delay        = float(os.getenv("REQUEST_DELAY", "1.0"))

        self.output_dir   = "media/output/scrapping/walmart"
        self.failure_dir  = "media/output/scrapping/failure_csv"
        self.output_csv   = f"walmart_products_chunk_{self.sitemap_offset}.csv"
        self.scraped_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        self.csv_header = [
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
            "Date Scrapped",
        ]

        self.csv_lock  = threading.Lock()
        self.fail_lock = threading.Lock()
        self.seen      = set()
        self.stats     = {
            "sitemaps_processed": 0,
            "urls_processed":     0,
            "products_fetched":   0,
            "errors":             0,
        }

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language":           "en-US,en;q=0.9",
            "Connection":                "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest":            "document",
            "Sec-Fetch-Mode":            "navigate",
            "Sec-Fetch-Site":            "none",
            "Sec-Fetch-User":            "?1",
        })

    # ============================================================
    #  Logger
    # ============================================================

    def log(self, msg: str, level: str = "INFO"):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sys.stderr.write(f"[{timestamp}] [{level}] {msg}\n")
        sys.stderr.flush()

    # ============================================================
    #  HTTP
    # ============================================================

    def http_get(self, url: str, is_json: bool = False) -> Optional[str]:
        """GET with up to 3 retries. Switches headers for JSON vs HTML requests."""
        for attempt in range(3):
            try:
                if is_json:
                    headers = {
                        "User-Agent": (
                            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/120.0.0.0 Safari/537.36"
                        ),
                        "Accept":           "application/json, text/javascript, */*; q=0.01",
                        "Accept-Language":  "en-US,en;q=0.9",
                        "Referer":          f"{self.curr_url}/",
                        "X-Requested-With": "XMLHttpRequest",
                        "Sec-Fetch-Dest":   "empty",
                        "Sec-Fetch-Mode":   "cors",
                        "Sec-Fetch-Site":   "same-origin",
                    }
                    r = self.session.get(url, headers=headers, timeout=15, verify=True)
                else:
                    r = self.session.get(url, timeout=15, verify=True)

                if r.status_code == 200:
                    self.log(f"Success: {url}", "DEBUG")
                    return r.text

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

    # ============================================================
    #  Sitemap
    # ============================================================

    def load_xml(self, url: str) -> Optional[ET.Element]:
        """Fetch and parse an XML sitemap. Strips <script/> before parsing."""
        data = None
        for attempt in range(3):
            try:
                data = self.http_get(url, is_json=False)
                if data:
                    break
            except Exception as e:
                self.log(f"Attempt {attempt + 1} for sitemap failed: {e}", "WARNING")
                time.sleep(2)

        if not data:
            self.log(f"Failed to load XML from {url}", "ERROR")
            return None

        # Walmart injects <script/> into the sitemap index which breaks XML parsing
        # Simply strip it out before parsing — same as ovr.py approach
        data = re.sub(r'<script[^>]*/>', '', data)
        data = re.sub(r'<script[^>]*>.*?</script>', '', data, flags=re.DOTALL)
        try:
            if "<?xml" not in data[:100]:
                data = '<?xml version="1.0" encoding="UTF-8"?>\n' + data
            return ET.fromstring(data)
        except ET.ParseError as e:
            self.log(f"XML parse error for {url}: {e}", "ERROR")
            return None

    def get_child_sitemaps(self, index_url: str) -> List[str]:
        """Parse sitemap index and return list of child sitemap URLs."""
        ns    = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        index = self.load_xml(index_url)
        if index is None:
            return []

        for path in [".//ns:sitemap/ns:loc", ".//sitemap/loc", ".//loc"]:
            elements = index.findall(path, ns) if "ns:" in path else index.findall(path)
            if elements:
                sitemaps = [e.text.strip() for e in elements if e.text]
                if sitemaps:
                    self.log(f"Found {len(sitemaps)} child sitemaps", "INFO")
                    return sitemaps

        self.log("No child sitemaps found", "WARNING")
        return []

    def get_product_urls(self, sitemap_url: str) -> List[str]:
        """Parse a product sitemap and return only Walmart /ip/ URLs."""
        ns  = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        xml = self.load_xml(sitemap_url)
        if not xml:
            return []

        for path in [".//ns:url/ns:loc", ".//url/loc", ".//loc"]:
            elements = xml.findall(path, ns) if "ns:" in path else xml.findall(path)
            if elements:
                urls = [e.text.strip() for e in elements if e.text and "/ip/" in e.text]
                if urls:
                    return urls

        self.log(f"No /ip/ product URLs found in: {sitemap_url}", "WARNING")
        return []

    # ============================================================
    #  Helpers
    # ============================================================

    def extract_product_id(self, url: str) -> Optional[str]:
        """Pull Walmart item ID from /ip/product-name/123456789 style URLs."""
        if not url:
            return None
        match = re.search(r'/ip/(?:[^/]+/)?(\d+)', url)
        if match:
            return match.group(1)
        last = url.rstrip("/").split("/")[-1].split("?")[0]
        if last.isdigit():
            return last
        self.log(f"No product ID found in URL: {url}", "WARNING")
        return None

    def clean_url(self, url: str) -> str:
        """Strip query params and trailing slashes."""
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path.rstrip('/')}"

    def normalize_image(self, url: str) -> str:
        """Make image URL absolute."""
        if not url:
            return ""
        if url.startswith("//"):
            return "https:" + url
        if url.startswith("/"):
            return f"{self.curr_url}{url}"
        if not url.startswith("http"):
            return f"https://{url}"
        return url

    # ============================================================
    #  Product Extraction
    # ============================================================

    def extract_walmart_data(self, soup: BeautifulSoup, url: str) -> List[Dict]:
        """Parse JSON-LD blocks from a Walmart page. Returns one dict per variant."""
        product_id = self.extract_product_id(url)
        results: List[Dict] = []

        for script in soup.find_all("script", type="application/ld+json"):
            try:
                raw = script.string
                if not raw:
                    continue
                data = json.loads(raw)

                if isinstance(data, list):
                    data = data[0]
                if not isinstance(data, dict):
                    continue

                variants = data.get("hasVariant") or [data]

                for variant in variants:
                    if not isinstance(variant, dict):
                        continue

                    # ---- Pick best matching offer ----
                    offers         = variant.get("offers", {})
                    selected_offer = {}
                    if isinstance(offers, list):
                        for offer in offers:
                            offer_url = self.clean_url(offer.get("url", ""))
                            if self.extract_product_id(offer_url) == product_id:
                                selected_offer = offer
                                break
                        if not selected_offer and offers:
                            selected_offer = offers[0]
                    elif isinstance(offers, dict):
                        selected_offer = offers

                    # ---- Image ----
                    images     = variant.get("image", "")
                    main_image = images[0] if isinstance(images, list) else images or ""

                    # ---- Price ----
                    price = (
                        selected_offer.get("price", "")
                        or selected_offer.get("lowPrice", "")
                        or (data.get("offers", {}).get("price", "")
                            if isinstance(data.get("offers"), dict) else "")
                    )

                    # ---- Status ----
                    availability = selected_offer.get("availability", "") or variant.get("availability", "")
                    status       = "In Stock" if "InStock" in availability else "Out of Stock"

                    # ---- Brand ----
                    brand_raw = variant.get("brand", {})
                    brand     = brand_raw.get("name", "") if isinstance(brand_raw, dict) else str(brand_raw)

                    results.append({
                        "competitor_product_id": product_id or "",
                        "comp_received_name":    variant.get("name", ""),
                        "comp_received_sku":     variant.get("sku", ""),
                        "brand":                 brand,
                        "mpn":                   variant.get("model", "") or variant.get("mpn", ""),
                        "category":              "",
                        "category_url":          "",
                        "gtin":                  variant.get("gtin13", "") or variant.get("gtin", ""),
                        "variation_id":          variant.get("variationId", ""),
                        "quantity":              variant.get("inventory", {}).get("quantityAvailable", 0),
                        "status":                status,
                        "competitor_price":      price,
                        "group_attr_1":          variant.get("description", ""),
                        "group_attr_2":          variant.get("color", ""),
                        "main_image":            self.normalize_image(main_image),
                        "competitor_url":        url,
                        "scraped_date":          self.scraped_date,
                    })

                if results:
                    return results

            except (json.JSONDecodeError, AttributeError) as e:
                self.log(f"JSON-LD parse error: {e}", "WARNING")
                continue

        return results

    # ============================================================
    #  CSV Write
    # ============================================================

    def write_row(self, writer: csv.writer, product: Dict):
        """Thread-safe write of one product row to CSV."""
        row = [
            product["competitor_url"],
            product["competitor_product_id"],
            product["variation_id"],
            product["category"],
            product["category_url"],
            product["brand"],
            product["comp_received_name"],
            product["comp_received_sku"],
            product["mpn"],
            product["gtin"],
            product["competitor_price"],
            product["main_image"],
            product["quantity"],
            product["group_attr_1"],
            product["group_attr_2"],
            product["status"],
            product["scraped_date"],
        ]
        with self.csv_lock:
            writer.writerow(row)

    def log_failure(self, url: str, reason: str):
        """Append a failed URL to the failure CSV."""
        os.makedirs(self.failure_dir, exist_ok=True)
        failure_path = os.path.join(self.failure_dir, "Walmart_failures.csv")
        file_exists  = os.path.isfile(failure_path)
        with self.fail_lock:
            with open(failure_path, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                if not file_exists:
                    w.writerow(["URL", "Reason", "Timestamp"])
                w.writerow([url, reason, self.scraped_date])

    # ============================================================
    #  Process Single Product
    # ============================================================

    def process_product(self, product_url: str, writer: csv.writer):
        """Fetch, extract, and save one Walmart product URL."""
        base_url = self.clean_url(product_url)

        if base_url in self.seen:
            return
        self.seen.add(base_url)

        self.log(f"Processing: {base_url}", "DEBUG")

        product_id = self.extract_product_id(base_url)
        if not product_id:
            self.stats["errors"] += 1
            self.log_failure(base_url, "No product ID extracted")
            return

        html = self.http_get(base_url, is_json=False)
        if not html:
            self.stats["errors"] += 1
            self.log_failure(base_url, "HTTP fetch failed")
            return

        soup     = BeautifulSoup(html, "html.parser")
        products = self.extract_walmart_data(soup, base_url)

        if not products:
            self.stats["errors"] += 1
            self.log_failure(base_url, "No product data found in JSON-LD")
            return

        for product in products:
            if not product.get("comp_received_name"):
                continue
            try:
                self.write_row(writer, product)
                self.stats["products_fetched"] += 1
                self.log(
                    f"Saved [{product['competitor_product_id']}] "
                    f"{product['comp_received_name'][:60]}"
                )
            except Exception as e:
                self.log(f"Row write error for {product_id}: {e}", "ERROR")
                self.stats["errors"] += 1

        time.sleep(self.request_delay)
        self.stats["urls_processed"] += 1

    # ============================================================
    #  Run — main orchestrator
    # ============================================================

    def run(self):
        self.log("=" * 60)
        self.log("Walmart Parallel Bulk Scraper")
        self.log(f"Timestamp:            {self.scraped_date}")
        self.log(f"Base URL:             {self.curr_url}")
        self.log(f"Sitemap Offset:       {self.sitemap_offset}")
        self.log(f"Max Sitemaps:         {self.max_sitemaps or 'All'}")
        self.log(f"Max URLs per Sitemap: {self.max_urls_per_sitemap or 'All'}")
        self.log(f"Max Workers:          {self.max_workers}")
        self.log(f"Request Delay:        {self.request_delay}s")
        self.log("=" * 60)

        sitemap_index_url = f"{self.curr_url}/sitemap_hi_ip.xml"
        all_sitemaps      = self.get_child_sitemaps(sitemap_index_url)

        if not all_sitemaps:
            self.log("No sitemaps found. Exiting.", "ERROR")
            sys.exit(1)

        if self.sitemap_offset >= len(all_sitemaps):
            self.log(f"Offset {self.sitemap_offset} >= total ({len(all_sitemaps)}), nothing to do.", "WARNING")
            sys.exit(0)

        end_idx         = self.sitemap_offset + self.max_sitemaps if self.max_sitemaps > 0 else len(all_sitemaps)
        sitemaps_to_run = all_sitemaps[self.sitemap_offset:end_idx]

        self.log(f"Total sitemaps found:  {len(all_sitemaps)}")
        self.log(f"Sitemaps to process:   {len(sitemaps_to_run)}")

        os.makedirs(self.output_dir, exist_ok=True)
        output_path = os.path.join(self.output_dir, self.output_csv)

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(self.csv_header)

            for sitemap_url in sitemaps_to_run:
                self.stats["sitemaps_processed"] += 1
                self.log(f"Sitemap {self.stats['sitemaps_processed']}/{len(sitemaps_to_run)}: {sitemap_url}")

                urls = self.get_product_urls(sitemap_url)
                if not urls:
                    continue

                if self.max_urls_per_sitemap > 0:
                    self.log(f"Limiting to {self.max_urls_per_sitemap} of {len(urls)} URLs")
                    urls = urls[:self.max_urls_per_sitemap]
                else:
                    self.log(f"Found {len(urls)} product URLs")

                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = [
                        executor.submit(self.process_product, url, writer)
                        for url in urls
                    ]
                    for future in as_completed(futures):
                        try:
                            future.result()
                        except Exception as e:
                            self.log(f"Thread error: {e}", "ERROR")
                            self.stats["errors"] += 1

                gc.collect()

        self.log("=" * 60)
        self.log("SCRAPING COMPLETE")
        self.log(f"  Sitemaps processed:  {self.stats['sitemaps_processed']}")
        self.log(f"  URLs processed:      {self.stats['urls_processed']}")
        self.log(f"  Products saved:      {self.stats['products_fetched']}")
        self.log(f"  Errors:              {self.stats['errors']}")
        if self.stats["urls_processed"] > 0:
            rate = self.stats["products_fetched"] / self.stats["urls_processed"] * 100
            self.log(f"  Success rate:        {rate:.1f}%")
        self.log(f"  Output:              {output_path}")
        self.log("=" * 60)


# ============================================================
#  Entry point
# ============================================================

if __name__ == "__main__":
    if not os.getenv("CURR_URL"):
        sys.stderr.write("[ERROR] CURR_URL environment variable is required\n")
        sys.exit(1)

    WalmartScraper().run()
