#!/usr/bin/env python3
"""
generate_chunks.py – Fetches sitemap index, counts product URLs per sitemap,
and generates a GitHub Actions matrix where each job processes one chunk
of at most URLS_PER_JOB URLs.
"""

import os
import sys
import json
import time
import requests
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, List, Dict, Tuple
from xml.etree import ElementTree as ET
from datetime import datetime, timezone

# ---------- ENV ----------
CURR_URL = os.environ.get("CURR_URL", "").rstrip("/")
if not CURR_URL:
    print("ERROR: CURR_URL environment variable is required", file=sys.stderr)
    sys.exit(1)

SITEMAP_INDEX = f"{CURR_URL}/sitemap.xml"
MAX_SITEMAPS = int(os.environ.get("MAX_SITEMAPS", "0"))
MAX_URLS_PER_SITEMAP = int(os.environ.get("MAX_URLS_PER_SITEMAP", "0"))
URLS_PER_JOB = int(os.environ.get("URLS_PER_JOB", "500"))
SITEMAP_OFFSET = int(os.environ.get("SITEMAP_OFFSET", "0"))
FLARESOLVERR_URL = os.environ.get("FLARESOLVERR_URL")
CHUNK_GEN_WORKERS = int(os.environ.get("CHUNK_GEN_WORKERS", "2"))

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; SitemapParser/1.0)"}

FLARESOLVERR_TIMEOUT = int(os.getenv("FLARESOLVERR_TIMEOUT", "120"))

class FlareSolverrSession:
    def __init__(self):
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
                    "maxTimeout": FLARESOLVERR_TIMEOUT * 1000,
                    "session": None,  # Create new session
                    "headers": self.headers
                }
                
                response = self.session.post(
                    FLARESOLVERR_URL,
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
                
                log(f"FlareSolverr attempt {attempt + 1} failed for {url}: {response.status_code}")
                
            except requests.exceptions.Timeout:
                log(f"FlareSolverr timeout on attempt {attempt + 1} for {url}")
            except requests.exceptions.ConnectionError:
                log(f"FlareSolverr connection error on attempt {attempt + 1} for {url}")
            except Exception as e:
                log(f"FlareSolverr error on attempt {attempt + 1} for {url}: {e}")
            
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
        
        return None, 0

    def fetch(self, url: str) -> Optional[Tuple[str, int]]:
        """Fetch URL through FlareSolverr"""
        return self.flaresolverr_request(url)

flaresolverr_session = FlareSolverrSession()

def log(msg: str, level: str = "INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sys.stderr.write(f"[{timestamp}] [{level}] {msg}\n")
    sys.stderr.flush()

def sanitize_url_text(text: str) -> str:
    clean = re.sub(r"<[^>]+>", " ", text or "")
    m = re.search(r"https?://[^\s\"'<>]+", clean)
    return m.group(0).strip() if m else ""


def check_robots_txt():
    """Check robots.txt for crawl delays and sitemap location"""
    robots_url = f"{CURR_URL}/robots.txt"
    log(f"Checking robots.txt: {robots_url}")
    
    content, status = flaresolverr_session.fetch(robots_url)
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


# ---------- FETCH with fallback to FlareSolverr ----------
def fetch_xml(url):
    """Try normal GET first, fallback to FlareSolverr if needed."""
    try:
        content, status = flaresolverr_session.fetch(url)
        if status == 200:
            return content
        elif status in (403, 503) and FLARESOLVERR_URL:
            # Fallback to FlareSolverr
            payload = {
                "cmd": "request.get",
                "url": url,
                "maxTimeout": 30000,
                "headers": HEADERS
            }
            fs = requests.post(FLARESOLVERR_URL, json=payload, timeout=60)
            if fs.status_code == 200:
                return fs.json().get("solution", {}).get("response")
    except Exception as e:
        print(f"Error fetching {url}: {e}", file=sys.stderr)
    return None

# ---------- 1. Get sitemap index ----------
crawl_delay, robots_sitemap = check_robots_txt()
if robots_sitemap and robots_sitemap.startswith('http'):
        SITEMAP_INDEX = robots_sitemap
print(f"Fetching sitemap index: {SITEMAP_INDEX}")
index_xml = fetch_xml(SITEMAP_INDEX)
if not index_xml:
    print("Failed to fetch sitemap index", file=sys.stderr)
    sys.exit(1)

try:
    root = ET.fromstring(index_xml)
except ET.ParseError as e:
    print(f"Failed to parse sitemap index XML: {e}", file=sys.stderr)
    sys.exit(1)

ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
sitemap_locs = []
for loc in root.findall(".//ns:loc", ns) or root.findall(".//loc"):
    if loc.text:
        sitemap_locs.append(loc.text.strip())

if not sitemap_locs:
    print("No sitemaps found in index", file=sys.stderr)
    sys.exit(1)

# Apply sitemap offset & limit
if SITEMAP_OFFSET >= len(sitemap_locs):
    print(f"Offset {SITEMAP_OFFSET} exceeds total sitemaps ({len(sitemap_locs)})", file=sys.stderr)
    sys.exit(0)

end = SITEMAP_OFFSET + MAX_SITEMAPS if MAX_SITEMAPS > 0 else len(sitemap_locs)
sitemap_locs = sitemap_locs[SITEMAP_OFFSET:end]

print(f"Total sitemaps to analyze: {len(sitemap_locs)}")
print(f"Chunk generator workers: {CHUNK_GEN_WORKERS}")

# ---------- 2. For each sitemap, count product URLs ----------
sitemap_stats = []

def process_sitemap(sm_url):
    xml = fetch_xml(sm_url)
    if not xml:
        return {"url": sm_url, "total_urls": 0}
    try:
        root_sm = ET.fromstring(xml)
    except ET.ParseError:
        return {"url": sm_url, "total_urls": 0}
    urls = []
    for loc in root_sm.findall(".//ns:loc", ns) or root_sm.findall(".//loc"):
        if loc.text and ".html" in loc.text and not any(
            ext in loc.text for ext in [".jpg", ".jpeg", ".png", ".gif", ".webp"]
        ):
            urls.append(loc.text.strip())
    total = len(urls)
    if MAX_URLS_PER_SITEMAP > 0 and total > MAX_URLS_PER_SITEMAP:
        total = MAX_URLS_PER_SITEMAP
    return {"url": sm_url, "total_urls": total}

worker_count = max(1, min(CHUNK_GEN_WORKERS, len(sitemap_locs)))
with ThreadPoolExecutor(max_workers=worker_count) as executor:
    futures = [executor.submit(process_sitemap, url) for url in sitemap_locs]
    for future in as_completed(futures):
        sitemap_stats.append(future.result())
        time.sleep(0.2)  # be polite

# ---------- 3. Generate chunks (one matrix entry per chunk) ----------
chunks = []
chunk_id = 0
for sm in sitemap_stats:
    total = sm["total_urls"]
    if total == 0:
        continue
    num_chunks = (total + URLS_PER_JOB - 1) // URLS_PER_JOB
    for i in range(num_chunks):
        offset = i * URLS_PER_JOB
        limit = min(URLS_PER_JOB, total - offset)
        chunks.append(
            {
                "sitemap_url": sm["url"],
                "offset": offset,
                "limit": limit,
                "chunk_id": chunk_id,
                "base_url": CURR_URL,
            }
        )
        chunk_id += 1

print(f"Generated {len(chunks)} chunks")

# ---------- 4. Output matrix to GITHUB_OUTPUT ----------
matrix_json = json.dumps(chunks)
github_output = os.environ.get("GITHUB_OUTPUT")
if github_output:
    with open(github_output, "a") as f:
        f.write(f"matrix={matrix_json}\n")
else:
    # When running locally, just print
    print(f"matrix={matrix_json}")
