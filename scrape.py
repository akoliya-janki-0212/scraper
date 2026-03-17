import os
import sys
import json
import argparse
import subprocess
from pathlib import Path

COMPETITORS_FILE = "competitors.json"

def load_competitors():
    if not os.path.exists(COMPETITORS_FILE):
        print(f"Error: {COMPETITORS_FILE} not found.")
        sys.exit(1)
    with open(COMPETITORS_FILE, "r") as f:
        return json.load(f)

def run_scraper(competitor_key, max_workers=None, delay=None):
    competitors = load_competitors()
    if competitor_key not in competitors:
        print(f"Error: Competitor '{competitor_key}' not found in {COMPETITORS_FILE}.")
        print("Run 'python scrape.py --list' to see available competitors.")
        sys.exit(1)

    comp = competitors[competitor_key]
    scraper_path = comp.get("scraper")
    url = comp.get("url")

    if not scraper_path:
        print(f"Error: No scraper script configured for '{competitor_key}'.")
        print(f"Status: {comp.get('status')} | Notes: {comp.get('notes')}")
        sys.exit(1)

    if not os.path.exists(scraper_path):
        print(f"Error: Scraper script '{scraper_path}' not found on disk.")
        sys.exit(1)

    env = os.environ.copy()
    env["CURR_URL"] = url
    
    if max_workers:
        env["MAX_WORKERS"] = str(max_workers)
    if delay:
        env["REQUEST_DELAY"] = str(delay)

    print("=" * 60)
    print(f"Starting scraper for: {comp['name']}")
    print(f"URL: {url}")
    print(f"Script: {scraper_path}")
    print("=" * 60)

    try:
        subprocess.run([sys.executable, scraper_path], env=env, check=True)
    except subprocess.CalledProcessError as e:
        print(f"\nScraper failed with exit code {e.returncode}")
        sys.exit(e.returncode)
    except KeyboardInterrupt:
        print("\nScraping interrupted by user.")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Unified Scraper Entry Point")
    parser.add_argument("-l", "--list", action="store_true", help="List all competitors")
    parser.add_argument("-c", "--competitor", type=str, help="Competitor key to run (e.g. france-and-son)")
    parser.add_argument("-w", "--max-workers", type=int, help="Override MAX_WORKERS environment variable")
    parser.add_argument("-d", "--delay", type=float, help="Override REQUEST_DELAY environment variable")

    args = parser.parse_args()

    if args.list:
        competitors = load_competitors()
        print(f"{'Key':<30} | {'Name':<30} | {'Status':<10} | {'Scraper Script'}")
        print("-" * 100)
        for key, info in sorted(competitors.items()):
            script = info.get("scraper", "") or "N/A"
            status = info.get("status", "unknown")
            print(f"{key:<30} | {info['name'][:30]:<30} | {status:<10} | {script}")
        sys.exit(0)

    if args.competitor:
        run_scraper(args.competitor, max_workers=args.max_workers, delay=args.delay)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
