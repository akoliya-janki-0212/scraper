#!/usr/bin/env python3
"""
Scraper Workflow Dashboard
A Flask-based web UI to view and start all scraping workflows.
Run: python dashboard/app.py
Open: http://localhost:5050
"""

import os
import sys
import signal
import subprocess
import threading
import time
from collections import deque
from datetime import datetime
from pathlib import Path

from flask import Flask, jsonify, render_template, request

# ---------------------------------------------------------------------------
# Project root (one level up from dashboard/)
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# ---------------------------------------------------------------------------
# Workflow registry
# ---------------------------------------------------------------------------
WORKFLOWS = {
    "cymax": {
        "name": "Cymax Sitemap",
        "description": "Discover product .htm URLs from Cymax sitemaps via FlareSolverr",
        "script": "cymax/cymax.py",
        "category": "Sitemap",
        "config_hint": "Uses YAML config file (cymax/sitemap_config.yml)",
        "default_env": {},
        "color": "#FF6B6B",
    },
    "dlr": {
        "name": "Discount Living Rooms",
        "description": "Scrape product data from discountlivingrooms.com via dataLayer",
        "script": "drl/dlr_scraper.py",
        "category": "DataLayer",
        "config_hint": "CURR_URL, SITEMAP_OFFSET, MAX_SITEMAPS, MAX_URLS_PER_SITEMAP, MAX_WORKERS",
        "default_env": {
            "CURR_URL": "https://www.discountlivingrooms.com",
            "SITEMAP_OFFSET": "0",
            "MAX_SITEMAPS": "0",
            "MAX_URLS_PER_SITEMAP": "0",
            "MAX_WORKERS": "4",
        },
        "color": "#4ECDC4",
    },
    "em_scraper": {
        "name": "Emma Mason (FlareSolverr)",
        "description": "Scrape Emma Mason product data via FlareSolverr with multi-endpoint support",
        "script": "drl/em_scraper.py",
        "category": "FlareSolverr",
        "config_hint": "CURR_URL, FLARESOLVERR_URL, SITEMAP_OFFSET, MAX_SITEMAPS, MAX_WORKERS",
        "default_env": {
            "CURR_URL": "https://www.emmamason.com",
            "SITEMAP_OFFSET": "0",
            "MAX_SITEMAPS": "0",
            "MAX_URLS_PER_SITEMAP": "0",
            "MAX_WORKERS": "10",
        },
        "color": "#45B7D1",
    },
    "em_algolia": {
        "name": "Emma Mason Algolia",
        "description": "Fetch Emma Mason products from Algolia search index",
        "script": "drl/em_algolia_fetch.py",
        "category": "API",
        "config_hint": "--page, --hits-per-page, --max-workers, --output-csv",
        "default_env": {},
        "color": "#96CEB4",
    },
    "fpfc": {
        "name": "FurnitureCart / FurniturePick",
        "description": "Scrape FurnitureCart products with bundle variation support via FlareSolverr",
        "script": "fpfc/fp_fc_scraper.py",
        "category": "FlareSolverr",
        "config_hint": "CURR_URL, FLARESOLVERR_URL, SITEMAP_OFFSET, MAX_SITEMAPS, MAX_WORKERS",
        "default_env": {
            "CURR_URL": "https://www.furniturecart.com",
            "SITEMAP_OFFSET": "0",
            "MAX_SITEMAPS": "0",
            "MAX_URLS_PER_SITEMAP": "0",
            "MAX_WORKERS": "4",
        },
        "color": "#FFEAA7",
    },
    "graphql": {
        "name": "Home Depot GraphQL",
        "description": "Scrape Home Depot products via GraphQL API with sitemap discovery",
        "script": "graphql/gql.py",
        "category": "GraphQL",
        "config_hint": "CURR_URL, GRAPHQL_URL, STORE_ID, ZIP_CODE, SITEMAP_OFFSET, MAX_SITEMAPS",
        "default_env": {
            "CURR_URL": "https://www.homedepot.com",
            "SITEMAP_OFFSET": "0",
            "MAX_SITEMAPS": "0",
            "MAX_URLS_PER_SITEMAP": "0",
            "MAX_WORKERS": "4",
        },
        "color": "#DDA0DD",
    },
    "gshopping": {
        "name": "Google Shopping",
        "description": "Scrape Google Shopping competitor data using Selenium + CAPTCHA solver",
        "script": "gshopping/gscrapper.py",
        "category": "Selenium",
        "config_hint": "Reads product_urls.json for input URLs",
        "default_env": {},
        "color": "#F39C12",
    },
    "ovs": {
        "name": "Overstock + BBB",
        "description": "Scrape Overstock products with BBB API cross-reference",
        "script": "ovs-bbb/ovr.py",
        "category": "API",
        "config_hint": "CURR_URL, API_BASE_URL, BBB_API_BASE_URL, SITEMAP_OFFSET, MAX_SITEMAPS",
        "default_env": {
            "CURR_URL": "",
            "API_BASE_URL": "",
            "BBB_API_BASE_URL": "",
            "SITEMAP_OFFSET": "0",
            "MAX_SITEMAPS": "0",
            "MAX_URLS_PER_SITEMAP": "0",
            "MAX_WORKERS": "4",
        },
        "color": "#E74C3C",
    },
    "bbb": {
        "name": "BBB SKU Extractor",
        "description": "Extract modelNumber/SKU from BBB API for variant IDs",
        "script": "ovs-bbb/bbb.py",
        "category": "API",
        "config_hint": "--chunk-id, --total-chunks, --input-file (required CLI args)",
        "default_env": {},
        "color": "#8E44AD",
    },
    "shopify_cf": {
        "name": "Shopify (Cloudflare)",
        "description": "Scrape Shopify stores protected by Cloudflare using cloudscraper + curl_cffi",
        "script": "shopify-scrapper/shopifyscrap-cloudflare.py",
        "category": "Cloudflare",
        "config_hint": "CURR_URL, SITEMAP_OFFSET, MAX_SITEMAPS, MAX_URLS_PER_SITEMAP",
        "default_env": {
            "CURR_URL": "",
            "SITEMAP_OFFSET": "0",
            "MAX_SITEMAPS": "0",
            "MAX_URLS_PER_SITEMAP": "0",
            "MAX_WORKERS": "4",
        },
        "color": "#1ABC9C",
    },
    "shopify_normal": {
        "name": "Shopify (Normal)",
        "description": "Scrape standard Shopify stores via .js product endpoint",
        "script": "shopify-scrapper/shopifyscrap-normal.py",
        "category": "HTTP",
        "config_hint": "CURR_URL, SITEMAP_OFFSET, MAX_SITEMAPS, MAX_URLS_PER_SITEMAP",
        "default_env": {
            "CURR_URL": "",
            "SITEMAP_OFFSET": "0",
            "MAX_SITEMAPS": "0",
            "MAX_URLS_PER_SITEMAP": "0",
            "MAX_WORKERS": "8",
        },
        "color": "#2ECC71",
    },
}

# ---------------------------------------------------------------------------
# Process manager
# ---------------------------------------------------------------------------
class ProcessManager:
    """Track running scraper sub-processes."""

    def __init__(self):
        self._procs: dict[str, dict] = {}
        self._logs: dict[str, deque] = {}
        self._lock = threading.Lock()

    def start(self, key: str, env_overrides: dict | None = None) -> dict:
        with self._lock:
            if key in self._procs and self._procs[key]["proc"].poll() is None:
                return {"error": "already running"}

            wf = WORKFLOWS[key]
            script = str(PROJECT_ROOT / wf["script"])

            env = os.environ.copy()
            # Apply default env from workflow config
            env.update(wf.get("default_env", {}))
            # Apply user overrides
            if env_overrides:
                env.update(env_overrides)

            proc = subprocess.Popen(
                [sys.executable, script],
                cwd=str(PROJECT_ROOT),
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )

            log_buf = deque(maxlen=200)
            self._logs[key] = log_buf
            self._procs[key] = {
                "proc": proc,
                "started": datetime.now().isoformat(),
                "pid": proc.pid,
            }

            # Background thread to read output
            t = threading.Thread(target=self._reader, args=(key, proc, log_buf), daemon=True)
            t.start()

            return {"status": "started", "pid": proc.pid}

    def _reader(self, key: str, proc: subprocess.Popen, buf: deque):
        try:
            for line in proc.stdout:
                buf.append(line.rstrip("\n"))
        except Exception:
            pass

    def stop(self, key: str) -> dict:
        with self._lock:
            info = self._procs.get(key)
            if not info or info["proc"].poll() is not None:
                return {"status": "not_running"}
            try:
                os.killpg(os.getpgid(info["proc"].pid), signal.SIGTERM)
            except Exception:
                info["proc"].terminate()
            info["proc"].wait(timeout=5)
            return {"status": "stopped"}

    def status(self, key: str) -> dict:
        info = self._procs.get(key)
        if not info:
            return {"state": "idle", "logs": []}

        running = info["proc"].poll() is None
        return_code = info["proc"].returncode

        state = "running" if running else ("completed" if return_code == 0 else "error")

        logs = list(self._logs.get(key, []))
        return {
            "state": state,
            "pid": info["pid"],
            "started": info["started"],
            "return_code": return_code,
            "logs": logs[-80:],
        }

    def all_statuses(self) -> dict:
        result = {}
        for key in WORKFLOWS:
            result[key] = self.status(key)
        return result


pm = ProcessManager()

# ---------------------------------------------------------------------------
# Flask app
# ---------------------------------------------------------------------------
app = Flask(__name__)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/workflows")
def api_workflows():
    statuses = pm.all_statuses()
    workflows = []
    for key, wf in WORKFLOWS.items():
        st = statuses.get(key, {"state": "idle"})
        workflows.append({
            "key": key,
            "name": wf["name"],
            "description": wf["description"],
            "script": wf["script"],
            "category": wf["category"],
            "config_hint": wf["config_hint"],
            "color": wf["color"],
            "default_env": wf.get("default_env", {}),
            **st,
        })
    return jsonify(workflows)


@app.route("/api/workflows/<key>/start", methods=["POST"])
def api_start(key):
    if key not in WORKFLOWS:
        return jsonify({"error": "unknown workflow"}), 404
    body = request.get_json(silent=True) or {}
    env_overrides = body.get("env", {})
    result = pm.start(key, env_overrides)
    code = 200 if "error" not in result else 409
    return jsonify(result), code


@app.route("/api/workflows/<key>/status")
def api_status(key):
    if key not in WORKFLOWS:
        return jsonify({"error": "unknown workflow"}), 404
    return jsonify(pm.status(key))


@app.route("/api/workflows/<key>/stop", methods=["POST"])
def api_stop(key):
    if key not in WORKFLOWS:
        return jsonify({"error": "unknown workflow"}), 404
    return jsonify(pm.stop(key))


if __name__ == "__main__":
    print("🚀 Scraper Dashboard running at http://localhost:5050")
    app.run(host="0.0.0.0", port=5050, debug=False)
