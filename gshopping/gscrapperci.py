#!/usr/bin/env python3
"""
Google Shopping Scraper - GitHub Actions Version
Optimized for headless execution in CI/CD environment
"""

import sys
import json
import random
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
import time
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
import undetected_chromedriver as uc
import os
import csv
import traceback

# Disable unnecessary logging in CI
os.environ['WDM_LOG_LEVEL'] = '0'
os.environ['WDM_PRINT_FIRST_LINE'] = 'False'

def setup_driver():
    """Setup Chrome driver optimized for headless execution"""
    options = uc.ChromeOptions()
    
    # Headless mode for GitHub Actions
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--log-level=3")
    options.add_argument("--silent")
    
    # User agents
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    ]
    options.add_argument(f"user-agent={random.choice(user_agents)}")
    
    # Disable automation flags
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    try:
        driver = uc.Chrome(options=options)
        # Remove webdriver property
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        return driver
    except Exception as e:
        print(f"Error setting up Chrome: {e}")
        raise

def save_to_csv(data, filename, headers=None):
    """Save data to CSV with all fields"""
    if not data:
        print(f"No data to save to {filename}")
        return
    
    os.makedirs('scraping_results', exist_ok=True)
    filepath = os.path.join('scraping_results', filename)
    
    # Get all unique keys
    all_keys = set()
    for item in data:
        if isinstance(item, dict):
            all_keys.update(item.keys())
    
    if not all_keys:
        return
    
    headers = list(all_keys)
    
    with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()
        
        for row in data:
            # Ensure all keys are present
            for header in headers:
                if header not in row:
                    row[header] = ''
            writer.writerow(row)
    
    print(f"Saved {len(data)} rows to {filepath}")

def load_product_urls():
    """Load product URLs from file or environment variable"""
    try:
        # Try to load from file
        with open('product_urls.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        # Try to load from environment variable
        urls_json = os.environ.get('PRODUCT_URLS_JSON')
        if urls_json:
            return json.loads(urls_json)
        else:
            print("No product URLs found. Please create product_urls.json or set PRODUCT_URLS_JSON environment variable.")
            return []

def scrape_product(driver, product_info, all_results):
    """Scrape a single product"""
    product_id = product_info.get('product_id')
    url = product_info.get('url')
    keyword = product_info.get('keyword')
    
    print(f"Scraping product {product_id}: {keyword}")
    
    try:
        driver.get(url)
        time.sleep(random.uniform(5, 8))
        
        # Simple scraping logic (simplified for GitHub Actions)
        product_data = {
            'product_id': product_id,
            'keyword': keyword,
            'url': url,
            'scraped_at': datetime.now().isoformat(),
            'status': 'attempted'
        }
        
        # Try to find product container
        try:
            mains = driver.find_element(By.CLASS_NAME, "dURPMd")
            product_data['status'] = 'container_found'
            
            # Find products
            products = mains.find_elements(By.CLASS_NAME, 'MtXiu')
            if products:
                product_data['product_count'] = len(products)
                
                # Get first product
                first_product = products[0]
                try:
                    product_name = first_product.find_element(By.XPATH, ".//div[contains(@class,'gkQHve')]").text
                    product_data['product_name'] = product_name
                except:
                    pass
                
                try:
                    seller = first_product.find_element(By.XPATH, ".//span[contains(@class,'WJMUdc')]").text
                    product_data['seller'] = seller
                except:
                    pass
        
        except Exception as e:
            product_data['status'] = f'error: {str(e)[:100]}'
        
        all_results['products'].append(product_data)
        
    except Exception as e:
        print(f"Error scraping product {product_id}: {e}")
        all_results['products'].append({
            'product_id': product_id,
            'keyword': keyword,
            'url': url,
            'scraped_at': datetime.now().isoformat(),
            'status': f'failed: {str(e)[:100]}'
        })

def main():
    """Main scraping function optimized for GitHub Actions"""
    print("Starting Google Shopping Scraper on GitHub Actions")
    print("=" * 60)
    
    # Load product URLs
    products = load_product_urls()
    if not products:
        print("No products to scrape. Exiting.")
        return
    
    print(f"Loaded {len(products)} products to scrape")
    
    # Initialize results
    all_results = {
        'products': [],
        'metadata': {
            'run_at': datetime.now().isoformat(),
            'total_products': len(products),
            'environment': 'github-actions'
        }
    }
    
    driver = None
    try:
        driver = setup_driver()
        
        for i, product in enumerate(products, 1):
            print(f"\nProcessing product {i}/{len(products)}")
            scrape_product(driver, product, all_results)
            
            # Take a break every 3 products
            if i % 3 == 0:
                print("Taking a short break...")
                time.sleep(random.uniform(10, 15))
            
            # Save progress every 5 products
            if i % 5 == 0:
                save_to_csv(all_results['products'], 'progress_products.csv')
        
        print(f"\nScraping completed. Total results: {len(all_results['products'])}")
        
    except Exception as e:
        print(f"Error in main scraping loop: {e}")
        traceback.print_exc()
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass
    
    # Save final results
    if all_results['products']:
        save_to_csv(all_results['products'], 'all_products.csv')
    
    # Save summary JSON
    summary_file = os.path.join('scraping_results', 'summary.json')
    with open(summary_file, 'w') as f:
        json.dump(all_results, f, indent=2, default=str)
    
    print(f"\nResults saved to scraping_results/")
    print(f"Successful scrapes: {len([p for p in all_results['products'] if p.get('status') == 'container_found'])}")
    print(f"Failed scrapes: {len([p for p in all_results['products'] if 'failed' in str(p.get('status'))])}")

if __name__ == "__main__":
    main()