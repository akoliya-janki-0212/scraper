#!/usr/bin/env python3
"""
Google Shopping Scraper - GitHub Actions Version
Fixed for Chrome compatibility issues
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
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
import os
import csv
import traceback

# Disable unnecessary logging
os.environ['WDM_LOG_LEVEL'] = '0'

def setup_driver():
    """Setup Chrome driver for GitHub Actions - FIXED VERSION"""
    chrome_options = Options()
    
    # Headless mode for GitHub Actions
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-notifications")
    chrome_options.add_argument("--disable-popup-blocking")
    chrome_options.add_argument("--log-level=3")
    chrome_options.add_argument("--silent")
    
    # User agents
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    ]
    chrome_options.add_argument(f"user-agent={random.choice(user_agents)}")
    
    # DON'T use excludeSwitches with undetected_chromedriver
    # Just use plain Selenium Chrome driver for GitHub Actions
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    
    try:
        # Use Chrome directly with webdriver-manager
        from webdriver_manager.chrome import ChromeDriverManager
        from selenium.webdriver.chrome.service import Service
        
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # Remove webdriver property
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        print("Chrome driver setup successful")
        return driver
        
    except Exception as e:
        print(f"Error setting up Chrome with webdriver-manager: {e}")
        
        # Fallback: Try system Chrome
        try:
            driver = webdriver.Chrome(options=chrome_options)
            print("Fallback Chrome driver setup successful")
            return driver
        except Exception as e2:
            print(f"Fallback also failed: {e2}")
            raise

def save_to_csv(data, filename, headers=None):
    """Save data to CSV with all fields"""
    if not data:
        print(f"No data to save to {filename}")
        return
    
    # Create directory if it doesn't exist
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
    
    try:
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            
            for row in data:
                # Ensure all keys are present
                for header in headers:
                    if header not in row:
                        row[header] = ''
                writer.writerow(row)
        
        print(f"✓ Saved {len(data)} rows to {filepath}")
        return True
        
    except Exception as e:
        print(f"✗ Error saving {filename}: {str(e)}")
        return False

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
            return json.loads(urlls_json)
        else:
            print("No product URLs found.")
            print("Please create product_urls.json or set PRODUCT_URLS_JSON environment variable.")
            return []

def scrape_product(driver, product_info, all_results):
    """Scrape a single product"""
    product_id = product_info.get('product_id')
    url = product_info.get('url')
    keyword = product_info.get('keyword')
    
    print(f"\n{'='*60}")
    print(f"Scraping product {product_id}: {keyword}")
    print(f"URL: {url[:80]}...")
    
    product_data = {
        'product_id': product_id,
        'keyword': keyword,
        'url': url,
        'scraped_at': datetime.now().isoformat(),
        'status': 'attempted'
    }
    
    try:
        # Navigate to URL
        driver.get(url)
        time.sleep(random.uniform(5, 8))
        
        # Check for recaptcha
        try:
            if "recaptcha" in driver.page_source.lower() or "captcha" in driver.page_source.lower():
                product_data['status'] = 'recaptcha_detected'
                print("⚠️  reCAPTCHA detected")
                all_results['products'].append(product_data)
                return
        except:
            pass
        
        # Get page title
        product_data['page_title'] = driver.title[:200]
        
        # Try to find shopping results container
        try:
            # Look for shopping container
            shopping_containers = driver.find_elements(By.CSS_SELECTOR, "div[data-initq], div.sh-dgr__content, div[jscontroller]")
            
            if shopping_containers:
                product_data['status'] = 'shopping_container_found'
                product_data['container_count'] = len(shopping_containers)
                print(f"✓ Found {len(shopping_containers)} shopping containers")
                
                # Try to find product items
                try:
                    product_items = driver.find_elements(By.CSS_SELECTOR, "div.sh-dlr__list-result, div[data-cid], div.MtXiu")
                    product_data['product_items_found'] = len(product_items)
                    print(f"✓ Found {len(product_items)} product items")
                    
                    if product_items:
                        # Try to get first product name
                        try:
                            first_product = product_items[0]
                            product_name = first_product.text[:200]
                            product_data['sample_product'] = product_name
                        except:
                            pass
                except Exception as e:
                    product_data['product_items_error'] = str(e)[:100]
                    
            else:
                product_data['status'] = 'no_shopping_container'
                print("✗ No shopping container found")
                
        except Exception as e:
            product_data['status'] = f'container_error: {str(e)[:100]}'
            print(f"✗ Error finding container: {str(e)[:100]}")
        
        # Check if page loaded successfully
        product_data['page_load_success'] = True
        if "did not match any documents" in driver.page_source or "no results" in driver.page_source.lower():
            product_data['status'] = 'no_results'
            print("⚠️  No results found for this query")
            
    except Exception as e:
        product_data['status'] = f'scraping_error: {str(e)[:100]}'
        product_data['page_load_success'] = False
        print(f"✗ Scraping error: {str(e)[:100]}")
    
    all_results['products'].append(product_data)

def main():
    """Main scraping function optimized for GitHub Actions"""
    print("Starting Google Shopping Scraper on GitHub Actions")
    print("=" * 60)
    
    # Load product URLs
    products = load_product_urls()
    if not products:
        print("No products to scrape. Using sample products...")
        # Create sample products for testing
        products = [
            {
                "product_id": 1,
                "url": "https://www.google.com/search?q=office+chair&tbm=shop",
                "keyword": "office chair"
            },
            {
                "product_id": 2,
                "url": "https://www.google.com/search?q=wireless+headphones&tbm=shop",
                "keyword": "wireless headphones"
            }
        ]
    
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
            
            # Take a break between requests
            if i < len(products):
                wait_time = random.uniform(5, 10)
                print(f"Waiting {wait_time:.1f} seconds before next request...")
                time.sleep(wait_time)
        
        print(f"\n{'='*60}")
        print(f"Scraping completed. Total results: {len(all_results['products'])}")
        
        # Calculate success rate
        successful = len([p for p in all_results['products'] 
                         if 'container_found' in str(p.get('status')) or 'found' in str(p.get('status'))])
        print(f"Successful scrapes: {successful}/{len(all_results['products'])}")
        
    except Exception as e:
        print(f"Error in main scraping loop: {e}")
        traceback.print_exc()
        
        # Save partial results even if error occurs
        if all_results['products']:
            save_to_csv(all_results['products'], 'partial_results.csv')
            
    finally:
        if driver:
            try:
                driver.quit()
                print("Chrome driver closed")
            except:
                pass
    
    # Ensure scraping_results directory exists
    os.makedirs('scraping_results', exist_ok=True)
    
    # Save final results
    if all_results['products']:
        save_to_csv(all_results['products'], 'all_products.csv')
    
    # Save summary JSON
    try:
        summary_file = os.path.join('scraping_results', 'summary.json')
        with open(summary_file, 'w') as f:
            # Convert datetime objects to string for JSON serialization
            def json_serial(obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                raise TypeError(f"Type {type(obj)} not serializable")
            
            json.dump(all_results, f, indent=2, default=json_serial)
        print(f"✓ Summary saved to {summary_file}")
    except Exception as e:
        print(f"✗ Error saving JSON summary: {str(e)}")
    
    print(f"\n{'='*60}")
    print("PROCESS COMPLETED")
    print(f"{'='*60}")
    print(f"Results saved to: scraping_results/")

if __name__ == "__main__":
    main()