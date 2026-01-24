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
from selenium.webdriver.common.keys import Keys
import undetected_chromedriver as uc
import os
from solvecaptcha import solve_recaptcha_audio
import csv
import traceback

def swipe_element(driver, element, start_x, start_y, end_x, end_y, duration=1000):
    driver.execute_script("arguments[0].scrollIntoView();", element)
    action = ActionChains(driver)
    action.move_to_element_with_offset(element, start_x, start_y).click_and_hold().pause(0.2)
    action.move_by_offset(end_x, end_y).release().perform()

def setup_driver():
    if os.getenv("GITHUB_ACTIONS") != "true":
        os.system("pkill chrome")
    time.sleep(2)
    options = uc.ChromeOptions()
    if os.getenv("GITHUB_ACTIONS") == "true":
        options.add_argument("--headless=new")
    # options.add_argument("--headless=new")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--disable-logging")
    options.add_argument("--log-level=3")
    options.add_argument("--remote-debugging-port=9222")
    options.add_argument("--disable-background-timer-throttling")
    options.add_argument("--disable-backgrounding-occluded-windows")
    options.add_argument("--disable-ipc-flooding-protection")
    options.add_argument("--enable-features=NetworkService,NetworkServiceInProcess")
    options.add_argument("--disable-renderer-backgrounding")

    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    ]
    options.add_argument(f"user-agent={random.choice(user_agents)}")

    # Let undetected_chromedriver auto-detect the correct version
    driver = uc.Chrome(options=options)
    
    return driver

def detects_recaptcha(driver):
    try:
        if driver.find_elements(By.CLASS_NAME, "rc-imageselect-challenge"):
            print("Puzzle reCAPTCHA detected!")
            return True
        elif driver.find_elements(By.TAG_NAME, "iframe"):
            for iframe in driver.find_elements(By.TAG_NAME, "iframe"):
                if iframe.get_attribute("src") and "recaptcha" in iframe.get_attribute("src"):
                    print("reCAPTCHA iframe detected!")
                    return True
        else:
            print("No reCAPTCHA found.")
            return False
    except:
        print("Error detecting reCAPTCHA")
        return True

def start_new_driver(search_url):
    while True:
        try:
            driver.quit()
        except:
            pass
        driver = setup_driver()
        driver.get(search_url)
        recaptcha = detects_recaptcha(driver)
        if not recaptcha:
            return driver
        else:
            result = solve_recaptcha_audio(driver)
            if result == "solved":
                return driver
            else:
                driver.quit()

def get_product_options(driver):
    scraped_data = {}
    
    try:
        panel = driver.find_element(By.XPATH, "//div[@jsname='Ql2bfc']")
    except NoSuchElementException:
        try:
            panel = driver.find_element(By.XPATH, "//div[@jsname='jzfSje']")
        except NoSuchElementException:
            print("Error: Could not find any product panel container.")
            return json.dumps({}, indent=2)

    # Scrape Swatch-style Filters
    swatch_groups = panel.find_elements(By.XPATH, ".//div[@jsname='iaBacd']")
    
    for group in swatch_groups:
        try:
            title = group.find_element(By.XPATH, ".//span[@class='ZMOBjc']").text
            if not title:
                continue
            
            options = []
            swatches = group.find_elements(By.XPATH, ".//a[@jsname='dbgGYd']")
            for swatch in swatches:
                label = swatch.get_attribute('data-label')
                if label:
                    options.append(label)
            
            if title and options:
                scraped_data[title] = list(dict.fromkeys(options))
                
        except Exception as e:
            print(f"Warning: Could not parse a swatch group. Error: {e}")
            continue

    # Scrape Dropdown-style Filters
    dropdown_groups = panel.find_elements(By.XPATH, ".//div[@data-attrid='variant_picker_chip']")

    for group in dropdown_groups:
        try:
            title_text_element = group.find_element(By.XPATH, ".//div[contains(@class, 'PQev6c')]")
            title_text = title_text_element.get_attribute('textContent').strip()
            
            if ":" in title_text:
                title = title_text.split(":")[0].strip()
            else:
                title = title_text.strip()
                
            if not title:
                continue
                
            options = []
            menu_items = group.find_elements(By.XPATH, ".//g-menu/g-menu-item")
            if menu_items:
                for item in menu_items:
                    try:
                        item_text = item.find_element(By.XPATH, ".//span").get_attribute('textContent').strip()
                        if item_text:
                            options.append(item_text)
                    except NoSuchElementException:
                        continue
            else:
                popup_items = group.find_elements(By.XPATH, ".//g-popup//div[@role='menuitemradio']")
                for item in popup_items:
                    try:
                        item_text = item.find_element(By.XPATH, ".//div[@class='PQev6c']").get_attribute('textContent').strip()
                        if item_text:
                            options.append(item_text)
                    except NoSuchElementException:
                        continue

            if title and options:
                scraped_data[title] = list(dict.fromkeys(options))

        except Exception as e:
            print(f"Warning: Could not parse a dropdown group ('{title}'). Error: {e}")
            continue
    
    return json.dumps(scraped_data, indent=2)

def save_to_csv(data, filename, headers=None):
    """Save data to CSV file"""
    if not data:
        print(f"No data to save to {filename}")
        return
    
    # Create directory if it doesn't exist
    os.makedirs('scraping_results', exist_ok=True)
    filepath = os.path.join('scraping_results', filename)
    
    # If no headers provided, extract from all dictionaries
    if headers is None:
        # Collect all unique keys from all dictionaries
        all_keys = set()
        for item in data:
            if isinstance(item, dict):
                all_keys.update(item.keys())
        headers = list(all_keys)
    elif isinstance(data[0], dict):
        # Ensure all dictionaries have the same structure
        # Add any missing keys to make all dictionaries consistent
        all_keys = set(headers)
        for item in data:
            if isinstance(item, dict):
                all_keys.update(item.keys())
        headers = list(all_keys)
    
    with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
        if isinstance(data[0], dict):
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            for row in data:
                # Ensure all rows have all headers (fill missing with empty strings)
                for header in headers:
                    if header not in row:
                        row[header] = ''
                writer.writerow(row)
        else:
            writer = csv.writer(csvfile)
            writer.writerows(data)
    
    print(f"✓ Data saved to {filepath}")

def scrape_google_keyword_competitior(url, product_id, keyword, driver, all_results):
    driver.get(url)
    recaptcha = detects_recaptcha(driver)
    if recaptcha:
        result = solve_recaptcha_audio(driver)
        if result == "solved":
            driver.switch_to.default_content()
        elif result == "quit":
            time.sleep(random.uniform(5, 8))
            driver = start_new_driver(url)
    
    time.sleep(random.uniform(5, 10))
    
    try:
        mains = driver.find_element(By.CLASS_NAME, "dURPMd")
        print(f"[{product_id}] Product container found")
    except Exception as e:
        print(f"[{product_id}] Product container not found")
        return driver
    
    for i in range(3):
        products = mains.find_elements(By.CLASS_NAME, 'MtXiu')
        if not products:
            try:
                driver.quit()
            except:
                pass
            driver = start_new_driver(url)
            continue
        break
    
    print(f"[{product_id}] Processing Product")
    productData = {}
    products = mains.find_elements(By.CLASS_NAME, 'MtXiu')
    
    for product in products:
        try:
            product_name = product.find_element(By.XPATH, ".//div[contains(@class,'gkQHve')]").text
        except Exception:
            product_name = None
        
        try:
            seller = product.find_element(By.XPATH, ".//span[contains(@class,'WJMUdc')]").text
        except Exception:
            seller = None
        
        try:
            cid = product.get_attribute('id')
            pid = None
        except Exception:
            cid = pid = None
        
        if (not "Set" in product_name and "Set" in keyword) or ("Set" in product_name and not "Set" in keyword):
            print(f"[{product_id}] Skipping container - name mismatch")
            continue
        
        productData = {
            'product_id': product_id,
            'keyword': keyword,
            'product_url': None,
            'seller': seller,
            'product_name': product_name,
            'cid': cid,
            'pid': pid,
            'last_fetched_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'status': 'found'
        }
        break
    
    if not productData:
        print(f"[{product_id}] No product data found")
        all_results['products'].append({
            'product_id': product_id,
            'keyword': keyword,
            'product_url': None,
            'seller': None,
            'product_name': None,
            'cid': None,
            'pid': None,
            'last_fetched_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'status': 'not_found'
        })
        return driver
    
    cid = productData['cid']
    if cid:
        try:
            element = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, f'//div[@id="{cid}"]'))
            )
            if element:
                driver.execute_script("arguments[0].scrollIntoView(true);", element)
                time.sleep(1)
                element.click()
                print(f"[{product_id}] Clicked on the element successfully")
            time.sleep(random.uniform(1, 3))
        except:
            print(f"[{product_id}] Could not click element")
    
    productData['product_url'] = driver.current_url
    productData['status'] = 'clicked'
    all_results['products'].append(productData)
    
    # Save individual product data
    save_to_csv([productData], f"product_{product_id}.csv", headers=productData.keys())
    
    all_competitors = []
    i = 0
    while True:
        try:
            i += 1
            more_stores = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//div[contains(@class,'duf-h')]//div[@role='button']"))
            )
            more_stores.click()
            time.sleep(random.uniform(2, 4))
            if i == 2:
                break
        except Exception:
            break
    
    try:
        offers_grid = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//div[@jsname='RSFNod' and @data-attrid='organic_offers_grid']"))
        )
        
        exists = len(driver.find_elements(
            By.XPATH,
            "//div[contains(@class,'iI1aN')]//div[@class='EDblX kjqWgb']"
        )) > 0
        
        if exists > 0:
            product_options = get_product_options(driver)
            print(f"[{product_id}] Found offers grid with options")
            all_results['products'][-1]['options'] = product_options
        else:
            print(f"[{product_id}] Found offers grid")
        
        offer_elements = offers_grid.find_elements(By.CLASS_NAME, 'R5K7Cb')
        print(f"[{product_id}] Found {len(offer_elements)} offers")
        
        for seller_html in offer_elements:
            try:
                store_name = seller_html.find_element(By.CSS_SELECTOR, "div.hP4iBf.gUf0b.uWvFpd").text.strip()
            except:
                store_name = "N/A"
            
            try:
                product_name = seller_html.find_element(By.CSS_SELECTOR, "div.Rp8BL").text.strip()
            except:
                product_name = "N/A"
            
            try:
                seller_url = seller_html.find_element(By.CSS_SELECTOR, "a.P9159d").get_attribute('href')
            except:
                seller_url = "N/A"
            
            try:
                seller_price_element = seller_html.find_element(By.CSS_SELECTOR, "div.QcEgce span[aria-hidden='true']")
                seller_price = seller_price_element.text.strip()
            except:
                try:
                    seller_price_element = seller_html.find_element(By.CSS_SELECTOR, "div.GBgquf span")
                    seller_price = seller_price_element.text.strip()
                except:
                    seller_price = "N/A"
            
            competitor_data = {
                'product_id': product_id,
                'keyword': keyword,
                'seller': store_name,
                'seller_product_name': product_name,
                'seller_url': seller_url,
                'seller_price': seller_price,
                'last_fetched_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            all_competitors.append(competitor_data)
            all_results['competitors'].append(competitor_data)
        
        # Save competitors data
        if all_competitors:
            save_to_csv(all_competitors, f"competitors_{product_id}.csv", headers=all_competitors[0].keys())
        
        # Calculate OSB position
        search_seller = '1StopBedrooms'
        sellers = [c['seller'] for c in all_competitors]
        osb_position = 0
        seller_count = len(sellers)
        
        if search_seller in sellers:
            osb_position = sellers.index(search_seller) + 1
        
        # Update product data with OSB position
        all_results['products'][-1]['osb_position'] = osb_position
        all_results['products'][-1]['seller_count'] = seller_count
        all_results['products'][-1]['status'] = 'completed'
        
        print(f"[{product_id}] OSB Position: {osb_position}, Total Sellers: {seller_count}")
        
    except Exception as e:
        print(f"[{product_id}] Offers grid not found: {str(e)}")
        all_results['products'][-1]['status'] = 'no_offers_found'
    
    print(f"[{product_id}] Scraping completed")
    return driver

def load_product_urls(filepath='product_urls.json'):
    """Load product URLs from JSON file"""
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
        return data
    except FileNotFoundError:
        print(f"Error: {filepath} not found")
        print("Please create a product_urls.json file with format:")
        print('''
[
    {
        "product_id": 1,
        "url": "https://www.google.com/search?q=Acme+Louis+Phillipe+III+Twin+Sleigh+Bed",
        "keyword": "Acme Louis Phillipe III Twin Sleigh Bed"
    }
]
        ''')
        return []

def main():
    # Create results directory
    os.makedirs('scraping_results', exist_ok=True)
    
    # Load product URLs
    products = load_product_urls()
    if not products:
        print("No products to scrape. Exiting...")
        return
    
    print(f"Loaded {len(products)} products to scrape")
    
    # Initialize results storage
    all_results = {
        'products': [],
        'competitors': []
    }
    
    driver = setup_driver()
    
    for i, product in enumerate(products, 1):
        try:
            product_id = product.get('product_id', i)
            url = product['url']
            keyword = product['keyword']
            
            print(f"\n{'='*60}")
            print(f"Scraping {i}/{len(products)}")
            print(f"Product ID: {product_id}")
            print(f"Keyword: {keyword}")
            print(f"URL: {url[:100]}...")
            print(f"{'='*60}")
            
            if i % 10 == 0:
                print(f"Taking a 30-second break after 10 products...")
                time.sleep(30)
            
            time.sleep(random.uniform(3, 6))
            
            driver = scrape_google_keyword_competitior(url, product_id, keyword, driver, all_results)
            
        except Exception as e:
            print(f"Error scraping product {product_id}: {str(e)}")
            traceback.print_exc()
            print("Reinitializing driver...")
            try:
                driver.quit()
            except:
                pass
            driver = setup_driver()
        
        finally:
            now = datetime.now()
            print(f"Progress: {i}/{len(products)} - {now.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Save summary files
    if all_results['products']:
        save_to_csv(all_results['products'], 'all_products_summary.csv', headers=all_results['products'][0].keys())
    
    if all_results['competitors']:
        save_to_csv(all_results['competitors'], 'all_competitors_summary.csv', headers=all_results['competitors'][0].keys())
    
    # Save JSON summary
    summary_file = os.path.join('scraping_results', 'scraping_summary.json')
    with open(summary_file, 'w') as f:
        json.dump(all_results, f, indent=2, default=str)
    
    print(f"\n{'='*60}")
    print("SCRAPING COMPLETED")
    print(f"{'='*60}")
    print(f"Products scraped: {len(all_results['products'])}")
    print(f"Competitors found: {len(all_results['competitors'])}")
    print(f"Results saved to: scraping_results/")
    print(f"Summary file: scraping_summary.json")
    
    driver.quit()

if __name__ == "__main__":
    IS_CI = os.getenv("GITHUB_ACTIONS") == "true"
    print("Google Shopping Scraper - CSV Output Version")
    print("=" * 60)
    print("This script will:")
    print("1. Scrape product data from Google Shopping")
    print("2. Find competitor offers")
    print("3. Save results to CSV files")
    print("=" * 60)
    
    # Check if product_urls.json exists
    if not os.path.exists('product_urls.json'):
        print("\nCreating sample product_urls.json file...")
        sample_data = [
            {
                "product_id": 1,
                "url": "https://www.google.com/search?q=Glory+Furniture+G3150+Twin+Low+Profile+Storage+Bed&udm=28&gl=US&hl=en&pws=0",
                "keyword": "Glory Furniture G3150 Twin Low Profile Storage Bed"
            },
            {
                "product_id": 2,
                "url": "https://www.google.com/search?q=Acme+Louis+Phillipe+III+Twin+Sleigh+Bed&udm=28&gl=US&hl=en&pws=0",
                "keyword": "Acme Louis Phillipe III Twin Sleigh Bed"
            }
        ]
        with open('product_urls.json', 'w') as f:
            json.dump(sample_data, f, indent=2)
        print("Sample product_urls.json created. Please edit with your actual URLs.")
        if IS_CI:
            print("CI detected — continuing automatically")
        else:
            input("Press Enter to continue with sample data, or Ctrl+C to exit: ")
    
    main()