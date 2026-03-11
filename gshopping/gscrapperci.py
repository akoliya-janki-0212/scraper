import sys
import json
import random
import ftplib
import os
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
import time
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
import undetected_chromedriver as uc
import csv
import traceback
import pandas as pd
import argparse
import re
import shutil
from urllib.parse import urlparse, unquote

PRODUCT_FINAL_COLUMNS = [
    "product_id",
    "web_id",
    "name",
    "mpn_sku",
    "gtin",
    "brand",
    "category",
    "keyword",
    "url",
    "osb_url",
    "last_response",
    "osb_url_match",
    "product_url",
    "seller",
    "product_name",
    "cid",
    "pid",
    "last_fetched_date",
    "osb_position",
    "osb_id",
    "seller_count",
    "status",
]
# Import the existing captcha solving functions
try:
    from solvecaptcha import solve_recaptcha_audio
except ImportError:
    # If solvecaptcha is not in same directory, try to import from current directory
    import importlib.util
    import sys
    
    # Add current directory to path
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    
    try:
        from solvecaptcha import solve_recaptcha_audio
    except ImportError:
        print("Warning: solvecaptcha module not found. Captcha solving will be disabled.")
        
        # Define a dummy function if module is not available
        def solve_recaptcha_audio(driver):
            print("Captcha solving module not available. Please install solvecaptcha.")
            return "failed"

def setup_driver():
    time.sleep(2)
    options = uc.ChromeOptions()
    
    # Comment out for local testing to see browser
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
    options.add_argument("--disable-features=IsolateOrigins,site-per-process")
    options.add_argument("--disable-site-isolation-trials")

    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    ]
    options.add_argument(f"user-agent={random.choice(user_agents)}")

    
    # driver = uc.Chrome(options=options)
    driver = uc.Chrome(options=options,version_main=146)
    return driver

def detects_recaptcha(driver):
    """Detect if reCAPTCHA is present on the page"""
    try:
        if driver.find_elements(By.CLASS_NAME, "rc-imageselect-challenge"):
            print("Puzzle reCAPTCHA detected!")
            return True
        elif driver.find_elements(By.TAG_NAME, "iframe"):
            for iframe in driver.find_elements(By.TAG_NAME, "iframe"):
                src = iframe.get_attribute("src")
                if src and "recaptcha" in src:
                    print("reCAPTCHA iframe detected!")
                    return True
        else:
            print("No reCAPTCHA found.")
            return False
    except Exception as e:
        print(f"Error detecting reCAPTCHA: {e}")
        return False

# In your main gscrapperci.py, update the handle_captcha function:

def handle_captcha(driver, url):
    """Handle captcha if detected with retry logic"""
    max_retries = 1

    for attempt in range(max_retries):
        recaptcha = detects_recaptcha(driver)
        if recaptcha:
            print(f"Attempt {attempt + 1}/{max_retries} to solve captcha...")
            result = solve_recaptcha_audio(driver)
            
            if result == "solved":
                print("Captcha solved successfully!")
                driver.switch_to.default_content()
                return "solved"
            else:
                print(f"Captcha solving attempt {attempt + 1} failed")
                
                # if attempt < max_retries - 1:
                #     # Try refreshing the page
                #     print("Refreshing page and retrying...")
                #     driver.refresh()
                #     time.sleep(5)
                # else:
                #     print("All captcha solving attempts failed")
                #     return "failed"
                print("All captcha solving attempts failed")
                return "failed"
        else:
            print("No reCAPTCHA found.")
            return "no_captcha"
    
    return "failed"

def start_new_driver(search_url):
    """Start a new driver and handle captcha if present"""
    while True:
        try:
            driver.quit()
        except:
            pass
        
        driver = setup_driver()
        driver.get(search_url)
        
        # Handle captcha
        captcha_result = handle_captcha(driver, search_url)
        
        if captcha_result == "solved":
            return driver
        elif captcha_result == "no_captcha":
            return driver
        else:
            # Captcha solving failed, retry with new driver
            print("Captcha solving failed, retrying with new driver...")
            try:
                driver.quit()
            except:
                pass
            time.sleep(random.uniform(5, 8))

def download_csv_from_ftp(ftp_host, ftp_user, ftp_pass, ftp_path, remote_filename, local_filename):
    """Download CSV file from FTP"""
    try:
        print(f"Downloading {remote_filename} from FTP...")
        
        ftp = ftplib.FTP()
        ftp.connect(ftp_host, int(os.getenv("FTP_PORT", 21)))
        ftp.login(ftp_user, ftp_pass)
        ftp.set_pasv(True)
        
        if ftp_path and ftp_path != '/':
            try:
                ftp.cwd(ftp_path)
            except:
                print(f"Error: Could not change to directory {ftp_path}")
                return None
        
        with open(local_filename, 'wb') as f:
            ftp.retrbinary(f'RETR {remote_filename}', f.write)
        
        ftp.quit()
        print(f"✓ Downloaded {remote_filename} to {local_filename}")
        return local_filename
        
    except Exception as e:
        print(f"Error downloading from FTP: {str(e)}")
        return None

def upload_to_ftp(ftp_host, ftp_user, ftp_pass, ftp_path, local_file, remote_filename):
    """Upload file to FTP server"""
    try:
        print(f"Uploading {remote_filename} to FTP...")
        
        ftp = ftplib.FTP()
        ftp.connect(ftp_host, 21)
        ftp.login(ftp_user, ftp_pass)
        ftp.set_pasv(True)
        
        if ftp_path and ftp_path != '/':
            try:
                ftp.cwd(ftp_path)
            except:
                dirs = ftp_path.strip('/').split('/')
                current_path = ''
                for dir in dirs:
                    current_path += '/' + dir
                    try:
                        ftp.cwd(current_path)
                    except:
                        ftp.mkd(current_path)
                        ftp.cwd(current_path)
        
        with open(local_file, 'rb') as f:
            ftp.storbinary(f'STOR {remote_filename}', f)
        
        ftp.quit()
        print(f"✓ Uploaded {remote_filename} to FTP")
        return True
        
    except Exception as e:
        print(f"Error uploading to FTP: {str(e)}")
        return False

def split_csv(input_csv, output_dir, chunk_id, total_chunks):
    """Split CSV into chunks and return specific chunk"""
    try:
        df = pd.read_csv(input_csv)
        
        if df.empty:
            print("CSV file is empty")
            return None
        
        total_rows = len(df)
        rows_per_chunk = total_rows // total_chunks
        
        start_idx = (chunk_id - 1) * rows_per_chunk
        end_idx = chunk_id * rows_per_chunk if chunk_id < total_chunks else total_rows
        
        chunk_df = df.iloc[start_idx:end_idx]
        
        os.makedirs(output_dir, exist_ok=True)
        chunk_filename = f"chunk_{chunk_id}.csv"
        chunk_path = os.path.join(output_dir, chunk_filename)
        
        chunk_df.to_csv(chunk_path, index=False)
        
        print(f"Chunk {chunk_id}: Rows {start_idx+1} to {end_idx} ({len(chunk_df)} rows)")
        return chunk_path
        
    except Exception as e:
        print(f"Error splitting CSV: {str(e)}")
        return None

def get_product_options(driver):
    """Extract product variant options from the product panel"""
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

def normalize_url_path_slug(raw_url):
    """Return normalized last path segment (slug), removing query/fragment."""
    try:
        if not raw_url:
            return ""
        cleaned = str(raw_url).strip()
        if not cleaned or cleaned.lower() == "n/a":
            return ""
        if "://" not in cleaned and cleaned.startswith("www."):
            cleaned = f"https://{cleaned}"

        parsed = urlparse(cleaned)
        path = unquote(parsed.path or "").strip()
        path = re.sub(r"/+", "/", path).rstrip("/")
        if not path:
            return ""
        return path.split("/")[-1].strip().lower()
    except:
        return ""

def scrape_product(driver, product_id, keyword, url, osb_url=""):
    """Scrape individual product from Google Shopping"""
    try:
        print(f"\nScraping Product ID: {product_id}")
        print(f"Keyword: {keyword}")
        
        driver.get(url)
        
        # Handle captcha before proceeding
        captcha_result = handle_captcha(driver, url)
        if captcha_result == "failed":
            return {
                'product_id': product_id,
                'keyword': keyword,
                'url': url,
                'last_response': 'Captcha solving failed',
                'status': 'captcha_failed',
                'last_fetched_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'product_url': '',  # ADD THIS LINE
                'seller': '',  # ADD THIS LINE
                'product_name': '',  # ADD THIS LINE
                'cid': '',  # ADD THIS LINE
                'pid': '',  # ADD THIS LINE
                'osb_position': 0,  # ADD THIS LINE
                'osb_id': '',  # ADD THIS LINE
                'seller_count': 0,  # ADD THIS LINE
                'competitors': []  # Already present
            }
        
        time.sleep(random.uniform(4, 8))
        
        # Initialize result structure
        result = {
            'product_id': product_id,
            'keyword': keyword,
            'url': url,
            'last_response': '',
            'product_url': '',
            'seller': '',
            'product_name': '',
            'cid': '',
            'pid': '',
            'last_fetched_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'osb_position': 0,
            'osb_id': '',
            'seller_count': 0,
            'status': '',
            'competitors': []
        }
        
        # Try to find product container
        try:
            mains = driver.find_element(By.CLASS_NAME, "dURPMd")
            result['last_response'] = "Product container found"
            result['status'] = "found"
        except Exception as e:
            result['last_response'] = f"Product container not found: {str(e)[:20]}..."
            result['status'] = "container_not_found"
            return result
        
        # Find products in container
        products = mains.find_elements(By.CLASS_NAME, 'MtXiu')
        if not products:
            result['last_response'] = "No products found in container"
            result['status'] = "no_products"
            return result
        
        # Process first matching product
        for product in products:
            try:
                product_name = product.find_element(By.XPATH, ".//div[contains(@class,'gkQHve')]").text
            except:
                product_name = ""
            
            try:
                seller = product.find_element(By.XPATH, ".//span[contains(@class,'WJMUdc')]").text
            except:
                seller = ""
            
            try:
                cid = product.get_attribute('id')
            except:
                cid = ""
            
            # Normalize keyword: remove "set of" (case-insensitive) before comparison
            normalized_keyword = re.sub(r'\bset\s+of\b', '', keyword or '', flags=re.IGNORECASE)
            normalized_product_name = re.sub(r'\bset\s+of\b', '', product_name or '', flags=re.IGNORECASE)

            def has_set_word(text):
                return bool(re.search(r'\bset\b', text or '', flags=re.IGNORECASE))

            # Check for Set keyword mismatch (exact word match, case-insensitive)
            if has_set_word(normalized_product_name) != has_set_word(normalized_keyword):
                continue
            
            result.update({
                'product_name': product_name,
                'seller': seller,
                'cid': cid,
                'pid': '',
                'status': 'product_found'
            })
            break
        
        if not result['product_name']:
            result['last_response'] = "No matching product found"
            result['status'] = "no_match"
            return result
        
        # Click on product if CID exists
        if result['cid']:
            try:
                element = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, f'//div[@id="{result["cid"]}"]'))
                )
                if element:
                    driver.execute_script("arguments[0].scrollIntoView(true);", element)
                    time.sleep(1)
                    element.click()
                    result['last_response'] = "Clicked on product successfully"
                    time.sleep(random.uniform(1, 3))
            except:
                result['last_response'] = "Could not click product element"
        
        # Prefer the stable Google "Share link" URL from the right panel.
        share_url = ""
        try:
            share_button = WebDriverWait(driver, 8).until(
                EC.element_to_be_clickable((
                    By.XPATH,
                    "//div[contains(@class,'RSNrZe') and @role='button' and @aria-label='Share']"
                ))
            )
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", share_button)
            share_button.click()

            share_dialog = WebDriverWait(driver, 8).until(
                EC.visibility_of_element_located((By.XPATH, "//div[@role='dialog' and @aria-label='Share']"))
            )

            try:
                share_input = share_dialog.find_element(By.CSS_SELECTOR, "input[aria-label='Share link'][type='url']")
                share_url = (share_input.get_attribute("value") or "").strip()
            except:
                share_url = ""

            if not share_url:
                try:
                    share_url = share_dialog.find_element(By.CSS_SELECTOR, "div[jsname='tQ9n1c']").text.strip()
                except:
                    share_url = ""

            # Close share dialog so it doesn't block following actions.
            try:
                close_button = share_dialog.find_element(By.CSS_SELECTOR, "[jsname='tqp7ud']")
                close_button.click()
            except:
                try:
                    ActionChains(driver).send_keys(u'\ue00c').perform()  # ESC
                except:
                    pass
        except:
            share_url = ""
        result['product_url'] = share_url or driver.current_url
        
        # Try to get more stores
        i = 0
        while i < 2:
            try:
                more_stores = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//div[contains(@class,'duf-h')]//div[@role='button']"))
                )
                more_stores.click()
                time.sleep(random.uniform(2, 4))
                i += 1
            except:
                break
        
        # Try to find offers grid
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
                result['options'] = product_options
            
            offer_elements = offers_grid.find_elements(By.CLASS_NAME, 'R5K7Cb')
            print(f"Found {len(offer_elements)} offers")
            
            competitors = []
            for seller_html in offer_elements:
                try:
                    store_name = seller_html.find_element(By.CSS_SELECTOR, "div.hP4iBf.gUf0b.uWvFpd").text.strip()
                except:
                    store_name = "N/A"
                
                try:
                    seller_product_name = seller_html.find_element(By.CSS_SELECTOR, "div.Rp8BL").text.strip()
                except:
                    seller_product_name = "N/A"
                
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
                    'seller': store_name,
                    'seller_product_name': seller_product_name,
                    'seller_url': seller_url,
                    'seller_price': seller_price,
                    'last_fetched_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                
                competitors.append(competitor_data)
                result['competitors'].append(competitor_data)
            
            # Calculate OSB position
            search_seller = '1StopBedrooms'
            sellers = [c['seller'] for c in competitors]
            osb_position = 0
            seller_count = len(sellers)
            osb_id = ''
            osb_url_match = False
            
            if search_seller in sellers:
                osb_position = sellers.index(search_seller) + 1
                for competitor in competitors:
                    if competitor['seller'] == search_seller:
                        seller_slug = normalize_url_path_slug(competitor.get('seller_url', ''))
                        osb_id = seller_slug
                        target_slug = normalize_url_path_slug(osb_url)
                        if seller_slug and target_slug:
                            osb_url_match = seller_slug == target_slug
                        break
            
            result.update({
                'osb_position': osb_position,
                'seller_count': seller_count,
                'osb_id': osb_id,
                'status': 'completed',
                'osb_url_match': f'{"Yes" if osb_url_match else "No"}',
                'last_response': f'Completed - OSB Position: {osb_position}, Total Sellers: {seller_count}'
            })
            
        except Exception as e:
            result['status'] = 'no_offers_found'
            result['last_response'] = f'No offers found: {str(e)}'
        
        return result
        
    except Exception as e:
        print(f"Error scraping product {product_id}: {str(e)}")
        traceback.print_exc()
        return {
            'product_id': product_id,
            'keyword': keyword,
            'url': url,
            'last_response': f'Error: {str(e)}',
            'status': 'error',
            'last_fetched_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'competitors': []
        }

def merge_csv_files(file_paths, output_path, sort_columns=None, expected_columns=None):
    """Merge CSV files into one output CSV."""
    valid_files = [p for p in file_paths if p and os.path.exists(p) and os.path.getsize(p) > 0]
    if not valid_files:
        return None, 0

    frames = []
    for path in valid_files:
        try:
            df = pd.read_csv(path)
            if not df.empty:
                frames.append(df)
        except Exception as e:
            print(f"Warning: Could not read {path}: {e}")

    if not frames:
        return None, 0

    merged_df = pd.concat(frames, ignore_index=True)
    if expected_columns:
        for col in expected_columns:
            if col not in merged_df.columns:
                merged_df[col] = ""
        merged_df = merged_df.loc[:, expected_columns]
    if sort_columns:
        available_cols = [c for c in sort_columns if c in merged_df.columns]
        if available_cols:
            merged_df = merged_df.sort_values(available_cols)

    merged_df.to_csv(output_path, index=False)
    return output_path, len(merged_df)


def split_dataframe_to_chunk_files(df, output_dir, total_chunks, prefix):
    """Split DataFrame into up to total_chunks chunk files and return file paths."""
    os.makedirs(output_dir, exist_ok=True)
    total_rows = len(df)
    if total_rows == 0:
        return []

    chunk_count = max(1, min(int(total_chunks), total_rows))
    base_size = total_rows // chunk_count
    remainder = total_rows % chunk_count

    chunk_files = []
    start_idx = 0
    for i in range(chunk_count):
        extra = 1 if i < remainder else 0
        end_idx = start_idx + base_size + extra
        chunk_df = df.iloc[start_idx:end_idx]
        if chunk_df.empty:
            start_idx = end_idx
            continue

        chunk_file = os.path.join(output_dir, f"{prefix}_chunk_{i + 1}.csv")
        chunk_df.to_csv(chunk_file, index=False)
        chunk_files.append(chunk_file)
        print(f"Prepared chunk {i + 1}/{chunk_count}: rows {start_idx + 1}-{end_idx}")
        start_idx = end_idx

    return chunk_files


def process_chunk(chunk_file, chunk_id, total_chunks, round_id=1, output_dir='output'):
    """Process a chunk of products"""
    try:
        # Read chunk file
        df = pd.read_csv(chunk_file)
        if df.empty:
            print(f"Chunk {chunk_id} is empty, skipping")
            return {
                "success": True,
                "product_file": None,
                "seller_file": None,
                "remaining_file": None,
                "product_rows": 0,
                "seller_rows": 0,
                "remaining_rows": 0,
            }

        print(f"Processing {len(df)} products from chunk {chunk_id}")
        
        # Initialize results
        product_results = []
        seller_results = []
        remaining_results = []
        
        # Setup driver
        driver = setup_driver()
        
        # Process each product
        for index, row in df.iterrows():
            product_id = row['product_id']
            web_id = row['web_id']
            keyword = row['keyword']
            url = row['url']
            osb_url = row['osb_url']
            name = row['name']
            mpnsku = row['mpn_sku']
            gtin = row['gtin']
            brand = row['brand']
            cat = row['category']
            
            print(f"\nProcessing {index+1}/{len(df)}: Product ID {product_id}")
            
            # Scrape product
            scraped_data = scrape_product(driver, product_id, keyword, url, osb_url)
            
            # Add original fields back
            scraped_data['web_id'] = web_id
            scraped_data['keyword'] = keyword
            scraped_data['osb_url'] = osb_url
            scraped_data['name'] = name
            scraped_data['mpn_sku'] = mpnsku
            scraped_data['gtin'] = gtin
            scraped_data['brand'] = brand
            scraped_data['category'] = cat
            
            # Add to results
            product_results.append(scraped_data)
            seller_results.extend(scraped_data['competitors'])
            if str(scraped_data.get('status', '')).strip().lower() in {'captcha_failed', 'error'}:
                remaining_row = {
                    col: ('' if pd.isna(row[col]) else row[col])
                    for col in df.columns
                }
                remaining_results.append(remaining_row)
            
            # Sleep between products
            if index < len(df) - 1:
                time.sleep(random.uniform(1,3))
        
        # Close driver
        driver.quit()
        
        # Keep only non-remaining results in round outputs.
        completed_product_results = [
            r for r in product_results
            if str(r.get('status', '')).strip().lower() not in {'captcha_failed', 'error'}
        ]

        # Create CSV 1: Product Information
        csv1_data = []
        for result in completed_product_results:
            csv1_row = {
                'product_id': result.get('product_id', ''),
                'web_id': result.get('web_id', ''),
                'name' : result.get('name',''),
                'mpn_sku' : result.get('mpn_sku',''),
                'gtin' : result.get('gtin',''),
                'brand' : result.get('brand',''),
                'category': result.get('category', ''),
                'keyword': result.get('keyword', ''),
                'url': result.get('url', ''),
                'osb_url': result.get('osb_url', ''),
                'last_response': result.get('last_response', ''),
                'osb_url_match' : result.get('osb_url_match', ''),
                'product_url': result.get('product_url', ''),
                'seller': result.get('seller', ''),
                'product_name': result.get('product_name', ''),
                'cid': result.get('cid', ''),
                'pid': result.get('pid', ''),
                'last_fetched_date': result.get('last_fetched_date', ''),
                'osb_position': result.get('osb_position', 0),
                'osb_id': result.get('osb_id', ''),
                'seller_count': result.get('seller_count', 0),
                'status': result.get('status', 'error')
            }

            csv1_data.append(csv1_row)
        
        # Create CSV 2: Seller Information
        csv2_data = []
        completed_product_ids = {str(r.get('product_id', '')).strip() for r in completed_product_results}
        for seller in seller_results:
            if str(seller.get('product_id', '')).strip() not in completed_product_ids:
                continue
            csv2_row = {
                'product_id': seller.get('product_id', ''),
                'seller': seller.get('seller', ''),
                'seller_product_name': seller.get('seller_product_name', ''),
                'seller_url': seller.get('seller_url', ''),
                'seller_price': seller.get('seller_price', ''),
                'last_fetched_date': seller.get('last_fetched_date', '')
            }
            csv2_data.append(csv2_row)
        
        # Save CSV files locally
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        os.makedirs(output_dir, exist_ok=True)
        
        csv1_filename = f"product_info_round{round_id}_chunk{chunk_id}_{timestamp}.csv"
        csv2_filename = f"seller_info_round{round_id}_chunk{chunk_id}_{timestamp}.csv"
        csv3_filename = f"gshopping_remaining_round{round_id}_chunk{chunk_id}_{timestamp}.csv"
        
        csv1_path = os.path.join(output_dir, csv1_filename)
        csv2_path = os.path.join(output_dir, csv2_filename)
        csv3_path = os.path.join(output_dir, csv3_filename)
        
        if csv1_data:
            pd.DataFrame(csv1_data, columns=PRODUCT_FINAL_COLUMNS).to_csv(csv1_path, index=False)
            print(f"✓ Saved product info: {csv1_filename}")
        
        if csv2_data:
            pd.DataFrame(csv2_data).to_csv(csv2_path, index=False)
            print(f"✓ Saved seller info: {csv2_filename}")

        if remaining_results:
            pd.DataFrame(remaining_results).to_csv(csv3_path, index=False)
            print(f"✓ Saved remaining rows: {csv3_filename}")
        
        # Upload to FTP STOPPED TO AVOID UNNECESSARY FTP USAGE DURING TESTING
        # if csv1_data:
        #     upload_to_ftp(ftp_host, ftp_user, ftp_pass, ftp_path, csv1_path, csv1_filename)
        
        # if csv2_data:
        #     upload_to_ftp(ftp_host, ftp_user, ftp_pass, ftp_path, csv2_path, csv2_filename)
        
        print(f"\n✓ Chunk {chunk_id} processing completed")
        return {
            "success": True,
            "product_file": csv1_path if csv1_data else None,
            "seller_file": csv2_path if csv2_data else None,
            "remaining_file": csv3_path if remaining_results else None,
            "product_rows": len(csv1_data),
            "seller_rows": len(csv2_data),
            "remaining_rows": len(remaining_results),
        }
        
    except Exception as e:
        print(f"Error processing chunk {chunk_id}: {str(e)}")
        traceback.print_exc()
        return {
            "success": False,
            "product_file": None,
            "seller_file": None,
            "remaining_file": None,
            "product_rows": 0,
            "seller_rows": 0,
            "remaining_rows": 0,
        }


def run_recursive_pipeline(input_csv, total_chunks, ftp_host, ftp_user, ftp_pass, ftp_path, max_rounds=10):
    """Process chunks recursively until no remaining rows are left."""
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_root = os.path.join("output", f"recursive_run_{run_ts}")
    rounds_root = os.path.join(run_root, "rounds")
    os.makedirs(rounds_root, exist_ok=True)

    all_product_files = []
    all_seller_files = []
    current_input = input_csv
    round_id = 1

    while round_id <= max_rounds:
        print(f"\n{'=' * 60}")
        print(f"Starting round {round_id}")
        print(f"Input file: {current_input}")
        print(f"{'=' * 60}")

        try:
            current_df = pd.read_csv(current_input)
        except Exception as e:
            print(f"Error reading round input file {current_input}: {e}")
            return False

        if current_df.empty:
            print("No rows left to process. Ending recursion.")
            break

        round_dir = os.path.join(rounds_root, f"round_{round_id}")
        os.makedirs(round_dir, exist_ok=True)

        chunk_files = split_dataframe_to_chunk_files(
            current_df,
            output_dir=round_dir,
            total_chunks=max(1, int(total_chunks)),
            prefix=f"round_{round_id}",
        )
        if not chunk_files:
            print("No chunks generated for current round. Ending recursion.")
            break

        round_product_files = []
        round_seller_files = []
        round_remaining_files = []
        any_chunk_failed = False

        for idx, chunk_file in enumerate(chunk_files, start=1):
            chunk_result = process_chunk(
                chunk_file=chunk_file,
                chunk_id=idx,
                total_chunks=len(chunk_files),
                round_id=round_id,
                output_dir=round_dir,
            )
            if not chunk_result.get("success"):
                any_chunk_failed = True
                continue

            if chunk_result.get("product_file"):
                round_product_files.append(chunk_result["product_file"])
                all_product_files.append(chunk_result["product_file"])
            if chunk_result.get("seller_file"):
                round_seller_files.append(chunk_result["seller_file"])
                all_seller_files.append(chunk_result["seller_file"])
            if chunk_result.get("remaining_file"):
                round_remaining_files.append(chunk_result["remaining_file"])

        if any_chunk_failed:
            print("One or more chunks failed in this round.")

        round_product_merged, round_product_rows = merge_csv_files(
            round_product_files,
            os.path.join(round_dir, f"merged_products_round{round_id}.csv"),
            sort_columns=["product_id"],
            expected_columns=PRODUCT_FINAL_COLUMNS,
        )
        round_seller_merged, round_seller_rows = merge_csv_files(
            round_seller_files,
            os.path.join(round_dir, f"merged_sellers_round{round_id}.csv"),
            sort_columns=["product_id", "seller"],
        )
        round_remaining_merged, round_remaining_rows = merge_csv_files(
            round_remaining_files,
            os.path.join(round_dir, f"gshopping_remaining_round{round_id}.csv"),
            sort_columns=["product_id"],
            expected_columns=PRODUCT_FINAL_COLUMNS,
        )

        # Upload round-level merged files only after the full round has finished.
        if round_product_merged:
            upload_to_ftp(
                ftp_host, ftp_user, ftp_pass, ftp_path,
                round_product_merged, os.path.basename(round_product_merged)
            )
        if round_seller_merged:
            upload_to_ftp(
                ftp_host, ftp_user, ftp_pass, ftp_path,
                round_seller_merged, os.path.basename(round_seller_merged)
            )
        if round_remaining_merged:
            upload_to_ftp(
                ftp_host, ftp_user, ftp_pass, ftp_path,
                round_remaining_merged, os.path.basename(round_remaining_merged)
            )

        print(
            f"Round {round_id} summary: products={round_product_rows}, "
            f"sellers={round_seller_rows}, remaining={round_remaining_rows}"
        )

        if not round_remaining_merged or round_remaining_rows == 0:
            print("No remaining rows after this round. Recursive processing is complete.")
            break

        current_input = round_remaining_merged
        round_id += 1

    if round_id > max_rounds:
        print(f"Reached max rounds limit ({max_rounds}). Stopping recursion.")

    final_products_file, final_product_rows = merge_csv_files(
        all_product_files,
        os.path.join(run_root, f"merged_products_final_{run_ts}.csv"),
        sort_columns=["product_id"],
        expected_columns=PRODUCT_FINAL_COLUMNS,
    )
    final_sellers_file, final_seller_rows = merge_csv_files(
        all_seller_files,
        os.path.join(run_root, f"merged_sellers_final_{run_ts}.csv"),
        sort_columns=["product_id", "seller"],
    )

    if final_products_file:
        upload_to_ftp(
            ftp_host, ftp_user, ftp_pass, ftp_path,
            final_products_file, os.path.basename(final_products_file)
        )
    if final_sellers_file:
        upload_to_ftp(
            ftp_host, ftp_user, ftp_pass, ftp_path,
            final_sellers_file, os.path.basename(final_sellers_file)
        )

    print("\nFinal merge summary:")
    print(f"Final products: {final_product_rows} rows")
    print(f"Final sellers:  {final_seller_rows} rows")
    print(f"Output root:    {run_root}")

    return bool(final_products_file or final_sellers_file)

def main():
    parser = argparse.ArgumentParser(description='Google Shopping Scraper with Captcha Solving')
    parser.add_argument('--chunk-id', type=int, default=1, help='Chunk ID (1-based)')
    parser.add_argument('--total-chunks', type=int, required=True, help='Total number of chunks')
    parser.add_argument('--input-file', type=str, required=True, help='Input CSV filename on FTP')
    parser.add_argument('--recursive', action='store_true', help='Run recursive chunk processing until remaining is empty')
    parser.add_argument('--max-rounds', type=int, default=10, help='Maximum recursive rounds')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("Google Shopping Scraper with Captcha Solving")
    print(f"Chunk: {args.chunk_id} of {args.total_chunks}")
    print(f"Input file: {args.input_file}")
    print(f"Recursive mode: {'Yes' if args.recursive else 'No'}")
    print("=" * 60)
    

    ftp_host = os.getenv('FTP_HOST')
    ftp_user = os.getenv('FTP_USER')
    ftp_pass = os.getenv('FTP_PASS')
    ftp_path = os.getenv('FTP_PATH', '/scrap/')
    
    if not all([ftp_host, ftp_user, ftp_pass]):
        print("Error: FTP credentials not found in environment variables")
        print("Please set FTP_HOST, FTP_USER, FTP_PASS environment variables")
        sys.exit(1)
    
    # Download input CSV from FTP
    input_csv = 'input.csv'
    if not download_csv_from_ftp(ftp_host, ftp_user, ftp_pass, ftp_path, args.input_file, input_csv):
        print("Failed to download input CSV")
        sys.exit(1)
    
    if args.recursive:
        success = run_recursive_pipeline(
            input_csv=input_csv,
            total_chunks=args.total_chunks,
            ftp_host=ftp_host,
            ftp_user=ftp_user,
            ftp_pass=ftp_pass,
            ftp_path=ftp_path,
            max_rounds=max(1, args.max_rounds),
        )
    else:
        chunk_file = split_csv(input_csv, 'chunks', args.chunk_id, args.total_chunks)
        if not chunk_file:
            print("Failed to split CSV")
            sys.exit(1)
        
        chunk_result = process_chunk(chunk_file, args.chunk_id, args.total_chunks)
        success = chunk_result.get("success", False)
        
        try:
            os.remove(chunk_file)
            shutil.rmtree('chunks', ignore_errors=True)
        except:
            pass

    try:
        os.remove(input_csv)
    except:
        pass
    
    if success:
        print("\n✓ Processing completed successfully")
        sys.exit(0)
    else:
        print("\n✗ Processing failed")
        sys.exit(1)

if __name__ == "__main__":
    main()
