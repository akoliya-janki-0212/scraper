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
import urllib.request
import pydub
from selenium.webdriver.common.keys import Keys

recaptcha_words = [
    "apple tree", "blue sky", "silver coin", "happy child", "gold star",
    "fast car", "river bank", "mountain peak", "red house", "sun flower",
    "deep ocean", "bright moon", "green grass", "snow fall", "strong wind",
    "dark night", "big city", "tall building", "small village", "soft pillow",
    "quiet room", "loud noise", "warm fire", "cold water", "heavy rain",
    "hot coffee", "empty street", "open door", "closed window", "white cloud",
    "yellow light", "long road", "short path", "new book", "old paper",
    "broken clock", "silent night", "early morning", "late evening", "clear sky",
    "dusty road", "sharp knife", "dull pencil", "lost key", "found wallet",
    "strong bridge", "weak signal", "fast train", "slow boat", "hidden message",
    "bright future", "dark past", "deep forest", "shallow lake", "frozen river",
    "burning candle", "flying bird", "running horse", "jumping fish", "falling leaf",
    "climbing tree", "rolling stone", "melting ice", "whispering wind", "shining star",
    "crying baby", "laughing child", "singing voice", "barking dog", "meowing cat",
    "chirping bird", "roaring lion", "galloping horse", "buzzing bee", "silent whisper",
    "drifting boat", "rushing water", "ticking clock", "clicking sound", "typing keyboard",
    "ringing bell", "blinking light", "floating balloon", "spinning wheel", "crashing waves",
    "boiling water", "freezing air", "burning wood", "echoing voice", "howling wind",
    "glowing candle", "rustling leaves", "dancing flame", "rattling chains", "splashing water",
    "twisting road", "swinging door", "glistening snow", "pouring rain", "shaking ground"
]
def voicereco(AUDIO_FILE):
    import speech_recognition as sr

    # Initialize recognizer
    recognizer = sr.Recognizer()

    # ✅ Use a raw string or double backslashes to avoid path issues

    # Load the audio file
    with sr.AudioFile(AUDIO_FILE) as source:
        print("🔄 Processing audio file...")
        recognizer.adjust_for_ambient_noise(source)
        audio = recognizer.record(source)  # Read the entire audio file

        try:
            text = recognizer.recognize_google(audio)
            print("📝 Extracted Text:", text)
            return text
        except sr.UnknownValueError:
            random_text = random.choice(recaptcha_words)
            print("❌ Could not understand the audio.")
            return random_text
        except sr.RequestError:
            print("❌ Could not request results, check your internet.")
            return None
def solve_recaptcha_audio(driver):
    """
    Solves the Google reCAPTCHA v2 audio challenge.

    Arguments:
    - driver: Selenium WebDriver instance
    """

    time.sleep(2)  # Initial wait

    # ✅ Locate reCAPTCHA Iframes
    frames = driver.find_elements(By.TAG_NAME, "iframe")
    recaptcha_control_frame = None
    recaptcha_challenge_frame = None

    for frame in frames:
        title = frame.get_attribute("title")
        if "reCAPTCHA" in title:
            recaptcha_control_frame = frame
        if "challenge" in title:
            recaptcha_challenge_frame = frame
    time.sleep(random.uniform(2, 4))
    # ✅ Click the reCAPTCHA Checkbox
    if recaptcha_control_frame:
        driver.switch_to.frame(recaptcha_control_frame)
        driver.find_element(By.CLASS_NAME, "recaptcha-checkbox-border").click()
        print("✅ Clicked reCAPTCHA checkbox")
        time.sleep(random.uniform(2, 4))  # Random wait to mimic human behavior
        driver.switch_to.default_content()

    time.sleep(2)

    # ✅ Switch to Challenge Frame
    if recaptcha_challenge_frame:
        try:
            driver.switch_to.frame(recaptcha_challenge_frame)
            time.sleep(2)
        except:
            return "solved"

        # ✅ Click the "Get an audio challenge" button
        try:
            audio_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, "//button[@title='Get an audio challenge']"))
            )
            audio_button.click()
            print("✅ Clicked 'Get an audio challenge' button")
            time.sleep(2)
        except:
            print("❌ Could not find the audio challenge button.")
            driver.switch_to.default_content()
            return "quit"

        # ✅ Get the audio file URL
        def get_audio(driver):
            time.sleep(3)  # Increased wait time

            # Try multiple times to find audio source
            max_attempts = 3
            for attempt in range(max_attempts):
                try:
                    print(f"Attempt {attempt + 1} to find audio source...")

                    # Check if audio source is present
                    audio_source = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.ID, "audio-source"))
                    )
                    src = audio_source.get_attribute("src")

                    if src and src.startswith("http"):
                        print(f"✅ Audio source found: {src[:50]}...")
                        break
                    else:
                        print("Audio source found but URL is invalid")
                        time.sleep(2)
                except:
                    if attempt < max_attempts - 1:
                        print("Audio source not found, retrying...")
                        time.sleep(2)
                    else:
                        print("❌ Could not find the audio source after multiple attempts.")
                        return "no audio"

            # Rest of your audio processing code remains the same...
            # ✅ Define file paths
            mp3_path = os.path.join(os.getcwd(), "captcha_audio.mp3")
            wav_path = os.path.join(os.getcwd(), "captcha_audio.wav")

            try:
                # ✅ Download the audio file
                urllib.request.urlretrieve(src, mp3_path)
                print("✅ Audio file downloaded.")

                # ✅ Convert MP3 to WAV
                sound = pydub.AudioSegment.from_mp3(mp3_path)
                sound.export(wav_path, format="wav")
                print("✅ Audio file converted to WAV.")
            except Exception as e:
                print(f"❌ Audio processing error: {e}")
                return "quit"

            # ✅ Call voicereco() to process the audio file
            captcha_text = voicereco(wav_path)

            if captcha_text:
                print(f"📝 Recognized Text: {captcha_text}")
                time.sleep(random.uniform(1, 3))

                # ✅ Enter the recognized text into the response box
                response_box = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.ID, "audio-response"))
                )
                response_box.send_keys(captcha_text.lower())
                time.sleep(1)
                response_box.send_keys(Keys.ENTER)
                time.sleep(random.uniform(2, 3))

                # Check if verification failed
                try:
                    error_msg = driver.find_element(By.XPATH, "//div[contains(text(), 'Try again')]")
                    if error_msg:
                        print("⚠️ Verification failed, trying again...")
                        return "retry"
                except:
                    pass

                return "success"
            else:
                print("❌ Could not extract text from the audio.")
                return "quit"
        def submit(driver):
            # ✅ Click the Submit Button
            driver.switch_to.default_content()
            driver.find_element(By.ID, "recaptcha-demo-submit").click()
            print("🎉 CAPTCHA Solved!")
            return "solved"

        count=1
        while True:
            result=get_audio(driver)
            if (result=="no audio" and count==1) or result=="quit":
                return "quit"
            elif result=="no audio" and count>1:
                # try:
                #     submit(driver)
                # except:
                #     return "solved"
                return "solved"
            count+=1
    else :
        return "solved"

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

    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    ]
    options.add_argument(f"user-agent={random.choice(user_agents)}")

    driver = uc.Chrome(options=options)
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

def handle_captcha(driver, url):
    """Handle captcha if detected"""
    recaptcha = detects_recaptcha(driver)
    if recaptcha:
        print("Attempting to solve captcha...")
        result = solve_recaptcha_audio(driver)
        if result == "solved":
            print("Captcha solved successfully!")
            driver.switch_to.default_content()
            return "solved"
        else:
            print(f"Captcha solving failed: {result}")
            return "failed"
    return "no_captcha"

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
        ftp.connect(ftp_host, 21)
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

def scrape_product(driver, product_id, keyword, url):
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
                'competitors': []
            }
        
        time.sleep(random.uniform(5, 10))
        
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
            result['last_response'] = f"Product container not found: {str(e)}"
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
            
            # Check for Set keyword mismatch
            if (not "Set" in product_name and "Set" in keyword) or ("Set" in product_name and not "Set" in keyword):
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
        
        result['product_url'] = driver.current_url
        
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
            
            if search_seller in sellers:
                osb_position = sellers.index(search_seller) + 1
                for competitor in competitors:
                    if competitor['seller'] == search_seller:
                        osb_id = competitor.get('seller_url', '').split('/')[-1] if competitor.get('seller_url') else ''
                        break
            
            result.update({
                'osb_position': osb_position,
                'seller_count': seller_count,
                'osb_id': osb_id,
                'status': 'completed',
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

def process_chunk(chunk_file, chunk_id, total_chunks):
    """Process a chunk of products"""
    try:
        # Get FTP credentials from environment
        ftp_host = os.getenv('FTP_HOST')
        ftp_user = os.getenv('FTP_USER')
        ftp_pass = os.getenv('FTP_PASS')
        ftp_path = os.getenv('FTP_PATH', '/scrap/')
        
        if not all([ftp_host, ftp_user, ftp_pass]):
            print("Error: FTP credentials not found")
            return False
        
        # Read chunk file
        df = pd.read_csv(chunk_file)
        print(f"Processing {len(df)} products from chunk {chunk_id}")
        
        # Initialize results
        product_results = []
        seller_results = []
        
        # Setup driver
        driver = setup_driver()
        
        # Process each product
        for index, row in df.iterrows():
            product_id = row['product_id']
            web_id = row['web_id']
            keyword = row['keyword']
            url = row['url']
            osb_url = row['osb_url']
            
            print(f"\nProcessing {index+1}/{len(df)}: Product ID {product_id}")
            
            # Scrape product
            scraped_data = scrape_product(driver, product_id, keyword, url)
            
            # Add original fields back
            scraped_data['web_id'] = web_id
            scraped_data['osb_url'] = osb_url
            
            # Add to results
            product_results.append(scraped_data)
            seller_results.extend(scraped_data['competitors'])
            
            # Sleep between products
            if index < len(df) - 1:
                time.sleep(random.uniform(3, 6))
        
        # Close driver
        driver.quit()
        
        # Create CSV 1: Product Information
        csv1_data = []
        for result in product_results:
            csv1_row = {
                'product_id': result.get('product_id'),
                'web_id': result.get('web_id'),
                'keyword': result.get('keyword'),
                'url': result.get('url'),
                'osb_url': result.get('osb_url'),
                'last_response': result.get('last_response'),
                'product_url': result.get('product_url', ''),   # ✅ FIX
                'seller': result.get('seller', ''),
                'product_name': result.get('product_name', ''),
                'cid': result.get('cid', ''),
                'pid': result.get('pid', ''),
                'last_fetched_date': result.get('last_fetched_date'),
                'osb_position': result.get('osb_position', 0),
                'osb_id': result.get('osb_id', ''),
                'seller_count': result.get('seller_count', 0),
                'status': result.get('status')
            }
            csv1_data.append(csv1_row)
        
        # Create CSV 2: Seller Information
        csv2_data = []
        for seller in seller_results:
            csv2_row = {
                'product_id': seller.get('product_id'),
                'seller': seller.get('seller',''),
                'seller_product_name': seller.get('seller_product_name',''),
                'seller_url': seller.get('seller_url',''),
                'seller_price': seller.get('seller_price',0),
                'last_fetched_date': seller.get('last_fetched_date','')
            }
            csv2_data.append(csv2_row)
        
        # Save CSV files locally
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = 'output'
        os.makedirs(output_dir, exist_ok=True)
        
        csv1_filename = f"product_info_chunk{chunk_id}_{timestamp}.csv"
        csv2_filename = f"seller_info_chunk{chunk_id}_{timestamp}.csv"
        
        csv1_path = os.path.join(output_dir, csv1_filename)
        csv2_path = os.path.join(output_dir, csv2_filename)
        
        if csv1_data:
            pd.DataFrame(csv1_data).to_csv(csv1_path, index=False)
            print(f"✓ Saved product info: {csv1_filename}")
        
        if csv2_data:
            pd.DataFrame(csv2_data).to_csv(csv2_path, index=False)
            print(f"✓ Saved seller info: {csv2_filename}")
        
        # Upload to FTP
        if csv1_data:
            upload_to_ftp(ftp_host, ftp_user, ftp_pass, ftp_path, csv1_path, csv1_filename)
        
        if csv2_data:
            upload_to_ftp(ftp_host, ftp_user, ftp_pass, ftp_path, csv2_path, csv2_filename)
        
        print(f"\n✓ Chunk {chunk_id} processing completed")
        return True
        
    except Exception as e:
        print(f"Error processing chunk {chunk_id}: {str(e)}")
        traceback.print_exc()
        return False

def main():
    parser = argparse.ArgumentParser(description='Google Shopping Scraper with Captcha Solving')
    parser.add_argument('--chunk-id', type=int, required=True, help='Chunk ID (1-based)')
    parser.add_argument('--total-chunks', type=int, required=True, help='Total number of chunks')
    parser.add_argument('--input-file', type=str, required=True, help='Input CSV filename on FTP')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("Google Shopping Scraper with Captcha Solving")
    print(f"Chunk: {args.chunk_id} of {args.total_chunks}")
    print(f"Input file: {args.input_file}")
    print("=" * 60)
    
    # Get FTP credentials
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
    
    # Split CSV and get our chunk
    chunk_file = split_csv(input_csv, 'chunks', args.chunk_id, args.total_chunks)
    if not chunk_file:
        print("Failed to split CSV")
        sys.exit(1)
    
    # Process the chunk
    success = process_chunk(chunk_file, args.chunk_id, args.total_chunks)
    
    # Clean up
    try:
        os.remove(input_csv)
        os.remove(chunk_file)
        import shutil
        shutil.rmtree('chunks', ignore_errors=True)
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