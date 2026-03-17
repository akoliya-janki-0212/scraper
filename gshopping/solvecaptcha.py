import time
import os
import urllib.request
import random
import pydub
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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

    recognizer = sr.Recognizer()
    
    try:
        with sr.AudioFile(AUDIO_FILE) as source:
            logger.info("🔄 Processing audio file...")
            recognizer.adjust_for_ambient_noise(source, duration=0.5)
            audio = recognizer.record(source)

            try:
                text = recognizer.recognize_google(audio)
                logger.info(f"📝 Extracted Text: {text}")
                return text
            except sr.UnknownValueError:
                random_text = random.choice(recaptcha_words)
                logger.warning(f"❌ Could not understand audio, using fallback: {random_text}")
                return random_text
            except sr.RequestError as e:
                logger.error(f"❌ Speech recognition request error: {e}")
                random_text = random.choice(recaptcha_words)
                return random_text
    except Exception as e:
        logger.error(f"❌ Error processing audio file: {e}")
        random_text = random.choice(recaptcha_words)
        return random_text

def download_audio_file(src, mp3_path, wav_path):
    """Download and convert audio file with retries"""
    max_retries = 2
    for attempt in range(max_retries):
        try:
            logger.info(f"Downloading audio (attempt {attempt + 1}/{max_retries})...")
            
            # Add headers to mimic browser request
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'audio/webm,audio/ogg,audio/wav,audio/*;q=0.9,application/ogg;q=0.7,video/*;q=0.6,*/*;q=0.5',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br',
                'Range': 'bytes=0-',
                'Connection': 'keep-alive',
                'Referer': 'https://www.google.com/',
                'Sec-Fetch-Dest': 'audio',
                'Sec-Fetch-Mode': 'no-cors',
                'Sec-Fetch-Site': 'same-origin',
            }
            
            req = urllib.request.Request(src, headers=headers)
            
            with urllib.request.urlopen(req) as response:
                with open(mp3_path, 'wb') as f:
                    f.write(response.read())
            
            logger.info("✅ Audio file downloaded.")
            
            # Check file size
            file_size = os.path.getsize(mp3_path)
            logger.info(f"Audio file size: {file_size} bytes")
            
            if file_size < 1000:  # Too small, probably not an audio file
                logger.error(f"File too small ({file_size} bytes), probably not audio")
                return False
            
            # Convert MP3 to WAV
            try:
                sound = pydub.AudioSegment.from_mp3(mp3_path)
                sound.export(wav_path, format="wav")
                logger.info("✅ Audio file converted to WAV.")
                return True
            except Exception as e:
                logger.error(f"❌ Audio conversion error: {e}")
                # Try alternative format
                try:
                    sound = pydub.AudioSegment.from_file(mp3_path)
                    sound.export(wav_path, format="wav")
                    logger.info("✅ Audio file converted to WAV (alternative method).")
                    return True
                except Exception as e2:
                    logger.error(f"❌ Alternative conversion also failed: {e2}")
                    return False
                    
        except Exception as e:
            logger.error(f"❌ Audio download error (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
            else:
                return False

def get_audio_source(driver):
    """Get the actual audio source URL from reCAPTCHA with multiple approaches"""
    try:
        # Wait for audio to load
        time.sleep(3)
        
        logger.info("Looking for audio source using multiple methods...")
        
        # METHOD 1: Look for audio element directly
        audio_elements = driver.find_elements(By.TAG_NAME, "audio")
        logger.info(f"Found {len(audio_elements)} audio elements")
        
        for i, audio in enumerate(audio_elements):
            try:
                src = audio.get_attribute("src") or ""
                id_attr = audio.get_attribute("id") or ""
                
                if src and not src.endswith('.js'):
                    logger.info(f"✅ Found audio element {i}: id='{id_attr}', src='{src[:80]}...'")
                    return src
            except:
                continue
        
        # METHOD 2: Look for iframe within iframe (nested structure)
        logger.info("Checking for nested iframes...")
        nested_frames = driver.find_elements(By.TAG_NAME, "iframe")
        
        for frame_idx, frame in enumerate(nested_frames):
            try:
                driver.switch_to.frame(frame)
                logger.info(f"Switched to nested frame {frame_idx}")
                
                # Look for audio in nested frame
                nested_audio = driver.find_elements(By.TAG_NAME, "audio")
                for audio in nested_audio:
                    src = audio.get_attribute("src") or ""
                    if src:
                        logger.info(f"✅ Found audio in nested frame: {src[:80]}...")
                        driver.switch_to.parent_frame()  # Go back one level
                        return src
                
                driver.switch_to.parent_frame()  # Go back to challenge frame
            except Exception as e:
                logger.error(f"Error checking nested frame {frame_idx}: {e}")
                driver.switch_to.default_content()
                driver.switch_to.frame(driver.find_element(By.TAG_NAME, "iframe"))
        
        # METHOD 3: Use JavaScript to find all audio sources
        logger.info("Using JavaScript to find audio sources...")
        audio_sources = driver.execute_script("""
            // Find all audio elements in the entire document
            var audios = document.getElementsByTagName('audio');
            var sources = [];
            
            for (var i = 0; i < audios.length; i++) {
                var src = audios[i].src;
                if (src && src.trim() !== '') {
                    sources.push({
                        src: src,
                        id: audios[i].id,
                        hidden: audios[i].style.display === 'none'
                    });
                }
            }
            
            // Also check for source tags within audio elements
            var sourceTags = document.querySelectorAll('audio source');
            for (var j = 0; j < sourceTags.length; j++) {
                var src = sourceTags[j].src;
                if (src && src.trim() !== '') {
                    sources.push({
                        src: src,
                        id: 'source-tag-' + j,
                        hidden: false
                    });
                }
            }
            
            return sources;
        """)
        
        if audio_sources:
            logger.info(f"JavaScript found {len(audio_sources)} audio sources")
            for source in audio_sources:
                logger.info(f"JS Source: {source['src'][:100]}...")
                if source['src'] and not source['src'].endswith('.js'):
                    return source['src']
        
        # METHOD 4: Try to extract from page source
        logger.info("Checking page source for audio URLs...")
        page_source = driver.page_source
        
        # Look for common audio URL patterns
        import re
        patterns = [
            r'https://www\.google\.com/recaptcha/api2/[^"\']*\.mp3[^"\']*',
            r'https://www\.google\.com/recaptcha/api2/[^"\']*audio[^"\']*',
            r'https://[^"\']*recaptcha[^"\']*audio[^"\']*',
            r'src=["\'][^"\']*\.mp3[^"\']*["\']',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, page_source, re.IGNORECASE)
            if matches:
                for match in matches:
                    if '.mp3' in match.lower() and 'recaptcha' in match.lower():
                        # Clean up the URL
                        url = match
                        if url.startswith('src='):
                            url = url[5:-1]  # Remove src=" and "
                        logger.info(f"✅ Found audio URL in source: {url[:100]}...")
                        return url
        
        logger.error("❌ No valid audio source found after all attempts")
        return None
        
    except Exception as e:
        logger.error(f"❌ Error finding audio source: {e}")
        return None

def solve_recaptcha_audio(driver):
    """
    Main function to solve reCAPTCHA with improved iframe handling
    """
    try:
        logger.info("Attempting to solve captcha...")
        time.sleep(2)
        
        # First, ensure we're on the main content
        driver.switch_to.default_content()
        
        # Find all iframes
        frames = driver.find_elements(By.TAG_NAME, "iframe")
        logger.info(f"Found {len(frames)} iframes on page")
        
        # Look for recaptcha frame by title, src, or name
        recaptcha_frame = None
        recaptcha_frame_index = -1
        
        for i, frame in enumerate(frames):
            try:
                src = frame.get_attribute("src") or ""
                title = frame.get_attribute("title") or ""
                name = frame.get_attribute("name") or ""
                
                logger.info(f"Frame {i}: src={src[:50]}..., title={title}, name={name}")
                
                if any(x in (src + title + name).lower() for x in ["recaptcha", "captcha"]):
                    recaptcha_frame = frame
                    recaptcha_frame_index = i
                    logger.info(f"✅ Found recaptcha frame at index {i}")
                    break
            except:
                continue
        
        if not recaptcha_frame:
            logger.info("No recaptcha frame found, might already be solved")
            return "solved"
        
        # Switch to recaptcha frame
        try:
            driver.switch_to.frame(recaptcha_frame)
            logger.info(f"Switched to recaptcha frame {recaptcha_frame_index}")
            time.sleep(2)
        except Exception as e:
            logger.error(f"Failed to switch to recaptcha frame: {e}")
            driver.switch_to.default_content()
            return "quit"
        
        # Click checkbox using JavaScript for reliability
        try:
            # Try multiple selectors
            selectors = [
                ".recaptcha-checkbox-border",
                ".recaptcha-checkbox",
                "#recaptcha-anchor",
                "div.recaptcha-checkbox-border"
            ]
            
            checkbox = None
            for selector in selectors:
                try:
                    checkbox = driver.find_element(By.CSS_SELECTOR, selector)
                    break
                except:
                    continue
            
            if not checkbox:
                # Try XPath
                try:
                    checkbox = driver.find_element(By.XPATH, "//div[@role='checkbox']")
                except:
                    pass
            
            if checkbox:
                driver.execute_script("arguments[0].click();", checkbox)
                logger.info("✅ Clicked reCAPTCHA checkbox")
                time.sleep(4)  # Give time for challenge to load
            else:
                logger.error("❌ Could not find checkbox element")
                driver.switch_to.default_content()
                return "quit"
                
        except Exception as e:
            logger.error(f"❌ Error clicking checkbox: {e}")
            driver.switch_to.default_content()
            return "quit"
        
        # Switch back to default and look for challenge
        driver.switch_to.default_content()
        time.sleep(3)
        
        # Look for challenge iframe - it might be a new one
        challenge_frame = None
        frames = driver.find_elements(By.TAG_NAME, "iframe")
        
        for i, frame in enumerate(frames):
            try:
                src = frame.get_attribute("src") or ""
                title = frame.get_attribute("title") or ""
                name = frame.get_attribute("name") or ""
                
                # Look for challenge indicators
                if any(x in (src + title + name).lower() for x in ["challenge", "bframe", "recaptcha/api2/bframe"]):
                    challenge_frame = frame
                    logger.info(f"✅ Found challenge frame at index {i}: {src[:50]}...")
                    break
            except:
                continue
        
        if not challenge_frame:
            logger.info("No challenge frame found, CAPTCHA might be solved")
            return "solved"
        
        # Switch to challenge frame
        try:
            driver.switch_to.frame(challenge_frame)
            logger.info("Switched to challenge frame")
            time.sleep(3)
        except Exception as e:
            logger.error(f"Failed to switch to challenge frame: {e}")
            driver.switch_to.default_content()
            return "quit"
        
        # Click audio challenge button
        try:
            # Wait for page to load
            time.sleep(2)
            
            # Try multiple ways to find audio button
            audio_button = None
            
            # Method 1: By ID
            try:
                audio_button = driver.find_element(By.ID, "recaptcha-audio-button")
            except:
                pass
            
            # Method 2: By title
            if not audio_button:
                try:
                    audio_button = driver.find_element(By.XPATH, "//button[contains(@title, 'audio') or contains(@title, 'Audio')]")
                except:
                    pass
            
            # Method 3: By class
            if not audio_button:
                try:
                    audio_button = driver.find_element(By.CLASS_NAME, "rc-button-audio")
                except:
                    pass
            
            # Method 4: By text content
            if not audio_button:
                try:
                    audio_button = driver.find_element(By.XPATH, "//button[contains(., 'audio') or contains(., 'Audio')]")
                except:
                    pass
            
            if audio_button:
                driver.execute_script("arguments[0].click();", audio_button)
                logger.info("✅ Clicked audio challenge button")
                time.sleep(5)  # Wait longer for audio to load
            else:
                logger.error("❌ Could not find audio button")
                driver.switch_to.default_content()
                return "quit"
                
        except Exception as e:
            logger.error(f"❌ Error clicking audio button: {e}")
            driver.switch_to.default_content()
            return "quit"
        
        # Get audio source URL with retry
        audio_src = None
        for attempt in range(1):
            logger.info(f"Attempt {attempt + 1}/3 to get audio source...")
            audio_src = get_audio_source(driver)
            if audio_src:
                break
            time.sleep(2)
        
        if not audio_src:
            logger.error("❌ Could not get audio source URL after multiple attempts")
            
            # Try to reload audio
            try:
                reload_button = driver.find_element(By.ID, "recaptcha-reload-button")
                driver.execute_script("arguments[0].click();", reload_button)
                logger.info("Clicked reload button, waiting for new audio...")
                time.sleep(4)
                audio_src = get_audio_source(driver)
            except:
                pass
            
            if not audio_src:
                driver.switch_to.default_content()
                return "quit"
        
        # Validate it's actually an audio URL
        if audio_src and (audio_src.endswith('.js') or 'recaptcha__en.js' in audio_src):
            logger.error(f"❌ Got JavaScript file instead of audio: {audio_src[:100]}...")
            driver.switch_to.default_content()
            return "quit"
        
        # Download and process audio
        timestamp = int(time.time())
        mp3_path = os.path.join(os.getcwd(), f"captcha_audio_{timestamp}.mp3")
        wav_path = os.path.join(os.getcwd(), f"captcha_audio_{timestamp}.wav")
        
        logger.info(f"Downloading audio from: {audio_src[:100]}...")
        if not download_audio_file(audio_src, mp3_path, wav_path):
            logger.error("❌ Failed to download audio file")
            driver.switch_to.default_content()
            return "quit"
        
        # Recognize text from audio
        captcha_text = voicereco(wav_path)
        if not captcha_text:
            logger.error("❌ Failed to recognize audio")
            driver.switch_to.default_content()
            return "quit"
        
        # Enter the response
        try:
            # Find response input box
            response_box = None
            response_selectors = [
                "#audio-response",
                "input[type='text']",
                "input[name='audio-response']",
                "input.audio-response"
            ]
            
            for selector in response_selectors:
                try:
                    response_box = driver.find_element(By.CSS_SELECTOR, selector)
                    break
                except:
                    continue
            
            if not response_box:
                # Try by placeholder
                try:
                    response_box = driver.find_element(By.XPATH, "//input[@placeholder]")
                except:
                    pass
            
            if response_box:
                # Clear and enter text
                response_box.clear()
                time.sleep(0.5)
                
                # Type character by character
                captcha_text = captcha_text.lower().strip()
                logger.info(f"Entering response: {captcha_text}")
                
                for ch in captcha_text:
                    response_box.send_keys(ch)
                    time.sleep(random.uniform(0.05, 0.15))
                
                # Submit
                response_box.send_keys(Keys.ENTER)
                logger.info(f"✅ Submitted response: {captcha_text}")
                time.sleep(5)  # Wait for verification
                
                # Switch back to main content
                driver.switch_to.default_content()
                
                logger.info("🎉 CAPTCHA solved successfully!")
                return "solved"
            else:
                logger.error("❌ Could not find response input box")
                driver.switch_to.default_content()
                return "quit"
                
        except Exception as e:
            logger.error(f"❌ Error entering response: {e}")
            driver.switch_to.default_content()
            return "quit"
            
    except Exception as e:
        logger.error(f"❌ Unexpected error in solve_recaptcha_audio: {e}")
        import traceback
        traceback.print_exc()
        return "quit"
    finally:
        try:
            driver.switch_to.default_content()
        except:
            pass
        # Cleanup
        cleanup_audio_files()

# Cleanup function to remove old audio files
def cleanup_audio_files():
    import glob
    import os
    
    audio_files = glob.glob("captcha_audio_*")
    for file in audio_files:
        try:
            os.remove(file)
            logger.debug(f"Cleaned up: {file}")
        except:
            pass