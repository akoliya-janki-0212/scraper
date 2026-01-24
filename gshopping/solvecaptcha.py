import time
import os
import urllib.request
import random
import pydub
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import random
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
            time.sleep(2)
            try:
                audio_source = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.ID, "audio-source"))
                )
                src = audio_source.get_attribute("src")  # Get direct MP3 URL
                print(f"[INFO] Audio source URL: {src}")
            except:
                print("❌ Could not find the audio source.")
                driver.switch_to.default_content()
                return "no audio"
            # ✅ Define file paths
            mp3_path = os.path.join(os.getcwd(), "captcha_audio.mp3")
            wav_path = os.path.join(os.getcwd(), "captcha_audio.wav")

            # ✅ Download the audio file
            urllib.request.urlretrieve(src, mp3_path)
            print("✅ Audio file downloaded.")

            # ✅ Convert MP3 to WAV
            try:
                sound = pydub.AudioSegment.from_mp3(mp3_path)
                sound.export(wav_path, format="wav")
                print("✅ Audio file converted to WAV.")
            except Exception as e:
                print(f"❌ Audio conversion error: {e}")
                driver.switch_to.default_content()
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


# ✅ Initialize Chrome Driver
# driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

# # ✅ Open the reCAPTCHA Demo Page
# driver.get("https://www.google.com/recaptcha/api2/demo")
# solve_recaptcha_audio(driver)
