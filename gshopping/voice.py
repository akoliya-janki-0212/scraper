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