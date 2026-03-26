from fastapi import FastAPI
from transformers import pipeline

app = FastAPI()

print("Loading model...")

pipe = pipeline(
    "text-generation",
    model="sshleifer/tiny-gpt2"
)

print("Model loaded")

@app.get("/")
def health():
    return {"status": "ok"}

@app.get("/gen")
def generate(q: str):
    out = pipe(q, max_length=50)
    return {"text": out[0]["generated_text"]}