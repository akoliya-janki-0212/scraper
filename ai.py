from fastapi import FastAPI
from transformers import pipeline

app = FastAPI()

pipe = pipeline("text-generation", model="sshleifer/tiny-gpt2")

@app.get("/")
def root():
    return {"status": "running"}

@app.get("/gen")
def gen(q: str):
    out = pipe(q, max_length=50)
    return {"text": out[0]["generated_text"]}