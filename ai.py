from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def root():
    return {"status": "running"}

@app.get("/gen")
def gen(q: str):
    return {"response": f"hello {q}"}