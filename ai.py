from fastapi import FastAPI
from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline
import torch

ai = FastAPI()

pipe = None

@ai.on_event("startup")
def load_model():
    global pipe

    model_id = "TinyLlama/TinyLlama-1.1B-Chat-v1.0"

    tokenizer = AutoTokenizer.from_pretrained(model_id)
    model = AutoModelForCausalLM.from_pretrained(
        model_id,
        torch_dtype=torch.float32,
        low_cpu_mem_usage=True
    )

    pipe = pipeline(
        "text-generation",
        model=model,
        tokenizer=tokenizer
    )

@ai.get("/")
def health():
    return {"status": "tinyllama-ready"}

@ai.get("/chat")
def chat(q: str):

    prompt = f"<|user|>\n{q}\n<|assistant|>\n"

    out = pipe(
        prompt,
        max_new_tokens=120,
        do_sample=True,
        temperature=0.7,
        top_p=0.9
    )

    text = out[0]["generated_text"].split("<|assistant|>")[-1]

    return {"response": text.strip()}