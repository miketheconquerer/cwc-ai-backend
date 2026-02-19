# server.py
from fastapi import FastAPI
from pydantic import BaseModel
from agent import ask_agent  # import your working AI

app = FastAPI()

# Request format
class ChatRequest(BaseModel):
    message: str

# Endpoint
@app.post("/chat")
def chat(req: ChatRequest):
    reply = ask_agent(req.message)
    return {"reply": reply}
