from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import requests
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

app = FastAPI()

origins = [
    "https://www.chinawestconnector.com",
    "https://chinawestconnector.com",
    "http://localhost:8000",
    "http://localhost:3000",
    "https://localhost",
    "null",
    "*"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY")

class ChatRequest(BaseModel):
    message: str

@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "groq_configured": bool(GROQ_API_KEY),
        "tavily_configured": bool(TAVILY_API_KEY)
    }

@app.get("/")
def root():
    return {"message": "CWC AI stable backend running"}

def search_web(query: str) -> str:
    if not TAVILY_API_KEY:
        return ""
    
    current_year = 2026
    enhanced_query = f"{query} China business {current_year} {current_year+1} latest"
    
    url = "https://api.tavily.com/search"
    payload = {
        "api_key": TAVILY_API_KEY,
        "query": enhanced_query,
        "max_results": 2,
        "search_depth": "basic",
        "include_answer": True
    }

    try:
        res = requests.post(url, json=payload, timeout=8)
        res.raise_for_status()
        data = res.json()
        answer = data.get("answer", "")
        results = [r.get("content", "") for r in data.get("results", [])]
        combined = answer if answer else ""
        combined += "\n" + "\n".join(results[:1])
        return combined
    except Exception as e:
        return ""

def ask_groq(prompt: str) -> str:
    if not GROQ_API_KEY:
        return "Contact Michail Digkas at CWC."

    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }

    system_prompt = """
You are CWC AI for China West Connector, founded by Michail Digkas.

CURRENT: February 2026

GEOGRAPHY: The West = Europe, Americas, Africa, Middle East, Central Asia, LATAM

CWC SERVICES:
1. Contract & Legal - Bilingual contracts, IP protection
2. Supplier Due Diligence - Factory audits, verification
3. Joint Ventures - Strategic matching, negotiations
4. FDI Consulting - Market entry, incentives, compliance
5. Logistics - Supply chain optimization
6. Liaison - On-ground China representation

RULES:
- MAX 2 short paragraphs (100 words total max)
- Be direct, no fluff
- Mention Michail Digkas naturally
- Suggest consultation when relevant
"""

    data = {
        "model": "llama-3.3-70b-versatile",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.3,
        "max_tokens": 200
    }

    try:
        res = requests.post(url, headers=headers, json=data, timeout=10)
        res.raise_for_status()
        content = res.json()
        return content["choices"][0]["message"]["content"]
    except Exception as e:
        return "Contact Michail Digkas directly at CWC."

@app.post("/chat")
def chat(req: ChatRequest):
    user_msg = req.message.lower()
    current_year = 2026
    
    if "stop" in user_msg and len(user_msg) < 10:
        return {"response": "Stopped. What else can I help you with?"}
    
    consultation_keywords = ["book", "consultation", "call", "schedule", "meet", "contact", "michail", "digkas"]
    is_consultation_request = any(kw in user_msg for kw in consultation_keywords)
    
    live_data = ""
    if not is_consultation_request:
        live_data = search_web(req.message)
    
    context = ""
    if live_data:
        context = f"\nData: {live_data}\n"
    
    final_prompt = f"""Q: {req.message}{context}
Answer in 2 short paragraphs max. Mention Michail Digkas if relevant."""

    reply = ask_groq(final_prompt)
    
    if any(word in user_msg for word in ["price", "cost", "start", "help", "interested", "manufacturer"]):
        if "consultation" not in reply.lower():
            reply += f" Book a consultation with Michail Digkas?"
    
    return {"response": reply}