from fastapi import FastAPI
from pydantic import BaseModel
import requests
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
TAVILY_API_KEY = os.getenv("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")

class ChatRequest(BaseModel):
    message: str

# ---- Real-time search (stable) ----
def search_web(query: str) -> str:
    if not TAVILY_API_KEY:
        return ""

    url = "https://api.tavily.com/search"
    payload = {
        "api_key": TAVILY_API_KEY,
        "query": query,
        "max_results": 3
    }

    try:
        res = requests.post(url, json=payload, timeout=8)
        data = res.json()
        results = [r["content"] for r in data.get("results", [])]
        return "\n".join(results)
    except:
        return ""

# ---- Groq AI call ----
def ask_groq(prompt: str) -> str:
    url = "https://api.groq.com/openai/v1/chat/completions"

    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }

    system_prompt = """
You are CWC AI — elite China business strategist trained on:
- China-Europe trade
- Manufacturing sourcing
- Investments in China
- Cross-border deals
- China market entry

Your personality:
- Sharp
- Strategic
- Concise
- Practical
- Sounds like a high-level consultant

If unsure, give strategic insights instead of generic answers.
"""

    data = {
        "model": "llama3-70b-8192",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.4
    }

    res = requests.post(url, headers=headers, json=data)
    return res.json()["choices"][0]["message"]["content"]

# ---- Chat endpoint ----
@app.post("/chat")
def chat(req: ChatRequest):
    user_msg = req.message

    # Real-time context
    live_data = search_web(user_msg)

    final_prompt = f"""
User question: {user_msg}

Relevant real-time context:
{live_data}

Answer as CWC AI.
"""

    reply = ask_groq(final_prompt)

    return {"reply": reply}

@app.get("/")
def root():
    return {"message": "CWC AI stable backend running"}
