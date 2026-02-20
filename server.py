from fastapi import FastAPI
from pydantic import BaseModel
import requests
import os
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

app = FastAPI()

# Get API keys from environment variables
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY")

class ChatRequest(BaseModel):
    message: str

# ---- Real-time web search via Tavily ----
def search_web(query: str) -> str:
    if not TAVILY_API_KEY:
        print("TAVILY_API_KEY is missing!")
        return "No live search available. Tavily API key not set."

    url = "https://api.tavily.com/search"
    payload = {
        "api_key": TAVILY_API_KEY,
        "query": query,
        "max_results": 3
    }

    try:
        res = requests.post(url, json=payload, timeout=8)
        res.raise_for_status()
        data = res.json()
        results = [r.get("content", "") for r in data.get("results", [])]
        return "\n".join(results) if results else "No relevant live search results found."
    except Exception as e:
        print("Error in Tavily search:", e)
        return "Error fetching live search results."

# ---- Groq AI call ----
def ask_groq(prompt: str) -> str:
    if not GROQ_API_KEY:
        print("GROQ_API_KEY is missing!")
        return "Cannot generate AI response. GROQ API key not set."

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
        "model": "llama-3.3-70b-versatile",  # <-- fixed your exact model
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.4
    }

    try:
        res = requests.post(url, headers=headers, json=data, timeout=12)
        res.raise_for_status()
        content = res.json()
        return content["choices"][0]["message"]["content"]
    except Exception as e:
        print("Error in Groq AI call:", e)
        return "AI service failed to generate a response."

# ---- Chat endpoint ----
@app.post("/chat")
def chat(req: ChatRequest):
    print("User message received:", req.message)
    user_msg = req.message

    # Real-time context
    live_data = search_web(user_msg)
    print("Live search data:", live_data)

    final_prompt = f"""
User question: {user_msg}

Relevant real-time context:
{live_data}

Answer as CWC AI.
"""
    reply = ask_groq(final_prompt)
    print("AI reply generated:", reply)

    return {"reply": reply}

# ---- Root endpoint ----
@app.get("/")
def root():
    return {"message": "CWC AI stable backend running"}

