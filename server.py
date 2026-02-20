from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import requests
import os
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

app = FastAPI()

# ---- CORS configuration ----
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

# ---- API keys ----
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY")

class ChatRequest(BaseModel):
    message: str

# ---- Health check endpoint ----
@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "groq_configured": bool(GROQ_API_KEY),
        "tavily_configured": bool(TAVILY_API_KEY)
    }

# ---- Root endpoint ----
@app.get("/")
def root():
    return {"message": "CWC AI stable backend running"}

# ---- Real-time search with time-aware context ----
def search_web(query: str) -> str:
    if not TAVILY_API_KEY:
        return ""
    
    # Current year is 2026
    current_year = 2026
    
    # Enhance search with time-aware context
    enhanced_query = f"{query} China business {current_year} {current_year+1} latest trends"
    
    url = "https://api.tavily.com/search"
    payload = {
        "api_key": TAVILY_API_KEY,
        "query": enhanced_query,
        "max_results": 3,
        "search_depth": "advanced",
        "include_answer": True
    }

    try:
        res = requests.post(url, json=payload, timeout=10)
        res.raise_for_status()
        data = res.json()
        
        answer = data.get("answer", "")
        results = [r.get("content", "") for r in data.get("results", [])]
        
        combined = ""
        if answer:
            combined += f"Summary: {answer}\n\n"
        combined += "\n".join(results[:2])
        
        return combined if combined else ""
    except Exception as e:
        print("Tavily search error:", e)
        return ""

# ---- Groq AI call with CWC knowledge ----
def ask_groq(prompt: str) -> str:
    if not GROQ_API_KEY:
        return "System temporarily unavailable. Contact Michail Digkas at CWC."

    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }

    system_prompt = """
You are CWC AI — the official AI assistant for China West Connector (CWC), founded by Michail Digkas.

CURRENT DATE: February 2026

GEOGRAPHIC FOCUS:
- "The West" includes: Europe, North America, Latin America (LATAM), Africa, Middle East, Central Asia
- CWC connects China with ALL Western markets globally, not just Europe
- Mention specific regions when relevant to the user's query

ABOUT CWC & FOUNDER:
- China West Connector (CWC) bridges Chinese markets with Western businesses worldwide
- Founded by Michail Digkas, expert in China cross-border deals across multiple continents
- CWC is part of G.P.A. ecosystem: 147+ years experience, 2700+ projects, global reach
- Michail specializes in: China market entry, FDI, supplier verification, negotiations

CWC CORE SERVICES:
1. CONTRACT & LEGAL - Bilingual contracts, IP protection, dispute resolution
2. SUPPLIER DUE DILIGENCE - Factory audits, background checks, quality control
3. JOINT VENTURES - Strategic matching, negotiation, deal optimization
4. FDI CONSULTING - Market entry, incentives, compliance
5. LOGISTICS - Supply chain optimization, shipping solutions
6. LIAISON - On-ground China representation, relationship management

RESPONSE RULES:
- MAXIMUM 2-3 short paragraphs (150 words max)
- Be concise, direct, actionable
- Always mention Michail Digkas naturally
- Push toward consultation booking
- Use current year 2026 and future 2027 when relevant
"""

    data = {
        "model": "llama-3.3-70b-versatile",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.3,
<<<<<<< HEAD
        "max_tokens": 300  # Reduced for shorter answers
    }

    try:
        res = requests.post(url, headers=headers, json=data, timeout=12)
=======
        "max_tokens": 800
    }

    try:
        res = requests.post(url, headers=headers, json=data, timeout=15)
>>>>>>> 7fa2d9c2d348bc01d72a8e980e5235c0d88a9b7f
        res.raise_for_status()
        content = res.json()
        return content["choices"][0]["message"]["content"]
    except Exception as e:
        print("Groq error:", e)
<<<<<<< HEAD
        return "Connection issue. Contact Michail Digkas directly at CWC."

@app.post("/chat")
def chat(req: ChatRequest):
    user_msg = req.message.lower()
    current_year = 2026
    
    # Check for stop/shorten requests
    if any(word in user_msg for word in ["stop", "shorter", "brief", "short", "too long"]):
        return {"response": "Got it. I'll keep my answers brief. What would you like to know about China business opportunities?"}
    
    # Get real-time data
=======
        return "I apologize, but I'm having trouble connecting. Please reach out to Michail Digkas directly at CWC for immediate assistance."

# ---- Chat endpoint ----
@app.post("/chat")
def chat(req: ChatRequest):
    user_msg = req.message.lower()
    
    # Get real-time data if not a direct consultation request
>>>>>>> 7fa2d9c2d348bc01d72a8e980e5235c0d88a9b7f
    consultation_keywords = ["book", "consultation", "call", "schedule", "meet", "contact", "michail", "digkas"]
    is_consultation_request = any(kw in user_msg for kw in consultation_keywords)
    
    live_data = ""
    if not is_consultation_request:
        live_data = search_web(req.message)
    
<<<<<<< HEAD
    # Build prompt
    context = ""
    if live_data:
        context = f"\n\nReal-time data (Feb 2026):\n{live_data}\n"
    
    future_context = ""
    if any(year in user_msg for year in ["2026", "2027", "2028", "future", "next year"]):
        future_context = f"\nNote: User asking about 2026-2028. Frame as current/future opportunities."
    
    final_prompt = f"""Question: {req.message}{context}{future_context}

Respond in 2-3 short paragraphs max. Mention Michail Digkas. Suggest consultation if relevant."""

    reply = ask_groq(final_prompt)
    
    # Add CTA for high-intent queries
    if any(word in user_msg for word in ["price", "cost", "start", "help", "serious", "interested", "manufacturer", "2026", "2027"]):
        if "consultation" not in reply.lower():
            reply += f"\n\nWant to discuss your 2026-2027 China strategy with Michail Digkas? Click 'Book Consultation' above."
=======
    # Build enhanced prompt
    context = ""
    if live_data:
        context = f"\n\nRelevant market data:\n{live_data}\n"
    
    final_prompt = f"""User question: {req.message}{context}

Respond as CWC AI, representing China West Connector and Michail Digkas. 
Be specific about CWC services. Reference Michail's expertise naturally.
If the user shows buying intent or complex needs, suggest booking a consultation with Michail Digkas.
Keep response concise but authoritative (2-4 paragraphs max)."""

    reply = ask_groq(final_prompt)
    
    # Add consultation CTA for high-intent queries
    if any(word in user_msg for word in ["price", "cost", "fee", "how much", "start", "begin", "help me", "serious", "interested", "manufacturer", "supplier", "factory"]):
        if "consultation" not in reply.lower() and "book" not in reply.lower():
            reply += "\n\nWould you like to schedule a personal consultation with Michail Digkas to discuss your specific situation? Click 'Book Consultation' above or let me know your preferred time."
>>>>>>> 7fa2d9c2d348bc01d72a8e980e5235c0d88a9b7f
    
    return {"response": reply}