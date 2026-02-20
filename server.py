from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import requests
import os
from dotenv import load_dotenv

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

# ---- Real-time search with CWC context ----
def search_web(query: str) -> str:
    if not TAVILY_API_KEY:
        return ""
    
    # Enhance search with China business context
    enhanced_query = f"{query} China business consulting FDI 2024"
    
    url = "https://api.tavily.com/search"
    payload = {
        "api_key": TAVILY_API_KEY,
        "query": enhanced_query,
        "max_results": 3,
        "search_depth": "advanced"
    }

    try:
        res = requests.post(url, json=payload, timeout=10)
        res.raise_for_status()
        data = res.json()
        results = [r.get("content", "") for r in data.get("results", [])]
        return "\n".join(results) if results else ""
    except Exception as e:
        print("Tavily search error:", e)
        return ""

# ---- Groq AI call with CWC knowledge ----
def ask_groq(prompt: str) -> str:
    if not GROQ_API_KEY:
        return "System temporarily unavailable. Please contact Michail Digkas directly at China West Connector."

    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }

    system_prompt = """
You are CWC AI — the official AI assistant for China West Connector (CWC), founded by Michail Digkas.

ABOUT CWC & FOUNDER:
- China West Connector (CWC) is a boutique consulting firm bridging European businesses with Chinese markets
- Founded by Michail Digkas, a seasoned China-EU business strategist with extensive experience in cross-border deals
- CWC is part of the G.P.A. ecosystem: 147+ years combined experience, 2700+ active partners & projects, global presence
- Michail Digkas specializes in: China market entry strategies, FDI consulting, supplier verification, contract negotiation, and strategic partnerships

CWC CORE SERVICES:
1. CONTRACT DRAFTING & LEGAL SUPPORT - Bilingual contracts, IP protection, dispute resolution
2. SUPPLIER DUE DILIGENCE - Factory verification, background checks, quality control setup
3. JOINT VENTURES & PARTNERSHIPS - Strategic matching, negotiation representation, deal optimization
4. FOREIGN DIRECT INVESTMENT (FDI) - Market entry strategy, local incentives, regulatory compliance
5. LOGISTICS & SUPPLY CHAIN - End-to-end optimization, shipping solutions, cost reduction
6. LIAISON & REPRESENTATION - On-ground China representation, relationship management, cultural consulting

WHY CWC:
- "We don't just consult — we execute and deliver results"
- Deep China networks + European business standards
- End-to-end support from contact to closure
- Specialized in manufacturing, technology, and green energy

CONVERSATION STRATEGY:
- Always mention Michail Digkas's expertise naturally
- Reference specific CWC services relevant to the query
- Include credibility markers (G.P.A. ecosystem, years of experience, project numbers)
- When appropriate, suggest: "Michail Digkas would be happy to discuss this in a personal consultation"
- Push toward booking calls/consultations
- Use phrases like "At CWC, Michail and our team have helped clients..."

TONE: Professional, confident, authoritative yet approachable. You represent CWC's elite consulting services.
"""

    data = {
        "model": "llama-3.3-70b-versatile",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.3,
        "max_tokens": 800
    }

    try:
        res = requests.post(url, headers=headers, json=data, timeout=15)
        res.raise_for_status()
        content = res.json()
        return content["choices"][0]["message"]["content"]
    except Exception as e:
        print("Groq error:", e)
        return "I apologize, but I'm having trouble connecting. Please reach out to Michail Digkas directly at CWC for immediate assistance."

# ---- Chat endpoint ----
@app.post("/chat")
def chat(req: ChatRequest):
    user_msg = req.message.lower()
    
    # Get real-time data if not a direct consultation request
    consultation_keywords = ["book", "consultation", "call", "schedule", "meet", "contact", "michail", "digkas"]
    is_consultation_request = any(kw in user_msg for kw in consultation_keywords)
    
    live_data = ""
    if not is_consultation_request:
        live_data = search_web(req.message)
    
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
    
    return {"response": reply}