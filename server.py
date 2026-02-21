from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import requests
import os
import sqlite3
import json
import smtplib
from email.mime.text import MIMEText
from datetime import datetime, timedelta
from dotenv import load_dotenv

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

# ---- Database Setup ----
def init_db():
    conn = sqlite3.connect('cwc_leads.db')
    c = conn.cursor()
    
    # Conversations table
    c.execute('''CREATE TABLE IF NOT EXISTS conversations
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  session_id TEXT,
                  user_message TEXT,
                  ai_response TEXT,
                  timestamp DATETIME,
                  email TEXT,
                  company TEXT,
                  region TEXT,
                  intent TEXT)''')
    
    # Leads table
    c.execute('''CREATE TABLE IF NOT EXISTS leads
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  name TEXT,
                  email TEXT,
                  company TEXT,
                  region TEXT,
                  session_id TEXT,
                  source TEXT,
                  timestamp TEXT,
                  status TEXT)''')
    
    conn.commit()
    conn.close()

init_db()

# ---- Pydantic Models ----
class ChatRequest(BaseModel):
    message: str
    session_id: str = "anonymous"

class LeadCapture(BaseModel):
    name: str
    email: str
    company: str = ""
    region: str = ""
    session_id: str = ""
    source: str = "chat_widget"
    timestamp: str = ""

# ---- Database Functions ----
def save_conversation(session_id, user_msg, ai_response, email=None, company=None, region=None, intent=None):
    conn = sqlite3.connect('cwc_leads.db')
    c = conn.cursor()
    c.execute("""INSERT INTO conversations 
                 (session_id, user_message, ai_response, timestamp, email, company, region, intent)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
              (session_id, user_msg, ai_response, datetime.now(), email, company, region, intent))
    conn.commit()
    conn.close()

def get_conversation_history(session_id, limit=5):
    conn = sqlite3.connect('cwc_leads.db')
    c = conn.cursor()
    c.execute("""SELECT user_message, ai_response FROM conversations 
                 WHERE session_id = ? ORDER BY timestamp DESC LIMIT ?""", 
              (session_id, limit))
    history = c.fetchall()
    conn.close()
    return history[::-1]

# ---- Intent Detection ----
def detect_intent(message: str) -> dict:
    msg_lower = message.lower()
    
    intents = {
        "high_intent_lead": ["price", "cost", "quote", "proposal", "start", "begin", "hire", "contract", "serious", "budget", "invest"],
        "consultation_request": ["book", "consultation", "call", "schedule", "meet", "contact", "talk", "discuss"],
        "information_gathering": ["how", "what", "tell me", "explain", "information"],
        "supplier_verification": ["verify", "check", "audit", "due diligence", "factory", "supplier", "manufacturer"],
        "regional_interest": {
            "africa": ["africa", "african", "mining", "infrastructure"],
            "middle_east": ["middle east", "mea", "gcc", "dubai", "saudi", "energy", "oil", "gas"],
            "latam": ["latam", "latin america", "brazil", "mexico", "argentina", "chile", "lithium"],
            "europe": ["europe", "eu", "germany", "france", "green tech", "automotive"],
            "central_asia": ["central asia", "kazakhstan", "uzbekistan", "belt and road", "bri"]
        }
    }
    
    detected = {"primary": "general", "region": None, "score": 0}
    
    if any(kw in msg_lower for kw in intents["high_intent_lead"]):
        detected["primary"] = "high_intent_lead"
        detected["score"] = 90
    elif any(kw in msg_lower for kw in intents["consultation_request"]):
        detected["primary"] = "consultation_request"
        detected["score"] = 85
    elif any(kw in msg_lower for kw in intents["supplier_verification"]):
        detected["primary"] = "supplier_verification"
        detected["score"] = 80
    
    for region, keywords in intents["regional_interest"].items():
        if any(kw in msg_lower for kw in keywords):
            detected["region"] = region
            break
    
    return detected

# ---- Web Search ----
def search_web(query: str) -> str:
    if not TAVILY_API_KEY:
        return ""
    
    current_year = 2026
    news_keywords = ["news", "latest", "update", "today", "recent", "announced"]
    is_news_query = any(kw in query.lower() for kw in news_keywords)
    
    if is_news_query:
        enhanced_query = f"{query} China business trade investment {current_year} latest news"
    else:
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

# ---- Groq AI ----
def ask_groq(prompt: str, session_id: str = "anonymous") -> str:
    if not GROQ_API_KEY:
        return "System temporarily unavailable. Contact Michail Digkas at CWC."

    history = get_conversation_history(session_id)
    context = ""
    if history:
        context = "\nPrevious conversation:\n"
        for user_msg, ai_resp in history:
            context += f"User: {user_msg}\nAI: {ai_resp[:100]}...\n"
    
    intent_data = detect_intent(prompt)
    
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }

    system_prompt = f"""
You are CWC AI — the official AI assistant for China West Connector (CWC), founded by Michail Digkas.

CURRENT DATE: February 2026

USER INTENT: {intent_data['primary']}
REGION: {intent_data['region'] or 'Global'}

CONVERSATION MEMORY: {context}

RESPONSE STRATEGY:
- If high_intent_lead: Be consultative, ask qualifying questions, push for meeting
- If consultation_request: Offer calendar link immediately, be enthusiastic
- If supplier_verification: Emphasize CWC's audit capabilities
- If information_gathering: Provide value but always suggest consultation for specifics

GEOGRAPHIC FOCUS:
- "The West" includes: Europe, North America, Latin America (LATAM), Africa, Middle East, Central Asia

REGIONAL INTELLIGENCE:
- AFRICA: Mining partnerships, infrastructure financing, tech transfer, agri-processing
- MIDDLE EAST: Energy partnerships, Belt & Road, fintech bridges, petrochemicals
- LATAM: Agri-tech, EV supply chain, critical minerals (lithium, copper), soybean trade
- CENTRAL ASIA: Energy corridors, logistics hubs, cross-border trade
- EUROPE: Green tech, automotive, luxury goods, compliance consulting

ABOUT CWC & FOUNDER:
- China West Connector bridges Chinese markets with Western businesses
- Founded by Michail Digkas, expert in China cross-border deals
- Part of G.P.A. ecosystem: 147+ years experience, 2700+ projects

CWC CORE SERVICES:
1. CONTRACT & LEGAL - Bilingual contracts, IP protection
2. SUPPLIER DUE DILIGENCE - Factory audits, verification
3. JOINT VENTURES - Strategic matching, negotiations
4. FDI CONSULTING - Market entry, incentives, compliance
5. LOGISTICS - Supply chain optimization
6. LIAISON - On-ground China representation

RULES:
- MAX 2-3 short paragraphs (150 words max)
- Be concise, direct, actionable
- Always mention Michail Digkas naturally
- Push toward consultation booking for high-intent users
- Use conversation memory to personalize responses
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
        response_text = content["choices"][0]["message"]["content"]
        
        save_conversation(session_id, prompt, response_text, 
                         region=intent_data['region'], 
                         intent=intent_data['primary'])
        
        return response_text
    except Exception as e:
        print("Groq error:", e)
        return "I apologize, but I'm having trouble connecting. Please reach out to Michail Digkas directly at CWC."

# ---- Email Notification ----
def send_lead_notification(lead: LeadCapture):
    """Send email notification for new lead to ProtonMail"""
    
    # ProtonMail Bridge configuration (if installed locally)
    # For Render cloud, email will be logged to console
    SMTP_SERVER = "127.0.0.1"
    SMTP_PORT = 1025
    
    SENDER_EMAIL = "info@chinawestconnector.com"
    RECIPIENT_EMAIL = "digkasm@proton.me"
    
    try:
        msg = MIMEText(f"""
New Lead Captured from CWC AI Chat!

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

👤 NAME: {lead.name}
📧 EMAIL: {lead.email}
🏢 COMPANY: {lead.company or 'Not provided'}
🌍 REGION: {lead.region or 'Not specified'}
📱 SOURCE: {lead.source}
⏰ TIME: {lead.timestamp}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

View all leads: https://cwc-ai-backend.onrender.com/leads?password=your-secret-password

Reply to this lead: mailto:{lead.email}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        """)
        
        msg['Subject'] = f'🎯 New Lead: {lead.name} from {lead.company or "Website"}'
        msg['From'] = SENDER_EMAIL
        msg['To'] = RECIPIENT_EMAIL
        msg['Reply-To'] = lead.email
        
        # Try ProtonMail Bridge first
        try:
            server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
            server.starttls()
            server.send_message(msg)
            server.quit()
            print("Lead notification sent via ProtonMail Bridge")
            return
        except Exception as bridge_error:
            print(f"ProtonMail Bridge failed: {bridge_error}")
        
        # Fallback: log to console (view in Render logs)
        print("=" * 50)
        print("NEW LEAD CAPTURED (Email failed - check dashboard)")
        print(f"Name: {lead.name}")
        print(f"Email: {lead.email}")
        print(f"Company: {lead.company}")
        print(f"Region: {lead.region}")
        print("=" * 50)
        
    except Exception as e:
        print(f"Email notification error: {e}")

# ---- API Endpoints ----
@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "groq_configured": bool(GROQ_API_KEY),
        "tavily_configured": bool(TAVILY_API_KEY)
    }

@app.get("/")
def root():
    return {"message": "CWC AI backend running"}

@app.post("/chat")
def chat(req: ChatRequest):
    user_msg = req.message.lower()
    
    if any(word in user_msg for word in ["stop", "shorter", "brief", "short", "too long"]):
        return {"response": "Got it. I'll keep my answers brief. What would you like to know about China business opportunities?"}
    
    consultation_keywords = ["book", "consultation", "call", "schedule", "meet", "contact", "michail", "digkas"]
    is_consultation_request = any(kw in user_msg for kw in consultation_keywords)
    
    live_data = ""
    if not is_consultation_request:
        live_data = search_web(req.message)
    
    context = ""
    if live_data:
        context = f"\n\nRelevant market data:\n{live_data}\n"
    
    final_prompt = f"""User question: {req.message}{context}

Respond as CWC AI, representing China West Connector and Michail Digkas. 
Be specific about CWC services. Reference Michail's expertise naturally.
If the user shows buying intent or complex needs, suggest booking a consultation with Michail Digkas.
Keep response concise but authoritative (2-4 paragraphs max)."""

    reply = ask_groq(final_prompt, req.session_id)
    
    if any(word in user_msg for word in ["price", "cost", "fee", "how much", "start", "begin", "help me", "serious", "interested", "manufacturer", "supplier", "factory"]):
        if "consultation" not in reply.lower() and "book" not in reply.lower():
            reply += "\n\nWould you like to schedule a personal consultation with Michail Digkas to discuss your specific situation? Click 'Speak with Michail Digkas' above."
    
    return {"response": reply}

@app.post("/capture-lead")
async def capture_lead(lead: LeadCapture, background_tasks: BackgroundTasks):
    conn = sqlite3.connect('cwc_leads.db')
    c = conn.cursor()
    c.execute("""INSERT INTO leads 
                 (name, email, company, region, session_id, source, timestamp, status)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
              (lead.name, lead.email, lead.company, lead.region, 
               lead.session_id, lead.source, lead.timestamp, 'new'))
    conn.commit()
    conn.close()
    
    background_tasks.add_task(send_lead_notification, lead)
    
    return {"status": "success", "message": "Lead captured"}

@app.get("/leads")
def view_leads(password: str = None):
    """Simple lead dashboard - password protected"""
    if password != "your-secret-password":
        return {"error": "Unauthorized"}
    
    conn = sqlite3.connect('cwc_leads.db')
    c = conn.cursor()
    c.execute("SELECT * FROM leads ORDER BY timestamp DESC LIMIT 50")
    leads = c.fetchall()
    conn.close()
    
    lead_list = []
    for lead in leads:
        lead_list.append({
            "id": lead[0],
            "name": lead[1],
            "email": lead[2],
            "company": lead[3],
            "region": lead[4],
            "timestamp": lead[7],
            "status": lead[8]
        })
    
    return {"leads": lead_list, "count": len(lead_list)}