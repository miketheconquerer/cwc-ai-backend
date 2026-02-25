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
import asyncio
from contextlib import asynccontextmanager

load_dotenv()

# ---- Configuration ----
BREVO_API_KEY = os.getenv("BREVO_API_KEY", "")
SENDER_EMAIL = "888nv666@gmail.com"  # Your verified Brevo sender
RECIPIENT_EMAIL = "digkasm@proton.me"  # Where to receive reports

# ---- Scheduler Setup ----
scheduler_running = False

async def schedule_weekly_report():
    """Send weekly report every Monday at 9 AM"""
    global scheduler_running
    scheduler_running = True
    
    while scheduler_running:
        now = datetime.now()
        # Calculate seconds until next Monday 9 AM
        days_until_monday = (7 - now.weekday()) % 7
        if days_until_monday == 0 and now.hour >= 9:
            days_until_monday = 7
        next_monday = now + timedelta(days=days_until_monday)
        next_monday = next_monday.replace(hour=9, minute=0, second=0, microsecond=0)
        seconds_until = (next_monday - now).total_seconds()
        
        await asyncio.sleep(seconds_until)
        
        # Send the report
        try:
            send_weekly_report()
        except Exception as e:
            print(f"Weekly report error: {e}")

def send_email_brevo(to_email: str, subject: str, body: str, from_name: str = "CWC AI") -> bool:
    """Send email using Brevo API"""
    url = "https://api.brevo.com/v3/smtp/email"
    
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "api-key": BREVO_API_KEY
    }
    
    payload = {
        "sender": {
            "name": from_name,
            "email": SENDER_EMAIL
        },
        "to": [
            {
                "email": to_email,
                "name": "Michail Digkas"
            }
        ],
        "subject": subject,
        "htmlContent": f"<html><body><pre style='font-family: monospace; white-space: pre-wrap;'>{body}</pre></body></html>",
        "textContent": body
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        if response.status_code == 201:
            print(f"✅ Email sent successfully to {to_email}")
            return True
        else:
            print(f"❌ Brevo error: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"❌ Email sending failed: {e}")
        return False

def send_weekly_report():
    """Generate and send weekly analytics report"""
    conn = sqlite3.connect('cwc_leads.db')
    c = conn.cursor()
    
    # Get stats for last 7 days
    week_ago = (datetime.now() - timedelta(days=7)).isoformat()
    
    # Total conversations
    c.execute("SELECT COUNT(DISTINCT session_id) FROM conversations WHERE timestamp > ?", (week_ago,))
    unique_users = c.fetchone()[0]
    
    # Total messages
    c.execute("SELECT COUNT(*) FROM conversations WHERE timestamp > ?", (week_ago,))
    total_messages = c.fetchone()[0]
    
    # New leads
    c.execute("SELECT COUNT(*) FROM leads WHERE timestamp > ?", (week_ago,))
    new_leads = c.fetchone()[0]
    
    # Returning users
    c.execute("SELECT COUNT(*) FROM user_profiles WHERE visit_count > 1 AND last_seen > ?", (week_ago,))
    returning_users = c.fetchone()[0]
    
    # Top intents
    c.execute("""SELECT intent, COUNT(*) as count FROM conversations 
                 WHERE timestamp > ? GROUP BY intent ORDER BY count DESC LIMIT 5""", (week_ago,))
    top_intents = c.fetchall()
    
    # Top regions
    c.execute("""SELECT region, COUNT(*) as count FROM conversations 
                 WHERE timestamp > ? AND region IS NOT NULL GROUP BY region ORDER BY count DESC LIMIT 5""", (week_ago,))
    top_regions = c.fetchall()
    
    # Recent leads details
    c.execute("""SELECT name, email, company, region, timestamp FROM leads 
                 WHERE timestamp > ? ORDER BY timestamp DESC LIMIT 10""", (week_ago,))
    recent_leads = c.fetchall()
    
    # Hot leads (high score)
    c.execute("""SELECT name, email, company, lead_score FROM user_profiles 
                 WHERE lead_score >= 50 ORDER BY lead_score DESC LIMIT 5""")
    hot_leads = c.fetchall()
    
    conn.close()
    
    # Build email
    intent_text = "\n".join([f"  • {i[0]}: {i[1]} queries" for i in top_intents]) if top_intents else "  No data"
    region_text = "\n".join([f"  • {r[0]}: {r[1]} queries" for r in top_regions]) if top_regions else "  No data"
    leads_text = "\n".join([f"  • {l[0]} ({l[2] or 'No company'}) - {l[1]} [{l[3] or 'No region'}]" for l in recent_leads]) if recent_leads else "  No new leads"
    hot_text = "\n".join([f"  • {h[0]} ({h[2] or 'No company'}) - Score: {h[3]}/100 - {h[1]}" for h in hot_leads if h[0]]) if hot_leads else "  No hot leads"
    
    email_body = f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📊 CWC AI WEEKLY REPORT
Week of {week_ago[:10]} to {datetime.now().strftime('%Y-%m-%d')}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📈 OVERVIEW
├─ Unique Users: {unique_users}
├─ Total Conversations: {total_messages}
├─ Returning Users: {returning_users}
└─ New Leads Captured: {new_leads}

🎯 TOP INTENTS
{intent_text}

🌍 TOP REGIONS
{region_text}

🔥 HOT LEADS (Score 50+)
{hot_text}

👤 RECENT LEADS (Last 7 Days)
{leads_text}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📈 Dashboard: https://cwc-ai-backend.onrender.com/analytics?password=your-secret-password
📊 Leads: https://cwc-ai-backend.onrender.com/leads?password=your-secret-password
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

This is an automated weekly report from your CWC AI Assistant.
"""
    
    # Send email via Brevo
    success = send_email_brevo(
        to_email=RECIPIENT_EMAIL,
        subject=f"📊 CWC AI Weekly Report - {unique_users} Users, {new_leads} Leads",
        body=email_body
    )
    
    if success:
        print("✅ Weekly report sent successfully!")
    else:
        print("❌ Weekly report failed to send")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start background tasks on app startup"""
    # Start the weekly report scheduler
    asyncio.create_task(schedule_weekly_report())
    yield
    # Cleanup
    global scheduler_running
    scheduler_running = False

app = FastAPI(lifespan=lifespan)

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
    
    # User profiles table (for returning user memory)
    c.execute('''CREATE TABLE IF NOT EXISTS user_profiles
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  session_id TEXT UNIQUE,
                  first_seen DATETIME,
                  last_seen DATETIME,
                  visit_count INTEGER DEFAULT 1,
                  name TEXT,
                  email TEXT,
                  company TEXT,
                  region_interest TEXT,
                  topics_discussed TEXT,
                  lead_score INTEGER DEFAULT 0,
                  last_intent TEXT)''')
    
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

# ---- User Profile Functions ----
def get_or_create_user_profile(session_id: str) -> dict:
    """Get existing user profile or create new one"""
    conn = sqlite3.connect('cwc_leads.db')
    c = conn.cursor()
    
    c.execute("SELECT * FROM user_profiles WHERE session_id = ?", (session_id,))
    profile = c.fetchone()
    
    if profile:
        # Update last seen and increment visit count
        c.execute("""UPDATE user_profiles 
                     SET last_seen = ?, visit_count = visit_count + 1 
                     WHERE session_id = ?""", 
                  (datetime.now(), session_id))
        conn.commit()
        
        user_profile = {
            "session_id": profile[1],
            "first_seen": profile[2],
            "last_seen": profile[3],
            "visit_count": profile[4] + 1,
            "name": profile[5],
            "email": profile[6],
            "company": profile[7],
            "region_interest": profile[8],
            "topics_discussed": profile[9],
            "lead_score": profile[10],
            "last_intent": profile[11],
            "is_returning": True
        }
    else:
        # Create new profile
        c.execute("""INSERT INTO user_profiles 
                     (session_id, first_seen, last_seen, visit_count)
                     VALUES (?, ?, ?, 1)""", 
                  (session_id, datetime.now(), datetime.now()))
        conn.commit()
        
        user_profile = {
            "session_id": session_id,
            "first_seen": datetime.now(),
            "last_seen": datetime.now(),
            "visit_count": 1,
            "name": None,
            "email": None,
            "company": None,
            "region_interest": None,
            "topics_discussed": None,
            "lead_score": 0,
            "last_intent": None,
            "is_returning": False
        }
    
    conn.close()
    return user_profile

def update_user_profile(session_id: str, **kwargs):
    """Update user profile with new information"""
    conn = sqlite3.connect('cwc_leads.db')
    c = conn.cursor()
    
    valid_fields = ['name', 'email', 'company', 'region_interest', 
                   'topics_discussed', 'lead_score', 'last_intent']
    
    updates = []
    values = []
    for key, value in kwargs.items():
        if key in valid_fields and value:
            updates.append(f"{key} = ?")
            values.append(value)
    
    if updates:
        values.append(session_id)
        query = f"UPDATE user_profiles SET {', '.join(updates)} WHERE session_id = ?"
        c.execute(query, values)
        conn.commit()
    
    conn.close()

def calculate_lead_score(user_profile: dict, message: str, intent: str) -> int:
    """Calculate lead score based on user behavior"""
    score = user_profile.get('lead_score', 0)
    
    intent_scores = {
        "high_intent_lead": 30,
        "consultation_request": 25,
        "supplier_verification": 20,
        "information_gathering": 5
    }
    score += intent_scores.get(intent, 0)
    
    if user_profile.get('visit_count', 1) > 1:
        score += 10
    
    high_value_keywords = ["budget", "invest", "contract", "serious", "start", "hire", "price"]
    if any(kw in message.lower() for kw in high_value_keywords):
        score += 15
    
    return min(score, 100)

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

def get_conversation_history(session_id, limit=10):
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
            "central_asia": ["central asia", "kazakhstan", "uzbekistan", "belt and road", "bri"],
            "china": ["china", "chinese", "mainland", "prc", "shenzhen", "shanghai", "beijing", "guangzhou"]
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
def ask_groq(prompt: str, session_id: str = "anonymous", user_profile: dict = None) -> str:
    if not GROQ_API_KEY:
        return "System temporarily unavailable. Contact the team at CWC."

    history = get_conversation_history(session_id)
    context = ""
    if history:
        context = "\nPrevious conversation:\n"
        for user_msg, ai_resp in history:
            context += f"User: {user_msg}\nAI: {ai_resp[:150]}...\n"
    
    intent_data = detect_intent(prompt)
    
    returning_context = ""
    if user_profile and user_profile.get('is_returning'):
        visit_count = user_profile.get('visit_count', 1)
        last_intent = user_profile.get('last_intent', '')
        region = user_profile.get('region_interest', '')
        name = user_profile.get('name', '')
        
        returning_context = f"""
RETURNING USER DETECTED:
- Visit count: {visit_count}
- Last visit intent: {last_intent}
- Region of interest: {region}
- Known name: {name if name else 'Unknown'}
- You SHOULD acknowledge their return warmly if this is their 2nd+ visit
"""

    system_prompt = f"""
You are Sophia — the official AI assistant for China West Connector (CWC).

CURRENT DATE: February 2026

USER INTENT: {intent_data['primary']}
REGION: {intent_data['region'] or 'Unknown'}
{returning_context}

CONVERSATION MEMORY: {context}

CRITICAL REGIONAL INTELLIGENCE PROTOCOL:

Before discussing ANY specific regions, you must FIRST determine:
1. Is the user Chinese (asking in Chinese language, mentions Chinese companies expanding, or references being in China)?
2. Or is the user foreign/Western (English/other languages, looking to enter China)?

IF USER IS CHINESE:
- Focus on: Africa, Middle East, LATAM, Europe, Central Asia as EXPANSION destinations
- Frame as: "Where would you like to expand? We bridge Chinese enterprises with these markets"
- Ask: "Are you looking to expand overseas? Which region interests your organization?"

IF USER IS FOREIGN/WESTERN:
- Focus on: CHINA as the primary market of interest
- Frame as: "How can we help you navigate the Chinese market?"
- Ask about their China goals: sourcing, market entry, partnerships, compliance

NEVER assume Africa or any region without first understanding user origin and intent.

GEOGRAPHIC FOCUS:
- "The West" includes: Europe, North America, Latin America (LATAM), Africa, Middle East, Central Asia
- For Chinese users: These are expansion targets
- For foreign users: China is the destination, these are their home markets

REGIONAL INTELLIGENCE (apply based on user origin):
- AFRICA: Mining partnerships, infrastructure financing, tech transfer, agri-processing (for Chinese expansion)
- MIDDLE EAST: Energy partnerships, Belt & Road, fintech bridges (for Chinese expansion)
- LATAM: Agri-tech, EV supply chain, critical minerals (for Chinese expansion)
- CENTRAL ASIA: Energy corridors, logistics hubs (for Chinese expansion)
- EUROPE: Green tech, automotive, luxury goods, compliance (for Chinese expansion OR foreign companies entering China)

ABOUT CWC:
- China West Connector bridges Chinese markets with Western businesses
- Founded by Michail Digkas, a practicing international business lawyer with over a decade of experience in China
- Part of G.P.A. ecosystem: 147+ years experience, 2700+ projects

CWC CORE SERVICES:
1. CONTRACT & LEGAL - Bilingual contracts, IP protection
2. SUPPLIER DUE DILIGENCE - Factory audits, verification
3. JOINT VENTURES - Strategic matching, negotiations
4. FDI CONSULTING - Market entry, incentives, compliance
5. LOGISTICS - Supply chain optimization
6. LIAISON - On-ground China representation

RESPONSE STRATEGY:
- If returning user: Acknowledge their return warmly, reference previous topics if relevant
- If high_intent_lead: Be consultative, ask qualifying questions, suggest speaking with the team
- If consultation_request: Direct them to click the "Speak with Michail" button
- If supplier_verification: Emphasize CWC's audit capabilities
- If information_gathering: Provide value but always suggest consultation for specifics

STYLE:
Max 2 short paragraphs (150 words preferred, never over 180).
Clear, confident, professional tone.
Concise and practical.
No hype, no buzzwords, no exaggerated claims.
Avoid sounding like marketing.

CORE POSITIONING:
Present CWC as a strategic bridge between foreign companies and Chinese enterprises.
Emphasize years of direct China experience, institutional access, and cross-border execution ability.

QUALIFICATIONS (when asked about credibility):
- Practicing international lawyer associated with leading firms in China and Greece
- Director of Foreign-Related Projects at Sichuan Technical Exchange Center (STEC)
- Represents both foreign companies in China and Chinese companies expanding overseas
- Over a decade of direct experience in China
- Facilitates real communication with Chinese companies and local authorities

PERSONALIZATION:
Adapt emphasis based on user intent:
- Investors → access, structuring, deal flow
- Manufacturers → sourcing, factories, compliance
- Institutions → coordination and government navigation
- Entrepreneurs → partnerships and execution clarity

CONVERSION RULE:
If the user shows serious business intent or asks about consultations:
ALWAYS direct them to click the "Speak with Michail" button at the top of the chat widget.
Keep it subtle and professional.

CRITICAL - CONSULTATION INSTRUCTIONS:
NEVER mention "calendar link", "click on this link", or "follow this link" - these links DO NOT EXIST.
When suggesting a consultation, ONLY say: "To arrange a consultation, click the 'Speak with Michail' button above."

FIRST MESSAGE PROTOCOL:
If this is the first message (no conversation history):
"Hello! I'm Sophia, the CWC AI intelligence. I'm here to help you navigate cross-border business opportunities with China.

To point you in the right direction, may I ask: Are you currently based in China looking to expand internationally, or are you looking to enter the Chinese market?"

SAFETY:
No unrealistic promises. No guarantees of outcomes. Stay credible and grounded.
"""

    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }

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
        
        new_score = calculate_lead_score(user_profile or {}, prompt, intent_data['primary'])
        update_user_profile(
            session_id,
            last_intent=intent_data['primary'],
            region_interest=intent_data['region'],
            lead_score=new_score
        )
        
        save_conversation(session_id, prompt, response_text, 
                         region=intent_data['region'], 
                         intent=intent_data['primary'])
        
        return response_text
    except Exception as e:
        print("Groq error:", e)
        return "I apologize, but I'm having trouble connecting. Please reach out to the team at CWC."

# ---- Email Notification ----
def send_lead_notification(lead: LeadCapture):
    """Send email notification for new lead using Brevo"""
    
    # Get user profile for lead score
    conn = sqlite3.connect('cwc_leads.db')
    c = conn.cursor()
    c.execute("SELECT lead_score, visit_count FROM user_profiles WHERE session_id = ?", (lead.session_id,))
    profile = c.fetchone()
    conn.close()
    
    lead_score = profile[0] if profile else 0
    visit_count = profile[1] if profile else 1
    
    email_body = f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🎯 NEW LEAD CAPTURED
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

👤 NAME: {lead.name}
📧 EMAIL: {lead.email}
🏢 COMPANY: {lead.company or 'Not provided'}
🌍 REGION: {lead.region or 'Not specified'}
📱 SOURCE: {lead.source}
⏰ TIME: {lead.timestamp}
🎯 LEAD SCORE: {lead_score}/100
🔄 VISIT COUNT: {visit_count}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📈 Dashboard: https://cwc-ai-backend.onrender.com/analytics?password=your-secret-password
📊 Leads: https://cwc-ai-backend.onrender.com/leads?password=your-secret-password

Reply to this lead: mailto:{lead.email}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
    
    send_email_brevo(
        to_email=RECIPIENT_EMAIL,
        subject=f"🎯 New Lead: {lead.name} from {lead.company or 'Website'} (Score: {lead_score})",
        body=email_body
    )

# ---- API Endpoints ----
@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "groq_configured": bool(GROQ_API_KEY),
        tavily_configured": bool(TAVILY_API_KEY),
        "brevo_configured": bool(BREVO_API_KEY)
    }

@app.get("/")
def root():
    return {"message": "CWC AI backend running"}

@app.post("/chat")
def chat(req: ChatRequest):
    user_msg = req.message.lower()
    
    # Get or create user profile (for returning user detection)
    user_profile = get_or_create_user_profile(req.session_id)
    
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
    
    # Check if this is first conversation
    history = get_conversation_history(req.session_id)
    is_first_message = len(history) <= 1  # Current message just saved, so <=1 means first real exchange
    
    returning_hint = ""
    if user_profile.get('is_returning') and user_profile.get('visit_count', 1) > 1:
        returning_hint = "\n(Note: This is a returning user - acknowledge their return warmly, no need for introduction)"
    elif is_first_message:
        returning_hint = "\n(Note: This is the FIRST message - you MUST introduce yourself as Sophia and ask about their origin/China direction)"
    else:
        returning_hint = "\n(Note: Continue conversation naturally, no need for introduction)"

    final_prompt = f"""User question: {req.message}{context}

Respond as Sophia, representing China West Connector.
Be specific about CWC services. Reference expertise naturally WITHOUT repeating the founder's name excessively.
{returning_hint}

INTELLIGENCE GATHERING PRIORITY:
If you don't know user's origin yet:
- Ask if they are Chinese company expanding abroad OR foreign company entering China
- This determines whether you discuss China opportunities vs. Africa/Middle East/LATAM/Europe opportunities

If user shows buying intent or complex needs, suggest clicking the "Speak with Michail" button above.
Keep response concise but authoritative (2-4 paragraphs max).

IMPORTANT: Never mention calendar links or clickable links - they do not exist. Only refer to the "Speak with Michail" button."""

    reply = ask_groq(final_prompt, req.session_id, user_profile)
    
    if any(word in user_msg for word in ["price", "cost", "fee", "how much", "start", "begin", "help me", "serious", "interested", "manufacturer", "supplier", "factory"]):
        if "consultation" not in reply.lower() and "button" not in reply.lower():
            reply += "\n\nTo discuss next steps, click the 'Speak with Michail' button above."
    
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
    
    # Update user profile with lead info
    update_user_profile(
        lead.session_id,
        name=lead.name,
        email=lead.email,
        company=lead.company,
        region_interest=lead.region
    )
    
    background_tasks.add_task(send_lead_notification, lead)
    
    return {"status": "success", "message": "Lead captured"}

@app.get("/leads")
def view_leads(password: str = None):
    """Simple lead dashboard - password protected"""
    if password != "CwC$x7Km9#Lp2QvN@2026!Md":
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

@app.get("/analytics")
def get_analytics(password: str = None, days: int = 7):
    """Get analytics data - password protected"""
    if password != "CwC$x7Km9#Lp2QvN@2026!Md":
        return {"error": "Unauthorized"}
    
    conn = sqlite3.connect('cwc_leads.db')
    c = conn.cursor()
    
    since_date = (datetime.now() - timedelta(days=days)).isoformat()
    
    # Unique users
    c.execute("SELECT COUNT(DISTINCT session_id) FROM conversations WHERE timestamp > ?", (since_date,))
    unique_users = c.fetchone()[0]
    
    # Total conversations
    c.execute("SELECT COUNT(*) FROM conversations WHERE timestamp > ?", (since_date,))
    total_conversations = c.fetchone()[0]
    
    # New leads
    c.execute("SELECT COUNT(*) FROM leads WHERE timestamp > ?", (since_date,))
    new_leads = c.fetchone()[0]
    
    # Returning users
    c.execute("SELECT COUNT(*) FROM user_profiles WHERE visit_count > 1 AND last_seen > ?", (since_date,))
    returning_users = c.fetchone()[0]
    
    # Top intents
    c.execute("""SELECT intent, COUNT(*) as count FROM conversations 
                 WHERE timestamp > ? GROUP BY intent ORDER BY count DESC LIMIT 5""", (since_date,))
    top_intents = [{"intent": r[0], "count": r[1]} for r in c.fetchall()]
    
    # Top regions
    c.execute("""SELECT region, COUNT(*) as count FROM conversations 
                 WHERE timestamp > ? AND region IS NOT NULL GROUP BY region ORDER BY count DESC LIMIT 5""", (since_date,))
    top_regions = [{"region": r[0], "count": r[1]} for r in c.fetchall()]
    
    # High score leads
    c.execute("""SELECT name, email, company, lead_score FROM user_profiles 
                 WHERE lead_score >= 50 ORDER BY lead_score DESC LIMIT 10""")
    hot_leads = [{"name": r[0], "email": r[1], "company": r[2], "score": r[3]} for r in c.fetchall() if r[0]]
    
    conn.close()
    
    return {
        "period_days": days,
        "unique_users": unique_users,
        "total_conversations": total_conversations,
        "new_leads": new_leads,
        "returning_users": returning_users,
        "top_intents": top_intents,
        "top_regions": top_regions,
        "hot_leads": hot_leads
    }

@app.get("/trigger-report")
def trigger_report(password: str = None):
    """Manually trigger weekly report - for testing"""
    if password != "CwC$x7Km9#Lp2QvN@2026!Md":
        return {"error": "Unauthorized"}
    
    try:
        send_weekly_report()
        return {"status": "Report sent successfully!", "sent_to": RECIPIENT_EMAIL}
    except Exception as e:
        return {"error": str(e)}

@app.get("/test-email")
def test_email(password: str = None):
    """Test email functionality"""
    if password != "CwC$x7Km9#Lp2QvN@2026!Md":
        return {"error": "Unauthorized"}
    
    success = send_email_brevo(
        to_email=RECIPIENT_EMAIL,
        subject="✅ CWC AI Email Test - Working!",
        body="Congratulations! Your Brevo email setup is working correctly.\n\nYou will receive weekly reports and lead notifications."
    )
    
    if success:
        return {"status": "Test email sent successfully!", "sent_to": RECIPIENT_EMAIL}
    else:
        return {"error": "Email failed to send"}