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
        days_until_monday = (7 - now.weekday()) % 7
        if days_until_monday == 0 and now.hour >= 9:
            days_until_monday = 7
        next_monday = now + timedelta(days=days_until_monday)
        next_monday = next_monday.replace(hour=9, minute=0, second=0, microsecond=0)
        seconds_until = (next_monday - now).total_seconds()
        
        await asyncio.sleep(seconds_until)
        
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
    
    week_ago = (datetime.now() - timedelta(days=7)).isoformat()
    
    c.execute("SELECT COUNT(DISTINCT session_id) FROM conversations WHERE timestamp > ?", (week_ago,))
    unique_users = c.fetchone()[0]
    
    c.execute("SELECT COUNT(*) FROM conversations WHERE timestamp > ?", (week_ago,))
    total_messages = c.fetchone()[0]
    
    c.execute("SELECT COUNT(*) FROM leads WHERE timestamp > ?", (week_ago,))
    new_leads = c.fetchone()[0]
    
    c.execute("SELECT COUNT(*) FROM user_profiles WHERE visit_count > 1 AND last_seen > ?", (week_ago,))
    returning_users = c.fetchone()[0]
    
    c.execute("""SELECT intent, COUNT(*) as count FROM conversations 
                 WHERE timestamp > ? GROUP BY intent ORDER BY count DESC LIMIT 5""", (week_ago,))
    top_intents = c.fetchall()
    
    c.execute("""SELECT region, COUNT(*) as count FROM conversations 
                 WHERE timestamp > ? AND region IS NOT NULL GROUP BY region ORDER BY count DESC LIMIT 5""", (week_ago,))
    top_regions = c.fetchall()
    
    c.execute("""SELECT name, email, company, region, timestamp FROM leads 
                 WHERE timestamp > ? ORDER BY timestamp DESC LIMIT 10""", (week_ago,))
    recent_leads = c.fetchall()
    
    c.execute("""SELECT name, email, company, lead_score FROM user_profiles 
                 WHERE lead_score >= 50 ORDER BY lead_score DESC LIMIT 5""")
    hot_leads = c.fetchall()
    
    conn.close()
    
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
    asyncio.create_task(schedule_weekly_report())
    yield
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

# NEW: Quick action button model
class QuickActionRequest(BaseModel):
    action: str  # "robotics" | "energy" | "biotech" | "shipping" | "verify" | "market_entry"
    session_id: str = "anonymous"

# ---- User Profile Functions ----
def get_or_create_user_profile(session_id: str) -> dict:
    conn = sqlite3.connect('cwc_leads.db')
    c = conn.cursor()
    
    c.execute("SELECT * FROM user_profiles WHERE session_id = ?", (session_id,))
    profile = c.fetchone()
    
    if profile:
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

# ---- Quick Action Button Opening Messages ----
# These are the scripted openers for each of the 6 Quick Action buttons.
# They are returned instantly (no LLM call needed) to start the conversation flow.
QUICK_ACTION_OPENERS = {
    "robotics": (
        "Great choice — China is currently the world's largest industrial robotics market, "
        "producing over 70% of global units.\n\n"
        "Before I connect you with the right intelligence, let me ask:\n\n"
        "Are you looking to **SOURCE** robotics technology from China for your business, "
        "or are you a Chinese robotics company seeking **Western partners or markets**?"
    ),
    "energy": (
        "Energy is one of the most dynamic China-West collaboration areas right now. "
        "China accounts for over 80% of global solar panel production and leads in battery storage technology.\n\n"
        "To point you in the right direction — what's your energy focus?\n\n"
        "① Solar PV — panels, inverters, mounting systems\n"
        "② Battery storage — utility-scale or commercial/industrial\n"
        "③ EV charging infrastructure\n"
        "④ Wind energy components\n"
        "⑤ Green hydrogen technology\n"
        "⑥ Energy trading or investment opportunities"
    ),
    "biotech": (
        "China's biotech sector is experiencing extraordinary growth — it is now the world's "
        "second-largest pharmaceutical market and a global leader in biosimilar manufacturing "
        "and genomic research.\n\n"
        "What brings you to the Biotech section?\n\n"
        "① Western pharma/biotech seeking Chinese manufacturing partners (CMO/CDMO)\n"
        "② Looking to license or access Chinese biotech innovations for Western markets\n"
        "③ Entering the Chinese healthcare/pharma market with a Western product\n"
        "④ Seeking R&D or clinical trial partnerships in China\n"
        "⑤ Medical devices (see also our Medical section)"
    ),
    "shipping": (
        "China handles over 30% of global container shipping volume — getting your logistics "
        "right is as important as finding the right supplier.\n\n"
        "What's your shipping challenge?\n\n"
        "① Moving goods FROM China to my country (import logistics)\n"
        "② Shipping products TO China (export logistics)\n"
        "③ Optimising an existing supply chain — reduce costs or lead times\n"
        "④ Customs clearance, documentation, or compliance\n"
        "⑤ Maritime technology partnerships with Chinese shipbuilders"
    ),
    "verify": (
        "Smart move — verifying a Chinese company before signing contracts or transferring "
        "funds is one of the most important steps in any China business engagement. "
        "CWC's Due Diligence service has protected clients from fraudulent suppliers, "
        "shell companies, and misrepresented certifications.\n\n"
        "What do you need to verify?\n\n"
        "① A Chinese supplier or manufacturer (before placing an order)\n"
        "② A Chinese business partner or JV candidate\n"
        "③ A Chinese investment target\n"
        "④ Certificates or documents a Chinese company has provided (business licence, ISO, CE, etc.)\n"
        "⑤ A Chinese individual's background and credentials"
    ),
    "market_entry": (
        "Market entry — whether into China or into Western markets using Chinese partnerships "
        "— is CWC's core expertise. We've guided companies from initial concept to operational "
        "presence in both directions.\n\n"
        "First, help me understand your direction:\n\n"
        "① We are a **Western company** looking to enter the Chinese market\n"
        "② We are a **Chinese company** looking to expand into Western markets\n"
        "③ We're considering both — bilateral partnership or trade\n"
        "④ We're not sure yet — we want to explore the opportunity"
    )
}

# ---- UPGRADED Groq AI with Agentic System Prompt ----
def ask_groq(prompt: str, session_id: str = "anonymous", user_profile: dict = None, quick_action: str = None) -> str:
    if not GROQ_API_KEY:
        return "System temporarily unavailable. Please contact the CWC team directly."

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
- Acknowledge their return warmly. No need for full introduction.
"""

    # Sector-specific intelligence injected when a Quick Action was triggered
    sector_context = ""
    if quick_action:
        sector_contexts = {
            "robotics": """
ACTIVE SECTOR: ROBOTICS
The user clicked the Robotics button. Your first priority is to determine:
- Are they WESTERN (sourcing Chinese robotics) or CHINESE (seeking Western markets)?
Key CWC services for this sector: supplier sourcing, factory audits, CE certification navigation, 
distribution partner matching, IP protection in supply agreements.
For Western buyers: highlight verified manufacturer network, quality benchmarking, on-site audit capability.
For Chinese sellers: highlight European distribution networks, CE/regulatory guidance, market entry strategy.
""",
            "energy": """
ACTIVE SECTOR: ENERGY
The user clicked the Energy button. Sub-sectors: solar PV, battery storage, EV infrastructure, wind, hydrogen, energy investment/trading.
Key CWC services: Tier-1 manufacturer sourcing, bankability assessment, logistics coordination, 
JV structuring for battery tech, government partnership access (Hainan FTP, Chengdu tech zones).
For solar: ask about project scale (MW) and geography — these determine manufacturer fit.
For battery/investment: ask about deal structure preference (equity, JV, offtake).
""",
            "biotech": """
ACTIVE SECTOR: BIOTECH
The user clicked the Biotech button. Sub-sectors: CMO/CDMO manufacturing, pharma market entry into China, 
biotech licensing, R&D partnerships, medical devices.
Key CWC services: CDMO shortlisting, tech transfer IP protection, NMPA registration strategy, 
distributor matching, Hainan FTP accelerated regulatory pathway access.
For CDMO: ask about molecule type, development stage, GMP requirements, batch volume.
For China market entry: explain NMPA pathway, NRDL inclusion strategy, JV vs. WFOE structure.
""",
            "shipping": """
ACTIVE SECTOR: SHIPPING & LOGISTICS
The user clicked the Shipping button. Sub-sectors: import/export freight, supply chain optimisation, 
customs compliance, maritime technology partnerships.
Key CWC services: China Logistics Simplified — vetted forwarder network, customs brokerage, 
Shenzhen-Greece corridor expertise, incoterms advisory, cost benchmarking (typically 10-25% savings identified).
For maritime tech: highlight Chinese shipyard partnership facilitation, IP disclosure protection strategy.
Ask about: shipping volume (FCL/LCL/air), frequency, current pain points.
""",
            "verify": """
ACTIVE SECTOR: DUE DILIGENCE / VERIFY
The user clicked the Verify button. This is URGENT territory — they likely have a real decision pending.
Key CWC services: SAMR business registration verification, factory existence confirmation, 
manufacturing capability assessment, export history check, certificate authentication (ISO, CE, business licence).
Standard turnaround: 5-7 business days for full report. Certificate verification: 48 hours.
URGENCY RULE: If they mention a large deposit or imminent payment — flag as HIGH PRIORITY and push 
immediately toward collecting company name, location, and their email for same-day team response.
Always ask: company name, location, what they were offered/shown.
""",
            "market_entry": """
ACTIVE SECTOR: MARKET ENTRY
The user clicked the Market Entry button. This is the highest-value, most strategic flow.
First determine direction: Western into China, or Chinese expanding West.

FOR WESTERN INTO CHINA:
- Ask: product/service category, B2B or B2C, timeline, and primary goal (sales, sourcing, JV, or manufacturing base)
- Deliver a phased roadmap: regulatory → partner selection → launch
- Highlight: government partnerships (Chengdu, Hainan FTP), CWC's on-ground liaison network

FOR CHINESE INTO WEST:
- Ask: technology/product type, target countries, deal structure preference (distribution, JV, direct entity, M&A)
- Highlight: regulatory navigation (EU AI Act, GDPR, CE), reputation strategy for Chinese brands, 
  CWC networks in Europe, Africa, Middle East, LATAM, Central Asia
- Be candid about regulatory complexity where relevant (e.g., surveillance tech in EU)

Always end with a tailored roadmap summary and CTA to speak with Michail.
"""
        }
        sector_context = sector_contexts.get(quick_action, "")

    system_prompt = f"""
You are Sophia — the official AI Intelligence assistant for China West Connector (CWC).

CURRENT DATE: February 2026

USER INTENT DETECTED: {intent_data['primary']}
REGION DETECTED: {intent_data['region'] or 'Unknown'}
ACTIVE QUICK ACTION: {quick_action or 'None — organic conversation'}
{returning_context}
{sector_context}

CONVERSATION HISTORY: {context}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
YOUR CORE MISSION — AGENTIC BEHAVIOUR
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
You are NOT a passive Q&A bot. You are an active business intelligence advisor.
Your job is to:
1. QUALIFY the user (understand their direction, industry, goal, and urgency)
2. PERSONALISE your response to their specific situation
3. RECOMMEND the most relevant CWC service with a clear reason why
4. END every response with a concrete next step — never a dead end

QUALIFICATION PRIORITY (if not yet known, always ask first):
① Are they Western (looking into China) or Chinese (expanding West)?
② What industry/sector?
③ What is their specific goal — sourcing, investment, legal, partnerships, market entry?
④ What is their urgency/timeline?

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ABOUT CWC
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
China West Connector is a premier strategic bridge between Chinese and Western businesses.
Founded by Michail Digkas — international business lawyer with 10+ years of direct China experience.
Part of G.P.A. ecosystem: 147+ years combined experience, 2,700+ active projects, 50+ countries.

Government partnerships: Sichuan International Technical Transfer Center, Chengdu AI Association, 
Tianfu International Technology Transformation Center, Hainan Free Trade Port, CISTEA.

CWC CORE SERVICES:
1. LEGAL SERVICES — bilingual contract drafting, IP protection, dispute resolution
2. DUE DILIGENCE — factory audits, supplier verification, SAMR registration checks, certificate authentication
3. B2B PARTNERSHIPS — strategic partner matching, JV structuring, negotiations
4. FDI CONSULTING — market entry strategy, local incentives, entity setup, compliance
5. CHINA LOGISTICS SIMPLIFIED — vetted freight forwarder network, customs brokerage, supply chain optimisation
6. LIAISON & REPRESENTATION — on-ground China representation, government navigation, bilingual communication

CWC REGIONAL NETWORKS:
Western companies entering China | Chinese companies expanding to:
Europe • Africa • Middle East • Latin America • Central Asia • North America

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CRITICAL DIRECTIONAL INTELLIGENCE RULE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
NEVER assume direction without asking.

IF USER IS WESTERN / FOREIGN:
- China is the destination market or sourcing hub
- Focus on: market entry, supplier sourcing, due diligence, legal compliance, China logistics
- Frame: "How can we help you navigate China?"

IF USER IS CHINESE:
- Africa, Middle East, LATAM, Europe, Central Asia are expansion targets
- Focus on: partner matching, regulatory navigation in target market, reputation strategy, entity setup abroad
- Frame: "Which Western market are you targeting, and what structure are you considering?"
- If user writes in Chinese: respond in Chinese, flag internally as high-priority Chinese company lead

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RESPONSE STRATEGY BY INTENT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
high_intent_lead → Ask 1-2 qualifying questions, then recommend specific CWC service + push to Michail button
consultation_request → Validate their request, confirm CWC can help, direct to "Speak with Michail" button
supplier_verification → Treat as URGENT. Ask for company name + what's at risk. Push to team contact immediately
information_gathering → Provide real value (specific, credible insight) then offer a deeper consultation
returning_user → Acknowledge warmly, reference their previous topic if known, advance the conversation

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STYLE & FORMAT RULES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- Maximum 180 words per response (150 preferred)
- 2-3 short paragraphs OR a short paragraph + a numbered/bulleted list
- Professional, confident, commercially sharp tone
- No hype. No buzzwords. No exaggerated claims.
- Never sound like marketing copy — sound like a senior advisor
- Use specific numbers and facts where relevant (adds credibility)
- Adapt language register: formal for legal/finance queries, warmer for exploratory conversations

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CONSULTATION & ESCALATION RULES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- NEVER mention calendar links, clickable links, or external URLs — they do not exist in this widget
- When suggesting a consultation, ONLY say: "To arrange a consultation, click the 'Speak with Michail' button above."
- Push toward Michail when: user shows serious intent, asks about pricing, mentions a specific deal, 
  or their need is too complex for the bot to resolve alone
- For URGENT due diligence cases (large deposit at risk, imminent payment): escalate immediately every time

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FIRST MESSAGE PROTOCOL (no history, no quick action)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
If this is the very first message and no quick action was triggered, introduce as:
"Hello! I'm Sophia, CWC's AI intelligence advisor. I'm here to help you navigate cross-border 
business opportunities with China.

To point you in the right direction — are you currently based in China looking to expand internationally, 
or are you looking to enter or source from the Chinese market?"

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SAFETY & CREDIBILITY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- No unrealistic promises or guaranteed outcomes
- If asked about regulatory matters (EU AI Act, GDPR, NMPA, etc.) — give accurate general guidance, 
  then recommend a proper legal consultation
- Stay grounded, specific, and honest
- Never invent facts, contacts, or capabilities CWC does not have
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
        return "I apologise, but I'm having trouble connecting right now. Please reach out to the CWC team directly."

# ---- Email Notification ----
def send_lead_notification(lead: LeadCapture):
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
        "tavily_configured": bool(TAVILY_API_KEY),
        "brevo_configured": bool(BREVO_API_KEY)
    }

@app.get("/")
def root():
    return {"message": "CWC AI backend running"}

@app.post("/chat")
def chat(req: ChatRequest):
    user_msg = req.message.lower()
    
    user_profile = get_or_create_user_profile(req.session_id)
    
    if any(word in user_msg for word in ["stop", "shorter", "brief", "short", "too long"]):
        return {"response": "Got it — I'll keep answers brief. What would you like to know about China business opportunities?"}
    
    consultation_keywords = ["book", "consultation", "call", "schedule", "meet", "contact", "michail", "digkas"]
    is_consultation_request = any(kw in user_msg for kw in consultation_keywords)
    
    live_data = ""
    if not is_consultation_request:
        live_data = search_web(req.message)
    
    context = ""
    if live_data:
        context = f"\n\nRelevant market data:\n{live_data}\n"
    
    history = get_conversation_history(req.session_id)
    is_first_message = len(history) <= 1

    returning_hint = ""
    if user_profile.get('is_returning') and user_profile.get('visit_count', 1) > 1:
        returning_hint = "\n(RETURNING USER — acknowledge their return warmly, no need for introduction)"
    elif is_first_message:
        returning_hint = "\n(FIRST MESSAGE — introduce yourself as Sophia and ask about their direction: Western into China, or Chinese expanding West)"
    else:
        returning_hint = "\n(ONGOING CONVERSATION — continue naturally, no introduction needed)"

    final_prompt = f"""User question: {req.message}{context}

{returning_hint}

Respond as Sophia, CWC's AI intelligence advisor.
Be specific and commercially sharp. Reference CWC services naturally where relevant.
If user shows buying intent or has a complex need, suggest clicking the "Speak with Michail" button above.
Keep response concise and authoritative (150 words preferred, 180 max).

IMPORTANT: Never mention calendar links or clickable links — they do not exist. 
Only refer to the "Speak with Michail" button when escalating."""

    reply = ask_groq(final_prompt, req.session_id, user_profile)
    
    # Auto-append CTA for high-intent keywords if not already present
    high_intent_words = ["price", "cost", "fee", "how much", "start", "begin", "help me", 
                         "serious", "interested", "manufacturer", "supplier", "factory", "invest"]
    if any(word in user_msg for word in high_intent_words):
        if "consultation" not in reply.lower() and "button" not in reply.lower():
            reply += "\n\nTo discuss next steps, click the 'Speak with Michail' button above."
    
    return {"response": reply}


# ---- NEW: Quick Action Button Endpoint ----
@app.post("/quick-action")
def quick_action(req: QuickActionRequest):
    """
    Called when a user clicks one of the 6 Quick Action buttons.
    Returns the scripted opening message for that sector instantly,
    then primes the AI with sector-specific context for follow-up messages.
    
    Valid actions: robotics | energy | biotech | shipping | verify | market_entry
    
    Frontend usage:
    POST /quick-action  { "action": "robotics", "session_id": "abc123" }
    → Returns { "response": "...", "action": "robotics" }
    
    Then all subsequent /chat calls from this session will carry sector context
    because the opening message is saved to conversation history.
    """
    action = req.action.lower().strip()
    
    if action not in QUICK_ACTION_OPENERS:
        return {
            "response": "Hello! I'm Sophia, CWC's AI intelligence advisor. How can I help you with China-West business today?",
            "action": "general"
        }
    
    opening_message = QUICK_ACTION_OPENERS[action]
    
    # Save this as the opening bot message in conversation history
    # so follow-up /chat calls have full context of what sector was triggered
    save_conversation(
        session_id=req.session_id,
        user_msg=f"[Quick Action: {action}]",
        ai_response=opening_message,
        intent=action
    )
    
    # Update user profile with sector interest
    update_user_profile(
        req.session_id,
        last_intent=action,
        topics_discussed=action
    )
    
    return {
        "response": opening_message,
        "action": action
    }


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
    if password != "CwC$x7Km9#Lp2QvN@2026!Md":
        return {"error": "Unauthorized"}
    
    conn = sqlite3.connect('cwc_leads.db')
    c = conn.cursor()
    
    since_date = (datetime.now() - timedelta(days=days)).isoformat()
    
    c.execute("SELECT COUNT(DISTINCT session_id) FROM conversations WHERE timestamp > ?", (since_date,))
    unique_users = c.fetchone()[0]
    
    c.execute("SELECT COUNT(*) FROM conversations WHERE timestamp > ?", (since_date,))
    total_conversations = c.fetchone()[0]
    
    c.execute("SELECT COUNT(*) FROM leads WHERE timestamp > ?", (since_date,))
    new_leads = c.fetchone()[0]
    
    c.execute("SELECT COUNT(*) FROM user_profiles WHERE visit_count > 1 AND last_seen > ?", (since_date,))
    returning_users = c.fetchone()[0]
    
    c.execute("""SELECT intent, COUNT(*) as count FROM conversations 
                 WHERE timestamp > ? GROUP BY intent ORDER BY count DESC LIMIT 5""", (since_date,))
    top_intents = [{"intent": r[0], "count": r[1]} for r in c.fetchall()]
    
    c.execute("""SELECT region, COUNT(*) as count FROM conversations 
                 WHERE timestamp > ? AND region IS NOT NULL GROUP BY region ORDER BY count DESC LIMIT 5""", (since_date,))
    top_regions = [{"region": r[0], "count": r[1]} for r in c.fetchall()]
    
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
    if password != "CwC$x7Km9#Lp2QvN@2026!Md":
        return {"error": "Unauthorized"}
    
    try:
        send_weekly_report()
        return {"status": "Report sent successfully!", "sent_to": RECIPIENT_EMAIL}
    except Exception as e:
        return {"error": str(e)}

@app.get("/test-email")
def test_email(password: str = None):
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
