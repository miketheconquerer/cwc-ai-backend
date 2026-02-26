from fastapi import FastAPI, BackgroundTasks, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
import requests
import os
import sqlite3
import json
import time
from collections import defaultdict
from datetime import datetime, timedelta
from dotenv import load_dotenv
import asyncio
from contextlib import asynccontextmanager

load_dotenv()

# ============================================================
# CONFIGURATION
# ============================================================
BREVO_API_KEY    = os.getenv("BREVO_API_KEY", "")
GROQ_API_KEY     = os.getenv("GROQ_API_KEY")
TAVILY_API_KEY   = os.getenv("TAVILY_API_KEY")
ADMIN_PASSWORD   = os.getenv("ADMIN_PASSWORD", "")
SENDER_EMAIL     = "888nv666@gmail.com"
RECIPIENT_EMAIL  = "digkasm@proton.me"

# ============================================================
# UPGRADE 6 — RATE LIMITING (in-memory, free)
# ============================================================
_rate_store: dict = defaultdict(list)   # ip -> [timestamps]
RATE_LIMIT_REQUESTS = 30                # max requests
RATE_LIMIT_WINDOW   = 60               # per 60 seconds

def is_rate_limited(ip: str) -> bool:
    now = time.time()
    window_start = now - RATE_LIMIT_WINDOW
    _rate_store[ip] = [t for t in _rate_store[ip] if t > window_start]
    if len(_rate_store[ip]) >= RATE_LIMIT_REQUESTS:
        return True
    _rate_store[ip].append(now)
    return False

# ============================================================
# SCHEDULER
# ============================================================
scheduler_running = False

async def schedule_weekly_report():
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

@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(schedule_weekly_report())
    yield
    global scheduler_running
    scheduler_running = False

app = FastAPI(
    lifespan=lifespan,
    title="CWC Sophia AI — China-West Business Intelligence",
    description="Sophia is CWC's agentic AI advisor for China-West cross-border business. "
                "Query her for market intelligence, supplier verification, FDI strategy, and more.",
    version="2.0.0",
)

# ============================================================
# CORS
# ============================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://www.chinawestconnector.com",
        "https://chinawestconnector.com",
        "http://localhost:8000",
        "http://localhost:3000",
        "https://localhost",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================
# DATABASE SETUP
# ============================================================
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

# ============================================================
# PYDANTIC MODELS
# ============================================================
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

class QuickActionRequest(BaseModel):
    action: str
    session_id: str = "anonymous"

# ============================================================
# USER PROFILE FUNCTIONS
# ============================================================
def get_or_create_user_profile(session_id: str, new_session: bool = False) -> dict:
    conn = sqlite3.connect('cwc_leads.db')
    c = conn.cursor()
    c.execute("SELECT * FROM user_profiles WHERE session_id = ?", (session_id,))
    profile = c.fetchone()

    if profile:
        if new_session:
            c.execute("""UPDATE user_profiles
                         SET last_seen = ?, visit_count = visit_count + 1
                         WHERE session_id = ?""",
                      (datetime.now(), session_id))
        else:
            c.execute("UPDATE user_profiles SET last_seen = ? WHERE session_id = ?",
                      (datetime.now(), session_id))
        conn.commit()
        user_profile = {
            "session_id": profile[1], "first_seen": profile[2],
            "last_seen": profile[3],
            "visit_count": profile[4] + (1 if new_session else 0),
            "name": profile[5], "email": profile[6], "company": profile[7],
            "region_interest": profile[8], "topics_discussed": profile[9],
            "lead_score": profile[10], "last_intent": profile[11],
            "is_returning": True
        }
    else:
        c.execute("""INSERT INTO user_profiles
                     (session_id, first_seen, last_seen, visit_count)
                     VALUES (?, ?, ?, 1)""",
                  (session_id, datetime.now(), datetime.now()))
        conn.commit()
        user_profile = {
            "session_id": session_id, "first_seen": datetime.now(),
            "last_seen": datetime.now(), "visit_count": 1,
            "name": None, "email": None, "company": None,
            "region_interest": None, "topics_discussed": None,
            "lead_score": 0, "last_intent": None, "is_returning": False
        }
    conn.close()
    return user_profile


def update_user_profile(session_id: str, **kwargs):
    conn = sqlite3.connect('cwc_leads.db')
    c = conn.cursor()
    valid_fields = ['name', 'email', 'company', 'region_interest',
                    'topics_discussed', 'lead_score', 'last_intent']
    updates, values = [], []
    for key, value in kwargs.items():
        if key in valid_fields and value:
            updates.append(f"{key} = ?")
            values.append(value)
    if updates:
        values.append(session_id)
        c.execute(f"UPDATE user_profiles SET {', '.join(updates)} WHERE session_id = ?", values)
        conn.commit()
    conn.close()


def calculate_lead_score(user_profile: dict, message: str, intent: str) -> int:
    score = user_profile.get('lead_score', 0)
    intent_scores = {
        "high_intent_lead": 30, "consultation_request": 25,
        "supplier_verification": 20, "information_gathering": 5
    }
    score += intent_scores.get(intent, 0)
    if user_profile.get('visit_count', 1) > 1:
        score += 10
    high_value_keywords = ["budget", "invest", "contract", "serious", "start", "hire", "price"]
    if any(kw in message.lower() for kw in high_value_keywords):
        score += 15
    return min(score, 100)

# ============================================================
# DATABASE FUNCTIONS
# ============================================================
def save_conversation(session_id, user_msg, ai_response,
                      email=None, company=None, region=None, intent=None):
    conn = sqlite3.connect('cwc_leads.db')
    c = conn.cursor()
    c.execute("""INSERT INTO conversations
                 (session_id, user_message, ai_response, timestamp, email, company, region, intent)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
              (session_id, user_msg, ai_response, datetime.now(),
               email, company, region, intent))
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

# ============================================================
# INTENT DETECTION
# ============================================================
def detect_intent(message: str) -> dict:
    msg_lower = message.lower()
    intents = {
        "high_intent_lead":     ["price", "cost", "quote", "proposal", "start", "begin",
                                  "hire", "contract", "serious", "budget", "invest"],
        "consultation_request": ["book", "consultation", "call", "schedule", "meet",
                                  "contact", "talk", "discuss"],
        "information_gathering":["how", "what", "tell me", "explain", "information"],
        "supplier_verification":["verify", "check", "audit", "due diligence",
                                  "factory", "supplier", "manufacturer"],
        "regional_interest": {
            "africa":       ["africa", "african", "mining", "infrastructure"],
            "middle_east":  ["middle east", "mea", "gcc", "dubai", "saudi", "energy", "oil", "gas"],
            "latam":        ["latam", "latin america", "brazil", "mexico", "argentina", "chile", "lithium"],
            "europe":       ["europe", "eu", "germany", "france", "green tech", "automotive"],
            "central_asia": ["central asia", "kazakhstan", "uzbekistan", "belt and road", "bri"],
            "china":        ["china", "chinese", "mainland", "prc", "shenzhen",
                             "shanghai", "beijing", "guangzhou"]
        }
    }
    detected = {"primary": "general", "region": None, "score": 0}
    if any(kw in msg_lower for kw in intents["high_intent_lead"]):
        detected.update({"primary": "high_intent_lead", "score": 90})
    elif any(kw in msg_lower for kw in intents["consultation_request"]):
        detected.update({"primary": "consultation_request", "score": 85})
    elif any(kw in msg_lower for kw in intents["supplier_verification"]):
        detected.update({"primary": "supplier_verification", "score": 80})
    for region, keywords in intents["regional_interest"].items():
        if any(kw in msg_lower for kw in keywords):
            detected["region"] = region
            break
    return detected

# ============================================================
# UPGRADE 1 — MULTI-TOOL SEARCH CASCADE (Free + Tavily)
# ============================================================
def search_duckduckgo(query: str) -> tuple[str, list[str]]:
    """Free search via DuckDuckGo Instant Answer API — no key required."""
    try:
        url = "https://api.duckduckgo.com/"
        params = {"q": query, "format": "json", "no_html": "1", "skip_disambig": "1"}
        res = requests.get(url, params=params, timeout=8)
        data = res.json()
        abstract = data.get("AbstractText", "")
        related = [r.get("Text", "") for r in data.get("RelatedTopics", [])[:3] if r.get("Text")]
        sources = [data.get("AbstractURL", "DuckDuckGo")] if abstract else []
        combined = abstract
        if related:
            combined += "\n" + "\n".join(related)
        return combined.strip(), sources
    except Exception as e:
        print(f"DuckDuckGo search error: {e}")
        return "", []


def search_wikipedia(query: str) -> tuple[str, list[str]]:
    """Free Wikipedia API search — great for background context."""
    try:
        url = "https://en.wikipedia.org/api/rest_v1/page/summary/" + query.replace(" ", "_")
        res = requests.get(url, timeout=8)
        if res.status_code == 200:
            data = res.json()
            extract = data.get("extract", "")
            page_url = data.get("content_urls", {}).get("desktop", {}).get("page", "Wikipedia")
            if extract:
                return extract[:600], [page_url]
        return "", []
    except Exception as e:
        print(f"Wikipedia search error: {e}")
        return "", []


def search_tavily(query: str) -> tuple[str, list[str]]:
    """Tavily search — best for live news and current market data."""
    if not TAVILY_API_KEY:
        return "", []
    current_year = datetime.now().year
    news_keywords = ["news", "latest", "update", "today", "recent", "announced"]
    is_news = any(kw in query.lower() for kw in news_keywords)
    enhanced = (f"{query} China business trade investment {current_year} latest news"
                if is_news else
                f"{query} China business {current_year} latest trends")
    try:
        res = requests.post(
            "https://api.tavily.com/search",
            json={"api_key": TAVILY_API_KEY, "query": enhanced,
                  "max_results": 3, "search_depth": "advanced", "include_answer": True},
            timeout=10
        )
        res.raise_for_status()
        data = res.json()
        answer = data.get("answer", "")
        results = data.get("results", [])
        sources = [r.get("url", "") for r in results if r.get("url")]
        content = answer + "\n" + "\n".join(r.get("content", "") for r in results[:2])
        return content.strip(), sources
    except Exception as e:
        print(f"Tavily search error: {e}")
        return "", []


def search_web(query: str) -> tuple[str, list[str]]:
    """
    UPGRADE 1: Cascading multi-tool search.
    1. Try Tavily first (best for live news)
    2. Fall back to DuckDuckGo (always free)
    3. Supplement with Wikipedia for background context
    Returns (content, sources_list)
    """
    all_content = []
    all_sources = []

    # Primary: Tavily
    tavily_content, tavily_sources = search_tavily(query)
    if tavily_content:
        all_content.append(tavily_content)
        all_sources.extend(tavily_sources)

    # Fallback: DuckDuckGo (always runs if Tavily gives nothing useful)
    if not tavily_content:
        ddg_content, ddg_sources = search_duckduckgo(query)
        if ddg_content:
            all_content.append(ddg_content)
            all_sources.extend(ddg_sources)

    # Supplement: Wikipedia for factual background
    wiki_content, wiki_sources = search_wikipedia(query)
    if wiki_content and len("\n".join(all_content)) < 400:
        all_content.append(f"Background: {wiki_content}")
        all_sources.extend(wiki_sources)

    combined = "\n\n".join(all_content)
    unique_sources = list(dict.fromkeys(s for s in all_sources if s))[:4]
    return combined, unique_sources

# ============================================================
# QUICK ACTION OPENERS
# ============================================================
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
        "China accounts for over 80% of global solar panel production and leads in battery storage.\n\n"
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
        "second-largest pharmaceutical market and a global leader in biosimilar manufacturing.\n\n"
        "What brings you to the Biotech section?\n\n"
        "① Western pharma/biotech seeking Chinese manufacturing partners (CMO/CDMO)\n"
        "② Looking to license or access Chinese biotech innovations for Western markets\n"
        "③ Entering the Chinese healthcare/pharma market with a Western product\n"
        "④ Seeking R&D or clinical trial partnerships in China\n"
        "⑤ Medical devices"
    ),
    "shipping": (
        "China handles over 30% of global container shipping volume.\n\n"
        "What's your shipping challenge?\n\n"
        "① Moving goods FROM China to my country (import logistics)\n"
        "② Shipping products TO China (export logistics)\n"
        "③ Optimising an existing supply chain — reduce costs or lead times\n"
        "④ Customs clearance, documentation, or compliance\n"
        "⑤ Maritime technology partnerships with Chinese shipbuilders"
    ),
    "verify": (
        "Smart move — verifying a Chinese company before signing contracts or transferring "
        "funds is one of the most important steps in any China business engagement.\n\n"
        "What do you need to verify?\n\n"
        "① A Chinese supplier or manufacturer (before placing an order)\n"
        "② A Chinese business partner or JV candidate\n"
        "③ A Chinese investment target\n"
        "④ Certificates or documents a Chinese company has provided\n"
        "⑤ A Chinese individual's background and credentials"
    ),
    "market_entry": (
        "Market entry is CWC's core expertise. We've guided companies from initial concept "
        "to operational presence in both directions.\n\n"
        "First, help me understand your direction:\n\n"
        "① We are a **Western company** looking to enter the Chinese market\n"
        "② We are a **Chinese company** looking to expand into Western markets\n"
        "③ We're considering both — bilateral partnership or trade\n"
        "④ We're not sure yet — we want to explore the opportunity"
    )
}

# ============================================================
# UPGRADE 3 — TOOL-USE / FUNCTION CALLING via Groq
# ============================================================
GROQ_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "search_market_intelligence",
            "description": (
                "Search for live China business intelligence, market data, trade news, "
                "regulatory updates, or company information. Call this when the user asks "
                "about current events, market conditions, specific companies, regulations, "
                "or any information that requires up-to-date data."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The specific search query to look up"
                    },
                    "search_type": {
                        "type": "string",
                        "enum": ["market_news", "company_lookup", "regulation", "general"],
                        "description": "Type of search to optimise results"
                    }
                },
                "required": ["query", "search_type"]
            }
        }
    }
]


def run_tool_call(tool_name: str, tool_args: dict) -> tuple[str, list[str]]:
    """Execute a tool called by the LLM and return (result, sources)."""
    if tool_name == "search_market_intelligence":
        query = tool_args.get("query", "")
        search_type = tool_args.get("search_type", "general")
        # Enrich query based on type
        if search_type == "market_news":
            query = f"{query} latest news 2025 2026"
        elif search_type == "company_lookup":
            query = f"{query} China company profile business registration"
        elif search_type == "regulation":
            query = f"{query} China regulation compliance 2026"
        return search_web(query)
    return "", []

# ============================================================
# UPGRADE 2 — GROQ WITH REAL CONVERSATION MEMORY + TOOL USE
# ============================================================
def ask_groq(prompt: str, session_id: str = "anonymous",
             user_profile: dict = None, quick_action: str = None) -> tuple[str, list[str]]:
    if not GROQ_API_KEY:
        return "System temporarily unavailable. Please contact the CWC team directly.", []

    # UPGRADE 2: Build real message history array (not just text injection)
    raw_history = get_conversation_history(session_id, limit=8)
    messages = []
    for user_msg, ai_resp in raw_history:
        messages.append({"role": "user",    "content": user_msg})
        messages.append({"role": "assistant","content": ai_resp})
    messages.append({"role": "user", "content": prompt})

    intent_data = detect_intent(prompt)

    returning_context = ""
    if user_profile and user_profile.get('is_returning'):
        name = user_profile.get('name', 'Unknown')
        returning_context = f"""
RETURNING USER DETECTED:
- Visit count: {user_profile.get('visit_count', 1)}
- Last visit intent: {user_profile.get('last_intent', '')}
- Region of interest: {user_profile.get('region_interest', '')}
- Known name: {name if name else 'Unknown'}
"""

    sector_context = ""
    if quick_action:
        sector_map = {
            "robotics": "ACTIVE SECTOR: ROBOTICS — Determine if Western (sourcing) or Chinese (expansion). "
                        "Services: supplier sourcing, factory audits, CE certification, partner matching, IP protection.",
            "energy": "ACTIVE SECTOR: ENERGY — Sub-sectors: solar PV, battery storage, EV, wind, hydrogen, investment. "
                      "Ask about project scale (MW) and deal structure.",
            "biotech": "ACTIVE SECTOR: BIOTECH — CMO/CDMO, pharma market entry, licensing, R&D, medical devices. "
                       "Ask molecule type, stage, GMP requirements.",
            "shipping": "ACTIVE SECTOR: SHIPPING — Import/export freight, supply chain, customs, maritime tech. "
                        "Ask volume (FCL/LCL/air) and pain points.",
            "verify": "ACTIVE SECTOR: DUE DILIGENCE — URGENT. Ask: company name, location, what was offered. "
                      "If large deposit at risk: escalate immediately.",
            "market_entry": "ACTIVE SECTOR: MARKET ENTRY — Highest value flow. Determine direction first. "
                            "Western into China OR Chinese into West. Deliver phased roadmap.",
        }
        sector_context = sector_map.get(quick_action, "")

    system_prompt = f"""You are Sophia — the official AI Intelligence advisor for China West Connector (CWC).

CURRENT DATE: {datetime.now().strftime('%B %Y')}
USER INTENT: {intent_data['primary']}
REGION DETECTED: {intent_data['region'] or 'Unknown'}
QUICK ACTION: {quick_action or 'None'}
{returning_context}
{sector_context}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
YOUR CORE MISSION — AGENTIC BEHAVIOUR
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
You are NOT a passive Q&A bot. You are an active business intelligence advisor.
1. QUALIFY the user (direction, industry, goal, urgency)
2. PERSONALISE your response to their situation
3. RECOMMEND the most relevant CWC service
4. END every response with a concrete next step

QUALIFICATION PRIORITY:
① Western (looking into China) or Chinese (expanding West)?
② Industry/sector?
③ Goal — sourcing, investment, legal, partnerships, market entry?
④ Urgency/timeline?

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ABOUT CWC
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
China West Connector bridges Chinese and Western businesses.
Founded by Michail Digkas — international business lawyer, 10+ years China experience.
CWC is a proud member of the G.P.A. Group — a global professional network.
The G.P.A. Group's COLLECTIVE track record includes 147+ years of combined experience,
2,700+ active projects, and operations in 50+ countries.
CRITICAL RULE: These figures represent the G.P.A. Group as a whole — NOT CWC alone.
NEVER say or imply that CWC has completed 2,700 projects by itself.
When referencing this, ALWAYS say: "CWC is part of the G.P.A. Group, which collectively has..."
Government partnerships: Sichuan Tech Transfer Center, Chengdu AI Association,
Tianfu Technology Center, Hainan Free Trade Port, CISTEA.

CORE SERVICES:
1. LEGAL — bilingual contracts, IP protection, dispute resolution
2. DUE DILIGENCE — factory audits, supplier verification, SAMR checks, certificate auth
3. B2B PARTNERSHIPS — partner matching, JV structuring, negotiations
4. FDI CONSULTING — market entry, incentives, entity setup, compliance
5. LOGISTICS — vetted freight forwarder network, customs brokerage, supply chain
6. LIAISON — on-ground China representation, government navigation

REGIONS: Europe • Africa • Middle East • Latin America • Central Asia • North America

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DIRECTIONAL INTELLIGENCE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
NEVER assume direction. Always ask if unknown.
WESTERN user → China is destination/sourcing hub
CHINESE user → Western markets are expansion targets; if writing in Chinese, respond in Chinese

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RESPONSE STRATEGY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
high_intent_lead      → Ask 1-2 qualifying questions + push to Michail button
consultation_request  → Confirm CWC can help + direct to "Speak with Michail" button
supplier_verification → URGENT. Ask company name + risk amount. Escalate immediately
information_gathering → Provide real insight then offer deeper consultation

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STYLE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- Max 180 words (150 preferred)
- Professional, confident, commercially sharp
- No hype, no buzzwords — sound like a senior advisor
- Use specific numbers where relevant
- NEVER mention calendar links or external URLs
- When escalating: "click the 'Speak with Michail' button above"
- For URGENT due diligence (large deposit at risk): escalate every time

FIRST MESSAGE (no history): Introduce as Sophia, ask direction (Western→China or Chinese→West)
"""

    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    all_sources = []

    # UPGRADE 3: First call WITH tools — let Sophia decide if she needs to search
    try:
        data = {
            "model": "llama-3.3-70b-versatile",
            "messages": [{"role": "system", "content": system_prompt}] + messages,
            "tools": GROQ_TOOLS,
            "tool_choice": "auto",
            "temperature": 0.3,
            "max_tokens": 800
        }
        res = requests.post(url, headers=headers, json=data, timeout=20)
        res.raise_for_status()
        content = res.json()
        choice = content["choices"][0]
        message_obj = choice["message"]

        # If the model decided to call a tool
        if choice.get("finish_reason") == "tool_calls" and message_obj.get("tool_calls"):
            tool_results_messages = list(messages)  # copy history
            tool_results_messages.insert(0, {"role": "system", "content": system_prompt})
            tool_results_messages.append(message_obj)  # assistant message with tool_calls

            for tool_call in message_obj["tool_calls"]:
                fn_name = tool_call["function"]["name"]
                fn_args = json.loads(tool_call["function"]["arguments"])
                tool_result, sources = run_tool_call(fn_name, fn_args)
                all_sources.extend(sources)

                tool_results_messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call["id"],
                    "content": tool_result or "No results found for this query."
                })

            # Second call — Sophia now has live data, generate final response
            data2 = {
                "model": "llama-3.3-70b-versatile",
                "messages": tool_results_messages,
                "temperature": 0.3,
                "max_tokens": 800
            }
            res2 = requests.post(url, headers=headers, json=data2, timeout=20)
            res2.raise_for_status()
            response_text = res2.json()["choices"][0]["message"]["content"]

        else:
            # Model answered directly without needing a search
            response_text = message_obj.get("content", "")

        # Update profile & save conversation
        new_score = calculate_lead_score(user_profile or {}, prompt, intent_data['primary'])
        update_user_profile(session_id,
                            last_intent=intent_data['primary'],
                            region_interest=intent_data['region'],
                            lead_score=new_score)
        save_conversation(session_id, prompt, response_text,
                          region=intent_data['region'],
                          intent=intent_data['primary'])

        return response_text, all_sources

    except Exception as e:
        print(f"Groq error: {e}")
        return ("I apologise, but I'm having trouble connecting right now. "
                "Please reach out to the CWC team directly."), []

# ============================================================
# EMAIL FUNCTIONS
# ============================================================
def send_email_brevo(to_email: str, subject: str, body: str,
                     from_name: str = "CWC AI") -> bool:
    url = "https://api.brevo.com/v3/smtp/email"
    headers = {"accept": "application/json", "content-type": "application/json",
               "api-key": BREVO_API_KEY}
    payload = {
        "sender": {"name": from_name, "email": SENDER_EMAIL},
        "to": [{"email": to_email, "name": "Michail Digkas"}],
        "subject": subject,
        "htmlContent": f"<html><body><pre style='font-family:monospace;white-space:pre-wrap;'>{body}</pre></body></html>",
        "textContent": body
    }
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        if response.status_code == 201:
            print(f"✅ Email sent to {to_email}")
            return True
        print(f"❌ Brevo error: {response.status_code} - {response.text}")
        return False
    except Exception as e:
        print(f"❌ Email failed: {e}")
        return False


def send_lead_notification(lead: LeadCapture):
    conn = sqlite3.connect('cwc_leads.db')
    c = conn.cursor()
    c.execute("SELECT lead_score, visit_count FROM user_profiles WHERE session_id = ?",
              (lead.session_id,))
    profile = c.fetchone()
    conn.close()
    lead_score  = profile[0] if profile else 0
    visit_count = profile[1] if profile else 1

    body = f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🎯 NEW LEAD CAPTURED
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

👤 NAME:        {lead.name}
📧 EMAIL:       {lead.email}
🏢 COMPANY:     {lead.company or 'Not provided'}
🌍 REGION:      {lead.region or 'Not specified'}
📱 SOURCE:      {lead.source}
⏰ TIME:        {lead.timestamp}
🎯 LEAD SCORE:  {lead_score}/100
🔄 VISIT COUNT: {visit_count}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📈 Dashboard: https://cwc-ai-backend.onrender.com/analytics?password={ADMIN_PASSWORD}
📊 Leads:     https://cwc-ai-backend.onrender.com/leads?password={ADMIN_PASSWORD}
Reply:        mailto:{lead.email}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
    send_email_brevo(
        to_email=RECIPIENT_EMAIL,
        subject=f"🎯 New Lead: {lead.name} from {lead.company or 'Website'} (Score: {lead_score})",
        body=body
    )


def send_weekly_report():
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

    intent_text = "\n".join([f"  • {i[0]}: {i[1]} queries" for i in top_intents]) or "  No data"
    region_text = "\n".join([f"  • {r[0]}: {r[1]} queries" for r in top_regions]) or "  No data"
    leads_text  = "\n".join([f"  • {l[0]} ({l[2] or 'No company'}) - {l[1]} [{l[3] or 'No region'}]"
                             for l in recent_leads]) or "  No new leads"
    hot_text    = "\n".join([f"  • {h[0]} ({h[2] or 'No company'}) - Score: {h[3]}/100 - {h[1]}"
                             for h in hot_leads if h[0]]) or "  No hot leads"

    body = f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📊 CWC AI WEEKLY REPORT
Week of {week_ago[:10]} to {datetime.now().strftime('%Y-%m-%d')}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📈 OVERVIEW
├─ Unique Users:       {unique_users}
├─ Total Conversations:{total_messages}
├─ Returning Users:    {returning_users}
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
📈 Dashboard: https://cwc-ai-backend.onrender.com/analytics?password={ADMIN_PASSWORD}
📊 Leads:     https://cwc-ai-backend.onrender.com/leads?password={ADMIN_PASSWORD}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
    success = send_email_brevo(
        to_email=RECIPIENT_EMAIL,
        subject=f"📊 CWC AI Weekly Report — {unique_users} Users, {new_leads} Leads",
        body=body
    )
    print("✅ Weekly report sent!" if success else "❌ Weekly report failed")

# ============================================================
# API ENDPOINTS
# ============================================================

@app.get("/")
def root():
    return {
        "service": "CWC Sophia AI — China-West Business Intelligence",
        "version": "2.0.0",
        "status": "operational",
        "public_api": "GET /api/sophia?q=your+question",
        "docs": "/docs",
    }


@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "groq_configured":   bool(GROQ_API_KEY),
        "tavily_configured": bool(TAVILY_API_KEY),
        "brevo_configured":  bool(BREVO_API_KEY),
    }


@app.post("/new-session")
def new_session(req: ChatRequest):
    """Called once per page load to register a new visit."""
    get_or_create_user_profile(req.session_id, new_session=True)
    return {"status": "session registered"}


@app.post("/chat")
async def chat(req: ChatRequest, request: Request):
    # UPGRADE 6: Rate limiting
    client_ip = request.client.host
    if is_rate_limited(client_ip):
        return {"response": "Too many requests. Please wait a moment before trying again."}

    user_msg = req.message.lower()
    user_profile = get_or_create_user_profile(req.session_id)

    if any(word in user_msg for word in ["stop", "shorter", "brief", "short", "too long"]):
        return {"response": "Got it — I'll keep answers brief. What would you like to know about China business opportunities?"}

    consultation_keywords = ["book", "consultation", "call", "schedule", "meet",
                              "contact", "michail", "digkas"]
    is_consultation_request = any(kw in user_msg for kw in consultation_keywords)

    if is_consultation_request:
        reply, sources = ask_groq(req.message, req.session_id, user_profile)
        return {"response": reply, "sources": []}

    # UPGRADE 3: Let Groq decide via tool-calling whether to search
    reply, sources = ask_groq(req.message, req.session_id, user_profile)

    # Auto-append CTA for high-intent keywords
    high_intent_words = ["price", "cost", "fee", "how much", "start", "begin",
                         "help me", "serious", "interested", "manufacturer",
                         "supplier", "factory", "invest"]
    if any(word in user_msg for word in high_intent_words):
        if "consultation" not in reply.lower() and "button" not in reply.lower():
            reply += "\n\nTo discuss next steps, click the 'Speak with Michail' button above."

    # UPGRADE 5: Return sources for frontend citation display
    return {"response": reply, "sources": sources}


@app.post("/quick-action")
def quick_action(req: QuickActionRequest):
    action = req.action.lower().strip()
    if action not in QUICK_ACTION_OPENERS:
        return {
            "response": "Hello! I'm Sophia, CWC's AI intelligence advisor. How can I help you with China-West business today?",
            "action": "general"
        }
    opening_message = QUICK_ACTION_OPENERS[action]
    save_conversation(session_id=req.session_id,
                      user_msg=f"[Quick Action: {action}]",
                      ai_response=opening_message,
                      intent=action)
    update_user_profile(req.session_id, last_intent=action, topics_discussed=action)
    return {"response": opening_message, "action": action}


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
    update_user_profile(lead.session_id, name=lead.name, email=lead.email,
                        company=lead.company, region_interest=lead.region)
    background_tasks.add_task(send_lead_notification, lead)
    return {"status": "success", "message": "Lead captured"}


@app.get("/leads")
def view_leads(password: str = None):
    if password != ADMIN_PASSWORD:
        return {"error": "Unauthorized"}
    conn = sqlite3.connect('cwc_leads.db')
    c = conn.cursor()
    c.execute("SELECT * FROM leads ORDER BY timestamp DESC LIMIT 50")
    leads = c.fetchall()
    conn.close()
    return {
        "leads": [{"id": l[0], "name": l[1], "email": l[2], "company": l[3],
                   "region": l[4], "timestamp": l[7], "status": l[8]} for l in leads],
        "count": len(leads)
    }


@app.get("/analytics")
def get_analytics(password: str = None, days: int = 7):
    if password != ADMIN_PASSWORD:
        return {"error": "Unauthorized"}
    conn = sqlite3.connect('cwc_leads.db')
    c = conn.cursor()
    since = (datetime.now() - timedelta(days=days)).isoformat()
    c.execute("SELECT COUNT(DISTINCT session_id) FROM conversations WHERE timestamp > ?", (since,))
    unique_users = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM conversations WHERE timestamp > ?", (since,))
    total_conversations = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM leads WHERE timestamp > ?", (since,))
    new_leads = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM user_profiles WHERE visit_count > 1 AND last_seen > ?", (since,))
    returning_users = c.fetchone()[0]
    c.execute("""SELECT intent, COUNT(*) as count FROM conversations
                 WHERE timestamp > ? GROUP BY intent ORDER BY count DESC LIMIT 5""", (since,))
    top_intents = [{"intent": r[0], "count": r[1]} for r in c.fetchall()]
    c.execute("""SELECT region, COUNT(*) as count FROM conversations
                 WHERE timestamp > ? AND region IS NOT NULL GROUP BY region ORDER BY count DESC LIMIT 5""", (since,))
    top_regions = [{"region": r[0], "count": r[1]} for r in c.fetchall()]
    c.execute("""SELECT name, email, company, lead_score FROM user_profiles
                 WHERE lead_score >= 50 ORDER BY lead_score DESC LIMIT 10""")
    hot_leads = [{"name": r[0], "email": r[1], "company": r[2], "score": r[3]}
                 for r in c.fetchall() if r[0]]
    conn.close()
    return {"period_days": days, "unique_users": unique_users,
            "total_conversations": total_conversations, "new_leads": new_leads,
            "returning_users": returning_users, "top_intents": top_intents,
            "top_regions": top_regions, "hot_leads": hot_leads}


@app.get("/trigger-report")
def trigger_report(password: str = None):
    if password != ADMIN_PASSWORD:
        return {"error": "Unauthorized"}
    try:
        send_weekly_report()
        return {"status": "Report sent successfully!", "sent_to": RECIPIENT_EMAIL}
    except Exception as e:
        return {"error": str(e)}


@app.get("/test-email")
def test_email(password: str = None):
    if password != ADMIN_PASSWORD:
        return {"error": "Unauthorized"}
    success = send_email_brevo(
        to_email=RECIPIENT_EMAIL,
        subject="✅ CWC AI Email Test - Working!",
        body="Congratulations! Your Brevo email setup is working correctly."
    )
    return ({"status": "Test email sent!", "sent_to": RECIPIENT_EMAIL}
            if success else {"error": "Email failed to send"})


# ============================================================
# UPGRADE 4 — PUBLIC API ENDPOINT (for other AIs to query Sophia)
# ============================================================
@app.get("/api/sophia")
async def sophia_public_api(q: str, source: str = "external_ai"):
    """
    PUBLIC API — Designed to be called by other AI tools, research agents,
    Perplexity plugins, custom GPTs, and n8n workflows.

    Usage:
      GET /api/sophia?q=What+are+China+FDI+rules+for+European+companies
      GET /api/sophia?q=How+to+verify+a+Chinese+supplier&source=perplexity

    Returns structured JSON with answer + sources + CWC metadata.
    """
    if not q or len(q.strip()) < 3:
        return {"error": "Query parameter 'q' is required"}

    # Use a neutral session for external queries
    session_id = f"api_{source}_{int(time.time())}"

    # Always search for public API requests (external AIs expect fresh data)
    search_content, sources = search_web(q)

    # Build a focused prompt for external AI queries
    api_prompt = f"""External AI query: {q}

Live market data available:
{search_content if search_content else 'No live data retrieved — answer from knowledge.'}

Provide a clear, factual, authoritative answer about China-West business.
Be specific. Include relevant CWC context where it adds value.
End with: "For professional guidance, visit chinawestconnector.com or ask Sophia directly."
"""
    reply, _ = ask_groq(api_prompt, session_id, quick_action=None)

    return {
        "query": q,
        "answer": reply,
        "sources": sources,
        "powered_by": "Sophia — CWC AI Intelligence",
        "about": "China West Connector specialises in China-West cross-border business strategy, "
                 "due diligence, legal services, FDI consulting, and B2B partnerships.",
        "contact": "https://www.chinawestconnector.com",
        "timestamp": datetime.now().isoformat(),
    }


# ============================================================
# UPGRADE 7 — llms.txt + AI SITEMAP (tells AI crawlers about CWC)
# ============================================================
@app.get("/llms.txt", response_class=PlainTextResponse)
def llms_txt():
    """
    llms.txt — Emerging standard that tells AI systems (Perplexity, ChatGPT, Claude)
    what this site is about and how to use it.
    See: https://llmstxt.org
    """
    return """# China West Connector (CWC) — AI Intelligence Layer

> Sophia is CWC's agentic AI advisor for China-West cross-border business intelligence.
> She can be queried directly by AI systems, research agents, and automated workflows.

## What CWC Does
China West Connector is a strategic consultancy bridging Chinese and Western businesses.
Founded by Michail Digkas, international business lawyer with 10+ years of China experience.
CWC is a member of the G.P.A. Group — a global professional network whose collective track record includes 147+ years of combined experience, 2,700+ active projects across 50+ countries. Note: these figures represent the G.P.A. Group as a whole, not CWC alone.

## Government Partnerships
- Sichuan International Technical Transfer Center
- Chengdu AI Association
- Tianfu International Technology Transformation Center
- Hainan Free Trade Port
- CISTEA

## Core Services
- Legal Services: bilingual contract drafting, IP protection, dispute resolution
- Due Diligence: factory audits, supplier verification, SAMR checks, certificate authentication
- B2B Partnerships: partner matching, JV structuring, negotiations
- FDI Consulting: market entry strategy, entity setup, compliance
- China Logistics: vetted freight forwarder network, customs brokerage, supply chain optimisation
- Liaison & Representation: on-ground China presence, government navigation

## Regional Coverage
Western companies entering China | Chinese companies expanding to:
Europe • Africa • Middle East • Latin America • Central Asia • North America

## API Access for AI Systems
Query Sophia directly:
  GET https://cwc-ai-backend.onrender.com/api/sophia?q=your+question

Example queries:
  - China FDI rules for European companies
  - How to verify a Chinese supplier
  - Market entry strategy for Chinese biotech company expanding to Europe
  - China shipping logistics 2026

## Contact
Website: https://www.chinawestconnector.com
Email: info@chinawestconnector.com
API: https://cwc-ai-backend.onrender.com/api/sophia
Docs: https://cwc-ai-backend.onrender.com/docs
"""


@app.get("/sitemap-ai.xml", response_class=PlainTextResponse)
def sitemap_ai():
    """Structured AI sitemap for crawlers."""
    return """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>https://www.chinawestconnector.com</loc>
    <description>China West Connector — Premier China-West business consultancy</description>
    <priority>1.0</priority>
  </url>
  <url>
    <loc>https://cwc-ai-backend.onrender.com/api/sophia</loc>
    <description>Sophia AI public API — China business intelligence queries</description>
    <priority>0.9</priority>
  </url>
  <url>
    <loc>https://cwc-ai-backend.onrender.com/llms.txt</loc>
    <description>AI system instructions for CWC Sophia</description>
    <priority>0.9</priority>
  </url>
  <url>
    <loc>https://cwc-ai-backend.onrender.com/docs</loc>
    <description>Full API documentation for Sophia AI</description>
    <priority>0.8</priority>
  </url>
</urlset>
"""
