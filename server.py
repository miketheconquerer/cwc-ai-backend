from fastapi import FastAPI, BackgroundTasks, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
import requests
import os
import sqlite3
import json
import time
import hashlib
import re
from collections import defaultdict
from datetime import datetime, timedelta
from dotenv import load_dotenv
import asyncio
from contextlib import asynccontextmanager

load_dotenv()

# ============================================================
# CONFIGURATION
# ============================================================
BREVO_API_KEY   = os.getenv("BREVO_API_KEY", "")
GROQ_API_KEY    = os.getenv("GROQ_API_KEY")
TAVILY_API_KEY  = os.getenv("TAVILY_API_KEY")
ADMIN_PASSWORD  = os.getenv("ADMIN_PASSWORD", "")
SENDER_EMAIL    = "888nv666@gmail.com"
RECIPIENT_EMAIL = "digkasm@proton.me"

# ============================================================
# RATE LIMITING (in-memory, free)
# ============================================================
_rate_store: dict = defaultdict(list)
RATE_LIMIT_REQUESTS = 30
RATE_LIMIT_WINDOW   = 60

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
    version="3.0.0",
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
                  last_intent TEXT,
                  language TEXT DEFAULT "en",
                  conversation_summary TEXT)''')

    # UPGRADE H: Response cache table
    c.execute('''CREATE TABLE IF NOT EXISTS response_cache
                 (cache_key TEXT PRIMARY KEY,
                  response TEXT,
                  sources TEXT,
                  created_at DATETIME)''')

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
# UPGRADE G — LANGUAGE AUTO-DETECTION (free, no library needed)
# ============================================================
def detect_language(text: str) -> str:
    """
    Detect language from character ranges — no library needed.
    Returns ISO code: 'zh', 'ar', 'es', 'fr', 'de', 'ru', 'en'
    """
    if not text:
        return "en"
    # Chinese characters (CJK Unified Ideographs)
    chinese_chars = len(re.findall(r'[\u4e00-\u9fff\u3400-\u4dbf]', text))
    # Arabic characters
    arabic_chars  = len(re.findall(r'[\u0600-\u06ff]', text))
    # Cyrillic (Russian)
    cyrillic_chars = len(re.findall(r'[\u0400-\u04ff]', text))
    total = max(len(text), 1)

    if chinese_chars / total > 0.15:  return "zh"
    if arabic_chars  / total > 0.15:  return "ar"
    if cyrillic_chars / total > 0.15: return "ru"

    # Latin-script language hints via common words
    lower = text.lower()
    es_words = ["que", "como", "para", "con", "una", "por", "del", "los"]
    fr_words = ["que", "les", "des", "est", "pour", "dans", "avec", "vous"]
    de_words = ["und", "die", "der", "das", "ist", "ich", "mit", "ein"]

    words = set(lower.split())
    if len(words & set(es_words)) >= 2: return "es"
    if len(words & set(fr_words)) >= 2: return "fr"
    if len(words & set(de_words)) >= 2: return "de"
    return "en"


LANGUAGE_INSTRUCTIONS = {
    "zh": "用中文回复。这是高优先级中国企业客户。",
    "ar": "الرجاء الرد باللغة العربية.",
    "es": "Por favor responde en español.",
    "fr": "Veuillez répondre en français.",
    "de": "Bitte antworte auf Deutsch.",
    "ru": "Пожалуйста, отвечайте на русском языке.",
    "en": "",
}

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
            "session_id":           profile[1],
            "first_seen":           profile[2],
            "last_seen":            profile[3],
            "visit_count":          profile[4] + (1 if new_session else 0),
            "name":                 profile[5],
            "email":                profile[6],
            "company":              profile[7],
            "region_interest":      profile[8],
            "topics_discussed":     profile[9],
            "lead_score":           profile[10],
            "last_intent":          profile[11],
            "language":             profile[12] if len(profile) > 12 else "en",
            "conversation_summary": profile[13] if len(profile) > 13 else None,
            "is_returning":         True
        }
    else:
        c.execute("""INSERT INTO user_profiles
                     (session_id, first_seen, last_seen, visit_count, language)
                     VALUES (?, ?, ?, 1, 'en')""",
                  (session_id, datetime.now(), datetime.now()))
        conn.commit()
        user_profile = {
            "session_id": session_id, "first_seen": datetime.now(),
            "last_seen": datetime.now(), "visit_count": 1,
            "name": None, "email": None, "company": None,
            "region_interest": None, "topics_discussed": None,
            "lead_score": 0, "last_intent": None,
            "language": "en", "conversation_summary": None,
            "is_returning": False
        }
    conn.close()
    return user_profile


def update_user_profile(session_id: str, **kwargs):
    conn = sqlite3.connect('cwc_leads.db')
    c = conn.cursor()
    valid_fields = ['name', 'email', 'company', 'region_interest',
                    'topics_discussed', 'lead_score', 'last_intent',
                    'language', 'conversation_summary']
    updates, values = [], []
    for key, value in kwargs.items():
        if key in valid_fields and value is not None:
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
    # Boost for non-English speakers (often higher-value international leads)
    if user_profile.get('language', 'en') == 'zh':
        score += 20
    elif user_profile.get('language', 'en') != 'en':
        score += 10
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


def get_message_count(session_id: str) -> int:
    conn = sqlite3.connect('cwc_leads.db')
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM conversations WHERE session_id = ?", (session_id,))
    count = c.fetchone()[0]
    conn.close()
    return count

# ============================================================
# UPGRADE H — RESPONSE CACHE (SQLite, free, 24hr TTL)
# ============================================================
def get_cached_response(query: str) -> tuple[str, list] | None:
    """Return cached response if fresh (< 24 hours old)."""
    cache_key = hashlib.md5(query.strip().lower().encode()).hexdigest()
    conn = sqlite3.connect('cwc_leads.db')
    c = conn.cursor()
    c.execute("""SELECT response, sources, created_at FROM response_cache
                 WHERE cache_key = ?""", (cache_key,))
    row = c.fetchone()
    conn.close()
    if row:
        created = datetime.fromisoformat(row[2])
        if datetime.now() - created < timedelta(hours=24):
            sources = json.loads(row[1]) if row[1] else []
            return row[0], sources
    return None


def set_cached_response(query: str, response: str, sources: list):
    """Cache a response for 24 hours."""
    cache_key = hashlib.md5(query.strip().lower().encode()).hexdigest()
    conn = sqlite3.connect('cwc_leads.db')
    c = conn.cursor()
    c.execute("""INSERT OR REPLACE INTO response_cache
                 (cache_key, response, sources, created_at)
                 VALUES (?, ?, ?, ?)""",
              (cache_key, response, json.dumps(sources), datetime.now()))
    conn.commit()
    conn.close()


# Queries worth caching — factual, stable, frequently asked
CACHEABLE_PATTERNS = [
    "hainan free trade", "samr", "wfoe", "vat", "fdi rules",
    "what is cwc", "what is china west", "belt and road",
    "how to register", "free trade zone", "import duties",
    "nmpa", "ce certification", "iso certification",
]

def is_cacheable(query: str) -> bool:
    lower = query.lower()
    return any(p in lower for p in CACHEABLE_PATTERNS)

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
# MULTI-TOOL SEARCH CASCADE (Free + Tavily)
# ============================================================
def search_duckduckgo(query: str) -> tuple[str, list[str]]:
    try:
        res = requests.get("https://api.duckduckgo.com/",
                           params={"q": query, "format": "json",
                                   "no_html": "1", "skip_disambig": "1"},
                           timeout=8)
        data = res.json()
        abstract = data.get("AbstractText", "")
        related  = [r.get("Text", "") for r in data.get("RelatedTopics", [])[:3] if r.get("Text")]
        sources  = [data.get("AbstractURL", "DuckDuckGo")] if abstract else []
        combined = abstract + ("\n" + "\n".join(related) if related else "")
        return combined.strip(), sources
    except Exception as e:
        print(f"DuckDuckGo error: {e}")
        return "", []


def search_wikipedia(query: str) -> tuple[str, list[str]]:
    try:
        url = "https://en.wikipedia.org/api/rest_v1/page/summary/" + query.replace(" ", "_")
        res = requests.get(url, timeout=8)
        if res.status_code == 200:
            data = res.json()
            extract  = data.get("extract", "")
            page_url = data.get("content_urls", {}).get("desktop", {}).get("page", "Wikipedia")
            if extract:
                return extract[:600], [page_url]
        return "", []
    except Exception as e:
        print(f"Wikipedia error: {e}")
        return "", []


def search_tavily(query: str) -> tuple[str, list[str]]:
    if not TAVILY_API_KEY:
        return "", []
    current_year = datetime.now().year
    news_keywords = ["news", "latest", "update", "today", "recent", "announced"]
    is_news = any(kw in query.lower() for kw in news_keywords)
    enhanced = (f"{query} China business trade {current_year} latest news"
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
        data    = res.json()
        answer  = data.get("answer", "")
        results = data.get("results", [])
        sources = [r.get("url", "") for r in results if r.get("url")]
        content = answer + "\n" + "\n".join(r.get("content", "") for r in results[:2])
        return content.strip(), sources
    except Exception as e:
        print(f"Tavily error: {e}")
        return "", []


def search_web(query: str) -> tuple[str, list[str]]:
    all_content, all_sources = [], []
    tavily_content, tavily_sources = search_tavily(query)
    if tavily_content:
        all_content.append(tavily_content)
        all_sources.extend(tavily_sources)
    if not tavily_content:
        ddg_content, ddg_sources = search_duckduckgo(query)
        if ddg_content:
            all_content.append(ddg_content)
            all_sources.extend(ddg_sources)
    wiki_content, wiki_sources = search_wikipedia(query)
    if wiki_content and len("\n".join(all_content)) < 400:
        all_content.append(f"Background: {wiki_content}")
        all_sources.extend(wiki_sources)
    combined = "\n\n".join(all_content)
    unique_sources = list(dict.fromkeys(s for s in all_sources if s))[:4]
    return combined, unique_sources

# ============================================================
# UPGRADE D — RSS NEWS FEED (Free, real China business news)
# ============================================================
_news_cache: dict = {"items": [], "fetched_at": None}

def fetch_china_news() -> list[dict]:
    """
    Fetch real China business news from free RSS feeds.
    Caches for 2 hours to avoid hammering feeds.
    """
    global _news_cache
    now = datetime.now()
    if (_news_cache["fetched_at"] and
            now - _news_cache["fetched_at"] < timedelta(hours=2) and
            _news_cache["items"]):
        return _news_cache["items"]

    feeds = [
        "https://www.scmp.com/rss/2/feed",
        "https://www.scmp.com/rss/4/feed",
        "https://www.caixinglobal.com/rss/latest-stories.xml",
        "https://www.chinadaily.com.cn/rss/bizchina_rss.xml",
        "https://www.xinhuanet.com/english/rss/financerss.xml",
    ]

    items = []
    for feed_url in feeds:
        try:
            res = requests.get(feed_url, timeout=8,
                               headers={"User-Agent": "Mozilla/5.0 CWC-Sophia/3.0"})
            if res.status_code != 200:
                continue

            # Parse each <item> block individually — fixes title/URL mismatch
            item_blocks = re.findall(r'<item[^>]*>(.*?)</item>', res.text, re.DOTALL)

            for block in item_blocks[:5]:
                title_match = re.search(
                    r'<title><!\[CDATA\[(.*?)\]\]></title>|<title>(.*?)</title>',
                    block, re.DOTALL)
                if not title_match:
                    continue
                title = (title_match.group(1) or title_match.group(2) or "").strip()
                title = re.sub(r'<[^>]+>', '', title).strip()
                if not title or len(title) < 10:
                    continue

                link_match = re.search(r'<link>(https?://[^<]+)</link>', block)
                if not link_match:
                    link_match = re.search(r'<guid[^>]*>(https?://[^<]+)</guid>', block)
                link = link_match.group(1).strip() if link_match else feed_url

                date_match = re.search(r'<pubDate>(.*?)</pubDate>', block)
                date = date_match.group(1).strip()[:16] if date_match else ""

                # Filter out non-China-business stories
                skip_keywords = ["ukraine", "russia", "greenland", "denmark",
                                 "epstein", "nato", "israel", "gaza", "afghanistan"]
                if any(kw in title.lower() for kw in skip_keywords):
                    continue

                category = "China Business"
                if any(w in title.lower() for w in ["trade", "tariff", "export", "import", "wto"]):
                    category = "Trade"
                elif any(w in title.lower() for w in ["invest", "fdi", "fund", "deal", "acquisition"]):
                    category = "Investment"
                elif any(w in title.lower() for w in ["policy", "regulat", "law", "rule", "government"]):
                    category = "Policy"
                elif any(w in title.lower() for w in ["tech", "ai", "robot", "digital", "semiconductor"]):
                    category = "Technology"
                elif any(w in title.lower() for w in ["energy", "solar", "ev", "battery", "green"]):
                    category = "Energy"
                elif any(w in title.lower() for w in ["pharma", "biotech", "health", "medical"]):
                    category = "Biotech"
                elif any(w in title.lower() for w in ["ship", "freight", "logistics", "port", "supply"]):
                    category = "Logistics"

                items.append({
                    "title": title, "url": link,
                    "category": category, "date": date
                })

            if len(items) >= 8:
                break
        except Exception as e:
            print(f"RSS feed error ({feed_url}): {e}")
            continue

    if items:
        _news_cache["items"]      = items[:8]
        _news_cache["fetched_at"] = now
        return items[:8]

    # Fallback: return curated static items if all feeds fail
    return [
        {"title": "China announces new FDI incentives for tech sector",
         "url": "", "category": "Policy", "date": ""},
        {"title": "Chinese EV makers accelerate LATAM battery plant investments",
         "url": "", "category": "Investment", "date": ""},
        {"title": "Major lithium partnerships signed between Chinese and African firms",
         "url": "", "category": "Trade", "date": ""},
        {"title": "UAE and China launch cross-border digital currency pilot",
         "url": "", "category": "Fintech", "date": ""},
        {"title": "New due diligence requirements for foreign buyers in China",
         "url": "", "category": "Compliance", "date": ""},
    ]

# ============================================================
# UPGRADE E — SAMR COMPANY LOOKUP (Free, China business registry)
# ============================================================
def lookup_chinese_company(company_name: str) -> dict:
    """
    Free preliminary lookup of Chinese companies via:
    1. SAMR public search (qixin.com proxy, free)
    2. DuckDuckGo search for public registration info
    Returns structured dict with what was found.
    """
    result = {
        "company": company_name,
        "found": False,
        "registration_status": "Unknown",
        "details": "",
        "sources": [],
        "warning": None
    }

    # Try DuckDuckGo for public business info
    query = f"{company_name} China company registration SAMR business license"
    ddg_content, ddg_sources = search_duckduckgo(query)
    tavily_content, tavily_sources = search_tavily(query)

    combined = (tavily_content or ddg_content or "").lower()
    all_sources = tavily_sources + ddg_sources

    if combined:
        result["found"] = True
        result["sources"] = all_sources[:3]

        # Look for red flags in results
        red_flags = ["scam", "fraud", "fake", "blacklist", "warning", "complaint",
                     "dispute", "lawsuit", "suspended", "revoked"]
        flags_found = [f for f in red_flags if f in combined]
        if flags_found:
            result["warning"] = f"⚠️ Potential red flags detected: {', '.join(flags_found)}"
            result["registration_status"] = "Requires Investigation"
        else:
            result["registration_status"] = "Preliminary search complete — full audit recommended"

        # Extract snippet
        raw = tavily_content or ddg_content
        result["details"] = raw[:400] if raw else ""
    else:
        result["details"] = ("No public information found. This could mean the company is "
                             "very small, recently registered, or the name may be incorrect. "
                             "A full CWC Due Diligence report is strongly recommended.")
        result["warning"] = "⚠️ No public data found — treat with caution"

    return result

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
# GROQ TOOL DEFINITIONS
# ============================================================
GROQ_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "search_market_intelligence",
            "description": (
                "Search for live China business intelligence, market data, trade news, "
                "regulatory updates, or industry information. Use when the user asks about "
                "current events, market conditions, regulations, or any topic needing fresh data."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "The search query"},
                    "search_type": {
                        "type": "string",
                        "enum": ["market_news", "company_lookup", "regulation", "general"],
                        "description": "Type of search"
                    }
                },
                "required": ["query", "search_type"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "lookup_company",
            "description": (
                "Look up a specific Chinese company in public registries and databases. "
                "Use when the user mentions a specific company name and wants verification, "
                "background check, or due diligence information."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "company_name": {
                        "type": "string",
                        "description": "The Chinese company name to look up"
                    }
                },
                "required": ["company_name"]
            }
        }
    }
]


def run_tool_call(tool_name: str, tool_args: dict) -> tuple[str, list[str]]:
    if tool_name == "search_market_intelligence":
        query = tool_args.get("query", "")
        search_type = tool_args.get("search_type", "general")
        if search_type == "market_news":
            query = f"{query} latest news 2025 2026"
        elif search_type == "company_lookup":
            query = f"{query} China company profile business registration"
        elif search_type == "regulation":
            query = f"{query} China regulation compliance 2026"
        return search_web(query)

    elif tool_name == "lookup_company":
        company_name = tool_args.get("company_name", "")
        result = lookup_chinese_company(company_name)
        summary = (
            f"Company: {result['company']}\n"
            f"Status: {result['registration_status']}\n"
            f"Details: {result['details']}\n"
        )
        if result.get('warning'):
            summary += f"WARNING: {result['warning']}\n"
        return summary, result.get('sources', [])

    return "", []

# ============================================================
# UPGRADE F — CONVERSATION SUMMARY & MICHAIL HANDOFF BRIEF
# ============================================================
def generate_handoff_brief(session_id: str, user_profile: dict) -> str:
    """
    Generate a structured briefing for Michail when a user requests consultation.
    Summarises the full conversation, user profile, lead score, and key needs.
    """
    history = get_conversation_history(session_id, limit=20)
    conv_text = "\n".join([f"User: {u}\nSophia: {a}" for u, a in history])

    name    = user_profile.get('name') or 'Unknown'
    email   = user_profile.get('email') or 'Not captured'
    company = user_profile.get('company') or 'Not provided'
    region  = user_profile.get('region_interest') or 'Not specified'
    score   = user_profile.get('lead_score', 0)
    visits  = user_profile.get('visit_count', 1)
    lang    = user_profile.get('language', 'en')
    intent  = user_profile.get('last_intent', 'Unknown')

    # Score interpretation
    if score >= 70:
        priority = "🔥 HOT — Contact within 24 hours"
    elif score >= 40:
        priority = "🟡 WARM — Follow up within 48 hours"
    else:
        priority = "🔵 COLD — Add to nurture sequence"

    brief = f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📋 SOPHIA HANDOFF BRIEF FOR MICHAIL
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

👤 CONTACT
   Name:    {name}
   Email:   {email}
   Company: {company}
   Region:  {region}
   Language:{lang.upper()}

📊 LEAD INTELLIGENCE
   Score:    {score}/100
   Priority: {priority}
   Visits:   {visits}
   Intent:   {intent}

💬 CONVERSATION SUMMARY
{conv_text[:1500] if conv_text else 'No conversation recorded'}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⚡ RECOMMENDED ACTION
{_recommend_action(score, intent, region)}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
    return brief


def _recommend_action(score: int, intent: str, region: str) -> str:
    if intent == "supplier_verification":
        return "URGENT: User needs due diligence. Start with SAMR check + factory audit proposal."
    elif intent == "high_intent_lead" and score >= 60:
        return f"HIGH VALUE: User ready to engage. Prepare service proposal for {region or 'target market'}."
    elif intent == "consultation_request":
        return "User explicitly requested consultation. Schedule discovery call ASAP."
    elif region:
        return f"User interested in {region} opportunities. Prepare regional market brief."
    else:
        return "Send personalised intro email with CWC capabilities deck."

# ============================================================
# UPGRADE B — PROACTIVE QUALIFICATION ENGINE
# ============================================================
def check_qualification_gaps(user_profile: dict, message_count: int) -> str | None:
    """
    After 3 messages, if Sophia still doesn't know direction/sector/goal,
    return a proactive qualification prompt to inject into the system.
    Returns None if user is already qualified.
    """
    if message_count < 3:
        return None

    missing = []
    if not user_profile.get('region_interest'):
        missing.append("their direction (Western into China, or Chinese expanding West)")
    if not user_profile.get('topics_discussed'):
        missing.append("their industry or sector")
    if not user_profile.get('last_intent') or user_profile.get('last_intent') == 'general':
        missing.append("their specific goal")

    if len(missing) >= 2:
        return (
            f"\n⚡ PROACTIVE QUALIFICATION REQUIRED: After {message_count} messages, "
            f"you still don't know: {', '.join(missing)}. "
            f"Interrupt the current flow and ask ONE direct qualifying question now. "
            f"Do not answer the current question until you have this critical information. "
            f"Be direct but warm: 'Before I go further — I want to make sure I give you "
            f"the most relevant intelligence. Can I ask...' "
        )
    return None


def check_escalation_trigger(user_profile: dict, message_count: int,
                               current_message: str) -> bool:
    """
    Returns True if Sophia should proactively escalate to Michail
    (user going in circles, very high score, or explicit urgency signals).
    """
    urgency_words = ["urgent", "asap", "immediately", "today", "deposit",
                     "already paid", "already transferred", "fraud", "scam",
                     "lost money", "emergency"]
    if any(w in current_message.lower() for w in urgency_words):
        return True
    if user_profile.get('lead_score', 0) >= 75 and message_count >= 4:
        return True
    return False

# ============================================================
# UPGRADE A — CHAIN OF THOUGHT + UPGRADE C — PERSISTENT MEMORY
# UPGRADE G embedded — LANGUAGE DETECTION
# ============================================================
def ask_groq(prompt: str, session_id: str = "anonymous",
             user_profile: dict = None, quick_action: str = None) -> tuple[str, list[str]]:
    if not GROQ_API_KEY:
        return "System temporarily unavailable. Please contact the CWC team directly.", []

    # UPGRADE G: Detect language from current message
    detected_lang = detect_language(prompt)
    if detected_lang != "en" and user_profile:
        update_user_profile(session_id, language=detected_lang)
        if user_profile:
            user_profile['language'] = detected_lang

    lang = (user_profile or {}).get('language', 'en') if user_profile else detected_lang
    lang_instruction = LANGUAGE_INSTRUCTIONS.get(lang, "")

    # Build real message history (Upgrade 2 — already in place)
    raw_history = get_conversation_history(session_id, limit=8)
    messages = []
    for user_msg, ai_resp in raw_history:
        messages.append({"role": "user",      "content": user_msg})
        messages.append({"role": "assistant", "content": ai_resp})
    messages.append({"role": "user", "content": prompt})

    message_count = get_message_count(session_id)
    intent_data   = detect_intent(prompt)

    # UPGRADE C: Build rich returning user context from stored profile
    returning_context = ""
    if user_profile and user_profile.get('is_returning'):
        name    = user_profile.get('name') or 'Unknown'
        summary = user_profile.get('conversation_summary', '')
        returning_context = f"""
RETURNING USER DETECTED:
- Visit #{user_profile.get('visit_count', 1)}
- Known name: {name}
- Last intent: {user_profile.get('last_intent', 'Unknown')}
- Region of interest: {user_profile.get('region_interest', 'Unknown')}
- Company: {user_profile.get('company', 'Unknown')}
- Lead score: {user_profile.get('lead_score', 0)}/100
- Previous session summary: {summary or 'First tracked session'}
INSTRUCTION: Reference their previous interest naturally. Do NOT re-introduce yourself.
If you know their name, use it. Advance the conversation — don't restart it.
"""

    # UPGRADE B: Check if proactive qualification is needed
    qualification_prompt = check_qualification_gaps(user_profile or {}, message_count)

    # UPGRADE B: Check if escalation should be triggered
    should_escalate = check_escalation_trigger(user_profile or {}, message_count, prompt)
    escalation_instruction = ""
    if should_escalate:
        escalation_instruction = (
            "\n🚨 ESCALATION TRIGGER ACTIVE: This user shows urgent need or high intent. "
            "End your response by strongly directing them to click 'Speak with Michail' button. "
            "Use language like: 'This situation needs immediate personal attention from our team.'"
        )

    sector_context = ""
    if quick_action:
        sector_map = {
            "robotics":     "ACTIVE SECTOR: ROBOTICS — Determine if Western (sourcing) or Chinese (expansion). Services: supplier sourcing, factory audits, CE certification, partner matching, IP protection.",
            "energy":       "ACTIVE SECTOR: ENERGY — Sub-sectors: solar PV, battery storage, EV, wind, hydrogen. Ask project scale (MW) and deal structure.",
            "biotech":      "ACTIVE SECTOR: BIOTECH — CMO/CDMO, pharma market entry, licensing, R&D. Ask molecule type, stage, GMP needs.",
            "shipping":     "ACTIVE SECTOR: SHIPPING — Import/export freight, supply chain, customs. Ask volume (FCL/LCL/air) and pain points.",
            "verify":       "ACTIVE SECTOR: DUE DILIGENCE — URGENT. Ask company name, location, what was offered. Large deposit at risk = escalate immediately.",
            "market_entry": "ACTIVE SECTOR: MARKET ENTRY — Highest value. Determine direction first. Deliver phased roadmap.",
        }
        sector_context = sector_map.get(quick_action, "")

    # UPGRADE A: Chain-of-Thought instruction
    cot_instruction = """
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CHAIN OF THOUGHT — THINK BEFORE ANSWERING
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Before writing your response, internally reason through these steps:
1. WHAT does the user actually need? (not just what they asked)
2. WHAT do I already know about this user from their profile?
3. DO I need live data to answer accurately? (if yes, use search tool)
4. WHICH CWC service is most relevant to their need?
5. WHAT is the single most valuable next step for this user?
Only then write your response. Never skip this reasoning process.
"""

    system_prompt = f"""You are Sophia — the official AI Intelligence advisor for China West Connector (CWC).
Version 3.0 — Fully Agentic

CURRENT DATE: {datetime.now().strftime('%B %Y')}
USER INTENT: {intent_data['primary']}
REGION DETECTED: {intent_data['region'] or 'Unknown'}
QUICK ACTION: {quick_action or 'None'}
MESSAGES THIS SESSION: {message_count}
{lang_instruction}
{returning_context}
{sector_context}
{qualification_prompt or ''}
{escalation_instruction}

{cot_instruction}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
YOUR CORE MISSION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
You are NOT a passive Q&A bot. You are an active business intelligence advisor.
1. QUALIFY the user (direction, industry, goal, urgency)
2. PERSONALISE using everything you know about them
3. RECOMMEND the most relevant CWC service with a clear reason
4. END every response with a concrete next step — never a dead end

QUALIFICATION PRIORITY (ask if unknown):
① Western (into China) or Chinese (expanding West)?
② Industry/sector?
③ Specific goal — sourcing, investment, legal, partnerships, market entry?
④ Urgency/timeline?

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ABOUT CWC
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
China West Connector bridges Chinese and Western businesses.
Founded by Michail Digkas — international business lawyer, 10+ years China experience.

CWC is a proud member of the G.P.A. Group — a global professional network.
The G.P.A. Group's COLLECTIVE track record: 147+ years combined experience,
2,700+ active projects, 50+ countries.
CRITICAL RULE: These figures = G.P.A. Group as a whole — NOT CWC alone.
NEVER say CWC has done 2,700 projects. ALWAYS say "CWC is part of the G.P.A. Group, which collectively has..."

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
CHINESE user → Western markets are expansion targets
Non-English user → respond in their language (already set above if applicable)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RESPONSE STRATEGY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
high_intent_lead      → Ask 1-2 qualifying questions + recommend CWC service + push to Michail
consultation_request  → Confirm CWC can help + direct to 'Speak with Michail' button
supplier_verification → URGENT. Ask company name + risk amount. Escalate immediately
information_gathering → Provide real insight then offer deeper consultation

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STYLE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- Max 180 words (150 preferred)
- Professional, confident, commercially sharp — sound like a senior advisor
- No hype, no buzzwords, no exaggerated claims
- Use specific numbers and facts for credibility
- NEVER mention calendar links or external URLs
- When escalating: "click the 'Speak with Michail' button above"
- URGENT due diligence (large deposit at risk): escalate every single time

FIRST MESSAGE (no history, no quick action): Introduce as Sophia, ask direction.
"""

    url     = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    all_sources = []

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
        content    = res.json()
        choice     = content["choices"][0]
        message_obj = choice["message"]

        if choice.get("finish_reason") == "tool_calls" and message_obj.get("tool_calls"):
            tool_results_messages = [{"role": "system", "content": system_prompt}] + list(messages)
            tool_results_messages.append(message_obj)

            for tool_call in message_obj["tool_calls"]:
                fn_name = tool_call["function"]["name"]
                fn_args = json.loads(tool_call["function"]["arguments"])
                tool_result, sources = run_tool_call(fn_name, fn_args)
                all_sources.extend(sources)
                tool_results_messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call["id"],
                    "content": tool_result or "No results found."
                })

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
            response_text = message_obj.get("content", "")

        # Update profile, lead score, save conversation
        new_score = calculate_lead_score(user_profile or {}, prompt, intent_data['primary'])
        update_user_profile(session_id,
                            last_intent=intent_data['primary'],
                            region_interest=intent_data['region'],
                            lead_score=new_score,
                            language=lang)
        save_conversation(session_id, prompt, response_text,
                          region=intent_data['region'],
                          intent=intent_data['primary'])

        # UPGRADE C: After every 5 messages, generate a summary of the session
        if message_count > 0 and message_count % 5 == 0:
            _update_conversation_summary(session_id)

        return response_text, all_sources

    except Exception as e:
        print(f"Groq error: {e}")
        return ("I apologise, but I'm having trouble connecting right now. "
                "Please reach out to the CWC team directly."), []


def _update_conversation_summary(session_id: str):
    """
    UPGRADE C: Periodically summarise the conversation into the user profile
    so Sophia can reference it in future sessions.
    """
    if not GROQ_API_KEY:
        return
    history = get_conversation_history(session_id, limit=10)
    if not history:
        return
    conv_text = "\n".join([f"User: {u}\nSophia: {a[:100]}" for u, a in history])
    try:
        res = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": "llama-3.3-70b-versatile",
                "messages": [
                    {"role": "system", "content": "Summarise this business conversation in 2-3 sentences. Focus on: what the user wants, their direction (Western/Chinese), sector, and urgency. Be factual and concise."},
                    {"role": "user", "content": conv_text}
                ],
                "temperature": 0.1,
                "max_tokens": 150
            },
            timeout=10
        )
        summary = res.json()["choices"][0]["message"]["content"]
        update_user_profile(session_id, conversation_summary=summary)
    except Exception as e:
        print(f"Summary generation error: {e}")

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


def send_lead_notification(lead: LeadCapture, session_id: str = None):
    conn = sqlite3.connect('cwc_leads.db')
    c = conn.cursor()
    c.execute("SELECT * FROM user_profiles WHERE session_id = ?",
              (lead.session_id,))
    profile_row = c.fetchone()
    conn.close()

    lead_score  = profile_row[10] if profile_row else 0
    visit_count = profile_row[4]  if profile_row else 1

    # Build user_profile dict for handoff brief
    user_profile = {}
    if profile_row:
        user_profile = {
            "name": profile_row[5], "email": profile_row[6],
            "company": profile_row[7], "region_interest": profile_row[8],
            "topics_discussed": profile_row[9], "lead_score": profile_row[10],
            "last_intent": profile_row[11], "visit_count": profile_row[4],
            "language": profile_row[12] if len(profile_row) > 12 else "en",
            "conversation_summary": profile_row[13] if len(profile_row) > 13 else None,
        }

    # UPGRADE F: Generate full handoff brief
    handoff = generate_handoff_brief(lead.session_id, user_profile)

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
{handoff}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📈 Dashboard: https://cwc-ai-backend.onrender.com/analytics?password={ADMIN_PASSWORD}
📊 Leads:     https://cwc-ai-backend.onrender.com/leads?password={ADMIN_PASSWORD}
Reply:        mailto:{lead.email}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
    send_email_brevo(
        to_email=RECIPIENT_EMAIL,
        subject=f"🎯 New Lead: {lead.name} from {lead.company or 'Website'} (Score: {lead_score}/100)",
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
    # Language breakdown
    c.execute("""SELECT language, COUNT(*) as count FROM user_profiles
                 WHERE last_seen > ? GROUP BY language ORDER BY count DESC""", (week_ago,))
    languages = c.fetchall()
    conn.close()

    intent_text  = "\n".join([f"  • {i[0]}: {i[1]} queries" for i in top_intents]) or "  No data"
    region_text  = "\n".join([f"  • {r[0]}: {r[1]} queries" for r in top_regions]) or "  No data"
    leads_text   = "\n".join([f"  • {l[0]} ({l[2] or 'No company'}) - {l[1]} [{l[3] or '?'}]"
                              for l in recent_leads]) or "  No new leads"
    hot_text     = "\n".join([f"  • {h[0]} ({h[2] or 'No company'}) - Score: {h[3]}/100 - {h[1]}"
                              for h in hot_leads if h[0]]) or "  No hot leads"
    lang_text    = "\n".join([f"  • {l[0].upper()}: {l[1]} users" for l in languages]) or "  No data"

    body = f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📊 CWC AI WEEKLY REPORT v3.0
Week of {week_ago[:10]} to {datetime.now().strftime('%Y-%m-%d')}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📈 OVERVIEW
├─ Unique Users:        {unique_users}
├─ Total Conversations: {total_messages}
├─ Returning Users:     {returning_users}
└─ New Leads Captured:  {new_leads}

🎯 TOP INTENTS
{intent_text}

🌍 TOP REGIONS
{region_text}

🌐 USER LANGUAGES
{lang_text}

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
        "service":    "CWC Sophia AI — China-West Business Intelligence",
        "version":    "3.0.0",
        "status":     "operational",
        "public_api": "GET /api/sophia?q=your+question",
        "news_api":   "GET /api/news",
        "docs":       "/docs",
    }


@app.get("/health")
def health_check():
    return {
        "status":            "healthy",
        "groq_configured":   bool(GROQ_API_KEY),
        "tavily_configured": bool(TAVILY_API_KEY),
        "brevo_configured":  bool(BREVO_API_KEY),
        "version":           "3.0.0",
    }


@app.post("/new-session")
def new_session(req: ChatRequest):
    get_or_create_user_profile(req.session_id, new_session=True)
    return {"status": "session registered"}


@app.post("/chat")
async def chat(req: ChatRequest, request: Request):
    client_ip = request.client.host
    if is_rate_limited(client_ip):
        return {"response": "Too many requests. Please wait a moment before trying again."}

    user_msg     = req.message.lower()
    user_profile = get_or_create_user_profile(req.session_id)

    if any(word in user_msg for word in ["stop", "shorter", "brief", "short", "too long"]):
        return {"response": "Got it — I'll keep answers brief. What would you like to know about China business opportunities?", "sources": []}

    # UPGRADE H: Check cache for common factual queries
    if is_cacheable(req.message):
        cached = get_cached_response(req.message)
        if cached:
            print(f"✅ Cache hit for: {req.message[:50]}")
            return {"response": cached[0], "sources": cached[1], "cached": True}

    consultation_keywords = ["book", "consultation", "call", "schedule", "meet",
                              "contact", "michail", "digkas"]
    is_consultation = any(kw in user_msg for kw in consultation_keywords)

    reply, sources = ask_groq(req.message, req.session_id, user_profile)

    # UPGRADE F: If consultation requested, also email handoff brief to Michail
    if is_consultation and user_profile.get('lead_score', 0) >= 20:
        brief = generate_handoff_brief(req.session_id, user_profile)
        send_email_brevo(
            to_email=RECIPIENT_EMAIL,
            subject=f"📋 Sophia Handoff: {user_profile.get('name') or 'Prospect'} requested consultation",
            body=brief
        )

    # Auto-append CTA for high-intent keywords
    high_intent_words = ["price", "cost", "fee", "how much", "start", "begin",
                         "help me", "serious", "interested", "manufacturer",
                         "supplier", "factory", "invest"]
    if any(word in user_msg for word in high_intent_words):
        if "consultation" not in reply.lower() and "button" not in reply.lower():
            reply += "\n\nTo discuss next steps, click the 'Speak with Michail' button above."

    # Cache if appropriate
    if is_cacheable(req.message) and reply:
        set_cached_response(req.message, reply, sources)

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
    c.execute("""SELECT language, COUNT(*) as count FROM user_profiles
                 WHERE last_seen > ? GROUP BY language ORDER BY count DESC""", (since,))
    languages = [{"language": r[0], "count": r[1]} for r in c.fetchall()]
    conn.close()
    return {
        "period_days": days, "unique_users": unique_users,
        "total_conversations": total_conversations, "new_leads": new_leads,
        "returning_users": returning_users, "top_intents": top_intents,
        "top_regions": top_regions, "hot_leads": hot_leads,
        "languages": languages
    }


@app.get("/trigger-report")
def trigger_report(password: str = None):
    if password != ADMIN_PASSWORD:
        return {"error": "Unauthorized"}
    try:
        send_weekly_report()
        return {"status": "Report sent!", "sent_to": RECIPIENT_EMAIL}
    except Exception as e:
        return {"error": str(e)}


@app.get("/test-email")
def test_email(password: str = None):
    if password != ADMIN_PASSWORD:
        return {"error": "Unauthorized"}
    success = send_email_brevo(
        to_email=RECIPIENT_EMAIL,
        subject="✅ CWC AI v3.0 Email Test",
        body="Congratulations! CWC Sophia v3.0 email is working correctly."
    )
    return ({"status": "Test email sent!", "sent_to": RECIPIENT_EMAIL}
            if success else {"error": "Email failed"})


# UPGRADE D — Live News API Endpoint
@app.get("/api/news")
def get_news():
    """
    Returns real China business news from RSS feeds.
    Frontend can call this to show live news in the widget.
    Updates every 2 hours automatically.
    """
    news = fetch_china_news()
    return {"news": news, "count": len(news),
            "cached_until": (_news_cache["fetched_at"] + timedelta(hours=2)).isoformat()
            if _news_cache["fetched_at"] else None}


# UPGRADE E — Company Lookup API Endpoint
@app.get("/api/verify-company")
async def verify_company(name: str, password: str = None):
    """
    Preliminary free lookup of a Chinese company.
    Can be called by the widget when user types a company name in verify flow.
    No password needed for basic lookup (public data only).
    """
    if not name or len(name.strip()) < 2:
        return {"error": "Company name required"}
    result = lookup_chinese_company(name.strip())
    return result


# PUBLIC API (for other AIs)
@app.get("/api/sophia")
async def sophia_public_api(q: str, source: str = "external_ai"):
    if not q or len(q.strip()) < 3:
        return {"error": "Query parameter 'q' is required"}

    # Check cache first
    if is_cacheable(q):
        cached = get_cached_response(q)
        if cached:
            return {
                "query": q, "answer": cached[0], "sources": cached[1],
                "powered_by": "Sophia — CWC AI Intelligence v3.0",
                "about": "China West Connector specialises in China-West cross-border business.",
                "contact": "https://www.chinawestconnector.com",
                "timestamp": datetime.now().isoformat(), "cached": True
            }

    session_id = f"api_{source}_{int(time.time())}"
    search_content, sources = search_web(q)
    api_prompt = f"""External AI query: {q}

Live market data:
{search_content if search_content else 'No live data — answering from knowledge.'}

Provide a clear, factual, authoritative answer about China-West business.
Be specific. Include relevant CWC context where it adds genuine value.
End with: "For professional guidance, visit chinawestconnector.com or ask Sophia directly."
"""
    reply, _ = ask_groq(api_prompt, session_id)

    if is_cacheable(q) and reply:
        set_cached_response(q, reply, sources)

    return {
        "query": q, "answer": reply, "sources": sources,
        "powered_by": "Sophia — CWC AI Intelligence v3.0",
        "about": "China West Connector specialises in China-West cross-border business strategy, "
                 "due diligence, legal services, FDI consulting, and B2B partnerships.",
        "contact": "https://www.chinawestconnector.com",
        "timestamp": datetime.now().isoformat(),
    }


# llms.txt — AI crawler discovery
@app.get("/llms.txt", response_class=PlainTextResponse)
def llms_txt():
    return """# China West Connector (CWC) — AI Intelligence Layer

> Sophia is CWC's agentic AI advisor for China-West cross-border business intelligence.
> She can be queried directly by AI systems, research agents, and automated workflows.
> Version 3.0 — Fully Agentic with multi-language support, live news, and company lookup.

## What CWC Does
China West Connector is a strategic consultancy bridging Chinese and Western businesses.
Founded by Michail Digkas, international business lawyer with 10+ years of China experience.
CWC is a member of the G.P.A. Group — a global professional network whose collective track
record includes 147+ years of combined experience, 2,700+ active projects across 50+ countries.
Note: these figures represent the G.P.A. Group as a whole, not CWC alone.

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
- China Logistics: vetted freight forwarder network, customs brokerage, supply chain
- Liaison & Representation: on-ground China presence, government navigation

## Regional Coverage
Western companies entering China | Chinese companies expanding to:
Europe • Africa • Middle East • Latin America • Central Asia • North America

## Languages Supported
English • Chinese (Mandarin) • Arabic • Spanish • French • German • Russian

## API Access for AI Systems
Query Sophia:     GET https://cwc-ai-backend.onrender.com/api/sophia?q=your+question
Live China News:  GET https://cwc-ai-backend.onrender.com/api/news
Company Lookup:   GET https://cwc-ai-backend.onrender.com/api/verify-company?name=company+name
API Docs:         https://cwc-ai-backend.onrender.com/docs

## Contact
Website: https://www.chinawestconnector.com
Email:   info@chinawestconnector.com
"""


@app.get("/sitemap-ai.xml", response_class=PlainTextResponse)
def sitemap_ai():
    return """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://www.chinawestconnector.com</loc><priority>1.0</priority></url>
  <url><loc>https://cwc-ai-backend.onrender.com/api/sophia</loc><priority>0.9</priority></url>
  <url><loc>https://cwc-ai-backend.onrender.com/api/news</loc><priority>0.8</priority></url>
  <url><loc>https://cwc-ai-backend.onrender.com/api/verify-company</loc><priority>0.8</priority></url>
  <url><loc>https://cwc-ai-backend.onrender.com/llms.txt</loc><priority>0.9</priority></url>
  <url><loc>https://cwc-ai-backend.onrender.com/docs</loc><priority>0.8</priority></url>
</urlset>
"""
