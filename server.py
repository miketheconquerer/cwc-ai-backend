from fastapi import FastAPI, BackgroundTasks, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
import requests
import os
import psycopg2
import psycopg2.extras
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
SENDER_EMAIL    = os.getenv("SENDER_EMAIL", "888nv666@gmail.com")
RECIPIENT_EMAIL = "digkasm@proton.me"
DATABASE_URL    = os.getenv("DATABASE_URL")

# ============================================================
# RATE LIMITING
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
    description="Sophia is CWC's agentic AI advisor for China-West cross-border business.",
    version="5.0.0",
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
# DATABASE
# ============================================================
def get_db():
    return psycopg2.connect(DATABASE_URL)

def init_db():
    conn = get_db()
    c = conn.cursor()

    c.execute('''CREATE TABLE IF NOT EXISTS conversations
                 (id SERIAL PRIMARY KEY, session_id TEXT, user_message TEXT,
                  ai_response TEXT, timestamp TIMESTAMP, email TEXT, company TEXT,
                  region TEXT, intent TEXT, reflection_score INTEGER DEFAULT 0)''')

    c.execute('''CREATE TABLE IF NOT EXISTS leads
                 (id SERIAL PRIMARY KEY, name TEXT, email TEXT, company TEXT,
                  region TEXT, session_id TEXT, source TEXT, timestamp TEXT, status TEXT)''')

    c.execute('''CREATE TABLE IF NOT EXISTS user_profiles
                 (id SERIAL PRIMARY KEY, session_id TEXT UNIQUE, first_seen TIMESTAMP,
                  last_seen TIMESTAMP, visit_count INTEGER DEFAULT 1, name TEXT, email TEXT,
                  company TEXT, region_interest TEXT, topics_discussed TEXT,
                  lead_score INTEGER DEFAULT 0, last_intent TEXT, language TEXT DEFAULT 'en',
                  conversation_summary TEXT, key_facts JSONB DEFAULT '{}',
                  task_history JSONB DEFAULT '[]')''')

    c.execute('''CREATE TABLE IF NOT EXISTS response_cache
                 (cache_key TEXT PRIMARY KEY, response TEXT, sources TEXT, created_at TIMESTAMP)''')

    c.execute('''CREATE TABLE IF NOT EXISTS agent_tasks
                 (id SERIAL PRIMARY KEY, session_id TEXT, task_description TEXT,
                  sub_tasks JSONB DEFAULT '[]', status TEXT DEFAULT 'pending',
                  result TEXT, created_at TIMESTAMP, completed_at TIMESTAMP)''')

    c.execute('''CREATE TABLE IF NOT EXISTS supplier_searches
                 (id SERIAL PRIMARY KEY, session_id TEXT, query TEXT,
                  results JSONB DEFAULT '[]', created_at TIMESTAMP)''')

    # Safe migrations for existing deployments
    migrations = [
        ("user_profiles",  "key_facts",        "JSONB DEFAULT '{}'"),
        ("user_profiles",  "task_history",      "JSONB DEFAULT '[]'"),
        ("conversations",  "reflection_score",  "INTEGER DEFAULT 0"),
    ]
    for table, col, definition in migrations:
        try:
            c.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col} {definition}")
        except Exception:
            conn.rollback()

    conn.commit()
    conn.close()

init_db()

# ============================================================
# PYDANTIC MODELS
# ============================================================
class ChatRequest(BaseModel):
    message: str
    session_id: str = "anonymous"
    deep_search: bool = False  # v5: triggers specialist sub-agents + task decomposition

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

class SupplierSearchRequest(BaseModel):
    query: str
    sector: str = ""
    region: str = ""
    session_id: str = "anonymous"

# ============================================================
# LANGUAGE DETECTION
# ============================================================
def detect_language(text: str) -> str:
    if not text: return "en"
    chinese_chars  = len(re.findall(r'[\u4e00-\u9fff\u3400-\u4dbf]', text))
    arabic_chars   = len(re.findall(r'[\u0600-\u06ff]', text))
    cyrillic_chars = len(re.findall(r'[\u0400-\u04ff]', text))
    total = max(len(text), 1)
    if chinese_chars  / total > 0.15: return "zh"
    if arabic_chars   / total > 0.15: return "ar"
    if cyrillic_chars / total > 0.15: return "ru"
    words = set(text.lower().split())
    if len(words & {"que","como","para","con","una","por","del","los"}) >= 2: return "es"
    if len(words & {"que","les","des","est","pour","dans","avec","vous"}) >= 2: return "fr"
    if len(words & {"und","die","der","das","ist","ich","mit","ein"}) >= 2:    return "de"
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
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT * FROM user_profiles WHERE session_id = %s", (session_id,))
    profile = c.fetchone()
    if profile:
        if new_session:
            c.execute("UPDATE user_profiles SET last_seen=%s, visit_count=visit_count+1 WHERE session_id=%s",
                      (datetime.now(), session_id))
        else:
            c.execute("UPDATE user_profiles SET last_seen=%s WHERE session_id=%s",
                      (datetime.now(), session_id))
        conn.commit()
        raw_kf = profile[14] if len(profile) > 14 else {}
        raw_th = profile[15] if len(profile) > 15 else []
        user_profile = {
            "session_id": profile[1], "first_seen": profile[2],
            "last_seen": profile[3], "visit_count": profile[4] + (1 if new_session else 0),
            "name": profile[5], "email": profile[6], "company": profile[7],
            "region_interest": profile[8], "topics_discussed": profile[9],
            "lead_score": profile[10], "last_intent": profile[11],
            "language": profile[12] if len(profile) > 12 else "en",
            "conversation_summary": profile[13] if len(profile) > 13 else None,
            "key_facts":    raw_kf if isinstance(raw_kf, dict) else {},
            "task_history": raw_th if isinstance(raw_th, list) else [],
            "is_returning": True
        }
    else:
        c.execute("INSERT INTO user_profiles (session_id, first_seen, last_seen, visit_count, language, key_facts, task_history) VALUES (%s,%s,%s,1,'en','{}','[]')",
                  (session_id, datetime.now(), datetime.now()))
        conn.commit()
        user_profile = {
            "session_id": session_id, "first_seen": datetime.now(),
            "last_seen": datetime.now(), "visit_count": 1,
            "name": None, "email": None, "company": None, "region_interest": None,
            "topics_discussed": None, "lead_score": 0, "last_intent": None,
            "language": "en", "conversation_summary": None,
            "key_facts": {}, "task_history": [], "is_returning": False
        }
    conn.close()
    return user_profile


def update_user_profile(session_id: str, **kwargs):
    conn = get_db()
    c = conn.cursor()
    valid = ['name','email','company','region_interest','topics_discussed','lead_score',
             'last_intent','language','conversation_summary','key_facts','task_history']
    updates, values = [], []
    for key, value in kwargs.items():
        if key in valid and value is not None:
            if key in ('key_facts','task_history'):
                updates.append(f"{key} = %s::jsonb")
                values.append(json.dumps(value))
            else:
                updates.append(f"{key} = %s")
                values.append(value)
    if updates:
        values.append(session_id)
        c.execute(f"UPDATE user_profiles SET {', '.join(updates)} WHERE session_id = %s", values)
        conn.commit()
    conn.close()


def calculate_lead_score(user_profile: dict, message: str, intent: str) -> int:
    score = user_profile.get('lead_score', 0)
    score += {"high_intent_lead": 30, "consultation_request": 25,
              "supplier_verification": 20, "supplier_search": 15,
              "information_gathering": 5}.get(intent, 0)
    if user_profile.get('visit_count', 1) > 1: score += 10
    if any(kw in message.lower() for kw in ["budget","invest","contract","serious","start","hire","price"]): score += 15
    lang = user_profile.get('language','en')
    if lang == 'zh': score += 20
    elif lang != 'en': score += 10
    return min(score, 100)

# ============================================================
# DATABASE FUNCTIONS
# ============================================================
def save_conversation(session_id, user_msg, ai_response,
                      email=None, company=None, region=None, intent=None, reflection_score=0):
    conn = get_db()
    c = conn.cursor()
    c.execute("INSERT INTO conversations (session_id,user_message,ai_response,timestamp,email,company,region,intent,reflection_score) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
              (session_id, user_msg, ai_response, datetime.now(), email, company, region, intent, reflection_score))
    conn.commit()
    conn.close()


def get_conversation_history(session_id, limit=10):
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT user_message, ai_response FROM conversations WHERE session_id=%s ORDER BY timestamp DESC LIMIT %s",
              (session_id, limit))
    history = c.fetchall()
    conn.close()
    return history[::-1]


def get_message_count(session_id: str) -> int:
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM conversations WHERE session_id=%s", (session_id,))
    count = c.fetchone()[0]
    conn.close()
    return count

# ============================================================
# RESPONSE CACHE
# ============================================================
def get_cached_response(query: str):
    cache_key = hashlib.md5(query.strip().lower().encode()).hexdigest()
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT response,sources,created_at FROM response_cache WHERE cache_key=%s", (cache_key,))
    row = c.fetchone()
    conn.close()
    if row:
        created = row[2] if isinstance(row[2], datetime) else datetime.fromisoformat(str(row[2]))
        if datetime.now() - created < timedelta(hours=24):
            return row[0], (json.loads(row[1]) if row[1] else [])
    return None


def set_cached_response(query: str, response: str, sources: list):
    cache_key = hashlib.md5(query.strip().lower().encode()).hexdigest()
    conn = get_db()
    c = conn.cursor()
    c.execute("INSERT INTO response_cache (cache_key,response,sources,created_at) VALUES (%s,%s,%s,%s) ON CONFLICT (cache_key) DO UPDATE SET response=EXCLUDED.response,sources=EXCLUDED.sources,created_at=EXCLUDED.created_at",
              (cache_key, response, json.dumps(sources), datetime.now()))
    conn.commit()
    conn.close()


CACHEABLE_PATTERNS = ["hainan free trade","samr","wfoe","vat","fdi rules","what is cwc",
                      "what is china west","belt and road","how to register","free trade zone",
                      "import duties","nmpa","ce certification","iso certification"]

def is_cacheable(query: str) -> bool:
    return any(p in query.lower() for p in CACHEABLE_PATTERNS)

# ============================================================
# INTENT DETECTION
# ============================================================
def detect_intent(message: str) -> dict:
    msg = message.lower()
    detected = {"primary": "general", "region": None, "score": 0}
    if any(kw in msg for kw in ["price","cost","quote","proposal","start","begin","hire","contract","serious","budget","invest"]):
        detected.update({"primary": "high_intent_lead", "score": 90})
    elif any(kw in msg for kw in ["book","consultation","call","schedule","meet","contact","talk","discuss"]):
        detected.update({"primary": "consultation_request", "score": 85})
    elif any(kw in msg for kw in ["verify","check","audit","due diligence","factory","supplier","manufacturer"]):
        detected.update({"primary": "supplier_verification", "score": 80})
    elif any(kw in msg for kw in ["find supplier","find manufacturer","source","sourcing","who makes","find factory","best supplier"]):
        detected.update({"primary": "supplier_search", "score": 75})
    elif any(kw in msg for kw in ["how","what","tell me","explain","information"]):
        detected.update({"primary": "information_gathering", "score": 40})
    regions = {
        "africa":       ["africa","african","mining","infrastructure"],
        "middle_east":  ["middle east","mea","gcc","dubai","saudi","energy","oil","gas"],
        "latam":        ["latam","latin america","brazil","mexico","argentina","chile","lithium"],
        "europe":       ["europe","eu","germany","france","green tech","automotive"],
        "central_asia": ["central asia","kazakhstan","uzbekistan","belt and road","bri"],
        "china":        ["china","chinese","mainland","prc","shenzhen","shanghai","beijing","guangzhou"]
    }
    for region, keywords in regions.items():
        if any(kw in msg for kw in keywords):
            detected["region"] = region
            break
    return detected

# ============================================================
# SEARCH FUNCTIONS
# ============================================================
def search_duckduckgo(query: str) -> tuple:
    try:
        res = requests.get("https://api.duckduckgo.com/",
                           params={"q":query,"format":"json","no_html":"1","skip_disambig":"1"}, timeout=8)
        data = res.json()
        abstract = data.get("AbstractText","")
        related  = [r.get("Text","") for r in data.get("RelatedTopics",[])[:3] if r.get("Text")]
        sources  = [data.get("AbstractURL","DuckDuckGo")] if abstract else []
        return (abstract + ("\n" + "\n".join(related) if related else "")).strip(), sources
    except Exception as e:
        print(f"DDG error: {e}"); return "", []

def search_wikipedia(query: str) -> tuple:
    try:
        import urllib.parse
        res = requests.get(f"https://en.wikipedia.org/api/rest_v1/page/summary/{urllib.parse.quote(query.replace(' ','_'))}", timeout=8)
        if res.status_code == 200:
            data = res.json()
            extract = data.get("extract","")
            url = data.get("content_urls",{}).get("desktop",{}).get("page","Wikipedia")
            if extract: return extract[:600], [url]
        return "", []
    except Exception as e:
        print(f"Wiki error: {e}"); return "", []

def search_tavily(query: str) -> tuple:
    if not TAVILY_API_KEY: return "", []
    year = datetime.now().year
    is_news = any(kw in query.lower() for kw in ["news","latest","update","today","recent"])
    enhanced = f"{query} China business trade {year} latest news" if is_news else f"{query} China business {year}"
    try:
        res = requests.post("https://api.tavily.com/search",
                            json={"api_key":TAVILY_API_KEY,"query":enhanced,"max_results":3,"search_depth":"advanced","include_answer":True}, timeout=10)
        res.raise_for_status()
        data = res.json()
        answer = data.get("answer","")
        results = data.get("results",[])
        sources = [r.get("url","") for r in results if r.get("url")]
        return (answer + "\n" + "\n".join(r.get("content","") for r in results[:2])).strip(), sources
    except Exception as e:
        print(f"Tavily error: {e}"); return "", []

def search_web(query: str) -> tuple:
    all_content, all_sources = [], []
    tv, ts = search_tavily(query)
    if tv: all_content.append(tv); all_sources.extend(ts)
    if not tv:
        dq, ds = search_duckduckgo(query)
        if dq: all_content.append(dq); all_sources.extend(ds)
    wq, ws = search_wikipedia(query)
    if wq and len("\n".join(all_content)) < 400:
        all_content.append(f"Background: {wq}"); all_sources.extend(ws)
    return "\n\n".join(all_content), list(dict.fromkeys(s for s in all_sources if s))[:4]

# ============================================================
# RSS NEWS FEED
# ============================================================
_news_cache: dict = {"items": [], "fetched_at": None}

def fetch_china_news() -> list:
    global _news_cache
    now = datetime.now()
    if _news_cache["fetched_at"] and now - _news_cache["fetched_at"] < timedelta(hours=2) and _news_cache["items"]:
        return _news_cache["items"]
    feeds = ["https://www.scmp.com/rss/2/feed","https://www.scmp.com/rss/4/feed",
             "https://www.caixinglobal.com/rss/latest-stories.xml",
             "https://www.chinadaily.com.cn/rss/bizchina_rss.xml",
             "https://www.xinhuanet.com/english/rss/financerss.xml"]
    items = []
    for feed_url in feeds:
        try:
            res = requests.get(feed_url, timeout=8, headers={"User-Agent":"Mozilla/5.0 CWC-Sophia/5.0"})
            if res.status_code != 200: continue
            for block in re.findall(r'<item[^>]*>(.*?)</item>', res.text, re.DOTALL)[:5]:
                tm = re.search(r'<title><!\[CDATA\[(.*?)\]\]></title>|<title>(.*?)</title>', block, re.DOTALL)
                if not tm: continue
                title = re.sub(r'<[^>]+>','', (tm.group(1) or tm.group(2) or "")).strip()
                if not title or len(title) < 10: continue
                lm = re.search(r'<link>(https?://[^<]+)</link>', block) or re.search(r'<guid[^>]*>(https?://[^<]+)</guid>', block)
                link = lm.group(1).strip() if lm else feed_url
                dm = re.search(r'<pubDate>(.*?)</pubDate>', block)
                date = dm.group(1).strip()[:16] if dm else ""
                if any(kw in title.lower() for kw in ["ukraine","russia","greenland","epstein","nato","israel","gaza","afghanistan"]): continue
                cat = "China Business"
                if any(w in title.lower() for w in ["trade","tariff","export","import","wto"]):   cat = "Trade"
                elif any(w in title.lower() for w in ["invest","fdi","fund","deal"]):              cat = "Investment"
                elif any(w in title.lower() for w in ["policy","regulat","law","rule"]):           cat = "Policy"
                elif any(w in title.lower() for w in ["tech","ai","robot","digital"]):             cat = "Technology"
                elif any(w in title.lower() for w in ["energy","solar","ev","battery","green"]):   cat = "Energy"
                elif any(w in title.lower() for w in ["pharma","biotech","health","medical"]):     cat = "Biotech"
                elif any(w in title.lower() for w in ["ship","freight","logistics","port"]):       cat = "Logistics"
                items.append({"title":title,"url":link,"category":cat,"date":date})
            if len(items) >= 8: break
        except Exception as e: print(f"RSS error ({feed_url}): {e}")
    if items:
        _news_cache["items"] = items[:8]; _news_cache["fetched_at"] = now; return items[:8]
    return [
        {"title":"China announces new FDI incentives for tech sector","url":"","category":"Policy","date":""},
        {"title":"Chinese EV makers accelerate LATAM battery investments","url":"","category":"Investment","date":""},
        {"title":"Major lithium partnerships: Chinese and African firms","url":"","category":"Trade","date":""},
        {"title":"UAE and China launch cross-border digital currency pilot","url":"","category":"Fintech","date":""},
        {"title":"New due diligence requirements for foreign buyers","url":"","category":"Compliance","date":""},
    ]

# ============================================================
# COMPANY LOOKUP
# ============================================================
def lookup_chinese_company(company_name: str) -> dict:
    result = {"company":company_name,"found":False,"registration_status":"Unknown","details":"","sources":[],"warning":None}
    query = f"{company_name} China company registration SAMR business license"
    ddg, ddg_s = search_duckduckgo(query)
    tav, tav_s = search_tavily(query)
    combined = (tav or ddg or "").lower()
    if combined:
        result["found"] = True
        result["sources"] = (tav_s + ddg_s)[:3]
        flags = [f for f in ["scam","fraud","fake","blacklist","warning","complaint","dispute","lawsuit","suspended","revoked"] if f in combined]
        if flags:
            result["warning"] = f"⚠️ Red flags: {', '.join(flags)}"
            result["registration_status"] = "Requires Investigation"
        else:
            result["registration_status"] = "Preliminary search complete — full audit recommended"
        result["details"] = (tav or ddg)[:400]
    else:
        result["details"] = "No public data found. Full CWC Due Diligence strongly recommended."
        result["warning"] = "⚠️ No public data found — treat with caution"
    return result

# ============================================================
# v5: ACCIO-STYLE SUPPLIER SEARCH ENGINE
# ============================================================
def search_suppliers(product_or_sector: str, region: str = "") -> dict:
    year = datetime.now().year
    queries = [
        f"China {product_or_sector} manufacturer exporter verified {year}",
        f"{product_or_sector} Chinese supplier factory MOQ price certification",
    ]
    if region: queries.append(f"China {product_or_sector} export to {region}")
    all_raw, all_sources = [], []
    for q in queries[:2]:
        content, sources = search_web(q)
        if content:
            all_raw.append(content[:600]); all_sources.extend(sources)
    if not GROQ_API_KEY or not all_raw:
        return {"market_context":"","sources":[]}
    raw_text = "\n\n".join(all_raw)
    try:
        res = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization":f"Bearer {GROQ_API_KEY}","Content-Type":"application/json"},
            json={
                "model":"llama-3.3-70b-versatile",
                "messages":[
                    {"role":"system","content":(
                        "You are a China B2B sourcing analyst. Extract structured supplier intelligence from raw web data. "
                        "Return ONLY valid JSON: {\"market_context\":\"2-3 sentence overview\","
                        "\"key_considerations\":[\"3-4 important factors\"],"
                        "\"typical_moq\":\"MOQ range\",\"price_range\":\"price range if available\","
                        "\"certifications_required\":[\"list\"],\"top_regions\":[\"Chinese manufacturing regions\"],"
                        "\"red_flags\":[\"common fraud/quality risks\"],"
                        "\"cwc_recommendation\":\"one sentence how CWC can specifically help\"}"
                        " Return ONLY JSON."
                    )},
                    {"role":"user","content":f"Product/sector: {product_or_sector}\nRegion: {region or 'Global'}\n\n{raw_text}"}
                ],
                "temperature":0.1,"max_tokens":600
            }, timeout=15
        )
        raw = res.json()["choices"][0]["message"]["content"].strip()
        raw = re.sub(r"^```json\s*|```$","",raw.strip(),flags=re.MULTILINE).strip()
        structured = json.loads(raw)
        structured["sources"] = list(dict.fromkeys(s for s in all_sources if s))[:4]
        return structured
    except Exception as e:
        print(f"Supplier structuring error: {e}")
        return {"market_context":raw_text[:300],"sources":all_sources[:3]}

# ============================================================
# v5: TASK DECOMPOSITION ENGINE
# ============================================================
def decompose_task(user_message: str, user_profile: dict) -> dict | None:
    complex_triggers = ["help me find","i want to source","find suppliers for","market entry plan",
                        "full due diligence","compare","analyse","research and recommend",
                        "step by step","comprehensive","full report","everything about"]
    if not any(t in user_message.lower() for t in complex_triggers): return None
    if not GROQ_API_KEY: return None
    key_facts = user_profile.get('key_facts',{})
    try:
        res = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization":f"Bearer {GROQ_API_KEY}","Content-Type":"application/json"},
            json={
                "model":"llama-3.3-70b-versatile",
                "messages":[
                    {"role":"system","content":(
                        "You are a China trade intelligence task planner. Break complex requests into 2-4 sequential sub-tasks. "
                        "Return ONLY JSON: {\"is_complex\":true,\"task_summary\":\"one line\","
                        "\"sub_tasks\":[{\"step\":1,\"action\":\"search_market|lookup_company|search_suppliers|generate_risk_report\","
                        "\"query\":\"...\",\"reason\":\"...\"}],"
                        "\"expected_output\":\"what final response should contain\"}"
                    )},
                    {"role":"user","content":f"Request: {user_message}\nContext: {json.dumps(key_facts)}"}
                ],
                "temperature":0.1,"max_tokens":400
            }, timeout=10
        )
        raw = res.json()["choices"][0]["message"]["content"].strip()
        raw = re.sub(r"^```json\s*|```$","",raw.strip(),flags=re.MULTILINE).strip()
        plan = json.loads(raw)
        if plan.get('is_complex') and plan.get('sub_tasks'):
            print(f"📋 Decomposed: {plan['task_summary']} ({len(plan['sub_tasks'])} steps)")
            return plan
    except Exception as e:
        print(f"Decomposition error: {e}")
    return None

# ============================================================
# v5: SPECIALIST SUB-AGENTS
# ============================================================
def run_specialist_agent(agent_type: str, context: str) -> str:
    if not GROQ_API_KEY: return ""
    personas = {
        "due_diligence": (
            "You are a China due diligence specialist with 15 years experience. "
            "Focus on: SAMR registration, red flag detection, financial health, certificate authenticity, "
            "ownership structure, litigation history. Be direct about risks. Always recommend next steps."
        ),
        "market_entry": (
            "You are a China market entry strategist. Focus on: entity structures (WFOE, JV, RO, FICE), "
            "timeline, capital requirements, licensing, sector restrictions, typical pitfalls. "
            "Give a practical phased roadmap with specific costs and timelines."
        ),
        "legal": (
            "You are a bilingual China business lawyer. Focus on: contract structure, IP protection, "
            "dispute resolution, governing law, force majeure, payment terms, liability caps. "
            "Flag common Western mistakes in China contracts."
        ),
        "logistics": (
            "You are a China export logistics expert. Focus on: Incoterms, freight (FCL/LCL/air), "
            "customs HS codes, documentation (packing list, COO, BL), typical lead times, port congestion, "
            "cost optimisation. Be specific with numbers."
        ),
        "supplier_match": (
            "You are a China sourcing specialist. Focus on: supplier qualification criteria, "
            "factory audit checklist, sample order process, MOQ negotiation, payment terms (TT vs LC), "
            "quality control, and IP protection when working with Chinese manufacturers."
        ),
    }
    persona = personas.get(agent_type, personas["due_diligence"])
    try:
        res = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization":f"Bearer {GROQ_API_KEY}","Content-Type":"application/json"},
            json={
                "model":"llama-3.3-70b-versatile",
                "messages":[
                    {"role":"system","content":persona},
                    {"role":"user","content":f"Specialist analysis needed for:\n{context}"}
                ],
                "temperature":0.2,"max_tokens":400
            }, timeout=15
        )
        return res.json()["choices"][0]["message"]["content"]
    except Exception as e:
        print(f"Specialist agent error ({agent_type}): {e}")
        return ""

# ============================================================
# GROQ TOOL DEFINITIONS — v5 (5 tools)
# ============================================================
GROQ_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "search_market_intelligence",
            "description": "Search live China business intel, market data, trade news, regulatory updates.",
            "parameters": {"type":"object","properties":{
                "query":{"type":"string"},
                "search_type":{"type":"string","enum":["market_news","company_lookup","regulation","general"]}
            },"required":["query","search_type"]}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "lookup_company",
            "description": "Look up a Chinese company in public registries for verification and due diligence.",
            "parameters": {"type":"object","properties":{"company_name":{"type":"string"}},"required":["company_name"]}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "generate_risk_report",
            "description": "Generate a full structured due diligence risk report for a Chinese company. Use when user wants safety assessment before payment or contract.",
            "parameters": {"type":"object","properties":{
                "company_name":{"type":"string"},
                "context":{"type":"string","description":"What user plans to do with this company"}
            },"required":["company_name"]}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "find_suppliers",
            "description": (
                "Accio-style supplier discovery. Search for Chinese manufacturers/suppliers for a product or sector. "
                "Returns structured market intel: MOQ ranges, price ranges, required certifications, top manufacturing regions, "
                "common red flags. Use when user wants to source products from China or find manufacturing partners."
            ),
            "parameters": {"type":"object","properties":{
                "product_or_sector":{"type":"string","description":"Product or sector to source"},
                "destination_region":{"type":"string","description":"Buyer country/region (e.g. Europe, USA)"},
                "additional_requirements":{"type":"string","description":"Certifications, MOQ, etc."}
            },"required":["product_or_sector"]}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "reflect_and_improve",
            "description": (
                "Self-reflection tool. After drafting a response, use this to evaluate quality. "
                "Checks: (1) answers real need not just literal question, (2) uses specific facts not generalities, "
                "(3) advances the CWC sales conversation, (4) has clear next step. "
                "Returns score 1-10 and improvement instructions if needed. "
                "ALWAYS use this before finalising a response to a complex or high-value user query."
            ),
            "parameters": {"type":"object","properties":{
                "draft_response":{"type":"string"},
                "user_need":{"type":"string"}
            },"required":["draft_response","user_need"]}
        }
    }
]


def run_tool_call(tool_name: str, tool_args: dict) -> tuple:
    if tool_name == "search_market_intelligence":
        query = tool_args.get("query","")
        st = tool_args.get("search_type","general")
        if st == "market_news":     query = f"{query} latest news 2025 2026"
        elif st == "company_lookup": query = f"{query} China company profile registration"
        elif st == "regulation":    query = f"{query} China regulation compliance 2026"
        return search_web(query)

    elif tool_name == "lookup_company":
        r = lookup_chinese_company(tool_args.get("company_name",""))
        summary = f"Company: {r['company']}\nStatus: {r['registration_status']}\nDetails: {r['details']}\n"
        if r.get('warning'): summary += f"WARNING: {r['warning']}\n"
        return summary, r.get('sources',[])

    elif tool_name == "generate_risk_report":
        name    = tool_args.get("company_name","")
        context = tool_args.get("context","business engagement")
        lookup  = lookup_chinese_company(name)
        risk, rs = search_web(f"{name} China fraud scam complaints blacklist 2024 2025")
        news, ns = search_web(f"{name} China company news recent 2025")
        all_src = lookup.get('sources',[]) + rs + ns
        report = (
            f"=== CWC RISK REPORT: {name} ===\n"
            f"Context: {context}\n"
            f"REGISTRATION: {lookup['registration_status']}\n"
            f"Details: {lookup['details'][:300]}\n"
            f"{('⚠️ ' + lookup['warning']) if lookup.get('warning') else '✅ No registry red flags.'}\n"
            f"RISK SIGNALS: {risk[:300] if risk else 'None found in open sources.'}\n"
            f"RECENT NEWS: {news[:300] if news else 'No recent news found.'}\n"
            f"VERDICT: {'⚠️ ESCALATE — Risk signals detected. Full CWC audit required.' if lookup.get('warning') else '✅ No critical signals. Standard CWC verification recommended.'}"
        )
        return report, list(dict.fromkeys(s for s in all_src if s))[:5]

    elif tool_name == "find_suppliers":
        product = tool_args.get("product_or_sector","")
        region  = tool_args.get("destination_region","")
        reqs    = tool_args.get("additional_requirements","")
        result  = search_suppliers(product, region)
        lines = [f"=== SUPPLIER INTELLIGENCE: {product} ===",
                 f"Target: {region or 'Global'} | Requirements: {reqs or 'Not specified'}",
                 f"\nMARKET CONTEXT: {result.get('market_context','N/A')}",
                 "\nKEY CONSIDERATIONS:"]
        lines += ["• " + k for k in result.get('key_considerations',[])]
        lines += [f"\nTYPICAL MOQ: {result.get('typical_moq','Varies')}",
                  f"PRICE RANGE: {result.get('price_range','Request quotes')}",
                  "\nCERTIFICATIONS REQUIRED:"]
        lines += ["• " + c for c in result.get('certifications_required',[])]
        lines += ["\nTOP MANUFACTURING REGIONS:"]
        lines += ["• " + r for r in result.get('top_regions',[])]
        lines += ["\nRED FLAGS:"]
        lines += ["⚠️ " + f for f in result.get('red_flags',[])]
        lines.append(f"\nCWC: {result.get('cwc_recommendation','Contact CWC for verified supplier matching.')}")
        return "\n".join(lines), result.get('sources',[])

    elif tool_name == "reflect_and_improve":
        draft = tool_args.get("draft_response","")
        need  = tool_args.get("user_need","")
        if not GROQ_API_KEY: return "Reflection unavailable.", []
        try:
            res = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization":f"Bearer {GROQ_API_KEY}","Content-Type":"application/json"},
                json={
                    "model":"llama-3.3-70b-versatile",
                    "messages":[
                        {"role":"system","content":(
                            "Senior CWC quality reviewer. Evaluate this draft against user need. Be harsh. "
                            "Return ONLY JSON: {\"score\":1-10,\"passes\":true/false,"
                            "\"issues\":[\"problems\"],\"improvement_instructions\":\"rewrite guidance or empty\"}"
                        )},
                        {"role":"user","content":f"User need: {need}\n\nDraft:\n{draft}"}
                    ],
                    "temperature":0.0,"max_tokens":300
                }, timeout=10
            )
            raw = re.sub(r"^```json\s*|```$","",res.json()["choices"][0]["message"]["content"].strip(),flags=re.MULTILINE).strip()
            ev = json.loads(raw)
            score = ev.get('score',5)
            if ev.get('passes',True) or score >= 7:
                return f"REFLECTION PASSED (score {score}/10). Response is good. Proceed.", []
            issues = "\n".join(ev.get('issues',[]))
            return (f"REFLECTION FAILED (score {score}/10).\nISSUES:\n{issues}\n"
                    f"REWRITE: {ev.get('improvement_instructions','')}"), []
        except Exception as e:
            print(f"Reflection error: {e}")
            return "Reflection unavailable — proceed with current response.", []

    return "", []

# ============================================================
# QUICK ACTION OPENERS
# ============================================================
QUICK_ACTION_OPENERS = {
    "robotics": (
        "Great choice — China is currently the world's largest industrial robotics market, "
        "producing over 70% of global units.\n\n"
        "Before I connect you with the right intelligence:\n\n"
        "Are you looking to **SOURCE** robotics technology from China, "
        "or are you a Chinese robotics company seeking **Western partners or markets**?"
    ),
    "energy": (
        "Energy is one of the most dynamic China-West collaboration areas right now. "
        "China accounts for over 80% of global solar production and leads in battery storage.\n\n"
        "What's your energy focus?\n\n"
        "① Solar PV — panels, inverters, mounting systems\n"
        "② Battery storage — utility-scale or commercial\n"
        "③ EV charging infrastructure\n"
        "④ Wind energy components\n"
        "⑤ Green hydrogen\n"
        "⑥ Energy trading or investment"
    ),
    "biotech": (
        "China is now the world's second-largest pharma market and leads in biosimilar manufacturing.\n\n"
        "What brings you here?\n\n"
        "① Western pharma seeking Chinese CMO/CDMO partners\n"
        "② Licensing Chinese biotech innovations for Western markets\n"
        "③ Entering the Chinese healthcare market with a Western product\n"
        "④ R&D or clinical trial partnerships\n"
        "⑤ Medical devices"
    ),
    "shipping": (
        "China handles over 30% of global container shipping volume.\n\n"
        "What's your shipping challenge?\n\n"
        "① Moving goods FROM China (import logistics)\n"
        "② Shipping TO China (export logistics)\n"
        "③ Optimising existing supply chain\n"
        "④ Customs clearance and documentation\n"
        "⑤ Maritime technology partnerships"
    ),
    "verify": (
        "Smart move. Verifying before contracts or payments is critical in China business.\n\n"
        "What do you need to verify?\n\n"
        "① A Chinese supplier or manufacturer\n"
        "② A business partner or JV candidate\n"
        "③ A Chinese investment target\n"
        "④ Certificates or documents from a Chinese company\n"
        "⑤ A Chinese individual's background"
    ),
    "market_entry": (
        "Market entry is CWC's core expertise.\n\n"
        "First — your direction:\n\n"
        "① We are a **Western company** entering the Chinese market\n"
        "② We are a **Chinese company** expanding into Western markets\n"
        "③ Bilateral partnership or trade\n"
        "④ Still exploring the opportunity"
    )
}

# ============================================================
# CONVERSATION SUMMARY & HANDOFF
# ============================================================
def generate_handoff_brief(session_id: str, user_profile: dict) -> str:
    history   = get_conversation_history(session_id, limit=20)
    conv_text = "\n".join([f"User: {u}\nSophia: {a}" for u, a in history])
    name      = user_profile.get('name') or 'Unknown'
    email     = user_profile.get('email') or 'Not captured'
    company   = user_profile.get('company') or 'Not provided'
    region    = user_profile.get('region_interest') or 'Not specified'
    score     = user_profile.get('lead_score', 0)
    visits    = user_profile.get('visit_count', 1)
    lang      = user_profile.get('language', 'en')
    intent    = user_profile.get('last_intent', 'Unknown')
    key_facts = user_profile.get('key_facts', {})
    facts_text = "\n".join([f"   {k}: {v}" for k, v in key_facts.items() if v]) or "   Not yet extracted."
    priority  = "🔥 HOT — Contact within 24h" if score >= 70 else ("🟡 WARM — Follow up 48h" if score >= 40 else "🔵 COLD — Nurture")
    return f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📋 SOPHIA v5 HANDOFF BRIEF
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
👤 CONTACT
   Name:     {name}
   Email:    {email}
   Company:  {company}
   Region:   {region}
   Language: {lang.upper()}

📊 LEAD INTELLIGENCE
   Score:    {score}/100 | {priority}
   Visits:   {visits} | Intent: {intent}

🧠 AI KEY FACTS
{facts_text}

💬 CONVERSATION
{conv_text[:1500] if conv_text else 'No conversation recorded'}

⚡ ACTION: {_recommend_action(score, intent, region)}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""


def _recommend_action(score: int, intent: str, region: str) -> str:
    if intent == "supplier_verification": return "URGENT: Due diligence needed. SAMR check + factory audit proposal."
    elif intent == "supplier_search":     return f"User wants supplier matching in {region or 'target market'}. Prepare shortlist."
    elif intent == "high_intent_lead" and score >= 60: return f"HIGH VALUE: Prepare service proposal for {region or 'target market'}."
    elif intent == "consultation_request": return "Schedule discovery call ASAP."
    elif region: return f"Prepare {region} market brief."
    else: return "Send personalised intro email with CWC capabilities deck."


def check_qualification_gaps(user_profile: dict, message_count: int) -> str | None:
    if message_count < 3: return None
    missing = []
    if not user_profile.get('region_interest'): missing.append("direction (Western→China or Chinese→West)")
    if not user_profile.get('topics_discussed'): missing.append("sector/industry")
    if not user_profile.get('last_intent') or user_profile.get('last_intent') == 'general': missing.append("specific goal")
    if len(missing) >= 2:
        return (f"\n⚡ QUALIFY NOW: After {message_count} messages you still don't know: {', '.join(missing)}. "
                "Ask ONE direct qualifying question before answering. Warm tone: 'Before I go further — can I ask...'")
    return None


def check_escalation_trigger(user_profile: dict, message_count: int, current_message: str) -> bool:
    urgency = ["urgent","asap","immediately","today","deposit","already paid","already transferred","fraud","scam","lost money","emergency"]
    if any(w in current_message.lower() for w in urgency): return True
    if user_profile.get('lead_score',0) >= 75 and message_count >= 4: return True
    return False

# ============================================================
# MAIN AI FUNCTION — v5 FULLY AGENTIC
# ============================================================
def ask_groq(prompt: str, session_id: str = "anonymous",
             user_profile: dict = None, quick_action: str = None,
             deep_search: bool = False) -> tuple:

    if not GROQ_API_KEY:
        return "System temporarily unavailable. Please contact the CWC team directly.", []

    detected_lang = detect_language(prompt)
    if detected_lang != "en" and user_profile:
        update_user_profile(session_id, language=detected_lang)
        user_profile['language'] = detected_lang

    lang = (user_profile or {}).get('language','en') if user_profile else detected_lang
    lang_instruction = LANGUAGE_INSTRUCTIONS.get(lang,"")

    raw_history = get_conversation_history(session_id, limit=8)
    messages = []
    for um, ar in raw_history:
        messages.append({"role":"user","content":um})
        messages.append({"role":"assistant","content":ar})
    messages.append({"role":"user","content":prompt})

    message_count = get_message_count(session_id)
    intent_data   = detect_intent(prompt)

    # ── v5 TASK DECOMPOSITION ─────────────────────────────────────────────
    specialist_context = ""
    if deep_search:
        task_plan = decompose_task(prompt, user_profile or {})
        if task_plan:
            sub_results = []
            for sub in task_plan.get('sub_tasks',[])[:3]:
                action = sub.get('action','')
                query  = sub.get('query','')
                if action == "search_market" and query:
                    c, _ = search_web(query)
                    sub_results.append(f"[Market Research] {c[:400]}")
                elif action == "lookup_company" and query:
                    r = lookup_chinese_company(query)
                    sub_results.append(f"[Company: {query}] {r['registration_status']} — {r['details'][:200]}")
                elif action == "search_suppliers" and query:
                    r = search_suppliers(query)
                    sub_results.append(f"[Suppliers: {query}] {r.get('market_context','')[:300]}")
                elif action == "generate_risk_report" and query:
                    r = lookup_chinese_company(query)
                    rk, _ = search_web(f"{query} China fraud scam 2025")
                    sub_results.append(f"[Risk: {query}] {r['registration_status']} — {rk[:200]}")
            if sub_results:
                specialist_context = (f"\n\n📋 TASK PLAN: {task_plan.get('task_summary','')}\n"
                                     + "\n\n".join(sub_results)
                                     + f"\n\nExpected: {task_plan.get('expected_output','')}")

        # ── v5 SPECIALIST SUB-AGENT ────────────────────────────────────────
        agent_map = {"supplier_verification":"due_diligence","supplier_search":"supplier_match","consultation_request":"market_entry"}
        if intent_data['primary'] in agent_map:
            agent_type = agent_map[intent_data['primary']]
            print(f"🤖 Specialist sub-agent: {agent_type}")
            output = run_specialist_agent(agent_type, prompt)
            if output:
                specialist_context += f"\n\n🎓 SPECIALIST ({agent_type.upper().replace('_',' ')}):\n{output}"

    returning_context = ""
    if user_profile and user_profile.get('is_returning'):
        kf = user_profile.get('key_facts',{})
        facts_str = ", ".join([f"{k}:{v}" for k,v in kf.items() if v]) if kf else "none"
        returning_context = (
            f"\nRETURNING USER: Visit #{user_profile.get('visit_count',1)} | "
            f"Name: {user_profile.get('name') or 'Unknown'} | "
            f"Intent: {user_profile.get('last_intent','?')} | "
            f"Region: {user_profile.get('region_interest','?')} | "
            f"Score: {user_profile.get('lead_score',0)}/100 | "
            f"Key facts: {facts_str}\n"
            f"Summary: {user_profile.get('conversation_summary') or 'First tracked session'}\n"
            "INSTRUCTION: Reference previous interest naturally. Don't re-introduce yourself."
        )

    qualification_prompt   = check_qualification_gaps(user_profile or {}, message_count)
    should_escalate        = check_escalation_trigger(user_profile or {}, message_count, prompt)
    escalation_instruction = ("\n🚨 ESCALATION: Urgent/high intent. End response directing them to 'Speak with Michail' button."
                               if should_escalate else "")

    sector_context = ""
    if quick_action:
        sector_map = {
            "robotics":"ACTIVE: ROBOTICS — sourcing vs Chinese expansion. Factory audits, CE, IP.",
            "energy":"ACTIVE: ENERGY — solar/battery/EV/wind/hydrogen. Ask scale (MW) and deal structure.",
            "biotech":"ACTIVE: BIOTECH — CMO/CDMO, pharma entry, R&D. Ask molecule type, GMP.",
            "shipping":"ACTIVE: SHIPPING — import/export, customs. Ask volume (FCL/LCL/air).",
            "verify":"ACTIVE: DUE DILIGENCE — URGENT. Ask company name, amounts at risk.",
            "market_entry":"ACTIVE: MARKET ENTRY — Determine direction. Deliver phased roadmap.",
        }
        sector_context = sector_map.get(quick_action,"")

    system_prompt = f"""You are Sophia — official AI advisor for China West Connector (CWC).
Version 5.0 | Deep Search: {'ON' if deep_search else 'OFF'} | {datetime.now().strftime('%B %Y')}

INTENT: {intent_data['primary']} | REGION: {intent_data['region'] or '?'} | MESSAGES: {message_count}
{lang_instruction}
{returning_context}
{sector_context}
{qualification_prompt or ''}
{escalation_instruction}
{specialist_context}

━━━ CHAIN OF THOUGHT (always execute) ━━━
1. What does the user ACTUALLY need (beyond what they literally asked)?
2. What do I know from their profile and pre-gathered intelligence above?
3. Do I need more tools — or is pre-gathered intel sufficient?
4. Which CWC service maps most directly to their need?
5. What is the single most valuable next step?
6. After drafting, CALL reflect_and_improve to self-evaluate. Rewrite if score < 7.

TOOLS: search_market_intelligence | lookup_company | generate_risk_report | find_suppliers | reflect_and_improve

━━━ MISSION ━━━
You are NOT a Q&A bot. You are an active business advisor.
1. QUALIFY (direction, sector, goal, urgency)
2. PERSONALISE using everything known about this user
3. RECOMMEND CWC service with clear reasoning
4. END with a concrete next step — never a dead end

QUALIFICATION PRIORITY: ① West→China or China→West? ② Sector? ③ Goal? ④ Urgency?

━━━ ABOUT CWC ━━━
Bridges Chinese and Western businesses. Founded by Michail Digkas — international business lawyer, 10+ years China.
Member of G.P.A. Group: 147+ years combined experience, 2,700+ projects, 50+ countries.
CRITICAL: These are G.P.A. Group figures. Never say CWC alone has 2,700 projects.
Gov partners: Sichuan Tech Transfer, Chengdu AI Association, Tianfu Tech Center, Hainan FTZ, CISTEA.
Services: Legal | Due Diligence | B2B Partnerships | FDI Consulting | Logistics | Liaison
Regions: Europe • Africa • Middle East • LATAM • Central Asia • North America

━━━ RESPONSE STRATEGY ━━━
supplier_search       → USE find_suppliers. Present structured intel. Offer CWC verified matching.
supplier_verification → URGENT. USE generate_risk_report. Ask company name + amounts. Escalate.
high_intent_lead      → 1-2 qualifying questions + CWC recommendation + push to Michail
consultation_request  → Confirm CWC can help + 'Speak with Michail' button
information_gathering → Specific insight with data, then offer deeper consultation

STYLE: Max 200 words | Sharp, specific, commercial | No buzzwords | Specific numbers
Escalate → "click the 'Speak with Michail' button above"
FIRST MESSAGE (no history, no quick action): Introduce as Sophia, ask direction.
"""

    url     = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization":f"Bearer {GROQ_API_KEY}","Content-Type":"application/json"}
    all_sources      = []
    reflection_score = 5

    MAX_ITERATIONS   = 6
    response_text    = ""
    current_messages = [{"role":"system","content":system_prompt}] + list(messages)

    try:
        for iteration in range(MAX_ITERATIONS):
            data = {
                "model":"llama-3.3-70b-versatile",
                "messages":current_messages,
                "tools":GROQ_TOOLS,
                "tool_choice":"auto",
                "temperature":0.3,
                "max_tokens":900
            }
            res = requests.post(url, headers=headers, json=data, timeout=25)
            res.raise_for_status()
            choice      = res.json()["choices"][0]
            message_obj = choice["message"]
            current_messages.append(message_obj)

            if choice.get("finish_reason") != "tool_calls" or not message_obj.get("tool_calls"):
                response_text = message_obj.get("content","")
                break

            for tool_call in message_obj.get("tool_calls",[]):
                fn_name = tool_call["function"]["name"]
                fn_args = json.loads(tool_call["function"]["arguments"])
                print(f"🔧 [{iteration+1}] {fn_name}({list(fn_args.keys())})")
                tool_result, sources = run_tool_call(fn_name, fn_args)
                all_sources.extend(sources)
                if fn_name == "reflect_and_improve":
                    m = re.search(r'score (\d+)/10', tool_result)
                    if m: reflection_score = int(m.group(1))
                current_messages.append({
                    "role":"tool","tool_call_id":tool_call["id"],
                    "content":tool_result or "No results found."
                })

        if not response_text:
            res2 = requests.post(url, headers=headers, json={**data,"tools":[],"tool_choice":"none"}, timeout=25)
            res2.raise_for_status()
            response_text = res2.json()["choices"][0]["message"]["content"]

        new_score = calculate_lead_score(user_profile or {}, prompt, intent_data['primary'])
        update_user_profile(session_id, last_intent=intent_data['primary'],
                            region_interest=intent_data['region'], lead_score=new_score, language=lang)
        save_conversation(session_id, prompt, response_text,
                          region=intent_data['region'], intent=intent_data['primary'],
                          reflection_score=reflection_score)

        if message_count > 0 and message_count % 5 == 0:
            _update_conversation_summary(session_id)
        if message_count > 0 and message_count % 3 == 0:
            _extract_and_save_key_facts(session_id, user_profile or {})

        return response_text, list(dict.fromkeys(s for s in all_sources if s))[:5]

    except Exception as e:
        print(f"Groq error: {e}")
        return "I apologise — connection trouble. Please reach out to the CWC team directly.", []


def _update_conversation_summary(session_id: str):
    if not GROQ_API_KEY: return
    history = get_conversation_history(session_id, limit=10)
    if not history: return
    conv_text = "\n".join([f"User: {u}\nSophia: {a[:100]}" for u, a in history])
    try:
        res = requests.post("https://api.groq.com/openai/v1/chat/completions",
                            headers={"Authorization":f"Bearer {GROQ_API_KEY}","Content-Type":"application/json"},
                            json={"model":"llama-3.3-70b-versatile","messages":[
                                {"role":"system","content":"Summarise this conversation in 2-3 sentences: what user wants, direction, sector, urgency. Factual."},
                                {"role":"user","content":conv_text}
                            ],"temperature":0.1,"max_tokens":150}, timeout=10)
        update_user_profile(session_id, conversation_summary=res.json()["choices"][0]["message"]["content"])
    except Exception as e: print(f"Summary error: {e}")


def _extract_and_save_key_facts(session_id: str, user_profile: dict):
    if not GROQ_API_KEY: return
    history = get_conversation_history(session_id, limit=6)
    if not history: return
    conv_text = "\n".join([f"User: {u}\nSophia: {a[:80]}" for u, a in history])
    existing  = user_profile.get('key_facts',{}) or {}
    try:
        res = requests.post("https://api.groq.com/openai/v1/chat/completions",
                            headers={"Authorization":f"Bearer {GROQ_API_KEY}","Content-Type":"application/json"},
                            json={"model":"llama-3.3-70b-versatile","messages":[
                                {"role":"system","content":(
                                    "Extract business facts from conversation. Return ONLY JSON with keys "
                                    "(null if unknown): direction, sector, goal, urgency, company_name, "
                                    "supplier_names (array), budget_mentioned (bool), target_market. No markdown."
                                )},
                                {"role":"user","content":conv_text}
                            ],"temperature":0.0,"max_tokens":200}, timeout=10)
        raw = re.sub(r"^```json\s*|```$","",res.json()["choices"][0]["message"]["content"].strip(),flags=re.MULTILINE).strip()
        extracted = json.loads(raw)
        for k, v in extracted.items():
            if v is not None and v != [] and v is not False: existing[k] = v
            elif k not in existing: existing[k] = v
        update_user_profile(session_id, key_facts=existing)
        print(f"🧠 Facts: {existing}")
    except Exception as e: print(f"Key facts error: {e}")

# ============================================================
# EMAIL FUNCTIONS
# ============================================================
def send_email_brevo(to_email: str, subject: str, body: str, from_name: str = "CWC AI") -> bool:
    try:
        res = requests.post("https://api.brevo.com/v3/smtp/email",
                            headers={"accept":"application/json","content-type":"application/json","api-key":BREVO_API_KEY},
                            json={"sender":{"name":from_name,"email":SENDER_EMAIL},"to":[{"email":to_email,"name":"Michail Digkas"}],
                                  "subject":subject,
                                  "htmlContent":f"<html><body><pre style='font-family:monospace;white-space:pre-wrap;'>{body}</pre></body></html>",
                                  "textContent":body}, timeout=10)
        if res.status_code == 201: print(f"✅ Email sent to {to_email}"); return True
        print(f"❌ Brevo error: {res.status_code}"); return False
    except Exception as e: print(f"❌ Email failed: {e}"); return False


def send_lead_notification(lead: LeadCapture):
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT * FROM user_profiles WHERE session_id=%s", (lead.session_id,))
    pr = c.fetchone(); conn.close()
    lead_score  = pr[10] if pr else 0
    visit_count = pr[4]  if pr else 1
    user_profile = {}
    if pr:
        raw_kf = pr[14] if len(pr) > 14 else {}
        user_profile = {"name":pr[5],"email":pr[6],"company":pr[7],"region_interest":pr[8],
                        "topics_discussed":pr[9],"lead_score":pr[10],"last_intent":pr[11],
                        "visit_count":pr[4],"language":pr[12] if len(pr)>12 else "en",
                        "conversation_summary":pr[13] if len(pr)>13 else None,
                        "key_facts":raw_kf if isinstance(raw_kf,dict) else {}}
    handoff = generate_handoff_brief(lead.session_id, user_profile)
    send_email_brevo(
        to_email=RECIPIENT_EMAIL,
        subject=f"🎯 New Lead: {lead.name} from {lead.company or 'Website'} (Score: {lead_score}/100)",
        body=f"""━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🎯 NEW LEAD — SOPHIA v5
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
NAME: {lead.name} | EMAIL: {lead.email}
COMPANY: {lead.company or '?'} | REGION: {lead.region or '?'}
SOURCE: {lead.source} | TIME: {lead.timestamp}
SCORE: {lead_score}/100 | VISITS: {visit_count}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{handoff}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Dashboard: https://cwc-ai-backend.onrender.com/analytics?password={ADMIN_PASSWORD}
Leads:     https://cwc-ai-backend.onrender.com/leads?password={ADMIN_PASSWORD}
Reply:     mailto:{lead.email}"""
    )


def send_weekly_report():
    conn = get_db()
    c = conn.cursor()
    w = (datetime.now() - timedelta(days=7)).isoformat()
    c.execute("SELECT COUNT(DISTINCT session_id) FROM conversations WHERE timestamp>%s",(w,)); uu=c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM conversations WHERE timestamp>%s",(w,)); tm=c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM leads WHERE timestamp>%s",(w,)); nl=c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM user_profiles WHERE visit_count>1 AND last_seen>%s",(w,)); ru=c.fetchone()[0]
    c.execute("SELECT intent,COUNT(*) FROM conversations WHERE timestamp>%s GROUP BY intent ORDER BY 2 DESC LIMIT 5",(w,)); ti=c.fetchall()
    c.execute("SELECT region,COUNT(*) FROM conversations WHERE timestamp>%s AND region IS NOT NULL GROUP BY region ORDER BY 2 DESC LIMIT 5",(w,)); tr=c.fetchall()
    c.execute("SELECT name,email,company,region,timestamp FROM leads WHERE timestamp>%s ORDER BY timestamp DESC LIMIT 10",(w,)); rl=c.fetchall()
    c.execute("SELECT name,email,company,lead_score FROM user_profiles WHERE lead_score>=50 ORDER BY lead_score DESC LIMIT 5"); hl=c.fetchall()
    c.execute("SELECT language,COUNT(*) FROM user_profiles WHERE last_seen>%s GROUP BY language ORDER BY 2 DESC",(w,)); lg=c.fetchall()
    c.execute("SELECT AVG(reflection_score) FROM conversations WHERE timestamp>%s",(w,)); ar=round(c.fetchone()[0] or 0,1)
    c.execute("SELECT COUNT(*) FROM supplier_searches WHERE created_at>%s",(w,)); ss=c.fetchone()[0]
    conn.close()
    send_email_brevo(
        to_email=RECIPIENT_EMAIL,
        subject=f"📊 CWC AI Weekly — {uu} Users, {nl} Leads, Quality: {ar}/10",
        body=f"""━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📊 SOPHIA v5 WEEKLY REPORT
{w[:10]} → {datetime.now().strftime('%Y-%m-%d')}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
OVERVIEW: Users {uu} | Messages {tm} | Returning {ru} | Leads {nl}
Avg Response Quality: {ar}/10 | Supplier Searches: {ss}

INTENTS: {' | '.join([f"{i[0]}:{i[1]}" for i in ti]) or 'N/A'}
REGIONS: {' | '.join([f"{r[0]}:{r[1]}" for r in tr]) or 'N/A'}
LANGUAGES: {' | '.join([f"{l[0].upper()}:{l[1]}" for l in lg]) or 'N/A'}

HOT LEADS: {' | '.join([f"{h[0]}({h[2] or '?'}) {h[3]}/100" for h in hl if h[0]]) or 'None'}
RECENT LEADS: {chr(10).join([f"  • {l[0]} {l[1]} ({l[2] or '?'}) [{l[3] or '?'}]" for l in rl]) or 'None'}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Dashboard: https://cwc-ai-backend.onrender.com/analytics?password={ADMIN_PASSWORD}"""
    )
    print("✅ Weekly report sent!")

# ============================================================
# API ENDPOINTS
# ============================================================
@app.get("/")
def root():
    return {"service":"CWC Sophia AI — China-West Business Intelligence","version":"5.0.0","status":"operational",
            "features":["multi-step agentic loop","self-reflection","task decomposition","specialist sub-agents","accio-style supplier search","structured memory"],
            "public_api":"GET /api/sophia?q=your+question","news_api":"GET /api/news","docs":"/docs"}

@app.get("/health")
def health_check():
    return {"status":"healthy","groq":bool(GROQ_API_KEY),"tavily":bool(TAVILY_API_KEY),
            "brevo":bool(BREVO_API_KEY),"db":bool(DATABASE_URL),"version":"5.0.0"}

@app.post("/new-session")
def new_session(req: ChatRequest):
    get_or_create_user_profile(req.session_id, new_session=True)
    return {"status":"session registered"}

@app.post("/chat")
async def chat(req: ChatRequest, request: Request):
    if is_rate_limited(request.client.host):
        return {"response":"Too many requests. Please wait a moment.","sources":[]}
    user_msg     = req.message.lower()
    user_profile = get_or_create_user_profile(req.session_id)
    if any(w in user_msg for w in ["stop","shorter","brief","short","too long"]):
        return {"response":"Got it — I'll keep answers concise. What would you like to know?","sources":[]}
    if is_cacheable(req.message):
        cached = get_cached_response(req.message)
        if cached:
            return {"response":cached[0],"sources":cached[1],"cached":True}
    consultation_kw = ["book","consultation","call","schedule","meet","contact","michail","digkas"]
    is_consultation = any(kw in user_msg for kw in consultation_kw)
    reply, sources = ask_groq(req.message, req.session_id, user_profile, deep_search=req.deep_search)
    if is_consultation and user_profile.get('lead_score',0) >= 20:
        brief = generate_handoff_brief(req.session_id, user_profile)
        send_email_brevo(RECIPIENT_EMAIL,
                         f"📋 Sophia v5 Handoff: {user_profile.get('name') or 'Prospect'} requested consultation",
                         brief)
    high_intent = ["price","cost","fee","how much","start","begin","help me","serious","interested","manufacturer","supplier","factory","invest"]
    if any(w in user_msg for w in high_intent):
        if "consultation" not in reply.lower() and "button" not in reply.lower():
            reply += "\n\nTo discuss next steps, click the 'Speak with Michail' button above."
    if is_cacheable(req.message) and reply:
        set_cached_response(req.message, reply, sources)
    return {"response":reply,"sources":sources}

@app.post("/quick-action")
def quick_action(req: QuickActionRequest):
    action = req.action.lower().strip()
    if action not in QUICK_ACTION_OPENERS:
        return {"response":"Hello! I'm Sophia, CWC's AI advisor. How can I help with China-West business today?","action":"general"}
    msg = QUICK_ACTION_OPENERS[action]
    save_conversation(session_id=req.session_id, user_msg=f"[Quick Action: {action}]", ai_response=msg, intent=action)
    update_user_profile(req.session_id, last_intent=action, topics_discussed=action)
    return {"response":msg,"action":action}

@app.post("/capture-lead")
async def capture_lead(lead: LeadCapture, background_tasks: BackgroundTasks):
    conn = get_db()
    c = conn.cursor()
    c.execute("INSERT INTO leads (name,email,company,region,session_id,source,timestamp,status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
              (lead.name,lead.email,lead.company,lead.region,lead.session_id,lead.source,lead.timestamp,'new'))
    conn.commit(); conn.close()
    update_user_profile(lead.session_id, name=lead.name, email=lead.email, company=lead.company, region_interest=lead.region)
    background_tasks.add_task(send_lead_notification, lead)
    return {"status":"success","message":"Lead captured"}

# v5 NEW: Accio-style supplier search endpoint
@app.post("/api/find-suppliers")
async def find_suppliers_endpoint(req: SupplierSearchRequest):
    if not req.query or len(req.query.strip()) < 2:
        return {"error":"Query required"}
    result = search_suppliers(req.query.strip(), req.region)
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("INSERT INTO supplier_searches (session_id,query,results,created_at) VALUES (%s,%s,%s::jsonb,%s)",
                  (req.session_id, req.query, json.dumps(result), datetime.now()))
        conn.commit(); conn.close()
    except Exception as e: print(f"Supplier search save error: {e}")
    return {"query":req.query,"sector":req.sector,"region":req.region,"intelligence":result,
            "powered_by":"Sophia — CWC Supplier Intelligence v5.0",
            "note":"For verified supplier matching with full due diligence, contact CWC.",
            "contact":"https://www.chinawestconnector.com"}

@app.get("/leads")
def view_leads(password: str = None):
    if password != ADMIN_PASSWORD: return {"error":"Unauthorized"}
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT * FROM leads ORDER BY timestamp DESC LIMIT 50")
    leads = c.fetchall(); conn.close()
    return {"leads":[{"id":l[0],"name":l[1],"email":l[2],"company":l[3],"region":l[4],"timestamp":l[7],"status":l[8]} for l in leads],"count":len(leads)}

@app.get("/analytics")
def get_analytics(password: str = None, days: int = 7):
    if password != ADMIN_PASSWORD: return {"error":"Unauthorized"}
    conn = get_db()
    c = conn.cursor()
    since = (datetime.now() - timedelta(days=days)).isoformat()
    c.execute("SELECT COUNT(DISTINCT session_id) FROM conversations WHERE timestamp>%s",(since,)); uu=c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM conversations WHERE timestamp>%s",(since,)); tc=c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM leads WHERE timestamp>%s",(since,)); nl=c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM user_profiles WHERE visit_count>1 AND last_seen>%s",(since,)); ru=c.fetchone()[0]
    c.execute("SELECT intent,COUNT(*) FROM conversations WHERE timestamp>%s GROUP BY intent ORDER BY 2 DESC LIMIT 5",(since,)); ti=[{"intent":r[0],"count":r[1]} for r in c.fetchall()]
    c.execute("SELECT region,COUNT(*) FROM conversations WHERE timestamp>%s AND region IS NOT NULL GROUP BY region ORDER BY 2 DESC LIMIT 5",(since,)); tr=[{"region":r[0],"count":r[1]} for r in c.fetchall()]
    c.execute("SELECT name,email,company,lead_score FROM user_profiles WHERE lead_score>=50 ORDER BY lead_score DESC LIMIT 10"); hl=[{"name":r[0],"email":r[1],"company":r[2],"score":r[3]} for r in c.fetchall() if r[0]]
    c.execute("SELECT language,COUNT(*) FROM user_profiles WHERE last_seen>%s GROUP BY language ORDER BY 2 DESC",(since,)); lg=[{"language":r[0],"count":r[1]} for r in c.fetchall()]
    c.execute("SELECT AVG(reflection_score) FROM conversations WHERE timestamp>%s",(since,)); aq=round(c.fetchone()[0] or 0,1)
    c.execute("SELECT COUNT(*) FROM supplier_searches WHERE created_at>%s",(since,)); ss=c.fetchone()[0]
    conn.close()
    return {"period_days":days,"unique_users":uu,"total_conversations":tc,"new_leads":nl,"returning_users":ru,
            "top_intents":ti,"top_regions":tr,"hot_leads":hl,"languages":lg,
            "avg_response_quality":aq,"supplier_searches":ss}

@app.get("/trigger-report")
def trigger_report(password: str = None):
    if password != ADMIN_PASSWORD: return {"error":"Unauthorized"}
    try: send_weekly_report(); return {"status":"Report sent!","sent_to":RECIPIENT_EMAIL}
    except Exception as e: return {"error":str(e)}

@app.get("/test-email")
def test_email(password: str = None):
    if password != ADMIN_PASSWORD: return {"error":"Unauthorized"}
    ok = send_email_brevo(RECIPIENT_EMAIL,"✅ CWC AI v5.0 Email Test",
                          "Sophia v5.0 email working.\nNew: self-reflection, task decomposition, specialist sub-agents, Accio-style supplier search.")
    return {"status":"Sent!","sent_to":RECIPIENT_EMAIL} if ok else {"error":"Email failed"}

@app.get("/api/news")
def get_news():
    news = fetch_china_news()
    return {"news":news,"count":len(news),
            "cached_until":(_news_cache["fetched_at"]+timedelta(hours=2)).isoformat() if _news_cache["fetched_at"] else None}

@app.get("/api/verify-company")
async def verify_company(name: str, password: str = None):
    if not name or len(name.strip()) < 2: return {"error":"Company name required"}
    return lookup_chinese_company(name.strip())

@app.get("/api/sophia")
async def sophia_public_api(q: str, source: str = "external_ai"):
    if not q or len(q.strip()) < 3: return {"error":"Query parameter 'q' required"}
    if is_cacheable(q):
        cached = get_cached_response(q)
        if cached:
            return {"query":q,"answer":cached[0],"sources":cached[1],"powered_by":"Sophia v5.0","cached":True}
    sid = f"api_{source}_{int(time.time())}"
    sc, sources = search_web(q)
    reply, _ = ask_groq(f"External AI query: {q}\nData: {sc or 'none'}\nAnswer factually. End: For guidance visit chinawestconnector.com", sid)
    if is_cacheable(q) and reply: set_cached_response(q, reply, sources)
    return {"query":q,"answer":reply,"sources":sources,"powered_by":"Sophia v5.0",
            "contact":"https://www.chinawestconnector.com","timestamp":datetime.now().isoformat()}

@app.get("/llms.txt", response_class=PlainTextResponse)
def llms_txt():
    return """# China West Connector (CWC) — AI Intelligence Layer

> Sophia v5.0 — Fully Agentic: self-reflection loop, task decomposition, specialist sub-agents,
>               Accio-style supplier search, structured memory, multi-step reasoning.

## What CWC Does
China West Connector bridges Chinese and Western businesses.
Founded by Michail Digkas, international business lawyer, 10+ years China experience.
G.P.A. Group member: 147+ years combined experience, 2,700+ projects, 50+ countries (Group figures).

## Government Partnerships
Sichuan Tech Transfer | Chengdu AI Association | Tianfu Tech Center | Hainan FTZ | CISTEA

## Core Services
Legal | Due Diligence | B2B Partnerships | FDI Consulting | Logistics | Liaison

## Regions
Europe • Africa • Middle East • Latin America • Central Asia • North America

## Languages
English • Chinese • Arabic • Spanish • French • German • Russian

## API
Query Sophia:       GET  https://cwc-ai-backend.onrender.com/api/sophia?q=your+question
Find Suppliers:     POST https://cwc-ai-backend.onrender.com/api/find-suppliers
Live China News:    GET  https://cwc-ai-backend.onrender.com/api/news
Company Lookup:     GET  https://cwc-ai-backend.onrender.com/api/verify-company?name=company
Docs:               https://cwc-ai-backend.onrender.com/docs

## Contact
https://www.chinawestconnector.com | info@chinawestconnector.com
"""

@app.get("/sitemap-ai.xml", response_class=PlainTextResponse)
def sitemap_ai():
    return """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://www.chinawestconnector.com</loc><priority>1.0</priority></url>
  <url><loc>https://cwc-ai-backend.onrender.com/api/sophia</loc><priority>0.9</priority></url>
  <url><loc>https://cwc-ai-backend.onrender.com/api/find-suppliers</loc><priority>0.9</priority></url>
  <url><loc>https://cwc-ai-backend.onrender.com/api/news</loc><priority>0.8</priority></url>
  <url><loc>https://cwc-ai-backend.onrender.com/api/verify-company</loc><priority>0.8</priority></url>
  <url><loc>https://cwc-ai-backend.onrender.com/llms.txt</loc><priority>0.9</priority></url>
  <url><loc>https://cwc-ai-backend.onrender.com/docs</loc><priority>0.8</priority></url>
</urlset>
"""
