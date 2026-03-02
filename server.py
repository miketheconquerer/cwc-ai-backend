# ============================================================
# SOPHIA AI v12 – REAL MEMORY AGENT (STABLE)
# Planner • Executor • Critic • Memory • Brave + Wikipedia
# ============================================================

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import requests, os, json, asyncio, psycopg2, hashlib, math
from datetime import datetime

# ============================================================
# CONFIG
# ============================================================

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
CLOUDFLARE_API_KEY = os.getenv("CLOUDFLARE_API_KEY", "")
CLOUDFLARE_ACCOUNT_ID = os.getenv("CLOUDFLARE_ACCOUNT_ID", "")
BRAVE_API_KEY = os.getenv("BRAVE_API_KEY", "")  # optional
DATABASE_URL = os.getenv("DATABASE_URL")

REFLECTION_THRESHOLD = 0.7
MAX_TOKENS = 900

# ============================================================
# FASTAPI
# ============================================================

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
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

    c.execute("""
    CREATE TABLE IF NOT EXISTS conversations (
        id SERIAL PRIMARY KEY,
        session_id TEXT,
        user_message TEXT,
        ai_response TEXT,
        embedding TEXT,
        confidence FLOAT,
        created_at TIMESTAMP DEFAULT NOW()
    )
    """)

    conn.commit()
    conn.close()

init_db()

# ============================================================
# FREE AI PROVIDER
# ============================================================

class FreeAIProvider:
    def __init__(self):
        self.providers = []
        self.current = 0
        if OPENROUTER_API_KEY:
            self.providers.append("openrouter")
        if CLOUDFLARE_API_KEY and CLOUDFLARE_ACCOUNT_ID:
            self.providers.append("cloudflare")

    def switch(self):
        self.current = (self.current + 1) % len(self.providers)

    async def chat(self, messages, temperature=0.3):
        provider = self.providers[self.current]

        try:
            if provider == "openrouter":
                r = requests.post(
                    "https://openrouter.ai/api/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                        "Content-Type": "application/json"
                    },
                    json={
                        "model": "meta-llama/llama-3.1-8b-instruct:free",
                        "messages": messages,
                        "temperature": temperature,
                        "max_tokens": MAX_TOKENS
                    },
                    timeout=60
                )
                return r.json()["choices"][0]["message"]["content"]

            else:
                r = requests.post(
                    f"https://api.cloudflare.com/client/v4/accounts/{CLOUDFLARE_ACCOUNT_ID}/ai/run/@cf/meta/llama-3.1-8b-instruct",
                    headers={"Authorization": f"Bearer {CLOUDFLARE_API_KEY}"},
                    json={"messages": messages},
                    timeout=60
                )
                return r.json()["result"]["response"]

        except:
            self.switch()
            return await self.chat(messages)

ai = FreeAIProvider()

# ============================================================
# MEMORY ENGINE (COSINE SIMILARITY)
# ============================================================

class Memory:
    def encode(self, text):
        vec = []
        for i in range(128):
            h = hashlib.md5(f"{text}_{i}".encode()).hexdigest()
            val = (int(h[:8], 16) / 0xFFFFFFFF) * 2 - 1
            vec.append(val)
        norm = math.sqrt(sum(x*x for x in vec))
        return [x/norm for x in vec]

    def cosine(self, a, b):
        return sum(x*y for x, y in zip(a, b))

memory = Memory()

def retrieve_memory(session_id, query_vec, top_k=3):
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT user_message, ai_response, embedding FROM conversations WHERE session_id=%s", (session_id,))
    rows = c.fetchall()
    conn.close()

    scored = []
    for u, a, emb in rows:
        if emb:
            vec = json.loads(emb)
            sim = memory.cosine(query_vec, vec)
            scored.append((sim, u, a))

    scored.sort(reverse=True)
    return scored[:top_k]

# ============================================================
# TOOLS
# ============================================================

async def wikipedia_tool(query):
    r = requests.get(
        f"https://en.wikipedia.org/api/rest_v1/page/summary/{query}",
        timeout=10
    )
    if r.status_code == 200:
        return r.json().get("extract", "No summary")
    return "No result"

async def brave_search(query):
    if not BRAVE_API_KEY:
        return "Brave API not configured"
    r = requests.get(
        "https://api.search.brave.com/res/v1/web/search",
        headers={"X-Subscription-Token": BRAVE_API_KEY},
        params={"q": query},
        timeout=10
    )
    data = r.json()
    results = data.get("web", {}).get("results", [])
    return results[0]["description"] if results else "No results"

TOOLS = {
    "wikipedia": wikipedia_tool,
    "brave_search": brave_search
}

async def execute_tool(name, param):
    if name in TOOLS:
        return await TOOLS[name](param)
    return "Tool not found"

# ============================================================
# PLANNER (JSON SAFE)
# ============================================================

async def planner(user_input):
    prompt = f"""
Decide reasoning steps and if a tool is needed.

User: {user_input}

Return JSON:
{{"needs_tool": true/false, "tool": "wikipedia|brave_search|none", "param": "...", "plan": "..."}}
"""
    raw = await ai.chat([{"role":"user","content":prompt}])

    try:
        return json.loads(raw)
    except:
        # JSON repair fallback
        return {"needs_tool": False, "plan": user_input}

# ============================================================
# CRITIC
# ============================================================

async def critic(user_input, answer):
    prompt = f"""
Evaluate quality from 0-1 and improve if needed.

Q: {user_input}
A: {answer}

Return JSON: {{"confidence":0-1,"improve":"better version"}}
"""
    raw = await ai.chat([{"role":"user","content":prompt}])
    try:
        return json.loads(raw)
    except:
        return {"confidence":0.8}

# ============================================================
# CHAT
# ============================================================

class ChatRequest(BaseModel):
    session_id: str
    message: str

@app.post("/chat")
async def chat(req: ChatRequest):

    user_input = req.message
    query_vec = memory.encode(user_input)

    # MEMORY RETRIEVAL
    memories = retrieve_memory(req.session_id, query_vec)
    memory_block = "\n".join([f"Past: {m[1]} -> {m[2]}" for m in memories])

    # PLAN
    plan = await planner(user_input)

    tool_output = None
    if plan.get("needs_tool") and plan.get("tool") in TOOLS:
        tool_output = await execute_tool(plan["tool"], plan["param"])

    context = f"""
Relevant past memory:
{memory_block}

User request: {user_input}
Tool output: {tool_output}
"""

    answer = await ai.chat([
        {"role":"system","content":"You are Sophia, a strategic China business AI advisor with memory."},
        {"role":"user","content":context}
    ])

    # CRITIC
    review = await critic(user_input, answer)
    confidence = review.get("confidence", 0.8)
    if confidence < REFLECTION_THRESHOLD:
        answer = review.get("improve", answer)

    # STORE MEMORY
    conn = get_db()
    c = conn.cursor()
    c.execute("""
    INSERT INTO conversations (session_id,user_message,ai_response,embedding,confidence)
    VALUES (%s,%s,%s,%s,%s)
    """,(req.session_id,user_input,answer,json.dumps(query_vec),confidence))
    conn.commit()
    conn.close()

    return {"response": answer, "confidence": confidence}

@app.get("/")
def root():
    return {"status":"Sophia v12 Memory Agent Running"}