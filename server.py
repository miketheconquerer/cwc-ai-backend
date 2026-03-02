# ============================================================
# SOPHIA AI v11 – STABLE AGENTIC EDITION (100% FREE)
# Planner • Executor • Critic • Goal Reprioritization
# ============================================================

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import requests
import os
import json
import time
import asyncio
import psycopg2
import psycopg2.extras
import hashlib
import math
from collections import defaultdict
from datetime import datetime

# ============================================================
# CONFIG
# ============================================================

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
CLOUDFLARE_API_KEY = os.getenv("CLOUDFLARE_API_KEY", "")
CLOUDFLARE_ACCOUNT_ID = os.getenv("CLOUDFLARE_ACCOUNT_ID", "")
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
        confidence FLOAT,
        created_at TIMESTAMP DEFAULT NOW()
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS autonomous_goals (
        id SERIAL PRIMARY KEY,
        description TEXT,
        priority INTEGER DEFAULT 5,
        status TEXT DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT NOW()
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS tool_stats (
        tool_name TEXT PRIMARY KEY,
        success_count INTEGER DEFAULT 0,
        use_count INTEGER DEFAULT 0
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
        if not self.providers:
            raise Exception("No AI provider configured")

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
                r.raise_for_status()
                return r.json()["choices"][0]["message"]["content"]

            elif provider == "cloudflare":
                r = requests.post(
                    f"https://api.cloudflare.com/client/v4/accounts/{CLOUDFLARE_ACCOUNT_ID}/ai/run/@cf/meta/llama-3.1-8b-instruct",
                    headers={
                        "Authorization": f"Bearer {CLOUDFLARE_API_KEY}",
                        "Content-Type": "application/json"
                    },
                    json={"messages": messages},
                    timeout=60
                )
                r.raise_for_status()
                return r.json()["result"]["response"]

        except:
            self.switch()
            return await self.chat(messages)

ai = FreeAIProvider()

# ============================================================
# MEMORY (LIGHTWEIGHT HASH EMBEDDING)
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

memory = Memory()

# ============================================================
# TOOL SYSTEM (SELF-LEARNING)
# ============================================================

async def tool_duckduckgo(query):
    r = requests.get(
        "https://api.duckduckgo.com/",
        params={"q": query, "format": "json"},
        timeout=15
    )
    data = r.json()
    return data.get("Abstract", "No result")

TOOLS = {
    "search_web": tool_duckduckgo
}

async def execute_tool(name, param):
    conn = get_db()
    c = conn.cursor()

    c.execute("INSERT INTO tool_stats (tool_name,use_count) VALUES (%s,1) ON CONFLICT (tool_name) DO UPDATE SET use_count = tool_stats.use_count + 1",
              (name,))
    conn.commit()

    try:
        result = await TOOLS[name](param)

        c.execute("UPDATE tool_stats SET success_count = success_count + 1 WHERE tool_name=%s",
                  (name,))
        conn.commit()
        conn.close()
        return result
    except:
        conn.close()
        return "Tool failed"

# ============================================================
# PLANNER → EXECUTOR → CRITIC
# ============================================================

async def planner(user_input):
    prompt = f"""
Break this into reasoning steps and decide if a tool is needed.

User: {user_input}

Return JSON:
{{"needs_tool": true/false, "tool_name": "...", "tool_param": "...", "plan": "..."}}
"""
    response = await ai.chat([{"role":"user","content":prompt}])
    try:
        return json.loads(response)
    except:
        return {"needs_tool": False, "plan": user_input}

async def critic(user_input, answer):
    prompt = f"""
Evaluate this answer.

User question: {user_input}
Answer: {answer}

Return JSON:
{{"confidence": 0-1, "improve": "better answer if needed"}}
"""
    response = await ai.chat([{"role":"user","content":prompt}])
    try:
        return json.loads(response)
    except:
        return {"confidence":0.8}

# ============================================================
# GOAL REPRIORITIZATION LOOP
# ============================================================

async def reprioritize_goals():
    while True:
        await asyncio.sleep(600)
        conn = get_db()
        c = conn.cursor()

        c.execute("UPDATE autonomous_goals SET priority = priority + 1 WHERE status='pending'")
        conn.commit()
        conn.close()

@app.on_event("startup")
async def startup():
    asyncio.create_task(reprioritize_goals())

# ============================================================
# CHAT ENDPOINT
# ============================================================

class ChatRequest(BaseModel):
    session_id: str
    message: str

@app.post("/chat")
async def chat(req: ChatRequest):

    user_input = req.message

    # 1️⃣ PLAN
    plan = await planner(user_input)

    tool_output = None

    # 2️⃣ EXECUTE TOOL IF NEEDED
    if plan.get("needs_tool") and plan.get("tool_name") in TOOLS:
        tool_output = await execute_tool(plan["tool_name"], plan["tool_param"])

    # 3️⃣ FINAL ANSWER
    context = f"{plan.get('plan','')}\nTool output: {tool_output}" if tool_output else plan.get("plan","")

    answer = await ai.chat([
        {"role":"system","content":"You are Sophia, a strategic China business AI advisor."},
        {"role":"user","content":context}
    ])

    # 4️⃣ CRITIC
    review = await critic(user_input, answer)
    confidence = review.get("confidence",0.8)

    if confidence < REFLECTION_THRESHOLD:
        answer = review.get("improve",answer)

    # 5️⃣ STORE
    conn = get_db()
    c = conn.cursor()
    c.execute("""
    INSERT INTO conversations (session_id,user_message,ai_response,confidence)
    VALUES (%s,%s,%s,%s)
    """,(req.session_id,user_input,answer,confidence))
    conn.commit()
    conn.close()

    return {
        "response": answer,
        "confidence": confidence
    }

@app.get("/")
def root():
    return {"status":"Sophia v11 Agentic Running"}