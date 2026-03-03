"""
================================================================================
SOPHIA AI SERVER v11.0 - FULLY AGENTIC EDITION
================================================================================
NEW IN v11.0:
🤖 Agent Swarm - Multi-agent collaboration with specialization
🎯 Convergent Depth - Dynamic iteration until answer stabilizes  
🔧 Self-Improving Tools - Auto-generates tools when gaps detected
👁️ Autonomous Monitoring - Self-triggering background research
📈 Self-Improvement Engine - Continuous learning from performance
🧠 Local FAISS Memory - Zero-cost vector storage
🕵️ Stealth Browser - No Playwright needed
💾 ZeroCostTracer - Local file-based observability
⚡ Prompt Cache - 30-50% speedup via caching
🔄 Resilient Execution - Smart retries with fallbacks
================================================================================
"""

from fastapi import FastAPI, BackgroundTasks, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse, JSONResponse, FileResponse, StreamingResponse
from pydantic import BaseModel
import requests
import os
import psycopg2
import psycopg2.extras
import json
import time
import hashlib
import re
import asyncio
import threading
import random
import numpy as np
from collections import defaultdict
from datetime import datetime, timedelta
from dotenv import load_dotenv
from contextlib import asynccontextmanager
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path
import uuid
import traceback
import base64
import math
import urllib.parse
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from enum import Enum, auto
from bs4 import BeautifulSoup
import faiss

# Optional imports with fallbacks
FEEDPARSER_AVAILABLE = False
try:
    import feedparser
    FEEDPARSER_AVAILABLE = True
    print("✅ feedparser available")
except ImportError:
    print("⚠️ feedparser not installed. Install with: pip install feedparser")

SENTENCE_TRANSFORMERS_AVAILABLE = False
try:
    from sentence_transformers import SentenceTransformer
    SENTENCE_TRANSFORMERS_AVAILABLE = True
    print("✅ Sentence Transformers available")
except Exception as e:
    print(f"⚠️ sentence-transformers not installed: {e}")

PLAYWRIGHT_AVAILABLE = False
try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
    print("✅ Playwright available")
except Exception as e:
    print(f"⚠️ Playwright not installed: {e}")

load_dotenv()

# ============================================================
# CONFIGURATION
# ============================================================
BREVO_API_KEY = os.getenv("BREVO_API_KEY", "")
SENDER_EMAIL = os.getenv("SENDER_EMAIL", "888nv666@gmail.com")
RECIPIENT_EMAIL = "digkasm@proton.me"
DATABASE_URL = os.getenv("DATABASE_URL")

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
CLOUDFLARE_API_KEY = os.getenv("CLOUDFLARE_API_KEY", "")
CLOUDFLARE_ACCOUNT_ID = os.getenv("CLOUDFLARE_ACCOUNT_ID", "")

ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")
if ADMIN_PASSWORD == "admin123":
    print("⚠️  WARNING: ADMIN_PASSWORD is using insecure default")

CHROMA_CLOUD_API_KEY = os.getenv("CHROMA_CLOUD_API_KEY", "")
CHROMA_TENANT = os.getenv("CHROMA_TENANT", "default")
CHROMA_DATABASE = os.getenv("CHROMA_DATABASE", "default")
CHROMA_SERVER_URL = os.getenv("CHROMA_SERVER_URL", "")
CHROMA_SERVER_AUTH = os.getenv("CHROMA_SERVER_AUTH", "")

_supabase_url = os.getenv("SUPABASE_DB_URL", "")
if _supabase_url and "${" not in _supabase_url:
    SUPABASE_DB_URL = _supabase_url
else:
    SUPABASE_DB_URL = DATABASE_URL
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY", "")

CHROMA_LOCAL_PATH = os.getenv("CHROMA_LOCAL_PATH", "./chroma_db")
VECTOR_DB_TYPE = os.getenv("VECTOR_DB_TYPE", "auto")

TAVILY_API_KEY = os.getenv("TAVILY_API_KEY", "")
JINA_API_KEY = os.getenv("JINA_API_KEY", "")
NEWS_API_KEY = os.getenv("NEWS_API_KEY", "")

INDEXNOW_KEY = os.getenv("INDEXNOW_KEY", "")
GOOGLE_SEARCH_CONSOLE_KEY = os.getenv("GOOGLE_SEARCH_CONSOLE_KEY", "")

BING_WEBMASTER_API_KEY = os.getenv("BING_WEBMASTER_API_KEY", "")
BING_SEARCH_API_KEY = os.getenv("BING_SEARCH_API_KEY", "")
BING_CUSTOM_CONFIG_ID = os.getenv("BING_CUSTOM_CONFIG_ID", "")

REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID", "")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET", "")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT", "SophiaAI/1.0 by ChinaWestConnector")

ZENROWS_API_KEY = os.getenv("ZENROWS_API_KEY", "")

# Agentic Configuration
AUTO_IMPROVEMENT_INTERVAL_HOURS = 24
ENVIRONMENT_CHECK_INTERVAL_HOURS = 6
GOAL_EXECUTION_INTERVAL_MINUTES = 5
MAX_CONCURRENT_GOALS = 3
MIN_CONFIDENCE_FOR_AUTO_ACTION = 0.75
ENABLE_TOOL_CHAINING = True
ENABLE_SELF_REFLECTION = True
ENABLE_REACT_REASONING = True
ENABLE_LEARNING_LOOP = True
MAX_TOOL_CHAIN_DEPTH = 3
REFLECTION_THRESHOLD = 0.7
NEWS_CACHE_MAX_AGE_MINUTES = 30

# Convergent Agent Settings
CONVERGENCE_THRESHOLD = 0.95
MAX_SAFETY_ITERATIONS = 10
CONVERGENCE_WINDOW = 3

# ============================================================
# ZERO COST TRACER - 100% Free Observability
# ============================================================
class ZeroCostTracer:
    """100% free tracing - writes to local JSONL files with rotation"""
    
    def __init__(self, log_dir: str = "./logs"):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        self.current_file = self._get_log_file()
        self._rotate_if_needed()
        self.metrics = {
            'total_traces': 0,
            'total_latency_ms': 0,
            'tool_usage': defaultdict(int)
        }
    
    def _get_log_file(self):
        date_str = datetime.now().strftime("%Y-%m-%d")
        return self.log_dir / f"traces_{date_str}.jsonl"
    
    def _rotate_if_needed(self):
        import gzip
        for old_file in self.log_dir.glob("traces_*.jsonl"):
            if old_file != self.current_file and old_file.stat().st_size > 100_000:
                with open(old_file, 'rb') as f_in:
                    with gzip.open(f"{old_file}.gz", 'wb') as f_out:
                        f_out.writelines(f_in)
                old_file.unlink()
    
    def trace(self, session_id: str, operation: str, 
              inputs: dict, outputs: dict, 
              latency_ms: float, tools_used: list = None,
              iteration: int = 1, converged: bool = True):
        entry = {
            "timestamp": datetime.now().isoformat(),
            "session_id": session_id[:16],
            "operation": operation,
            "inputs": self._sanitize(inputs),
            "outputs": self._sanitize(outputs),
            "latency_ms": latency_ms,
            "tools_used": tools_used or [],
            "iteration": iteration,
            "converged": converged
        }
        
        with open(self.current_file, 'a') as f:
            f.write(json.dumps(entry) + '\n')
        
        self.metrics['total_traces'] += 1
        self.metrics['total_latency_ms'] += latency_ms
        for tool in (tools_used or []):
            self.metrics['tool_usage'][tool] += 1
    
    def _sanitize(self, data: dict) -> dict:
        if not isinstance(data, dict):
            return data
        sanitized = {}
        for k, v in data.items():
            if any(sensitive in k.lower() for sensitive in ['password', 'key', 'token', 'secret']):
                sanitized[k] = '[REDACTED]'
            else:
                sanitized[k] = v
        return sanitized
    
    def get_stats(self) -> dict:
        avg_latency = self.metrics['total_latency_ms'] / max(self.metrics['total_traces'], 1)
        return {
            'total_traces': self.metrics['total_traces'],
            'avg_latency_ms': round(avg_latency, 2),
            'tool_usage': dict(self.metrics['tool_usage'])
        }

tracer = ZeroCostTracer()

# ============================================================
# PROMPT CACHE - 100% Free Speedup
# ============================================================
class PromptCache:
    """Cache LLM responses to avoid redundant calls"""
    
    def __init__(self, cache_dir: str = "./prompt_cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.memory_cache = {}
        self.hit_count = 0
        self.miss_count = 0
    
    def _get_key(self, messages: List[dict], model: str, tools: list = None) -> str:
        content = json.dumps({
            "messages": messages, 
            "model": model,
            "tools": [t['function']['name'] for t in (tools or [])]
        }, sort_keys=True)
        return hashlib.sha256(content.encode()).hexdigest()[:32]
    
    async def get_or_compute(self, messages: List[dict], model: str,
                             compute_func, tools: list = None) -> dict:
        key = self._get_key(messages, model, tools)
        
        if key in self.memory_cache:
            self.hit_count += 1
            return self.memory_cache[key]
        
        cache_file = self.cache_dir / f"{key}.json"
        if cache_file.exists():
            with open(cache_file) as f:
                result = json.load(f)
                self.memory_cache[key] = result
                self.hit_count += 1
                return result
        
        self.miss_count += 1
        result = await compute_func()
        
        self.memory_cache[key] = result
        with open(cache_file, 'w') as f:
            json.dump(result, f)
        
        return result
    
    def get_stats(self) -> dict:
        total = self.hit_count + self.miss_count
        hit_rate = self.hit_count / total if total > 0 else 0
        return {
            "hits": self.hit_count,
            "misses": self.miss_count,
            "hit_rate": f"{hit_rate:.1%}",
            "memory_entries": len(self.memory_cache),
            "disk_entries": len(list(self.cache_dir.glob("*.json")))
        }

prompt_cache = PromptCache()

# ============================================================
# LOCAL FAISS MEMORY - 100% Free Vector Storage
# ============================================================
class LocalFAISSMemory:
    """Zero-cost vector memory using FAISS"""
    
    def __init__(self, dim: int = 384, storage_dir: str = "./faiss_memory"):
        self.dim = dim
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(exist_ok=True)
        
        self.index = faiss.IndexFlatIP(dim)
        self.metadata = []
        self.metadata_file = self.storage_dir / "metadata.pkl"
        self.index_file = self.storage_dir / "faiss.index"
        
        self._load()
    
    def _load(self):
        if self.index_file.exists():
            self.index = faiss.read_index(str(self.index_file))
        if self.metadata_file.exists():
            import pickle
            with open(self.metadata_file, 'rb') as f:
                self.metadata = pickle.load(f)
    
    def _save(self):
        faiss.write_index(self.index, str(self.index_file))
        import pickle
        with open(self.metadata_file, 'wb') as f:
            pickle.dump(self.metadata, f)
    
    def add(self, text: str, embedding: List[float], metadata: dict = None):
        vec = np.array(embedding, dtype=np.float32).reshape(1, -1)
        faiss.normalize_L2(vec)
        
        self.index.add(vec)
        self.metadata.append({
            "text": text,
            "metadata": metadata or {},
            "timestamp": time.time()
        })
        
        if len(self.metadata) % 10 == 0:
            self._save()
    
    def search(self, query_embedding: List[float], k: int = 5) -> List[dict]:
        if self.index.ntotal == 0:
            return []
        
        vec = np.array(query_embedding, dtype=np.float32).reshape(1, -1)
        faiss.normalize_L2(vec)
        
        scores, indices = self.index.search(vec, k)
        
        results = []
        for score, idx in zip(scores[0], indices[0]):
            if idx >= 0 and idx < len(self.metadata):
                results.append({
                    "text": self.metadata[idx]["text"],
                    "score": float(score),
                    "metadata": self.metadata[idx]["metadata"]
                })
        
        return results
    
    def get_status(self) -> dict:
        return {
            "backend": "faiss_local",
            "vectors": self.index.ntotal,
            "dimension": self.dim,
            "storage_size_mb": sum(f.stat().st_size for f in self.storage_dir.glob("*")) / 1024 / 1024
        }

local_memory = LocalFAISSMemory()

# ============================================================
# DATABASE LAYER
# ============================================================
def get_db():
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL not configured")
    return psycopg2.connect(DATABASE_URL)

def init_db():
    try:
        conn = get_db()
        c = conn.cursor()
        
        tables = [
            ("conversations", """
                CREATE TABLE IF NOT EXISTS conversations (
                    id SERIAL PRIMARY KEY,
                    session_id VARCHAR(100),
                    user_message TEXT,
                    ai_response TEXT,
                    intent VARCHAR(50),
                    reflection_score INTEGER DEFAULT 5,
                    goals_extracted JSONB,
                    tools_used JSONB,
                    confidence_score FLOAT,
                    iterations INTEGER DEFAULT 1,
                    converged BOOLEAN DEFAULT TRUE,
                    timestamp TIMESTAMP DEFAULT NOW()
                )
            """),
            ("user_profiles", """
                CREATE TABLE IF NOT EXISTS user_profiles (
                    id SERIAL PRIMARY KEY,
                    session_id VARCHAR(100) UNIQUE,
                    email VARCHAR(255),
                    name VARCHAR(255),
                    phone VARCHAR(50),
                    company VARCHAR(255),
                    lead_score INTEGER DEFAULT 0,
                    last_intent VARCHAR(50),
                    key_facts JSONB,
                    region_interest VARCHAR(100),
                    sector_interest VARCHAR(100),
                    visit_count INTEGER DEFAULT 1,
                    first_seen TIMESTAMP DEFAULT NOW(),
                    last_seen TIMESTAMP DEFAULT NOW(),
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """),
            ("autonomous_goals", """
                CREATE TABLE IF NOT EXISTS autonomous_goals (
                    id SERIAL PRIMARY KEY,
                    session_id VARCHAR(100),
                    goal_type VARCHAR(50),
                    goal_description TEXT,
                    priority INTEGER DEFAULT 5,
                    status VARCHAR(20) DEFAULT 'pending',
                    subtasks JSONB,
                    completed_subtasks JSONB,
                    result TEXT,
                    confidence FLOAT,
                    source VARCHAR(50),
                    created_at TIMESTAMP DEFAULT NOW(),
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    retry_count INTEGER DEFAULT 0
                )
            """),
            ("user_feedback", """
                CREATE TABLE IF NOT EXISTS user_feedback (
                    id SERIAL PRIMARY KEY,
                    session_id VARCHAR(100),
                    conversation_id INTEGER REFERENCES conversations(id) ON DELETE SET NULL,
                    rating INTEGER CHECK (rating >= 1 AND rating <= 5),
                    comment TEXT,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """),
            ("generated_tools", """
                CREATE TABLE IF NOT EXISTS generated_tools (
                    id SERIAL PRIMARY KEY,
                    tool_name VARCHAR(100) UNIQUE,
                    code TEXT,
                    description TEXT,
                    created_for TEXT,
                    test_result JSONB,
                    use_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """),
            ("proactive_notifications", """
                CREATE TABLE IF NOT EXISTS proactive_notifications (
                    id SERIAL PRIMARY KEY,
                    session_id VARCHAR(100),
                    notification_type VARCHAR(50),
                    subject TEXT,
                    content TEXT,
                    sent BOOLEAN DEFAULT FALSE,
                    sent_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
        ]
        
        for name, sql in tables:
            c.execute(sql)
            print(f"✅ Table {name} ready")
        
        conn.commit()
        conn.close()
        print("✅ Database initialized")
        
    except Exception as e:
        print(f"⚠️ Database initialization error: {e}")

def get_or_create_user_profile(session_id: str) -> dict:
    try:
        conn = get_db()
        c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        c.execute("SELECT * FROM user_profiles WHERE session_id = %s", (session_id,))
        profile = c.fetchone()
        
        if not profile:
            c.execute("""
                INSERT INTO user_profiles (session_id, key_facts, visit_count, first_seen, last_seen)
                VALUES (%s, '{}'::jsonb, 1, NOW(), NOW())
                RETURNING *
            """, (session_id,))
            profile = c.fetchone()
            conn.commit()
        else:
            c.execute("""
                UPDATE user_profiles 
                SET visit_count = visit_count + 1, last_seen = NOW()
                WHERE session_id = %s
            """, (session_id,))
            conn.commit()
        
        conn.close()
        return dict(profile) if profile else {}
    except Exception as e:
        print(f"Profile error: {e}")
        return {}

def update_user_profile(session_id: str, **kwargs):
    try:
        conn = get_db()
        c = conn.cursor()
        
        set_clauses = []
        values = []
        for key, value in kwargs.items():
            if key in ['key_facts', 'goals_extracted']:
                set_clauses.append(f"{key} = %s::jsonb")
                values.append(json.dumps(value) if isinstance(value, dict) else value)
            else:
                set_clauses.append(f"{key} = %s")
                values.append(value)
        
        values.append(session_id)
        
        c.execute(f"""
            UPDATE user_profiles 
            SET {', '.join(set_clauses)}, updated_at = NOW()
            WHERE session_id = %s
        """, values)
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Update profile error: {e}")

# ============================================================
# FREE AI PROVIDER MANAGER
# ============================================================
class FreeAIProvider:
    def __init__(self):
        self.providers = []
        self.current_provider = 0
        self.request_counts = defaultdict(int)
        
        if OPENROUTER_API_KEY:
            self.providers.append({
                'name': 'openrouter',
                'key': OPENROUTER_API_KEY,
                'endpoint': 'https://openrouter.ai/api/v1/chat/completions',
                'models': {
                    'default': 'meta-llama/llama-3.1-8b-instruct:free',
                    'smart': 'meta-llama/llama-3.1-8b-instruct:free',
                    'fast': 'meta-llama/llama-3.2-3b-instruct:free'
                },
                'headers': {
                    'Authorization': f'Bearer {OPENROUTER_API_KEY}',
                    'Content-Type': 'application/json',
                    'HTTP-Referer': 'https://chinawestconnector.com',
                    'X-Title': 'Sophia AI - CWC'
                }
            })
            print("✅ OpenRouter configured")
        
        if CLOUDFLARE_API_KEY and CLOUDFLARE_ACCOUNT_ID:
            self.providers.append({
                'name': 'cloudflare',
                'key': CLOUDFLARE_API_KEY,
                'account_id': CLOUDFLARE_ACCOUNT_ID,
                'models': {
                    'default': '@cf/meta/llama-3.1-8b-instruct',
                    'smart': '@cf/meta/llama-3.1-8b-instruct',
                    'fast': '@cf/meta/llama-3.1-8b-instruct'
                },
                'headers': {
                    'Authorization': f'Bearer {CLOUDFLARE_API_KEY}',
                    'Content-Type': 'application/json'
                }
            })
            print("✅ Cloudflare configured")
        
        if not self.providers:
            print("⚠️ No AI providers configured!")
        else:
            print(f"🎯 Total providers: {len(self.providers)}")
    
    def get_current_provider(self):
        return self.providers[self.current_provider] if self.providers else None
    
    def switch_provider(self):
        if len(self.providers) <= 1:
            return self.get_current_provider()
        self.current_provider = (self.current_provider + 1) % len(self.providers)
        print(f"🔄 Switched to: {self.providers[self.current_provider]['name']}")
        return self.get_current_provider()
    
    async def chat_completion(self, messages, model_type='default', temperature=0.3,
                              max_tokens=1000, tools=None, tool_choice=None):
        if not self.providers:
            raise Exception("No AI providers configured")
        
        last_error = None
        for attempt in range(len(self.providers)):
            provider = self.get_current_provider()
            try:
                if provider['name'] == 'openrouter':
                    result = await self._call_openrouter(provider, messages, model_type,
                                                         temperature, max_tokens, tools, tool_choice)
                else:
                    result = await self._call_cloudflare(provider, messages, model_type,
                                                         temperature, max_tokens)
                
                self.request_counts[provider['name']] += 1
                return result
                
            except Exception as e:
                last_error = str(e)
                print(f"⚠️ {provider['name']} failed: {e}")
                if any(code in str(e) for code in ['429', '404', '401', 'rate limit']):
                    self.switch_provider()
                    await asyncio.sleep(1)
        
        raise Exception(f"All providers failed: {last_error}")
    
    async def _call_openrouter(self, provider, messages, model_type, temperature, max_tokens, tools, tool_choice):
        payload = {
            "model": provider['models'][model_type],
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens
        }
        if tools:
            payload["tools"] = tools
            payload["tool_choice"] = tool_choice or "auto"
        
        response = requests.post(provider['endpoint'], headers=provider['headers'],
                                json=payload, timeout=60)
        
        if response.status_code == 429:
            raise Exception("Rate limit (429)")
        elif response.status_code == 404:
            raise Exception(f"Model not found (404)")
        elif response.status_code == 401:
            raise Exception("Invalid API key (401)")
        
        response.raise_for_status()
        return response.json()
    
    async def _call_cloudflare(self, provider, messages, model_type, temperature, max_tokens):
        model = provider['models'][model_type]
        url = f"https://api.cloudflare.com/client/v4/accounts/{provider['account_id']}/ai/run/{model}"
        
        response = requests.post(url, headers=provider['headers'],
                                json={"messages": messages, "temperature": temperature,
                                      "max_tokens": max_tokens}, timeout=60)
        
        if response.status_code == 429:
            raise Exception("Rate limit (429)")
        
        response.raise_for_status()
        data = response.json()
        
        return {
            "choices": [{
                "message": {"role": "assistant",
                           "content": data.get('result', {}).get('response', '')},
                "finish_reason": "stop"
            }]
        }

ai_provider = FreeAIProvider()

# ============================================================
# ENCODER
# ============================================================
class LightweightEncoder:
    def __init__(self):
        self.encoder = None
        if SENTENCE_TRANSFORMERS_AVAILABLE:
            try:
                from sentence_transformers import SentenceTransformer
                self.encoder = SentenceTransformer('all-MiniLM-L6-v2')
                print("✅ Using sentence-transformers")
            except:
                pass
        
        if self.encoder is None:
            print("✅ Using hash-based embeddings")
            self.encoder = "hash"
    
    def encode(self, text: str) -> List[float]:
        if self.encoder and self.encoder != "hash":
            try:
                return self.encoder.encode(text).tolist()
            except:
                pass
        
        embedding = []
        for i in range(384):
            hash_input = f"{text}_{i}".encode('utf-8')
            hash_val = hashlib.md5(hash_input).hexdigest()
            val = (int(hash_val[:8], 16) / 0xFFFFFFFF) * 2 - 1
            embedding.append(round(val, 6))
        
        norm = math.sqrt(sum(x*x for x in embedding))
        if norm > 0:
            embedding = [x/norm for x in embedding]
        
        return embedding

encoder = LightweightEncoder()

# ============================================================
# WIKIPEDIA & WIKIDATA
# ============================================================
class WikipediaKnowledge:
    def __init__(self):
        self.wikipedia_api = "https://en.wikipedia.org/api/rest_v1"
        self.wikidata_api = "https://www.wikidata.org/w/api.php"
        self.wikipedia_action_api = "https://en.wikipedia.org/w/api.php"
        self.cache = {}
        self.cache_time = {}
        self.cache_duration = 3600
    
    def _get_cached(self, key: str) -> Optional[dict]:
        if key in self.cache and key in self.cache_time:
            if time.time() - self.cache_time[key] < self.cache_duration:
                return self.cache[key]
        return None
    
    def _set_cache(self, key: str, value: dict):
        self.cache[key] = value
        self.cache_time[key] = time.time()
    
    async def search_wikipedia(self, query: str, limit: int = 5) -> List[dict]:
        cache_key = f"search_{query}"
        cached = self._get_cached(cache_key)
        if cached:
            return cached
        
        try:
            response = requests.get(
                self.wikipedia_action_api,
                params={
                    "action": "query",
                    "list": "search",
                    "srsearch": query,
                    "srlimit": limit,
                    "format": "json",
                    "utf8": 1
                },
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                results = []
                for item in data.get("query", {}).get("search", []):
                    results.append({
                        "title": item.get("title"),
                        "snippet": item.get("snippet", "").replace("<span class=\"searchmatch\">", "**").replace("</span>", "**"),
                        "pageid": item.get("pageid"),
                        "wordcount": item.get("wordcount"),
                        "url": f"https://en.wikipedia.org/wiki/{item.get('title', '').replace(' ', '_')}"
                    })
                self._set_cache(cache_key, results)
                return results
            return []
        except Exception as e:
            print(f"Wikipedia search error: {e}")
            return []
    
    async def get_article(self, title: str) -> dict:
        cache_key = f"article_{title}"
        cached = self._get_cached(cache_key)
        if cached:
            return cached
        
        try:
            response = requests.get(
                f"{self.wikipedia_api}/page/summary/{title.replace(' ', '_')}",
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                result = {
                    "title": data.get("title"),
                    "extract": data.get("extract"),
                    "description": data.get("description"),
                    "url": data.get("content_urls", {}).get("desktop", {}).get("page"),
                    "image": data.get("thumbnail", {}).get("source"),
                    "coordinates": data.get("coordinates"),
                    "last_modified": data.get("timestamp")
                }
                self._set_cache(cache_key, result)
                return result
            
            response = requests.get(
                self.wikipedia_action_api,
                params={
                    "action": "query",
                    "titles": title,
                    "prop": "extracts|info",
                    "exintro": True,
                    "explaintext": True,
                    "inprop": "url",
                    "format": "json"
                },
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                pages = data.get("query", {}).get("pages", {})
                for page_id, page in pages.items():
                    if page_id != "-1":
                        result = {
                            "title": page.get("title"),
                            "extract": page.get("extract"),
                            "url": page.get("fullurl"),
                            "pageid": page.get("pageid")
                        }
                        self._set_cache(cache_key, result)
                        return result
            
            return {"error": f"Article not found: {title}"}
        except Exception as e:
            return {"error": str(e)}
    
    async def search_wikidata(self, query: str, limit: int = 5) -> List[dict]:
        try:
            response = requests.get(
                self.wikidata_api,
                params={
                    "action": "wbsearchentities",
                    "search": query,
                    "language": "en",
                    "limit": limit,
                    "format": "json"
                },
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                results = []
                for item in data.get("search", []):
                    results.append({
                        "id": item.get("id"),
                        "label": item.get("label"),
                        "description": item.get("description"),
                        "url": f"https://www.wikidata.org/wiki/{item.get('id')}"
                    })
                return results
            return []
        except Exception as e:
            print(f"Wikidata search error: {e}")
            return []
    
    async def get_wikidata_entity(self, entity_id: str) -> dict:
        try:
            response = requests.get(
                self.wikidata_api,
                params={
                    "action": "wbgetentities",
                    "ids": entity_id,
                    "languages": "en|zh",
                    "format": "json"
                },
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                entity = data.get("entities", {}).get(entity_id, {})
                
                result = {
                    "id": entity.get("id"),
                    "labels": entity.get("labels", {}),
                    "descriptions": entity.get("descriptions", {}),
                    "aliases": entity.get("aliases", {}),
                    "claims": {}
                }
                
                for prop_id, claims in entity.get("claims", {}).items():
                    values = []
                    for claim in claims[:3]:
                        mainsnak = claim.get("mainsnak", {})
                        if mainsnak.get("datatype") == "string":
                            values.append(mainsnak.get("datavalue", {}).get("value"))
                        elif mainsnak.get("datatype") == "wikibase-item":
                            val_id = mainsnak.get("datavalue", {}).get("value", {}).get("id")
                            if val_id:
                                values.append(val_id)
                    if values:
                        result["claims"][prop_id] = values
                
                return result
            return {"error": f"Entity not found: {entity_id}"}
        except Exception as e:
            return {"error": str(e)}

wikipedia_knowledge = WikipediaKnowledge()

# ============================================================
# RESILIENT TOOL EXECUTOR
# ============================================================
class ResilientToolExecutor:
    async def execute_with_recovery(self, tool_name: str, params: dict,
                                    max_retries: int = 3) -> dict:
        last_error = None
        
        for attempt in range(max_retries):
            try:
                result = await tool_registry.execute(tool_name, params)
                
                if result.get('success'):
                    return result
                
                if self._is_partially_valid(result):
                    return {**result, "partial": True}
                
                last_error = result.get('error', 'Unknown error')
                
            except Exception as e:
                last_error = str(e)
            
            wait_time = (2 ** attempt) + random.random()
            await asyncio.sleep(wait_time)
            
            params = self._adapt_params(params, attempt)
        
        fallback_result = await self._try_fallback(tool_name, params)
        if fallback_result:
            return fallback_result
        
        return {
            "success": False,
            "error": f"Failed after {max_retries} attempts: {last_error}",
            "suggestion": "Try rephrasing your query"
        }
    
    def _is_partially_valid(self, result: dict) -> bool:
        if not isinstance(result, dict):
            return False
        has_content = any(k in result for k in ['text', 'content', 'extract', 'results'])
        return has_content and not result.get('error')
    
    def _adapt_params(self, params: dict, attempt: int) -> dict:
        adapted = params.copy()
        
        if 'query' in adapted and attempt > 0:
            query = adapted['query']
            if attempt == 1:
                adapted['query'] = query[:100]
            elif attempt == 2:
                words = query.split()
                adapted['query'] = ' '.join(words[:5])
        
        return adapted
    
    async def _try_fallback(self, tool_name: str, params: dict) -> Optional[dict]:
        fallbacks = {
            'tavily_search': ['duckduckgo_search', 'wikipedia_search'],
            'browse_page': ['jina_reader', 'stealth_fetch'],
            'wikipedia_article': ['wikidata_entity', 'duckduckgo_search']
        }
        
        for fallback in fallbacks.get(tool_name, []):
            try:
                result = await tool_registry.execute(fallback, params)
                if result.get('success'):
                    return {**result, "fallback_used": fallback}
            except:
                continue
        
        return None

resilient_executor = ResilientToolExecutor()

# ============================================================
# AGENT SWARM
# ============================================================
class AgentSwarm:
    def __init__(self):
        self.agents = {
            'planner': {
                'system': """You are a planning agent. Break complex tasks into steps.
Output format: STEP 1: [action] | TOOL: [tool_name] | VERIFY: [expected_result]""",
                'model': 'fast'
            },
            'executor': {
                'system': """You are an execution agent. Execute one step at a time.
Use tools proactively. Report success/failure with evidence.""",
                'model': 'default'
            },
            'verifier': {
                'system': """You are a verification agent. Check if results meet goals.
Identify gaps, errors, or missing information. Be critical.""",
                'model': 'smart'
            },
            'synthesizer': {
                'system': """You are a synthesis agent. Combine multiple results into coherent answer.
Resolve conflicts and present balanced view.""",
                'model': 'smart'
            }
        }
    
    async def solve(self, task: str, session_id: str) -> dict:
        start_time = time.time()
        
        # Phase 1: Planning
        plan = await self._agent_call('planner', f"Create plan for: {task}")
        steps = self._parse_plan(plan)
        
        # Phase 2: Parallel execution
        results = await self._execute_parallel(steps, session_id)
        
        # Phase 3: Verification loop
        verified = False
        iterations = 0
        
        while not verified and iterations < 5:
            verification = await self._agent_call('verifier',
                f"Task: {task}\nResults: {json.dumps(results)}\nAre these complete?")
            
            if "COMPLETE" in verification.upper():
                verified = True
            else:
                gaps = self._extract_gaps(verification)
                additional = await self._fill_gaps(gaps, session_id)
                results.extend(additional)
                iterations += 1
        
        # Phase 4: Synthesis
        final = await self._agent_call('synthesizer',
            f"Synthesize from: {json.dumps(results)}")
        
        latency = (time.time() - start_time) * 1000
        
        return {
            'answer': final,
            'plan': steps,
            'iterations': iterations,
            'tools_used': self._extract_tools(results),
            'confidence': self._calculate_confidence(results, verification),
            'latency_ms': latency
        }
    
    async def _agent_call(self, agent_type: str, message: str) -> str:
        agent = self.agents[agent_type]
        
        response = await ai_provider.chat_completion(
            messages=[
                {"role": "system", "content": agent['system']},
                {"role": "user", "content": message}
            ],
            model_type=agent['model'],
            max_tokens=800
        )
        return response['choices'][0]['message']['content']
    
    def _parse_plan(self, plan_text: str) -> List[dict]:
        steps = []
        for line in plan_text.split('\n'):
            if 'STEP' in line:
                parts = line.split('|')
                step = {'action': line}
                for part in parts:
                    if 'TOOL:' in part:
                        step['tool'] = part.replace('TOOL:', '').strip()
                    if 'VERIFY:' in part:
                        step['verify'] = part.replace('VERIFY:', '').strip()
                steps.append(step)
        return steps or [{'action': plan_text}]
    
    async def _execute_parallel(self, steps: List[dict], session_id: str) -> List[dict]:
        tasks = []
        for step in steps:
            if step.get('tool'):
                task = self._execute_tool_step(step, session_id)
            else:
                task = self._agent_call('executor', step['action'])
            tasks.append(task)
        
        return await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _execute_tool_step(self, step: dict, session_id: str) -> dict:
        tool_name = step['tool']
        params = self._extract_params(step['action'])
        return await resilient_executor.execute_with_recovery(tool_name, params)
    
    def _extract_params(self, action: str) -> dict:
        # Simple extraction - can be enhanced
        return {'query': action}
    
    def _extract_gaps(self, verification: str) -> List[str]:
        gaps = []
        for line in verification.split('\n'):
            if any(x in line.lower() for x in ['missing', 'lack', 'need', 'no ', 'not '] ):
                gaps.append(line.strip('- '))
        return gaps
    
    async def _fill_gaps(self, gaps: List[str], session_id: str) -> List[dict]:
        results = []
        for gap in gaps:
            result = await self._agent_call('executor', f"Research and fill gap: {gap}")
            results.append({'gap': gap, 'result': result})
        return results
    
    def _extract_tools(self, results: List[dict]) -> List[str]:
        tools = []
        for r in results:
            if isinstance(r, dict):
                if 'tool' in r:
                    tools.append(r['tool'])
                if 'fallback_used' in r:
                    tools.append(r['fallback_used'])
        return list(set(tools))
    
    def _calculate_confidence(self, results: List[dict], verification: str) -> float:
        if "COMPLETE" in verification.upper() and "ERROR" not in verification.upper():
            return 0.9
        elif "PARTIAL" in verification.upper():
            return 0.6
        return 0.4

swarm = AgentSwarm()

# ============================================================
# CONVERGENT AGENT
# ============================================================
class ConvergentAgent:
    async def solve_with_convergence(self, task: str, session_id: str) -> dict:
        history = []
        
        for iteration in range(MAX_SAFETY_ITERATIONS):
            context = self._build_iteration_context(history)
            
            response = await ai_provider.chat_completion(
                messages=[
                    {"role": "system", "content": f"Iteration {iteration}. Refine answer."},
                    {"role": "user", "content": f"Task: {task}\nPrevious: {context}"}
                ],
                tools=tool_registry.get_tools_schema(),
                max_tokens=1000
            )
            
            current_answer = response['choices'][0]['message']['content']
            tools_used = self._extract_tools_from_response(response)
            
            embedding = encoder.encode(current_answer)
            
            history.append({
                'iteration': iteration,
                'answer': current_answer,
                'tools_used': tools_used,
                'embedding': embedding
            })
            
            if len(history) >= CONVERGENCE_WINDOW:
                if self._check_convergence(history[-CONVERGENCE_WINDOW:]):
                    return {
                        'answer': current_answer,
                        'iterations': iteration + 1,
                        'converged': True,
                        'tools_used': tools_used,
                        'history': history
                    }
            
            if iteration > 2 and not self._improvement_potential(history):
                return {
                    'answer': current_answer,
                    'iterations': iteration + 1,
                    'converged': False,
                    'reason': 'diminishing_returns',
                    'tools_used': tools_used
                }
        
        return {
            'answer': history[-1]['answer'],
            'iterations': MAX_SAFETY_ITERATIONS,
            'converged': False,
            'reason': 'safety_limit',
            'tools_used': history[-1]['tools_used']
        }
    
    def _build_iteration_context(self, history: List[dict]) -> str:
        if not history:
            return "No previous attempts."
        return f"Previous answer: {history[-1]['answer'][:500]}..."
    
    def _extract_tools_from_response(self, response: dict) -> List[str]:
        tools = []
        message = response['choices'][0]['message']
        if 'tool_calls' in message:
            for tc in message['tool_calls']:
                tools.append(tc['function']['name'])
        return tools
    
    def _check_convergence(self, recent_history: List[dict]) -> bool:
        if len(recent_history) < 2:
            return False
        
        similarities = []
        for i in range(len(recent_history) - 1):
            sim = self._cosine_similarity(
                recent_history[i]['embedding'],
                recent_history[i+1]['embedding']
            )
            similarities.append(sim)
        
        return all(s > CONVERGENCE_THRESHOLD for s in similarities)
    
    def _improvement_potential(self, history: List[dict]) -> bool:
        if len(history) < 3:
            return True
        
        recent_lengths = [len(h['answer']) for h in history[-3:]]
        return recent_lengths[-1] != recent_lengths[0]
    
    def _cosine_similarity(self, a: List[float], b: List[float]) -> float:
        dot = sum(x*y for x,y in zip(a,b))
        norm_a = sum(x*x for x in a) ** 0.5
        norm_b = sum(x*x for x in b) ** 0.5
        return dot / (norm_a * norm_b) if norm_a and norm_b else 0

convergent_agent = ConvergentAgent()

# ============================================================
# SELF-IMPROVING TOOLS
# ============================================================
class SelfImprovingTools:
    def __init__(self):
        self.generated_tools_dir = Path("./generated_tools")
        self.generated_tools_dir.mkdir(exist_ok=True)
        self.runtime_registry = {}
        self._load_persisted_tools()
    
    def _load_persisted_tools(self):
        try:
            conn = get_db()
            c = conn.cursor()
            c.execute("SELECT tool_name, code, description FROM generated_tools WHERE use_count > 0")
            for row in c.fetchall():
                self.runtime_registry[row[0]] = {
                    'code': row[1],
                    'description': row[2],
                    'persisted': True
                }
            conn.close()
        except Exception as e:
            print(f"Loading generated tools error: {e}")
    
    async def create_tool_on_demand(self, need_description: str, failed_tool: str = None) -> Optional[str]:
        analysis_prompt = f"""A tool has failed or is missing for: {need_description}
Failed tool: {failed_tool or 'None'}

Create a Python async function. Requirements:
1. Use only requests/bs4/stdlib (no paid APIs)
2. Handle errors gracefully
3. Return dict with 'success' boolean
4. Function name: auto_{int(time.time())}

Output ONLY the function code, no explanation."""
        
        code_response = await ai_provider.chat_completion(
            [{"role": "user", "content": analysis_prompt}],
            max_tokens=600
        )
        code = code_response['choices'][0]['message']['content']
        code = self._extract_code(code)
        
        tool_name = f"auto_{int(time.time())}"
        test_result = await self._test_tool(code, tool_name)
        
        if test_result['success']:
            tool_file = self.generated_tools_dir / f"{tool_name}.py"
            with open(tool_file, 'w') as f:
                f.write(code)
            
            try:
                conn = get_db()
                c = conn.cursor()
                c.execute("""
                    INSERT INTO generated_tools (tool_name, code, description, test_result)
                    VALUES (%s, %s, %s, %s)
                """, (tool_name, code, need_description, json.dumps(test_result)))
                conn.commit()
                conn.close()
            except Exception as e:
                print(f"Storing generated tool error: {e}")
            
            self.runtime_registry[tool_name] = {
                'code': code,
                'created_for': need_description,
                'test_result': test_result
            }
            
            # Add to tool registry dynamically
            tool_registry.tools[tool_name] = {
                'description': f"Auto-generated for: {need_description[:50]}",
                'parameters': {'query': 'string'},
                'handler': self._make_handler(code, tool_name)
            }
            
            return tool_name
        
        return None
    
    def _extract_code(self, text: str) -> str:
        if "```python" in text:
            return text.split("```python")[1].split("```")[0].strip()
        if "```" in text:
            return text.split("```")[1].split("```")[0].strip()
        return text.strip()
    
    async def _test_tool(self, code: str, tool_name: str) -> dict:
        try:
            namespace = {}
            exec(code, namespace)
            func = namespace.get(tool_name) or [v for v in namespace.values() if callable(v)][0]
            
            test_result = await func({'query': 'test'})
            
            return {
                'success': isinstance(test_result, dict) and 'success' in test_result,
                'result': test_result
            }
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def _make_handler(self, code: str, tool_name: str):
        async def handler(params: dict) -> str:
            try:
                namespace = {}
                exec(code, namespace)
                func = namespace.get(tool_name) or [v for v in namespace.values() if callable(v)][0]
                result = await func(params)
                return json.dumps(result) if not isinstance(result, str) else result
            except Exception as e:
                return f"Error: {e}"
        return handler

self_improving = SelfImprovingTools()

# ============================================================
# AUTONOMOUS MONITOR
# ============================================================
class AutonomousMonitor:
    def __init__(self):
        self.monitors = {}
        self.running = False
    
    def start_monitoring(self):
        self.running = True
        thread = threading.Thread(target=self._monitor_loop, daemon=True)
        thread.start()
        print("👁️ Autonomous monitoring started")
    
    def _monitor_loop(self):
        while self.running:
            for monitor_id, config in list(self.monitors.items()):
                try:
                    should_act = self._check_condition(config)
                    if should_act:
                        asyncio.run(self._execute_autonomous_action(config))
                        config['last_triggered'] = time.time()
                except Exception as e:
                    print(f"Monitor {monitor_id} error: {e}")
            
            time.sleep(60)
    
    def _check_condition(self, config: dict) -> bool:
        source_type = config.get('source_type')
        
        if source_type == 'time':
            return time.time() > config.get('schedule', 0)
        elif source_type == 'pattern':
            return self._check_conversation_pattern(config)
        
        return False
    
    def _check_conversation_pattern(self, config: dict) -> bool:
        try:
            conn = get_db()
            c = conn.cursor()
            c.execute("""
                SELECT user_message, ai_response 
                FROM conversations 
                WHERE timestamp > NOW() - INTERVAL '1 hour'
                AND session_id = %s
            """, (config.get('session_id'),))
            
            recent = c.fetchall()
            conn.close()
            
            topic = config.get('watch_topic', '').lower()
            mentions = sum(1 for r in recent if topic in r[0].lower())
            failures = sum(1 for r in recent if any(x in r[1].lower() for x in ['sorry', 'cannot', 'don\'t have']))
            
            return mentions >= 2 and failures >= 1
            
        except:
            return False
    
    async def _execute_autonomous_action(self, config: dict):
        action_type = config.get('action_type')
        
        if action_type == 'deep_research':
            result = await sophia.process_message(
                session_id=config['session_id'],
                user_message=f"Proactive research: {config['topic']}",
                context={'intent': 'autonomous_research', 'autonomous': True}
            )
            self._store_proactive_result(config['session_id'], result)
        
        elif action_type == 'tool_creation':
            await self_improving.create_tool_on_demand(config['need'])
    
    def _store_proactive_result(self, session_id: str, result: tuple):
        try:
            conn = get_db()
            c = conn.cursor()
            c.execute("""
                INSERT INTO proactive_notifications 
                (session_id, notification_type, subject, content, created_at)
                VALUES (%s, 'autonomous_research', 'Research Complete', %s, NOW())
            """, (session_id, result[0][:1000]))
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Storing proactive result error: {e}")

monitor = AutonomousMonitor()

# ============================================================
# TOOL REGISTRY
# ============================================================
class ToolRegistry:
    def __init__(self):
        self.tools = {}
        self._register_builtin_tools()
        self._load_generated_tools()
    
    def _register_builtin_tools(self):
        self.tools.update({
            'search_web': {
                'description': 'Search the web',
                'parameters': {'query': 'string'},
                'handler': self._tool_search_web
            },
            'duckduckgo_search': {
                'description': 'Search DuckDuckGo',
                'parameters': {'query': 'string', 'max_results': 'integer'},
                'handler': self._tool_duckduckgo_search
            },
            'wikipedia_search': {
                'description': 'Search Wikipedia',
                'parameters': {'query': 'string', 'limit': 'integer'},
                'handler': self._tool_wikipedia_search
            },
            'wikipedia_article': {
                'description': 'Get Wikipedia article',
                'parameters': {'title': 'string'},
                'handler': self._tool_wikipedia_article
            },
            'wikidata_search': {
                'description': 'Search Wikidata',
                'parameters': {'query': 'string', 'limit': 'integer'},
                'handler': self._tool_wikidata_search
            },
            'wikidata_entity': {
                'description': 'Get Wikidata entity',
                'parameters': {'entity_id': 'string'},
                'handler': self._tool_wikidata_entity
            },
            'company_info': {
                'description': 'Get company information',
                'parameters': {'company_name': 'string'},
                'handler': self._tool_company_info
            },
            'stealth_fetch': {
                'description': 'Fetch webpage with stealth headers',
                'parameters': {'url': 'string'},
                'handler': self._tool_stealth_fetch
            },
            'jina_reader': {
                'description': 'Read webpage as markdown',
                'parameters': {'url': 'string'},
                'handler': self._tool_jina_reader
            },
            'news_monitor': {
                'description': 'Monitor news on topic',
                'parameters': {'topic': 'string'},
                'handler': self._tool_news_monitor
            },
            'translate_chinese': {
                'description': 'Translate between English and Chinese',
                'parameters': {'text': 'string', 'target_language': 'string'},
                'handler': self._tool_translate_chinese
            },
            'china_rss_news': {
                'description': 'Fetch China news from RSS',
                'parameters': {'limit': 'integer'},
                'handler': self._tool_china_rss_news
            },
            'china_economic_indicator': {
                'description': 'Get China economic indicators',
                'parameters': {'indicator': 'string'},
                'handler': self._tool_china_economic_indicator
            }
        })
    
    def _load_generated_tools(self):
        for tool_name, tool_data in self_improving.runtime_registry.items():
            self.tools[tool_name] = {
                'description': tool_data.get('description', f'Generated tool {tool_name}'),
                'parameters': {'query': 'string'},
                'handler': self_improving._make_handler(tool_data['code'], tool_name)
            }
    
    async def execute(self, tool_name: str, params: dict) -> dict:
        if tool_name not in self.tools:
            return {'success': False, 'error': f"Tool '{tool_name}' not found"}
        
        tool = self.tools[tool_name]
        try:
            if 'handler' in tool:
                result = await tool['handler'](params)
            else:
                result = {'success': False, 'error': 'No handler'}
            
            if isinstance(result, str):
                return {'success': True, 'result': result}
            return result
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def get_tools_schema(self) -> List[dict]:
        OPTIONAL_PARAMS = {
            'limit', 'wait_time', 'full_page', 'submit_selector',
            'subreddit', 'sort_by', 'max_results', 'search_depth',
            'css_extractor', 'n_results', 'depth', 'format',
            'importance', 'context', 'priority', 'keywords', 'target_language'
        }

        schema = []
        for name, tool in self.tools.items():
            params = tool.get('parameters', {})
            properties = {}
            required = []

            for i, (pname, ptype) in enumerate(params.items()):
                if isinstance(ptype, str):
                    json_type = 'string'
                    if 'integer' in ptype.lower():
                        json_type = 'integer'
                    elif 'float' in ptype.lower():
                        json_type = 'number'
                    elif 'array' in ptype.lower():
                        json_type = 'array'
                    elif 'object' in ptype.lower():
                        json_type = 'object'
                    elif 'boolean' in ptype.lower():
                        json_type = 'boolean'
                    properties[pname] = {"type": json_type}
                    if i == 0 and pname not in OPTIONAL_PARAMS:
                        required.append(pname)

            schema.append({
                "type": "function",
                "function": {
                    "name": name,
                    "description": tool.get('description', ''),
                    "parameters": {
                        "type": "object",
                        "properties": properties,
                        "required": required
                    }
                }
            })
        return schema
    
    # Tool handlers
    async def _tool_search_web(self, params: dict) -> dict:
        return await self._tool_duckduckgo_search(params)
    
    async def _tool_duckduckgo_search(self, params: dict) -> dict:
        query = params.get('query', '')
        try:
            response = requests.get(
                "https://api.duckduckgo.com/",
                params={"q": query, "format": "json", "no_html": 1},
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                results = []
                
                if data.get('Abstract'):
                    results.append(f"Summary: {data['Abstract'][:500]}")
                
                for topic in data.get('RelatedTopics', [])[:5]:
                    if isinstance(topic, dict) and 'Text' in topic:
                        results.append(f"- {topic['Text'][:200]}")
                
                return {'success': True, 'results': results}
        except Exception as e:
            return {'success': False, 'error': str(e)}
        
        return {'success': False, 'error': 'No results'}
    
    async def _tool_wikipedia_search(self, params: dict) -> dict:
        query = params.get('query', '')
        limit = params.get('limit', 5)
        results = await wikipedia_knowledge.search_wikipedia(query, limit)
        return {'success': True, 'results': results}
    
    async def _tool_wikipedia_article(self, params: dict) -> dict:
        title = params.get('title', '')
        article = await wikipedia_knowledge.get_article(title)
        return {'success': 'error' not in article, **article}
    
    async def _tool_wikidata_search(self, params: dict) -> dict:
        query = params.get('query', '')
        limit = params.get('limit', 5)
        results = await wikipedia_knowledge.search_wikidata(query, limit)
        return {'success': True, 'results': results}
    
    async def _tool_wikidata_entity(self, params: dict) -> dict:
        entity_id = params.get('entity_id', '')
        entity = await wikipedia_knowledge.get_wikidata_entity(entity_id)
        return {'success': 'error' not in entity, **entity}
    
    async def _tool_company_info(self, params: dict) -> dict:
        company_name = params.get('company_name', '')
        
        wiki_results = await wikipedia_knowledge.search_wikipedia(f"{company_name} company", limit=1)
        
        result = {'company': company_name, 'wikipedia': None, 'wikidata': None, 'is_chinese': False}
        
        if wiki_results:
            article = await wikipedia_knowledge.get_article(wiki_results[0]["title"])
            result["wikipedia"] = article
            
            wd_results = await wikipedia_knowledge.search_wikidata(company_name, limit=1)
            if wd_results:
                entity = await wikipedia_knowledge.get_wikidata_entity(wd_results[0]["id"])
                result["wikidata"] = entity
                
                if "P17" in entity.get("claims", {}):
                    if "Q148" in str(entity["claims"]["P17"]):
                        result["is_chinese"] = True
                
                if entity.get("labels", {}).get("zh"):
                    result["chinese_name"] = entity["labels"]["zh"].get("value")
        
        return {'success': True, **result}
    
    async def _tool_stealth_fetch(self, params: dict) -> dict:
        url = params.get('url', '')
        
        headers_pool = [
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate, br",
                "DNT": "1",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1"
            },
            {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://www.google.com/"
            }
        ]
        
        headers = random.choice(headers_pool)
        
        try:
            session = requests.Session()
            response = session.get(url, headers=headers, timeout=30, allow_redirects=True)
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            for script in soup(["script", "style", "nav", "footer", "header"]):
                script.decompose()
            
            text = soup.get_text(separator='\n', strip=True)
            lines = [line.strip() for line in text.splitlines() if line.strip()]
            
            return {
                'success': True,
                'title': soup.title.string if soup.title else "No title",
                'text': '\n'.join(lines[:100])[:5000],
                'links': [a.get('href') for a in soup.find_all('a', href=True)][:20]
            }
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    async def _tool_jina_reader(self, params: dict) -> dict:
        url = params.get('url', '')
        try:
            response = requests.get(
                f"https://r.jina.ai/{url}",
                headers={"User-Agent": "SophiaAI/1.0"},
                timeout=30
            )
            
            if response.status_code == 200:
                return {'success': True, 'content': response.text[:3000]}
            return {'success': False, 'error': f"Status {response.status_code}"}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    async def _tool_news_monitor(self, params: dict) -> dict:
        topic = params.get('topic', 'China business')
        
        if NEWS_API_KEY:
            try:
                response = requests.get(
                    "https://newsapi.org/v2/everything",
                    params={"q": topic, "apiKey": NEWS_API_KEY, "pageSize": 5},
                    timeout=15
                )
                
                if response.status_code == 200:
                    data = response.json()
                    articles = data.get('articles', [])
                    if articles:
                        results = [f"- {a.get('title')} ({a.get('source', {}).get('name')})" for a in articles]
                        return {'success': True, 'results': results}
            except:
                pass
        
        if FEEDPARSER_AVAILABLE:
            try:
                query_encoded = urllib.parse.quote(topic)
                feed_url = f"https://news.google.com/rss/search?q={query_encoded}&hl=en-US&gl=US&ceid=US:en"
                feed = feedparser.parse(feed_url)
                
                if feed.entries:
                    results = []
                    for entry in feed.entries[:5]:
                        title = entry.get('title', 'No title')
                        source = entry.get('source', {}).get('title', 'Google News')
                        results.append(f"- {title} ({source})")
                    return {'success': True, 'results': results}
            except:
                pass
        
        return {'success': False, 'error': 'News fetch failed'}
    
    async def _tool_translate_chinese(self, params: dict) -> dict:
        text = params.get('text', '')
        target = params.get('target_language', 'en')
        
        if not text:
            return {'success': False, 'error': 'No text provided'}
        
        url = "https://libretranslate.de/translate"
        payload = {
            'q': text,
            'source': 'auto',
            'target': target,
            'format': 'text'
        }
        
        try:
            response = requests.post(url, data=payload, timeout=10)
            if response.status_code == 200:
                result = response.json()
                return {'success': True, 'translated': result.get('translatedText', '')}
            return {'success': False, 'error': f"Status {response.status_code}"}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    async def _tool_china_rss_news(self, params: dict) -> dict:
        if not FEEDPARSER_AVAILABLE:
            return {'success': False, 'error': 'feedparser not installed'}
        
        limit = params.get('limit', 5)
        
        feeds = [
            ("China Briefing", "https://www.china-briefing.com/news/feed/"),
            ("China Daily", "http://www.chinadaily.com.cn/business/rss.xml"),
            ("SCMP", "https://www.scmp.com/rss/4/feed"),
        ]
        
        all_entries = []
        for name, url in feeds:
            try:
                feed = feedparser.parse(url)
                for entry in feed.entries[:3]:
                    published = entry.get('published', '')[:10]
                    title = entry.get('title', 'No title')
                    link = entry.get('link', '')
                    all_entries.append((published, f"- {title} ({published})\n  {link}"))
            except:
                pass
        
        all_entries.sort(reverse=True)
        results = [item for _, item in all_entries[:limit]]
        
        return {'success': True, 'results': results} if results else {'success': False, 'error': 'No news found'}
    
    async def _tool_china_economic_indicator(self, params: dict) -> dict:
        indicator_map = {
            'gdp': 'NY.GDP.MKTP.CD',
            'gdp_growth': 'NY.GDP.MKTP.KD.ZG',
            'trade': 'NE.EXP.GNFS.CD',
            'imports': 'NE.IMP.GNFS.CD',
            'inflation': 'FP.CPI.TOTL.ZG',
        }
        
        indicator = params.get('indicator', 'gdp').lower()
        if indicator not in indicator_map:
            return {'success': False, 'error': f'Unknown indicator. Use: {list(indicator_map.keys())}'}
        
        wb_code = indicator_map[indicator]
        url = f"http://api.worldbank.org/v2/country/CN/indicator/{wb_code}?format=json&per_page=1&date=2022:2024"
        
        try:
            response = requests.get(url, timeout=15)
            if response.status_code == 200:
                data = response.json()
                if len(data) > 1 and data[1]:
                    latest = data[1][0]
                    return {
                        'success': True,
                        'indicator': indicator,
                        'value': latest.get('value'),
                        'date': latest.get('date')
                    }
            return {'success': False, 'error': 'No data found'}
        except Exception as e:
            return {'success': False, 'error': str(e)}

tool_registry = ToolRegistry()

# ============================================================
# MAIN SOPHIA AGENT - FULLY AGENTIC
# ============================================================
_session_histories: Dict[str, List[dict]] = defaultdict(list)
_session_histories_lock = threading.Lock()
_session_last_seen: Dict[str, float] = {}
SESSION_HISTORY_TTL_SECONDS = 3600
MAX_HISTORY_TURNS = 10

def _prune_stale_sessions():
    now = time.time()
    with _session_histories_lock:
        stale = [sid for sid, ts in _session_last_seen.items()
                 if now - ts > SESSION_HISTORY_TTL_SECONDS]
        for sid in stale:
            _session_histories.pop(sid, None)
            _session_last_seen.pop(sid, None)

class FullyAgenticSophia:
    def __init__(self):
        self.swarm = swarm
        self.convergent = convergent_agent
        self.monitor = monitor
        self.self_improving = self_improving
    
    async def process_message(self, session_id: str, user_message: str,
                              context: dict = None) -> Tuple[str, dict]:
        context = context or {}
        start_time = time.time()
        
        # Assess complexity for routing
        complexity = await self._assess_complexity(user_message)
        
        # Route to appropriate agent
        if complexity == 'high':
            result = await self.swarm.solve(user_message, session_id)
        elif complexity == 'medium':
            result = await self.convergent.solve_with_convergence(user_message, session_id)
        else:
            result = await self._direct_response(user_message, session_id)
        
        # Check for tool failures and auto-create if needed
        if not result.get('converged', True) or result.get('confidence', 1) < 0.5:
            new_tool = await self.self_improving.create_tool_on_demand(
                need_description=user_message,
                failed_tool=result.get('failed_tool')
            )
            if new_tool:
                # Retry with new tool
                result = await self.swarm.solve(user_message, session_id)
        
        # Schedule proactive research if low confidence
        if result.get('confidence', 0) < 0.7:
            await self._schedule_proactive(session_id, user_message)
        
        # Calculate latency
        latency_ms = (time.time() - start_time) * 1000
        
        # Trace
        tracer.trace(
            session_id=session_id,
            operation="agentic_process",
            inputs={"message": user_message[:100], "complexity": complexity},
            outputs={"answer": result['answer'][:200]},
            latency_ms=latency_ms,
            tools_used=result.get('tools_used', []),
            iteration=result.get('iterations', 1),
            converged=result.get('converged', True)
        )
        
        # Store to FAISS memory
        embedding = encoder.encode(f"User: {user_message}\nAssistant: {result['answer']}")
        local_memory.add(
            text=f"User: {user_message}\nAssistant: {result['answer']}",
            embedding=embedding,
            metadata={'session_id': session_id, 'intent': context.get('intent')}
        )
        
        # Update history
        self._update_history(session_id, user_message, result['answer'])
        
        # Store conversation
        conv_id = self._store_conversation(session_id, user_message, result, context)
        
        return result['answer'], {
            'tools_used': result.get('tools_used', []),
            'iterations': result.get('iterations', 1),
            'converged': result.get('converged', True),
            'confidence': result.get('confidence', 0.8),
            'complexity': complexity,
            'conversation_id': conv_id,
            'latency_ms': latency_ms
        }
    
    async def _assess_complexity(self, message: str) -> str:
        indicators = {
            'high': ['research', 'compare', 'analyze', 'investigate', 'find all', 'comprehensive', 'detailed report'],
            'medium': ['explain', 'how to', 'what is', 'help with', 'tell me about'],
            'low': ['hello', 'hi', 'thanks', 'yes', 'no', 'ok']
        }
        
        msg_lower = message.lower()
        for level, keywords in indicators.items():
            if any(k in msg_lower for k in keywords):
                return level
        
        # Use LLM for ambiguous cases
        check = await prompt_cache.get_or_compute(
            messages=[{"role": "user", "content": f"Classify complexity (high/medium/low): {message[:100]}"}],
            model='fast',
            compute_func=lambda: ai_provider.chat_completion(
                [{"role": "user", "content": f"Classify complexity (high/medium/low): {message[:100]}"}],
                model_type='fast', max_tokens=10
            )
        )
        
        response = check['choices'][0]['message']['content'].lower()
        if 'high' in response:
            return 'high'
        elif 'medium' in response:
            return 'medium'
        return 'low'
    
    async def _direct_response(self, user_message: str, session_id: str) -> dict:
        """Simple direct response for low complexity"""
        response = await ai_provider.chat_completion(
            messages=[{"role": "user", "content": user_message}],
            max_tokens=500
        )
        
        return {
            'answer': response['choices'][0]['message']['content'],
            'iterations': 1,
            'converged': True,
            'confidence': 0.9,
            'tools_used': []
        }
    
    async def _schedule_proactive(self, session_id: str, topic: str):
        """Schedule background research"""
        self.monitor.monitors[f"research_{session_id}_{int(time.time())}"] = {
            'source_type': 'time',
            'action_type': 'deep_research',
            'session_id': session_id,
            'topic': topic,
            'schedule': time.time() + 300  # 5 minutes
        }
    
    def _update_history(self, session_id: str, user_msg: str, assistant_msg: str):
        with _session_histories_lock:
            _session_last_seen[session_id] = time.time()
            hist = _session_histories[session_id]
            hist.append({"role": "user", "content": user_msg})
            hist.append({"role": "assistant", "content": assistant_msg})
            if len(hist) > MAX_HISTORY_TURNS * 2:
                _session_histories[session_id] = hist[-(MAX_HISTORY_TURNS * 2):]
        _prune_stale_sessions()
    
    def _store_conversation(self, session_id: str, user_message: str, 
                           result: dict, context: dict) -> Optional[int]:
        try:
            conn = get_db()
            c = conn.cursor()
            c.execute("""
                INSERT INTO conversations 
                (session_id, user_message, ai_response, intent, tools_used, 
                 confidence_score, iterations, converged)
                VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, %s)
                RETURNING id
            """, (session_id, user_message, result['answer'], 
                  context.get('intent'), json.dumps(result.get('tools_used', [])),
                  result.get('confidence'), result.get('iterations'), 
                  result.get('converged')))
            conv_id = c.fetchone()[0]
            conn.commit()
            conn.close()
            return conv_id
        except Exception as e:
            print(f"Conversation storage error: {e}")
            return None

sophia = FullyAgenticSophia()

# ============================================================
# BACKGROUND WORKERS
# ============================================================
def goal_executor():
    while True:
        try:
            time.sleep(GOAL_EXECUTION_INTERVAL_MINUTES * 60)
            
            if not DATABASE_URL:
                continue
            
            conn = get_db()
            c = conn.cursor()
            c.execute("""
                SELECT id, goal_type, goal_description FROM autonomous_goals 
                WHERE status = 'pending' AND priority >= 5
                ORDER BY priority DESC, created_at ASC
                LIMIT %s
            """, (MAX_CONCURRENT_GOALS,))
            goals = c.fetchall()
            conn.close()
            
            for goal_id, goal_type, description in goals:
                try:
                    conn2 = get_db()
                    c2 = conn2.cursor()
                    c2.execute(
                        "UPDATE autonomous_goals SET status = 'in_progress', started_at = NOW() WHERE id = %s",
                        (goal_id,)
                    )
                    conn2.commit()
                    conn2.close()
                    
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    result, meta = loop.run_until_complete(
                        sophia.process_message(
                            session_id=f"autonomous_goal_{goal_id}",
                            user_message=description,
                            context={"intent": goal_type, "autonomous": True}
                        )
                    )
                    loop.close()
                    
                    conn3 = get_db()
                    c3 = conn3.cursor()
                    c3.execute("""
                        UPDATE autonomous_goals 
                        SET status = 'completed', completed_at = NOW(),
                            result = %s,
                            completed_subtasks = %s::jsonb
                        WHERE id = %s
                    """, (result[:2000], json.dumps({"tools_used": meta.get('tools_used', [])}), goal_id))
                    conn3.commit()
                    conn3.close()
                    
                except Exception as e:
                    print(f"Goal {goal_id} failed: {e}")
                    
        except Exception as outer:
            print(f"Goal executor error: {outer}")

# ============================================================
# FASTAPI APP
# ============================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    
    goal_thread = threading.Thread(target=goal_executor, daemon=True)
    goal_thread.start()
    
    monitor.start_monitoring()
    
    print(f"""
    ╔══════════════════════════════════════════════════════════════╗
    ║     SOPHIA AI SERVER v11.0 - FULLY AGENTIC EDITION          ║
    ╠══════════════════════════════════════════════════════════════╣
    ║  🤖 Agent Swarm: Multi-agent collaboration                   ║
    ║  🎯 Convergent Depth: Dynamic iteration control              ║
    ║  🔧 Self-Improving Tools: Auto-generation                    ║
    ║  👁️ Autonomous Monitor: Self-triggering actions              ║
    ║  🧠 Local FAISS: Zero-cost vector storage                    ║
    ║  🕵️ Stealth Browser: No Playwright needed                    ║
    ║  💾 ZeroCostTracer: Local observability                      ║
    ║  ⚡ Prompt Cache: 30-50% speedup                             ║
    ║  🔄 Resilient Execution: Smart retries                       ║
    ╚══════════════════════════════════════════════════════════════╝
    """)
    
    yield
    
    print("🛑 Sophia AI Server shutting down...")

app = FastAPI(
    title="Sophia AI Server v11.0",
    description="Fully Agentic Edition with Multi-Agent Swarm",
    version="11.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================
# API ENDPOINTS
# ============================================================
class ChatRequest(BaseModel):
    session_id: str
    message: str
    context: Optional[dict] = None

class ChatResponse(BaseModel):
    response: str
    tools_used: List[str] = []
    iterations: int = 1
    converged: bool = True
    confidence: float = 0.8
    complexity: str = "medium"
    conversation_id: Optional[int] = None
    latency_ms: float = 0

@app.get("/")
async def root():
    return {
        "service": "Sophia AI Server",
        "version": "11.0.0",
        "features": [
            "agent_swarm", "convergent_depth", "self_improving_tools",
            "autonomous_monitor", "local_faiss", "zero_cost_tracer"
        ],
        "tools_count": len(tool_registry.tools),
        "status": "running"
    }

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "faiss_vectors": local_memory.index.ntotal,
        "cache_stats": prompt_cache.get_stats(),
        "tracer_stats": tracer.get_stats(),
        "generated_tools": len(self_improving.runtime_registry)
    }

@app.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    response, metadata = await sophia.process_message(
        request.session_id,
        request.message,
        request.context
    )
    return ChatResponse(
        response=response,
        tools_used=metadata.get('tools_used', []),
        iterations=metadata.get('iterations', 1),
        converged=metadata.get('converged', True),
        confidence=metadata.get('confidence', 0.8),
        complexity=metadata.get('complexity', 'medium'),
        conversation_id=metadata.get('conversation_id'),
        latency_ms=metadata.get('latency_ms', 0)
    )

@app.post("/chat/stream")
async def chat_stream(request: ChatRequest):
    async def event_generator():
        try:
            response_text, metadata = await sophia.process_message(
                request.session_id,
                request.message,
                request.context
            )
            
            chunk_size = 50
            for i in range(0, len(response_text), chunk_size):
                chunk = response_text[i:i + chunk_size]
                yield f"data: {json.dumps({'type': 'text', 'content': chunk})}\n\n"
                await asyncio.sleep(0.02)
            
            yield f"data: {json.dumps({'type': 'done', **metadata})}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
    
    return StreamingResponse(event_generator(), media_type="text/event-stream")

@app.get("/tools")
async def list_tools():
    return {
        "count": len(tool_registry.tools),
        "tools": [{"name": k, "description": v.get('description', '')} 
                  for k, v in tool_registry.tools.items()]
    }

@app.post("/tools/{tool_name}/execute")
async def execute_tool(tool_name: str, params: dict):
    result = await tool_registry.execute(tool_name, params)
    return result

@app.get("/memory/status")
async def get_memory_status():
    return local_memory.get_status()

@app.get("/memory/search")
async def search_memory(query: str, k: int = 5):
    embedding = encoder.encode(query)
    results = local_memory.search(embedding, k)
    return {"query": query, "results": results}

@app.get("/admin/stats")
async def admin_stats(password: str):
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid password")
    
    return {
        "tracer": tracer.get_stats(),
        "cache": prompt_cache.get_stats(),
        "faiss": local_memory.get_status(),
        "generated_tools": len(self_improving.runtime_registry),
        "active_monitors": len(monitor.monitors)
    }

@app.post("/feedback")
async def submit_feedback(session_id: str, rating: int, comment: str = None, conversation_id: int = None):
    if rating < 1 or rating > 5:
        raise HTTPException(status_code=400, detail="Rating must be 1-5")
    
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("""
            INSERT INTO user_feedback (session_id, conversation_id, rating, comment)
            VALUES (%s, %s, %s, %s)
        """, (session_id, conversation_id, rating, comment))
        conn.commit()
        conn.close()
        return {"status": "thank you!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)