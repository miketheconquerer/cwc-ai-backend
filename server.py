"""
================================================================================
SOPHIA AI SERVER v10.7.2 - CHINA BUSINESS ENHANCED EDITION
================================================================================
PATCH v10.7.2:
🔴 FIX: Pool leak — all conn.close() replaced with release_db() across every function/endpoint
🔢 MAX_AGENT_ITERATIONS raised to 8 (6 real tool rounds after planning + reflection)
⚠️  Tool exceptions now reported back to Sophia (no longer silently swallowed)
📈 Serper quota tracker restored (auto-fallback to DuckDuckGo at 80 searches/day)
🚀 _init_pool() now called at startup via lifespan
================================================================================
"""

from fastapi import FastAPI, BackgroundTasks, Request, HTTPException, Query
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
from collections import defaultdict
from datetime import datetime, timedelta
from dotenv import load_dotenv
from contextlib import asynccontextmanager
from typing import List, Dict, Any, Optional, Tuple, Callable
import uuid
import traceback
import base64
import math
import urllib.parse
import xml.etree.ElementTree as ET

# Optional: RSS feed parser for free news
FEEDPARSER_AVAILABLE = False
try:
    import feedparser
    FEEDPARSER_AVAILABLE = True
    print("✅ feedparser available for Google News RSS")
except ImportError:
    print("⚠️ feedparser not installed. Install with: pip install feedparser")

# Optional: Sentence Transformers for Embeddings
SENTENCE_TRANSFORMERS_AVAILABLE = False
try:
    from sentence_transformers import SentenceTransformer
    SENTENCE_TRANSFORMERS_AVAILABLE = True
    print("✅ Sentence Transformers available")
except Exception as e:
    print(f"⚠️ sentence-transformers not installed: {e}")

# Optional: Playwright for Browser Automation
PLAYWRIGHT_AVAILABLE = False
try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
    print("✅ Playwright available for browser automation")
except Exception as e:
    print(f"⚠️ Playwright not installed: {e}")
    print("   Browser automation will use HTTP fallback")

load_dotenv()

# ============================================================
# CONFIGURATION
# ============================================================
BREVO_API_KEY   = os.getenv("BREVO_API_KEY", "")
SENDER_EMAIL    = os.getenv("SENDER_EMAIL", "888nv666@gmail.com")
RECIPIENT_EMAIL = "digkasm@proton.me"
DATABASE_URL    = os.getenv("DATABASE_URL")

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
CLOUDFLARE_API_KEY = os.getenv("CLOUDFLARE_API_KEY", "")
CLOUDFLARE_ACCOUNT_ID = os.getenv("CLOUDFLARE_ACCOUNT_ID", "")

ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")
if ADMIN_PASSWORD == "admin123":
    print("⚠️  WARNING: ADMIN_PASSWORD is using the insecure default 'admin123'. Set the ADMIN_PASSWORD env var!")

# ============================================================
# VECTOR DATABASE CONFIGURATION
# ============================================================
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

# ============================================================
# Web Search & Discovery APIs
# ============================================================
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY", "")
JINA_API_KEY = os.getenv("JINA_API_KEY", "")
NEWS_API_KEY = os.getenv("NEWS_API_KEY", "")

# ============================================================
# Promotion & SEO APIs
# ============================================================
INDEXNOW_KEY = os.getenv("INDEXNOW_KEY", "")
GOOGLE_SEARCH_CONSOLE_KEY = os.getenv("GOOGLE_SEARCH_CONSOLE_KEY", "")

# ============================================================
# Bing Webmaster API
# ============================================================
BING_WEBMASTER_API_KEY = os.getenv("BING_WEBMASTER_API_KEY", "")
BING_SEARCH_API_KEY = os.getenv("BING_SEARCH_API_KEY", "")
BING_CUSTOM_CONFIG_ID = os.getenv("BING_CUSTOM_CONFIG_ID", "")

# ============================================================
# Social & Research APIs
# ============================================================
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID", "")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET", "")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT", "SophiaAI/1.0 by ChinaWestConnector")

# ============================================================
# Web Scraping & Geocoding APIs
# ============================================================
ZENROWS_API_KEY = os.getenv("ZENROWS_API_KEY", "")

# ============================================================
# Serper.dev — Google Search API (100 free searches/day)
# ============================================================
SERPER_API_KEY = os.getenv("SERPER_API_KEY", "")

# ============================================================
# Hugging Face API (free embeddings — zero RAM cost on Render)
# ============================================================
HUGGINGFACE_API_KEY = os.getenv("HUGGINGFACE_API_KEY", "")
HF_EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
HF_EMBEDDING_URL = f"https://api-inference.huggingface.co/pipeline/feature-extraction/{HF_EMBEDDING_MODEL}"

# Intelligence Settings
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

# ============================================================
# TOOL OUTPUT TRUNCATION
# Keeps context window lean — long tool results are summarised
# ============================================================
TOOL_OUTPUT_MAX_CHARS = 800   # truncate tool result if longer than this

def _truncate_tool_output(text: str, max_chars: int = TOOL_OUTPUT_MAX_CHARS) -> str:
    """Truncate a tool result to max_chars, appending a note if trimmed."""
    if not isinstance(text, str):
        text = json.dumps(text)
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + f"\n… [truncated — {len(text) - max_chars} chars omitted]"

# ============================================================
# CONFIDENCE SCORING
# Rates the response based on how many tools verified the answer
# ============================================================
def _compute_confidence(tools_used: List[str], final_response: str) -> dict:
    """
    Compute a simple confidence score based on tool usage.
    Returns dict with level (High/Medium/Low) and score 0-100.
    """
    n = len(tools_used)
    unique = len(set(tools_used))

    # Penalty keywords that suggest uncertainty
    uncertain_phrases = ['i\'m not sure', 'i don\'t know', 'unclear', 'cannot confirm',
                         'may not be accurate', 'might be', 'possibly', 'not certain']
    has_uncertainty = any(p in final_response.lower() for p in uncertain_phrases)

    if unique >= 3 and not has_uncertainty:
        level, score = 'High', 90
    elif unique >= 1 and not has_uncertainty:
        level, score = 'Medium', 65
    elif has_uncertainty:
        level, score = 'Low', 35
    else:
        level, score = 'Low', 25

    return {'level': level, 'score': score, 'tools_verified': unique}

# ============================================================
# DATABASE LAYER — Connection Pool
# release_db() MUST be called after every get_db() call.
# ============================================================
from psycopg2 import pool as _psycopg2_pool

_db_pool = None

def _init_pool():
    global _db_pool
    if DATABASE_URL and _db_pool is None:
        try:
            _db_pool = _psycopg2_pool.SimpleConnectionPool(1, 8, DATABASE_URL)
            print("✅ DB connection pool initialised (min=1, max=8)")
        except Exception as e:
            print(f"⚠️ DB pool init failed — falling back to direct connections: {e}")

def get_db():
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL not configured")
    if _db_pool:
        return _db_pool.getconn()
    return psycopg2.connect(DATABASE_URL)

def release_db(conn):
    if conn is None:
        return
    try:
        if _db_pool:
            _db_pool.putconn(conn)
        else:
            conn.close()
    except Exception:
        try:
            conn.close()
        except Exception:
            pass

def init_db():
    """Initialize all database tables"""
    try:
        conn = get_db()
        c = conn.cursor()
        
        # Conversations table
        c.execute("""
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
                timestamp TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # User profiles table
        c.execute("""
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
        """)
        
        # Autonomous goals table
        c.execute("""
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
        """)
        
        # Agent tasks table
        c.execute("""
            CREATE TABLE IF NOT EXISTS agent_tasks (
                id SERIAL PRIMARY KEY,
                session_id VARCHAR(100),
                task_type VARCHAR(50),
                task_description TEXT,
                status VARCHAR(20) DEFAULT 'pending',
                assigned_agent VARCHAR(50),
                result TEXT,
                metadata JSONB,
                created_at TIMESTAMP DEFAULT NOW(),
                started_at TIMESTAMP,
                completed_at TIMESTAMP
            )
        """)
        
        # Agent versions table
        c.execute("""
            CREATE TABLE IF NOT EXISTS agent_versions (
                id SERIAL PRIMARY KEY,
                version_number INTEGER,
                prompt_hash VARCHAR(64) UNIQUE,
                prompt_text TEXT,
                performance_score FLOAT,
                deployed BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # Environment alerts table
        c.execute("""
            CREATE TABLE IF NOT EXISTS environment_alerts (
                id SERIAL PRIMARY KEY,
                source VARCHAR(100),
                change_detected TEXT,
                user_segments JSONB,
                notified BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # Tool registry table
        c.execute("""
            CREATE TABLE IF NOT EXISTS tool_registry (
                id SERIAL PRIMARY KEY,
                tool_name VARCHAR(100) UNIQUE,
                description TEXT,
                parameters JSONB,
                implementation TEXT,
                created_by VARCHAR(100),
                success_rate FLOAT DEFAULT 1.0,
                use_count INTEGER DEFAULT 0,
                deployed BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # Proactive notifications table
        c.execute("""
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
        
        # Learning events table
        c.execute("""
            CREATE TABLE IF NOT EXISTS learning_events (
                id SERIAL PRIMARY KEY,
                event_type VARCHAR(50),
                description TEXT,
                improvement_data JSONB,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # Learning feedback table
        c.execute("""
            CREATE TABLE IF NOT EXISTS learning_feedback (
                id SERIAL PRIMARY KEY,
                session_id VARCHAR(100),
                original_response TEXT,
                corrected_response TEXT,
                user_feedback TEXT,
                intent VARCHAR(50),
                learned_improvement TEXT,
                applied BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # News cache table
        c.execute("""
            CREATE TABLE IF NOT EXISTS news_cache (
                id SERIAL PRIMARY KEY,
                topic VARCHAR(100),
                news_data JSONB,
                source VARCHAR(50),
                fetched_at TIMESTAMP DEFAULT NOW(),
                expires_at TIMESTAMP
            )
        """)
        
        # Tool chain history
        c.execute("""
            CREATE TABLE IF NOT EXISTS tool_chain_history (
                id SERIAL PRIMARY KEY,
                session_id VARCHAR(100),
                query TEXT,
                tools_used JSONB,
                success BOOLEAN,
                user_satisfied BOOLEAN,
                execution_time_ms INTEGER,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # Reflection history
        c.execute("""
            CREATE TABLE IF NOT EXISTS reflection_history (
                id SERIAL PRIMARY KEY,
                session_id VARCHAR(100),
                original_response TEXT,
                reflection TEXT,
                improved_response TEXT,
                confidence_before FLOAT,
                confidence_after FLOAT,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # Vector memory tables for pgvector
        try:
            c.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            print("✅ pgvector extension enabled")
        except:
            print("⚠️ pgvector extension not available")
        
        c.execute("""
            CREATE TABLE IF NOT EXISTS episodic_memories (
                id SERIAL PRIMARY KEY,
                session_id VARCHAR(100),
                memory_text TEXT,
                embedding vector(384),
                intent VARCHAR(50),
                success_score INTEGER,
                metadata JSONB,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        c.execute("""
            CREATE TABLE IF NOT EXISTS semantic_memories (
                id SERIAL PRIMARY KEY,
                fact_type VARCHAR(100),
                fact_value TEXT,
                embedding vector(384),
                importance INTEGER DEFAULT 5,
                source VARCHAR(100),
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # Create vector indexes
        try:
            c.execute("""
                CREATE INDEX IF NOT EXISTS episodic_embedding_idx 
                ON episodic_memories USING ivfflat (embedding vector_cosine_ops)
                WITH (lists = 100);
            """)
            c.execute("""
                CREATE INDEX IF NOT EXISTS semantic_embedding_idx 
                ON semantic_memories USING ivfflat (embedding vector_cosine_ops)
                WITH (lists = 100);
            """)
            print("✅ Vector indexes created")
        except Exception as e:
            print(f"⚠️ Vector indexes not created: {e}")

        # User feedback table (v10.1)
        c.execute("""
            CREATE TABLE IF NOT EXISTS user_feedback (
                id SERIAL PRIMARY KEY,
                session_id VARCHAR(100),
                conversation_id INTEGER REFERENCES conversations(id) ON DELETE SET NULL,
                rating INTEGER CHECK (rating >= 1 AND rating <= 5),
                comment TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        print("✅ User feedback table created")

        # Persistent session histories table (v10.4)
        # Survives Render restarts — replaces in-memory only storage
        c.execute("""
            CREATE TABLE IF NOT EXISTS session_histories (
                id SERIAL PRIMARY KEY,
                session_id VARCHAR(100) UNIQUE,
                history JSONB DEFAULT '[]'::jsonb,
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        print("✅ Persistent session histories table created")

        conn.commit()
        release_db(conn)
        print("✅ Database tables initialized")
        run_migrations()
    except Exception as e:
        print(f"⚠️ Database initialization error: {e}")

def run_migrations():
    """Add missing columns to existing tables"""
    migrations = [
        ("conversations", "confidence_score", "FLOAT"),
        ("tool_registry", "parameters", "JSONB"),
        ("conversations", "goals_extracted", "JSONB"),
        ("conversations", "tools_used", "JSONB"),
        ("user_profiles", "updated_at", "TIMESTAMP DEFAULT NOW()"),
    ]
    
    try:
        conn = get_db()
        c = conn.cursor()
        
        for table, column, dtype in migrations:
            try:
                c.execute(f"""
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_name = %s AND column_name = %s
                """, (table, column))
                if not c.fetchone():
                    c.execute(f"ALTER TABLE {table} ADD COLUMN {column} {dtype}")
                    print(f"✅ Migration: Added {column} to {table}")
            except Exception as e:
                if "already exists" not in str(e):
                    print(f"⚠️ Migration warning ({table}.{column}): {e}")
        
        conn.commit()
        release_db(conn)
    except Exception as e:
        print(f"⚠️ Migration error: {e}")

def get_or_create_user_profile(session_id: str) -> dict:
    """Get or create user profile"""
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
        
        release_db(conn)
        return dict(profile) if profile else {}
    except Exception as e:
        print(f"Profile error: {e}")
        return {}

def update_user_profile(session_id: str, **kwargs):
    """Update user profile fields"""
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
        release_db(conn)
    except Exception as e:
        print(f"Update profile error: {e}")

# ============================================================
# EMAIL HELPER
# ============================================================
def send_email_brevo(to_email: str, subject: str, content: str) -> bool:
    """Send email via Brevo API"""
    if not BREVO_API_KEY or not to_email:
        return False
    
    try:
        response = requests.post(
            "https://api.brevo.com/v3/smtp/email",
            headers={
                "accept": "application/json",
                "content-type": "application/json",
                "api-key": BREVO_API_KEY
            },
            json={
                "sender": {"email": SENDER_EMAIL, "name": "Sophia - CWC"},
                "to": [{"email": to_email}],
                "subject": subject,
                "textContent": content
            },
            timeout=30
        )
        return response.status_code == 201
    except Exception as e:
        print(f"Email error: {e}")
        return False

# ============================================================
# FREE AI PROVIDER MANAGER
# ============================================================
class FreeAIProvider:
    """Manages 100% free AI providers with automatic fallback"""
    
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
            print("✅ OpenRouter configured (50 requests/day FREE)")
        
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
            print("✅ Cloudflare Workers AI configured (10K neurons/day FREE)")
        
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
                if any(code in str(e) for code in ['429', '404', '401', 'rate limit', 'not found', 'invalid']):
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
            error_detail = response.text[:200] if response.text else "Model not found"
            print(f"❌ OpenRouter 404: {error_detail}")
            raise Exception(f"Model not found (404): {provider['models'][model_type]}")
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
# HYBRID VECTOR MEMORY SYSTEM
# ============================================================
class HybridVectorMemory:
    """Hybrid memory system supporting multiple backends"""
    
    def __init__(self):
        self.encoder = None
        self.backend_type = "memory"
        self.chroma_client = None
        self.episodic_collection = None
        self.semantic_collection = None
        self.supabase_db_url = SUPABASE_DB_URL
        self.memory_store = {"episodic": [], "semantic": []}
        self.initialized = False
        
        self._init_encoder()
        backend = self._determine_backend()
        self._init_backend(backend)
    
    def _init_encoder(self):
        """Initialize the sentence encoder.
        Priority:
          1. Hugging Face Inference API (free, zero RAM on Render) ✅
          2. sentence-transformers local (if installed)
          3. hash-based fallback (last resort)
        """
        if HUGGINGFACE_API_KEY:
            self.encoder = "huggingface_api"
            print("✅ Using Hugging Face API embeddings (all-MiniLM-L6-v2) — zero RAM cost")
            return

        if SENTENCE_TRANSFORMERS_AVAILABLE:
            try:
                from sentence_transformers import SentenceTransformer
                self.encoder = SentenceTransformer('all-MiniLM-L6-v2')
                print("✅ Sentence encoder initialized (all-MiniLM-L6-v2)")
                return
            except Exception as e:
                print(f"⚠️ Encoder init failed: {e}")

        print("⚠️ No HF API key found — using hash-based embeddings (memory recall will be limited)")
        self.encoder = "hash"

    def encode(self, text: str) -> List[float]:
        """Encode text to a semantic embedding vector.
        Uses Hugging Face API when available, falls back gracefully."""

        # --- Hugging Face API (best, free, no RAM cost) ---
        if self.encoder == "huggingface_api":
            try:
                response = requests.post(
                    HF_EMBEDDING_URL,
                    headers={"Authorization": f"Bearer {HUGGINGFACE_API_KEY}"},
                    json={"inputs": text[:512], "options": {"wait_for_model": True}},
                    timeout=15
                )
                if response.status_code == 200:
                    data = response.json()
                    # HF returns list-of-lists for batches; we sent one string
                    if isinstance(data, list) and len(data) > 0:
                        vec = data[0] if isinstance(data[0], list) else data
                        # Normalise
                        norm = math.sqrt(sum(x * x for x in vec))
                        if norm > 0:
                            vec = [x / norm for x in vec]
                        return vec
                else:
                    print(f"⚠️ HF embedding API error {response.status_code}: {response.text[:200]}")
            except Exception as e:
                print(f"⚠️ HF embedding failed, falling back to hash: {e}")
            # Fall through to hash if API call failed

        # --- Local sentence-transformers ---
        if self.encoder and self.encoder not in ("hash", "huggingface_api"):
            try:
                return self.encoder.encode(text).tolist()
            except Exception:
                pass

        # --- Hash-based fallback ---
        embedding = []
        for i in range(384):
            hash_input = f"{text}_{i}".encode('utf-8')
            hash_val = hashlib.md5(hash_input).hexdigest()
            val = (int(hash_val[:8], 16) / 0xFFFFFFFF) * 2 - 1
            embedding.append(round(val, 6))
        norm = math.sqrt(sum(x * x for x in embedding))
        if norm > 0:
            embedding = [x / norm for x in embedding]
        return embedding
    
    def _determine_backend(self) -> str:
        """Determine which backend to use based on config"""
        if VECTOR_DB_TYPE != "auto":
            return VECTOR_DB_TYPE
        
        if CHROMA_CLOUD_API_KEY:
            return "chroma_cloud"
        elif CHROMA_SERVER_URL:
            return "chroma_remote"
        elif SUPABASE_DB_URL:
            return "supabase"
        else:
            return "memory"
    
    def _init_backend(self, backend: str):
        """Initialize the selected backend"""
        print(f"🗄️ Initializing vector backend: {backend}")
        
        if backend == "supabase":
            self._init_supabase()
        else:
            self._init_memory()
    
    def _init_supabase(self):
        """Initialize Supabase pgvector backend"""
        try:
            conn = psycopg2.connect(self.supabase_db_url)
            conn.close()
            
            self.backend_type = "supabase"
            self.initialized = True
            print("✅ Supabase pgvector connected")
            
        except Exception as e:
            print(f"⚠️ Supabase connection failed: {e}")
            print("   Falling back to in-memory storage...")
            self._init_memory()
    
    def _init_memory(self):
        """Initialize in-memory fallback"""
        self.backend_type = "memory"
        self.initialized = True
        print("✅ In-memory vector storage initialized (ephemeral)")
    
    def store_episodic(self, session_id: str, user_msg: str, response: str, 
                       success_score: int, intent: str, metadata: dict = None):
        """Store episodic memory (conversation)"""
        if not self.initialized:
            return
        
        try:
            text = f"User: {user_msg}\nSophia: {response}"
            embedding = self.encode(text)
            memory_id = f"ep_{session_id}_{int(time.time())}"
            metadata = metadata or {}
            metadata.update({
                "session_id": session_id, "intent": intent,
                "success_score": success_score, "timestamp": datetime.now().isoformat()
            })
            
            if self.backend_type == "supabase":
                self._store_supabase_episodic(memory_id, session_id, text, embedding, metadata)
            else:
                self.memory_store["episodic"].append({
                    "id": memory_id, "text": text, "embedding": embedding, "metadata": metadata
                })
                
        except Exception as e:
            print(f"Episodic storage error: {e}")
    
    def _store_supabase_episodic(self, memory_id: str, session_id: str, text: str, 
                                  embedding: List[float], metadata: dict):
        """Store episodic memory in Supabase pgvector"""
        try:
            conn = psycopg2.connect(self.supabase_db_url)
            c = conn.cursor()
            c.execute("""
                INSERT INTO episodic_memories (session_id, memory_text, embedding, intent, success_score, metadata)
                VALUES (%s, %s, %s::vector, %s, %s, %s::jsonb)
            """, (session_id, text, str(embedding), metadata.get("intent"), 
                  metadata.get("success_score"), json.dumps(metadata)))
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Supabase episodic storage error: {e}")
    
    def store_semantic(self, fact_type: str, fact_value: str, importance: int, source: str):
        """Store semantic memory (fact)"""
        if not self.initialized or importance < 5:
            return
        
        try:
            text = f"{fact_type}: {fact_value}"
            embedding = self.encode(text)
            memory_id = f"sem_{fact_type}_{int(time.time())}"
            
            if self.backend_type == "supabase":
                self._store_supabase_semantic(fact_type, fact_value, embedding, importance, source)
            else:
                self.memory_store["semantic"].append({
                    "id": memory_id, "text": text, "embedding": embedding,
                    "metadata": {"fact_type": fact_type, "importance": importance, "source": source}
                })
                
        except Exception as e:
            print(f"Semantic storage error: {e}")
    
    def _store_supabase_semantic(self, fact_type: str, fact_value: str, 
                                  embedding: List[float], importance: int, source: str):
        """Store semantic memory in Supabase pgvector"""
        try:
            conn = psycopg2.connect(self.supabase_db_url)
            c = conn.cursor()
            c.execute("""
                INSERT INTO semantic_memories (fact_type, fact_value, embedding, importance, source)
                VALUES (%s, %s, %s::vector, %s, %s)
            """, (fact_type, fact_value, str(embedding), importance, source))
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Supabase semantic storage error: {e}")
    
    def recall_similar_episodes(self, query: str, n_results: int = 5) -> List[dict]:
        """Recall similar episodic memories"""
        if not self.initialized:
            return []
        
        try:
            query_embedding = self.encode(query)
            
            if self.backend_type == "supabase":
                return self._recall_supabase_episodic(query_embedding, n_results)
            else:
                return self._memory_similarity_search("episodic", query_embedding, n_results)
                
        except Exception as e:
            print(f"Recall error: {e}")
            return []
    
    def _recall_supabase_episodic(self, query_embedding: List[float], n_results: int) -> List[dict]:
        """Recall episodic memories from Supabase pgvector"""
        try:
            conn = psycopg2.connect(self.supabase_db_url)
            c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            c.execute("""
                SELECT id, session_id, memory_text, embedding <=> %s::vector as distance,
                       intent, success_score, metadata, created_at
                FROM episodic_memories
                ORDER BY embedding <=> %s::vector
                LIMIT %s
            """, (str(query_embedding), str(query_embedding), n_results))
            
            results = []
            for row in c.fetchall():
                results.append({
                    'id': row['id'],
                    'text': row['memory_text'],
                    'metadata': dict(row)
                })
            conn.close()
            return results
        except Exception as e:
            print(f"Supabase recall error: {e}")
            return []
    
    def _memory_similarity_search(self, store_type: str, query_embedding: List[float], 
                                   n_results: int) -> List[dict]:
        """In-memory cosine similarity search"""
        memories = self.memory_store.get(store_type, [])
        if not memories:
            return []
        
        similarities = []
        for mem in memories:
            sim = self._cosine_similarity(query_embedding, mem.get('embedding', [0.0]*384))
            similarities.append((sim, mem))
        
        similarities.sort(key=lambda x: x[0], reverse=True)
        return [mem for _, mem in similarities[:n_results]]
    
    def _cosine_similarity(self, a: List[float], b: List[float]) -> float:
        """Calculate cosine similarity between two vectors"""
        if len(a) != len(b):
            return 0.0
        
        dot_product = sum(x * y for x, y in zip(a, b))
        norm_a = math.sqrt(sum(x * x for x in a))
        norm_b = math.sqrt(sum(x * x for x in b))
        
        if norm_a == 0 or norm_b == 0:
            return 0.0
        return dot_product / (norm_a * norm_b)
    
    def get_status(self) -> dict:
        """Get memory system status"""
        if self.encoder == "huggingface_api":
            encoder_type = "huggingface-api (semantic, zero RAM)"
        elif self.encoder == "hash":
            encoder_type = "hash-based (lightweight)"
        elif self.encoder:
            encoder_type = "sentence-transformers (ML)"
        else:
            encoder_type = "none"
        
        status = {
            "backend": self.backend_type,
            "initialized": self.initialized,
            "encoder": encoder_type,
        }
        
        if self.backend_type == "supabase":
            status["database"] = "Supabase pgvector"
        else:
            status["episodic_count"] = len(self.memory_store["episodic"])
            status["semantic_count"] = len(self.memory_store["semantic"])
        
        return status

hybrid_memory = HybridVectorMemory()

# ============================================================
# WIKIPEDIA & WIKIDATA API
# ============================================================
class WikipediaKnowledge:
    """Free Wikipedia and Wikidata API integration"""
    
    def __init__(self):
        self.wikipedia_api = "https://en.wikipedia.org/api/rest_v1"
        self.wikidata_api = "https://www.wikidata.org/w/api.php"
        self.wikipedia_action_api = "https://en.wikipedia.org/w/api.php"
        self.cache = {}
        self.cache_time = {}
        self.cache_duration = 3600  # 1 hour cache
    
    def _get_cached(self, key: str) -> Optional[dict]:
        """Get cached result if still valid"""
        if key in self.cache and key in self.cache_time:
            if time.time() - self.cache_time[key] < self.cache_duration:
                return self.cache[key]
        return None
    
    def _set_cache(self, key: str, value: dict):
        """Cache a result"""
        self.cache[key] = value
        self.cache_time[key] = time.time()
    
    async def search_wikipedia(self, query: str, limit: int = 5) -> List[dict]:
        """Search Wikipedia for articles matching query"""
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
        """Get full Wikipedia article by title"""
        cache_key = f"article_{title}"
        cached = self._get_cached(cache_key)
        if cached:
            return cached
        
        try:
            # Get article summary and content
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
            
            # Fallback to action API
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
    
    async def get_article_html(self, title: str) -> str:
        """Get full HTML content of Wikipedia article"""
        try:
            response = requests.get(
                f"{self.wikipedia_api}/page/html/{title.replace(' ', '_')}",
                timeout=30
            )
            
            if response.status_code == 200:
                return response.text[:50000]  # Limit to 50KB
            return ""
        except Exception as e:
            return f"Error: {e}"
    
    async def search_wikidata(self, query: str, limit: int = 5) -> List[dict]:
        """Search Wikidata for entities"""
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
        """Get detailed information about a Wikidata entity"""
        try:
            response = requests.get(
                self.wikidata_api,
                params={
                    "action": "wbgetentities",
                    "ids": entity_id,
                    "languages": "en|zh",  # Request both English and Chinese labels
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
                    "claims": {}  # Simplified claims
                }
                
                # Extract key claims (properties)
                for prop_id, claims in entity.get("claims", {}).items():
                    values = []
                    for claim in claims[:3]:  # Limit to first 3 values
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
    
    async def get_company_info(self, company_name: str) -> dict:
        """Get company information from Wikipedia and Wikidata, with China focus"""
        # Search Wikipedia
        wiki_results = await self.search_wikipedia(f"{company_name} company", limit=1)
        
        result = {
            "company": company_name,
            "wikipedia": None,
            "wikidata": None,
            "is_chinese": False,
            "chinese_name": None
        }
        
        if wiki_results:
            article = await self.get_article(wiki_results[0]["title"])
            result["wikipedia"] = article
            
            # Try to find Wikidata entity
            wd_results = await self.search_wikidata(company_name, limit=1)
            if wd_results:
                entity = await self.get_wikidata_entity(wd_results[0]["id"])
                result["wikidata"] = entity
                
                # Check if the company is in China (country property P17 = Q148)
                if "P17" in entity.get("claims", {}):
                    countries = entity["claims"]["P17"]
                    if "Q148" in str(countries):
                        result["is_chinese"] = True
                
                # Try to get Chinese name
                if "labels" in entity and "zh" in entity["labels"]:
                    result["chinese_name"] = entity["labels"]["zh"].get("value")
                elif "labels" in entity and "zh-cn" in entity["labels"]:
                    result["chinese_name"] = entity["labels"]["zh-cn"].get("value")
        
        return result
    
    async def get_topic_summary(self, topic: str) -> str:
        """Get a concise summary of any topic"""
        article = await self.get_article(topic)
        
        if "error" in article:
            # Try searching
            results = await self.search_wikipedia(topic, limit=1)
            if results:
                article = await self.get_article(results[0]["title"])
        
        if "extract" in article:
            return article["extract"]
        return f"No Wikipedia article found for: {topic}"

wikipedia_knowledge = WikipediaKnowledge()

# ============================================================
# BROWSER AUTOMATION (Playwright)
# ============================================================
class BrowserAutomation:
    """Browser automation using Playwright or HTTP fallback"""
    
    def __init__(self):
        self.playwright_available = PLAYWRIGHT_AVAILABLE
        self.browser = None
        self.context = None
        self.page = None
    
    async def init_browser(self):
        """Initialize Playwright browser"""
        if not PLAYWRIGHT_AVAILABLE:
            return False
        
        try:
            playwright = await async_playwright.start()
            self.browser = await playwright.chromium.launch(headless=True)
            self.context = await self.browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            self.page = await self.context.new_page()
            print("✅ Playwright browser initialized")
            return True
        except Exception as e:
            print(f"⚠️ Browser init failed: {e}")
            return False
    
    async def close_browser(self):
        """Close browser"""
        if self.browser:
            await self.browser.close()
            self.browser = None
            self.context = None
            self.page = None
    
    async def browse_page(self, url: str, wait_time: int = 2) -> dict:
        """
        Browse to a URL and get page content.
        Uses Playwright if available, otherwise falls back to HTTP requests.
        """
        result = {
            "url": url,
            "title": "",
            "content": "",
            "html": "",
            "screenshot": None,
            "success": False,
            "method": "http"
        }
        
        # Try Playwright first
        if PLAYWRIGHT_AVAILABLE:
            try:
                if not self.page:
                    await self.init_browser()
                
                if self.page:
                    await self.page.goto(url, timeout=30000)
                    await asyncio.sleep(wait_time)
                    
                    result["title"] = await self.page.title()
                    result["content"] = await self.page.content()
                    
                    # Get visible text
                    result["text"] = await self.page.evaluate("""
                        () => document.body.innerText
                    """)
                    
                    # Take screenshot
                    screenshot_bytes = await self.page.screenshot(full_page=False)
                    result["screenshot"] = base64.b64encode(screenshot_bytes).decode('utf-8')
                    
                    result["success"] = True
                    result["method"] = "playwright"
                    return result
            except Exception as e:
                print(f"Playwright error: {e}")
        
        # HTTP fallback
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                result["html"] = response.text[:100000]  # Limit to 100KB
                result["content"] = response.text[:10000]  # Shorter content
                result["success"] = True
                result["method"] = "http"
                
                # Extract title
                title_match = re.search(r'<title>([^<]+)</title>', response.text, re.IGNORECASE)
                if title_match:
                    result["title"] = title_match.group(1)
        except Exception as e:
            result["error"] = str(e)
        
        return result
    
    async def fill_form(self, url: str, form_data: dict, submit_selector: str = None) -> dict:
        """Fill out a form on a webpage"""
        if not PLAYWRIGHT_AVAILABLE:
            return {"error": "Playwright not available. Install with: pip install playwright && playwright install"}
        
        try:
            if not self.page:
                await self.init_browser()
            
            await self.page.goto(url, timeout=30000)
            
            # Fill form fields
            for selector, value in form_data.items():
                try:
                    await self.page.fill(selector, str(value))
                except Exception as e:
                    print(f"Could not fill {selector}: {e}")
            
            # Submit if selector provided
            if submit_selector:
                await self.page.click(submit_selector)
                await asyncio.sleep(2)
            
            result = {
                "success": True,
                "url": self.page.url,
                "title": await self.page.title(),
                "content": await self.page.content()
            }
            
            return result
        except Exception as e:
            return {"error": str(e)}
    
    async def take_screenshot(self, url: str, full_page: bool = False) -> dict:
        """Take a screenshot of a webpage"""
        result = {
            "url": url,
            "screenshot": None,
            "success": False
        }
        
        if not PLAYWRIGHT_AVAILABLE:
            # Use external service as fallback
            try:
                # Use a screenshot API service
                screenshot_url = f"https://api.microlink.io/?url={url}&screenshot=true&embed=screenshot.url"
                response = requests.get(screenshot_url, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    if data.get("screenshot"):
                        result["screenshot_url"] = data["screenshot"].get("url")
                        result["success"] = True
                        result["method"] = "microlink"
            except Exception as e:
                result["error"] = str(e)
            
            return result
        
        try:
            if not self.page:
                await self.init_browser()
            
            await self.page.goto(url, timeout=30000)
            await asyncio.sleep(2)
            
            screenshot_bytes = await self.page.screenshot(full_page=full_page)
            result["screenshot"] = base64.b64encode(screenshot_bytes).decode('utf-8')
            result["success"] = True
            result["method"] = "playwright"
        except Exception as e:
            result["error"] = str(e)
        
        return result
    
    async def extract_data(self, url: str, selectors: dict) -> dict:
        """Extract specific data from a webpage using CSS selectors"""
        result = {
            "url": url,
            "data": {},
            "success": False
        }
        
        if not PLAYWRIGHT_AVAILABLE:
            # HTTP fallback with regex
            try:
                headers = {"User-Agent": "Mozilla/5.0"}
                response = requests.get(url, headers=headers, timeout=30)
                
                if response.status_code == 200:
                    html = response.text
                    for key, selector in selectors.items():
                        # Simple regex extraction
                        pattern = rf'{selector}[^>]*>([^<]+)</'
                        match = re.search(pattern, html, re.IGNORECASE)
                        if match:
                            result["data"][key] = match.group(1).strip()
                    result["success"] = True
                    result["method"] = "http"
            except Exception as e:
                result["error"] = str(e)
            
            return result
        
        try:
            if not self.page:
                await self.init_browser()
            
            await self.page.goto(url, timeout=30000)
            
            for key, selector in selectors.items():
                try:
                    element = await self.page.query_selector(selector)
                    if element:
                        result["data"][key] = await element.inner_text()
                except Exception as e:
                    result["data"][key] = f"Error: {e}"
            
            result["success"] = True
            result["method"] = "playwright"
        except Exception as e:
            result["error"] = str(e)
        
        return result
    
    async def click_and_navigate(self, url: str, click_selector: str) -> dict:
        """Click an element and wait for navigation"""
        if not PLAYWRIGHT_AVAILABLE:
            return {"error": "Playwright not available"}
        
        try:
            if not self.page:
                await self.init_browser()
            
            await self.page.goto(url, timeout=30000)
            await self.page.click(click_selector)
            await self.page.wait_for_load_state("networkidle")
            
            return {
                "success": True,
                "new_url": self.page.url,
                "title": await self.page.title(),
                "content": await self.page.content()
            }
        except Exception as e:
            return {"error": str(e)}

browser_automation = BrowserAutomation()

# ============================================================
# TOOL REGISTRY & EXECUTION PIPELINE
# ============================================================
class ToolRegistry:
    """Dynamic tool registry with execution capabilities"""
    
    def __init__(self):
        self.tools = {}
        self._register_builtin_tools()
        self._load_from_db()
    
    def _register_builtin_tools(self):
        """Register built-in tools"""
        self.tools.update({
            # Original tools
            'search_web': {
                'description': 'Search the web for information',
                'parameters': {'query': 'string'},
                'handler': self._tool_search_web
            },
            'calculate_risk_score': {
                'description': 'Calculate risk score for a company',
                'parameters': {'company_name': 'string', 'context': 'object'},
                'handler': self._tool_calculate_risk
            },
            'generate_report': {
                'description': 'Generate a detailed report',
                'parameters': {'topic': 'string', 'format': 'string'},
                'handler': self._tool_generate_report
            },
            'send_notification': {
                'description': 'Send proactive notification to user',
                'parameters': {'session_id': 'string', 'message': 'string'},
                'handler': self._tool_send_notification
            },
            'create_goal': {
                'description': 'Create an autonomous goal',
                'parameters': {'goal_type': 'string', 'description': 'string', 'priority': 'integer'},
                'handler': self._tool_create_goal
            },
            'schedule_followup': {
                'description': 'Schedule a followup action',
                'parameters': {'session_id': 'string', 'delay_hours': 'integer', 'action': 'string'},
                'handler': self._tool_schedule_followup
            },
            'analyze_sentiment': {
                'description': 'Analyze sentiment of text',
                'parameters': {'text': 'string'},
                'handler': self._tool_analyze_sentiment
            },
            'tavily_search': {
                'description': 'AI-powered web search',
                'parameters': {'query': 'string', 'search_depth': 'string'},
                'handler': self._tool_tavily_search
            },
            'jina_reader': {
                'description': 'Read any webpage as clean markdown',
                'parameters': {'url': 'string'},
                'handler': self._tool_jina_reader
            },
            'news_monitor': {
                'description': 'Monitor global news on any topic',
                'parameters': {'topic': 'string'},
                'handler': self._tool_news_monitor
            },
            'indexnow_ping': {
                'description': 'Notify search engines to index a URL',
                'parameters': {'url': 'string'},
                'handler': self._tool_indexnow_ping
            },
            'content_writer': {
                'description': 'Generate SEO-optimized content',
                'parameters': {'topic': 'string', 'content_type': 'string', 'keywords': 'array'},
                'handler': self._tool_content_writer
            },
            'duckduckgo_search': {
                'description': 'Search the web using DuckDuckGo. 100% FREE.',
                'parameters': {'query': 'string', 'max_results': 'integer'},
                'handler': self._tool_duckduckgo_search
            },
            'reddit_search': {
                'description': 'Search Reddit for discussions',
                'parameters': {'query': 'string', 'subreddit': 'string', 'limit': 'integer'},
                'handler': self._tool_reddit_search
            },
            'reddit_get_posts': {
                'description': 'Get recent posts from a subreddit',
                'parameters': {'subreddit': 'string', 'limit': 'integer', 'sort_by': 'string'},
                'handler': self._tool_reddit_get_posts
            },
            'geocode_address': {
                'description': 'Verify and geocode an address',
                'parameters': {'address': 'string'},
                'handler': self._tool_geocode_address
            },
            'zenrows_scrape': {
                'description': 'Scrape any website with anti-bot bypass',
                'parameters': {'url': 'string', 'css_extractor': 'string'},
                'handler': self._tool_zenrows_scrape
            },
            'memory_status': {
                'description': 'Get the status of the vector memory system',
                'parameters': {},
                'handler': self._tool_memory_status
            },
            'store_memory': {
                'description': 'Store a fact in vector memory',
                'parameters': {'fact_type': 'string', 'fact_value': 'string', 'importance': 'integer'},
                'handler': self._tool_store_memory
            },
            'recall_memories': {
                'description': 'Recall similar memories',
                'parameters': {'query': 'string', 'n_results': 'integer'},
                'handler': self._tool_recall_memories
            },
            
            # Wikipedia & Wikidata tools
            'wikipedia_search': {
                'description': 'Search Wikipedia encyclopedia for articles. FREE and instant knowledge!',
                'parameters': {'query': 'string', 'limit': 'integer'},
                'handler': self._tool_wikipedia_search
            },
            'wikipedia_article': {
                'description': 'Get full Wikipedia article by title. Comprehensive information on any topic.',
                'parameters': {'title': 'string'},
                'handler': self._tool_wikipedia_article
            },
            'wikipedia_summary': {
                'description': 'Get a quick summary of any topic from Wikipedia. Perfect for quick lookups.',
                'parameters': {'topic': 'string'},
                'handler': self._tool_wikipedia_summary
            },
            'wikidata_search': {
                'description': 'Search Wikidata for structured entity data. Companies, people, places, etc.',
                'parameters': {'query': 'string', 'limit': 'integer'},
                'handler': self._tool_wikidata_search
            },
            'wikidata_entity': {
                'description': 'Get detailed structured data about a Wikidata entity by ID.',
                'parameters': {'entity_id': 'string'},
                'handler': self._tool_wikidata_entity
            },
            'company_info': {
                'description': 'Get comprehensive company information from Wikipedia and Wikidata, with China focus.',
                'parameters': {'company_name': 'string'},
                'handler': self._tool_company_info
            },
            
            # Browser automation tools
            'browse_page': {
                'description': 'Browse to a URL and get page content. Can take screenshots!',
                'parameters': {'url': 'string', 'wait_time': 'integer'},
                'handler': self._tool_browse_page
            },
            'screenshot_page': {
                'description': 'Take a screenshot of any webpage. Great for visual verification.',
                'parameters': {'url': 'string', 'full_page': 'boolean'},
                'handler': self._tool_screenshot_page
            },
            'extract_web_data': {
                'description': 'Extract specific data from a webpage using CSS selectors.',
                'parameters': {'url': 'string', 'selectors': 'object'},
                'handler': self._tool_extract_web_data
            },
            'fill_web_form': {
                'description': 'Fill out and submit a web form. Useful for automating submissions.',
                'parameters': {'url': 'string', 'form_data': 'object', 'submit_selector': 'string'},
                'handler': self._tool_fill_web_form
            },
            'research_topic': {
                'description': 'Deep research on any topic: combines Wikipedia, Wikidata, and web search.',
                'parameters': {'topic': 'string', 'depth': 'string'},
                'handler': self._tool_research_topic
            },

            # ============================================================
            # NEW v10.2: China Business News tool
            # ============================================================
            'china_business_news': {
                'description': 'Get the latest news about China business, suppliers, and economy – 100% free via Google News RSS.',
                'parameters': {},
                'handler': self._tool_china_business_news
            },

            # ============================================================
            # NEW v10.3: Chinese Translation, RSS Aggregator, Economic Indicators
            # ============================================================
            'translate_chinese': {
                'description': 'Translate text between English and Chinese using free LibreTranslate API.',
                'parameters': {'text': 'string', 'target_language': 'string'},  # 'en' or 'zh'
                'handler': self._tool_translate_chinese
            },
            'china_rss_news': {
                'description': 'Fetch the latest China business headlines from multiple RSS feeds (China Briefing, China Daily, SCMP).',
                'parameters': {'limit': 'integer'},
                'handler': self._tool_china_rss_news
            },
            'china_economic_indicator': {
                'description': 'Get current economic indicators for China (GDP, PMI, trade) from World Bank open data.',
                'parameters': {'indicator': 'string'},  # 'gdp', 'pmi', 'trade'
                'handler': self._tool_china_economic_indicator
            },

            # ============================================================
            # NEW v10.5: Serper Google Search, Currency, Countries, Wayback, HackerNews
            # ============================================================
            'serper_search': {
                'description': 'Google Search via Serper.dev — best quality web results. Use for company research, news, any topic.',
                'parameters': {'query': 'string', 'num': 'integer'},
                'handler': self._tool_serper_search
            },
            'currency_rates': {
                'description': 'Get live currency exchange rates, especially USD/CNY. Free, no key needed.',
                'parameters': {'base': 'string', 'target': 'string'},
                'handler': self._tool_currency_rates
            },
            'country_info': {
                'description': 'Get country information: trade data, languages, currencies, region. Free, no key needed.',
                'parameters': {'country': 'string'},
                'handler': self._tool_country_info
            },
            'wayback_machine': {
                'description': 'Retrieve cached/archived version of any URL via Wayback Machine. Useful when sites are blocked.',
                'parameters': {'url': 'string'},
                'handler': self._tool_wayback_machine
            },
            'hackernews_search': {
                'description': 'Search HackerNews for tech and business discussions. Great for Chinese tech company news.',
                'parameters': {'query': 'string', 'limit': 'integer'},
                'handler': self._tool_hackernews_search
            },

            # ============================================================
            # v10.6 tools (re-added in v10.7.2)
            # ============================================================
            'opencorporates_search': {
                'description': 'Search OpenCorporates company registry. Verify real registered company data for supplier due diligence. Free, no key.',
                'parameters': {'company_name': 'string', 'jurisdiction': 'string'},
                'handler': self._tool_opencorporates_search
            },
            'un_comtrade': {
                'description': 'Get China import/export trade statistics by product from UN Comtrade. Free, no key.',
                'parameters': {'product': 'string', 'flow': 'string'},
                'handler': self._tool_un_comtrade
            },
            'ip_geolocation': {
                'description': 'Detect user country/region from IP address to tailor China business advice. Free, no key.',
                'parameters': {'ip': 'string'},
                'handler': self._tool_ip_geolocation
            },
            'get_user_profile': {
                'description': 'Look up what Sophia knows about the current user: name, company, interests, visit count.',
                'parameters': {'session_id': 'string'},
                'handler': self._tool_get_user_profile
            },
        })
    
    def _load_from_db(self):
        """Load custom tools from database"""
        try:
            conn = get_db()
            c = conn.cursor()
            try:
                c.execute("SELECT tool_name, description, parameters, implementation FROM tool_registry WHERE deployed = TRUE")
                for name, desc, params, impl in c.fetchall():
                    self.tools[name] = {
                        'description': desc,
                        'parameters': params or {},
                        'implementation': impl
                    }
            except:
                pass
            release_db(conn)
            print(f"🔧 Loaded {len(self.tools)} tools")
        except Exception as e:
            print(f"Tool load error: {e}")
    
    async def execute(self, tool_name: str, params: dict) -> dict:
        """Execute a tool with one automatic retry on failure."""
        if tool_name not in self.tools:
            return {'success': False, 'error': f"Tool '{tool_name}' not found"}

        tool = self.tools[tool_name]

        async def _run(p):
            if 'handler' in tool:
                return await tool['handler'](p)
            return self._execute_custom(tool, p)

        try:
            result = await _run(params)
            # Truncate long outputs to keep context window lean
            if isinstance(result, str):
                result = _truncate_tool_output(result)
            return {'success': True, 'result': result}
        except Exception as first_err:
            # Auto-retry: rephrase the primary query param once
            retry_params = dict(params)
            primary_key = next(iter(params), None)
            if primary_key and isinstance(params.get(primary_key), str):
                original = params[primary_key]
                retry_params[primary_key] = original.replace('"', '').strip() + ' overview'
                try:
                    result = await _run(retry_params)
                    if isinstance(result, str):
                        result = _truncate_tool_output(result)
                    print(f"🔁 Tool '{tool_name}' succeeded on retry")
                    return {'success': True, 'result': result, 'retried': True}
                except Exception as second_err:
                    return {'success': False, 'error': f"{first_err} | retry also failed: {second_err}"}
            return {'success': False, 'error': str(first_err)}
    
    def _execute_custom(self, tool: dict, params: dict) -> str:
        """Execute custom tool implementation"""
        impl = tool.get('implementation', '')
        locals_dict = {'params': params, 'result': ''}
        exec(impl, {}, locals_dict)
        return locals_dict.get('result', 'Executed')
    
    def get_tools_schema(self) -> List[dict]:
        """Get OpenAI-style tools schema."""
        # Parameters that are always optional regardless of position
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
                    if 'integer' in ptype.lower() or 'int' in ptype.lower():
                        json_type = 'integer'
                    elif 'float' in ptype.lower() or 'number' in ptype.lower():
                        json_type = 'number'
                    elif 'array' in ptype.lower() or 'list' in ptype.lower():
                        json_type = 'array'
                    elif 'object' in ptype.lower() or 'dict' in ptype.lower():
                        json_type = 'object'
                    elif 'boolean' in ptype.lower() or 'bool' in ptype.lower():
                        json_type = 'boolean'
                    properties[pname] = {"type": json_type}
                    # Mark required only if it's the primary param and not in optional set
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
    
    # ============================================================
    # TOOL HANDLERS
    # ============================================================
    
    async def _tool_search_web(self, params: dict) -> str:
        """Fallback web search"""
        query = params.get('query', '')
        return await self._tool_duckduckgo_search({'query': query})
    
    async def _tool_calculate_risk(self, params: dict) -> str:
        """Calculate risk score"""
        company = params.get('company_name', 'Unknown')
        context = params.get('context', {})
        risk = 50
        
        if context.get('years_in_business', 0) > 5:
            risk -= 10
        if context.get('verified'):
            risk -= 15
        if context.get('complaints', 0) > 0:
            risk += context['complaints'] * 5
        
        return f"Risk score for {company}: {max(0, min(100, risk))}/100"
    
    async def _tool_generate_report(self, params: dict) -> str:
        """Generate a report"""
        topic = params.get('topic', 'General')
        
        messages = [
            {"role": "system", "content": "You are a business report generator."},
            {"role": "user", "content": f"Generate a brief report on: {topic}"}
        ]
        
        result = await ai_provider.chat_completion(messages, max_tokens=500)
        return result['choices'][0]['message']['content']
    
    async def _tool_send_notification(self, params: dict) -> str:
        """Send notification"""
        session_id = params.get('session_id', '')
        message = params.get('message', '')
        
        try:
            conn = get_db()
            c = conn.cursor()
            c.execute("""
                INSERT INTO proactive_notifications (session_id, notification_type, subject, content)
                VALUES (%s, 'proactive', 'Sophia Update', %s)
            """, (session_id, message))
            conn.commit()
            release_db(conn)
            return f"Notification queued for session {session_id}"
        except Exception as e:
            return f"Failed to send notification: {e}"
    
    async def _tool_create_goal(self, params: dict) -> str:
        """Create an autonomous goal"""
        goal_type = params.get('goal_type', 'general')
        description = params.get('description', '')
        priority = params.get('priority', 5)
        
        try:
            conn = get_db()
            c = conn.cursor()
            c.execute("""
                INSERT INTO autonomous_goals (goal_type, goal_description, priority, source)
                VALUES (%s, %s, %s, 'tool')
                RETURNING id
            """, (goal_type, description, priority))
            goal_id = c.fetchone()[0]
            conn.commit()
            release_db(conn)
            return f"Created goal #{goal_id}: {description}"
        except Exception as e:
            return f"Failed to create goal: {e}"
    
    async def _tool_schedule_followup(self, params: dict) -> str:
        """Schedule a follow-up"""
        session_id = params.get('session_id', '')
        delay_hours = params.get('delay_hours', 24)
        action = params.get('action', '')
        
        return f"Scheduled follow-up for session {session_id} in {delay_hours} hours: {action}"
    
    async def _tool_analyze_sentiment(self, params: dict) -> str:
        """Analyze sentiment"""
        text = params.get('text', '')
        
        positive_words = ['good', 'great', 'excellent', 'happy', 'love', 'best', 'amazing']
        negative_words = ['bad', 'terrible', 'awful', 'hate', 'worst', 'poor', 'scam']
        
        text_lower = text.lower()
        pos_count = sum(1 for w in positive_words if w in text_lower)
        neg_count = sum(1 for w in negative_words if w in text_lower)
        
        if pos_count > neg_count:
            sentiment = "Positive"
        elif neg_count > pos_count:
            sentiment = "Negative"
        else:
            sentiment = "Neutral"
        
        return f"Sentiment: {sentiment} (positive: {pos_count}, negative: {neg_count})"
    
    async def _tool_tavily_search(self, params: dict) -> str:
        """Tavily search"""
        query = params.get('query', '')
        
        if not TAVILY_API_KEY:
            return await self._tool_duckduckgo_search({'query': query})
        
        try:
            response = requests.post(
                "https://api.tavily.com/search",
                headers={"Authorization": f"Bearer {TAVILY_API_KEY}"},
                json={"query": query, "search_depth": "basic"},
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                results = []
                for r in data.get('results', [])[:5]:
                    results.append(f"- {r.get('title', 'No title')}: {r.get('url', '')}")
                return f"Tavily results for '{query}':\n" + "\n".join(results)
            return f"Tavily error: {response.status_code}"
        except Exception as e:
            return f"Tavily search failed: {e}"
    
    async def _tool_jina_reader(self, params: dict) -> str:
        """Jina reader"""
        url = params.get('url', '')
        
        try:
            response = requests.get(
                f"https://r.jina.ai/{url}",
                headers={"User-Agent": "SophiaAI/1.0"},
                timeout=30
            )
            
            if response.status_code == 200:
                return response.text[:3000]
            return f"Jina Reader error: {response.status_code}"
        except Exception as e:
            return f"Failed to read webpage: {e}"
    
    async def _tool_news_monitor(self, params: dict) -> str:
        """Monitor global news on any topic – enhanced with Google News RSS fallback"""
        topic = params.get('topic', 'China business')
        
        # Try NewsAPI first if key is available
        if NEWS_API_KEY:
            try:
                response = requests.get(
                    "https://newsapi.org/v2/everything",
                    params={"q": topic, "apiKey": NEWS_API_KEY, "pageSize": 5, "sortBy": "publishedAt"},
                    timeout=15
                )
                
                if response.status_code == 200:
                    data = response.json()
                    articles = data.get('articles', [])
                    if articles:
                        results = []
                        for a in articles:
                            published = a.get('publishedAt', '')[:10]  # YYYY-MM-DD
                            results.append(f"- {a.get('title', 'No title')} ({a.get('source', {}).get('name', 'Unknown')}) - {published}")
                        return f"📰 Latest news for '{topic}':\n" + "\n".join(results)
            except Exception as e:
                print(f"NewsAPI error: {e}")
        
        # Fallback to Google News RSS (100% free, real-time)
        if FEEDPARSER_AVAILABLE:
            try:
                import urllib.parse
                query_encoded = urllib.parse.quote(topic)
                feed_url = f"https://news.google.com/rss/search?q={query_encoded}&hl=en-US&gl=US&ceid=US:en"
                feed = feedparser.parse(feed_url)
                
                if feed.entries and len(feed.entries) > 0:
                    results = []
                    for entry in feed.entries[:5]:
                        published = entry.get('published', '')
                        if hasattr(entry, 'published_parsed') and entry.published_parsed:
                            published_dt = datetime(*entry.published_parsed[:6])
                            published = published_dt.strftime('%Y-%m-%d')
                        title = entry.get('title', 'No title')
                        source = entry.get('source', {}).get('title', 'Google News')
                        link = entry.get('link', '')
                        results.append(f"- {title} ({source}) - {published}\n  {link}")
                    
                    return f"📰 Real-time news for '{topic}' (via Google News):\n" + "\n".join(results)
            except Exception as e:
                print(f"Google News RSS error: {e}")
        
        # Ultimate fallback: DuckDuckGo
        return await self._tool_duckduckgo_search({'query': f'{topic} news'})
    
    async def _tool_indexnow_ping(self, params: dict) -> str:
        """IndexNow ping"""
        url = params.get('url', '')
        
        if not INDEXNOW_KEY:
            return "IndexNow key not configured"
        
        try:
            response = requests.get(
                f"https://www.bing.com/indexnow",
                params={"url": url, "key": INDEXNOW_KEY},
                timeout=10
            )
            
            if response.status_code == 200:
                return f"✅ URL submitted to search engines: {url}"
            return f"IndexNow response: {response.status_code}"
        except Exception as e:
            return f"IndexNow failed: {e}"
    
    async def _tool_content_writer(self, params: dict) -> str:
        """Content writer"""
        topic = params.get('topic', '')
        keywords = params.get('keywords', [])
        
        prompt = f"""Write a blog post about "{topic}" promoting China West Connector (CWC).
CWC helps businesses connect with reliable Chinese suppliers.

Keywords: {', '.join(keywords) if keywords else 'China sourcing, supplier verification'}

Keep it professional and 300-500 words."""
        
        messages = [
            {"role": "system", "content": "You are a professional content writer."},
            {"role": "user", "content": prompt}
        ]
        
        result = await ai_provider.chat_completion(messages, max_tokens=800)
        return result['choices'][0]['message']['content']
    
    async def _tool_duckduckgo_search(self, params: dict) -> str:
        """DuckDuckGo search"""
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
                
                if results:
                    return f"DuckDuckGo results for '{query}':\n" + "\n".join(results)
        except Exception as e:
            return f"Search failed: {e}"
        
        return f"No results found for: {query}"
    
    async def _tool_reddit_search(self, params: dict) -> str:
        """Reddit search"""
        query = params.get('query', '')
        subreddit = params.get('subreddit', 'all')
        limit = params.get('limit', 5)
        
        try:
            url = f"https://www.reddit.com/r/{subreddit}/search.json"
            response = requests.get(
                url,
                params={"q": query, "restrict_sr": 1 if subreddit != 'all' else 0, "limit": limit},
                headers={"User-Agent": REDDIT_USER_AGENT},
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                results = []
                for post in data['data']['children']:
                    p = post['data']
                    results.append(f"- [{p.get('score', 0)}↑] {p.get('title', 'No title')} (r/{p.get('subreddit', 'unknown')})")
                return f"Reddit results for '{query}':\n" + "\n".join(results)
            return f"Reddit error: {response.status_code}"
        except Exception as e:
            return f"Reddit search failed: {e}"
    
    async def _tool_reddit_get_posts(self, params: dict) -> str:
        """Get Reddit posts"""
        subreddit = params.get('subreddit', 'ChinaSourcing')
        limit = params.get('limit', 5)
        sort_by = params.get('sort_by', 'hot')
        
        try:
            url = f"https://www.reddit.com/r/{subreddit}/{sort_by}.json"
            response = requests.get(
                url,
                params={"limit": limit},
                headers={"User-Agent": REDDIT_USER_AGENT},
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                results = []
                for post in data['data']['children']:
                    p = post['data']
                    results.append(f"- [{p.get('score', 0)}↑] {p.get('title', 'No title')}")
                return f"r/{subreddit} posts:\n" + "\n".join(results)
            return f"Reddit error: {response.status_code}"
        except Exception as e:
            return f"Reddit get posts failed: {e}"
    
    async def _tool_geocode_address(self, params: dict) -> str:
        """Geocode address"""
        address = params.get('address', '')
        
        try:
            response = requests.get(
                "https://nominatim.openstreetmap.org/search",
                params={"q": address, "format": "json", "limit": 1},
                headers={"User-Agent": "SophiaAI/1.0"},
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                if data:
                    r = data[0]
                    return f"Address verified: {r.get('display_name', 'Unknown')}\nCoordinates: {r.get('lat', '')}, {r.get('lon', '')}"
                return f"Address not found: {address}"
            return f"Geocoding error: {response.status_code}"
        except Exception as e:
            return f"Geocoding failed: {e}"
    
    async def _tool_zenrows_scrape(self, params: dict) -> str:
        """ZenRows scrape"""
        url = params.get('url', '')
        
        if ZENROWS_API_KEY:
            try:
                response = requests.get(
                    "https://api.zenrows.com/v1/",
                    params={"url": url, "apikey": ZENROWS_API_KEY},
                    timeout=30
                )
                if response.status_code == 200:
                    return response.text[:3000]
            except:
                pass
        
        return await self._tool_jina_reader({'url': url})
    
    async def _tool_memory_status(self, params: dict) -> str:
        """Memory status"""
        status = hybrid_memory.get_status()
        return f"🧠 Vector Memory Status:\n" + "\n".join(f"- {k}: {v}" for k, v in status.items())
    
    async def _tool_store_memory(self, params: dict) -> str:
        """Store memory"""
        fact_type = params.get('fact_type', 'general')
        fact_value = params.get('fact_value', '')
        importance = params.get('importance', 5)
        
        hybrid_memory.store_semantic(fact_type, fact_value, importance, 'user')
        return f"✅ Memory stored: {fact_type} = {fact_value}"
    
    async def _tool_recall_memories(self, params: dict) -> str:
        """Recall memories"""
        query = params.get('query', '')
        n_results = params.get('n_results', 5)
        
        episodes = hybrid_memory.recall_similar_episodes(query, n_results)
        
        result = f"🧠 Memories for '{query}':\n"
        for ep in episodes:
            result += f"- {ep['text'][:200]}...\n"
        
        return result or "No similar memories found."
    
    # Wikipedia & Wikidata tool handlers
    async def _tool_wikipedia_search(self, params: dict) -> str:
        """Search Wikipedia"""
        query = params.get('query', '')
        limit = params.get('limit', 5)
        
        results = await wikipedia_knowledge.search_wikipedia(query, limit)
        
        if not results:
            return f"No Wikipedia articles found for: {query}"
        
        output = f"📚 Wikipedia search results for '{query}':\n\n"
        for i, r in enumerate(results, 1):
            output += f"{i}. **{r['title']}**\n"
            output += f"   {r['snippet'][:150]}...\n"
            output += f"   🔗 {r['url']}\n\n"
        
        return output
    
    async def _tool_wikipedia_article(self, params: dict) -> str:
        """Get Wikipedia article"""
        title = params.get('title', '')
        
        article = await wikipedia_knowledge.get_article(title)
        
        if 'error' in article:
            return article['error']
        
        output = f"📚 **{article.get('title', title)}**\n\n"
        
        if article.get('description'):
            output += f"*{article['description']}*\n\n"
        
        if article.get('extract'):
            output += article['extract'][:2000]
        
        if article.get('url'):
            output += f"\n\n🔗 Read more: {article['url']}"
        
        return output
    
    async def _tool_wikipedia_summary(self, params: dict) -> str:
        """Get topic summary"""
        topic = params.get('topic', '')
        
        summary = await wikipedia_knowledge.get_topic_summary(topic)
        
        return f"📚 **{topic}**\n\n{summary}"
    
    async def _tool_wikidata_search(self, params: dict) -> str:
        """Search Wikidata"""
        query = params.get('query', '')
        limit = params.get('limit', 5)
        
        results = await wikipedia_knowledge.search_wikidata(query, limit)
        
        if not results:
            return f"No Wikidata entities found for: {query}"
        
        output = f"📊 Wikidata entities for '{query}':\n\n"
        for i, r in enumerate(results, 1):
            output += f"{i}. **{r['label']}** ({r['id']})\n"
            if r.get('description'):
                output += f"   {r['description']}\n"
            output += f"   🔗 {r['url']}\n\n"
        
        return output
    
    async def _tool_wikidata_entity(self, params: dict) -> str:
        """Get Wikidata entity"""
        entity_id = params.get('entity_id', '')
        
        entity = await wikipedia_knowledge.get_wikidata_entity(entity_id)
        
        if 'error' in entity:
            return entity['error']
        
        output = f"📊 **Wikidata Entity: {entity_id}**\n\n"
        
        if entity.get('labels', {}).get('en'):
            output += f"**Label:** {entity['labels']['en'].get('value', 'N/A')}\n\n"
        
        if entity.get('descriptions', {}).get('en'):
            output += f"**Description:** {entity['descriptions']['en'].get('value', 'N/A')}\n\n"
        
        if entity.get('claims'):
            output += "**Properties:**\n"
            for prop, values in list(entity['claims'].items())[:10]:
                output += f"- {prop}: {', '.join(str(v) for v in values[:3])}\n"
        
        return output
    
    async def _tool_company_info(self, params: dict) -> str:
        """Get company info with China focus"""
        company_name = params.get('company_name', '')
        
        info = await wikipedia_knowledge.get_company_info(company_name)
        
        output = f"🏢 **{company_name}** - Company Information\n\n"
        
        if info.get('wikipedia'):
            wp = info['wikipedia']
            output += "**Wikipedia:**\n"
            if wp.get('extract'):
                output += f"{wp['extract'][:1000]}\n\n"
            if wp.get('url'):
                output += f"🔗 {wp['url']}\n\n"
        
        if info.get('wikidata'):
            wd = info['wikidata']
            output += "**Wikidata:**\n"
            if wd.get('labels', {}).get('en'):
                output += f"Label: {wd['labels']['en'].get('value', 'N/A')}\n"
            if wd.get('descriptions', {}).get('en'):
                output += f"Description: {wd['descriptions']['en'].get('value', 'N/A')}\n"
        
        # China-specific additions
        if info.get('is_chinese'):
            output += "\n🇨🇳 **This is a Chinese company.**\n"
        if info.get('chinese_name'):
            output += f"Chinese name: {info['chinese_name']}\n"
        
        return output
    
    # Browser automation tool handlers
    async def _tool_browse_page(self, params: dict) -> str:
        """Browse a webpage"""
        url = params.get('url', '')
        wait_time = params.get('wait_time', 2)
        
        if not url.startswith('http'):
            url = 'https://' + url
        
        result = await browser_automation.browse_page(url, wait_time)
        
        if not result.get('success'):
            return f"❌ Failed to browse {url}: {result.get('error', 'Unknown error')}"
        
        output = f"🌐 **Browsed:** {url}\n"
        output += f"**Method:** {result['method']}\n"
        
        if result.get('title'):
            output += f"**Title:** {result['title']}\n\n"
        
        if result.get('text'):
            output += f"**Content Preview:**\n{result['text'][:2000]}"
        elif result.get('content'):
            # Strip HTML tags for display
            text = re.sub(r'<[^>]+>', '', result['content'][:2000])
            output += f"**Content Preview:**\n{text}"
        
        if result.get('screenshot'):
            output += f"\n\n📸 Screenshot available (base64, {len(result['screenshot'])} chars)"
        
        return output
    
    async def _tool_screenshot_page(self, params: dict) -> str:
        """Take screenshot"""
        url = params.get('url', '')
        full_page = params.get('full_page', False)
        
        if not url.startswith('http'):
            url = 'https://' + url
        
        result = await browser_automation.take_screenshot(url, full_page)
        
        if not result.get('success'):
            return f"❌ Failed to screenshot {url}: {result.get('error', 'Unknown error')}"
        
        output = f"📸 **Screenshot taken:** {url}\n"
        output += f"**Method:** {result['method']}\n"
        
        if result.get('screenshot'):
            output += f"**Screenshot:** Base64 encoded image ({len(result['screenshot'])} characters)\n"
            output += f"*Use the screenshot data to display or save the image.*"
        elif result.get('screenshot_url'):
            output += f"**Screenshot URL:** {result['screenshot_url']}"
        
        return output
    
    async def _tool_extract_web_data(self, params: dict) -> str:
        """Extract web data"""
        url = params.get('url', '')
        selectors = params.get('selectors', {})
        
        if not url.startswith('http'):
            url = 'https://' + url
        
        result = await browser_automation.extract_data(url, selectors)
        
        if not result.get('success'):
            return f"❌ Failed to extract data from {url}: {result.get('error', 'Unknown error')}"
        
        output = f"🔍 **Extracted data from:** {url}\n"
        output += f"**Method:** {result['method']}\n\n"
        
        if result.get('data'):
            output += "**Extracted Values:**\n"
            for key, value in result['data'].items():
                output += f"- {key}: {value}\n"
        
        return output
    
    async def _tool_fill_web_form(self, params: dict) -> str:
        """Fill web form"""
        url = params.get('url', '')
        form_data = params.get('form_data', {})
        submit_selector = params.get('submit_selector')
        
        if not url.startswith('http'):
            url = 'https://' + url
        
        result = await browser_automation.fill_form(url, form_data, submit_selector)
        
        if 'error' in result:
            return f"❌ Failed to fill form: {result['error']}"
        
        output = f"📝 **Form filled successfully:** {url}\n\n"
        output += f"**Fields filled:** {len(form_data)}\n"
        
        if result.get('title'):
            output += f"**Result page:** {result['title']}\n"
        
        if result.get('url'):
            output += f"**Final URL:** {result['url']}"
        
        return output
    
    async def _tool_research_topic(self, params: dict) -> str:
        """Deep research on a topic"""
        topic = params.get('topic', '')
        depth = params.get('depth', 'standard')  # quick, standard, deep
        
        output = f"🔬 **Research Report: {topic}**\n\n"
        
        # Step 1: Wikipedia
        wiki_article = await wikipedia_knowledge.get_article(topic)
        if 'extract' in wiki_article:
            output += f"📚 **Wikipedia Summary:**\n{wiki_article['extract'][:1500]}\n\n"
        
        # Step 2: Wikidata
        wd_results = await wikipedia_knowledge.search_wikidata(topic, limit=1)
        if wd_results:
            entity = await wikipedia_knowledge.get_wikidata_entity(wd_results[0]['id'])
            if entity.get('descriptions', {}).get('en'):
                output += f"📊 **Wikidata:** {entity['descriptions']['en'].get('value', '')}\n\n"
        
        # Step 3: Web search for more
        if depth in ['standard', 'deep']:
            web_results = await self._tool_duckduckgo_search({'query': topic})
            output += f"🌐 **Web Results:**\n{web_results[:1000]}\n\n"
        
        # Step 4: News (if deep)
        if depth == 'deep':
            news = await self._tool_news_monitor({'topic': topic})
            output += f"📰 **Recent News:**\n{news[:500]}\n"
        
        return output

    # ============================================================
    # v10.2: China Business News tool handler
    # ============================================================
    async def _tool_china_business_news(self, params: dict) -> str:
        """Get the latest China business news via Google News RSS (100% free)."""
        if not FEEDPARSER_AVAILABLE:
            return "⚠️ Feedparser not installed. Install with: pip install feedparser"
        
        import urllib.parse
        query = "China business OR Chinese suppliers OR China economy"
        query_encoded = urllib.parse.quote(query)
        feed_url = f"https://news.google.com/rss/search?q={query_encoded}&hl=en-US&gl=US&ceid=US:en"
        
        try:
            feed = feedparser.parse(feed_url)
            if not feed.entries:
                return "No recent China business news found."
            
            results = []
            for entry in feed.entries[:5]:
                published = entry.get('published', '')
                if hasattr(entry, 'published_parsed') and entry.published_parsed:
                    published_dt = datetime(*entry.published_parsed[:6])
                    published = published_dt.strftime('%Y-%m-%d')
                title = entry.get('title', 'No title')
                source = entry.get('source', {}).get('title', 'Google News')
                link = entry.get('link', '')
                results.append(f"- {title} ({source}) - {published}\n  {link}")
            
            return "🇨🇳 **China Business News** (latest):\n" + "\n".join(results)
        except Exception as e:
            return f"Error fetching China news: {e}"

    # ============================================================
    # v10.3: Chinese Translation (LibreTranslate public demo)
    # ============================================================
    async def _tool_translate_chinese(self, params: dict) -> str:
        """Translate text between English and Chinese using LibreTranslate."""
        text = params.get('text', '')
        target = params.get('target_language', 'en')  # 'en' or 'zh'
        
        if not text:
            return "Please provide text to translate."
        
        # Determine source language automatically (leave empty for auto-detect)
        source = 'en' if target == 'zh' else 'zh'
        
        url = "https://libretranslate.de/translate"
        payload = {
            'q': text,
            'source': 'auto',  # auto-detect source
            'target': target,
            'format': 'text'
        }
        
        try:
            response = requests.post(url, data=payload, timeout=10)
            if response.status_code == 200:
                result = response.json()
                translated = result.get('translatedText', '')
                return f"🔄 **Translated ({'auto' if source=='auto' else source} → {target}):**\n{translated}"
            else:
                return f"Translation failed (status {response.status_code}). Please try again later."
        except Exception as e:
            return f"Translation error: {e}"

    # ============================================================
    # v10.3: China RSS News Aggregator
    # ============================================================
    async def _tool_china_rss_news(self, params: dict) -> str:
        """Fetch China business news from multiple RSS feeds."""
        if not FEEDPARSER_AVAILABLE:
            return "⚠️ Feedparser not installed. Install with: pip install feedparser"
        
        limit = params.get('limit', 5)
        
        feeds = [
            ("China Briefing", "https://www.china-briefing.com/news/feed/"),
            ("China Daily Business", "http://www.chinadaily.com.cn/business/rss.xml"),
            ("SCMP Business", "https://www.scmp.com/rss/4/feed"),  # Business section
            ("Caixin", "https://www.caixinglobal.com/rss/top_news.xml"),  # May need update
        ]
        
        all_entries = []
        for name, url in feeds:
            try:
                feed = feedparser.parse(url)
                for entry in feed.entries[:3]:  # Take top 3 per feed
                    published = entry.get('published', '')
                    if hasattr(entry, 'published_parsed') and entry.published_parsed:
                        published_dt = datetime(*entry.published_parsed[:6])
                        published = published_dt.strftime('%Y-%m-%d')
                    title = entry.get('title', 'No title')
                    link = entry.get('link', '')
                    all_entries.append((published, f"- {title} ({published})\n  {link}"))
            except Exception as e:
                print(f"RSS feed error for {name}: {e}")
        
        # Sort by date (descending) – naive sort, may need parsing
        all_entries.sort(reverse=True)
        
        if not all_entries:
            return "No news entries found from RSS feeds."
        
        results = []
        for _, item in all_entries[:limit]:
            results.append(item)
        
        return "🇨🇳 **China Business RSS News** (latest):\n" + "\n".join(results)

    # ============================================================
    # v10.3: China Economic Indicators (World Bank)
    # ============================================================
    async def _tool_china_economic_indicator(self, params: dict) -> str:
        """Fetch economic indicators for China from World Bank open data."""
        indicator_map = {
            'gdp': 'NY.GDP.MKTP.CD',          # GDP (current US$)
            'gdp_growth': 'NY.GDP.MKTP.KD.ZG', # GDP growth (annual %)
            'pmi': None,  # Not available from World Bank, fallback to Trading Economics? We'll skip for now
            'trade': 'NE.EXP.GNFS.CD',          # Exports of goods and services (current US$)
            'imports': 'NE.IMP.GNFS.CD',        # Imports
            'inflation': 'FP.CPI.TOTL.ZG',      # Inflation, consumer prices (annual %)
        }
        
        indicator = params.get('indicator', 'gdp').lower()
        if indicator not in indicator_map:
            return f"Unknown indicator. Available: {', '.join(indicator_map.keys())}"
        
        wb_code = indicator_map[indicator]
        if wb_code is None:
            return f"Indicator '{indicator}' not available from World Bank."
        
        url = f"http://api.worldbank.org/v2/country/CN/indicator/{wb_code}?format=json&per_page=1&date=2022:2024"
        try:
            response = requests.get(url, timeout=15)
            if response.status_code == 200:
                data = response.json()
                if len(data) > 1 and data[1]:
                    latest = data[1][0]
                    value = latest.get('value')
                    date = latest.get('date')
                    if value is not None:
                        return f"🇨🇳 **China {indicator.upper()}** ({date}): {value:,.2f} (current US$ where applicable)"
                    else:
                        return f"No recent data for {indicator}."
                else:
                    return f"No data found for {indicator}."
            else:
                return f"World Bank API error: {response.status_code}"
        except Exception as e:
            return f"Error fetching economic indicator: {e}"

    # ============================================================
    # v10.5: Serper.dev — Google Search (100/day free)
    # ============================================================
    async def _tool_serper_search(self, params: dict) -> str:
        """Google Search via Serper.dev — quota-tracked, falls back to DuckDuckGo."""
        global _serper_usage
        query = params.get('query', '')
        num = params.get('num', 5)

        # Check daily quota
        today = datetime.now().strftime('%Y-%m-%d')
        daily_count = _serper_usage.get(today, 0)

        if not SERPER_API_KEY or daily_count >= SERPER_DAILY_LIMIT:
            if daily_count >= SERPER_DAILY_LIMIT:
                print(f"⚠️ Serper quota reached ({daily_count}/{SERPER_DAILY_LIMIT}) — DuckDuckGo fallback")
            return await self._tool_duckduckgo_search({'query': query})

        try:
            response = requests.post(
                "https://google.serper.dev/search",
                headers={"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"},
                json={"q": query, "num": num},
                timeout=15
            )
            if response.status_code == 200:
                _serper_usage[today] = daily_count + 1
                data = response.json()
                results = []
                # Organic results
                for r in data.get('organic', [])[:num]:
                    results.append(f"- **{r.get('title', '')}**\n  {r.get('snippet', '')}\n  🔗 {r.get('link', '')}")
                # Knowledge graph if available
                kg = data.get('knowledgeGraph', {})
                if kg.get('description'):
                    results.insert(0, f"📌 **{kg.get('title', '')}**: {kg.get('description', '')}")
                if results:
                    return f"🔍 Google results for '{query}':\n\n" + "\n\n".join(results)
                return f"No results found for: {query}"
            return await self._tool_duckduckgo_search({'query': query})
        except Exception as e:
            return await self._tool_duckduckgo_search({'query': query})

    # ============================================================
    # v10.5: Live Currency Rates (Open Exchange Rates — free tier)
    # ============================================================
    async def _tool_currency_rates(self, params: dict) -> str:
        """Get live currency exchange rates. Especially useful for USD/CNY."""
        base = params.get('base', 'USD').upper()
        target = params.get('target', 'CNY').upper()

        try:
            # Use frankfurter.app — completely free, no key needed
            response = requests.get(
                f"https://api.frankfurter.app/latest?from={base}&to={target}",
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                rate = data.get('rates', {}).get(target)
                date = data.get('date', 'unknown')
                if rate:
                    return f"💱 **{base} → {target}**: {rate} (as of {date})"
                return f"Rate not found for {base}/{target}"
            return f"Currency API error: {response.status_code}"
        except Exception as e:
            return f"Currency lookup failed: {e}"

    # ============================================================
    # v10.5: REST Countries — free, no key needed
    # ============================================================
    async def _tool_country_info(self, params: dict) -> str:
        """Get country info: trade data, languages, currencies, region."""
        country = params.get('country', '')
        if not country:
            return "Please provide a country name."

        try:
            response = requests.get(
                f"https://restcountries.com/v3.1/name/{urllib.parse.quote(country)}?fields=name,capital,region,subregion,population,currencies,languages,area,flags,borders",
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                if not data:
                    return f"No data found for country: {country}"
                c = data[0]
                name = c.get('name', {}).get('common', country)
                capital = c.get('capital', ['Unknown'])[0] if c.get('capital') else 'Unknown'
                region = c.get('region', 'Unknown')
                subregion = c.get('subregion', '')
                population = f"{c.get('population', 0):,}"
                currencies = ', '.join([f"{v.get('name', k)} ({v.get('symbol', '')})" for k, v in c.get('currencies', {}).items()])
                languages = ', '.join(c.get('languages', {}).values())
                area = f"{c.get('area', 0):,} km²"

                return (
                    f"🌍 **{name}**\n"
                    f"- Capital: {capital}\n"
                    f"- Region: {region}{' / ' + subregion if subregion else ''}\n"
                    f"- Population: {population}\n"
                    f"- Area: {area}\n"
                    f"- Currency: {currencies}\n"
                    f"- Languages: {languages}"
                )
            return f"Country not found: {country}"
        except Exception as e:
            return f"Country lookup failed: {e}"

    # ============================================================
    # v10.5: Wayback Machine — free, no key needed
    # ============================================================
    async def _tool_wayback_machine(self, params: dict) -> str:
        """Retrieve archived/cached version of any URL via Wayback Machine."""
        url = params.get('url', '')
        if not url:
            return "Please provide a URL."

        try:
            response = requests.get(
                f"https://archive.org/wayback/available?url={urllib.parse.quote(url)}",
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                snapshot = data.get('archived_snapshots', {}).get('closest', {})
                if snapshot.get('available'):
                    archived_url = snapshot.get('url', '')
                    timestamp = snapshot.get('timestamp', '')
                    # Format timestamp YYYYMMDDHHMMSS → readable
                    if len(timestamp) >= 8:
                        readable = f"{timestamp[:4]}-{timestamp[4:6]}-{timestamp[6:8]}"
                    else:
                        readable = timestamp
                    return f"📦 **Wayback Machine snapshot** (archived {readable}):\n🔗 {archived_url}"
                return f"No archived version found for: {url}"
            return f"Wayback Machine error: {response.status_code}"
        except Exception as e:
            return f"Wayback Machine lookup failed: {e}"

    # ============================================================
    # v10.5: HackerNews Search — free, no key needed
    # ============================================================
    async def _tool_hackernews_search(self, params: dict) -> str:
        """Search HackerNews for tech and business discussions."""
        query = params.get('query', '')
        limit = params.get('limit', 5)

        if not query:
            return "Please provide a search query."

        try:
            response = requests.get(
                "https://hn.algolia.com/api/v1/search",
                params={"query": query, "hitsPerPage": limit, "tags": "story"},
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                hits = data.get('hits', [])
                if not hits:
                    return f"No HackerNews results for: {query}"
                results = []
                for h in hits:
                    points = h.get('points', 0)
                    title = h.get('title', 'No title')
                    url = h.get('url', f"https://news.ycombinator.com/item?id={h.get('objectID', '')}")
                    date = h.get('created_at', '')[:10]
                    results.append(f"- [{points}pts] **{title}** ({date})\n  🔗 {url}")
                return f"🟠 HackerNews results for '{query}':\n\n" + "\n\n".join(results)
            return f"HackerNews search error: {response.status_code}"
        except Exception as e:
            return f"HackerNews search failed: {e}"


    # ============================================================
    # v10.6 tool handlers (re-added in v10.7.2)
    # ============================================================
    async def _tool_opencorporates_search(self, params: dict) -> str:
        """Search OpenCorporates for registered company data."""
        company_name = params.get('company_name', '')
        jurisdiction = params.get('jurisdiction', '')
        if not company_name:
            return "Please provide a company name."
        try:
            api_params = {'q': company_name, 'format': 'json'}
            if jurisdiction:
                api_params['jurisdiction_code'] = jurisdiction
            response = requests.get(
                "https://api.opencorporates.com/v0.4/companies/search",
                params=api_params, timeout=15
            )
            if response.status_code == 200:
                data = response.json()
                companies = data.get('results', {}).get('companies', [])
                if not companies:
                    return f"No registered companies found for: {company_name}"
                results = []
                for item in companies[:5]:
                    c = item.get('company', {})
                    name = c.get('name', 'Unknown')
                    jcode = c.get('jurisdiction_code', '').upper()
                    number = c.get('company_number', 'N/A')
                    status = c.get('current_status', 'Unknown')
                    inc_date = c.get('incorporation_date', 'Unknown')
                    oc_url = c.get('opencorporates_url', '')
                    results.append(
                        f"- **{name}** ({jcode})\n"
                        f"  Reg#: {number} | Status: {status} | Incorporated: {inc_date}\n"
                        f"  🔗 {oc_url}"
                    )
                return f"🏢 **OpenCorporates results for '{company_name}':**\n\n" + "\n\n".join(results)
            return f"OpenCorporates error: {response.status_code}"
        except Exception as e:
            return f"OpenCorporates search failed: {e}"

    async def _tool_un_comtrade(self, params: dict) -> str:
        """Get China import/export trade stats from UN Comtrade."""
        flow = params.get('flow', 'export').lower()
        flow_code = '2' if 'import' in flow else '1'
        flow_label = 'Imports' if flow_code == '2' else 'Exports'
        try:
            response = requests.get(
                "https://comtradeapi.un.org/public/v1/preview/C/A/HS",
                params={
                    'reporterCode': '156',
                    'period': '2022',
                    'flowCode': flow_code,
                    'cmdCode': 'TOTAL',
                    'includeDesc': 'true'
                },
                timeout=15
            )
            if response.status_code == 200:
                data = response.json()
                records = data.get('data', [])
                if not records:
                    return f"No UN Comtrade data found for China {flow_label}."
                results = []
                for r in records[:5]:
                    partner = r.get('partnerDesc', 'World')
                    value = r.get('primaryValue', 0)
                    year = r.get('period', '2022')
                    if value:
                        results.append(f"- {partner}: ${value:,.0f} USD ({year})")
                return (f"📊 **China {flow_label}** (UN Comtrade, top partners):\n" +
                        "\n".join(results)) if results else f"No data for China {flow_label}."
            return f"UN Comtrade unavailable ({response.status_code}). Try china_economic_indicator instead."
        except Exception as e:
            return f"UN Comtrade lookup failed: {e}"

    async def _tool_ip_geolocation(self, params: dict) -> str:
        """Detect country/region from IP address."""
        ip = params.get('ip', '').strip()
        endpoint = f"https://ipapi.co/{ip}/json/" if ip else "https://ipapi.co/json/"
        try:
            response = requests.get(endpoint, headers={"User-Agent": "SophiaAI/1.0"}, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get('error'):
                    return f"IP lookup error: {data.get('reason', 'unknown')}"
                country = data.get('country_name', 'Unknown')
                region = data.get('region', '')
                city = data.get('city', '')
                org = data.get('org', '')
                timezone = data.get('timezone', '')
                return (
                    f"📍 **IP Geolocation**:\n"
                    f"- Location: {city}{', ' + region if region else ''}, {country}\n"
                    f"- Timezone: {timezone}\n"
                    f"- ISP/Org: {org}"
                )
            return f"IP geolocation failed: {response.status_code}"
        except Exception as e:
            return f"IP geolocation error: {e}"

    async def _tool_get_user_profile(self, params: dict) -> str:
        """Look up what Sophia knows about the current user."""
        session_id = params.get('session_id', '')
        if not session_id:
            return "No session_id provided."
        try:
            conn = get_db()
            c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            c.execute("SELECT * FROM user_profiles WHERE session_id = %s", (session_id,))
            profile = c.fetchone()
            release_db(conn)
            if not profile:
                return f"No profile found for session: {session_id}"
            p = dict(profile)
            lines = [f"👤 **User Profile ({session_id})**:"]
            if p.get('name'):             lines.append(f"- Name: {p['name']}")
            if p.get('email'):            lines.append(f"- Email: {p['email']}")
            if p.get('company'):          lines.append(f"- Company: {p['company']}")
            if p.get('phone'):            lines.append(f"- Phone: {p['phone']}")
            if p.get('region_interest'):  lines.append(f"- Region interest: {p['region_interest']}")
            if p.get('sector_interest'):  lines.append(f"- Sector interest: {p['sector_interest']}")
            lines.append(f"- Visit count: {p.get('visit_count', 1)}")
            lines.append(f"- Lead score: {p.get('lead_score', 0)}")
            if p.get('last_seen'):        lines.append(f"- Last seen: {str(p['last_seen'])[:19]}")
            return "\n".join(lines)
        except Exception as e:
            return f"Profile lookup failed: {e}"

tool_registry = ToolRegistry()

# ============================================================
# SOPHIA MAIN CLASS - FULLY AGENTIC
# ============================================================

# In-memory conversation history per session (survives within process lifetime)
_session_histories: Dict[str, List[dict]] = defaultdict(list)
_session_histories_lock = threading.Lock()
_session_last_seen: Dict[str, float] = {}   # session_id -> epoch time of last access
SESSION_HISTORY_TTL_SECONDS = 3600          # evict sessions idle for >1 hour

MAX_HISTORY_TURNS = 10          # keep last N user/assistant pairs
MAX_AGENT_ITERATIONS = 8        # max tool-use rounds (6 real + planning + reflection)

# Serper daily quota tracker (in-memory, resets on restart — fine for free tier)
_serper_usage: Dict[str, int] = {}   # date_str -> count
SERPER_DAILY_LIMIT = 80              # switch to DuckDuckGo at 80% of 100 free quota
REFLECTION_MIN_TOOLS = 1        # reflect only if at least N tools were used


def _prune_stale_sessions():
    """Remove session histories that haven't been accessed for SESSION_HISTORY_TTL_SECONDS."""
    now = time.time()
    with _session_histories_lock:
        stale = [sid for sid, ts in _session_last_seen.items()
                 if now - ts > SESSION_HISTORY_TTL_SECONDS]
        for sid in stale:
            _session_histories.pop(sid, None)
            _session_last_seen.pop(sid, None)
    if stale:
        print(f"🧹 Pruned {len(stale)} stale session(s) from history")


class SophiaAgent:
    """Main Sophia AI Agent - v10.3 China Business Enhanced Edition"""
    
    def __init__(self):
        self.tool_registry = tool_registry
        self.ai_provider = ai_provider
        self.memory = hybrid_memory
    
    # ------------------------------------------------------------------
    # PUBLIC ENTRY POINT
    # ------------------------------------------------------------------
    async def process_message(self, session_id: str, user_message: str, 
                              context: dict = None) -> Tuple[str, dict]:
        """
        Fully agentic process:
        1. Load conversation history + vector memories + feedback examples
        2. Enhanced ReAct planning step
        3. Multi-step tool loop
        4. Self-reflection pass (optional)
        5. Persist everything
        """
        context = context or {}
        profile = get_or_create_user_profile(session_id)
        past_episodes = self.memory.recall_similar_episodes(user_message, n_results=3)
        feedback_examples = self._get_feedback_examples(user_message, n_results=2)
        system_prompt = self._build_system_prompt(profile, past_episodes, feedback_examples)

        # --- load persisted conversation history ---
        history = self._get_history(session_id)

        messages = [{"role": "system", "content": system_prompt}]
        messages.extend(history)
        messages.append({"role": "user", "content": user_message})

        tools_schema = self.tool_registry.get_tools_schema()
        all_tools_used: List[str] = []

        # ------------------------------------------------------------------
        # STEP 1: Intent gate — skip expensive planning for simple messages
        # ------------------------------------------------------------------
        _msg_lower = user_message.lower().strip()
        _words = user_message.split()

        _is_simple = any(_msg_lower == s for s in [
            'hi', 'hello', 'hey', 'thanks', 'thank you', 'ok', 'okay',
            'yes', 'no', 'bye', 'goodbye', 'good morning', 'good afternoon',
            'good evening', 'how are you', 'nice', 'great', 'cool', 'perfect'
        ]) or len(_words) <= 2

        _is_question = any(_msg_lower.startswith(q) for q in ['what', 'who', 'how', 'why', 'when', 'where', 'can you', 'could you'])
        _has_research_keywords = any(w in _msg_lower for w in [
            'research', 'find', 'search', 'analyze', 'analyse', 'compare',
            'tell me about', 'explain', 'report', 'summarize', 'summarise',
            'investigate', 'look up', 'latest', 'recent', 'news', 'company',
            'supplier', 'information', 'details', 'background', 'translate'
        ])
        _is_long = len(_words) >= 6

        _needs_planning = (
            not _is_simple and
            ENABLE_REACT_REASONING and
            tools_schema and
            (_is_question or _has_research_keywords or _is_long)
        )

        if _needs_planning:
            plan_messages = messages + [{
                "role": "user",
                "content": (
                    "Before answering, please create a step‑by‑step plan. "
                    "Think about what information you need and which tools (if any) would be useful. "
                    "List the tools in the order you would use them, and explain why. "
                    "Then, based on your plan, provide your final answer.\n\n"
                    "Format your plan like this:\n"
                    "PLAN:\n1. <tool name> – <reason>\n2. ...\n\n"
                    "Then write your answer."
                )
            }]
            try:
                plan_resp = await self.ai_provider.chat_completion(
                    plan_messages, max_tokens=500, temperature=0.2
                )
                plan_text = plan_resp['choices'][0]['message'].get('content', '')
                if plan_text:
                    messages.append({
                        "role": "system",
                        "content": f"[Reasoning plan]:\n{plan_text}"
                    })
            except Exception as e:
                print(f"⚠️ Planning failed: {e}")

        # ------------------------------------------------------------------
        # STEP 2: Agentic tool loop
        # ------------------------------------------------------------------
        iteration = 0
        while iteration < MAX_AGENT_ITERATIONS:
            iteration += 1

            response = await self.ai_provider.chat_completion(
                messages,
                tools=tools_schema,
                tool_choice="auto",
                max_tokens=1000
            )

            assistant_message = response['choices'][0]['message']
            finish_reason = response['choices'][0].get('finish_reason', 'stop')

            if not assistant_message.get('tool_calls') or finish_reason == 'stop':
                messages.append(assistant_message)
                break

            tool_calls = assistant_message['tool_calls']
            tool_tasks = []
            for tc in tool_calls:
                tool_name = tc['function']['name']
                try:
                    tool_args = json.loads(tc['function']['arguments'])
                except Exception:
                    tool_args = {}
                tool_tasks.append((tc, tool_name, tool_args))

            async def _run_tool(tc, name, args):
                result = await self.tool_registry.execute(name, args)
                return tc, name, result

            results = await asyncio.gather(
                *[_run_tool(tc, n, a) for tc, n, a in tool_tasks],
                return_exceptions=True
            )

            messages.append(assistant_message)

            for res in results:
                if isinstance(res, Exception):
                    # Report error back to Sophia so she can retry differently
                    messages.append({
                        "role": "tool",
                        "tool_call_id": "error",
                        "content": f"Tool execution error: {str(res)}"
                    })
                    continue
                tc, tool_name, tool_result = res
                all_tools_used.append(tool_name)
                messages.append({
                    "role": "tool",
                    "tool_call_id": tc['id'],
                    "content": json.dumps(tool_result) if not isinstance(tool_result, str) else tool_result
                })

        final_response = (
            messages[-1].get('content') if messages[-1].get('role') == 'assistant'
            else response['choices'][0]['message'].get('content', '')
        ) or 'I apologise, I could not generate a response.'

        # ------------------------------------------------------------------
        # STEP 3 (optional): Self-reflection
        # ------------------------------------------------------------------
        if ENABLE_SELF_REFLECTION and len(all_tools_used) >= REFLECTION_MIN_TOOLS:
            final_response = await self._reflect_and_improve(
                messages, user_message, final_response
            )

        # ------------------------------------------------------------------
        # STEP 4: Persist
        # ------------------------------------------------------------------
        # Pass already-loaded history to avoid a second DB read
        self._update_history(session_id, user_message, final_response, current_history=history)

        # Dynamic success score: use latest feedback rating if available, else default 7
        try:
            conn = get_db()
            c = conn.cursor()
            c.execute("""
                SELECT rating FROM user_feedback
                WHERE session_id = %s
                ORDER BY created_at DESC LIMIT 1
            """, (session_id,))
            row = c.fetchone()
            release_db(conn)
            success_score = int(row[0]) * 2 if row else 7  # scale 1-5 → 2-10
        except Exception:
            success_score = 7

        self.memory.store_episodic(
            session_id, user_message, final_response,
            success_score=success_score,
            intent=context.get('intent', 'unknown')
        )
        update_user_profile(session_id, last_intent=context.get('intent'))
        conversation_id = self._store_conversation(session_id, user_message, final_response, context, all_tools_used)

        # Confidence scoring
        confidence = _compute_confidence(all_tools_used, final_response)

        return final_response, {
            'tools_used': all_tools_used,
            'iterations': iteration,
            'conversation_id': conversation_id,
            'confidence': confidence
        }

    # ------------------------------------------------------------------
    # SELF-REFLECTION
    # ------------------------------------------------------------------
    async def _reflect_and_improve(self, messages: List[dict], 
                                   user_message: str, draft: str) -> str:
        """Ask the agent to critique its own draft using a structured checklist."""
        try:
            reflection_messages = messages + [
                {
                    "role": "user",
                    "content": (
                        f"Review your previous answer for this question: '{user_message}'\n\n"
                        f"Your draft answer:\n{draft}\n\n"
                        "Evaluate using this checklist:\n"
                        "1. Did I use at least one tool to verify facts, or did I rely on assumptions?\n"
                        "2. Is anything uncertain, outdated, or potentially wrong?\n"
                        "3. Is the answer complete and useful for a China business context?\n"
                        "4. Is the answer clear and well-structured?\n\n"
                        "If you can meaningfully improve the answer based on this checklist, provide the improved version. "
                        "If the answer is already complete and accurate, repeat it unchanged. "
                        "Do NOT add unnecessary caveats or padding."
                    )
                }
            ]
            refl_resp = await self.ai_provider.chat_completion(
                reflection_messages, max_tokens=1200, temperature=0.2
            )
            improved = refl_resp['choices'][0]['message'].get('content', '').strip()
            if improved and len(improved) > 30:
                return improved
        except Exception as e:
            print(f"⚠️ Reflection failed: {e}")
        return draft

    # ------------------------------------------------------------------
    # FEEDBACK LEARNING
    # ------------------------------------------------------------------
    def _get_feedback_examples(self, query: str, n_results: int = 2) -> List[dict]:
        """
        Retrieve feedback examples relevant to the current query topic.
        Uses vector similarity when available, falls back to recency.
        """
        try:
            conn = get_db()
            c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            # Try topic-matched: join feedback with conversations and filter by keyword overlap
            keywords = [w for w in query.lower().split() if len(w) > 4][:5]
            if keywords:
                keyword_conditions = " OR ".join([f"c.user_message ILIKE %s" for _ in keywords])
                values = [f"%{kw}%" for kw in keywords] + [n_results * 3]
                c.execute(f"""
                    SELECT f.rating, f.comment, c.user_message, c.ai_response
                    FROM user_feedback f
                    JOIN conversations c ON f.conversation_id = c.id
                    WHERE f.rating <= 2 AND f.comment IS NOT NULL AND f.comment != ''
                    AND ({keyword_conditions})
                    ORDER BY f.created_at DESC
                    LIMIT %s
                """, values)
                rows = c.fetchall()
                if rows:
                    release_db(conn)
                    return [dict(r) for r in rows[:n_results]]

            # Fallback: most recent bad ratings
            c.execute("""
                SELECT f.rating, f.comment, c.user_message, c.ai_response
                FROM user_feedback f
                JOIN conversations c ON f.conversation_id = c.id
                WHERE f.rating <= 2 AND f.comment IS NOT NULL AND f.comment != ''
                ORDER BY f.created_at DESC
                LIMIT %s
            """, (n_results,))
            rows = c.fetchall()
            release_db(conn)
            return [dict(r) for r in rows]
        except Exception as e:
            print(f"Feedback retrieval error: {e}")
            return []

    # ------------------------------------------------------------------
    # CONVERSATION HISTORY HELPERS
    # Persistent in Supabase — survives Render restarts ✅
    # Falls back to in-memory if DB is unavailable
    # ------------------------------------------------------------------
    def _get_history(self, session_id: str) -> List[dict]:
        try:
            conn = get_db()
            c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            c.execute("SELECT history FROM session_histories WHERE session_id = %s", (session_id,))
            row = c.fetchone()
            release_db(conn)
            if row:
                return list(row['history']) if row['history'] else []
            return []
        except Exception as e:
            print(f"⚠️ DB history read failed, using in-memory: {e}")
            with _session_histories_lock:
                _session_last_seen[session_id] = time.time()
                return list(_session_histories[session_id])

    def _update_history(self, session_id: str, user_msg: str, assistant_msg: str,
                        current_history: Optional[List[dict]] = None):
        """Update history. Pass current_history to avoid a redundant DB read."""
        current = list(current_history) if current_history is not None else self._get_history(session_id)
        current.append({"role": "user", "content": user_msg})
        current.append({"role": "assistant", "content": assistant_msg})
        if len(current) > MAX_HISTORY_TURNS * 2:
            current = current[-(MAX_HISTORY_TURNS * 2):]

        try:
            conn = get_db()
            c = conn.cursor()
            c.execute("""
                INSERT INTO session_histories (session_id, history, updated_at)
                VALUES (%s, %s::jsonb, NOW())
                ON CONFLICT (session_id)
                DO UPDATE SET history = %s::jsonb, updated_at = NOW()
            """, (session_id, json.dumps(current), json.dumps(current)))
            conn.commit()
            release_db(conn)
        except Exception as e:
            print(f"⚠️ DB history write failed, using in-memory: {e}")
            with _session_histories_lock:
                _session_last_seen[session_id] = time.time()
                _session_histories[session_id] = current
        _prune_stale_sessions()

    # ------------------------------------------------------------------
    # SYSTEM PROMPT
    # ------------------------------------------------------------------
    def _build_system_prompt(self, profile: dict, past_episodes: List[dict], feedback_examples: List[dict]) -> str:
        base_prompt = """You are Sophia, an intelligent AI assistant for China West Connector (CWC).

You operate as a FULLY AGENTIC AI: you can reason step-by-step, call multiple tools in sequence,
reflect on your results, and refine your answers autonomously.

Core capabilities:
- **Google Search** — serper_search (best quality, 100/day free) → fallback: duckduckgo_search
- **Wikipedia & Wikidata** — wikipedia_search, wikipedia_article, wikidata_search, company_info
- **China Business** — china_business_news, china_rss_news, china_economic_indicator, translate_chinese
- **Company Verification** — opencorporates_search (registry due diligence), company_info
- **Trade & Finance** — currency_rates (live USD/CNY), un_comtrade (import/export stats)
- **News & Social** — news_monitor, hackernews_search, reddit_search, reddit_get_posts
- **Browser & Web** — browse_page, screenshot_page, extract_web_data, jina_reader, wayback_machine
- **Geography** — country_info, geocode_address, ip_geolocation
- **User Intelligence** — get_user_profile, recall_memories, store_memory
- **Research** — research_topic (deep multi-source), generate_report, analyze_sentiment

Tool-use guidelines:
- Always prefer serper_search over duckduckgo_search for important queries.
- For supplier verification, chain: opencorporates_search → company_info → serper_search.
- For trade questions, use currency_rates and un_comtrade together.
- Call get_user_profile early to personalise responses.
- If a tool result is poor, rephrase and retry with a different tool.
- Always synthesise tool outputs into a clear, helpful final answer.
- Never fabricate facts; prefer verified tool results over assumptions."""

        if past_episodes:
            base_prompt += "\n\nRelevant past conversations:\n"
            for ep in past_episodes[:2]:
                base_prompt += f"- {ep['text'][:200]}…\n"

        if feedback_examples:
            base_prompt += "\n\n📝 **Learning from user feedback:**\n"
            for fb in feedback_examples:
                base_prompt += f"- A similar question was previously answered poorly. Here is a corrected response that users preferred:\n  “{fb['comment'][:200]}”\n\n"

        if profile.get('name'):
            base_prompt += f"\n\nCurrent user: {profile['name']}"
        if profile.get('company'):
            base_prompt += f" ({profile['company']})"

        return base_prompt

    # ------------------------------------------------------------------
    # PERSISTENCE
    # ------------------------------------------------------------------
    def _store_conversation(self, session_id: str, user_message: str, response: str,
                            context: dict, tools_used: List[str]) -> Optional[int]:
        """Store conversation and return the new conversation ID."""
        try:
            conn = get_db()
            c = conn.cursor()
            c.execute("""
                INSERT INTO conversations 
                (session_id, user_message, ai_response, intent, tools_used)
                VALUES (%s, %s, %s, %s, %s::jsonb)
                RETURNING id
            """, (session_id, user_message, response, context.get('intent'),
                  json.dumps(tools_used)))
            conv_id = c.fetchone()[0]
            conn.commit()
            release_db(conn)
            return conv_id
        except Exception as e:
            print(f"Conversation storage error: {e}")
            return None


sophia = SophiaAgent()

# ============================================================
# BACKGROUND WORKERS
# ============================================================

def _execute_single_goal(goal_id: int, goal_type: str, description: str):
    """Execute one goal in its own event loop (called from ThreadPoolExecutor)."""
    try:
        # Mark in-progress
        conn = get_db()
        c = conn.cursor()
        c.execute(
            "UPDATE autonomous_goals SET status = 'in_progress', started_at = NOW() WHERE id = %s",
            (goal_id,)
        )
        conn.commit()
        release_db(conn)

        # Run via agent
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

        tools_used = meta.get('tools_used', [])
        summary = result[:2000] if result else "No result"

        conn2 = get_db()
        c2 = conn2.cursor()
        c2.execute("""
            UPDATE autonomous_goals
            SET status = 'completed', completed_at = NOW(),
                result = %s, completed_subtasks = %s::jsonb
            WHERE id = %s
        """, (summary, json.dumps({"tools_used": tools_used}), goal_id))
        conn2.commit()
        release_db(conn2)
        print(f"✅ Goal {goal_id} ({goal_type}) completed. Tools: {tools_used}")

    except Exception as e:
        print(f"⚠️ Goal {goal_id} failed: {e}")
        try:
            conn_err = get_db()
            c_err = conn_err.cursor()
            c_err.execute("""
                UPDATE autonomous_goals
                SET status = 'failed', result = %s, retry_count = retry_count + 1
                WHERE id = %s
            """, (str(e)[:500], goal_id))
            conn_err.commit()
            release_db(conn_err)
        except Exception:
            pass


def goal_executor():
    """Background thread — runs goals in parallel via ThreadPoolExecutor."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    while True:
        try:
            time.sleep(GOAL_EXECUTION_INTERVAL_MINUTES * 60)

            if not DATABASE_URL:
                continue

            # ---- Stale goal cleanup (pending > 48h → mark stale) ----
            try:
                conn_stale = get_db()
                c_stale = conn_stale.cursor()
                c_stale.execute("""
                    UPDATE autonomous_goals
                    SET status = 'stale'
                    WHERE status = 'pending'
                      AND created_at < NOW() - INTERVAL '48 hours'
                """)
                stale_count = c_stale.rowcount
                conn_stale.commit()
                release_db(conn_stale)
                if stale_count:
                    print(f"🧹 Marked {stale_count} stale goal(s) (pending >48h)")
            except Exception as e:
                print(f"⚠️ Stale goal cleanup error: {e}")

            # ---- Fetch pending goals ----
            conn = get_db()
            c = conn.cursor()
            c.execute("""
                SELECT id, goal_type, goal_description FROM autonomous_goals
                WHERE status = 'pending' AND priority >= 5
                ORDER BY priority DESC, created_at ASC
                LIMIT %s
            """, (MAX_CONCURRENT_GOALS,))
            goals = c.fetchall()
            release_db(conn)

            if not goals:
                continue

            # ---- Run all goals in parallel ----
            with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_GOALS) as executor:
                futures = {
                    executor.submit(_execute_single_goal, gid, gtype, gdesc): gid
                    for gid, gtype, gdesc in goals
                }
                for future in as_completed(futures):
                    gid = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        print(f"⚠️ Goal {gid} thread error: {e}")

        except Exception as outer:
            print(f"⚠️ Goal executor error: {outer}")


# ============================================================
# NEW v10.3: Proactive China News Goal Generator (daily)
# ============================================================
def proactive_china_goal_generator():
    """Scans recent conversations for China‑interested users and creates monitoring goals."""
    while True:
        try:
            # Run once per day
            time.sleep(24 * 60 * 60)

            if not DATABASE_URL:
                continue

            conn = get_db()
            c = conn.cursor()
            # Find sessions with China keywords in the last 7 days
            c.execute("""
                SELECT DISTINCT session_id
                FROM conversations
                WHERE timestamp > NOW() - INTERVAL '7 days'
                  AND (user_message ILIKE '%china%' OR user_message ILIKE '%chinese%' OR user_message ILIKE '%supplier%')
                  AND session_id NOT LIKE 'autonomous_goal_%'
                ORDER BY session_id
            """)
            sessions = c.fetchall()

            for (session_id,) in sessions:
                # Check if a monitoring goal already exists for this session
                c.execute("""
                    SELECT id FROM autonomous_goals
                    WHERE session_id = %s AND goal_type = 'monitor_china_news' AND status IN ('pending', 'in_progress')
                """, (session_id,))
                if not c.fetchone():
                    # Create new goal
                    c.execute("""
                        INSERT INTO autonomous_goals (session_id, goal_type, goal_description, priority, source)
                        VALUES (%s, 'monitor_china_news', 'Monitor latest China business news for this user', 3, 'proactive')
                        RETURNING id
                    """, (session_id,))
                    goal_id = c.fetchone()[0]
                    print(f"🎯 Created proactive China news goal {goal_id} for session {session_id}")

            conn.commit()
            release_db(conn)

        except Exception as e:
            print(f"⚠️ Proactive goal generator error: {e}")


# ============================================================
# FASTAPI APP
# ============================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    _init_pool()
    init_db()
    
    # Start background threads
    goal_thread = threading.Thread(target=goal_executor, daemon=True)
    goal_thread.start()
    
    # Start proactive goal generator (only if database is configured)
    if DATABASE_URL:
        proactive_thread = threading.Thread(target=proactive_china_goal_generator, daemon=True)
        proactive_thread.start()
        print("🌐 Proactive China goal generator started (runs daily)")
    
    print(f"""
    ╔══════════════════════════════════════════════════════════════╗
    ║       SOPHIA AI SERVER v10.7.1 - CHINA BUSINESS EDITION     ║
    ╠══════════════════════════════════════════════════════════════╣
    ║  🧠 Vector Backend: {hybrid_memory.backend_type:<38} ║
    ║  🔧 Tools Loaded: {len(tool_registry.tools):<40} ║
    ║  🤖 AI Providers: {len(ai_provider.providers):<40} ║
    ║  🧬 Embeddings: {'HF API (semantic) ✅' if HUGGINGFACE_API_KEY else 'hash-based (limited) ⚠️':<38} ║
    ║  💾 Session History: {'Supabase (persistent) ✅' if DATABASE_URL else 'in-memory only ⚠️':<33} ║
    ║  🔍 Serper Search: {'✅ Google quality' if SERPER_API_KEY else '⚠️ DuckDuckGo fallback':<33} ║
    ║  ⚡ Parallel Goals: ✅ ThreadPoolExecutor (max {MAX_CONCURRENT_GOALS})            ║
    ║  🧹 Stale Goal Cleanup: ✅ (>48h auto-marked stale)         ║
    ║  ✂️  Tool Output Truncation: ✅ ({TOOL_OUTPUT_MAX_CHARS} chars max)               ║
    ║  🎯 Confidence Scoring: ✅ per-response                     ║
    ║  🔁 Auto-retry Failed Tools: ✅ (1 rephrase attempt)        ║
    ║  📚 Wikipedia + Wikidata: ✅                                 ║
    ║  🇨🇳 China Business Tools: ✅ (news/RSS/econ/translate)      ║
    ║  🏢 OpenCorporates: ✅ company registry                     ║
    ║  📊 UN Comtrade: ✅ trade statistics                        ║
    ║  💱 Currency Rates: ✅ live USD/CNY                         ║
    ║  📍 IP Geolocation: ✅                                       ║
    ║  🌐 Browser Automation: {'✅ Playwright' if PLAYWRIGHT_AVAILABLE else '⚠️ HTTP fallback':<29} ║
    ║  ♾️  Agentic Loop: up to {MAX_AGENT_ITERATIONS} iterations                     ║
    ║  🚦 Intent Gate: ✅ simple msgs skip planning               ║
    ║  🪞 Self-Reflection: ✅ structured checklist                ║
    ║  💬 Feedback Learning: ✅ topic-matched                     ║
    ╚══════════════════════════════════════════════════════════════╝
    """)
    
    yield
    
    print("🛑 Sophia AI Server shutting down...")

app = FastAPI(
    title="Sophia AI Server v10.7.2",
    description="China Business Enhanced Edition — Parallel Goals, Confidence Scoring, Auto-retry",
    version="10.7.2",
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
    conversation_id: Optional[int] = None
    confidence: Optional[dict] = None

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "Sophia AI Server",
        "version": "10.7.2",
        "vector_backend": hybrid_memory.backend_type,
        "tools_count": len(tool_registry.tools),
        "playwright": PLAYWRIGHT_AVAILABLE,
        "google_news_rss": FEEDPARSER_AVAILABLE,
        "status": "running"
    }

@app.get("/health")
async def health_check():
    """Health check"""
    return {
        "status": "healthy",
        "vector_backend": hybrid_memory.get_status(),
        "ai_providers": len(ai_provider.providers),
        "tools_available": len(tool_registry.tools),
        "playwright": PLAYWRIGHT_AVAILABLE
    }

@app.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """Main chat endpoint — fully agentic multi-step reasoning"""
    response, metadata = await sophia.process_message(
        request.session_id,
        request.message,
        request.context
    )
    return ChatResponse(
        response=response,
        tools_used=metadata.get('tools_used', []),
        iterations=metadata.get('iterations', 1),
        conversation_id=metadata.get('conversation_id'),
        confidence=metadata.get('confidence')
    )

@app.post("/chat/stream")
async def chat_stream(request: ChatRequest):
    """Streaming chat endpoint — returns Server-Sent Events"""
    async def event_generator():
        try:
            response_text, metadata = await sophia.process_message(
                request.session_id,
                request.message,
                request.context
            )
            tools_used = metadata.get('tools_used', [])
            iterations = metadata.get('iterations', 1)
            conv_id = metadata.get('conversation_id')

            chunk_size = 50
            for i in range(0, len(response_text), chunk_size):
                chunk = response_text[i:i + chunk_size]
                yield f"data: {json.dumps({'type': 'text', 'content': chunk})}\n\n"
                await asyncio.sleep(0.02)

            yield f"data: {json.dumps({'type': 'done', 'tools_used': tools_used, 'iterations': iterations, 'conversation_id': conv_id})}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")


class GoalRequest(BaseModel):
    session_id: str = "system"
    goal_type: str
    description: str
    priority: int = 5

@app.post("/goals")
async def create_goal(req: GoalRequest):
    """Create an autonomous goal for background execution"""
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("""
            INSERT INTO autonomous_goals (session_id, goal_type, goal_description, priority, status)
            VALUES (%s, %s, %s, %s, 'pending')
            RETURNING id
        """, (req.session_id, req.goal_type, req.description, req.priority))
        goal_id = c.fetchone()[0]
        conn.commit()
        release_db(conn)
        return {"status": "created", "goal_id": goal_id, "priority": req.priority}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/goals")
async def list_goals(status: Optional[str] = None, limit: int = 20):
    """List autonomous goals"""
    try:
        conn = get_db()
        c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if status:
            c.execute(
                "SELECT * FROM autonomous_goals WHERE status = %s ORDER BY created_at DESC LIMIT %s",
                (status, limit)
            )
        else:
            c.execute("SELECT * FROM autonomous_goals ORDER BY created_at DESC LIMIT %s", (limit,))
        goals = [dict(r) for r in c.fetchall()]
        release_db(conn)
        return {"goals": goals, "count": len(goals)}
    except Exception as e:
        return {"error": str(e)}

@app.delete("/chat/history/{session_id}")
async def clear_chat_history(session_id: str):
    """Clear conversation history for a session — both in-memory and Supabase"""
    # Clear in-memory
    with _session_histories_lock:
        _session_histories.pop(session_id, None)
    # Clear from Supabase
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("DELETE FROM session_histories WHERE session_id = %s", (session_id,))
        conn.commit()
        release_db(conn)
    except Exception as e:
        print(f"⚠️ Could not clear DB history for {session_id}: {e}")
    return {"status": "cleared", "session_id": session_id}

@app.get("/memory/status")
async def get_memory_status():
    """Get vector memory status"""
    return hybrid_memory.get_status()

@app.post("/memory/store")
async def store_memory(fact_type: str, fact_value: str, importance: int = 5):
    """Store a memory"""
    hybrid_memory.store_semantic(fact_type, fact_value, importance, 'manual')
    return {"status": "stored", "fact_type": fact_type}

@app.get("/memory/recall")
async def recall_memory(query: str, n_results: int = 5):
    """Recall memories"""
    episodes = hybrid_memory.recall_similar_episodes(query, n_results)
    return {"episodes": episodes}

@app.get("/tools")
async def list_tools():
    """List all tools"""
    return {
        "count": len(tool_registry.tools),
        "tools": [{"name": k, "description": v.get('description', '')} 
                  for k, v in tool_registry.tools.items()]
    }

@app.post("/tools/{tool_name}/execute")
async def execute_tool(tool_name: str, params: dict):
    """Execute a tool"""
    result = await tool_registry.execute(tool_name, params)
    return result

# ============================================================
# WIKIPEDIA & BROWSING ENDPOINTS
# ============================================================

@app.get("/wikipedia/search")
async def wikipedia_search(query: str, limit: int = 5):
    """Search Wikipedia"""
    results = await wikipedia_knowledge.search_wikipedia(query, limit)
    return {"query": query, "results": results}

@app.get("/wikipedia/article/{title}")
async def wikipedia_article(title: str):
    """Get Wikipedia article"""
    article = await wikipedia_knowledge.get_article(title)
    return article

@app.get("/wikidata/search")
async def wikidata_search(query: str, limit: int = 5):
    """Search Wikidata"""
    results = await wikipedia_knowledge.search_wikidata(query, limit)
    return {"query": query, "results": results}

@app.get("/wikidata/entity/{entity_id}")
async def wikidata_entity(entity_id: str):
    """Get Wikidata entity"""
    entity = await wikipedia_knowledge.get_wikidata_entity(entity_id)
    return entity

@app.get("/company/{company_name}")
async def company_info(company_name: str):
    """Get company information"""
    info = await wikipedia_knowledge.get_company_info(company_name)
    return info

@app.get("/browse")
async def browse_page(url: str, wait_time: int = 2):
    """Browse a webpage"""
    result = await browser_automation.browse_page(url, wait_time)
    return result

@app.get("/screenshot")
async def screenshot_page(url: str, full_page: bool = False):
    """Take a screenshot"""
    result = await browser_automation.take_screenshot(url, full_page)
    
    if result.get('screenshot'):
        # Return as image
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False, suffix='.png') as f:
            f.write(base64.b64decode(result['screenshot']))
            return FileResponse(f.name, media_type='image/png')
    
    return result

# ============================================================
# USER FEEDBACK ENDPOINT (v10.1)
# ============================================================

class FeedbackRequest(BaseModel):
    session_id: str
    conversation_id: Optional[int] = None
    rating: int  # 1-5
    comment: Optional[str] = None

@app.post("/feedback")
async def submit_feedback(fb: FeedbackRequest):
    """Submit user feedback on a conversation"""
    if fb.rating < 1 or fb.rating > 5:
        raise HTTPException(status_code=400, detail="Rating must be between 1 and 5")
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("""
            INSERT INTO user_feedback (session_id, conversation_id, rating, comment)
            VALUES (%s, %s, %s, %s)
        """, (fb.session_id, fb.conversation_id, fb.rating, fb.comment))
        conn.commit()
        release_db(conn)
        return {"status": "thank you for your feedback!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================
# NEW: China News Endpoint for Widget
# ============================================================
# Simple in-memory cache
_news_cache = {}
_news_cache_time = 0
CACHE_DURATION = 300  # 5 minutes (adjust as needed)

@app.get("/api/china-news")
async def china_news(limit: int = Query(5, ge=1, le=10), force_refresh: bool = Query(False)):
    """Returns latest China business news for the widget. Cached for 5 minutes."""
    global _news_cache, _news_cache_time

    # Return cached data if still fresh and not forced refresh
    if not force_refresh and time.time() - _news_cache_time < CACHE_DURATION:
        return _news_cache

    api_key = os.getenv("MEDIASTACK_API_KEY")
    if not api_key:
        return {"error": "MEDIASTACK_API_KEY not configured", "articles": []}

    # Build request – you can adjust keywords/categories
    url = (
        "http://api.mediastack.com/v1/news"
        f"?access_key={api_key}"
        "&keywords=China business OR Chinese suppliers OR China economy"
        "&countries=cn"
        "&languages=en"
        "&categories=business"
        f"&limit={limit}"
        "&sort=published_desc"
    )

    try:
        resp = requests.get(url, timeout=10)
        data = resp.json()

        if "error" in data:
            return {"error": data["error"]["message"], "articles": []}

        # Format response for widget
        articles = []
        for item in data.get("data", []):
            articles.append({
                "category": "Business",
                "headline": item.get("title"),
                "time": item.get("published_at", "")[:10],  # YYYY-MM-DD
                "query": f"Tell me more about: {item.get('title')}"
            })

        result = {"articles": articles, "updated": datetime.now().isoformat()}
        _news_cache = result
        _news_cache_time = time.time()
        return result

    except Exception as e:
        return {"error": str(e), "articles": []}

# ============================================================
# ADMIN STATS ENDPOINT
# ============================================================
@app.get("/admin/stats")
async def admin_stats(password: str):
    """Get admin statistics"""
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid password")
    
    try:
        conn = get_db()
        c = conn.cursor()
        
        c.execute("SELECT COUNT(*) FROM conversations")
        conversation_count = c.fetchone()[0]
        
        c.execute("SELECT COUNT(*) FROM user_profiles")
        user_count = c.fetchone()[0]
        
        c.execute("SELECT AVG(rating) FROM user_feedback")
        avg_rating = c.fetchone()[0]
        
        release_db(conn)
        
        return {
            "conversations": conversation_count,
            "users": user_count,
            "average_feedback": round(avg_rating, 2) if avg_rating else None,
            "vector_backend": hybrid_memory.backend_type,
            "playwright": PLAYWRIGHT_AVAILABLE
        }
    except Exception as e:
        return {"error": str(e)}

# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)