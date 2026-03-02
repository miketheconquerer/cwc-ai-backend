"""
================================================================================
SOPHIA AI SERVER v10.2 - CHINA BUSINESS & FEEDBACK LEARNING
================================================================================
NEW IN v10.2:
🇨🇳 China Business News tool – real‑time headlines via Google News RSS
📝 Learning from feedback – uses user corrections to improve future answers
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
from collections import defaultdict
from datetime import datetime, timedelta
from dotenv import load_dotenv
from contextlib import asynccontextmanager
from typing import List, Dict, Any, Optional, Tuple, Callable
import uuid
import traceback
import base64
import math

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
# DATABASE LAYER
# ============================================================
def get_db():
    """Get database connection"""
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL not configured")
    return psycopg2.connect(DATABASE_URL)

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

        conn.commit()
        conn.close()
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
        conn.close()
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
        
        conn.close()
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
        conn.close()
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
        """Initialize the sentence encoder - with lightweight fallback"""
        if SENTENCE_TRANSFORMERS_AVAILABLE:
            try:
                from sentence_transformers import SentenceTransformer
                self.encoder = SentenceTransformer('all-MiniLM-L6-v2')
                print("✅ Sentence encoder initialized (all-MiniLM-L6-v2)")
                return
            except Exception as e:
                print(f"⚠️ Encoder init failed: {e}")
        
        print("✅ Using lightweight hash-based embeddings (no ML libraries needed)")
        self.encoder = "hash"
    
    def encode(self, text: str) -> List[float]:
        """Encode text to embedding vector - with lightweight fallback"""
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
        if self.encoder == "hash":
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
                    "languages": "en",
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
        """Get company information from Wikipedia and Wikidata"""
        # Search Wikipedia
        wiki_results = await self.search_wikipedia(f"{company_name} company", limit=1)
        
        result = {
            "company": company_name,
            "wikipedia": None,
            "wikidata": None
        }
        
        if wiki_results:
            article = await self.get_article(wiki_results[0]["title"])
            result["wikipedia"] = article
            
            # Try to find Wikidata entity
            wd_results = await self.search_wikidata(company_name, limit=1)
            if wd_results:
                entity = await self.get_wikidata_entity(wd_results[0]["id"])
                result["wikidata"] = entity
        
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
                'description': 'Get comprehensive company information from Wikipedia and Wikidata.',
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
            conn.close()
            print(f"🔧 Loaded {len(self.tools)} tools")
        except Exception as e:
            print(f"Tool load error: {e}")
    
    async def execute(self, tool_name: str, params: dict) -> dict:
        """Execute a tool and return result"""
        if tool_name not in self.tools:
            return {'success': False, 'error': f"Tool '{tool_name}' not found"}
        
        tool = self.tools[tool_name]
        try:
            if 'handler' in tool:
                result = await tool['handler'](params)
            else:
                result = self._execute_custom(tool, params)
            
            return {'success': True, 'result': result}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
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
            'importance', 'context', 'priority', 'keywords'
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
            conn.close()
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
            conn.close()
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
                        if hasattr(entry, 'published_parsed'):
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
        """Get company info"""
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
    # NEW v10.2: China Business News tool handler
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
MAX_AGENT_ITERATIONS = 6        # max tool-use rounds per message
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
    """Main Sophia AI Agent - v10.2 China Business & Feedback Learning"""
    
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
        # STEP 1: Enhanced ReAct planning
        # ------------------------------------------------------------------
        _msg_lower = user_message.lower()
        _words = user_message.split()
        _is_question = any(_msg_lower.startswith(q) for q in ['what', 'who', 'how', 'why', 'when', 'where', 'can you', 'could you'])
        _has_research_keywords = any(w in _msg_lower for w in [
            'research', 'find', 'search', 'analyze', 'analyse', 'compare',
            'tell me about', 'explain', 'report', 'summarize', 'summarise',
            'investigate', 'look up', 'latest', 'recent', 'news', 'company',
            'supplier', 'information', 'details', 'background'
        ])
        _is_long = len(_words) >= 6

        _needs_planning = (
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
        self._update_history(session_id, user_message, final_response)
        self.memory.store_episodic(
            session_id, user_message, final_response,
            success_score=7,
            intent=context.get('intent', 'unknown')
        )
        update_user_profile(session_id, last_intent=context.get('intent'))
        conversation_id = self._store_conversation(session_id, user_message, final_response, context, all_tools_used)

        return final_response, {'tools_used': all_tools_used, 'iterations': iteration, 'conversation_id': conversation_id}

    # ------------------------------------------------------------------
    # SELF-REFLECTION
    # ------------------------------------------------------------------
    async def _reflect_and_improve(self, messages: List[dict], 
                                   user_message: str, draft: str) -> str:
        """Ask the agent to critique its own draft and produce an improved version."""
        try:
            reflection_messages = messages + [
                {
                    "role": "user",
                    "content": (
                        f"Review your previous answer:\n\n{draft}\n\n"
                        "Is it accurate, complete, and helpful? "
                        "If you can meaningfully improve it, provide the improved version. "
                        "Otherwise, repeat the original answer unchanged."
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
        Retrieve recent low‑rated conversations with user comments.
        In a production system you would also vector‑search; here we simply
        return the most recent ones as a starting point.
        """
        try:
            conn = get_db()
            c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            c.execute("""
                SELECT f.rating, f.comment, c.user_message, c.ai_response
                FROM user_feedback f
                JOIN conversations c ON f.conversation_id = c.id
                WHERE f.rating <= 2 AND f.comment IS NOT NULL AND f.comment != ''
                ORDER BY f.created_at DESC
                LIMIT %s
            """, (n_results,))
            rows = c.fetchall()
            conn.close()
            return [dict(r) for r in rows]
        except Exception as e:
            print(f"Feedback retrieval error: {e}")
            return []

    # ------------------------------------------------------------------
    # CONVERSATION HISTORY HELPERS
    # ------------------------------------------------------------------
    def _get_history(self, session_id: str) -> List[dict]:
        with _session_histories_lock:
            _session_last_seen[session_id] = time.time()
            return list(_session_histories[session_id])

    def _update_history(self, session_id: str, user_msg: str, assistant_msg: str):
        with _session_histories_lock:
            _session_last_seen[session_id] = time.time()
            hist = _session_histories[session_id]
            hist.append({"role": "user", "content": user_msg})
            hist.append({"role": "assistant", "content": assistant_msg})
            if len(hist) > MAX_HISTORY_TURNS * 2:
                _session_histories[session_id] = hist[-(MAX_HISTORY_TURNS * 2):]
        _prune_stale_sessions()

    # ------------------------------------------------------------------
    # SYSTEM PROMPT
    # ------------------------------------------------------------------
    def _build_system_prompt(self, profile: dict, past_episodes: List[dict], feedback_examples: List[dict]) -> str:
        base_prompt = """You are Sophia, an intelligent AI assistant for China West Connector (CWC).

You operate as a FULLY AGENTIC AI: you can reason step-by-step, call multiple tools in sequence, 
reflect on your results, and refine your answers autonomously.

Core capabilities:
- **Wikipedia & Wikidata** — Instant encyclopaedia knowledge
- **Browser Automation** — Browse websites, take screenshots, extract data
- **Web Search** — DuckDuckGo, Tavily, Bing
- **Social Monitoring** — Reddit discussions, news monitoring
- **Geocoding** — Address verification via OpenStreetMap
- **Vector Memory** — Recall past conversations
- **Research** — Deep multi-source topic research

Tool-use guidelines:
- Use tools proactively whenever they would improve accuracy or completeness.
- Chain tools when needed: e.g. search → browse → summarise.
- If a tool result is insufficient, try a different tool or query.
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
            conn.close()
            return conv_id
        except Exception as e:
            print(f"Conversation storage error: {e}")
            return None


sophia = SophiaAgent()

# ============================================================
# BACKGROUND WORKERS
# ============================================================
def goal_executor():
    """Background thread for autonomous goal execution."""
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
                    # Mark as in-progress
                    conn2 = get_db()
                    c2 = conn2.cursor()
                    c2.execute(
                        "UPDATE autonomous_goals SET status = 'in_progress', started_at = NOW() WHERE id = %s",
                        (goal_id,)
                    )
                    conn2.commit()
                    conn2.close()

                    # Execute goal via the agent (run in a new event loop for the thread)
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

                    conn3 = get_db()
                    c3 = conn3.cursor()
                    c3.execute("""
                        UPDATE autonomous_goals 
                        SET status = 'completed', completed_at = NOW(),
                            result = %s,
                            completed_subtasks = %s::jsonb
                        WHERE id = %s
                    """, (summary, json.dumps({"tools_used": tools_used}), goal_id))
                    conn3.commit()
                    conn3.close()

                    print(f"✅ Goal {goal_id} ({goal_type}) completed. Tools: {tools_used}")

                except Exception as e:
                    print(f"⚠️ Goal {goal_id} failed: {e}")
                    try:
                        conn_err = get_db()
                        c_err = conn_err.cursor()
                        c_err.execute("""
                            UPDATE autonomous_goals 
                            SET status = 'failed', result = %s,
                                retry_count = retry_count + 1
                            WHERE id = %s
                        """, (str(e)[:500], goal_id))
                        conn_err.commit()
                        conn_err.close()
                    except Exception:
                        pass

        except Exception as outer:
            print(f"⚠️ Goal executor error: {outer}")

# ============================================================
# FASTAPI APP
# ============================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    init_db()
    
    # Start background threads
    goal_thread = threading.Thread(target=goal_executor, daemon=True)
    goal_thread.start()
    
    print(f"""
    ╔══════════════════════════════════════════════════════════════╗
    ║        SOPHIA AI SERVER v10.2 - CHINA BUSINESS EDITION      ║
    ╠══════════════════════════════════════════════════════════════╣
    ║  🧠 Vector Backend: {hybrid_memory.backend_type:<38} ║
    ║  🔧 Tools Loaded: {len(tool_registry.tools):<40} ║
    ║  🤖 AI Model: llama-3.1-8b-instruct (upgraded)              ║
    ║  🤖 AI Providers: {len(ai_provider.providers):<40} ║
    ║  📚 Wikipedia API: ✅                                        ║
    ║  📰 Google News RSS: {'✅' if FEEDPARSER_AVAILABLE else '⚠️ feedparser missing':<29} ║
    ║  🇨🇳 China Business News: ✅ dedicated tool                   ║
    ║  📝 Learning from Feedback: ✅ active                         ║
    ║  🌐 Browser Automation: {'✅ Playwright' if PLAYWRIGHT_AVAILABLE else '⚠️ HTTP fallback':<29} ║
    ║  ♾️  Agentic Loop: up to {MAX_AGENT_ITERATIONS} iterations                     ║
    ║  🪞 Self-Reflection: {'✅ enabled' if ENABLE_SELF_REFLECTION else '❌ disabled':<31} ║
    ║  🧭 ReAct Reasoning: {'✅ smart-gated' if ENABLE_REACT_REASONING else '❌ disabled':<27} ║
    ║  🧹 Session Pruning: 1hr TTL                                 ║
    ║  💬 User Feedback: ✅ collecting & learning                   ║
    ╚══════════════════════════════════════════════════════════════╝
    """)
    
    yield
    
    print("🛑 Sophia AI Server shutting down...")

app = FastAPI(
    title="Sophia AI Server v10.2",
    description="China Business Edition with Feedback Learning",
    version="10.2.0",
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

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "Sophia AI Server",
        "version": "10.2.0",
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
        conversation_id=metadata.get('conversation_id')
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
        conn.close()
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
        conn.close()
        return {"goals": goals, "count": len(goals)}
    except Exception as e:
        return {"error": str(e)}

@app.delete("/chat/history/{session_id}")
async def clear_chat_history(session_id: str):
    """Clear in-memory conversation history for a session"""
    with _session_histories_lock:
        _session_histories.pop(session_id, None)
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
        conn.close()
        return {"status": "thank you for your feedback!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

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
        
        conn.close()
        
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