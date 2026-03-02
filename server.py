"""
================================================================================
SOPHIA AI SERVER v9.7 - EXTERNAL VECTOR DB EDITION
================================================================================
100% FREE AI with OpenRouter + Cloudflare

NEW IN v9.7 - EXTERNAL VECTOR DATABASE SUPPORT:
🗄️ External ChromaDB - Connect to ChromaDB hosted anywhere (Railway, Fly.io, Docker)
🗄️ Supabase pgvector - Use Supabase's built-in vector storage (FREE 500MB!)
🗄️ In-memory Fallback - Works without any vector DB
🗄️ Hybrid Memory - Combine multiple storage backends

EXTERNAL CHROMADB OPTIONS:
1. Railway.app (Easiest - one-click deploy)
2. Fly.io (Good free tier)
3. Docker on VPS (Full control)
4. Chroma Cloud (Managed service)

PREVIOUS VERSIONS:
✅ v9.6 Intelligence - Tool Chaining, Self-Reflection, ReAct Reasoning
✅ Reddit API - Social Listening (100% FREE!)
✅ Nominatim Geocoding - Address Verification (100% FREE!)
✅ ZenRows - Advanced Web Scraping (1,000/month FREE)
✅ Bing Webmaster API + DuckDuckGo + Tavily + NewsAPI
================================================================================
"""

from fastapi import FastAPI, BackgroundTasks, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse, JSONResponse
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

# Optional: Sentence Transformers for Embeddings (needed for Supabase pgvector)
SENTENCE_TRANSFORMERS_AVAILABLE = False
try:
    from sentence_transformers import SentenceTransformer
    SENTENCE_TRANSFORMERS_AVAILABLE = True
    print("✅ Sentence Transformers available")
except Exception as e:
    print(f"⚠️ sentence-transformers not installed: {e}")

# Optional: ChromaDB (only needed for ChromaDB modes, not Supabase)
CHROMA_AVAILABLE = False
try:
    import chromadb
    from chromadb.config import Settings
    CHROMA_AVAILABLE = True
    print("✅ ChromaDB available")
except Exception as e:
    print(f"⚠️ chromadb not available: {e}")
    print("   Using Supabase or memory fallback.")

load_dotenv()

# ============================================================
# CONFIGURATION
# ============================================================
BREVO_API_KEY   = os.getenv("BREVO_API_KEY", "")
SENDER_EMAIL    = os.getenv("SENDER_EMAIL", "888nv666@gmail.com")
RECIPIENT_EMAIL = "digkasm@proton.me"
DATABASE_URL    = os.getenv("DATABASE_URL")

# 100% FREE AI Providers
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
CLOUDFLARE_API_KEY = os.getenv("CLOUDFLARE_API_KEY", "")
CLOUDFLARE_ACCOUNT_ID = os.getenv("CLOUDFLARE_ACCOUNT_ID", "")

ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")

# ============================================================
# VECTOR DATABASE CONFIGURATION - v9.7
# ============================================================
# Option 1: Chroma Cloud (Managed)
# Sign up at: https://www.trychroma.com
CHROMA_CLOUD_API_KEY = os.getenv("CHROMA_CLOUD_API_KEY", "")  # Your Chroma Cloud API key
CHROMA_TENANT = os.getenv("CHROMA_TENANT", "default")         # Optional: Tenant name
CHROMA_DATABASE = os.getenv("CHROMA_DATABASE", "default")     # Optional: Database name

# Option 2: External ChromaDB Server (Railway, Fly.io, Docker)
CHROMA_SERVER_URL = os.getenv("CHROMA_SERVER_URL", "")  # e.g., "http://your-chroma:8000"
CHROMA_SERVER_AUTH = os.getenv("CHROMA_SERVER_AUTH", "")  # Optional: Basic auth token

# Option 3: Supabase pgvector (FREE 500MB!) - or reuse existing DATABASE_URL
# Note: If SUPABASE_DB_URL contains "${" it means user entered a variable reference, ignore it
_supabase_url = os.getenv("SUPABASE_DB_URL", "")
if _supabase_url and "${" not in _supabase_url:
    SUPABASE_DB_URL = _supabase_url
else:
    SUPABASE_DB_URL = DATABASE_URL  # Falls back to DATABASE_URL
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY", "")  # For REST API (optional)

# Option 4: Local/In-memory ChromaDB (default, ephemeral on Render)
CHROMA_LOCAL_PATH = os.getenv("CHROMA_LOCAL_PATH", "./chroma_db")

# Vector DB Selection
# Options: auto, chroma_cloud, chroma_remote, supabase, local, memory
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
        
        # ============================================================
        # v9.7: Vector Memory Tables (for Supabase pgvector or PostgreSQL)
        # ============================================================
        # Check if pgvector extension is available
        try:
            c.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            print("✅ pgvector extension enabled")
        except:
            print("⚠️ pgvector extension not available (using fallback)")
        
        # Episodic memory table (for pgvector)
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
        
        # Semantic memory table (for pgvector)
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
        
        # Create vector indexes if pgvector is available
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
        
        try:
            c.execute("""
                SELECT data_type FROM information_schema.columns 
                WHERE table_name = 'environment_alerts' AND column_name = 'change_detected'
            """)
            result = c.fetchone()
            if result and result[0] == 'jsonb':
                c.execute("ALTER TABLE environment_alerts ALTER COLUMN change_detected TYPE TEXT USING change_detected::TEXT")
                print("✅ Migration: Changed change_detected from JSONB to TEXT")
        except Exception as e:
            print(f"⚠️ Migration warning (environment_alerts.change_detected): {e}")
        
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
                    'default': 'meta-llama/llama-3.2-3b-instruct:free',
                    'smart': 'meta-llama/llama-3.2-3b-instruct:free',
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
# HYBRID VECTOR MEMORY SYSTEM - v9.7
# ============================================================
class HybridVectorMemory:
    """
    Hybrid memory system supporting multiple backends:
    1. External ChromaDB (Railway, Fly.io, Docker, Chroma Cloud)
    2. Supabase pgvector
    3. Local ChromaDB
    4. In-memory fallback
    """
    
    def __init__(self):
        self.encoder = None
        self.backend_type = "memory"  # Default to in-memory
        self.chroma_client = None
        self.episodic_collection = None
        self.semantic_collection = None
        self.supabase_db_url = SUPABASE_DB_URL
        self.memory_store = {"episodic": [], "semantic": []}  # In-memory fallback
        self.initialized = False
        
        # Initialize encoder
        self._init_encoder()
        
        # Determine and initialize backend
        backend = self._determine_backend()
        self._init_backend(backend)
    
    def _init_encoder(self):
        """Initialize the sentence encoder"""
        if not SENTENCE_TRANSFORMERS_AVAILABLE:
            print("⚠️ Sentence Transformers not available, embeddings disabled")
            self.encoder = None
            return
            
        try:
            from sentence_transformers import SentenceTransformer
            self.encoder = SentenceTransformer('all-MiniLM-L6-v2')
            print("✅ Sentence encoder initialized (all-MiniLM-L6-v2)")
        except Exception as e:
            print(f"⚠️ Encoder init failed: {e}")
            self.encoder = None
    
    def _determine_backend(self) -> str:
        """Determine which backend to use based on config"""
        if VECTOR_DB_TYPE != "auto":
            return VECTOR_DB_TYPE
        
        # Priority: Chroma Cloud > Remote ChromaDB > Supabase > Local ChromaDB > Memory
        if CHROMA_CLOUD_API_KEY:
            return "chroma_cloud"
        elif CHROMA_SERVER_URL:
            return "chroma_remote"
        elif SUPABASE_DB_URL:
            return "supabase"
        elif CHROMA_AVAILABLE:
            return "local"
        else:
            return "memory"
    
    def _init_backend(self, backend: str):
        """Initialize the selected backend"""
        print(f"🗄️ Initializing vector backend: {backend}")
        
        if backend == "chroma_cloud":
            self._init_chroma_cloud()
        elif backend == "chroma_remote":
            self._init_remote_chroma()
        elif backend == "supabase":
            self._init_supabase()
        elif backend == "local":
            self._init_local_chroma()
        else:
            self._init_memory()
    
    def _init_chroma_cloud(self):
        """Initialize Chroma Cloud connection (RECOMMENDED)"""
        try:
            import chromadb
            
            # Chroma Cloud uses CloudClient (new in chromadb 0.4.22+)
            # Documentation: https://docs.trychroma.com/cloud/getting-started
            print(f"   Connecting to Chroma Cloud...")
            
            # Try CloudClient first (newer API)
            try:
                self.chroma_client = chromadb.CloudClient(
                    api_key=CHROMA_CLOUD_API_KEY,
                    tenant=CHROMA_TENANT,
                    database=CHROMA_DATABASE
                )
            except AttributeError:
                # Fallback to HttpClient with Chroma Cloud endpoint
                self.chroma_client = chromadb.HttpClient(
                    host="api.trychroma.com",
                    port=443,
                    headers={"Authorization": f"Bearer {CHROMA_CLOUD_API_KEY}"}
                )
            
            # Test connection by listing collections
            _ = self.chroma_client.list_collections()
            
            # Get or create collections for Sophia
            self.episodic_collection = self.chroma_client.get_or_create_collection(
                name="sophia_episodic", 
                metadata={"hnsw:space": "cosine", "description": "Sophia AI conversation memories"}
            )
            self.semantic_collection = self.chroma_client.get_or_create_collection(
                name="sophia_semantic", 
                metadata={"hnsw:space": "cosine", "description": "Sophia AI semantic facts"}
            )
            
            self.backend_type = "chroma_cloud"
            self.initialized = True
            print("✅ Chroma Cloud connected successfully!")
            print(f"   Episodic collection: {self.episodic_collection.count()} memories")
            print(f"   Semantic collection: {self.semantic_collection.count()} facts")
            
        except Exception as e:
            print(f"⚠️ Chroma Cloud connection failed: {e}")
            print("   Make sure CHROMA_CLOUD_API_KEY is set correctly")
            print("   Sign up at: https://www.trychroma.com")
            print("   Falling back to in-memory storage...")
            self._init_memory()
    
    def _init_remote_chroma(self):
        """Initialize connection to remote ChromaDB server"""
        try:
            import chromadb
            
            # ChromaDB HTTP client
            if CHROMA_SERVER_AUTH:
                self.chroma_client = chromadb.HttpClient(
                    host=CHROMA_SERVER_URL.replace("http://", "").replace("https://", ""),
                    port=8000,
                    credentials=chromadb.Settings(
                        chroma_client_auth_provider="chromadb.auth.basic.BasicAuthClientProvider",
                        chroma_client_auth_credentials=CHROMA_SERVER_AUTH
                    )
                )
            else:
                self.chroma_client = chromadb.HttpClient(
                    host=CHROMA_SERVER_URL.replace("http://", "").replace("https://", ""),
                    port=8000
                )
            
            # Test connection
            self.chroma_client.heartbeat()
            
            # Get or create collections
            self.episodic_collection = self.chroma_client.get_or_create_collection(
                name="sophia_episodic", metadata={"hnsw:space": "cosine"})
            self.semantic_collection = self.chroma_client.get_or_create_collection(
                name="sophia_semantic", metadata={"hnsw:space": "cosine"})
            
            self.backend_type = "chroma_remote"
            self.initialized = True
            print(f"✅ Remote ChromaDB connected: {CHROMA_SERVER_URL}")
            
        except Exception as e:
            print(f"⚠️ Remote ChromaDB connection failed: {e}")
            print("   Falling back to in-memory storage...")
            self._init_memory()
    
    def _init_supabase(self):
        """Initialize Supabase pgvector backend"""
        try:
            # Test connection
            conn = psycopg2.connect(self.supabase_db_url)
            conn.close()
            
            self.backend_type = "supabase"
            self.initialized = True
            print("✅ Supabase pgvector connected")
            
        except Exception as e:
            print(f"⚠️ Supabase connection failed: {e}")
            print("   Falling back to in-memory storage...")
            self._init_memory()
    
    def _init_local_chroma(self):
        """Initialize local ChromaDB"""
        try:
            import chromadb
            from chromadb.config import Settings
            
            self.chroma_client = chromadb.PersistentClient(path=CHROMA_LOCAL_PATH)
            self.episodic_collection = self.chroma_client.get_or_create_collection(
                name="sophia_episodic", metadata={"hnsw:space": "cosine"})
            self.semantic_collection = self.chroma_client.get_or_create_collection(
                name="sophia_semantic", metadata={"hnsw:space": "cosine"})
            
            self.backend_type = "local"
            self.initialized = True
            print(f"✅ Local ChromaDB initialized: {CHROMA_LOCAL_PATH}")
            
        except Exception as e:
            print(f"⚠️ Local ChromaDB init failed: {e}")
            self._init_memory()
    
    def _init_memory(self):
        """Initialize in-memory fallback"""
        self.backend_type = "memory"
        self.initialized = True
        print("✅ In-memory vector storage initialized (ephemeral)")
    
    def encode(self, text: str) -> List[float]:
        """Encode text to embedding vector"""
        if self.encoder:
            return self.encoder.encode(text).tolist()
        return [0.0] * 384  # Fallback
    
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
            
            if self.backend_type in ["chroma_cloud", "chroma_remote", "local"]:
                self.episodic_collection.add(
                    embeddings=[embedding], documents=[text],
                    metadatas=[metadata], ids=[memory_id]
                )
            elif self.backend_type == "supabase":
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
            
            if self.backend_type in ["chroma_cloud", "chroma_remote", "local"]:
                self.semantic_collection.add(
                    embeddings=[embedding], documents=[text],
                    metadatas=[{"fact_type": fact_type, "importance": importance, 
                               "source": source, "timestamp": datetime.now().isoformat()}],
                    ids=[memory_id]
                )
            elif self.backend_type == "supabase":
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
            
            if self.backend_type in ["chroma_cloud", "chroma_remote", "local"]:
                results = self.episodic_collection.query(
                    query_embeddings=[query_embedding], n_results=n_results
                )
                episodes = []
                for i in range(len(results['ids'][0])):
                    episodes.append({
                        'id': results['ids'][0][i],
                        'text': results['documents'][0][i],
                        'metadata': results['metadatas'][0][i]
                    })
                return episodes
                
            elif self.backend_type == "supabase":
                return self._recall_supabase_episodic(query_embedding, n_results)
            else:
                # In-memory similarity search
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
        
        # Calculate similarities
        similarities = []
        for mem in memories:
            sim = self._cosine_similarity(query_embedding, mem.get('embedding', [0.0]*384))
            similarities.append((sim, mem))
        
        # Sort by similarity (descending)
        similarities.sort(key=lambda x: x[0], reverse=True)
        
        return [mem for _, mem in similarities[:n_results]]
    
    def _cosine_similarity(self, a: List[float], b: List[float]) -> float:
        """Calculate cosine similarity between two vectors"""
        import math
        if len(a) != len(b):
            return 0.0
        
        dot_product = sum(x * y for x, y in zip(a, b))
        norm_a = math.sqrt(sum(x * x for x in a))
        norm_b = math.sqrt(sum(x * x for x in b))
        
        if norm_a == 0 or norm_b == 0:
            return 0.0
        return dot_product / (norm_a * norm_b)
    
    def recall_semantic_facts(self, query: str, min_importance: int = 5) -> List[dict]:
        """Recall semantic facts"""
        if not self.initialized:
            return []
        
        try:
            query_embedding = self.encode(query)
            
            if self.backend_type in ["chroma_cloud", "chroma_remote", "local"]:
                results = self.semantic_collection.query(
                    query_embeddings=[query_embedding], n_results=10
                )
                facts = []
                for i in range(len(results['ids'][0])):
                    metadata = results['metadatas'][0][i]
                    if metadata.get('importance', 0) >= min_importance:
                        facts.append({
                            'id': results['ids'][0][i],
                            'text': results['documents'][0][i],
                            'metadata': metadata
                        })
                return facts
                
            elif self.backend_type == "supabase":
                return self._recall_supabase_semantic(query_embedding, min_importance)
            else:
                memories = self._memory_similarity_search("semantic", query_embedding, 10)
                return [m for m in memories if m.get('metadata', {}).get('importance', 0) >= min_importance]
                
        except Exception as e:
            print(f"Semantic recall error: {e}")
            return []
    
    def _recall_supabase_semantic(self, query_embedding: List[float], min_importance: int) -> List[dict]:
        """Recall semantic memories from Supabase pgvector"""
        try:
            conn = psycopg2.connect(self.supabase_db_url)
            c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            c.execute("""
                SELECT id, fact_type, fact_value, embedding <=> %s::vector as distance,
                       importance, source, created_at
                FROM semantic_memories
                WHERE importance >= %s
                ORDER BY embedding <=> %s::vector
                LIMIT 10
            """, (str(query_embedding), min_importance, str(query_embedding)))
            
            results = []
            for row in c.fetchall():
                results.append({
                    'id': row['id'],
                    'text': f"{row['fact_type']}: {row['fact_value']}",
                    'metadata': dict(row)
                })
            conn.close()
            return results
        except Exception as e:
            print(f"Supabase semantic recall error: {e}")
            return []
    
    def get_status(self) -> dict:
        """Get memory system status"""
        status = {
            "backend": self.backend_type,
            "initialized": self.initialized,
            "encoder_available": self.encoder is not None,
        }
        
        if self.backend_type == "chroma_cloud":
            status["provider"] = "Chroma Cloud (trychroma.com)"
            status["tenant"] = CHROMA_TENANT
            status["database"] = CHROMA_DATABASE
            if self.episodic_collection:
                status["episodic_count"] = self.episodic_collection.count()
            if self.semantic_collection:
                status["semantic_count"] = self.semantic_collection.count()
        elif self.backend_type == "chroma_remote":
            status["server_url"] = CHROMA_SERVER_URL
        elif self.backend_type == "supabase":
            status["database"] = "Supabase pgvector"
        elif self.backend_type == "local":
            status["path"] = CHROMA_LOCAL_PATH
        else:
            status["episodic_count"] = len(self.memory_store["episodic"])
            status["semantic_count"] = len(self.memory_store["semantic"])
        
        return status

# Initialize memory system
hybrid_memory = HybridVectorMemory()

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
            'extract_entities': {
                'description': 'Extract entities from text',
                'parameters': {'text': 'string'},
                'handler': self._tool_extract_entities
            },
            'tavily_search': {
                'description': 'AI-powered web search. Best for research, news, facts.',
                'parameters': {'query': 'string', 'search_depth': 'string'},
                'handler': self._tool_tavily_search
            },
            'jina_reader': {
                'description': 'Read any webpage as clean markdown. FREE unlimited.',
                'parameters': {'url': 'string'},
                'handler': self._tool_jina_reader
            },
            'news_monitor': {
                'description': 'Monitor global news on any topic.',
                'parameters': {'topic': 'string'},
                'handler': self._tool_news_monitor
            },
            'indexnow_ping': {
                'description': 'Notify search engines to index a URL. FREE SEO.',
                'parameters': {'url': 'string'},
                'handler': self._tool_indexnow_ping
            },
            'content_writer': {
                'description': 'Generate SEO-optimized content promoting CWC.',
                'parameters': {'topic': 'string', 'content_type': 'string', 'keywords': 'array'},
                'handler': self._tool_content_writer
            },
            'competitor_analysis': {
                'description': 'Analyze competitor website.',
                'parameters': {'competitor_url': 'string'},
                'handler': self._tool_competitor_analysis
            },
            'bing_submit_url': {
                'description': 'Submit URL to Bing for indexing via Webmaster API.',
                'parameters': {'url': 'string', 'site_url': 'string'},
                'handler': self._tool_bing_submit_url
            },
            'bing_get_index_stats': {
                'description': 'Get indexing statistics from Bing Webmaster.',
                'parameters': {'site_url': 'string'},
                'handler': self._tool_bing_get_index_stats
            },
            'bing_get_crawl_stats': {
                'description': 'Get crawl statistics and errors from Bing.',
                'parameters': {'site_url': 'string'},
                'handler': self._tool_bing_get_crawl_stats
            },
            'bing_search': {
                'description': 'Search the web using Bing Search API.',
                'parameters': {'query': 'string', 'count': 'integer'},
                'handler': self._tool_bing_search
            },
            'duckduckgo_search': {
                'description': 'Search the web using DuckDuckGo. 100% FREE, no API key needed.',
                'parameters': {'query': 'string', 'max_results': 'integer'},
                'handler': self._tool_duckduckgo_search
            },
            'reddit_search': {
                'description': 'Search Reddit for discussions about companies, suppliers, or topics. 100% FREE.',
                'parameters': {'query': 'string', 'subreddit': 'string', 'limit': 'integer'},
                'handler': self._tool_reddit_search
            },
            'reddit_get_posts': {
                'description': 'Get recent posts from a specific subreddit. Monitor China business communities.',
                'parameters': {'subreddit': 'string', 'limit': 'integer', 'sort_by': 'string'},
                'handler': self._tool_reddit_get_posts
            },
            'reddit_company_sentiment': {
                'description': 'Analyze Reddit sentiment about a company or supplier. Detect warnings or praise.',
                'parameters': {'company_name': 'string', 'limit': 'integer'},
                'handler': self._tool_reddit_company_sentiment
            },
            'geocode_address': {
                'description': 'Verify and geocode an address. Check if Chinese supplier address is real.',
                'parameters': {'address': 'string'},
                'handler': self._tool_geocode_address
            },
            'reverse_geocode': {
                'description': 'Get address from coordinates. Verify factory locations.',
                'parameters': {'lat': 'float', 'lon': 'float'},
                'handler': self._tool_reverse_geocode
            },
            'zenrows_scrape': {
                'description': 'Scrape any website with anti-bot bypass. Great for Alibaba, 1688, supplier sites.',
                'parameters': {'url': 'string', 'css_extractor': 'string'},
                'handler': self._tool_zenrows_scrape
            },
            'scrape_chinese_supplier': {
                'description': 'Scrape Chinese B2B platforms (Alibaba, 1688, Made-in-China) for supplier data.',
                'parameters': {'url': 'string', 'platform': 'string'},
                'handler': self._tool_scrape_chinese_supplier
            },
            # v9.7: Memory management tools
            'memory_status': {
                'description': 'Get the status of the vector memory system (ChromaDB/Supabase/Memory).',
                'parameters': {},
                'handler': self._tool_memory_status
            },
            'store_memory': {
                'description': 'Store a fact or memory in the vector database.',
                'parameters': {'fact_type': 'string', 'fact_value': 'string', 'importance': 'integer'},
                'handler': self._tool_store_memory
            },
            'recall_memories': {
                'description': 'Recall memories similar to a query from the vector database.',
                'parameters': {'query': 'string', 'n_results': 'integer'},
                'handler': self._tool_recall_memories
            }
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
            except Exception as e:
                if "does not exist" in str(e):
                    c.execute("SELECT tool_name, description, implementation FROM tool_registry WHERE deployed = TRUE")
                    for name, desc, impl in c.fetchall():
                        self.tools[name] = {
                            'description': desc,
                            'parameters': {},
                            'implementation': impl
                        }
                else:
                    raise
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
            
            self._update_tool_stats(tool_name, success=True)
            return {'success': True, 'result': result}
        except Exception as e:
            self._update_tool_stats(tool_name, success=False)
            return {'success': False, 'error': str(e)}
    
    def _execute_custom(self, tool: dict, params: dict) -> str:
        """Execute custom tool implementation"""
        impl = tool.get('implementation', '')
        locals_dict = {'params': params, 'result': ''}
        exec(impl, {}, locals_dict)
        return locals_dict.get('result', 'Executed')
    
    def _update_tool_stats(self, tool_name: str, success: bool):
        """Update tool success statistics"""
        try:
            conn = get_db()
            c = conn.cursor()
            c.execute("""
                UPDATE tool_registry 
                SET use_count = use_count + 1,
                    success_rate = (success_rate * use_count + %s) / (use_count + 1)
                WHERE tool_name = %s
            """, (1.0 if success else 0.0, tool_name))
            conn.commit()
            conn.close()
        except:
            pass
    
    def get_tools_schema(self) -> List[dict]:
        """Get OpenAI-style tools schema"""
        schema = []
        for name, tool in self.tools.items():
            params = tool.get('parameters', {})
            properties = {}
            required = []
            
            for pname, ptype in params.items():
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
        """Fallback web search using DuckDuckGo"""
        query = params.get('query', '')
        try:
            # Use DuckDuckGo HTML search (no API key needed)
            response = requests.get(
                "https://html.duckduckgo.com/html/",
                params={"q": query},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=15
            )
            # Simple extraction of results
            import re
            results = re.findall(r'<a[^>]*class="result__a"[^>]*>([^<]+)</a>', response.text)
            return f"Found {len(results)} results for '{query}':\n" + "\n".join(results[:5])
        except Exception as e:
            return f"Search error: {e}"
    
    async def _tool_calculate_risk(self, params: dict) -> str:
        """Calculate risk score"""
        company = params.get('company_name', 'Unknown')
        context = params.get('context', {})
        
        # Base risk score
        risk = 50
        
        # Adjust based on context
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
        format_type = params.get('format', 'text')
        
        # Use AI to generate report
        messages = [
            {"role": "system", "content": "You are a business report generator. Create concise, professional reports."},
            {"role": "user", "content": f"Generate a brief report on: {topic}"}
        ]
        
        result = await ai_provider.chat_completion(messages, max_tokens=500)
        return result['choices'][0]['message']['content']
    
    async def _tool_send_notification(self, params: dict) -> str:
        """Send notification to user"""
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
        """Schedule a follow-up action"""
        session_id = params.get('session_id', '')
        delay_hours = params.get('delay_hours', 24)
        action = params.get('action', '')
        
        return f"Scheduled follow-up for session {session_id} in {delay_hours} hours: {action}"
    
    async def _tool_analyze_sentiment(self, params: dict) -> str:
        """Analyze sentiment of text"""
        text = params.get('text', '')
        
        # Simple keyword-based sentiment
        positive_words = ['good', 'great', 'excellent', 'happy', 'love', 'best', 'amazing']
        negative_words = ['bad', 'terrible', 'awful', 'hate', 'worst', 'poor', 'disappointed']
        
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
    
    async def _tool_extract_entities(self, params: dict) -> str:
        """Extract entities from text"""
        text = params.get('text', '')
        
        # Simple regex-based extraction
        import re
        emails = re.findall(r'[\w.-]+@[\w.-]+\.\w+', text)
        phones = re.findall(r'\+?[\d\s-]{10,}', text)
        urls = re.findall(r'https?://[^\s]+', text)
        
        return f"Emails: {emails}\nPhones: {phones}\nURLs: {urls}"
    
    async def _tool_tavily_search(self, params: dict) -> str:
        """AI-powered search via Tavily"""
        query = params.get('query', '')
        search_depth = params.get('search_depth', 'basic')
        
        if not TAVILY_API_KEY:
            return "Tavily API key not configured. Use duckduckgo_search instead."
        
        try:
            response = requests.post(
                "https://api.tavily.com/search",
                headers={"Authorization": f"Bearer {TAVILY_API_KEY}"},
                json={"query": query, "search_depth": search_depth},
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                results = []
                for r in data.get('results', [])[:5]:
                    results.append(f"- {r.get('title', 'No title')}: {r.get('url', '')}")
                return f"Tavily results for '{query}':\n" + "\n".join(results)
            else:
                return f"Tavily error: {response.status_code}"
        except Exception as e:
            return f"Tavily search failed: {e}"
    
    async def _tool_jina_reader(self, params: dict) -> str:
        """Read webpage as markdown using Jina Reader"""
        url = params.get('url', '')
        
        try:
            # Jina Reader is FREE and doesn't need API key for basic usage
            response = requests.get(
                f"https://r.jina.ai/{url}",
                headers={"User-Agent": "SophiaAI/1.0"},
                timeout=30
            )
            
            if response.status_code == 200:
                content = response.text[:3000]  # Limit to 3000 chars
                return f"Content from {url}:\n\n{content}"
            else:
                return f"Jina Reader error: {response.status_code}"
        except Exception as e:
            return f"Failed to read webpage: {e}"
    
    async def _tool_news_monitor(self, params: dict) -> str:
        """Monitor news on a topic"""
        topic = params.get('topic', 'China business')
        
        # Check cache first (30-minute expiry)
        try:
            conn = get_db()
            c = conn.cursor()
            c.execute("""
                SELECT news_data, fetched_at FROM news_cache 
                WHERE topic = %s AND expires_at > NOW()
                ORDER BY fetched_at DESC LIMIT 1
            """, (topic,))
            cached = c.fetchone()
            
            if cached:
                conn.close()
                news_data = cached[0]
                return f"📰 Cached news for '{topic}' (fetched {cached[1]}):\n" + json.dumps(news_data, indent=2)[:2000]
            
            conn.close()
        except:
            pass
        
        # Fetch fresh news
        if NEWS_API_KEY:
            try:
                response = requests.get(
                    "https://newsapi.org/v2/everything",
                    params={"q": topic, "apiKey": NEWS_API_KEY, "pageSize": 5, 
                           "sortBy": "publishedAt", "language": "en"},
                    timeout=15
                )
                
                if response.status_code == 200:
                    data = response.json()
                    articles = data.get('articles', [])
                    
                    # Cache the results
                    try:
                        conn = get_db()
                        c = conn.cursor()
                        c.execute("""
                            INSERT INTO news_cache (topic, news_data, source, expires_at)
                            VALUES (%s, %s::jsonb, 'newsapi', NOW() + INTERVAL '30 minutes')
                        """, (topic, json.dumps(articles)))
                        conn.commit()
                        conn.close()
                    except:
                        pass
                    
                    results = []
                    for a in articles:
                        results.append(f"- {a.get('title', 'No title')} ({a.get('source', {}).get('name', 'Unknown')})")
                    return f"📰 Latest news for '{topic}':\n" + "\n".join(results)
            except Exception as e:
                return f"News API error: {e}"
        
        # Fallback to DuckDuckGo
        return await self._tool_duckduckgo_search({'query': f'{topic} news', 'max_results': 5})
    
    async def _tool_indexnow_ping(self, params: dict) -> str:
        """Submit URL to search engines via IndexNow"""
        url = params.get('url', '')
        
        if not INDEXNOW_KEY:
            return "IndexNow key not configured. Get one at indexnow.org"
        
        try:
            # IndexNow is FREE and instant
            response = requests.get(
                f"https://www.bing.com/indexnow",
                params={"url": url, "key": INDEXNOW_KEY},
                timeout=10
            )
            
            if response.status_code == 200:
                return f"✅ URL submitted to search engines: {url}"
            else:
                return f"IndexNow response: {response.status_code}"
        except Exception as e:
            return f"IndexNow failed: {e}"
    
    async def _tool_content_writer(self, params: dict) -> str:
        """Generate SEO-optimized content"""
        topic = params.get('topic', '')
        content_type = params.get('content_type', 'blog')
        keywords = params.get('keywords', [])
        
        prompt = f"""Write a {content_type} about "{topic}" promoting China West Connector (CWC).
CWC helps businesses connect with reliable Chinese suppliers for manufacturing, logistics, and sourcing.

Keywords to include: {', '.join(keywords) if keywords else 'China sourcing, supplier verification, manufacturing'}

Keep it professional, informative, and 300-500 words."""
        
        messages = [
            {"role": "system", "content": "You are a professional content writer specializing in B2B content about China sourcing and manufacturing."},
            {"role": "user", "content": prompt}
        ]
        
        result = await ai_provider.chat_completion(messages, max_tokens=800)
        return result['choices'][0]['message']['content']
    
    async def _tool_competitor_analysis(self, params: dict) -> str:
        """Analyze competitor website"""
        url = params.get('competitor_url', '')
        
        # Use Jina Reader to get content
        content = await self._tool_jina_reader({'url': url})
        
        # Use AI to analyze
        messages = [
            {"role": "system", "content": "You are a competitive intelligence analyst. Analyze websites for business insights."},
            {"role": "user", "content": f"Analyze this competitor content and identify their strengths, weaknesses, and unique selling points:\n\n{content[:2000]}"}
        ]
        
        result = await ai_provider.chat_completion(messages, max_tokens=500)
        return result['choices'][0]['message']['content']
    
    # Bing Webmaster Tools
    async def _tool_bing_submit_url(self, params: dict) -> str:
        """Submit URL to Bing via Webmaster API"""
        url = params.get('url', '')
        site_url = params.get('site_url', 'https://chinawestconnector.com')
        
        if not BING_WEBMASTER_API_KEY:
            return "Bing Webmaster API key not configured"
        
        try:
            response = requests.post(
                f"https://ssl.bing.com/webmaster/api.svc/json/SubmitUrl",
                params={"apikey": BING_WEBMASTER_API_KEY, "siteUrl": site_url, "url": url},
                timeout=15
            )
            
            if response.status_code == 200:
                return f"✅ URL submitted to Bing: {url}"
            else:
                return f"Bing API error: {response.status_code}"
        except Exception as e:
            return f"Bing submit failed: {e}"
    
    async def _tool_bing_get_index_stats(self, params: dict) -> str:
        """Get Bing indexing statistics"""
        site_url = params.get('site_url', 'https://chinawestconnector.com')
        
        if not BING_WEBMASTER_API_KEY:
            return "Bing Webmaster API key not configured"
        
        try:
            response = requests.get(
                f"https://ssl.bing.com/webmaster/api.svc/json/GetIndexStats",
                params={"apikey": BING_WEBMASTER_API_KEY, "siteUrl": site_url},
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                return f"Bing Index Stats for {site_url}:\n{json.dumps(data, indent=2)}"
            else:
                return f"Bing API error: {response.status_code}"
        except Exception as e:
            return f"Bing stats failed: {e}"
    
    async def _tool_bing_get_crawl_stats(self, params: dict) -> str:
        """Get Bing crawl statistics"""
        site_url = params.get('site_url', 'https://chinawestconnector.com')
        
        if not BING_WEBMASTER_API_KEY:
            return "Bing Webmaster API key not configured"
        
        try:
            response = requests.get(
                f"https://ssl.bing.com/webmaster/api.svc/json/GetCrawlStats",
                params={"apikey": BING_WEBMASTER_API_KEY, "siteUrl": site_url},
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                return f"Bing Crawl Stats for {site_url}:\n{json.dumps(data, indent=2)}"
            else:
                return f"Bing API error: {response.status_code}"
        except Exception as e:
            return f"Bing crawl stats failed: {e}"
    
    async def _tool_bing_search(self, params: dict) -> str:
        """Search using Bing Search API"""
        query = params.get('query', '')
        count = params.get('count', 5)
        
        if not BING_SEARCH_API_KEY:
            return "Bing Search API key not configured. Use duckduckgo_search instead."
        
        try:
            response = requests.get(
                "https://api.bing.microsoft.com/v7.0/search",
                headers={"Ocp-Apim-Subscription-Key": BING_SEARCH_API_KEY},
                params={"q": query, "count": count},
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                results = []
                for r in data.get('webPages', {}).get('value', []):
                    results.append(f"- {r.get('name', 'No title')}: {r.get('url', '')}")
                return f"Bing results for '{query}':\n" + "\n".join(results)
            else:
                return f"Bing Search error: {response.status_code}"
        except Exception as e:
            return f"Bing search failed: {e}"
    
    async def _tool_duckduckgo_search(self, params: dict) -> str:
        """Search using DuckDuckGo (100% FREE, no API key)"""
        query = params.get('query', '')
        max_results = params.get('max_results', 5)
        
        try:
            # DuckDuckGo Instant Answer API
            response = requests.get(
                "https://api.duckduckgo.com/",
                params={"q": query, "format": "json", "no_html": 1},
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                results = []
                
                # Get abstract
                if data.get('Abstract'):
                    results.append(f"Summary: {data['Abstract'][:500]}")
                
                # Get related topics
                for topic in data.get('RelatedTopics', [])[:max_results]:
                    if isinstance(topic, dict) and 'Text' in topic:
                        results.append(f"- {topic['Text'][:200]}")
                
                if results:
                    return f"DuckDuckGo results for '{query}':\n" + "\n".join(results)
                else:
                    # Fallback to HTML search
                    return await self._tool_search_web({'query': query})
            else:
                return f"DuckDuckGo error: {response.status_code}"
        except Exception as e:
            return f"DuckDuckGo search failed: {e}"
    
    # Reddit Tools
    async def _tool_reddit_search(self, params: dict) -> str:
        """Search Reddit (100% FREE)"""
        query = params.get('query', '')
        subreddit = params.get('subreddit', 'all')
        limit = params.get('limit', 5)
        
        try:
            # Reddit JSON API (no auth needed for basic search)
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
                return f"Reddit results for '{query}' in r/{subreddit}:\n" + "\n".join(results)
            else:
                return f"Reddit error: {response.status_code}"
        except Exception as e:
            return f"Reddit search failed: {e}"
    
    async def _tool_reddit_get_posts(self, params: dict) -> str:
        """Get posts from a subreddit"""
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
                    results.append(f"- [{p.get('score', 0)}↑] {p.get('title', 'No title')}\n  {p.get('selftext', '')[:100]}...")
                return f"r/{subreddit} posts ({sort_by}):\n" + "\n\n".join(results)
            else:
                return f"Reddit error: {response.status_code}"
        except Exception as e:
            return f"Reddit get posts failed: {e}"
    
    async def _tool_reddit_company_sentiment(self, params: dict) -> str:
        """Analyze Reddit sentiment about a company"""
        company = params.get('company_name', '')
        limit = params.get('limit', 10)
        
        # Search across business subreddits
        subreddits = ['ChinaSourcing', 'importexport', 'Entrepreneur', 'smallbusiness']
        all_posts = []
        
        for sub in subreddits:
            try:
                url = f"https://www.reddit.com/r/{sub}/search.json"
                response = requests.get(
                    url,
                    params={"q": company, "restrict_sr": 1, "limit": limit // len(subreddits)},
                    headers={"User-Agent": REDDIT_USER_AGENT},
                    timeout=10
                )
                if response.status_code == 200:
                    data = response.json()
                    for post in data['data']['children']:
                        all_posts.append(post['data'])
            except:
                continue
        
        if not all_posts:
            return f"No Reddit mentions found for '{company}'"
        
        # Analyze sentiment
        total_score = sum(p.get('score', 0) for p in all_posts)
        avg_score = total_score / len(all_posts) if all_posts else 0
        
        # Simple sentiment based on title/content
        positive_words = ['great', 'good', 'reliable', 'excellent', 'recommend', 'best']
        negative_words = ['scam', 'bad', 'avoid', 'terrible', 'fraud', 'warning', 'scammed']
        
        pos_mentions = 0
        neg_mentions = 0
        
        for p in all_posts:
            text = (p.get('title', '') + ' ' + p.get('selftext', '')).lower()
            if any(w in text for w in positive_words):
                pos_mentions += 1
            if any(w in text for w in negative_words):
                neg_mentions += 1
        
        if neg_mentions > pos_mentions:
            sentiment = "⚠️ NEGATIVE"
        elif pos_mentions > neg_mentions:
            sentiment = "✅ POSITIVE"
        else:
            sentiment = "➖ NEUTRAL"
        
        return f"Reddit sentiment for '{company}': {sentiment}\nPosts found: {len(all_posts)}\nAvg upvotes: {avg_score:.1f}\nPositive mentions: {pos_mentions}\nNegative mentions: {neg_mentions}"
    
    # Nominatim Geocoding Tools
    async def _tool_geocode_address(self, params: dict) -> str:
        """Geocode an address using Nominatim (100% FREE)"""
        address = params.get('address', '')
        
        try:
            response = requests.get(
                "https://nominatim.openstreetmap.org/search",
                params={"q": address, "format": "json", "limit": 1},
                headers={"User-Agent": "SophiaAI/1.0 China West Connector"},
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                if data:
                    r = data[0]
                    return f"Address verified: {r.get('display_name', 'Unknown')}\nCoordinates: {r.get('lat', '')}, {r.get('lon', '')}\nType: {r.get('type', 'unknown')}"
                else:
                    return f"Address not found: {address}"
            else:
                return f"Geocoding error: {response.status_code}"
        except Exception as e:
            return f"Geocoding failed: {e}"
    
    async def _tool_reverse_geocode(self, params: dict) -> str:
        """Reverse geocode coordinates"""
        lat = params.get('lat', 0)
        lon = params.get('lon', 0)
        
        try:
            response = requests.get(
                "https://nominatim.openstreetmap.org/reverse",
                params={"lat": lat, "lon": lon, "format": "json"},
                headers={"User-Agent": "SophiaAI/1.0 China West Connector"},
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                return f"Location: {data.get('display_name', 'Unknown')}\nAddress: {data.get('address', {})}"
            else:
                return f"Reverse geocoding error: {response.status_code}"
        except Exception as e:
            return f"Reverse geocoding failed: {e}"
    
    # ZenRows Scraping Tools
    async def _tool_zenrows_scrape(self, params: dict) -> str:
        """Scrape website using ZenRows"""
        url = params.get('url', '')
        css_extractor = params.get('css_extractor', '')
        
        if not ZENROWS_API_KEY:
            return "ZenRows API key not configured. Use jina_reader for basic scraping."
        
        try:
            response = requests.get(
                "https://api.zenrows.com/v1/",
                params={
                    "url": url,
                    "apikey": ZENROWS_API_KEY,
                    "css_extractor": css_extractor
                },
                timeout=30
            )
            
            if response.status_code == 200:
                return response.text[:3000]
            else:
                return f"ZenRows error: {response.status_code}"
        except Exception as e:
            return f"ZenRows scrape failed: {e}"
    
    async def _tool_scrape_chinese_supplier(self, params: dict) -> str:
        """Scrape Chinese B2B platforms"""
        url = params.get('url', '')
        platform = params.get('platform', 'alibaba')
        
        if ZENROWS_API_KEY:
            return await self._tool_zenrows_scrape({'url': url})
        else:
            return await self._tool_jina_reader({'url': url})
    
    # v9.7: Memory Management Tools
    async def _tool_memory_status(self, params: dict) -> str:
        """Get vector memory system status"""
        status = hybrid_memory.get_status()
        return f"🧠 Vector Memory Status:\n" + "\n".join(f"- {k}: {v}" for k, v in status.items())
    
    async def _tool_store_memory(self, params: dict) -> str:
        """Store a fact in vector memory"""
        fact_type = params.get('fact_type', 'general')
        fact_value = params.get('fact_value', '')
        importance = params.get('importance', 5)
        
        hybrid_memory.store_semantic(fact_type, fact_value, importance, 'user')
        return f"✅ Memory stored: {fact_type} = {fact_value} (importance: {importance})"
    
    async def _tool_recall_memories(self, params: dict) -> str:
        """Recall similar memories"""
        query = params.get('query', '')
        n_results = params.get('n_results', 5)
        
        episodes = hybrid_memory.recall_similar_episodes(query, n_results)
        facts = hybrid_memory.recall_semantic_facts(query)
        
        result = f"🧠 Memories similar to '{query}':\n\n"
        
        if episodes:
            result += "Episodic Memories:\n"
            for ep in episodes:
                result += f"- {ep['text'][:200]}...\n"
        
        if facts:
            result += "\nSemantic Facts:\n"
            for f in facts:
                result += f"- {f['text']}\n"
        
        return result or "No similar memories found."

tool_registry = ToolRegistry()

# ============================================================
# INTELLIGENCE ENGINE - v9.6/v9.7
# ============================================================
class IntelligenceEngine:
    """Core intelligence features: Tool Chaining, Self-Reflection, ReAct"""
    
    def __init__(self):
        self.tool_registry = tool_registry
        self.ai_provider = ai_provider
    
    async def think_act_observe(self, user_message: str, context: dict) -> Tuple[str, List[str]]:
        """ReAct reasoning: Think -> Act -> Observe cycle"""
        thoughts = []
        tools_used = []
        
        # Think: Analyze the request
        think_prompt = f"""Analyze this user request and determine the best approach:

User request: {user_message}
Context: {json.dumps(context, indent=2)[:500]}

Think step by step:
1. What is the user asking for?
2. What information do I need?
3. What tools should I use?
4. What is my reasoning?

Keep your analysis concise (3-5 sentences)."""
        
        messages = [
            {"role": "system", "content": "You are Sophia, an intelligent AI assistant. Think carefully before acting."},
            {"role": "user", "content": think_prompt}
        ]
        
        result = await self.ai_provider.chat_completion(messages, max_tokens=300, temperature=0.3)
        thinking = result['choices'][0]['message']['content']
        thoughts.append(f"Thinking: {thinking}")
        
        return thinking, tools_used
    
    async def chain_tools(self, initial_query: str, tools_sequence: List[str], params_list: List[dict]) -> Tuple[str, List[str]]:
        """Execute a chain of tools sequentially"""
        results = []
        tools_used = []
        current_context = initial_query
        
        for i, (tool_name, params) in enumerate(zip(tools_sequence, params_list)):
            if i >= MAX_TOOL_CHAIN_DEPTH:
                break
            
            # Execute tool
            result = await self.tool_registry.execute(tool_name, params)
            
            if result['success']:
                results.append(f"Tool {i+1} ({tool_name}): {str(result['result'])[:500]}")
                tools_used.append(tool_name)
                
                # Use result as context for next tool
                if i < len(tools_sequence) - 1:
                    current_context = str(result['result'])
            else:
                results.append(f"Tool {i+1} ({tool_name}): FAILED - {result['error']}")
                break
        
        return "\n".join(results), tools_used
    
    async def self_reflect(self, response: str, user_message: str) -> Tuple[str, float]:
        """Reflect on response quality and improve if needed"""
        
        # Skip reflection if response is very short
        if len(response) < 100:
            return response, 0.5
        
        reflect_prompt = f"""Review this AI response for quality:

User asked: {user_message}
AI responded: {response}

Rate the response quality (0-1) and suggest improvements if needed.
Format: SCORE: [number]
IMPROVEMENT: [improved response or "Good enough"]"""
        
        messages = [
            {"role": "system", "content": "You are a quality assurance reviewer for AI responses."},
            {"role": "user", "content": reflect_prompt}
        ]
        
        result = await self.ai_provider.chat_completion(messages, max_tokens=500, temperature=0.2)
        reflection = result['choices'][0]['message']['content']
        
        # Parse score
        import re
        score_match = re.search(r'SCORE:\s*([\d.]+)', reflection)
        score = float(score_match.group(1)) if score_match else 0.7
        
        # Parse improvement
        improvement_match = re.search(r'IMPROVEMENT:\s*(.+)', reflection, re.DOTALL)
        improved_response = improvement_match.group(1).strip() if improvement_match else response
        
        if score < REFLECTION_THRESHOLD:
            return improved_response, score
        return response, score

intelligence_engine = IntelligenceEngine()

# ============================================================
# SOPHIA MAIN CLASS
# ============================================================
class SophiaAgent:
    """Main Sophia AI Agent"""
    
    def __init__(self):
        self.tool_registry = tool_registry
        self.ai_provider = ai_provider
        self.memory = hybrid_memory
        self.intelligence = intelligence_engine
    
    async def process_message(self, session_id: str, user_message: str, 
                              context: dict = None) -> Tuple[str, dict]:
        """Process user message and generate response"""
        
        context = context or {}
        profile = get_or_create_user_profile(session_id)
        
        # Recall relevant memories
        past_episodes = self.memory.recall_similar_episodes(user_message, n_results=3)
        relevant_facts = self.memory.recall_semantic_facts(user_message)
        
        # Build system prompt
        system_prompt = self._build_system_prompt(profile, past_episodes, relevant_facts)
        
        # Build messages
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message}
        ]
        
        # Get available tools
        tools_schema = self.tool_registry.get_tools_schema()
        
        # Call AI with tools
        response = await self.ai_provider.chat_completion(
            messages, 
            tools=tools_schema, 
            tool_choice="auto",
            max_tokens=1000
        )
        
        assistant_message = response['choices'][0]['message']
        tools_used = []
        
        # Handle tool calls
        if assistant_message.get('tool_calls'):
            for tool_call in assistant_message['tool_calls']:
                tool_name = tool_call['function']['name']
                tool_args = json.loads(tool_call['function']['arguments'])
                
                result = await self.tool_registry.execute(tool_name, tool_args)
                tools_used.append(tool_name)
                
                # Add tool result to conversation
                messages.append(assistant_message)
                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call['id'],
                    "content": json.dumps(result)
                })
            
            # Get final response after tool execution
            response = await self.ai_provider.chat_completion(messages, max_tokens=1000)
            assistant_message = response['choices'][0]['message']
        
        final_response = assistant_message.get('content', 'I apologize, but I could not generate a response.')
        
        # Self-reflection
        if ENABLE_SELF_REFLECTION and len(final_response) > 100:
            final_response, confidence = await self.intelligence.self_reflect(final_response, user_message)
        else:
            confidence = 0.7
        
        # Store episodic memory
        self.memory.store_episodic(
            session_id, user_message, final_response,
            success_score=int(confidence * 10),
            intent=context.get('intent', 'unknown')
        )
        
        # Update profile
        update_user_profile(session_id, last_intent=context.get('intent'))
        
        # Store conversation
        self._store_conversation(session_id, user_message, final_response, context, tools_used, confidence)
        
        return final_response, {'tools_used': tools_used, 'confidence': confidence}
    
    def _build_system_prompt(self, profile: dict, past_episodes: List[dict], 
                             relevant_facts: List[dict]) -> str:
        """Build the system prompt with context"""
        
        base_prompt = """You are Sophia, an intelligent AI assistant for China West Connector (CWC).
CWC helps businesses connect with reliable Chinese suppliers for manufacturing, logistics, and sourcing.

Your capabilities:
- Search the web for supplier information (DuckDuckGo, Tavily, Bing)
- Monitor news about China business and trade
- Analyze Reddit for supplier discussions and warnings
- Verify supplier addresses using geocoding
- Scrape Alibaba, 1688, and other B2B platforms
- Submit URLs to search engines for SEO
- Generate business reports and content

Always be helpful, professional, and accurate. If you don't know something, say so.
Use tools when they would help answer the user's question more accurately."""

        # Add memory context
        memory_context = ""
        if past_episodes:
            memory_context += "\n\nRelevant past conversations:\n"
            for ep in past_episodes[:2]:
                memory_context += f"- {ep['text'][:200]}...\n"
        
        if relevant_facts:
            memory_context += "\n\nKnown facts:\n"
            for fact in relevant_facts[:3]:
                memory_context += f"- {fact['text']}\n"
        
        # Add profile context
        profile_context = ""
        if profile:
            interests = []
            if profile.get('region_interest'):
                interests.append(f"Interested in region: {profile['region_interest']}")
            if profile.get('sector_interest'):
                interests.append(f"Interested in sector: {profile['sector_interest']}")
            if interests:
                profile_context = "\n\nUser context: " + ", ".join(interests)
        
        return base_prompt + memory_context + profile_context
    
    def _store_conversation(self, session_id: str, user_message: str, response: str,
                           context: dict, tools_used: List[str], confidence: float):
        """Store conversation in database"""
        try:
            conn = get_db()
            c = conn.cursor()
            c.execute("""
                INSERT INTO conversations 
                (session_id, user_message, ai_response, intent, tools_used, confidence_score)
                VALUES (%s, %s, %s, %s, %s::jsonb, %s)
            """, (session_id, user_message, response, context.get('intent'),
                  json.dumps(tools_used), confidence))
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Conversation storage error: {e}")

sophia = SophiaAgent()

# ============================================================
# BACKGROUND WORKERS
# ============================================================
def goal_executor():
    """Background thread for autonomous goal execution"""
    while True:
        try:
            time.sleep(GOAL_EXECUTION_INTERVAL_MINUTES * 60)
            # Process pending goals
            conn = get_db()
            c = conn.cursor()
            c.execute("""
                SELECT id, goal_type, goal_description FROM autonomous_goals 
                WHERE status = 'pending' AND priority >= 5
                ORDER BY priority DESC, created_at ASC
                LIMIT %s
            """, (MAX_CONCURRENT_GOALS,))
            goals = c.fetchall()
            
            for goal_id, goal_type, description in goals:
                try:
                    c.execute("UPDATE autonomous_goals SET status = 'in_progress', started_at = NOW() WHERE id = %s", (goal_id,))
                    conn.commit()
                    
                    # Process goal (simplified)
                    result = f"Processed: {description[:100]}"
                    
                    c.execute("""
                        UPDATE autonomous_goals SET status = 'completed', result = %s, completed_at = NOW()
                        WHERE id = %s
                    """, (result, goal_id))
                    conn.commit()
                except Exception as e:
                    c.execute("UPDATE autonomous_goals SET status = 'failed', result = %s WHERE id = %s", (str(e), goal_id))
                    conn.commit()
            
            conn.close()
        except Exception as e:
            print(f"Goal executor error: {e}")

def environment_monitor():
    """Background thread for environment monitoring"""
    while True:
        try:
            time.sleep(ENVIRONMENT_CHECK_INTERVAL_HOURS * 3600)
            # Check for changes in news, market conditions, etc.
            # Store alerts in environment_alerts table
        except Exception as e:
            print(f"Environment monitor error: {e}")

# ============================================================
# FASTAPI APP
# ============================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    # Startup
    init_db()
    
    # Start background threads
    goal_thread = threading.Thread(target=goal_executor, daemon=True)
    goal_thread.start()
    
    env_thread = threading.Thread(target=environment_monitor, daemon=True)
    env_thread.start()
    
    print(f"""
    ╔══════════════════════════════════════════════════════════════╗
    ║            SOPHIA AI SERVER v9.7 - EXTERNAL VECTOR DB        ║
    ╠══════════════════════════════════════════════════════════════╣
    ║  🧠 Vector Backend: {hybrid_memory.backend_type:<42} ║
    ║  🔧 Tools Loaded: {len(tool_registry.tools):<42} ║
    ║  🤖 AI Providers: {len(ai_provider.providers):<42} ║
    ╚══════════════════════════════════════════════════════════════╝
    """)
    
    yield
    
    # Shutdown
    print("🛑 Sophia AI Server shutting down...")

app = FastAPI(
    title="Sophia AI Server v9.7",
    description="Intelligent AI Agent with External Vector DB Support",
    version="9.7.0",
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
    confidence: float = 0.0

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "Sophia AI Server",
        "version": "9.7.0",
        "vector_backend": hybrid_memory.backend_type,
        "tools_count": len(tool_registry.tools),
        "status": "running"
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    memory_status = hybrid_memory.get_status()
    return {
        "status": "healthy",
        "vector_backend": memory_status,
        "ai_providers": len(ai_provider.providers),
        "tools_available": len(tool_registry.tools)
    }

@app.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """Main chat endpoint"""
    response, metadata = await sophia.process_message(
        request.session_id,
        request.message,
        request.context
    )
    return ChatResponse(
        response=response,
        tools_used=metadata.get('tools_used', []),
        confidence=metadata.get('confidence', 0.0)
    )

@app.get("/memory/status")
async def get_memory_status():
    """Get vector memory status"""
    return hybrid_memory.get_status()

@app.post("/memory/store")
async def store_memory(fact_type: str, fact_value: str, importance: int = 5):
    """Manually store a memory"""
    hybrid_memory.store_semantic(fact_type, fact_value, importance, 'manual')
    return {"status": "stored", "fact_type": fact_type}

@app.get("/memory/recall")
async def recall_memory(query: str, n_results: int = 5):
    """Recall similar memories"""
    episodes = hybrid_memory.recall_similar_episodes(query, n_results)
    facts = hybrid_memory.recall_semantic_facts(query)
    return {
        "episodes": episodes,
        "facts": facts
    }

@app.get("/tools")
async def list_tools():
    """List all available tools"""
    return {
        "count": len(tool_registry.tools),
        "tools": [{"name": k, "description": v.get('description', '')} 
                  for k, v in tool_registry.tools.items()]
    }

@app.post("/tools/{tool_name}/execute")
async def execute_tool(tool_name: str, params: dict):
    """Execute a specific tool"""
    result = await tool_registry.execute(tool_name, params)
    return result

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
        
        c.execute("SELECT COUNT(*) FROM autonomous_goals WHERE status = 'pending'")
        pending_goals = c.fetchone()[0]
        
        conn.close()
        
        return {
            "conversations": conversation_count,
            "users": user_count,
            "pending_goals": pending_goals,
            "vector_backend": hybrid_memory.backend_type,
            "ai_provider": ai_provider.get_current_provider()['name'] if ai_provider.providers else 'none'
        }
    except Exception as e:
        return {"error": str(e)}

@app.post("/admin/clear-cache")
async def clear_cache(password: str):
    """Clear news cache"""
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid password")
    
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("DELETE FROM news_cache")
        deleted = c.rowcount
        conn.commit()
        conn.close()
        return {"status": "cache cleared", "entries_deleted": deleted}
    except Exception as e:
        return {"error": str(e)}

# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
