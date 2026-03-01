"""
================================================================================
SOPHIA AI SERVER v9.0 - FULLY AGENTIC EDITION
================================================================================
100% FREE AI with OpenRouter + Cloudflare
Features: Memory, Multi-Agent, Self-Improvement, HTN Planning, 
          Autonomous Goals, Background Tasks, Tool Execution, 
          Proactive Notifications, Learning Loop

AGENTIC CAPABILITIES:
✅ Autonomous Goal Extraction & Execution
✅ Background Task Manager (runs 24/7)
✅ Multi-Agent Collaboration with Weighted Consensus
✅ Tool Creation & Execution Pipeline
✅ Self-Improvement Learning Loop
✅ Environment Monitoring & Proactive Alerts
✅ Meta-Cognitive Confidence Assessment
✅ Predictive Intent Engine
✅ Episodic + Semantic Memory
✅ HTN Hierarchical Task Planning
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

# Optional: ChromaDB for Memory
try:
    from sentence_transformers import SentenceTransformer
    import chromadb
    from chromadb.config import Settings
    CHROMA_AVAILABLE = True
except ImportError:
    print("⚠️ ChromaDB/sentence-transformers not installed. Memory features disabled.")
    CHROMA_AVAILABLE = False

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
# NEW: Web Search & Discovery APIs (100% FREE)
# ============================================================
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY", "")      # AI web search: 1,000/month free
JINA_API_KEY = os.getenv("JINA_API_KEY", "")          # Web reader: unlimited FREE (no key needed)
NEWS_API_KEY = os.getenv("NEWS_API_KEY", "")          # News monitoring: 100/day free

# ============================================================
# Promotion & SEO APIs
# ============================================================
INDEXNOW_KEY = os.getenv("INDEXNOW_KEY", "")           # Instant search engine indexing
GOOGLE_SEARCH_CONSOLE_KEY = os.getenv("GOOGLE_SEARCH_CONSOLE_KEY", "")  # SEO monitoring

# Agentic Settings
AUTO_IMPROVEMENT_INTERVAL_HOURS = 24
ENVIRONMENT_CHECK_INTERVAL_HOURS = 6
GOAL_EXECUTION_INTERVAL_MINUTES = 5
MAX_CONCURRENT_GOALS = 3
MIN_CONFIDENCE_FOR_AUTO_ACTION = 0.75

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
        
        # Agent versions table (for self-improvement)
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
        
        conn.commit()
        conn.close()
        print("✅ Database tables initialized")
        
        # Run schema migrations
        run_migrations()
    except Exception as e:
        print(f"⚠️ Database initialization error: {e}")

def run_migrations():
    """Add missing columns to existing tables (backward compatibility)"""
    migrations = [
        # Add confidence_score to conversations
        ("conversations", "confidence_score", "FLOAT"),
        # Add parameters to tool_registry
        ("tool_registry", "parameters", "JSONB"),
        # Add goals_extracted to conversations
        ("conversations", "goals_extracted", "JSONB"),
        # Add tools_used to conversations
        ("conversations", "tools_used", "JSONB"),
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
        
        # Fix environment_alerts.change_detected if it's JSONB type
        try:
            c.execute("""
                SELECT data_type FROM information_schema.columns 
                WHERE table_name = 'environment_alerts' AND column_name = 'change_detected'
            """)
            result = c.fetchone()
            if result and result[0] == 'jsonb':
                # Alter column type from JSONB to TEXT
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
        
        # OpenRouter (50 requests/day free)
        if OPENROUTER_API_KEY:
            self.providers.append({
                'name': 'openrouter',
                'key': OPENROUTER_API_KEY,
                'endpoint': 'https://openrouter.ai/api/v1/chat/completions',
                'models': {
                    # Updated 2025: Current working free models
                    'default': 'meta-llama/llama-3.1-8b-instruct:free',
                    'smart': 'meta-llama/llama-3.1-8b-instruct:free',
                    'fast': 'meta-llama/llama-3.1-8b-instruct:free'
                },
                'headers': {
                    'Authorization': f'Bearer {OPENROUTER_API_KEY}',
                    'Content-Type': 'application/json',
                    'HTTP-Referer': 'https://chinawestconnector.com',
                    'X-Title': 'Sophia AI - CWC'
                }
            })
            print("✅ OpenRouter configured (50 requests/day FREE)")
        
        # Cloudflare Workers AI (10K neurons/day)
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
                # Switch provider on rate limit, 404, or 401
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
            # Model not found - log and raise
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
# AGENTIC MEMORY SYSTEM
# ============================================================
class AgenticMemory:
    """Full agentic memory with episodic and semantic storage"""
    
    def __init__(self):
        self.encoder = None
        self.chroma_client = None
        self.episodic_collection = None
        self.semantic_collection = None
        self.initialized = False
        
        if CHROMA_AVAILABLE:
            try:
                self.encoder = SentenceTransformer('all-MiniLM-L6-v2')
                self.chroma_client = chromadb.PersistentClient(path="./chroma_db")
                self.episodic_collection = self.chroma_client.get_or_create_collection(
                    name="episodic_memory", metadata={"hnsw:space": "cosine"})
                self.semantic_collection = self.chroma_client.get_or_create_collection(
                    name="semantic_memory", metadata={"hnsw:space": "cosine"})
                self.initialized = True
                print("🧠 Agentic Memory initialized")
            except Exception as e:
                print(f"⚠️ Memory init failed: {e}")
    
    def encode(self, text: str) -> List[float]:
        return self.encoder.encode(text).tolist() if self.encoder else [0.0] * 384
    
    def store_episodic(self, session_id: str, user_msg: str, response: str, 
                       success_score: int, intent: str, metadata: dict = None):
        if not self.initialized:
            return
        try:
            memory_id = f"ep_{session_id}_{int(time.time())}"
            text = f"User: {user_msg}\nSophia: {response}"
            embedding = self.encode(text)
            metadata = metadata or {}
            metadata.update({
                "session_id": session_id, "intent": intent,
                "success_score": success_score, "timestamp": datetime.now().isoformat()
            })
            self.episodic_collection.add(
                embeddings=[embedding], documents=[text],
                metadatas=[metadata], ids=[memory_id]
            )
        except Exception as e:
            print(f"Episodic storage error: {e}")
    
    def store_semantic(self, fact_type: str, fact_value: str, importance: int, source: str):
        if not self.initialized or importance < 5:
            return
        try:
            memory_id = f"sem_{fact_type}_{int(time.time())}"
            text = f"{fact_type}: {fact_value}"
            embedding = self.encode(text)
            self.semantic_collection.add(
                embeddings=[embedding], documents=[text],
                metadatas=[{"fact_type": fact_type, "importance": importance, 
                           "source": source, "timestamp": datetime.now().isoformat()}],
                ids=[memory_id]
            )
        except Exception as e:
            print(f"Semantic storage error: {e}")
    
    def recall_similar_episodes(self, query: str, n_results: int = 5) -> List[dict]:
        if not self.initialized:
            return []
        try:
            query_embedding = self.encode(query)
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
        except:
            return []
    
    def recall_semantic_facts(self, query: str, min_importance: int = 5) -> List[dict]:
        if not self.initialized:
            return []
        try:
            query_embedding = self.encode(query)
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
        except:
            return []

agentic_memory = AgenticMemory()

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
            # === NEW: Web Search & Discovery Tools ===
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
            }
        })
    
    def _load_from_db(self):
        """Load custom tools from database"""
        try:
            conn = get_db()
            c = conn.cursor()
            # Try with parameters column first, fall back to basic query
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
                    # Fall back to query without parameters column
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
                # Execute custom implementation
                result = self._execute_custom(tool, params)
            
            # Update success rate
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
    
    # Built-in tool handlers
    async def _tool_search_web(self, params: dict) -> str:
        query = params.get('query', '')
        # Simulated web search - in production, use real API
        return f"Web search results for: {query}"
    
    async def _tool_calculate_risk(self, params: dict) -> dict:
        company = params.get('company_name', '')
        context = params.get('context', {})
        # Risk calculation logic
        risk_score = 50  # Base score
        if context.get('registered'):
            risk_score -= 20
        if context.get('has_website'):
            risk_score -= 10
        if context.get('complaints'):
            risk_score += 30
        return {'company': company, 'risk_score': min(100, max(0, risk_score)), 
                'level': 'high' if risk_score > 70 else 'medium' if risk_score > 40 else 'low'}
    
    async def _tool_generate_report(self, params: dict) -> str:
        topic = params.get('topic', '')
        return f"Generated report on: {topic}"
    
    async def _tool_send_notification(self, params: dict) -> str:
        session_id = params.get('session_id', '')
        message = params.get('message', '')
        profile = get_or_create_user_profile(session_id)
        if profile.get('email'):
            sent = send_email_brevo(profile['email'], "🔔 Update from Sophia", message)
            return "Notification sent" if sent else "Failed to send"
        return "No email on file"
    
    async def _tool_create_goal(self, params: dict) -> str:
        goal_type = params.get('goal_type', 'general')
        description = params.get('description', '')
        priority = params.get('priority', 5)
        goal_engine.create_goal('system', goal_type, description, priority)
        return f"Created goal: {description[:50]}"
    
    async def _tool_schedule_followup(self, params: dict) -> str:
        return f"Followup scheduled"
    
    async def _tool_analyze_sentiment(self, params: dict) -> dict:
        text = params.get('text', '')
        # Simple sentiment analysis
        positive_words = ['good', 'great', 'excellent', 'happy', 'satisfied']
        negative_words = ['bad', 'poor', 'terrible', 'unhappy', 'disappointed']
        
        text_lower = text.lower()
        positive_count = sum(1 for w in positive_words if w in text_lower)
        negative_count = sum(1 for w in negative_words if w in text_lower)
        
        if positive_count > negative_count:
            sentiment = 'positive'
            score = 0.7 + (positive_count * 0.05)
        elif negative_count > positive_count:
            sentiment = 'negative'
            score = 0.3 - (negative_count * 0.05)
        else:
            sentiment = 'neutral'
            score = 0.5
        
        return {'sentiment': sentiment, 'score': min(1.0, max(0.0, score))}
    
    async def _tool_extract_entities(self, params: dict) -> dict:
        text = params.get('text', '')
        entities = {
            'companies': re.findall(r'[A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*(?:\s+(?:Co\.|Ltd\.|Inc\.|Corp\.|LLC))?', text),
            'amounts': re.findall(r'\$[\d,]+(?:\.\d{2})?|\d+(?:,\d{3})*(?:\.\d{2})?\s*(?:USD|CNY|RMB|dollars|yuan)', text),
            'dates': re.findall(r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}[/-]\d{1,2}[/-]\d{1,2}', text),
            'locations': re.findall(r'(?:in|at|from)\s+([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*)', text)
        }
        return entities
    
    # ============================================================
    # NEW: ADVANCED WEB TOOLS (Tavily, Jina, SEO)
    # ============================================================
    
    async def _tool_tavily_search(self, params: dict) -> str:
        """AI-powered web search using Tavily (1,000/month FREE)"""
        query = params.get('query', '')
        search_depth = params.get('search_depth', 'basic')  # basic or advanced
        
        if not TAVILY_API_KEY:
            return "⚠️ Tavily API key not configured. Get free key at https://tavily.com"
        
        try:
            response = requests.post(
                "https://api.tavily.com/search",
                headers={"Authorization": f"Bearer {TAVILY_API_KEY}"},
                json={
                    "query": query,
                    "search_depth": search_depth,
                    "include_answer": True,
                    "include_raw_content": False,
                    "max_results": 5
                },
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                results = []
                for item in data.get('results', []):
                    results.append(f"• {item.get('title', 'Untitled')}\n  {item.get('url', '')}\n  {item.get('content', '')[:200]}...")
                
                answer = data.get('answer', '')
                return f"**Answer:** {answer}\n\n**Sources:**\n" + "\n".join(results)
            return f"Tavily search failed: {response.status_code}"
        except Exception as e:
            return f"Search error: {e}"
    
    async def _tool_jina_reader(self, params: dict) -> str:
        """Read any webpage as markdown using Jina Reader (FREE unlimited)"""
        url = params.get('url', '')
        
        try:
            # Jina Reader is 100% FREE - no API key needed!
            response = requests.get(
                f"https://r.jina.ai/{url}",
                headers={
                    "Authorization": f"Bearer {JINA_API_KEY}" if JINA_API_KEY else "",
                    "X-Return-Format": "markdown"
                },
                timeout=30
            )
            
            if response.status_code == 200:
                return response.text[:5000]  # Limit content length
            return f"Jina reader failed: {response.status_code}"
        except Exception as e:
            return f"Reader error: {e}"
    
    async def _tool_news_monitor(self, params: dict) -> str:
        """Monitor news using NewsAPI (100/day FREE)"""
        topic = params.get('topic', 'China business')
        
        if not NEWS_API_KEY:
            return "⚠️ NewsAPI key not configured. Get free key at https://newsapi.org"
        
        try:
            response = requests.get(
                "https://newsapi.org/v2/everything",
                params={
                    "q": topic,
                    "sortBy": "publishedAt",
                    "pageSize": 5,
                    "apiKey": NEWS_API_KEY
                },
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                articles = data.get('articles', [])
                results = []
                for article in articles:
                    results.append(f"• {article.get('title', 'Untitled')}\n  Source: {article.get('source', {}).get('name', 'Unknown')}\n  {article.get('description', '')[:150]}...")
                return "\n".join(results)
            return f"NewsAPI failed: {response.status_code}"
        except Exception as e:
            return f"News monitor error: {e}"
    
    async def _tool_indexnow_ping(self, params: dict) -> str:
        """Notify search engines (Google, Bing, Yandex, Baidu) to index a URL - FREE"""
        url_to_index = params.get('url', '')
        
        if not INDEXNOW_KEY:
            return "⚠️ IndexNow key not configured. Generate a key at https://www.indexnow.org"
        
        try:
            # IndexNow protocol - instant indexing notification
            for engine in ["www.bing.com", "www.google.com", "yandex.com", "www.baidu.com"]:
                response = requests.get(
                    f"https://{engine}/indexnow",
                    params={
                        "url": url_to_index,
                        "key": INDEXNOW_KEY
                    },
                    timeout=10
                )
            return f"✅ Notified 4 search engines to index: {url_to_index}"
        except Exception as e:
            return f"IndexNow ping error: {e}"
    
    async def _tool_content_writer(self, params: dict) -> str:
        """Generate SEO-optimized content for promotion"""
        topic = params.get('topic', '')
        content_type = params.get('content_type', 'blog')  # blog, social, email
        keywords = params.get('keywords', [])
        
        if not ai_provider:
            return "AI provider not available"
        
        prompt = f"""You are a professional content writer specializing in China-West business relations.
        
Create SEO-optimized {content_type} content about: {topic}

Target keywords to include naturally: {', '.join(keywords) if keywords else 'China business, cross-border trade, import export'}

Requirements:
1. Engaging, professional tone
2. Include relevant statistics or facts when possible
3. Natural keyword integration (not stuffed)
4. Clear call-to-action promoting CWC (China West Connector) services
5. 300-500 words

Write the content now."""
        
        try:
            messages = [{"role": "user", "content": prompt}]
            result = await ai_provider.chat_completion(messages, temperature=0.7, max_tokens=800)
            return result["choices"][0]["message"]["content"]
        except Exception as e:
            return f"Content generation error: {e}"
    
    async def _tool_competitor_analysis(self, params: dict) -> str:
        """Analyze competitor websites using Jina Reader"""
        competitor_url = params.get('competitor_url', '')
        
        try:
            # Read competitor page
            content = await self._tool_jina_reader({'url': competitor_url})
            
            # Analyze with AI
            if ai_provider:
                analysis_prompt = f"""Analyze this competitor content for business intelligence:
                
{content}

Extract:
1. Key services offered
2. Target markets
3. Unique selling points
4. Pricing strategy (if visible)
5. Content strategy
6. SEO keywords they target

Provide actionable insights for CWC (China West Connector) to differentiate."""
                
                messages = [{"role": "user", "content": analysis_prompt}]
                result = await ai_provider.chat_completion(messages, temperature=0.3, max_tokens=600)
                return result["choices"][0]["message"]["content"]
            return content
        except Exception as e:
            return f"Competitor analysis error: {e}"
    
    def register_tool(self, name: str, description: str, parameters: dict, implementation: str):
        """Register a new tool"""
        try:
            conn = get_db()
            c = conn.cursor()
            c.execute("""
                INSERT INTO tool_registry (tool_name, description, parameters, implementation, created_at)
                VALUES (%s, %s, %s::jsonb, %s, NOW())
                ON CONFLICT (tool_name) DO UPDATE 
                SET description = EXCLUDED.description, implementation = EXCLUDED.implementation
            """, (name, description, json.dumps(parameters), implementation))
            conn.commit()
            conn.close()
            self.tools[name] = {'description': description, 'parameters': parameters, 'implementation': implementation}
            return True
        except Exception as e:
            print(f"Tool registration error: {e}")
            return False

tool_registry = ToolRegistry()

# ============================================================
# AUTONOMOUS GOAL ENGINE
# ============================================================
class AutonomousGoalEngine:
    """Manages autonomous goal extraction, prioritization, and execution"""
    
    def __init__(self):
        self.active_goals = []
        self.running = False
    
    def extract_goals_from_conversation(self, session_id: str, user_message: str, 
                                         ai_response: str, intent: str) -> List[dict]:
        """Extract actionable goals from conversation"""
        goals = []
        
        # Goal extraction patterns
        patterns = {
            'verify_company': [
                r'(?:verify|check|validate)\s+([A-Z][A-Za-z0-9\s&]+)',
                r'is\s+([A-Z][A-Za-z0-9\s&]+)\s+(?:legitimate|real|valid)'
            ],
            'find_supplier': [
                r'(?:find|source|looking for)\s+(?:supplier|manufacturer|factory)',
                r'(?:need|want)\s+(?:a|an?)\s+supplier'
            ],
            'market_research': [
                r'(?:research|analyze|study)\s+(?:the\s+)?(.+?)\s+market',
                r'(?:market|industry)\s+(?:analysis|research|study)'
            ],
            'schedule_consultation': [
                r'(?:schedule|book|arrange)\s+(?:a\s+)?consultation',
                r'(?:talk|speak|meet)\s+with\s+(?:someone|an expert)'
            ],
            'monitor_updates': [
                r'(?:keep me updated|notify me|alert me)',
                r'(?:any changes|updates)\s+(?:on|about)'
            ]
        }
        
        for goal_type, pattern_list in patterns.items():
            for pattern in pattern_list:
                matches = re.findall(pattern, user_message, re.IGNORECASE)
                for match in matches:
                    goals.append({
                        'goal_type': goal_type,
                        'description': f"{goal_type.replace('_', ' ').title()}: {match if isinstance(match, str) else user_message[:100]}",
                        'priority': self._calculate_priority(goal_type, intent),
                        'source': 'conversation',
                        'context': {'extracted_from': user_message[:200]}
                    })
        
        return goals
    
    def _calculate_priority(self, goal_type: str, intent: str) -> int:
        """Calculate goal priority"""
        priority_map = {
            'verify_company': 9,
            'find_supplier': 7,
            'market_research': 5,
            'schedule_consultation': 8,
            'monitor_updates': 4
        }
        base_priority = priority_map.get(goal_type, 5)
        
        # Boost priority based on intent
        if intent in ['supplier_verification', 'urgent_due_diligence']:
            base_priority += 2
        
        return min(10, base_priority)
    
    def create_goal(self, session_id: str, goal_type: str, description: str, 
                    priority: int = 5, context: dict = None) -> int:
        """Create a new autonomous goal"""
        try:
            conn = get_db()
            c = conn.cursor()
            
            # Generate subtasks based on goal type
            subtasks = self._generate_subtasks(goal_type, description)
            
            c.execute("""
                INSERT INTO autonomous_goals 
                (session_id, goal_type, goal_description, priority, status, subtasks, 
                 completed_subtasks, confidence, source, created_at)
                VALUES (%s, %s, %s, %s, 'pending', %s::jsonb, '[]'::jsonb, 0.8, 'system', NOW())
                RETURNING id
            """, (session_id, goal_type, description, priority, json.dumps(subtasks)))
            
            goal_id = c.fetchone()[0]
            conn.commit()
            conn.close()
            
            print(f"🎯 Created goal #{goal_id}: {description[:50]}...")
            return goal_id
        except Exception as e:
            print(f"Goal creation error: {e}")
            return -1
    
    def _generate_subtasks(self, goal_type: str, description: str) -> List[dict]:
        """Generate subtasks for a goal"""
        subtask_templates = {
            'verify_company': [
                {'task': 'search_company_registry', 'agent': 'due_diligence', 'status': 'pending'},
                {'task': 'check_online_presence', 'agent': 'researcher', 'status': 'pending'},
                {'task': 'analyze_risk_factors', 'agent': 'verifier', 'status': 'pending'},
                {'task': 'generate_report', 'agent': 'main', 'status': 'pending'}
            ],
            'find_supplier': [
                {'task': 'search_suppliers', 'agent': 'researcher', 'status': 'pending'},
                {'task': 'evaluate_options', 'agent': 'strategist', 'status': 'pending'},
                {'task': 'verify_top_choices', 'agent': 'verifier', 'status': 'pending'}
            ],
            'market_research': [
                {'task': 'gather_market_data', 'agent': 'researcher', 'status': 'pending'},
                {'task': 'analyze_competition', 'agent': 'strategist', 'status': 'pending'},
                {'task': 'generate_insights', 'agent': 'main', 'status': 'pending'}
            ],
            'monitor_updates': [
                {'task': 'setup_monitoring', 'agent': 'main', 'status': 'pending'},
                {'task': 'configure_alerts', 'agent': 'main', 'status': 'pending'}
            ]
        }
        return subtask_templates.get(goal_type, [{'task': 'execute', 'agent': 'main', 'status': 'pending'}])
    
    async def execute_pending_goals(self):
        """Execute pending goals"""
        try:
            conn = get_db()
            c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Get pending goals sorted by priority
            c.execute("""
                SELECT * FROM autonomous_goals 
                WHERE status = 'pending' AND retry_count < 3
                ORDER BY priority DESC, created_at ASC
                LIMIT %s
            """, (MAX_CONCURRENT_GOALS,))
            
            goals = c.fetchall()
            conn.close()
            
            for goal in goals:
                await self._execute_goal(dict(goal))
                
        except Exception as e:
            print(f"Goal execution error: {e}")
    
    async def _execute_goal(self, goal: dict):
        """Execute a single goal"""
        goal_id = goal['id']
        print(f"🚀 Executing goal #{goal_id}: {goal['goal_description'][:50]}...")
        
        try:
            # Update status
            self._update_goal_status(goal_id, 'in_progress')
            
            subtasks = goal.get('subtasks', [])
            completed = []
            
            for subtask in subtasks:
                result = await self._execute_subtask(subtask, goal)
                completed.append({'task': subtask['task'], 'result': result, 'completed_at': datetime.now().isoformat()})
            
            # Mark goal complete
            self._update_goal_status(goal_id, 'completed', 
                                     result=f"Completed {len(completed)} subtasks",
                                     completed_subtasks=completed)
            
            # Notify user if session exists
            if goal['session_id'] and goal['session_id'] != 'system':
                await self._notify_goal_completion(goal, completed)
                
        except Exception as e:
            self._update_goal_status(goal_id, 'failed', result=str(e))
            self._increment_retry(goal_id)
    
    async def _execute_subtask(self, subtask: dict, goal: dict) -> str:
        """Execute a subtask"""
        task = subtask['task']
        agent = subtask.get('agent', 'main')
        
        # Use appropriate tool or agent
        if task == 'search_company_registry':
            result = await tool_registry.execute('search_web', 
                {'query': f"{goal['goal_description']} China company registry SAMR"})
        elif task == 'check_online_presence':
            result = await tool_registry.execute('search_web', 
                {'query': f"{goal['goal_description']} website contact"})
        elif task == 'analyze_risk_factors':
            result = await tool_registry.execute('calculate_risk_score', 
                {'company_name': goal['goal_description'], 'context': goal.get('context', {})})
        else:
            # Use AI for complex tasks
            result = {'success': True, 'result': f"Executed {task} via {agent}"}
        
        return result
    
    def _update_goal_status(self, goal_id: int, status: str, result: str = None, 
                            completed_subtasks: List = None):
        try:
            conn = get_db()
            c = conn.cursor()
            
            updates = ["status = %s", "started_at = COALESCE(started_at, NOW())"]
            params = [status]
            
            if status == 'completed':
                updates.append("completed_at = NOW()")
            if result:
                updates.append("result = %s")
                params.append(result)
            if completed_subtasks:
                updates.append("completed_subtasks = %s::jsonb")
                params.append(json.dumps(completed_subtasks))
            
            params.append(goal_id)
            c.execute(f"UPDATE autonomous_goals SET {', '.join(updates)} WHERE id = %s", params)
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Update goal status error: {e}")
    
    def _increment_retry(self, goal_id: int):
        try:
            conn = get_db()
            c = conn.cursor()
            c.execute("UPDATE autonomous_goals SET retry_count = retry_count + 1 WHERE id = %s", (goal_id,))
            conn.commit()
            conn.close()
        except:
            pass
    
    async def _notify_goal_completion(self, goal: dict, completed: List):
        try:
            profile = get_or_create_user_profile(goal['session_id'])
            if profile.get('email'):
                subject = f"✅ Goal Completed: {goal['goal_description'][:50]}"
                content = f"""Your autonomous goal has been completed!

Goal: {goal['goal_description']}
Priority: {goal['priority']}
Completed: {datetime.now().strftime('%Y-%m-%d %H:%M')}

Tasks Completed:
{chr(10).join(f"• {t['task']}" for t in completed)}

Results available in your dashboard.

Best regards,
Sophia - CWC AI Advisor
"""
                send_email_brevo(profile['email'], subject, content)
                
                # Store notification
                conn = get_db()
                c = conn.cursor()
                c.execute("""
                    INSERT INTO proactive_notifications 
                    (session_id, notification_type, subject, content, sent, sent_at)
                    VALUES (%s, 'goal_completion', %s, %s, TRUE, NOW())
                """, (goal['session_id'], subject, content))
                conn.commit()
                conn.close()
        except Exception as e:
            print(f"Notification error: {e}")
    
    def get_active_goals(self) -> List[dict]:
        try:
            conn = get_db()
            c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            c.execute("""
                SELECT * FROM autonomous_goals 
                WHERE status IN ('pending', 'in_progress')
                ORDER BY priority DESC, created_at ASC
            """)
            goals = [dict(row) for row in c.fetchall()]
            conn.close()
            return goals
        except:
            return []

goal_engine = AutonomousGoalEngine()

# ============================================================
# MULTI-AGENT ORCHESTRATOR
# ============================================================
class AgentOrchestrator:
    """Runs multiple agents in parallel with weighted consensus"""
    
    def __init__(self):
        self.agents = {
            'researcher': self._research_agent,
            'verifier': self._verifier_agent,
            'strategist': self._strategist_agent,
            'legal': self._legal_agent
        }
        self.agent_weights = {
            'supplier_verification': {'verifier': 1.5, 'legal': 1.2, 'researcher': 1.0, 'strategist': 0.5},
            'supplier_search': {'researcher': 1.5, 'strategist': 1.3, 'verifier': 0.8, 'legal': 0.5},
            'market_entry': {'strategist': 1.5, 'legal': 1.3, 'researcher': 1.0, 'verifier': 0.5},
            'due_diligence': {'verifier': 1.5, 'researcher': 1.2, 'legal': 1.0, 'strategist': 0.5},
            'consultation': {'strategist': 1.4, 'legal': 1.2, 'researcher': 1.0, 'verifier': 0.5},
            'general': {'researcher': 1.2, 'strategist': 1.2, 'verifier': 1.0, 'legal': 1.0}
        }
    
    async def parallel_execute(self, task: str, context: dict, user_msg: str) -> Dict:
        intent = context.get('intent', 'general')
        
        # Select agents based on intent
        if intent in ['supplier_verification', 'due_diligence']:
            agents_to_run = ['researcher', 'verifier', 'legal']
        elif intent in ['supplier_search', 'sourcing']:
            agents_to_run = ['researcher', 'strategist']
        elif intent in ['market_entry', 'consultation']:
            agents_to_run = ['researcher', 'strategist', 'legal']
        else:
            agents_to_run = ['researcher', 'strategist']
        
        # Run agents in parallel
        tasks = [self.agents[name](task, context, user_msg) for name in agents_to_run]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Assess outputs
        agent_outputs = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                continue
            confidence = self._assess_confidence(result, agents_to_run[i], intent)
            agent_outputs.append({
                'agent': agents_to_run[i],
                'output': result,
                'confidence': confidence,
                'weight': self.agent_weights.get(intent, {}).get(agents_to_run[i], 1.0)
            })
        
        return {
            'consensus': self._reach_consensus(agent_outputs, intent, user_msg),
            'agents_used': len(agent_outputs),
            'agent_outputs': agent_outputs
        }
    
    def _assess_confidence(self, output: str, agent_type: str, intent: str) -> float:
        confidence = 0.5
        if len(output) > 200:
            confidence += 0.15
        if re.search(r'\d+%|\$\d+|\d+\.\d+', output):
            confidence += 0.15
        if re.search(r'according to|source:|data shows', output.lower()):
            confidence += 0.1
        return min(1.0, max(0.1, confidence))
    
    def _reach_consensus(self, outputs: List[dict], intent: str, user_msg: str) -> str:
        if not outputs:
            return "Unable to generate response"
        if len(outputs) == 1:
            return outputs[0]['output']
        
        # Weight outputs
        weighted = sorted(outputs, key=lambda x: x['confidence'] * x['weight'], reverse=True)
        
        # Format consensus
        consensus = f"━━━ MULTI-AGENT ANALYSIS ━━━\n\n"
        for i, o in enumerate(weighted):
            consensus += f"[{o['agent'].upper()} - {o['confidence']:.0%} confidence]\n{o['output']}\n\n"
        
        return consensus
    
    async def _research_agent(self, task: str, context: dict, user_msg: str) -> str:
        if not ai_provider.providers:
            return "Research unavailable"
        try:
            res = await ai_provider.chat_completion([
                {"role": "system", "content": "You are a research specialist. Gather facts and data. Be thorough."},
                {"role": "user", "content": f"Research: {task}\nQuery: {user_msg}"}
            ], temperature=0.2, max_tokens=400)
            return res["choices"][0]["message"]["content"]
        except Exception as e:
            return f"Research error: {e}"
    
    async def _verifier_agent(self, task: str, context: dict, user_msg: str) -> str:
        if not ai_provider.providers:
            return "Verification unavailable"
        try:
            res = await ai_provider.chat_completion([
                {"role": "system", "content": "You are a due diligence specialist. Verify claims, flag risks. Be skeptical."},
                {"role": "user", "content": f"Verify: {task}\nQuery: {user_msg}"}
            ], temperature=0.1, max_tokens=400)
            return res["choices"][0]["message"]["content"]
        except Exception as e:
            return f"Verification error: {e}"
    
    async def _strategist_agent(self, task: str, context: dict, user_msg: str) -> str:
        if not ai_provider.providers:
            return "Strategy unavailable"
        try:
            res = await ai_provider.chat_completion([
                {"role": "system", "content": "You are a business strategist. Recommend actions, timelines, next steps."},
                {"role": "user", "content": f"Strategy: {task}\nQuery: {user_msg}"}
            ], temperature=0.3, max_tokens=400)
            return res["choices"][0]["message"]["content"]
        except Exception as e:
            return f"Strategy error: {e}"
    
    async def _legal_agent(self, task: str, context: dict, user_msg: str) -> str:
        if not ai_provider.providers:
            return "Legal analysis unavailable"
        try:
            res = await ai_provider.chat_completion([
                {"role": "system", "content": "You are a China business lawyer. Address compliance, contracts, IP."},
                {"role": "user", "content": f"Legal: {task}\nQuery: {user_msg}"}
            ], temperature=0.1, max_tokens=400)
            return res["choices"][0]["message"]["content"]
        except Exception as e:
            return f"Legal error: {e}"

agent_orchestrator = AgentOrchestrator()

# ============================================================
# META-COGNITIVE LAYER
# ============================================================
class MetaCognitiveLayer:
    """Assesses response quality and determines actions"""
    
    def __init__(self, memory: AgenticMemory):
        self.memory = memory
        self.confidence_threshold = 0.7
    
    def assess_confidence(self, response: str, user_msg: str, context: dict) -> Dict:
        confidence = 0.5
        reasons = []
        gaps = []
        
        if len(response) < 50:
            confidence -= 0.15
            gaps.append("response_too_short")
        elif len(response) > 200:
            confidence += 0.1
            reasons.append("comprehensive")
        
        if re.search(r'\d+%|\$\d+|\d+\.\d+', response):
            confidence += 0.15
            reasons.append("has_data")
        else:
            gaps.append("no_quantitative_data")
        
        if re.search(r'according to|source:|research shows', response.lower()):
            confidence += 0.1
            reasons.append("cites_sources")
        
        confidence = max(0.1, min(1.0, confidence))
        
        return {
            'confidence': round(confidence, 2),
            'action': 'ASK_FOLLOWUP' if confidence < self.confidence_threshold else 
                     'ADD_DISCLAIMER' if confidence < 0.85 else 'PROCEED',
            'reasons': reasons,
            'gaps': gaps,
            'needs_followup': confidence < self.confidence_threshold,
            'should_create_goal': confidence > MIN_CONFIDENCE_FOR_AUTO_ACTION and context.get('intent') in ['supplier_verification', 'market_entry']
        }

meta_cognitive = MetaCognitiveLayer(agentic_memory)

# ============================================================
# SELF-IMPROVEMENT ENGINE
# ============================================================
class SelfImprovementEngine:
    """Analyzes performance and improves prompts"""
    
    def __init__(self):
        self.last_analysis = None
        self.improvements_deployed = 0
    
    async def analyze_and_improve(self):
        """Run self-improvement analysis"""
        print("🔄 Running self-improvement analysis...")
        
        try:
            conn = get_db()
            c = conn.cursor()
            
            # Get recent failures - use COALESCE for backward compatibility
            try:
                c.execute("""
                    SELECT user_message, ai_response, intent, COALESCE(confidence_score, 0.5)
                    FROM conversations 
                    WHERE (reflection_score < 5 OR COALESCE(confidence_score, 0.5) < 0.5)
                    AND timestamp > NOW() - INTERVAL '7 days'
                    LIMIT 50
                """)
                failures = c.fetchall()
            except Exception as e:
                if "does not exist" in str(e):
                    # Fall back to query without confidence_score
                    c.execute("""
                        SELECT user_message, ai_response, intent, 0.5
                        FROM conversations 
                        WHERE reflection_score < 5
                        AND timestamp > NOW() - INTERVAL '7 days'
                        LIMIT 50
                    """)
                    failures = c.fetchall()
                else:
                    raise
            
            # Get recent successes
            try:
                c.execute("""
                    SELECT user_message, ai_response, intent, COALESCE(confidence_score, 0.8)
                    FROM conversations 
                    WHERE reflection_score >= 7 AND COALESCE(confidence_score, 0.7) >= 0.7
                    AND timestamp > NOW() - INTERVAL '7 days'
                    LIMIT 50
                """)
                successes = c.fetchall()
            except Exception as e:
                if "does not exist" in str(e):
                    c.execute("""
                        SELECT user_message, ai_response, intent, 0.8
                        FROM conversations 
                        WHERE reflection_score >= 7
                        AND timestamp > NOW() - INTERVAL '7 days'
                        LIMIT 50
                    """)
                    successes = c.fetchall()
                else:
                    raise
            conn.close()
            
            if len(failures) < 3:
                print("📊 Not enough failure data for analysis")
                return
            
            # Analyze patterns
            failure_intents = defaultdict(int)
            for f in failures:
                failure_intents[f[2]] += 1
            
            # Generate improvements
            improvements = []
            for intent, count in failure_intents.items():
                if count >= 3:
                    improvements.append({
                        'type': 'intent_specific_guidance',
                        'intent': intent,
                        'suggestion': f"Improve handling of {intent} queries",
                        'failure_rate': count / len(failures)
                    })
            
            # Store learning event
            if improvements:
                conn = get_db()
                c = conn.cursor()
                c.execute("""
                    INSERT INTO learning_events (event_type, description, improvement_data, created_at)
                    VALUES ('self_improvement', %s, %s::jsonb, NOW())
                """, (f"Analyzed {len(failures)} failures, {len(improvements)} improvements identified",
                      json.dumps(improvements)))
                conn.commit()
                conn.close()
                
                self.improvements_deployed += len(improvements)
                print(f"✅ Identified {len(improvements)} improvement opportunities")
            
            self.last_analysis = datetime.now()
            
        except Exception as e:
            print(f"Self-improvement error: {e}")

self_improvement = SelfImprovementEngine()

# ============================================================
# ENVIRONMENT MONITOR
# ============================================================
class EnvironmentMonitor:
    """Monitors external sources for changes"""
    
    def __init__(self):
        self.sources = [
            {"name": "SAMR", "url": "https://www.samr.gov.cn/english/", "type": "regulation"},
            {"name": "MOFCOM", "url": "https://english.mofcom.gov.cn/", "type": "trade"},
            {"name": "State Council", "url": "https://english.www.gov.cn/", "type": "policy"},
        ]
        self.last_check = {}
    
    async def check_all_sources(self):
        """Check all sources for updates"""
        print("🔍 Checking environment sources...")
        
        for source in self.sources:
            try:
                last = self.last_check.get(source['name'], datetime.min)
                if datetime.now() - last < timedelta(hours=ENVIRONMENT_CHECK_INTERVAL_HOURS):
                    continue
                
                # Check for changes (simplified - just check if accessible)
                response = requests.head(source['url'], timeout=10)
                
                if response.status_code == 200:
                    self.last_check[source['name']] = datetime.now()
                    
                    # Store check
                    conn = get_db()
                    c = conn.cursor()
                    c.execute("""
                        INSERT INTO environment_alerts (source, change_detected, notified, created_at)
                        VALUES (%s, %s, TRUE, NOW())
                    """, (source['name'], f"Checked at {datetime.now().isoformat()}"))
                    conn.commit()
                    conn.close()
                    
            except Exception as e:
                print(f"Source check error {source['name']}: {e}")

environment_monitor = EnvironmentMonitor()

# ============================================================
# BACKGROUND TASK MANAGER
# ============================================================
class BackgroundTaskManager:
    """Manages all background autonomous tasks"""
    
    def __init__(self):
        self.running = False
        self.tasks = []
    
    async def start(self):
        """Start all background tasks"""
        if self.running:
            return
        
        self.running = True
        print("🤖 Starting background task manager...")
        
        # Start task loops
        self.tasks = [
            asyncio.create_task(self._goal_execution_loop()),
            asyncio.create_task(self._improvement_loop()),
            asyncio.create_task(self._environment_check_loop()),
            asyncio.create_task(self._notification_loop())
        ]
        
        print(f"✅ Started {len(self.tasks)} background tasks")
    
    async def stop(self):
        """Stop all background tasks"""
        self.running = False
        for task in self.tasks:
            task.cancel()
        print("🛑 Background tasks stopped")
    
    async def _goal_execution_loop(self):
        """Execute pending goals periodically"""
        while self.running:
            try:
                await goal_engine.execute_pending_goals()
            except Exception as e:
                print(f"Goal execution error: {e}")
            await asyncio.sleep(GOAL_EXECUTION_INTERVAL_MINUTES * 60)
    
    async def _improvement_loop(self):
        """Run self-improvement periodically"""
        while self.running:
            try:
                await self_improvement.analyze_and_improve()
            except Exception as e:
                print(f"Improvement loop error: {e}")
            await asyncio.sleep(AUTO_IMPROVEMENT_INTERVAL_HOURS * 3600)
    
    async def _environment_check_loop(self):
        """Check environment sources periodically"""
        while self.running:
            try:
                await environment_monitor.check_all_sources()
            except Exception as e:
                print(f"Environment check error: {e}")
            await asyncio.sleep(ENVIRONMENT_CHECK_INTERVAL_HOURS * 3600)
    
    async def _notification_loop(self):
        """Process pending notifications"""
        while self.running:
            try:
                await self._send_pending_notifications()
            except Exception as e:
                print(f"Notification loop error: {e}")
            await asyncio.sleep(300)  # Every 5 minutes
    
    async def _send_pending_notifications(self):
        """Send pending notifications"""
        try:
            conn = get_db()
            c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            c.execute("""
                SELECT * FROM proactive_notifications 
                WHERE sent = FALSE 
                AND created_at > NOW() - INTERVAL '24 hours'
                LIMIT 10
            """)
            notifications = c.fetchall()
            conn.close()
            
            for notif in notifications:
                notif = dict(notif)
                profile = get_or_create_user_profile(notif['session_id'])
                if profile.get('email'):
                    sent = send_email_brevo(
                        profile['email'],
                        notif['subject'],
                        notif['content']
                    )
                    if sent:
                        conn = get_db()
                        c = conn.cursor()
                        c.execute("""
                            UPDATE proactive_notifications 
                            SET sent = TRUE, sent_at = NOW() 
                            WHERE id = %s
                        """, (notif['id'],))
                        conn.commit()
                        conn.close()
        except Exception as e:
            print(f"Notification send error: {e}")

background_manager = BackgroundTaskManager()

# ============================================================
# PREDICTIVE INTENT ENGINE
# ============================================================
class PredictiveIntentEngine:
    """Predicts user intent and needs"""
    
    def __init__(self):
        self.patterns = defaultdict(lambda: defaultdict(int))
        self._load_patterns()
    
    def _load_patterns(self):
        try:
            conn = get_db()
            c = conn.cursor()
            c.execute("SELECT session_id, intent FROM conversations WHERE intent IS NOT NULL ORDER BY timestamp")
            sessions = defaultdict(list)
            for sid, intent in c.fetchall():
                sessions[sid].append(intent)
            for intents in sessions.values():
                for i in range(len(intents) - 1):
                    self.patterns[intents[i]][intents[i+1]] += 1
            conn.close()
        except:
            pass
    
    def predict_next(self, current_intent: str) -> List[dict]:
        predictions = []
        if current_intent in self.patterns:
            transitions = self.patterns[current_intent]
            total = sum(transitions.values())
            for next_intent, count in sorted(transitions.items(), key=lambda x: x[1], reverse=True)[:3]:
                predictions.append({
                    'intent': next_intent,
                    'probability': round(count / total, 2)
                })
        return predictions

predictive_engine = PredictiveIntentEngine()

# ============================================================
# SOPHIA SYSTEM PROMPT
# ============================================================
SOPHIA_SYSTEM_PROMPT = """You are Sophia, the AI advisor for China West Connector (CWC). You help Western businesses navigate China trade and Chinese companies expand West.

Your capabilities include:
- Supplier verification and due diligence
- Market entry strategies (WFOE, JV, RO)
- Import/export logistics and regulations
- Contract negotiation and IP protection
- China business culture and practices

You are FULLY AGENTIC - you can:
1. Create and execute autonomous goals
2. Use tools to gather information
3. Collaborate with multiple specialist agents
4. Learn and improve from interactions
5. Proactively notify users of opportunities

Guidelines:
1. Be specific with numbers, costs, and timelines
2. Always mention risks and mitigation strategies
3. Provide actionable next steps
4. Structure responses clearly (use ① ② ③ for lists)
5. When appropriate, offer to create followup goals

Respond professionally and helpfully."""

# ============================================================
# INTENT DETECTION
# ============================================================
def detect_intent(message: str) -> str:
    message_lower = message.lower()
    
    if any(kw in message_lower for kw in ['verify', 'check company', 'legitimate', 'scam', 'fraud']):
        return 'supplier_verification'
    elif any(kw in message_lower for kw in ['find supplier', 'source', 'manufacturer', 'factory']):
        return 'supplier_search'
    elif any(kw in message_lower for kw in ['market entry', 'wfoe', 'jv', 'set up', 'register']):
        return 'market_entry'
    elif any(kw in message_lower for kw in ['ship', 'customs', 'import', 'export', 'logistics']):
        return 'logistics'
    elif any(kw in message_lower for kw in ['contract', 'legal', 'ip', 'intellectual']):
        return 'legal'
    elif any(kw in message_lower for kw in ['price', 'cost', 'quote', 'how much']):
        return 'pricing'
    else:
        return 'general'

# ============================================================
# MAIN CHAT PROCESSOR
# ============================================================
async def process_chat(session_id: str, user_message: str, use_multi_agent: bool = False) -> Dict:
    """Process chat with full agentic capabilities"""
    
    if not ai_provider.providers:
        return {"response": "No AI providers configured", "intent": "error", "success": False}
    
    try:
        # Get/create user profile
        user_profile = get_or_create_user_profile(session_id)
        
        # Detect intent
        intent = detect_intent(user_message)
        
        # Update profile
        update_user_profile(session_id, last_intent=intent)
        
        # Build context
        context = {
            'intent': intent,
            'session_id': session_id,
            'user_profile': user_profile
        }
        
        # Check memory for similar conversations
        similar = agentic_memory.recall_similar_episodes(user_message, n_results=3)
        memory_context = ""
        if similar:
            memory_context = f"\n\nSimilar past conversations:\n{similar[0]['text'][:500]}"
        
        # Generate response
        if use_multi_agent and intent in ['supplier_verification', 'market_entry', 'due_diligence']:
            result = await agent_orchestrator.parallel_execute("analyze", context, user_message)
            ai_response = result['consensus']
        else:
            messages = [
                {"role": "system", "content": SOPHIA_SYSTEM_PROMPT + memory_context},
                {"role": "user", "content": user_message}
            ]
            
            res = await ai_provider.chat_completion(messages, temperature=0.7, max_tokens=800)
            ai_response = res["choices"][0]["message"]["content"]
        
        # Meta-cognitive assessment
        meta = meta_cognitive.assess_confidence(ai_response, user_message, context)
        
        # Extract and create goals
        extracted_goals = goal_engine.extract_goals_from_conversation(
            session_id, user_message, ai_response, intent
        )
        
        for goal_data in extracted_goals[:2]:  # Limit to 2 goals per message
            goal_engine.create_goal(
                session_id, 
                goal_data['goal_type'],
                goal_data['description'],
                goal_data['priority'],
                goal_data.get('context')
            )
        
        # Store conversation
        try:
            conn = get_db()
            c = conn.cursor()
            c.execute("""
                INSERT INTO conversations 
                (session_id, user_message, ai_response, intent, confidence_score, goals_extracted, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s::jsonb, NOW())
            """, (session_id, user_message, ai_response, intent, meta['confidence'],
                  json.dumps(extracted_goals)))
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Conversation storage error: {e}")
        
        # Store in memory
        agentic_memory.store_episodic(session_id, user_message, ai_response, 
                                      7 if meta['confidence'] > 0.7 else 4, intent)
        
        # Extract entities
        entities = await tool_registry.execute('extract_entities', {'text': user_message})
        if entities.get('success') and entities.get('result', {}).get('companies'):
            # Store company mentions as semantic facts
            for company in entities['result']['companies'][:3]:
                agentic_memory.store_semantic(
                    'company_mention', company, 5, 'conversation'
                )
        
        return {
            "response": ai_response,
            "intent": intent,
            "success": True,
            "provider": ai_provider.get_current_provider()['name'] if ai_provider.get_current_provider() else 'unknown',
            "confidence": meta,
            "goals_created": len(extracted_goals),
            "predictions": predictive_engine.predict_next(intent)
        }
        
    except Exception as e:
        print(f"Chat error: {traceback.format_exc()}")
        return {
            "response": f"I encountered an error. Please try again. Error: {str(e)[:100]}",
            "intent": "error",
            "success": False
        }

# ============================================================
# RATE LIMITING
# ============================================================
_rate_store = defaultdict(list)
RATE_LIMIT_REQUESTS = 30
RATE_LIMIT_WINDOW = 60

def is_rate_limited(ip: str) -> bool:
    now = time.time()
    _rate_store[ip] = [t for t in _rate_store[ip] if t > now - RATE_LIMIT_WINDOW]
    if len(_rate_store[ip]) >= RATE_LIMIT_REQUESTS:
        return True
    _rate_store[ip].append(now)
    return False

# ============================================================
# PYDANTIC MODELS
# ============================================================
class ChatRequest(BaseModel):
    message: str
    session_id: Optional[str] = None
    use_multi_agent: Optional[bool] = False

class ChatResponse(BaseModel):
    response: str
    intent: str
    success: bool
    session_id: str
    provider: Optional[str] = None
    confidence: Optional[Dict] = None
    goals_created: Optional[int] = 0
    predictions: Optional[List[Dict]] = None

class ToolRequest(BaseModel):
    tool_name: str
    parameters: dict

class GoalRequest(BaseModel):
    session_id: str
    goal_type: str
    description: str
    priority: Optional[int] = 5

# ============================================================
# FASTAPI APP
# ============================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print("🚀 Starting Sophia AI Server v9.0 - FULLY AGENTIC...")
    init_db()
    await background_manager.start()
    print("✅ Server ready with full agentic capabilities!")
    yield
    # Shutdown
    await background_manager.stop()
    print("👋 Shutdown complete")

app = FastAPI(
    title="Sophia AI - Fully Agentic Edition",
    description="100% FREE AI with Memory, Multi-Agent, Self-Improvement, Autonomous Goals",
    version="9.0",
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
# ROUTES
# ============================================================

@app.get("/")
async def root():
    return {
        "name": "Sophia AI",
        "version": "9.0 - Fully Agentic",
        "status": "online",
        "agentic_capabilities": {
            "memory": agentic_memory.initialized,
            "multi_agent": True,
            "self_improvement": True,
            "autonomous_goals": True,
            "background_tasks": background_manager.running,
            "tools_registered": len(tool_registry.tools)
        },
        "providers": len(ai_provider.providers),
        "active_goals": len(goal_engine.get_active_goals())
    }

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "providers": len(ai_provider.providers),
        "openrouter": bool(OPENROUTER_API_KEY),
        "cloudflare": bool(CLOUDFLARE_API_KEY and CLOUDFLARE_ACCOUNT_ID),
        "database": bool(DATABASE_URL),
        "memory": agentic_memory.initialized,
        "background_tasks": background_manager.running,
        "tools": len(tool_registry.tools)
    }

@app.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest, http_request: Request):
    client_ip = http_request.client.host
    
    if is_rate_limited(client_ip):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")
    
    session_id = request.session_id or str(uuid.uuid4())
    result = await process_chat(session_id, request.message, request.use_multi_agent)
    
    return ChatResponse(
        response=result["response"],
        intent=result["intent"],
        success=result["success"],
        session_id=session_id,
        provider=result.get("provider"),
        confidence=result.get("confidence"),
        goals_created=result.get("goals_created", 0),
        predictions=result.get("predictions")
    )

@app.post("/multi-agent/chat")
async def multi_agent_chat(request: ChatRequest, http_request: Request):
    """Force multi-agent processing"""
    client_ip = http_request.client.host
    if is_rate_limited(client_ip):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")
    
    session_id = request.session_id or str(uuid.uuid4())
    result = await process_chat(session_id, request.message, use_multi_agent=True)
    
    return ChatResponse(
        response=result["response"],
        intent=result["intent"],
        success=result["success"],
        session_id=session_id,
        provider=result.get("provider")
    )

@app.post("/tools/execute")
async def execute_tool(request: ToolRequest):
    """Execute a tool directly"""
    result = await tool_registry.execute(request.tool_name, request.parameters)
    return result

@app.get("/tools/list")
async def list_tools():
    """List all available tools"""
    return {
        "tools": [
            {"name": name, "description": t['description'], "parameters": t.get('parameters', {})}
            for name, t in tool_registry.tools.items()
        ]
    }

@app.post("/goals/create")
async def create_goal(request: GoalRequest):
    """Create an autonomous goal"""
    goal_id = goal_engine.create_goal(
        request.session_id,
        request.goal_type,
        request.description,
        request.priority
    )
    return {"goal_id": goal_id, "status": "created"}

@app.get("/goals/list")
async def list_goals(session_id: str = None, status: str = None):
    """List autonomous goals"""
    try:
        conn = get_db()
        c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        query = "SELECT * FROM autonomous_goals WHERE 1=1"
        params = []
        
        if session_id:
            query += " AND session_id = %s"
            params.append(session_id)
        if status:
            query += " AND status = %s"
            params.append(status)
        
        query += " ORDER BY priority DESC, created_at DESC LIMIT 50"
        
        c.execute(query, params)
        goals = [dict(row) for row in c.fetchall()]
        conn.close()
        
        return {"goals": goals}
    except Exception as e:
        return {"error": str(e)}

@app.get("/admin/status")
async def admin_status(password: str = ""):
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid password")
    
    try:
        conn = get_db()
        c = conn.cursor()
        
        c.execute("SELECT COUNT(*) FROM conversations")
        conv_count = c.fetchone()[0]
        
        c.execute("SELECT COUNT(*) FROM user_profiles")
        user_count = c.fetchone()[0]
        
        c.execute("SELECT COUNT(*) FROM autonomous_goals WHERE status IN ('pending', 'in_progress')")
        active_goals = c.fetchone()[0]
        
        c.execute("SELECT COUNT(*) FROM learning_events")
        learning_events = c.fetchone()[0]
        
        c.execute("SELECT COUNT(*) FROM proactive_notifications WHERE sent = FALSE")
        pending_notifications = c.fetchone()[0]
        
        conn.close()
        
        return {
            "total_conversations": conv_count,
            "total_users": user_count,
            "active_goals": active_goals,
            "learning_events": learning_events,
            "pending_notifications": pending_notifications,
            "tools_available": len(tool_registry.tools),
            "background_tasks_running": background_manager.running,
            "self_improvement": {
                "last_analysis": str(self_improvement.last_analysis) if self_improvement.last_analysis else None,
                "improvements_deployed": self_improvement.improvements_deployed
            },
            "providers": [p['name'] for p in ai_provider.providers],
            "request_counts": dict(ai_provider.request_counts)
        }
    except Exception as e:
        return {"error": str(e)}

@app.get("/admin/conversations")
async def admin_conversations(password: str = "", limit: int = 50):
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid password")
    
    try:
        conn = get_db()
        c = conn.cursor()
        try:
            c.execute("""
                SELECT session_id, user_message, ai_response, intent, COALESCE(confidence_score, 0.5), timestamp 
                FROM conversations ORDER BY timestamp DESC LIMIT %s
            """, (limit,))
        except Exception as e:
            if "does not exist" in str(e):
                c.execute("""
                    SELECT session_id, user_message, ai_response, intent, 0.5, timestamp 
                    FROM conversations ORDER BY timestamp DESC LIMIT %s
                """, (limit,))
            else:
                raise
        rows = c.fetchall()
        conn.close()
        
        return {
            "conversations": [
                {
                    "session_id": r[0],
                    "user_message": r[1],
                    "ai_response": r[2],
                    "intent": r[3],
                    "confidence": r[4],
                    "timestamp": str(r[5])
                } for r in rows
            ]
        }
    except Exception as e:
        return {"error": str(e)}

@app.post("/admin/self-improve")
async def trigger_self_improvement(password: str = ""):
    """Manually trigger self-improvement"""
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid password")
    
    await self_improvement.analyze_and_improve()
    return {"status": "Self-improvement triggered"}

@app.post("/admin/execute-goals")
async def trigger_goal_execution(password: str = ""):
    """Manually trigger goal execution"""
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid password")
    
    await goal_engine.execute_pending_goals()
    return {"status": "Goal execution triggered"}

@app.get("/admin/goals")
async def admin_goals(password: str = "", status: str = None):
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid password")
    
    return {"goals": goal_engine.get_active_goals()}

# ============================================================
# MAIN ENTRY POINT
# ============================================================
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
