"""
Sophia AI Server v8.0 - Complete Edition
========================================
100% FREE AI with OpenRouter + Cloudflare
Features: Memory System, Multi-Agent Orchestration, Self-Improvement, HTN Planning
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
from collections import defaultdict
from datetime import datetime, timedelta
from dotenv import load_dotenv
from contextlib import asynccontextmanager
from typing import List, Dict, Any, Optional, Tuple
import uuid

# v8.0: ChromaDB for Memory (optional)
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
# CONFIGURATION - 100% FREE TIER ONLY
# ============================================================
BREVO_API_KEY   = os.getenv("BREVO_API_KEY", "")
SENDER_EMAIL    = os.getenv("SENDER_EMAIL", "888nv666@gmail.com")
RECIPIENT_EMAIL = "digkasm@proton.me"
DATABASE_URL    = os.getenv("DATABASE_URL")

# 100% FREE AI Providers (No credit card required)
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
CLOUDFLARE_API_KEY = os.getenv("CLOUDFLARE_API_KEY", "")
CLOUDFLARE_ACCOUNT_ID = os.getenv("CLOUDFLARE_ACCOUNT_ID", "")

ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")

# ============================================================
# DATABASE HELPER
# ============================================================
def get_db():
    """Get database connection"""
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL not configured")
    return psycopg2.connect(DATABASE_URL)

def init_db():
    """Initialize database tables"""
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
                lead_score INTEGER DEFAULT 0,
                last_intent VARCHAR(50),
                key_facts JSONB,
                region_interest VARCHAR(100),
                visit_count INTEGER DEFAULT 1,
                first_seen TIMESTAMP DEFAULT NOW(),
                last_seen TIMESTAMP DEFAULT NOW(),
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # Agent versions table
        c.execute("""
            CREATE TABLE IF NOT EXISTS agent_versions (
                id SERIAL PRIMARY KEY,
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
                implementation TEXT,
                created_by VARCHAR(100),
                deployed BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # Agent tasks table
        c.execute("""
            CREATE TABLE IF NOT EXISTS agent_tasks (
                id SERIAL PRIMARY KEY,
                session_id VARCHAR(100),
                task_description TEXT,
                status VARCHAR(50) DEFAULT 'pending',
                sub_tasks JSONB,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        conn.commit()
        conn.close()
        print("✅ Database tables initialized")
    except Exception as e:
        print(f"⚠️ Database initialization error: {e}")

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
        else:
            # Update visit count and last seen
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
            if key == 'key_facts':
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
    if not BREVO_API_KEY:
        print("⚠️ BREVO_API_KEY not configured")
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
            }
        )
        return response.status_code == 201
    except Exception as e:
        print(f"Email error: {e}")
        return False

# ============================================================
# v8.0: FREE AI PROVIDER MANAGER - OpenRouter + Cloudflare
# ============================================================
class FreeAIProvider:
    """Manages 100% free AI providers with automatic fallback"""
    
    def __init__(self):
        self.providers = []
        self.current_provider = 0
        
        # Priority 1: OpenRouter (50 requests/day free, no CC required)
        if OPENROUTER_API_KEY:
            self.providers.append({
                'name': 'openrouter',
                'key': OPENROUTER_API_KEY,
                'endpoint': 'https://openrouter.ai/api/v1/chat/completions',
                'models': {
                    'default': 'mistralai/mistral-7b-instruct:free',
                    'smart': 'deepseek/deepseek-r1:free',
                    'fast': 'google/gemma-2-9b-it:free'
                },
                'headers': {
                    'Authorization': f'Bearer {OPENROUTER_API_KEY}',
                    'Content-Type': 'application/json',
                    'HTTP-Referer': 'https://chinawestconnector.com',
                    'X-Title': 'Sophia AI - CWC'
                }
            })
            print("✅ OpenRouter configured (50 requests/day FREE)")
        
        # Priority 2: Cloudflare Workers AI (10K neurons/day, resets daily)
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
            print("⚠️ No AI providers configured! Set OPENROUTER_API_KEY or CLOUDFLARE_API_KEY")
        else:
            print(f"🎯 Total providers: {len(self.providers)}")
    
    def get_current_provider(self):
        if not self.providers:
            return None
        return self.providers[self.current_provider]
    
    def switch_provider(self):
        """Switch to next available provider on failure"""
        if len(self.providers) <= 1:
            return self.get_current_provider()
        self.current_provider = (self.current_provider + 1) % len(self.providers)
        provider = self.get_current_provider()
        print(f"🔄 Switched to backup provider: {provider['name']}")
        return provider
    
    async def chat_completion(self, messages, model_type='default', temperature=0.3, max_tokens=1000, tools=None, tool_choice=None):
        """Try current provider, fallback to next on failure"""
        if not self.providers:
            raise Exception("No AI providers configured")
        
        last_error = None
        
        for attempt in range(len(self.providers)):
            provider = self.get_current_provider()
            if not provider:
                raise Exception("No provider available")
            
            try:
                if provider['name'] == 'openrouter':
                    result = await self._call_openrouter(provider, messages, model_type, temperature, max_tokens, tools, tool_choice)
                elif provider['name'] == 'cloudflare':
                    result = await self._call_cloudflare(provider, messages, model_type, temperature, max_tokens)
                else:
                    raise ValueError(f"Unknown provider: {provider['name']}")
                
                return result
                
            except Exception as e:
                last_error = str(e)
                print(f"⚠️ {provider['name']} failed: {e}")
                
                if 'rate limit' in last_error.lower() or '429' in last_error:
                    print(f"⏳ {provider['name']} rate limited, trying next provider...")
                
                if len(self.providers) > 1:
                    self.switch_provider()
                    await asyncio.sleep(1)
                else:
                    break
        
        raise Exception(f"All providers failed. Last error: {last_error}")
    
    async def _call_openrouter(self, provider, messages, model_type, temperature, max_tokens, tools, tool_choice):
        """Call OpenRouter API"""
        payload = {
            "model": provider['models'][model_type],
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens
        }
        
        if tools:
            payload["tools"] = tools
            payload["tool_choice"] = tool_choice or "auto"
        
        response = requests.post(provider['endpoint'], headers=provider['headers'], json=payload, timeout=60)
        
        if response.status_code == 429:
            raise Exception("OpenRouter rate limit exceeded (429) - 50/day free tier")
        elif response.status_code == 401:
            raise Exception("OpenRouter invalid API key (401)")
        
        response.raise_for_status()
        return response.json()
    
    async def _call_cloudflare(self, provider, messages, model_type, temperature, max_tokens):
        """Call Cloudflare Workers AI"""
        model = provider['models'][model_type]
        url = f"https://api.cloudflare.com/client/v4/accounts/{provider['account_id']}/ai/run/{model}"
        
        payload = {
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens
        }
        
        response = requests.post(url, headers=provider['headers'], json=payload, timeout=60)
        
        if response.status_code == 429:
            raise Exception("Cloudflare rate limit exceeded (429) - 10K neurons/day")
        elif response.status_code == 401:
            raise Exception("Cloudflare invalid API key (401)")
        
        response.raise_for_status()
        data = response.json()
        
        # Normalize Cloudflare response to OpenAI format
        return {
            "choices": [{
                "message": {
                    "role": "assistant",
                    "content": data.get('result', {}).get('response', '')
                },
                "finish_reason": "stop"
            }]
        }

# Initialize the free AI provider manager
ai_provider = FreeAIProvider()

# ============================================================
# v7.1: AGENTIC MEMORY SYSTEM (Episodic + Semantic)
# ============================================================
class AgenticMemory:
    """Full agentic memory architecture with embeddings"""
    
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
                    name="episodic_memory",
                    metadata={"hnsw:space": "cosine"}
                )
                self.semantic_collection = self.chroma_client.get_or_create_collection(
                    name="semantic_memory",
                    metadata={"hnsw:space": "cosine"}
                )
                self.initialized = True
                print("🧠 Agentic Memory initialized with ChromaDB + embeddings")
            except Exception as e:
                print(f"⚠️ Memory initialization failed: {e}")
    
    def encode(self, text: str) -> List[float]:
        if not self.encoder:
            return [0.0] * 384
        return self.encoder.encode(text).tolist()
    
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
                "session_id": session_id,
                "intent": intent,
                "success_score": success_score,
                "timestamp": datetime.now().isoformat()
            })
            self.episodic_collection.add(
                embeddings=[embedding],
                documents=[text],
                metadatas=[metadata],
                ids=[memory_id]
            )
            print(f"📝 Stored episodic memory: {memory_id}")
        except Exception as e:
            print(f"Episodic storage error: {e}")
    
    def store_semantic(self, fact_type: str, fact_value: str, 
                       importance: int, source: str, metadata: dict = None):
        if not self.initialized or importance < 5:
            return
        try:
            memory_id = f"sem_{fact_type}_{int(time.time())}"
            text = f"{fact_type}: {fact_value}"
            embedding = self.encode(text)
            metadata = metadata or {}
            metadata.update({
                "fact_type": fact_type,
                "importance": importance,
                "source": source,
                "timestamp": datetime.now().isoformat()
            })
            self.semantic_collection.add(
                embeddings=[embedding],
                documents=[text],
                metadatas=[metadata],
                ids=[memory_id]
            )
            print(f"💾 Stored semantic fact: {fact_type} = {fact_value}")
        except Exception as e:
            print(f"Semantic storage error: {e}")
    
    def recall_similar_episodes(self, query: str, n_results: int = 5) -> List[dict]:
        if not self.initialized:
            return []
        try:
            query_embedding = self.encode(query)
            results = self.episodic_collection.query(
                query_embeddings=[query_embedding],
                n_results=n_results
            )
            episodes = []
            for i in range(len(results['ids'][0])):
                episodes.append({
                    'id': results['ids'][0][i],
                    'text': results['documents'][0][i],
                    'metadata': results['metadatas'][0][i],
                    'distance': results['distances'][0][i] if 'distances' in results else None
                })
            return episodes
        except Exception as e:
            print(f"Recall error: {e}")
            return []
    
    def recall_semantic_facts(self, query: str, min_importance: int = 5) -> List[dict]:
        if not self.initialized:
            return []
        try:
            query_embedding = self.encode(query)
            results = self.semantic_collection.query(
                query_embeddings=[query_embedding],
                n_results=10
            )
            facts = []
            for i in range(len(results['ids'][0])):
                metadata = results['metadatas'][0][i]
                if metadata.get('importance', 0) >= min_importance:
                    facts.append({
                        'id': results['ids'][0][i],
                        'text': results['documents'][0][i],
                        'metadata': metadata,
                        'distance': results['distances'][0][i] if 'distances' in results else None
                    })
            return facts
        except Exception as e:
            print(f"Semantic recall error: {e}")
            return []

# Initialize memory
agentic_memory = AgenticMemory()

# ============================================================
# v7.1: SELF-IMPROVEMENT ENGINE
# ============================================================
class SelfImprovementEngine:
    """Analyzes performance and improves Sophia's own prompts"""
    
    def __init__(self):
        self.improvement_threshold = 0.15
        self.last_analysis = None
    
    async def analyze_performance(self, days: int = 7):
        try:
            conn = get_db()
            c = conn.cursor()
            c.execute("""
                SELECT user_message, ai_response, reflection_score, intent
                FROM conversations 
                WHERE reflection_score < 5 
                AND timestamp > NOW() - INTERVAL '%s days'
                LIMIT 50
            """, (days,))
            failures = c.fetchall()
            c.execute("""
                SELECT user_message, ai_response, reflection_score, intent
                FROM conversations 
                WHERE reflection_score >= 8 
                AND timestamp > NOW() - INTERVAL '%s days'
                LIMIT 50
            """, (days,))
            successes = c.fetchall()
            conn.close()
            
            if len(failures) < 5 or len(successes) < 5:
                print("📊 Not enough data for self-improvement analysis")
                return
            
            failure_patterns = self._extract_patterns(failures, is_failure=True)
            success_patterns = self._extract_patterns(successes, is_failure=False)
            improved_prompts = self._generate_improved_prompts(failure_patterns, success_patterns)
            
            for prompt_data in improved_prompts:
                await self._test_and_deploy(prompt_data, failures[:10])
            
            self.last_analysis = datetime.now()
            print(f"✅ Self-improvement analysis complete: {len(improved_prompts)} candidates")
        except Exception as e:
            print(f"Self-improvement error: {e}")
    
    def _extract_patterns(self, conversations: List[tuple], is_failure: bool) -> Dict:
        patterns = {'intents': defaultdict(int), 'response_styles': defaultdict(int), 'keywords': defaultdict(int), 'lengths': []}
        for user_msg, ai_response, score, intent in conversations:
            patterns['intents'][intent] += 1
            patterns['lengths'].append(len(ai_response))
            if '•' in ai_response or '①' in ai_response:
                patterns['response_styles']['structured'] += 1
            if any(c.isdigit() for c in ai_response):
                patterns['response_styles']['has_numbers'] += 1
            if '?' in ai_response[-20:]:
                patterns['response_styles']['ends_with_question'] += 1
            if 'contact' in ai_response.lower() or 'michail' in ai_response.lower():
                patterns['response_styles']['has_cta'] += 1
            words = user_msg.lower().split()
            for w in words:
                if len(w) > 4:
                    patterns['keywords'][w] += 1
        return patterns
    
    def _generate_improved_prompts(self, failures: Dict, successes: Dict) -> List[Dict]:
        candidates = []
        if failures['response_styles'].get('structured', 0) < 2:
            candidates.append({
                'type': 'structure_emphasis',
                'prompt_addition': "\nCRITICAL: Always structure responses with numbered lists (①, ②, ③). Users respond better to clear structure.",
                'hypothesis': 'Structured responses reduce failure rate'
            })
        if failures['response_styles'].get('has_numbers', 0) < 2:
            candidates.append({
                'type': 'data_emphasis',
                'prompt_addition': "\nMANDATORY: Include specific numbers, percentages, and ranges. Never say 'some' or 'many' - quantify everything.",
                'hypothesis': 'Specific data increases credibility'
            })
        if failures['response_styles'].get('has_cta', 0) < 2:
            candidates.append({
                'type': 'cta_emphasis',
                'prompt_addition': "\nREQUIRED: Every response must end with a specific next step. Never leave the user without a clear action.",
                'hypothesis': 'Clear CTAs drive engagement'
            })
        if successes['lengths'] and failures['lengths']:
            avg_success_len = sum(successes['lengths']) / len(successes['lengths'])
            avg_failure_len = sum(failures['lengths']) / len(failures['lengths'])
            if avg_failure_len > avg_success_len * 1.3:
                candidates.append({
                    'type': 'conciseness_emphasis',
                    'prompt_addition': f"\nCONCISENESS: Keep responses under {int(avg_success_len)} words. Be direct. No fluff.",
                    'hypothesis': 'Shorter responses perform better'
                })
        return candidates
    
    async def _test_and_deploy(self, prompt_data: Dict, test_cases: List[tuple]):
        if not ai_provider.providers:
            return
        try:
            base_prompt = self._get_base_system_prompt()
            test_prompt = base_prompt + prompt_data['prompt_addition']
            improvements = []
            for user_msg, _, original_score, intent in test_cases[:5]:
                test_response = await self._test_prompt(test_prompt, user_msg)
                if test_response:
                    reflection = await self._score_response(test_response, user_msg)
                    if reflection > original_score:
                        improvement = (reflection - original_score) / original_score
                        improvements.append(improvement)
            if improvements:
                avg_improvement = sum(improvements) / len(improvements)
                if avg_improvement >= self.improvement_threshold:
                    self._deploy_prompt_improvement(prompt_data, avg_improvement)
                    print(f"✅ Deployed improvement: {prompt_data['type']} ({avg_improvement:.1%} better)")
        except Exception as e:
            print(f"Prompt testing error: {e}")
    
    async def _test_prompt(self, prompt: str, user_msg: str) -> Optional[str]:
        try:
            messages = [
                {"role": "system", "content": prompt},
                {"role": "user", "content": user_msg}
            ]
            res = await ai_provider.chat_completion(messages, temperature=0.3, max_tokens=500)
            return res["choices"][0]["message"]["content"]
        except:
            return None
    
    async def _score_response(self, response: str, user_msg: str) -> int:
        try:
            messages = [
                {"role": "system", "content": "Score this response 1-10. Return ONLY the number."},
                {"role": "user", "content": f"User: {user_msg}\nResponse: {response}"}
            ]
            res = await ai_provider.chat_completion(messages, temperature=0.0, max_tokens=10)
            score_text = res["choices"][0]["message"]["content"].strip()
            return int(re.search(r'\d+', score_text).group())
        except:
            return 5
    
    def _get_base_system_prompt(self) -> str:
        return "You are Sophia, CWC's AI advisor for China-West business."
    
    def _deploy_prompt_improvement(self, prompt_data: Dict, improvement: float):
        try:
            conn = get_db()
            c = conn.cursor()
            prompt_hash = hashlib.md5(prompt_data['prompt_addition'].encode()).hexdigest()
            c.execute("""
                INSERT INTO agent_versions (prompt_hash, prompt_text, performance_score, deployed)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (prompt_hash) 
                DO UPDATE SET performance_score = EXCLUDED.performance_score
            """, (prompt_hash, prompt_data['prompt_addition'], improvement, True))
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Prompt deployment error: {e}")

# Initialize self-improvement engine
self_improvement = SelfImprovementEngine()

# ============================================================
# v7.1: ENVIRONMENT MONITOR
# ============================================================
class EnvironmentMonitor:
    def __init__(self):
        self.watched_sources = [
            {"name": "SAMR", "url": "https://www.samr.gov.cn/english/latest/", "type": "regulation"},
            {"name": "MOFCOM", "url": "https://english.mofcom.gov.cn/news/", "type": "trade"},
            {"name": "State Council", "url": "https://english.www.gov.cn/news/", "type": "policy"},
            {"name": "NDRC", "url": "https://en.ndrc.gov.cn/news/", "type": "investment"}
        ]
        self.last_check = {}
    
    async def poll_sources(self):
        while True:
            try:
                for source in self.watched_sources:
                    last = self.last_check.get(source['name'], datetime.min)
                    if datetime.now() - last < timedelta(hours=6):
                        continue
                    changes = await self._check_source(source)
                    if changes:
                        interested_users = await self._find_interested_users(changes)
                        await self._store_alerts(source, changes, interested_users)
                        if interested_users:
                            await self._notify_interested_users(source, changes, interested_users)
                    self.last_check[source['name']] = datetime.now()
            except Exception as e:
                print(f"Environment monitor error: {e}")
            await asyncio.sleep(3600)
    
    async def _check_source(self, source: dict) -> Optional[Dict]:
        try:
            res = requests.head(source['url'], timeout=10)
            last_modified = res.headers.get('last-modified')
            if last_modified:
                conn = get_db()
                c = conn.cursor()
                c.execute("""
                    SELECT change_detected FROM environment_alerts 
                    WHERE source = %s 
                    ORDER BY created_at DESC LIMIT 1
                """, (source['name'],))
                last = c.fetchone()
                if not last or last[0] != last_modified:
                    return {
                        'source': source['name'],
                        'type': source['type'],
                        'timestamp': last_modified,
                        'description': f"Update detected on {source['name']}"
                    }
                conn.close()
        except Exception as e:
            print(f"Source check error {source['name']}: {e}")
        return None
    
    async def _find_interested_users(self, changes: Dict) -> List[Dict]:
        try:
            conn = get_db()
            c = conn.cursor()
            if changes['type'] == 'regulation':
                c.execute("""
                    SELECT session_id, email, name, key_facts 
                    FROM user_profiles 
                    WHERE lead_score >= 30 
                    AND (last_intent IN ('market_entry', 'supplier_verification')
                         OR key_facts->>'sector' IS NOT NULL)
                    LIMIT 20
                """)
            elif changes['type'] == 'trade':
                c.execute("""
                    SELECT session_id, email, name, key_facts 
                    FROM user_profiles 
                    WHERE lead_score >= 30 
                    AND (region_interest IS NOT NULL
                         OR key_facts->>'direction' = 'west_to_china')
                    LIMIT 20
                """)
            else:
                c.execute("""
                    SELECT session_id, email, name, key_facts 
                    FROM user_profiles 
                    WHERE lead_score >= 50
                    LIMIT 10
                """)
            users = []
            for row in c.fetchall():
                users.append({'session_id': row[0], 'email': row[1], 'name': row[2], 'key_facts': row[3] if row[3] else {}})
            conn.close()
            return users
        except Exception as e:
            print(f"Find interested users error: {e}")
            return []
    
    async def _store_alerts(self, source: dict, changes: Dict, users: List[Dict]):
        try:
            conn = get_db()
            c = conn.cursor()
            user_segments = [u['session_id'] for u in users[:50]]
            c.execute("""
                INSERT INTO environment_alerts 
                (source, change_detected, user_segments, notified, created_at)
                VALUES (%s, %s, %s::jsonb, %s, %s)
            """, (source['name'], json.dumps(changes), json.dumps(user_segments), False, datetime.now()))
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Store alerts error: {e}")
    
    async def _notify_interested_users(self, source: dict, changes: Dict, users: List[Dict]):
        for user in users:
            if user.get('email'):
                try:
                    send_email_brevo(
                        user['email'],
                        f"🔔 China Business Alert: {source['name']} Update",
                        f"Dear {user['name'] or 'Client'},\n\n"
                        f"Sophia has detected an important update from {source['name']} "
                        f"that may affect your China business interests:\n\n"
                        f"{changes.get('description', 'New information available')}\n\n"
                        f"Would you like me to analyze how this impacts your plans?\n\n"
                        f"Reply to this email or visit https://www.chinawestconnector.com\n\n"
                        f"Best regards,\nSophia — CWC AI Advisor"
                    )
                except Exception as e:
                    print(f"Notification error for {user['email']}: {e}")

# ============================================================
# v7.1: META-COGNITIVE LAYER
# ============================================================
class MetaCognitiveLayer:
    def __init__(self, memory: AgenticMemory):
        self.memory = memory
        self.confidence_threshold = 0.7
    
    def assess_confidence(self, response: str, user_msg: str, context: dict, tool_calls: List[str]) -> Dict:
        confidence = 0.5
        reasons = []
        gaps = []
        
        if len(response) < 50:
            confidence -= 0.1
            reasons.append("response too short")
        elif len(response) > 300:
            confidence += 0.1
            reasons.append("comprehensive response")
        
        if re.search(r'\d+%|\d+ dollars|\d+ yuan|\d+\.\d+', response):
            confidence += 0.15
            reasons.append("contains specific numbers")
        else:
            confidence -= 0.05
            gaps.append("no quantitative data")
        
        if re.search(r'according to|source:|tavily|research shows', response.lower()):
            confidence += 0.15
            reasons.append("cites sources")
        else:
            confidence -= 0.05
            gaps.append("missing sources")
        
        intent = context.get('intent', 'general')
        if intent in response.lower():
            confidence += 0.1
            reasons.append("addresses user intent")
        
        if tool_calls:
            confidence += 0.1
            reasons.append(f"used {len(tool_calls)} tools")
        
        similar = self.memory.recall_similar_episodes(user_msg, n_results=3)
        if similar:
            avg_success = sum(s.get('metadata', {}).get('success_score', 5) for s in similar) / len(similar)
            if avg_success >= 7:
                confidence += 0.1
                reasons.append("similar to past successes")
            elif avg_success <= 4:
                confidence -= 0.1
                gaps.append("similar to past failures")
        
        confidence = max(0.1, min(1.0, confidence))
        
        if confidence < self.confidence_threshold:
            action = "ASK_FOLLOWUP"
            suggestion = self._generate_followup_question(user_msg, gaps)
        elif confidence < 0.85:
            action = "PROCEED_WITH_DISCLAIMER"
            suggestion = "I should add a disclaimer about limitations"
        else:
            action = "PROCEED_CONFIDENTLY"
            suggestion = None
        
        return {
            'confidence': round(confidence, 2),
            'action': action,
            'reasons': reasons,
            'gaps': gaps,
            'suggestion': suggestion,
            'needs_followup': confidence < self.confidence_threshold
        }
    
    def _generate_followup_question(self, user_msg: str, gaps: List[str]) -> str:
        if "no quantitative data" in gaps:
            return "Could you provide more specific details about volumes or budgets?"
        elif "missing sources" in gaps:
            return "Would you like me to search for specific sources on this?"
        else:
            return "Could you clarify your specific requirements so I can provide more targeted advice?"

# Initialize meta-cognitive layer
meta_cognitive = MetaCognitiveLayer(agentic_memory)

# ============================================================
# v7.1: COLLABORATIVE AGENT ORCHESTRATOR — WEIGHTED CONSENSUS
# ============================================================
class AgentOrchestrator:
    """Runs multiple agents in parallel with weighted consensus voting"""
    
    def __init__(self):
        self.agents = {
            'researcher': self._research_agent,
            'verifier': self._verifier_agent,
            'strategist': self._strategist_agent,
            'legal': self._legal_agent
        }
        # Agent expertise weights for different intents
        self.agent_weights = {
            'supplier_verification': {'verifier': 1.5, 'legal': 1.2, 'researcher': 1.0, 'strategist': 0.5},
            'supplier_search': {'researcher': 1.5, 'strategist': 1.3, 'verifier': 0.8, 'legal': 0.5},
            'market_entry': {'strategist': 1.5, 'legal': 1.3, 'researcher': 1.0, 'verifier': 0.5},
            'due_diligence': {'verifier': 1.5, 'researcher': 1.2, 'legal': 1.0, 'strategist': 0.5},
            'consultation': {'strategist': 1.4, 'legal': 1.2, 'researcher': 1.0, 'verifier': 0.5},
            'general': {'researcher': 1.2, 'strategist': 1.2, 'verifier': 1.0, 'legal': 1.0}
        }
    
    async def parallel_execute(self, task: str, context: dict, user_msg: str, session_id: str) -> Dict:
        """Run relevant agents in parallel and synthesize results with weighted voting"""
        intent = context.get('intent', 'general')
        agents_to_run = []
        
        if intent in ['supplier_verification', 'due_diligence']:
            agents_to_run = ['researcher', 'verifier', 'legal']
        elif intent in ['supplier_search', 'sourcing']:
            agents_to_run = ['researcher', 'strategist']
        elif intent in ['market_entry', 'consultation']:
            agents_to_run = ['researcher', 'strategist', 'legal']
        else:
            agents_to_run = ['researcher', 'strategist']
        
        tasks = []
        for agent_name in agents_to_run:
            if agent_name in self.agents:
                tasks.append(self.agents[agent_name](task, context, user_msg))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        agent_outputs = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                print(f"Agent {agents_to_run[i]} failed: {result}")
                continue
            confidence = self._assess_output_confidence(result, agents_to_run[i], intent)
            agent_outputs.append({
                'agent': agents_to_run[i],
                'output': result,
                'confidence': confidence,
                'weight': self.agent_weights.get(intent, self.agent_weights['general']).get(agents_to_run[i], 1.0)
            })
        
        consensus = self._reach_consensus(agent_outputs, intent, user_msg)
        
        return {
            'consensus': consensus,
            'agent_outputs': agent_outputs,
            'agents_used': len(agent_outputs),
            'voting_method': 'weighted_expertise'
        }
    
    def _assess_output_confidence(self, output: str, agent_type: str, intent: str) -> float:
        """Calculate confidence score for an agent's output"""
        confidence = 0.5
        
        if len(output) > 200:
            confidence += 0.15
        elif len(output) < 50:
            confidence -= 0.1
        
        if re.search(r'\d+%|\d+ dollars|\d+ yuan|\d+\.\d+', output):
            confidence += 0.15
        
        if re.search(r'according to|source:|research shows|data from', output.lower()):
            confidence += 0.1
        
        if any(marker in output for marker in ['①', '②', '③', '1.', '2.', '3.', '•']):
            confidence += 0.1
        
        domain_expertise = {
            'verifier': ['supplier_verification', 'due_diligence'],
            'strategist': ['market_entry', 'consultation'],
            'legal': ['contract', 'ip', 'compliance'],
            'researcher': ['general', 'information_gathering']
        }
        if agent_type in domain_expertise:
            if intent in domain_expertise[agent_type]:
                confidence += 0.2
        
        return min(1.0, max(0.1, confidence))
    
    def _reach_consensus(self, agent_outputs: List[dict], intent: str, user_msg: str) -> str:
        """Synthesize multiple agent outputs using weighted voting and conflict resolution"""
        if not agent_outputs:
            return "Unable to generate consensus - no agent outputs"
        
        if len(agent_outputs) == 1:
            return f"[{agent_outputs[0]['agent'].upper()} ANALYSIS]\n\n{agent_outputs[0]['output']}"
        
        weighted_outputs = []
        for output in agent_outputs:
            weighted_score = output['confidence'] * output['weight']
            weighted_outputs.append({**output, 'weighted_score': weighted_score})
        
        weighted_outputs.sort(key=lambda x: x['weighted_score'], reverse=True)
        
        if len(weighted_outputs) >= 2:
            top_confidence = weighted_outputs[0]['confidence']
            second_confidence = weighted_outputs[1]['confidence']
            if top_confidence - second_confidence > 0.3:
                winner = weighted_outputs[0]
                return self._format_consensus(winner, weighted_outputs, dominant=True)
        
        return self._synthesize_outputs(weighted_outputs, intent, user_msg)
    
    def _format_consensus(self, winner: dict, all_outputs: List[dict], dominant: bool = False) -> str:
        if dominant:
            consensus = f"""━━━ CONSENSUS ANALYSIS (Weighted Voting) ━━━
Primary Analysis: {winner['agent'].upper()} (confidence: {winner['confidence']:.0%}, weight: {winner['weight']:.1f}x)

{winner['output']}

━━━ SUPPORTING PERSPECTIVES ━━━"""
            for output in all_outputs[1:]:
                if output['confidence'] > 0.4:
                    consensus += f"\n\n[{output['agent'].upper()} - confidence: {output['confidence']:.0%}]\n{output['output'][:200]}..."
            return consensus
        else:
            return self._synthesize_outputs(all_outputs, "general", "")
    
    def _synthesize_outputs(self, weighted_outputs: List[dict], intent: str, user_msg: str) -> str:
        synthesis_input = f"""Synthesize the following expert analyses into a single coherent response.

User Query: {user_msg}
Intent: {intent}

Expert Analyses (ranked by confidence and expertise):
"""
        for i, output in enumerate(weighted_outputs, 1):
            synthesis_input += f"""
--- {i}. {output['agent'].upper()} (confidence: {output['confidence']:.0%}, expertise weight: {output['weight']:.1f}x) ---
{output['output']}
"""
        synthesis_input += """

Instructions:
1. Synthesize these perspectives into ONE coherent response
2. Prioritize higher-confidence analyses but acknowledge key points from others
3. Flag any significant disagreements between experts
4. Provide a clear, actionable answer
5. Keep under 300 words
"""
        # Return formatted output without additional AI call (to save API calls)
        parts = [f"[{o['agent'].upper()} - {o['confidence']:.0%} confidence]\n{o['output']}" for o in weighted_outputs]
        return "\n\n".join(parts)
    
    async def _research_agent(self, task: str, context: dict, user_msg: str) -> str:
        if not ai_provider.providers:
            return "Research unavailable"
        try:
            messages = [
                {"role": "system", "content": "You are a research specialist. Gather facts, data, and market intelligence. Be thorough and cite sources. Always include specific numbers and data points when available."},
                {"role": "user", "content": f"Research task: {task}\nUser query: {user_msg}\nContext: {json.dumps(context)}"}
            ]
            res = await ai_provider.chat_completion(messages, temperature=0.2, max_tokens=400)
            return res["choices"][0]["message"]["content"]
        except Exception as e:
            return f"Research error: {e}"
    
    async def _verifier_agent(self, task: str, context: dict, user_msg: str) -> str:
        if not ai_provider.providers:
            return "Verification unavailable"
        try:
            messages = [
                {"role": "system", "content": "You are a due diligence specialist. Verify claims, flag risks, and identify red flags. Be skeptical. Always quantify risk levels (low/medium/high) and explain why."},
                {"role": "user", "content": f"Verification task: {task}\nUser query: {user_msg}\nContext: {json.dumps(context)}"}
            ]
            res = await ai_provider.chat_completion(messages, temperature=0.1, max_tokens=400)
            return res["choices"][0]["message"]["content"]
        except Exception as e:
            return f"Verification error: {e}"
    
    async def _strategist_agent(self, task: str, context: dict, user_msg: str) -> str:
        if not ai_provider.providers:
            return "Strategy unavailable"
        try:
            messages = [
                {"role": "system", "content": "You are a business strategist. Recommend specific actions, timelines, and next steps. Be practical. Always include concrete next steps with timeframes."},
                {"role": "user", "content": f"Strategy task: {task}\nUser query: {user_msg}\nContext: {json.dumps(context)}"}
            ]
            res = await ai_provider.chat_completion(messages, temperature=0.3, max_tokens=400)
            return res["choices"][0]["message"]["content"]
        except Exception as e:
            return f"Strategy error: {e}"
    
    async def _legal_agent(self, task: str, context: dict, user_msg: str) -> str:
        if not ai_provider.providers:
            return "Legal analysis unavailable"
        try:
            messages = [
                {"role": "system", "content": "You are a China business lawyer. Address compliance, contracts, IP, and legal structures. Be precise. Always flag common Western mistakes in China contracts."},
                {"role": "user", "content": f"Legal task: {task}\nUser query: {user_msg}\nContext: {json.dumps(context)}"}
            ]
            res = await ai_provider.chat_completion(messages, temperature=0.1, max_tokens=400)
            return res["choices"][0]["message"]["content"]
        except Exception as e:
            return f"Legal error: {e}"

# Initialize agent orchestrator
agent_orchestrator = AgentOrchestrator()

# ============================================================
# v7.1: TOOL REGISTRY
# ============================================================
class ToolRegistry:
    def __init__(self):
        self.tools = {}
        self.load_registered_tools()
    
    def load_registered_tools(self):
        try:
            conn = get_db()
            c = conn.cursor()
            c.execute("SELECT tool_name, description, implementation FROM tool_registry WHERE deployed = TRUE")
            for name, desc, impl in c.fetchall():
                self.tools[name] = {'description': desc, 'implementation': impl}
            conn.close()
            print(f"🔧 Loaded {len(self.tools)} registered tools")
        except Exception as e:
            print(f"Tool registry load error: {e}")
    
    def register_tool(self, name: str, description: str, implementation: str, created_by: str = "sophia"):
        try:
            conn = get_db()
            c = conn.cursor()
            c.execute("""
                INSERT INTO tool_registry (tool_name, description, implementation, created_by, created_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (tool_name) DO UPDATE 
                SET description = EXCLUDED.description,
                    implementation = EXCLUDED.implementation
            """, (name, description, implementation, created_by, datetime.now()))
            conn.commit()
            conn.close()
            self.tools[name] = {'description': description, 'implementation': implementation}
            print(f"✅ Registered new tool: {name}")
            return True
        except Exception as e:
            print(f"Tool registration error: {e}")
            return False
    
    def execute_tool(self, name: str, args: dict) -> str:
        if name not in self.tools:
            return f"Tool '{name}' not found"
        try:
            impl = self.tools[name]['implementation']
            locals_dict = {'args': args, 'result': ''}
            exec(impl, {}, locals_dict)
            return locals_dict.get('result', 'Tool executed successfully')
        except Exception as e:
            return f"Tool execution error: {e}"

# Initialize tool registry
tool_registry = ToolRegistry()

# ============================================================
# v7.1: PREDICTIVE INTENT ENGINE
# ============================================================
class PredictiveIntentEngine:
    def __init__(self, memory: AgenticMemory):
        self.memory = memory
        self.patterns = defaultdict(lambda: defaultdict(int))
        self.load_patterns()
    
    def load_patterns(self):
        try:
            conn = get_db()
            c = conn.cursor()
            c.execute("""
                SELECT session_id, intent 
                FROM conversations 
                ORDER BY timestamp
            """)
            sessions = defaultdict(list)
            for sid, intent in c.fetchall():
                if intent:
                    sessions[sid].append(intent)
            for intents in sessions.values():
                for i in range(len(intents) - 1):
                    self.patterns[intents[i]][intents[i+1]] += 1
            conn.close()
            print(f"📊 Loaded intent patterns for {len(sessions)} sessions")
        except Exception as e:
            print(f"Pattern loading error: {e}")
    
    def predict_next_intent(self, current_intent: str, user_profile: dict) -> Dict:
        predictions = []
        if current_intent in self.patterns:
            transitions = self.patterns[current_intent]
            total = sum(transitions.values())
            for next_intent, count in sorted(transitions.items(), key=lambda x: x[1], reverse=True)[:3]:
                probability = count / total
                predictions.append({'intent': next_intent, 'probability': round(probability, 2), 'source': 'pattern'})
        if user_profile.get('task_history'):
            goals = [g for g in user_profile['task_history'] if g.get('status') in ('pending', 'in_progress')]
            for goal in goals:
                if 'verify' in goal.get('goal', '').lower():
                    predictions.append({'intent': 'supplier_verification', 'probability': 0.8, 'source': 'goal'})
                elif 'source' in goal.get('goal', '').lower():
                    predictions.append({'intent': 'supplier_search', 'probability': 0.8, 'source': 'goal'})
        seen = set()
        unique_predictions = []
        for p in predictions:
            if p['intent'] not in seen:
                seen.add(p['intent'])
                unique_predictions.append(p)
        return {'current_intent': current_intent, 'predictions': unique_predictions[:3], 'should_prepare': len(unique_predictions) > 0}

# Initialize predictive intent engine
predictive_engine = PredictiveIntentEngine(agentic_memory)

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
# v7.1: SELF-TRIGGERED TASKS
# ============================================================
def detect_self_triggers(user_message: str, intent: str, user_profile: dict, 
                         conversation_history: List[tuple]) -> List[Dict]:
    """Detect opportunities for self-triggered background tasks"""
    triggers = []
    msg_lower = user_message.lower()
    
    # Company mention trigger
    company_patterns = [
        r'(?:company|supplier|manufacturer|factory|vendor)\s+(?:called|named)?\s*["\']?([A-Z][A-Za-z0-9\s&]+)["\']?',
        r'(?:working with|partnering with|considering)\s+([A-Z][A-Za-z0-9\s&]{2,30})',
        r'([A-Z][A-Za-z0-9\s&]{2,30})\s+(?:from China|Chinese|in China|based in)',
    ]
    for pattern in company_patterns:
        matches = re.findall(pattern, user_message, re.IGNORECASE)
        for company in matches:
            company = company.strip()
            if len(company) > 2 and company.lower() not in ['china', 'chinese', 'the company']:
                triggers.append({
                    'type': 'monitor_company',
                    'task_description': f"Monitor {company} for changes and risks",
                    'priority': 8 if intent == 'supplier_verification' else 6,
                    'reason': f"User mentioned {company}",
                    'context': {'company_name': company}
                })
    
    # Urgency trigger
    urgency_keywords = ['asap', 'urgent', 'this week', 'next week', 'immediately', 'deadline']
    if any(kw in msg_lower for kw in urgency_keywords):
        triggers.append({
            'type': 'expedite_research',
            'task_description': f"Expedited research for {intent}",
            'priority': 9,
            'reason': 'Urgent timeline detected',
            'context': {'intent': intent}
        })
    
    triggers.sort(key=lambda x: x['priority'], reverse=True)
    return triggers[:3]

# ============================================================
# SOPHIA SYSTEM PROMPT
# ============================================================
SOPHIA_SYSTEM_PROMPT = """You are Sophia, the AI advisor for China West Connector (CWC). You help Western businesses navigate China trade and Chinese companies expand West.

Your expertise includes:
- Supplier verification and due diligence
- Market entry strategies (WFOE, JV, RO)
- Import/export logistics and regulations
- Contract negotiation and IP protection
- China business culture and practices

Key guidelines:
1. Be specific with numbers, costs, and timelines
2. Always mention risks and mitigation strategies
3. Provide actionable next steps
4. Ask clarifying questions when needed
5. Structure responses clearly (use ① ② ③ for lists)

If asked about specific company verification, explain you can provide general guidance but detailed verification requires professional services.

Respond professionally and helpfully. If the query is outside your expertise, acknowledge limitations and suggest alternatives."""

# ============================================================
# INTENT DETECTION
# ============================================================
def detect_intent(message: str) -> str:
    """Detect user intent from message"""
    message_lower = message.lower()
    
    if any(kw in message_lower for kw in ['verify', 'check company', 'legitimate', 'scam', 'fraud', 'due diligence']):
        return 'supplier_verification'
    elif any(kw in message_lower for kw in ['find supplier', 'source', 'manufacturer', 'factory']):
        return 'supplier_search'
    elif any(kw in message_lower for kw in ['market entry', 'wfoe', 'jv', 'joint venture', 'set up', 'register']):
        return 'market_entry'
    elif any(kw in message_lower for kw in ['ship', 'customs', 'import', 'export', 'freight', 'logistics']):
        return 'logistics'
    elif any(kw in message_lower for kw in ['contract', 'legal', 'ip', 'intellectual property']):
        return 'legal'
    elif any(kw in message_lower for kw in ['price', 'cost', 'quote', 'how much']):
        return 'pricing'
    else:
        return 'general'

# ============================================================
# HTN METHODS AND AGENT CAPABILITIES
# ============================================================
HTN_METHODS = {
    "handle_supplier_request": [
        {
            "name": "verify_then_source",
            "precondition": lambda ctx: ctx.get("intent") == "supplier_verification" or ctx.get("company_name"),
            "subtasks": [
                {"task": "lookup_company", "params": ["company_name"], "agent": "due_diligence"},
                {"task": "generate_risk_report", "params": ["company_name", "context"], "agent": "due_diligence"}
            ]
        },
        {
            "name": "source_new_suppliers",
            "precondition": lambda ctx: ctx.get("intent") == "supplier_search",
            "subtasks": [
                {"task": "search_suppliers", "params": ["product_or_sector", "region"], "agent": "supplier_match"}
            ]
        }
    ],
    "market_entry_strategy": [
        {
            "name": "wfoe_path",
            "precondition": lambda ctx: ctx.get("direction") == "west_to_china",
            "subtasks": [
                {"task": "check_sector_restrictions", "params": ["sector"], "agent": "market_entry"}
            ]
        }
    ]
}

AGENT_CAPABILITIES = {
    "due_diligence": {
        "can_handle": ["verify_company", "risk_assessment", "lookup_company", "generate_risk_report"],
        "expertise": "Chinese company verification, red flag detection"
    },
    "market_entry": {
        "can_handle": ["entity_setup", "regulatory_guide", "check_sector_restrictions"],
        "expertise": "WFOE/JV setup, market entry strategy"
    },
    "supplier_match": {
        "can_handle": ["sourcing", "search_suppliers"],
        "expertise": "Supplier identification, qualification"
    }
}

# ============================================================
# CONVERSATION HANDLER
# ============================================================
async def process_chat(session_id: str, user_message: str, use_multi_agent: bool = False) -> Dict:
    """Process user chat and return AI response"""
    
    if not ai_provider.providers:
        return {
            "response": "I'm currently offline - no AI providers are configured. Please contact support.",
            "intent": "error",
            "success": False
        }
    
    try:
        # Get or create user profile
        user_profile = get_or_create_user_profile(session_id)
        
        # Detect intent
        intent = detect_intent(user_message)
        
        # Update user profile with intent
        update_user_profile(session_id, last_intent=intent)
        
        # Build context
        context = {
            'intent': intent,
            'session_id': session_id,
            'user_profile': user_profile
        }
        
        # Check for multi-agent mode for complex queries
        if use_multi_agent and intent in ['supplier_verification', 'market_entry', 'due_diligence']:
            # Use multi-agent orchestration
            result = await agent_orchestrator.parallel_execute(
                task="analyze_query",
                context=context,
                user_msg=user_message,
                session_id=session_id
            )
            ai_response = result['consensus']
        else:
            # Standard single-agent response
            messages = [
                {"role": "system", "content": SOPHIA_SYSTEM_PROMPT},
                {"role": "user", "content": user_message}
            ]
            
            result = await ai_provider.chat_completion(
                messages, 
                model_type='default',
                temperature=0.7,
                max_tokens=800
            )
            
            ai_response = result["choices"][0]["message"]["content"]
        
        # Store conversation
        try:
            conn = get_db()
            c = conn.cursor()
            c.execute("""
                INSERT INTO conversations (session_id, user_message, ai_response, intent)
                VALUES (%s, %s, %s, %s)
            """, (session_id, user_message, ai_response, intent))
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"⚠️ Failed to store conversation: {e}")
        
        # Store in memory
        agentic_memory.store_episodic(session_id, user_message, ai_response, 7, intent)
        
        # Detect self-triggers
        triggers = detect_self_triggers(user_message, intent, user_profile, [])
        if triggers:
            print(f"🔔 Detected {len(triggers)} self-trigger(s)")
        
        # Meta-cognitive assessment
        confidence = meta_cognitive.assess_confidence(ai_response, user_message, context, [])
        
        return {
            "response": ai_response,
            "intent": intent,
            "success": True,
            "provider": ai_provider.get_current_provider()['name'] if ai_provider.get_current_provider() else 'unknown',
            "confidence": confidence,
            "triggers_detected": len(triggers)
        }
        
    except Exception as e:
        print(f"Chat error: {e}")
        return {
            "response": f"I encountered an error processing your request. Please try again or contact support if the issue persists. Error: {str(e)[:100]}",
            "intent": "error",
            "success": False
        }

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

# ============================================================
# FASTAPI APP
# ============================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """App lifespan - startup and shutdown"""
    # Startup
    print("🚀 Starting Sophia AI Server v8.0...")
    init_db()
    print("✅ Server ready!")
    yield
    # Shutdown
    print("👋 Shutting down...")

app = FastAPI(
    title="Sophia AI - China West Connector",
    description="AI-powered advisor for China-West business with Memory, Multi-Agent Orchestration, Self-Improvement",
    version="8.0",
    lifespan=lifespan
)

# CORS middleware
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
    """Root endpoint"""
    return {
        "name": "Sophia AI",
        "version": "8.0",
        "status": "online",
        "features": {
            "memory": agentic_memory.initialized,
            "multi_agent": True,
            "self_improvement": True,
            "predictive_intent": True
        },
        "providers": len(ai_provider.providers),
        "message": "Welcome to China West Connector AI Advisor"
    }

@app.get("/health")
async def health():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "providers": len(ai_provider.providers),
        "openrouter": bool(OPENROUTER_API_KEY),
        "cloudflare": bool(CLOUDFLARE_API_KEY and CLOUDFLARE_ACCOUNT_ID),
        "database": bool(DATABASE_URL),
        "memory": agentic_memory.initialized
    }

@app.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest, http_request: Request):
    """Main chat endpoint"""
    # Get client IP
    client_ip = http_request.client.host
    
    # Rate limiting
    if is_rate_limited(client_ip):
        raise HTTPException(status_code=429, detail="Rate limit exceeded. Please wait a moment.")
    
    # Get or create session ID
    session_id = request.session_id or str(uuid.uuid4())
    
    # Process message
    result = await process_chat(session_id, request.message, request.use_multi_agent)
    
    return ChatResponse(
        response=result["response"],
        intent=result["intent"],
        success=result["success"],
        session_id=session_id,
        provider=result.get("provider"),
        confidence=result.get("confidence")
    )

@app.get("/admin/status")
async def admin_status(password: str = ""):
    """Admin status endpoint"""
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid password")
    
    try:
        conn = get_db()
        c = conn.cursor()
        
        # Get conversation count
        c.execute("SELECT COUNT(*) FROM conversations")
        conv_count = c.fetchone()[0]
        
        # Get user count
        c.execute("SELECT COUNT(*) FROM user_profiles")
        user_count = c.fetchone()[0]
        
        # Get recent conversations
        c.execute("""
            SELECT user_message, ai_response, intent, timestamp 
            FROM conversations 
            ORDER BY timestamp DESC 
            LIMIT 5
        """)
        recent = c.fetchall()
        
        conn.close()
        
        return {
            "total_conversations": conv_count,
            "total_users": user_count,
            "ai_providers": [p['name'] for p in ai_provider.providers],
            "memory_initialized": agentic_memory.initialized,
            "recent_conversations": [
                {
                    "user": r[0][:100],
                    "response": r[1][:100],
                    "intent": r[2],
                    "time": str(r[3])
                } for r in recent
            ]
        }
    except Exception as e:
        return {"error": str(e)}

@app.get("/admin/conversations")
async def admin_conversations(password: str = "", limit: int = 50):
    """Get conversation history"""
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid password")
    
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("""
            SELECT session_id, user_message, ai_response, intent, timestamp 
            FROM conversations 
            ORDER BY timestamp DESC 
            LIMIT %s
        """, (limit,))
        rows = c.fetchall()
        conn.close()
        
        return {
            "conversations": [
                {
                    "session_id": r[0],
                    "user_message": r[1],
                    "ai_response": r[2],
                    "intent": r[3],
                    "timestamp": str(r[4])
                } for r in rows
            ]
        }
    except Exception as e:
        return {"error": str(e)}

@app.post("/admin/self-improve")
async def trigger_self_improvement(password: str = ""):
    """Trigger self-improvement analysis"""
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid password")
    
    await self_improvement.analyze_performance()
    return {"status": "Self-improvement analysis triggered"}

@app.post("/multi-agent/chat")
async def multi_agent_chat(request: ChatRequest, http_request: Request):
    """Multi-agent chat endpoint for complex queries"""
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

# For running directly
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
