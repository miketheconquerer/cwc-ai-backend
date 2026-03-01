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
import asyncio
import numpy as np
from collections import defaultdict
from datetime import datetime, timedelta
from dotenv import load_dotenv
from contextlib import asynccontextmanager
from typing import List, Dict, Any, Optional, Tuple
import uuid

# v8.0: 100% FREE AI - OpenRouter + Cloudflare Backup
try:
    from sentence_transformers import SentenceTransformer
    import chromadb
    from chromadb.config import Settings
    from sklearn.metrics.pairwise import cosine_similarity
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
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")  # Primary: 50/day free
CLOUDFLARE_API_KEY = os.getenv("CLOUDFLARE_API_KEY", "")  # Backup: 10K neurons/day
CLOUDFLARE_ACCOUNT_ID = os.getenv("CLOUDFLARE_ACCOUNT_ID", "")

# Legacy support (not used but kept for compatibility)
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")

ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")

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
            raise ValueError("No free AI providers configured! Set OPENROUTER_API_KEY or CLOUDFLARE_API_KEY")
        
        print(f"🎯 Total providers: {len(self.providers)}")
    
    def get_current_provider(self):
        return self.providers[self.current_provider]
    
    def switch_provider(self):
        """Switch to next available provider on failure"""
        self.current_provider = (self.current_provider + 1) % len(self.providers)
        provider = self.get_current_provider()
        print(f"🔄 Switched to backup provider: {provider['name']}")
        return provider
    
    async def chat_completion(self, messages, model_type='default', temperature=0.3, max_tokens=1000, tools=None, tool_choice=None):
        """Try current provider, fallback to next on failure"""
        last_error = None
        
        for attempt in range(len(self.providers)):
            provider = self.get_current_provider()
            
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
                
                # Don't retry if it's a rate limit (wait for next day)
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
        
        response = requests.post(provider['endpoint'], headers=provider['headers'], json=payload, timeout=30)
        
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
        
        response = requests.post(url, headers=provider['headers'], json=payload, timeout=30)
        
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
try:
    ai_provider = FreeAIProvider()
except ValueError as e:
    print(f"🔴 CRITICAL: {e}")
    ai_provider = None

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
        if not ai_provider:
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
        if ai_provider:
            try:
                messages = [
                    {"role": "system", "content": "You are a synthesis expert. Combine multiple expert opinions into one clear, actionable response."},
                    {"role": "user", "content": synthesis_input}
                ]
                res = asyncio.run(ai_provider.chat_completion(messages, temperature=0.2, max_tokens=500))
                synthesized = res["choices"][0]["message"]["content"]
                agent_list = ", ".join([f"{o['agent']} ({o['confidence']:.0%})" for o in weighted_outputs])
                return f"""━━━ MULTI-AGENT SYNTHESIS ━━━
Contributing Agents: {agent_list}

{synthesized}

━━━ END SYNTHESIS ━━━"""
            except Exception as e:
                print(f"Synthesis error: {e}")
        
        parts = [f"[{o['agent'].upper()} - {o['confidence']:.0%} confidence]\n{o['output']}" for o in weighted_outputs]
        return "\n\n".join(parts)
    
    async def _research_agent(self, task: str, context: dict, user_msg: str) -> str:
        if not ai_provider:
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
        if not ai_provider:
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
        if not ai_provider:
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
        if not ai_provider:
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
# v7.1: SELF-TRIGGERED TASKS — PROACTIVE INTELLIGENCE
# ============================================================
def detect_self_triggers(user_message: str, intent: str, user_profile: dict, 
                         conversation_history: List[tuple]) -> List[Dict]:
    """Detect opportunities for self-triggered background tasks based on conversation context."""
    triggers = []
    msg_lower = user_message.lower()
    
    # Trigger 1: Company mentioned but not verified
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
                if not _has_recent_company_data(user_profile, company):
                    triggers.append({
                        'type': 'monitor_company',
                        'task_description': f"Monitor {company} for changes and risks",
                        'priority': 8 if intent == 'supplier_verification' else 6,
                        'reason': f"User mentioned {company} - proactive monitoring recommended",
                        'context': {'company_name': company, 'mentioned_in': user_message[:100]}
                    })
    
    # Trigger 2: Timeline urgency detected
    urgency_keywords = ['asap', 'urgent', 'this week', 'next week', 'immediately', 'deadline', 'launching soon']
    if any(kw in msg_lower for kw in urgency_keywords):
        if intent in ['supplier_search', 'market_entry']:
            triggers.append({
                'type': 'expedite_research',
                'task_description': f"Expedited research: {intent} with urgency flag",
                'priority': 9,
                'reason': 'Urgent timeline detected - prepare comprehensive briefing',
                'context': {'intent': intent, 'urgency_signals': [kw for kw in urgency_keywords if kw in msg_lower]}
            })
    
    # Trigger 3: Sector/region interest without followup
    if user_profile.get('visit_count', 0) >= 2:
        if intent == 'information_gathering' and user_profile.get('lead_score', 0) > 30:
            sector = user_profile.get('topics_discussed') or 'general'
            region = user_profile.get('region_interest') or 'China'
            triggers.append({
                'type': 'market_intelligence',
                'task_description': f"Weekly market intel: {sector} in {region}",
                'priority': 5,
                'reason': f"Returning user ({user_profile['visit_count']} visits) - proactive intelligence",
                'context': {'sector': sector, 'region': region}
            })
    
    # Trigger 4: Regulatory topic mentioned
    regulatory_keywords = ['regulation', 'compliance', 'license', 'permit', 'certification', 'fdi', 'wfoe']
    if any(kw in msg_lower for kw in regulatory_keywords):
        triggers.append({
            'type': 'regulatory_watch',
            'task_description': f"Monitor regulatory changes: {next(kw for kw in regulatory_keywords if kw in msg_lower)}",
            'priority': 7,
            'reason': 'Regulatory topic mentioned - monitor for updates',
            'context': {'topic': next(kw for kw in regulatory_keywords if kw in msg_lower)}
        })
    
    # Trigger 5: Price/budget discussion
    price_keywords = ['budget', 'cost', 'price', 'investment', 'how much', 'fee']
    if any(kw in msg_lower for kw in price_keywords):
        if intent == 'high_intent_lead':
            triggers.append({
                'type': 'pricing_research',
                'task_description': f"Prepare pricing analysis for {user_profile.get('topics_discussed', 'relevant services')}",
                'priority': 8,
                'reason': 'High-intent lead asking about pricing - prepare tailored proposal',
                'context': {'sector': user_profile.get('topics_discussed'), 'region': user_profile.get('region_interest')}
            })
    
    # Trigger 6: Competitor/rival mentioned
    competitor_patterns = [
        r'(?:competitor|competition|rival)\s+(?:is|are)?\s*["\']?([A-Z][A-Za-z0-9\s&]+)["\']?',
        r'(?:competing with|against)\s+([A-Z][A-Za-z0-9\s&]{2,30})',
    ]
    for pattern in competitor_patterns:
        matches = re.findall(pattern, user_message, re.IGNORECASE)
        for competitor in matches:
            triggers.append({
                'type': 'competitive_intelligence',
                'task_description': f"Research competitor: {competitor.strip()}",
                'priority': 6,
                'reason': f"Competitor {competitor.strip()} mentioned",
                'context': {'competitor': competitor.strip()}
            })
    
    triggers.sort(key=lambda x: x['priority'], reverse=True)
    return triggers[:3]


def _has_recent_company_data(user_profile: dict, company_name: str) -> bool:
    key_facts = user_profile.get('key_facts', {})
    suppliers = key_facts.get('supplier_names', [])
    if company_name in suppliers:
        return True
    task_history = user_profile.get('task_history', [])
    for task in task_history[-5:]:
        if company_name.lower() in task.get('goal', '').lower():
            return True
    return False


def queue_self_triggered_task(session_id: str, trigger: Dict, user_profile: dict) -> bool:
    """Queue a self-triggered background task based on detected trigger."""
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("""
            SELECT id FROM agent_tasks 
            WHERE session_id = %s 
            AND task_description LIKE %s 
            AND status IN ('pending', 'running')
            AND created_at > NOW() - INTERVAL '24 hours'
        """, (session_id, f"%{trigger['context'].get('company_name', '')}%"))
        if c.fetchone():
            print(f"⏭️ Similar task already queued for {session_id}")
            conn.close()
            return False
        
        task_desc = trigger['task_description']
        if trigger['context']:
            context_json = json.dumps(trigger['context'])
            task_desc += f" | Context: {context_json}"
        
        c.execute("""
            INSERT INTO agent_tasks (session_id, task_description, status, sub_tasks, created_at)
            VALUES (%s, %s, 'pending', %s::jsonb, %s)
            RETURNING id
        """, (session_id, task_desc, json.dumps([{'type': trigger['type'], 'priority': trigger['priority'], 'reason': trigger['reason']}]), datetime.now()))
        task_id = c.fetchone()[0]
        conn.commit()
        conn.close()
        print(f"🤖 Self-triggered task #{task_id} queued: {trigger['task_description'][:60]}...")
        _store_pending_notification(session_id, trigger)
        return True
    except Exception as e:
        print(f"Self-trigger queue error: {e}")
        return False


def _store_pending_notification(session_id: str, trigger: Dict):
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("""
            INSERT INTO user_profiles (session_id, key_facts, first_seen, last_seen, visit_count)
            VALUES (%s, %s::jsonb, NOW(), NOW(), 1)
            ON CONFLICT (session_id) 
            DO UPDATE SET key_facts = jsonb_set(
                COALESCE(user_profiles.key_facts, '{}'::jsonb),
                '{pending_notification}',
                %s::jsonb
            )
        """, (session_id, json.dumps({'pending_notification': {'task_type': trigger['type'], 'message': f"I'm also researching: {trigger['reason']}", 'created_at': datetime.now().isoformat()}}), json.dumps({'task_type': trigger['type'], 'message': f"I'm also researching: {trigger['reason']}", 'created_at': datetime.now().isoformat()})))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Store notification error: {e}")


def get_and_clear_notification(session_id: str) -> str:
    try:
        profile = get_or_create_user_profile(session_id)
        key_facts = profile.get('key_facts', {})
        notification = key_facts.get('pending_notification')
        if notification:
            key_facts.pop('pending_notification', None)
            update_user_profile(session_id, key_facts=key_facts)
            return notification.get('message', '')
        return ''
    except Exception as e:
        print(f"Get notification error: {e}")
        return ''

# ============================================================
# v7.1: PROCEDURAL MEMORY WORKFLOWS
# ============================================================
PROCEDURAL_WORKFLOWS = {
    "supplier_verification_checklist": [
        {"step": 1, "action": "lookup_company", "param": "company_name", "critical": True},
        {"step": 2, "action": "search_web", "param": "company_name + fraud/scam/complaints", "critical": True},
        {"step": 3, "action": "generate_risk_report", "param": "company_name", "critical": False},
        {"step": 4, "action": "escalate_if_risk_detected", "condition": "warning_detected", "critical": True}
    ],
    "market_entry_roadmap": [
        {"step": 1, "action": "identify_direction", "param": "user_profile.region_interest", "critical": True},
        {"step": 2, "action": "search_regulations", "param": "sector + target_region", "critical": True},
        {"step": 3, "action": "estimate_timeline", "param": "entity_type", "critical": False},
        {"step": 4, "action": "calculate_costs", "param": "setup_type", "critical": False}
    ],
    "new_lead_qualification": [
        {"step": 1, "action": "detect_language", "param": "first_message", "critical": True},
        {"step": 2, "action": "ask_direction", "param": "west_to_china_or_china_to_west", "critical": True},
        {"step": 3, "action": "identify_sector", "param": "industry_keywords", "critical": True},
        {"step": 4, "action": "assess_urgency", "param": "timeline_keywords", "critical": False}
    ]
}

def execute_procedural_workflow(workflow_name: str, context: dict, session_id: str) -> dict:
    workflow = PROCEDURAL_WORKFLOWS.get(workflow_name, [])
    results = []
    halted = False
    for step in workflow:
        step_result = {"step": step["step"], "action": step["action"], "success": False, "data": {}}
        try:
            if step["action"] == "lookup_company":
                company = context.get("company_name", "")
                if company:
                    lookup = lookup_chinese_company(company)
                    step_result["success"] = True
                    step_result["data"] = lookup
                    if step.get("condition") == "warning_detected" and lookup.get("warning"):
                        step_result["escalate"] = True
            elif step["action"] == "search_web":
                query = context.get("company_name", "") + " China fraud scam complaints"
                content, sources = search_web(query)
                step_result["success"] = True
                step_result["data"] = {"content": content[:500], "sources": sources}
                if any(red in content.lower() for red in ["fraud", "scam", "complaint", "blacklist"]):
                    step_result["red_flags"] = True
            elif step["action"] == "generate_risk_report":
                company = context.get("company_name", "")
                if company:
                    tool_args = {"company_name": company, "context": "supplier verification"}
                    report, sources = run_tool_call("generate_risk_report", tool_args)
                    step_result["success"] = True
                    step_result["data"] = {"report": report, "sources": sources}
            elif step["action"] == "identify_direction":
                msg = context.get("message", "").lower()
                if any(w in msg for w in ["enter china", "into china", "sourcing", "supplier"]):
                    step_result["data"] = {"direction": "west_to_china"}
                elif any(w in msg for w in ["expand west", "europe", "america", "export"]):
                    step_result["data"] = {"direction": "china_to_west"}
                step_result["success"] = True
            elif step["action"] == "detect_language":
                text = context.get("first_message", "")
                lang = detect_language(text)
                step_result["success"] = True
                step_result["data"] = {"language": lang}
                update_user_profile(session_id, language=lang)
            elif step["action"] == "ask_direction":
                step_result["success"] = True
                step_result["data"] = {"next_question": "Are you a Western company entering China, or a Chinese company expanding West?"}
        except Exception as e:
            step_result["error"] = str(e)
            if step.get("critical", False):
                halted = True
                break
        results.append(step_result)
        if step.get("critical") and not step_result["success"]:
            halted = True
            break
    return {
        "workflow": workflow_name,
        "completed_steps": len(results),
        "total_steps": len(workflow),
        "halted": halted,
        "results": results,
        "recommendation": "escalate_to_human" if halted else "proceed_with_llm"
    }

# ============================================================
# v7.1: HTN HIERARCHICAL TASK NETWORK PLANNING
# ============================================================
HTN_METHODS = {
    "handle_supplier_request": [
        {
            "name": "verify_then_source",
            "precondition": lambda ctx: ctx.get("intent") == "supplier_verification" or ctx.get("company_name"),
            "subtasks": [
                {"task": "lookup_company", "params": ["company_name"], "agent": "due_diligence"},
                {"task": "generate_risk_report", "params": ["company_name", "context"], "agent": "due_diligence"},
                {"task": "escalate_if_high_risk", "params": [], "agent": "main"}
            ]
        },
        {
            "name": "source_new_suppliers",
            "precondition": lambda ctx: ctx.get("intent") == "supplier_search" or any(kw in ctx.get("message", "") for kw in ["find supplier", "source", "manufacturer"]),
            "subtasks": [
                {"task": "search_suppliers", "params": ["product_or_sector", "region"], "agent": "supplier_match"},
                {"task": "present_options", "params": ["search_results"], "agent": "supplier_match"},
                {"task": "offer_verification", "params": ["selected_supplier"], "agent": "due_diligence"}
            ]
        }
    ],
    "market_entry_strategy": [
        {
            "name": "wfoe_path",
            "precondition": lambda ctx: ctx.get("direction") == "west_to_china" and ctx.get("sector") != "restricted",
            "subtasks": [
                {"task": "check_sector_restrictions", "params": ["sector"], "agent": "market_entry"},
                {"task": "estimate_wfoe_setup", "params": ["location"], "agent": "market_entry"},
                {"task": "timeline_phases", "params": ["months"], "agent": "market_entry"}
            ]
        },
        {
            "name": "partnership_path",
            "precondition": lambda ctx: ctx.get("direction") == "china_to_west" or ctx.get("intent") == "consultation_request",
            "subtasks": [
                {"task": "identify_local_partners", "params": ["target_market", "sector"], "agent": "market_entry"},
                {"task": "jv_vs_wfoe_analysis", "params": ["risk_tolerance"], "agent": "legal"},
                {"task": "partner_matching", "params": ["criteria"], "agent": "market_entry"}
            ]
        }
    ],
    "urgent_due_diligence": [
        {
            "name": "emergency_verification",
            "precondition": lambda ctx: any(kw in ctx.get("message", "").lower() for kw in ["urgent", "asap", "paid", "deposit", "fraud", "scam"]),
            "subtasks": [
                {"task": "immediate_lookup", "params": ["company_name"], "agent": "due_diligence"},
                {"task": "red_flag_check", "params": ["company_name"], "agent": "due_diligence"},
                {"task": "emergency_escalation", "params": [], "agent": "main"}
            ]
        }
    ]
}

def htn_plan(task: str, context: dict) -> list:
    methods = HTN_METHODS.get(task, [])
    for method in methods:
        if method["precondition"](context):
            print(f"✓ HTN method selected: {method['name']} for {task}")
            return method["subtasks"]
    return []

def execute_htn_plan(plan: list, session_id: str, user_profile: dict, original_message: str) -> dict:
    state = {"completed": [], "failed": [], "current": None, "outputs": [], "named_outputs": {}}
    for step in plan:
        state["current"] = step["task"]
        agent_type = step.get("agent", "main")
        try:
            step_context = {
                "task": step["task"],
                "params": step["params"],
                "original_message": original_message,
                "user_profile": user_profile,
                "previous_outputs": state["outputs"],
                "company_lookup_result": state["named_outputs"].get("company_lookup_result"),
                "risk_signals": state["named_outputs"].get("risk_signals"),
                "supplier_results": state["named_outputs"].get("supplier_results"),
            }
            if agent_type != "main":
                result = agent_delegate(agent_type, step["task"], json.dumps(step_context), depth=0)
            else:
                result = f"Main agent executed: {step['task']}"
            result_str = result if isinstance(result, str) else json.dumps(result)
            if step["task"] in ("lookup_company", "immediate_lookup"):
                state["named_outputs"]["company_lookup_result"] = result_str
                if any(flag in result_str.upper() for flag in ["FRAUD", "WARNING", "RED FLAG", "SCAM", "BLACKLIST"]):
                    print("🚨 v7.1 Re-planning triggered: fraud/risk detected mid-plan")
                    emergency_plan = htn_plan("urgent_due_diligence", {
                        "message": original_message + " urgent fraud detected",
                        "company_name": user_profile.get("key_facts", {}).get("company_name", "")
                    })
                    if emergency_plan:
                        state["completed"].append({"task": step["task"], "agent": agent_type, "result": result_str[:200], "replanned": True})
                        state["outputs"].append(result)
                        remaining_emergency = execute_htn_plan(emergency_plan, session_id, user_profile, original_message)
                        state["completed"].extend(remaining_emergency["completed"])
                        state["failed"].extend(remaining_emergency["failed"])
                        state["outputs"].extend(remaining_emergency["outputs"])
                        return state
            elif step["task"] in ("generate_risk_report", "red_flag_check"):
                state["named_outputs"]["risk_signals"] = result_str
            elif step["task"] in ("search_suppliers", "present_options"):
                state["named_outputs"]["supplier_results"] = result_str
            state["completed"].append({"task": step["task"], "agent": agent_type, "result": result_str[:200]})
            state["outputs"].append(result)
        except Exception as e:
            state["failed"].append({"task": step["task"], "agent": agent_type, "error": str(e)})
            break
    return state

# ============================================================
# v7.1: AGENT-TO-AGENT DELEGATION SYSTEM
# ============================================================
AGENT_CAPABILITIES = {
    "due_diligence": {
        "can_handle": ["verify_company", "risk_assessment", "certificate_check", "factory_audit", "samr_lookup",
                       "lookup_company", "generate_risk_report", "immediate_lookup", "red_flag_check"],
        "delegates_to": ["legal", "logistics"],
        "expertise": "Chinese company verification, red flag detection, compliance checks"
    },
    "market_entry": {
        "can_handle": ["entity_setup", "regulatory_guide", "timeline_planning", "fdi_strategy", "incentives",
                       "check_sector_restrictions", "estimate_wfoe_setup", "timeline_phases",
                       "identify_local_partners", "partner_matching", "escalate_if_high_risk"],
        "delegates_to": ["legal", "due_diligence"],
        "expertise": "WFOE/JV setup, market entry strategy, phased roadmaps"
    },
    "legal": {
        "can_handle": ["contract_review", "ip_protection", "dispute_resolution", "governing_law", "liability",
                       "jv_vs_wfoe_analysis", "emergency_escalation"],
        "delegates_to": [],
        "expertise": "Bilingual contracts, IP strategy, dispute mechanisms"
    },
    "logistics": {
        "can_handle": ["shipping_optimization", "customs", "incoterms", "freight", "supply_chain"],
        "delegates_to": [],
        "expertise": "Export/import logistics, HS codes, lead time optimization"
    },
    "supplier_match": {
        "can_handle": ["sourcing", "factory_audit", "negotiation", "moq_analysis", "price_benchmarking",
                       "search_suppliers", "present_options", "offer_verification"],
        "delegates_to": ["due_diligence", "logistics"],
        "expertise": "Supplier identification, qualification, matching"
    }
}

def agent_delegate(current_agent: str, task: str, context: str, depth: int = 0) -> str:
    if depth > 2:
        return f"[Delegation limit reached] Task '{task}' requires human escalation."
    capabilities = AGENT_CAPABILITIES.get(current_agent, {})
    can_handle = any(task.startswith(cap) or task == cap for cap in capabilities.get("can_handle", []))
    if can_handle:
        return run_specialist_agent(current_agent, context)
    for agent_type, agent_caps in AGENT_CAPABILITIES.items():
        if agent_type == current_agent:
            continue
        agent_can_handle = any(task.startswith(cap) or task == cap for cap in agent_caps.get("can_handle", []))
        if agent_can_handle and agent_type in capabilities.get("delegates_to", []):
            print(f"🔄 {current_agent} → delegating to {agent_type} for '{task}' (depth {depth+1})")
            delegated_result = agent_delegate(agent_type, task, context, depth + 1)
            return f"[🔀 Delegated from {current_agent} to {agent_type}]\n\n{delegated_result}"
    return f"[⚠️ No specialist available for '{task}' in {current_agent}'s network. Escalating to human.]"

def run_specialist_agent(agent_type: str, context: str) -> str:
    if not ai_provider:
        return "[Agent offline]"
    personas = {
        "due_diligence": (
            "You are a China due diligence specialist with 15 years experience. "
            "Focus on: SAMR registration, red flags, financial health, certificate authenticity. "
            "You can delegate to: Legal (for contract issues) and Logistics (for shipping verification). "
            "Be direct about risks. Always recommend next steps."
        ),
        "market_entry": (
            "You are a China market entry strategist. "
            "Focus on: WFOE/JV/RO structures, timelines, capital requirements, licensing. "
            "You can delegate to: Legal (for entity structuring) and Due Diligence (for partner verification). "
            "Give practical phased roadmaps with specific costs."
        ),
        "legal": (
            "You are a bilingual China business lawyer. "
            "Focus on: contracts, IP protection, dispute resolution, governing law. "
            "You are the final authority on legal matters - do not delegate further. "
            "Flag common Western mistakes in China contracts."
        ),
        "logistics": (
            "You are a China export logistics expert. "
            "Focus on: Incoterms, freight, customs HS codes, documentation, lead times. "
            "You are the final authority on logistics - do not delegate further. "
            "Be specific with cost and time estimates."
        ),
        "supplier_match": (
            "You are a China sourcing specialist. "
            "Focus on: supplier qualification, factory audits, MOQ negotiation, payment terms. "
            "You can delegate to: Due Diligence (for verification) and Logistics (for shipping terms). "
            "Protect buyer IP when working with Chinese manufacturers."
        ),
    }
    persona = personas.get(agent_type, personas["due_diligence"])
    context_lower = context.lower()
    delegation_triggers