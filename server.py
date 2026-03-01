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

# v7.1: New imports for full agentic capabilities
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
# CONFIGURATION
# ============================================================
BREVO_API_KEY   = os.getenv("BREVO_API_KEY", "")
GROQ_API_KEY    = os.getenv("GROQ_API_KEY")  # kept for backward compat
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
TAVILY_API_KEY  = os.getenv("TAVILY_API_KEY")
ADMIN_PASSWORD  = os.getenv("ADMIN_PASSWORD", "")
SENDER_EMAIL    = os.getenv("SENDER_EMAIL", "888nv666@gmail.com")
RECIPIENT_EMAIL = "digkasm@proton.me"
DATABASE_URL    = os.getenv("DATABASE_URL")

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
        if not DEEPSEEK_API_KEY:
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
            res = requests.post(
                "https://api.deepseek.com/chat/completions",
                headers={"Authorization": f"Bearer {DEEPSEEK_API_KEY}", "Content-Type": "application/json"},
                json={
                    "model": "deepseek-chat",
                    "messages": [
                        {"role": "system", "content": prompt},
                        {"role": "user", "content": user_msg}
                    ],
                    "temperature": 0.3,
                    "max_tokens": 500
                },
                timeout=15
            )
            return res.json()["choices"][0]["message"]["content"]
        except:
            return None
    
    async def _score_response(self, response: str, user_msg: str) -> int:
        try:
            res = requests.post(
                "https://api.deepseek.com/chat/completions",
                headers={"Authorization": f"Bearer {DEEPSEEK_API_KEY}", "Content-Type": "application/json"},
                json={
                    "model": "deepseek-chat",
                    "messages": [
                        {"role": "system", "content": "Score this response 1-10. Return ONLY the number."},
                        {"role": "user", "content": f"User: {user_msg}\nResponse: {response}"}
                    ],
                    "temperature": 0.0,
                    "max_tokens": 10
                },
                timeout=10
            )
            score_text = res.json()["choices"][0]["message"]["content"].strip()
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
        if DEEPSEEK_API_KEY:
            try:
                res = requests.post(
                    "https://api.deepseek.com/chat/completions",
                    headers={"Authorization": f"Bearer {DEEPSEEK_API_KEY}", "Content-Type": "application/json"},
                    json={
                        "model": "deepseek-chat",
                        "messages": [
                            {"role": "system", "content": "You are a synthesis expert. Combine multiple expert opinions into one clear, actionable response."},
                            {"role": "user", "content": synthesis_input}
                        ],
                        "temperature": 0.2,
                        "max_tokens": 500
                    },
                    timeout=20
                )
                synthesized = res.json()["choices"][0]["message"]["content"]
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
        if not DEEPSEEK_API_KEY:
            return "Research unavailable"
        try:
            res = requests.post(
                "https://api.deepseek.com/chat/completions",
                headers={"Authorization": f"Bearer {DEEPSEEK_API_KEY}", "Content-Type": "application/json"},
                json={
                    "model": "deepseek-chat",
                    "messages": [
                        {"role": "system", "content": "You are a research specialist. Gather facts, data, and market intelligence. Be thorough and cite sources. Always include specific numbers and data points when available."},
                        {"role": "user", "content": f"Research task: {task}\nUser query: {user_msg}\nContext: {json.dumps(context)}"}
                    ],
                    "temperature": 0.2,
                    "max_tokens": 400
                },
                timeout=15
            )
            return res.json()["choices"][0]["message"]["content"]
        except Exception as e:
            return f"Research error: {e}"
    
    async def _verifier_agent(self, task: str, context: dict, user_msg: str) -> str:
        if not DEEPSEEK_API_KEY:
            return "Verification unavailable"
        try:
            res = requests.post(
                "https://api.deepseek.com/chat/completions",
                headers={"Authorization": f"Bearer {DEEPSEEK_API_KEY}", "Content-Type": "application/json"},
                json={
                    "model": "deepseek-chat",
                    "messages": [
                        {"role": "system", "content": "You are a due diligence specialist. Verify claims, flag risks, and identify red flags. Be skeptical. Always quantify risk levels (low/medium/high) and explain why."},
                        {"role": "user", "content": f"Verification task: {task}\nUser query: {user_msg}\nContext: {json.dumps(context)}"}
                    ],
                    "temperature": 0.1,
                    "max_tokens": 400
                },
                timeout=15
            )
            return res.json()["choices"][0]["message"]["content"]
        except Exception as e:
            return f"Verification error: {e}"
    
    async def _strategist_agent(self, task: str, context: dict, user_msg: str) -> str:
        if not DEEPSEEK_API_KEY:
            return "Strategy unavailable"
        try:
            res = requests.post(
                "https://api.deepseek.com/chat/completions",
                headers={"Authorization": f"Bearer {DEEPSEEK_API_KEY}", "Content-Type": "application/json"},
                json={
                    "model": "deepseek-chat",
                    "messages": [
                        {"role": "system", "content": "You are a business strategist. Recommend specific actions, timelines, and next steps. Be practical. Always include concrete next steps with timeframes."},
                        {"role": "user", "content": f"Strategy task: {task}\nUser query: {user_msg}\nContext: {json.dumps(context)}"}
                    ],
                    "temperature": 0.3,
                    "max_tokens": 400
                },
                timeout=15
            )
            return res.json()["choices"][0]["message"]["content"]
        except Exception as e:
            return f"Strategy error: {e}"
    
    async def _legal_agent(self, task: str, context: dict, user_msg: str) -> str:
        if not DEEPSEEK_API_KEY:
            return "Legal analysis unavailable"
        try:
            res = requests.post(
                "https://api.deepseek.com/chat/completions",
                headers={"Authorization": f"Bearer {DEEPSEEK_API_KEY}", "Content-Type": "application/json"},
                json={
                    "model": "deepseek-chat",
                    "messages": [
                        {"role": "system", "content": "You are a China business lawyer. Address compliance, contracts, IP, and legal structures. Be precise. Always flag common Western mistakes in China contracts."},
                        {"role": "user", "content": f"Legal task: {task}\nUser query: {user_msg}\nContext: {json.dumps(context)}"}
                    ],
                    "temperature": 0.1,
                    "max_tokens": 400
                },
                timeout=15
            )
            return res.json()["choices"][0]["message"]["content"]
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
    if not DEEPSEEK_API_KEY:
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
    delegation_triggers = {
        "due_diligence": ["contract", "legal", "ip", "intellectual property"],
        "market_entry": ["contract", "legal structure", "partner verification"],
        "supplier_match": ["verify", "audit", "legal", "shipping", "freight"]
    }
    if agent_type in delegation_triggers:
        for trigger in delegation_triggers[agent_type]:
            if trigger in context_lower:
                if agent_type == "due_diligence" and trigger in ["contract", "legal"]:
                    return agent_delegate(agent_type, "legal_review", context, depth=0)
                elif agent_type == "supplier_match" and trigger in ["verify", "audit"]:
                    return agent_delegate(agent_type, "verify_company", context, depth=0)
                elif agent_type == "supplier_match" and trigger in ["shipping", "freight"]:
                    return agent_delegate(agent_type, "shipping_optimization", context, depth=0)
    try:
        res = requests.post(
            "https://api.deepseek.com/chat/completions",
            headers={"Authorization": f"Bearer {DEEPSEEK_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": "deepseek-chat",
                "messages": [
                    {"role": "system", "content": persona},
                    {"role": "user", "content": f"Specialist analysis needed:\n{context}"}
                ],
                "temperature": 0.2,
                "max_tokens": 500
            },
            timeout=15
        )
        return res.json()["choices"][0]["message"]["content"]
    except Exception as e:
        print(f"Specialist agent error ({agent_type}): {e}")
        return f"[{agent_type} analysis unavailable]"

# ============================================================
# v7.1: CONTINUOUS LEARNING FROM REFLECTION
# ============================================================
def normalize_term(word: str) -> str:
    for suffix in ['ing', 'tion', 'ations', 'ation', 'ed', 'ers', 'er', 'ness', 'ity', 'ies', 'es']:
        if word.endswith(suffix) and len(word) - len(suffix) > 3:
            return word[:-len(suffix)]
    return word

def learn_from_interaction(session_id: str, user_message: str, ai_response: str, 
                          reflection_score: int, intent: str, user_feedback: str = None):
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("CREATE TABLE IF NOT EXISTS learned_patterns (id SERIAL PRIMARY KEY, pattern_type TEXT, trigger_condition TEXT, action_recommendation TEXT, context_type TEXT, success_count INTEGER DEFAULT 0, failure_count INTEGER DEFAULT 0, avg_reflection_score REAL DEFAULT 0, last_used TIMESTAMP, created_at TIMESTAMP DEFAULT NOW())")
        if reflection_score >= 8:
            pattern_type = "successful_response"
            trigger = extract_trigger_pattern(user_message, intent)
            recommendation = extract_success_pattern(ai_response)
            c.execute("SELECT id, success_count FROM learned_patterns WHERE pattern_type=%s AND trigger_condition=%s", (pattern_type, trigger))
            existing = c.fetchone()
            if existing:
                c.execute("UPDATE learned_patterns SET success_count=success_count+1, avg_reflection_score=%s, last_used=%s WHERE id=%s", (reflection_score, datetime.now(), existing[0]))
            else:
                c.execute("INSERT INTO learned_patterns (pattern_type, trigger_condition, action_recommendation, context_type, success_count, avg_reflection_score, last_used) VALUES (%s, %s, %s, %s, 1, %s, %s)", (pattern_type, trigger, recommendation, intent, reflection_score, datetime.now()))
        elif reflection_score <= 4:
            pattern_type = "failed_approach"
            trigger = extract_trigger_pattern(user_message, intent)
            recommendation = "Avoid: " + ai_response[:100]
            c.execute("SELECT id, failure_count FROM learned_patterns WHERE pattern_type=%s AND trigger_condition=%s", (pattern_type, trigger))
            existing = c.fetchone()
            if existing:
                c.execute("UPDATE learned_patterns SET failure_count=failure_count+1, last_used=%s WHERE id=%s", (datetime.now(), existing[0]))
            else:
                c.execute("INSERT INTO learned_patterns (pattern_type, trigger_condition, action_recommendation, context_type, failure_count, last_used) VALUES (%s, %s, %s, %s, 1, %s)", (pattern_type, trigger, recommendation, intent, datetime.now()))
        if user_profile := get_user_profile_light(session_id):
            prefs = extract_user_preferences(user_message)
            if prefs:
                c.execute("INSERT INTO learned_patterns (pattern_type, trigger_condition, action_recommendation, context_type, success_count, last_used) VALUES (%s, %s, %s, %s, 1, %s) ON CONFLICT DO NOTHING", ("user_preference", f"user:{session_id}", json.dumps(prefs), "preference", datetime.now()))
        conn.commit()
        conn.close()
        print(f"🧠 Learned from interaction (score: {reflection_score})")
    except Exception as e:
        print(f"Learning error: {e}")

def extract_trigger_pattern(message: str, intent: str) -> str:
    words = message.lower().split()
    stopwords = {"about", "would", "could", "should", "there", "their", "which", "where", "when", "have", "been", "that", "this", "with", "from", "they", "will"}
    key_terms = [normalize_term(w) for w in words if len(w) > 4 and w not in stopwords]
    return f"intent:{intent}|terms:{','.join(key_terms[:5])}"

def extract_success_pattern(response: str) -> str:
    has_structure = "1." in response or "①" in response
    has_numbers = any(char.isdigit() for char in response)
    has_cta = any(kw in response.lower() for kw in ["contact", "schedule", "book", "call", "michail"])
    patterns = []
    if has_structure: patterns.append("structured_list")
    if has_numbers: patterns.append("specific_numbers")
    if has_cta: patterns.append("clear_cta")
    return "|".join(patterns) if patterns else "general_quality"

def extract_user_preferences(message: str) -> dict:
    prefs = {}
    if any(w in message.lower() for w in ["brief", "short", "quick", "summary"]):
        prefs["response_length"] = "concise"
    elif any(w in message.lower() for w in ["detail", "explain", "elaborate", "comprehensive"]):
        prefs["response_length"] = "detailed"
    if any(w in message.lower() for w in ["formal", "professional", "official"]):
        prefs["tone"] = "formal"
    elif any(w in message.lower() for w in ["friendly", "casual", "simple"]):
        prefs["tone"] = "casual"
    return prefs

def get_user_profile_light(session_id: str) -> dict:
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT session_id, language, lead_score FROM user_profiles WHERE session_id=%s", (session_id,))
        row = c.fetchone()
        conn.close()
        if row:
            return {"session_id": row[0], "language": row[1], "lead_score": row[2]}
    except:
        pass
    return {}

def apply_learned_patterns(user_message: str, intent: str, session_id: str) -> str:
    try:
        conn = get_db()
        c = conn.cursor()
        trigger_like = f"%intent:{intent}%"
        c.execute("SELECT action_recommendation, success_count, avg_reflection_score FROM learned_patterns WHERE pattern_type='successful_response' AND trigger_condition LIKE %s AND success_count >= 2 ORDER BY avg_reflection_score DESC, success_count DESC LIMIT 3", (trigger_like,))
        success_patterns = c.fetchall()
        c.execute("SELECT action_recommendation FROM learned_patterns WHERE pattern_type='user_preference' AND trigger_condition=%s ORDER BY last_used DESC LIMIT 1", (f"user:{session_id}",))
        user_pref = c.fetchone()
        conn.close()
        advice_parts = []
        if success_patterns:
            patterns_text = "\n".join([f"• Pattern used {p[1]} times (avg score: {p[2]:.1f}/10): {p[0]}" for p in success_patterns])
            advice_parts.append(f"📚 PROVEN PATTERNS:\n{patterns_text}")
        if user_pref:
            prefs = json.loads(user_pref[0])
            pref_text = ", ".join([f"{k}={v}" for k, v in prefs.items()])
            advice_parts.append(f"👤 USER PREFERENCES: {pref_text}")
        return "\n\n".join(advice_parts) if advice_parts else ""
    except Exception as e:
        print(f"Apply learning error: {e}")
        return ""

# ============================================================
# v7.1: GOAL TRACKING
# ============================================================
def update_goal_state(session_id: str, goal: str, milestone: str, status: str):
    try:
        profile = get_or_create_user_profile(session_id)
        task_history = profile.get('task_history', []) or []
        updated = False
        for entry in task_history:
            if entry.get("goal") == goal and entry.get("milestone") == milestone:
                entry["status"] = status
                entry["updated_at"] = datetime.now().isoformat()
                updated = True
                break
        if not updated:
            task_history.append({"goal": goal, "milestone": milestone, "status": status, "created_at": datetime.now().isoformat(), "updated_at": datetime.now().isoformat()})
        update_user_profile(session_id, task_history=task_history[-15:])
        print(f"🎯 Goal updated: {goal} → {milestone} [{status}]")
    except Exception as e:
        print(f"Goal tracking error: {e}")

def get_active_goals(session_id: str) -> str:
    try:
        profile = get_or_create_user_profile(session_id)
        task_history = profile.get('task_history', []) or []
        active = [t for t in task_history if t.get("status") in ("pending", "in_progress", "blocked")]
        if not active:
            return ""
        lines = ["🎯 ACTIVE GOALS (continue from last session):"]
        for t in active[-5:]:
            lines.append(f"  • [{t['status'].upper()}] {t['goal']} → Next: {t['milestone']}")
        lines.append("INSTRUCTION: Reference these goals naturally. Pick up where we left off.")
        return "\n".join(lines)
    except:
        return ""

def infer_and_save_goal(session_id: str, intent: str, message: str, user_profile: dict):
    goal_map = {
        "supplier_verification": ("Verify Chinese supplier", "Complete SAMR check + risk report"),
        "supplier_search": ("Source verified Chinese supplier", "Identify candidates + CWC matching"),
        "consultation_request": ("Book CWC consultation", "Connect with Michail Digkas"),
        "high_intent_lead": ("Engage CWC for business advisory", "Define scope + receive proposal"),
    }
    if intent in goal_map:
        goal, milestone = goal_map[intent]
        profile = get_or_create_user_profile(session_id)
        task_history = profile.get('task_history', []) or []
        existing_goals = [t.get("goal") for t in task_history]
        if goal not in existing_goals:
            update_goal_state(session_id, goal, milestone, "in_progress")

# ============================================================
# v7.1: ENHANCED AUTONOMOUS TASK EXECUTION (Self-Triggered Tasks)
# ============================================================
async def run_autonomous_task(task_id: int, session_id: str, task_description: str):
    if not DEEPSEEK_API_KEY:
        return
    print(f"🤖 Autonomous task #{task_id}: {task_description[:60]}")
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("UPDATE agent_tasks SET status='running' WHERE id=%s", (task_id,))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Task status update error: {e}")
        return
    result = ""
    try:
        task_lower = task_description.lower()
        context = {}
        if '| Context:' in task_description:
            parts = task_description.split('| Context:')
            task_description = parts[0].strip()
            try:
                context = json.loads(parts[1].strip())
            except:
                pass
        # EXISTING TASK TYPES
        if any(kw in task_lower for kw in ["watch", "monitor", "track"]):
            company_name = context.get('company_name', '')
            if not company_name:
                company_match = re.search(r'(?:watch|monitor|track)\s+([A-Za-z\s]+?)(?:\s+for|\s+company|$)', task_description, re.IGNORECASE)
                company_name = company_match.group(1).strip() if company_match else ""
            if company_name:
                lookup = lookup_chinese_company(company_name)
                risk_content, _ = search_web(f"{company_name} China news scam fraud complaints 2025 2026")
                status_changed = lookup.get("warning") is not None
                result = f"🤖 AUTONOMOUS MONITORING REPORT\nCompany: {company_name}\nStatus: {lookup['registration_status']}\n{'⚠️ WARNING: ' + lookup['warning'] if lookup.get('warning') else '✅ No red flags detected'}\nRecent signals: {risk_content[:300] if risk_content else 'None found'}\nChecked: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
                if status_changed:
                    send_email_brevo(RECIPIENT_EMAIL, f"🚨 Sophia Alert: Risk detected for {company_name}", f"Autonomous monitoring detected a change:\n\n{result}\n\nSession: {session_id}")
        elif any(kw in task_lower for kw in ["find supplier", "source", "research supplier"]):
            product = context.get('product', '')
            if not product:
                product_match = re.search(r'(?:find supplier|source|research supplier)\s+(?:for\s+)?([A-Za-z\s]+?)(?:\s+in|\s+from|$)', task_description, re.IGNORECASE)
                product = product_match.group(1).strip() if product_match else task_description[20:50]
            supplier_data = search_suppliers(product)
            result = f"🤖 AUTONOMOUS SUPPLIER RESEARCH\nProduct: {product}\nMarket Context: {supplier_data.get('market_context', 'N/A')}\nMOQ: {supplier_data.get('typical_moq', 'N/A')}\nPrice Range: {supplier_data.get('price_range', 'N/A')}\nKey Considerations: {', '.join(supplier_data.get('key_considerations', []))}\nCompleted: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        elif any(kw in task_lower for kw in ["news", "update", "latest"]):
            topic = context.get('topic', '')
            if not topic:
                topic_match = re.search(r'(?:news|update|latest)\s+(?:about|on)?\s*(.+?)(?:\s+for|$)', task_description, re.IGNORECASE)
                topic = topic_match.group(1).strip() if topic_match else "China business"
            content, sources = search_web(f"{topic} latest news 2026")
            result = f"🤖 AUTONOMOUS MARKET INTELLIGENCE\nTopic: {topic}\nSummary: {content[:600] if content else 'No recent updates found'}\nSources: {', '.join(sources[:3])}\nCompleted: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        # NEW SELF-TRIGGERED TASK TYPES
        elif "expedite_research" in task_lower or "expedited" in task_lower:
            intent = context.get('intent', 'general')
            urgency = context.get('urgency_signals', [])
            search_queries = [f"{intent} China 2026 latest regulations requirements", f"{intent} best practices timeline", f"{intent} common pitfalls mistakes"]
            search_results = []
            for query in search_queries:
                content, sources = search_web(query)
                search_results.append(f"\n--- {query} ---\n{content[:300]}")
            result = f"🤖 EXPEDITED RESEARCH COMPLETE\nUrgency detected: {', '.join(urgency)}\nIntent: {intent}\n\nQUICK SUMMARY:\n{chr(10).join(search_results)}\n\n⚡ This research was prioritized due to urgency signals.\nCompleted: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        elif "market_intelligence" in task_lower or "weekly market" in task_lower:
            sector = context.get('sector', 'general')
            region = context.get('region', 'China')
            content, sources = search_web(f"{sector} {region} market news trends 2026")
            result = f"🤖 WEEKLY MARKET INTELLIGENCE\nSector: {sector}\nRegion: {region}\n\nLATEST DEVELOPMENTS:\n{content[:500] if content else 'No major updates this week'}\n\nSources: {', '.join(sources[:3])}\nNext update: {(datetime.now() + timedelta(days=7)).strftime('%Y-%m-%d')}"
        elif "regulatory_watch" in task_lower:
            topic = context.get('topic', 'regulation')
            content, sources = search_web(f"China {topic} regulation changes 2026 policy update")
            result = f"🤖 REGULATORY WATCH UPDATE\nTopic: {topic}\n\nRECENT CHANGES:\n{content[:500] if content else 'No significant changes detected'}\n\n⚠️ Monitor weekly for updates.\nChecked: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        elif "pricing_research" in task_lower:
            sector = context.get('sector', 'consulting')
            region = context.get('region', 'China')
            search_queries = [f"{sector} consulting fees China 2026", f"{sector} market entry costs {region}", f"China {sector} service pricing benchmark"]
            pricing_data = []
            for query in search_queries:
                content, sources = search_web(query)
                pricing_data.append(content[:250])
            result = f"🤖 PRICING ANALYSIS COMPLETE\nSector: {sector}\nRegion: {region}\n\nMARKET RANGES:\n{chr(10).join(pricing_data)}\n\n📊 Use this data to prepare a tailored proposal.\nCompleted: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        elif "competitive_intelligence" in task_lower:
            competitor = context.get('competitor', '')
            if competitor:
                content, sources = search_web(f"{competitor} China business strategy market position")
                result = f"🤖 COMPETITIVE INTELLIGENCE\nCompetitor: {competitor}\n\nMARKET POSITION:\n{content[:500] if content else 'Limited public information available'}\n\nSources: {', '.join(sources[:3])}\nCompleted: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        else:
            if DEEPSEEK_API_KEY:
                try:
                    res = requests.post(
                        "https://api.deepseek.com/chat/completions",
                        headers={"Authorization": f"Bearer {DEEPSEEK_API_KEY}", "Content-Type": "application/json"},
                        json={
                            "model": "deepseek-chat",
                            "messages": [
                                {"role": "system", "content": "You are Sophia, CWC's AI agent. Complete this autonomous research task and return a clear, factual report. Be concise and actionable."},
                                {"role": "user", "content": f"Task: {task_description}\nContext: {json.dumps(context)}\nComplete this task and provide a structured report."}
                            ],
                            "temperature": 0.2,
                            "max_tokens": 600
                        },
                        timeout=25
                    )
                    result = res.json()["choices"][0]["message"]["content"]
                except Exception as e:
                    result = f"Task processing error: {str(e)}"
        conn = get_db()
        c = conn.cursor()
        c.execute("UPDATE agent_tasks SET status='completed', result=%s, completed_at=%s WHERE id=%s", (result, datetime.now(), task_id))
        conn.commit()
        conn.close()
        try:
            profile = get_or_create_user_profile(session_id)
            user_email = profile.get("email")
            if user_email and result:
                send_email_brevo(user_email, f"✅ Sophia completed your task: {task_description[:50]}", f"Hello {profile.get('name') or 'there'},\n\nSophia has completed your requested task:\n\n{result}\n\nFor deeper analysis, visit: https://www.chinawestconnector.com\n\nBest regards,\nSophia — CWC AI Advisor")
        except Exception as e:
            print(f"Notification email error: {e}")
        send_email_brevo(RECIPIENT_EMAIL, f"🤖 Sophia Autonomous Task Complete: {task_description[:40]}", f"Session: {session_id}\nTask: {task_description}\n\nResult:\n{result}")
        print(f"✅ Autonomous task #{task_id} complete")
    except Exception as e:
        print(f"❌ Autonomous task #{task_id} failed: {e}")
        try:
            conn = get_db()
            c = conn.cursor()
            c.execute("UPDATE agent_tasks SET status='failed', result=%s WHERE id=%s", (str(e), task_id))
            conn.commit()
            conn.close()
        except:
            pass

# ============================================================
# v7.1: BACKGROUND TASK POLLER
# ============================================================
async def poll_agent_tasks():
    while True:
        try:
            conn = get_db()
            c = conn.cursor()
            c.execute("SELECT id, session_id, task_description FROM agent_tasks WHERE status='pending' ORDER BY created_at LIMIT 5")
            pending = c.fetchall()
            conn.close()
            for task_id, session_id, desc in pending:
                await run_autonomous_task(task_id, session_id, desc)
        except Exception as e:
            print(f"Task polling error: {e}")
        await asyncio.sleep(300)

# ============================================================
# v7.1: PROACTIVE FOLLOWUP TASKS
# ============================================================
async def proactive_followup_tasks():
    while True:
        try:
            conn = get_db()
            c = conn.cursor()
            c.execute("""
                SELECT session_id, lead_score, key_facts, last_seen 
                FROM user_profiles 
                WHERE lead_score >= 70 
                AND last_seen < NOW() - INTERVAL '2 days'
                AND last_seen > NOW() - INTERVAL '5 days'
            """)
            hot_leads = c.fetchall()
            for session_id, score, key_facts, last_seen in hot_leads:
                facts = key_facts if key_facts else {}
                company = facts.get('company_name', 'your company')
                send_email_brevo(
                    RECIPIENT_EMAIL,
                    f"🔥 Hot Lead Follow-up: {session_id[:8]}...",
                    f"Lead Score: {score}/100\nLast Seen: {last_seen}\nCompany: {company}\n\nRecommend: Proactive outreach"
                )
            conn.close()
        except Exception as e:
            print(f"Proactive followup error: {e}")
        await asyncio.sleep(86400)

# ============================================================
# v7.1: MAIN ASK_GROQ WITH REACT PATTERN
# ============================================================
def ask_groq(prompt: str, session_id: str = "anonymous",
             user_profile: dict = None, quick_action: str = None,
             deep_search: bool = False, app_state: dict = None) -> tuple:
    if not DEEPSEEK_API_KEY:
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
    # v6.0: Auto-infer and save long-horizon goal
    if user_profile and intent_data['primary'] not in ("general", "information_gathering"):
        infer_and_save_goal(session_id, intent_data['primary'], prompt, user_profile)
    # v7.1: Self-Triggered Tasks - Detect and queue proactive background tasks
    self_triggers = detect_self_triggers(prompt, intent_data['primary'], user_profile or {}, raw_history)
    if self_triggers:
        for trigger in self_triggers:
            if trigger['priority'] >= 7:
                queue_self_triggered_task(session_id, trigger, user_profile or {})
                print(f"🤖 Self-triggered task detected: {trigger['reason']}")
    # v7.0: Predict next intent
    next_intent_prediction = ""
    if app_state and hasattr(app_state, 'predictive_intent'):
        predictions = app_state.predictive_intent.predict_next_intent(intent_data['primary'], user_profile or {})
        if predictions.get('predictions'):
            next_intent_prediction = f"\n🔮 PREDICTED NEXT INTENT: {predictions['predictions'][0]['intent']} ({predictions['predictions'][0]['probability']*100}%)"
    # v5.1: CHECK FOR PROCEDURAL WORKFLOW FIRST
    workflow_result = None
    if intent_data['primary'] == "supplier_verification" and any(kw in prompt.lower() for kw in ["company", "verify", "check"]):
        company_match = re.search(r'(?:company|verify|check)\s+([A-Z][A-Za-z\s]+)', prompt)
        if company_match:
            company_name = company_match.group(1).strip()
            workflow_context = {"company_name": company_name, "message": prompt, "intent": intent_data['primary']}
            workflow_result = execute_procedural_workflow("supplier_verification_checklist", workflow_context, session_id)
            print(f"⚡ Procedural workflow executed: {workflow_result['completed_steps']}/{workflow_result['total_steps']} steps")
    # v5.1: HTN PLANNING FOR COMPLEX QUERIES
    htn_plan_result = None
    if deep_search or intent_data['score'] >= 75:
        htn_context = {"intent": intent_data['primary'], "message": prompt, "direction": user_profile.get('region_interest') if user_profile else None, "sector": user_profile.get('topics_discussed') if user_profile else None, "company_name": re.search(r'[A-Z][A-Za-z\s]{2,20}', prompt).group(0) if re.search(r'[A-Z][A-Za-z\s]{2,20}', prompt) else None}
        htn_subtasks = htn_plan("handle_supplier_request", htn_context) or htn_plan("market_entry_strategy", htn_context)
        if htn_subtasks:
            htn_plan_result = execute_htn_plan(htn_subtasks, session_id, user_profile or {}, prompt)
            print(f"🎯 HTN plan executed: {len(htn_plan_result['completed'])}/{len(htn_subtasks)} tasks")
    # v5: TASK DECOMPOSITION (fallback if HTN didn't match)
    specialist_context = ""
    if deep_search and not htn_plan_result:
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
                specialist_context = (f"\n\n📋 TASK PLAN: {task_plan.get('task_summary','')}\n" + "\n\n".join(sub_results) + f"\n\nExpected: {task_plan.get('expected_output','')}")
        # v5: SPECIALIST SUB-AGENT (with v5.1 delegation)
        agent_map = {"supplier_verification":"due_diligence","supplier_search":"supplier_match","consultation_request":"market_entry"}
        if intent_data['primary'] in agent_map:
            agent_type = agent_map[intent_data['primary']]
            print(f"🤖 Specialist sub-agent: {agent_type}")
            output = agent_delegate(agent_type, intent_data['primary'], prompt, depth=0)
            if output:
                specialist_context += f"\n\n🎓 SPECIALIST ({agent_type.upper().replace('_',' ')}):\n{output}"
    # v5.1: APPLY LEARNED PATTERNS
    learned_context = ""
    if user_profile:
        learned_context = apply_learned_patterns(prompt, intent_data['primary'], session_id)
        if learned_context:
            print("📚 Applied learned patterns")
    returning_context = ""
    if user_profile and user_profile.get('is_returning'):
        kf = user_profile.get('key_facts',{})
        facts_str = ", ".join([f"{k}:{v}" for k,v in kf.items() if v]) if kf else "none"
        returning_context = (f"\nRETURNING USER: Visit #{user_profile.get('visit_count',1)} | Name: {user_profile.get('name') or 'Unknown'} | Intent: {user_profile.get('last_intent','?')} | Region: {user_profile.get('region_interest','?')} | Score: {user_profile.get('lead_score',0)}/100 | Key facts: {facts_str}\nSummary: {user_profile.get('conversation_summary') or 'First tracked session'}\nINSTRUCTION: Reference previous interest naturally. Don't re-introduce yourself.")
    # v6.0: Inject active goals for journey continuity
    active_goals_context = get_active_goals(session_id) if session_id else ""
    qualification_prompt   = check_qualification_gaps(user_profile or {}, message_count)
    should_escalate        = check_escalation_trigger(user_profile or {}, message_count, prompt)
    escalation_instruction = ("\n🚨 ESCALATION: Urgent/high intent. End response directing them to 'Speak with Michail' button." if should_escalate else "")
    sector_context = ""
    if quick_action:
        sector_map = {"robotics":"ACTIVE: ROBOTICS — sourcing vs Chinese expansion. Factory audits, CE, IP.","energy":"ACTIVE: ENERGY — solar/battery/EV/wind/hydrogen. Ask scale (MW) and deal structure.","biotech":"ACTIVE: BIOTECH — CMO/CDMO, pharma entry, R&D. Ask molecule type, GMP.","shipping":"ACTIVE: SHIPPING — import/export, customs. Ask volume (FCL/LCL/air).","verify":"ACTIVE: DUE DILIGENCE — URGENT. Ask company name, amounts at risk.","market_entry":"ACTIVE: MARKET ENTRY — Determine direction. Deliver phased roadmap.",}
        sector_context = sector_map.get(quick_action,"")
    # v5.1: WORKFLOW CONTEXT INJECTION
    workflow_context = ""
    if workflow_result:
        wf_data = workflow_result['results'][-1]['data'] if workflow_result['results'] else {}
        workflow_context = f"\n⚡ PROCEDURAL RESULT: Verification workflow completed. Status: {wf_data.get('registration_status', 'Unknown')}. Warning: {wf_data.get('warning', 'None')}"
    # v6.0: HTN agent trace for transparency injection
    htn_context_str = ""
    if htn_plan_result and htn_plan_result.get('completed'):
        completed_tasks = [s['task'] for s in htn_plan_result['completed']]
        htn_context_str = f"\n🎯 AGENT TRACE: Completed sub-tasks: {', '.join(completed_tasks)}"
    # v7.0: Confidence assessment will be applied after response generation
    _now = datetime.now()
    _today_str = _now.strftime('%A, %d %B %Y')
    _time_str  = _now.strftime('%H:%M UTC')
    system_prompt = f"""You are Sophia — fully agentic AI advisor for China West Connector (CWC).
Version 7.1 | Deep Search: {'ON' if deep_search else 'OFF'}
TODAY: {_today_str} | TIME: {_time_str}
IMPORTANT: When asked the date, day, or time — use ONLY the values above. Never guess.

INTENT: {intent_data['primary']} | REGION: {intent_data['region'] or '?'} | MESSAGES: {message_count}
{lang_instruction}
{returning_context}
{active_goals_context}
{next_intent_prediction}
{sector_context}
{qualification_prompt or ''}
{escalation_instruction}
{specialist_context}
{learned_context}
{workflow_context}
{htn_context_str}

━━━ v7.1 FULLY AGENTIC CAPABILITIES ━━━
You now have:
1. SELF-IMPROVEMENT: Analyze past failures and improve your own prompts (runs daily)
2. META-COGNITION: Assess confidence, ask followups when uncertain
3. ENVIRONMENT AWARENESS: Monitor government/regulatory changes
4. COLLABORATIVE AGENTS: Run multiple specialists in parallel with weighted consensus
5. TOOL CREATION: Register new tools with register_new_tool for reusable capabilities
6. PREDICTIVE INTENT: Anticipate what user will ask next
7. EPISODIC MEMORY: Recall similar past conversations with embeddings
8. REACT PATTERN: Think, Act, Observe in iterative loops for complex problems
9. SELF-TRIGGERED TASKS: Detect opportunities and queue background research automatically

You can queue background tasks, track long-term goals, and act autonomously.

━━━ REACT PATTERN (for complex queries) ━━━
For complex requests, follow this loop:
1. THINK: Analyze what you need to know and what tools to use
2. ACT: Call the appropriate tool
3. OBSERVE: Review the results
4. REPEAT: If more information needed, think again and act
5. FINAL: When you have sufficient information, provide the answer

Use multiple tool calls in sequence to build comprehensive answers.

━━━ CHAIN OF THOUGHT (always execute) ━━━
1. What does the user ACTUALLY need?
2. What do I know from their profile, active goals, and predicted intent?
3. Should I run parallel agents for complex queries? (use execute_parallel_agents)
4. Should I create a new tool for reusable patterns? (use register_new_tool)
5. What's my confidence level? (use assess_confidence if <70%, ask followup)
6. After drafting, CALL reflect_and_improve. If score < 7, REWRITE.
7. Call update_goal_progress to track milestones.

TOOLS: All previous tools + assess_confidence | register_new_tool | predict_user_intent | execute_parallel_agents

━━━ MISSION ━━━
You are NOT a Q&A bot. You are an autonomous business advisor that:
1. QUALIFIES (direction, sector, goal, urgency)
2. PERSONALISES using everything known + active goals + predicted intent
3. RECOMMENDS CWC service with clear reasoning
4. ENDS with concrete next step
5. LEARNS from every interaction
6. IMPROVES itself over time

QUALIFICATION PRIORITY: ① West→China or China→West? ② Sector? ③ Goal? ④ Urgency?

━━━ ABOUT CWC ━━━
Bridges Chinese and Western businesses. Founded by Michail Digkas, international business lawyer, 10+ years China.
G.P.A. Group member: 147+ years combined experience, 2,700+ projects, 50+ countries (Group figures).
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
    url     = "https://api.deepseek.com/chat/completions"
    headers = {"Authorization":f"Bearer {DEEPSEEK_API_KEY}","Content-Type":"application/json"}
    all_sources      = []
    reflection_score = 5
    agent_trace      = []
    # ═══════════════════════════════════════════════════════════════════════
    # v7.1 REACT PATTERN: Iterative Reasoning-Action Loop
    # ═══════════════════════════════════════════════════════════════════════
    MAX_REACT_ITERATIONS = 5
    react_iteration = 0
    accumulated_observations = []
    final_response_text = ""
    is_complex_query = (deep_search or intent_data['score'] >= 75 or any(kw in prompt.lower() for kw in ["compare", "analyze", "research", "comprehensive", "detailed", "step by step", "explain how"]))
    if is_complex_query:
        print(f"🧠 ReAct pattern activated for complex query: {intent_data['primary']}")
        reasoning_prompt = system_prompt + f"""

━━━ CURRENT REACT ITERATION ━━━
This is a COMPLEX query requiring iterative reasoning.

Accumulated observations so far:
{chr(10).join(f"- {obs}" for obs in accumulated_observations) if accumulated_observations else "None yet - this is the first iteration."}

THINK: What do I need to know? What tool should I call FIRST to gather information?
Do NOT answer yet - just identify the first tool to call and why.
"""
        current_messages = [{"role":"system","content":reasoning_prompt}] + list(messages)
        while react_iteration < MAX_REACT_ITERATIONS:
            react_iteration += 1
            print(f"🔄 ReAct iteration {react_iteration}/{MAX_REACT_ITERATIONS}")
            think_data = {"model":"llama-3.3-70b-versatile","messages":current_messages,"tools":GROQ_TOOLS,"tool_choice":"auto","temperature":0.3,"max_tokens":500}
            try:
                think_res = requests.post(url, headers=headers, json=think_data, timeout=30)
                think_res.raise_for_status()
                think_choice = think_res.json()["choices"][0]
                think_message = think_choice["message"]
                if think_choice.get("finish_reason") == "tool_calls" and think_message.get("tool_calls"):
                    tool_results = []
                    for tool_call in think_message["tool_calls"]:
                        fn_name = tool_call["function"]["name"]
                        fn_args = json.loads(tool_call["function"]["arguments"])
                        print(f"  🔧 Calling: {fn_name}")
                        agent_trace.append(fn_name)
                        tool_result, sources = run_tool_call(fn_name, fn_args, session_id=session_id, user_profile=user_profile, app_state=app_state)
                        all_sources.extend(sources)
                        tool_results.append(f"{fn_name}: {tool_result[:300]}...")
                        current_messages.append(think_message)
                        current_messages.append({"role":"tool","tool_call_id":tool_call["id"],"content":tool_result or "No results found."})
                    observation = f"Iteration {react_iteration}: " + " | ".join(tool_results)
                    accumulated_observations.append(observation)
                    continue_prompt = f"""
Based on the tool results above, do you have enough information to provide a COMPREHENSIVE answer to the user's question: "{prompt}"

If YES - provide the final answer now (do not call more tools).
If NO - call additional tools to gather more information.

Be thorough - it's better to make 2-3 tool calls than to answer with incomplete information.
"""
                    current_messages.append({"role":"user", "content": continue_prompt})
                else:
                    final_response_text = think_message.get("content", "")
                    print(f"✅ ReAct complete after {react_iteration} iterations")
                    break
            except Exception as e:
                print(f"ReAct iteration error: {e}")
                break
        if not final_response_text:
            final_prompt = f"""
{system_prompt}

━━━ SYNTHESIZE FINAL ANSWER ━━━
Based on all the information gathered through {react_iteration} reasoning iterations:
{chr(10).join(f"- {obs}" for obs in accumulated_observations)}

Now provide a COMPREHENSIVE, WELL-STRUCTURED answer to the user's question: "{prompt}"
Synthesize all the information above into a clear, actionable response.
"""
            current_messages = [{"role":"system","content":final_prompt}] + list(messages)
            try:
                final_res = requests.post(url, headers=headers, json={"model":"llama-3.3-70b-versatile","messages":current_messages,"temperature":0.3,"max_tokens":1000}, timeout=30)
                final_res.raise_for_status()
                final_response_text = final_res.json()["choices"][0]["message"].get("content", "")
            except Exception as e:
                print(f"Final synthesis error: {e}")
    # ═══════════════════════════════════════════════════════════════════════
    # Standard single-pass processing (for non-complex queries)
    # ═══════════════════════════════════════════════════════════════════════
    if not final_response_text:
        MAX_ITERATIONS   = 10
        response_text    = ""
        current_messages = [{"role":"system","content":system_prompt}] + list(messages)
        reflection_attempts = 0
        MAX_REFLECTION_ATTEMPTS = 2
        try:
            for iteration in range(MAX_ITERATIONS):
                data = {"model":"llama-3.3-70b-versatile","messages":current_messages,"tools":GROQ_TOOLS,"tool_choice":"auto","temperature":0.3,"max_tokens":1000}
                # Retry up to 3 times on 429 rate limit with backoff
                for _retry in range(3):
                    res = requests.post(url, headers=headers, json=data, timeout=30)
                    if res.status_code == 429:
                        wait = 2 ** _retry  # 1s, 2s, 4s
                        print(f"⏳ Groq 429 rate limit — retrying in {wait}s (attempt {_retry+1}/3)")
                        time.sleep(wait)
                        continue
                    break
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
                    agent_trace.append(fn_name)
                    tool_result, sources = run_tool_call(fn_name, fn_args, session_id=session_id, user_profile=user_profile, app_state=app_state)
                    all_sources.extend(sources)
                    if fn_name == "reflect_and_improve":
                        m = re.search(r'score (\d+)/10', tool_result)
                        if m: reflection_score = int(m.group(1))
                        reflection_attempts += 1
                        if "REFLECTION FAILED" in tool_result and reflection_attempts < MAX_REFLECTION_ATTEMPTS:
                            print(f"🔄 Reflection failed (score {reflection_score}/10) — triggering rewrite")
                    elif fn_name == "update_semantic_memory":
                        print(f"💾 Memory update: {tool_result}")
                    elif fn_name == "queue_autonomous_task":
                        print(f"🤖 Autonomous task queued: {fn_args.get('task_description','')[:40]}")
                    elif fn_name == "update_goal_progress":
                        print(f"🎯 Goal progress: {tool_result}")
                    elif fn_name == "register_new_tool":
                        print(f"🔧 New tool registered: {fn_args.get('tool_name','')}")
                    elif fn_name == "assess_confidence":
                        try:
                            assessment = json.loads(tool_result)
                            if assessment.get('needs_followup'):
                                tool_result += f"\n\nSUGGESTED FOLLOWUP: {assessment.get('suggestion', '')}"
                        except:
                            pass
                    current_messages.append({"role":"tool","tool_call_id":tool_call["id"],"content":tool_result or "No results found."})
            if not response_text:
                for _retry in range(3):
                    res2 = requests.post(url, headers=headers, json={**data,"tools":[],"tool_choice":"none"}, timeout=25)
                    if res2.status_code == 429:
                        wait = 2 ** _retry
                        print(f"⏳ Groq 429 fallback rate limit — retrying in {wait}s")
                        time.sleep(wait)
                        continue
                    break
                res2.raise_for_status()
                response_text = res2.json()["choices"][0]["message"].get("content", "")
            final_response_text = response_text
        except Exception as e:
            import traceback
            print(f"🔴 GROQ ERROR TYPE: {type(e).__name__}")
            print(f"🔴 GROQ ERROR DETAIL: {e}")
            print(f"🔴 GROQ FULL TRACEBACK:\n{traceback.format_exc()}")
            return "I apologise — connection trouble. Please reach out to the CWC team directly.", []
    response_text = final_response_text
    # v7.1: Check for and append pending notifications
    pending_notification = get_and_clear_notification(session_id)
    if pending_notification:
        response_text += f"\n\n💡 {pending_notification}"
    # v7.0: Store in episodic memory
    if app_state and hasattr(app_state, 'memory'):
        app_state.memory.store_episodic(session_id, prompt, response_text, reflection_score, intent_data['primary'], metadata={"agent_trace": agent_trace, "react_iterations": react_iteration})
    new_score = calculate_lead_score(user_profile or {}, prompt, intent_data['primary'])
    update_user_profile(session_id, last_intent=intent_data['primary'], region_interest=intent_data['region'], lead_score=new_score, language=lang)
    save_conversation(session_id, prompt, response_text, region=intent_data['region'], intent=intent_data['primary'], reflection_score=reflection_score)
    learn_from_interaction(session_id, prompt, response_text, reflection_score, intent_data['primary'])
    if message_count > 0 and message_count % 5 == 0:
        _update_conversation_summary(session_id)
    if message_count > 0 and message_count % 3 == 0:
        _extract_and_save_key_facts(session_id, user_profile or {})
    return response_text, list(dict.fromkeys(s for s in all_sources if s))[:5]

# ============================================================
# DATABASE FUNCTIONS
# ============================================================
def get_db():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)

def init_db():
    conn = get_db()
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS conversations (id SERIAL PRIMARY KEY, session_id TEXT, user_message TEXT, ai_response TEXT, timestamp TIMESTAMP DEFAULT NOW(), region TEXT, intent TEXT, reflection_score INTEGER DEFAULT 5)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_conversations_session ON conversations(session_id, timestamp)")
    c.execute("CREATE TABLE IF NOT EXISTS user_profiles (session_id TEXT PRIMARY KEY, language TEXT, name TEXT, email TEXT, company TEXT, sector TEXT, region_interest TEXT, last_intent TEXT, lead_score INTEGER DEFAULT 0, first_seen TIMESTAMP DEFAULT NOW(), last_seen TIMESTAMP DEFAULT NOW(), visit_count INTEGER DEFAULT 1, is_returning BOOLEAN DEFAULT FALSE, conversation_summary TEXT, key_facts JSONB DEFAULT '{}', task_history JSONB DEFAULT '[]', topics_discussed TEXT, preferred_contact TEXT, urgency TEXT)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_user_profiles_lead_score ON user_profiles(lead_score DESC)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_user_profiles_last_seen ON user_profiles(last_seen DESC)")
    c.execute("CREATE TABLE IF NOT EXISTS agent_tasks (id SERIAL PRIMARY KEY, session_id TEXT, task_description TEXT, status TEXT DEFAULT 'pending', sub_tasks JSONB DEFAULT '[]', result TEXT, created_at TIMESTAMP DEFAULT NOW(), completed_at TIMESTAMP)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_agent_tasks_status ON agent_tasks(status, created_at)")
    c.execute("CREATE TABLE IF NOT EXISTS tool_registry (id SERIAL PRIMARY KEY, tool_name TEXT UNIQUE, description TEXT, implementation TEXT, created_by TEXT, created_at TIMESTAMP DEFAULT NOW(), deployed BOOLEAN DEFAULT FALSE)")
    c.execute("CREATE TABLE IF NOT EXISTS agent_versions (id SERIAL PRIMARY KEY, prompt_hash TEXT UNIQUE, prompt_text TEXT, performance_score REAL, deployed BOOLEAN DEFAULT FALSE, created_at TIMESTAMP DEFAULT NOW())")
    c.execute("CREATE TABLE IF NOT EXISTS environment_alerts (id SERIAL PRIMARY KEY, source TEXT, change_detected JSONB, user_segments JSONB, notified BOOLEAN DEFAULT FALSE, created_at TIMESTAMP DEFAULT NOW())")
    # Migrate existing databases that may be missing newer columns
    migrations = [
        "ALTER TABLE user_profiles ADD COLUMN IF NOT EXISTS is_returning BOOLEAN DEFAULT FALSE",
        "ALTER TABLE user_profiles ADD COLUMN IF NOT EXISTS conversation_summary TEXT",
        "ALTER TABLE user_profiles ADD COLUMN IF NOT EXISTS key_facts JSONB DEFAULT '{}'",
        "ALTER TABLE user_profiles ADD COLUMN IF NOT EXISTS task_history JSONB DEFAULT '[]'",
        "ALTER TABLE user_profiles ADD COLUMN IF NOT EXISTS topics_discussed TEXT",
        "ALTER TABLE user_profiles ADD COLUMN IF NOT EXISTS preferred_contact TEXT",
        "ALTER TABLE user_profiles ADD COLUMN IF NOT EXISTS urgency TEXT",
    ]
    for migration in migrations:
        try:
            c.execute(migration)
        except Exception as e:
            print(f"Migration skipped (already exists): {e}")
    conn.commit()
    conn.close()
    print("✅ Database initialized")

def save_conversation(session_id: str, user_msg: str, ai_response: str, region: str = None, intent: str = None, reflection_score: int = 5):
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("INSERT INTO conversations (session_id, user_message, ai_response, region, intent, reflection_score) VALUES (%s, %s, %s, %s, %s, %s)", (session_id, user_msg, ai_response, region, intent, reflection_score))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Save conversation error: {e}")

def get_conversation_history(session_id: str, limit: int = 10) -> List[tuple]:
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT user_message, ai_response FROM conversations WHERE session_id=%s ORDER BY timestamp DESC LIMIT %s", (session_id, limit))
        rows = c.fetchall()
        conn.close()
        return [(r['user_message'], r['ai_response']) for r in reversed(rows)]
    except Exception as e:
        print(f"Get history error: {e}")
        return []

def get_message_count(session_id: str) -> int:
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM conversations WHERE session_id=%s", (session_id,))
        count = c.fetchone()['count']
        conn.close()
        return count
    except:
        return 0

def get_or_create_user_profile(session_id: str) -> dict:
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT * FROM user_profiles WHERE session_id=%s", (session_id,))
        row = c.fetchone()
        if not row:
            c.execute("INSERT INTO user_profiles (session_id, first_seen, last_seen) VALUES (%s, NOW(), NOW())", (session_id,))
            conn.commit()
            c.execute("SELECT * FROM user_profiles WHERE session_id=%s", (session_id,))
            row = c.fetchone()
        else:
            c.execute("UPDATE user_profiles SET visit_count=visit_count+1, last_seen=NOW(), is_returning=TRUE WHERE session_id=%s", (session_id,))
            conn.commit()
        conn.close()
        return dict(row) if row else {}
    except Exception as e:
        print(f"Profile error: {e}")
        return {}

def update_user_profile(session_id: str, **kwargs):
    try:
        conn = get_db()
        c = conn.cursor()
        allowed = ['language', 'name', 'email', 'company', 'sector', 'region_interest', 'last_intent', 'lead_score', 'conversation_summary', 'key_facts', 'task_history', 'topics_discussed', 'preferred_contact', 'urgency']
        updates = {k: v for k, v in kwargs.items() if k in allowed}
        if updates:
            set_clause = ", ".join([f"{k}=%s" for k in updates.keys()])
            values = list(updates.values()) + [session_id]
            c.execute(f"UPDATE user_profiles SET {set_clause} WHERE session_id=%s", values)
            conn.commit()
        conn.close()
    except Exception as e:
        print(f"Update profile error: {e}")

# ============================================================
# EMAIL FUNCTIONS
# ============================================================
def send_email_brevo(to_email: str, subject: str, body: str) -> bool:
    if not BREVO_API_KEY:
        print("⚠️ Brevo API key not configured")
        return False
    try:
        url = "https://api.brevo.com/v3/smtp/email"
        headers = {"api-key": BREVO_API_KEY, "Content-Type": "application/json"}
        payload = {"sender": {"email": SENDER_EMAIL, "name": "Sophia - CWC AI"}, "to": [{"email": to_email}], "subject": subject, "htmlContent": body.replace("\n", "<br>")}
        res = requests.post(url, headers=headers, json=payload, timeout=15)
        if res.status_code in (200, 201, 202):
            print(f"✉️ Email sent to {to_email}")
            return True
        else:
            print(f"Email error: {res.status_code} - {res.text}")
            return False
    except Exception as e:
        print(f"Email send error: {e}")
        return False

# ============================================================
# TOOL FUNCTIONS
# ============================================================
def search_web(query: str) -> tuple:
    if not TAVILY_API_KEY:
        return "", []
    try:
        res = requests.post(
            "https://api.tavily.com/search",
            headers={"Authorization": f"Bearer {TAVILY_API_KEY}", "Content-Type": "application/json"},
            json={"query": query, "max_results": 5, "search_depth": "basic"},
            timeout=15
        )
        data = res.json()
        results = data.get("results", [])
        content = "\n".join([r.get("content", "") for r in results[:3]])
        sources = [r.get("url", "") for r in results if r.get("url")]
        return content, sources
    except Exception as e:
        print(f"Search error: {e}")
        return "", []

def lookup_chinese_company(company_name: str) -> dict:
    result = {"company_name": company_name, "registration_status": "Unknown", "details": "", "warning": None}
    try:
        content, sources = search_web(f"{company_name} China SAMR registration business license")
        if "registered" in content.lower() or "license" in content.lower():
            result["registration_status"] = "Registered"
        if any(red in content.lower() for red in ["fraud", "scam", "complaint", "blacklist", "lawsuit"]):
            result["warning"] = "Red flags detected in search results"
        result["details"] = content[:500]
    except Exception as e:
        result["details"] = f"Lookup error: {e}"
    return result

def search_suppliers(product: str) -> dict:
    return {
        "market_context": f"China is the world's largest producer of {product}. MOQ typically 500-1000 units. Lead time 30-60 days.",
        "typical_moq": "500-1000 units",
        "price_range": "Contact for quote based on volume",
        "key_considerations": ["Verify factory certifications", "Request samples before mass production", "Negotiate payment terms (30/70 common)", "Consider third-party inspection"]
    }

def generate_risk_report(company_name: str, context: str) -> tuple:
    lookup = lookup_chinese_company(company_name)
    risk_content, sources = search_web(f"{company_name} China fraud scam complaints risk")
    report = f"""RISK ASSESSMENT: {company_name}
Registration: {lookup['registration_status']}
Warning: {lookup['warning'] or 'None detected'}
Risk Signals: {risk_content[:400] if risk_content else 'No significant red flags'}
Recommendation: {'Proceed with caution - additional verification recommended' if lookup['warning'] else 'Standard due diligence advised'}
"""
    return report, sources

def find_suppliers(product: str, region: str = "China") -> tuple:
    market_data = search_suppliers(product)
    content = f"""SUPPLIER INTELLIGENCE: {product} from {region}
{market_data['market_context']}
MOQ: {market_data['typical_moq']}
Price Range: {market_data['price_range']}
Key Considerations:
{chr(10).join(['• ' + c for c in market_data['key_considerations']])}

CWC can provide verified supplier matching with factory audits.
"""
    return content, []

def decompose_task(user_message: str, user_profile: dict) -> dict:
    if not DEEPSEEK_API_KEY:
        return None
    try:
        res = requests.post(
            "https://api.deepseek.com/chat/completions",
            headers={"Authorization": f"Bearer {DEEPSEEK_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": "deepseek-chat",
                "messages": [
                    {"role": "system", "content": "You are a task planner. Break down the user's request into 2-4 specific sub-tasks. Return ONLY valid JSON with format: {task_summary: string, sub_tasks: [{action: string, query: string, expected_output: string}], expected_output: string}"},
                    {"role": "user", "content": f"Break down this request into sub-tasks: {user_message}"}
                ],
                "temperature": 0.2,
                "max_tokens": 400
            },
            timeout=15
        )
        response = res.json()["choices"][0]["message"]["content"]
        json_match = re.search(r'\{.*\}', response, re.DOTALL)
        if json_match:
            return json.loads(json_match.group())
    except Exception as e:
        print(f"Task decomposition error: {e}")
    return None

def reflect_and_improve(response: str, user_message: str, context: dict) -> tuple:
    if not DEEPSEEK_API_KEY:
        return 5, response
    try:
        res = requests.post(
            "https://api.deepseek.com/chat/completions",
            headers={"Authorization": f"Bearer {DEEPSEEK_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": "deepseek-chat",
                "messages": [
                    {"role": "system", "content": "You are a quality reviewer. Score this response 1-10 and suggest improvements. Return format: SCORE: X/10\nIMPROVED: [better response]"},
                    {"role": "user", "content": f"User: {user_message}\nResponse: {response}"}
                ],
                "temperature": 0.2,
                "max_tokens": 300
            },
            timeout=15
        )
        review = res.json()["choices"][0]["message"]["content"]
        score_match = re.search(r'SCORE:\s*(\d+)', review)
        score = int(score_match.group(1)) if score_match else 5
        improved_match = re.search(r'IMPROVED:\s*(.+)', review, re.DOTALL)
        improved = improved_match.group(1).strip() if improved_match else response
        return score, improved
    except Exception as e:
        print(f"Reflection error: {e}")
        return 5, response

def update_semantic_memory(fact_type: str, fact_value: str, importance: int, source: str) -> str:
    return f"Stored: {fact_type} = {fact_value} (importance: {importance}/10)"

def queue_autonomous_task(session_id: str, task_description: str, priority: str = "medium") -> str:
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("INSERT INTO agent_tasks (session_id, task_description, status, created_at) VALUES (%s, %s, 'pending', NOW()) RETURNING id", (session_id, task_description))
        task_id = c.fetchone()[0]
        conn.commit()
        conn.close()
        return f"Task #{task_id} queued: {task_description[:50]}"
    except Exception as e:
        return f"Queue error: {e}"

def update_goal_progress(session_id: str, goal: str, milestone: str, status: str) -> str:
    update_goal_state(session_id, goal, milestone, status)
    return f"Goal updated: {goal} → {milestone} [{status}]"

def assess_confidence(response: str, user_message: str, context: dict, tool_calls: List[str]) -> str:
    meta = MetaCognitiveLayer(None)
    result = meta.assess_confidence(response, user_message, context, tool_calls)
    return json.dumps(result)

def register_new_tool(tool_name: str, description: str, implementation: str) -> str:
    registry = ToolRegistry()
    success = registry.register_tool(tool_name, description, implementation)
    return f"Tool '{tool_name}' registered successfully" if success else f"Failed to register '{tool_name}'"

def predict_user_intent(current_intent: str, user_profile: dict) -> str:
    engine = PredictiveIntentEngine(None)
    result = engine.predict_next_intent(current_intent, user_profile)
    return json.dumps(result)

def execute_parallel_agents(task: str, context: dict, user_msg: str, session_id: str) -> str:
    orchestrator = AgentOrchestrator()
    result = asyncio.run(orchestrator.parallel_execute(task, context, user_msg, session_id))
    return result['consensus']

def run_tool_call(fn_name: str, fn_args: dict, session_id: str = None, user_profile: dict = None, app_state: dict = None) -> tuple:
    sources = []
    if fn_name == "search_web":
        result, sources = search_web(fn_args.get("query", ""))
    elif fn_name == "lookup_chinese_company":
        result = json.dumps(lookup_chinese_company(fn_args.get("company_name", "")))
    elif fn_name == "search_suppliers":
        result = json.dumps(search_suppliers(fn_args.get("product", "")))
    elif fn_name == "generate_risk_report":
        result, sources = generate_risk_report(fn_args.get("company_name", ""), fn_args.get("context", ""))
    elif fn_name == "find_suppliers":
        result, sources = find_suppliers(fn_args.get("product", ""), fn_args.get("region", "China"))
    elif fn_name == "reflect_and_improve":
        score, improved = reflect_and_improve(fn_args.get("response", ""), fn_args.get("user_message", ""), fn_args.get("context", {}))
        result = f"REFLECTION SCORE: {score}/10\n\n{improved}"
    elif fn_name == "update_semantic_memory":
        result = update_semantic_memory(fn_args.get("fact_type", ""), fn_args.get("fact_value", ""), fn_args.get("importance", 5), fn_args.get("source", ""))
    elif fn_name == "queue_autonomous_task":
        result = queue_autonomous_task(fn_args.get("session_id", session_id), fn_args.get("task_description", ""), fn_args.get("priority", "medium"))
    elif fn_name == "update_goal_progress":
        result = update_goal_progress(fn_args.get("session_id", session_id), fn_args.get("goal", ""), fn_args.get("milestone", ""), fn_args.get("status", "in_progress"))
    elif fn_name == "register_new_tool":
        result = register_new_tool(fn_args.get("tool_name", ""), fn_args.get("description", ""), fn_args.get("implementation", ""))
    elif fn_name == "assess_confidence":
        result = assess_confidence(fn_args.get("response", ""), fn_args.get("user_message", ""), fn_args.get("context", {}), fn_args.get("tool_calls", []))
    elif fn_name == "predict_user_intent":
        result = predict_user_intent(fn_args.get("current_intent", ""), fn_args.get("user_profile", {}))
    elif fn_name == "execute_parallel_agents":
        result = execute_parallel_agents(fn_args.get("task", ""), fn_args.get("context", {}), fn_args.get("user_msg", ""), fn_args.get("session_id", session_id))
    else:
        result = f"Unknown tool: {fn_name}"
    return result, sources

# ============================================================
# INTENT DETECTION
# ============================================================
def detect_language(text: str) -> str:
    text_lower = text.lower()
    chinese_chars = sum(1 for c in text if '\u4e00' <= c <= '\u9fff')
    if chinese_chars > len(text) * 0.3:
        return "zh"
    greek_chars = sum(1 for c in text if '\u0370' <= c <= '\u03ff' or '\u1f00' <= c <= '\u1fff')
    if greek_chars > len(text) * 0.3:
        return "el"
    arabic_chars = sum(1 for c in text if '\u0600' <= c <= '\u06ff')
    if arabic_chars > len(text) * 0.3:
        return "ar"
    if any(kw in text_lower for kw in ["bonjour", "comment", "français", "merci"]):
        return "fr"
    if any(kw in text_lower for kw in ["hola", "cómo", "español", "gracias"]):
        return "es"
    if any(kw in text_lower for kw in ["guten tag", "wie", "deutsch", "danke"]):
        return "de"
    return "en"

LANGUAGE_INSTRUCTIONS = {
    "zh": "Respond in Chinese (Mandarin). Use professional business language.",
    "el": "Respond in Greek. Use professional business language.",
    "ar": "Respond in Arabic. Use professional business language.",
    "fr": "Respond in French. Use professional business language.",
    "es": "Respond in Spanish. Use professional business language.",
    "de": "Respond in German. Use professional business language.",
    "en": "Respond in English. Use professional business language."
}

def detect_intent(message: str) -> dict:
    msg_lower = message.lower()
    intents = {
        "supplier_verification": ["verify", "check", "audit", "due diligence", "fraud", "scam", "legit", "real company", "background check"],
        "supplier_search": ["find supplier", "source", "manufacturer", "factory", "vendor", "looking for", "need a supplier"],
        "consultation_request": ["consultation", "meeting", "call", "talk", "discuss", "advice", "help me"],
        "high_intent_lead": ["quote", "proposal", "price", "cost", "budget", "investment", "interested in", "ready to"],
        "information_gathering": ["what is", "how to", "tell me about", "explain", "information", "learn about"]
    }
    scores = {}
    for intent, keywords in intents.items():
        score = sum(2 if kw in msg_lower else 0 for kw in keywords)
        if score > 0:
            scores[intent] = score
    primary_intent = max(scores, key=scores.get) if scores else "general"
    score = scores.get(primary_intent, 0)
    region = None
    if any(w in msg_lower for w in ["china", "chinese", "cn"]):
        region = "China"
    elif any(w in msg_lower for w in ["europe", "eu", "germany", "france", "italy"]):
        region = "Europe"
    return {"primary": primary_intent, "score": min(score * 10, 100), "region": region}

# ============================================================
# QUALIFICATION & LEAD SCORING
# ============================================================
def check_qualification_gaps(profile: dict, message_count: int) -> str:
    gaps = []
    if not profile.get('region_interest') and message_count >= 2:
        gaps.append("direction (West→China or China→West?)")
    if not profile.get('topics_discussed') and message_count >= 3:
        gaps.append("sector/industry")
    if not profile.get('urgency') and message_count >= 4:
        gaps.append("timeline/urgency")
    if gaps:
        return f"\n📋 QUALIFICATION GAPS: Still need: {', '.join(gaps)}"
    return ""

def check_escalation_trigger(profile: dict, message_count: int, message: str) -> bool:
    if profile.get('lead_score', 0) >= 70 and message_count >= 3:
        return True
    urgent_keywords = ["urgent", "asap", "immediately", "fraud", "scam", "lawsuit", "legal action"]
    if any(kw in message.lower() for kw in urgent_keywords):
        return True
    return False

def calculate_lead_score(profile: dict, message: str, intent: str) -> int:
    score = profile.get('lead_score', 0)
    if intent == "high_intent_lead":
        score += 15
    elif intent == "consultation_request":
        score += 10
    elif intent == "supplier_verification":
        score += 8
    if any(kw in message.lower() for kw in ["budget", "investment", "funding", "capital"]):
        score += 10
    if any(kw in message.lower() for kw in ["timeline", "deadline", "asap", "urgent"]):
        score += 5
    return min(score, 100)

# ============================================================
# CONVERSATION SUMMARY & KEY FACTS
# ============================================================
def _update_conversation_summary(session_id: str):
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT user_message, ai_response FROM conversations WHERE session_id=%s ORDER BY timestamp DESC LIMIT 10", (session_id,))
        recent = c.fetchall()
        if len(recent) >= 5 and GROQ_API_KEY:
            conversation_text = "\n".join([f"User: {r['user_message']}\nSophia: {r['ai_response']}" for r in recent])
            res = requests.post(
                "https://api.deepseek.com/chat/completions",
                headers={"Authorization": f"Bearer {DEEPSEEK_API_KEY}", "Content-Type": "application/json"},
                json={
                    "model": "deepseek-chat",
                    "messages": [
                        {"role": "system", "content": "Summarize this conversation in 2-3 sentences. Focus on user's goals, interests, and next steps."},
                        {"role": "user", "content": conversation_text}
                    ],
                    "temperature": 0.3,
                    "max_tokens": 150
                },
                timeout=15
            )
            summary = res.json()["choices"][0]["message"]["content"]
            c.execute("UPDATE user_profiles SET conversation_summary=%s WHERE session_id=%s", (summary, session_id))
            conn.commit()
        conn.close()
    except Exception as e:
        print(f"Summary update error: {e}")

def _extract_and_save_key_facts(session_id: str, profile: dict):
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT user_message FROM conversations WHERE session_id=%s ORDER BY timestamp DESC LIMIT 5", (session_id,))
        messages = [r['user_message'] for r in c.fetchall()]
        if messages and GROQ_API_KEY:
            combined = "\n".join(messages)
            res = requests.post(
                "https://api.deepseek.com/chat/completions",
                headers={"Authorization": f"Bearer {DEEPSEEK_API_KEY}", "Content-Type": "application/json"},
                json={
                    "model": "deepseek-chat",
                    "messages": [
                        {"role": "system", "content": "Extract key facts from these messages. Return JSON with fields: company_name, sector, region_interest, timeline, budget_range. Use null if not found."},
                        {"role": "user", "content": combined}
                    ],
                    "temperature": 0.2,
                    "max_tokens": 200
                },
                timeout=15
            )
            response = res.json()["choices"][0]["message"]["content"]
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if json_match:
                facts = json.loads(json_match.group())
                current_facts = profile.get('key_facts', {}) or {}
                for key, value in facts.items():
                    if value and value != "null":
                        current_facts[key] = value
                c.execute("UPDATE user_profiles SET key_facts=%s::jsonb WHERE session_id=%s", (json.dumps(current_facts), session_id))
                conn.commit()
        conn.close()
    except Exception as e:
        print(f"Key facts extraction error: {e}")

# ============================================================
# GROQ TOOLS DEFINITION
# ============================================================
GROQ_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "search_web",
            "description": "Search the web for current information about China business topics",
            "parameters": {
                "type": "object",
                "properties": {"query": {"type": "string", "description": "Search query"}},
                "required": ["query"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "lookup_chinese_company",
            "description": "Look up a Chinese company in SAMR registry and check for red flags",
            "parameters": {
                "type": "object",
                "properties": {"company_name": {"type": "string", "description": "Company name to look up"}},
                "required": ["company_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "generate_risk_report",
            "description": "Generate a comprehensive risk report for a Chinese company",
            "parameters": {
                "type": "object",
                "properties": {
                    "company_name": {"type": "string"},
                    "context": {"type": "string", "description": "Context for the risk assessment"}
                },
                "required": ["company_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "find_suppliers",
            "description": "Find suppliers for a specific product in China",
            "parameters": {
                "type": "object",
                "properties": {
                    "product": {"type": "string"},
                    "region": {"type": "string", "default": "China"}
                },
                "required": ["product"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "reflect_and_improve",
            "description": "Reflect on the drafted response and improve it if needed",
            "parameters": {
                "type": "object",
                "properties": {
                    "response": {"type": "string"},
                    "user_message": {"type": "string"},
                    "context": {"type": "object"}
                },
                "required": ["response", "user_message"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "update_semantic_memory",
            "description": "Store important facts in semantic memory for future reference",
            "parameters": {
                "type": "object",
                "properties": {
                    "fact_type": {"type": "string"},
                    "fact_value": {"type": "string"},
                    "importance": {"type": "integer", "minimum": 1, "maximum": 10},
                    "source": {"type": "string"}
                },
                "required": ["fact_type", "fact_value", "importance"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "queue_autonomous_task",
            "description": "Queue a background task for autonomous execution",
            "parameters": {
                "type": "object",
                "properties": {
                    "session_id": {"type": "string"},
                    "task_description": {"type": "string"},
                    "priority": {"type": "string", "enum": ["low", "medium", "high"]}
                },
                "required": ["session_id", "task_description"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "update_goal_progress",
            "description": "Update progress on a user's goal",
            "parameters": {
                "type": "object",
                "properties": {
                    "session_id": {"type": "string"},
                    "goal": {"type": "string"},
                    "milestone": {"type": "string"},
                    "status": {"type": "string", "enum": ["pending", "in_progress", "completed", "blocked"]}
                },
                "required": ["session_id", "goal", "milestone", "status"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "assess_confidence",
            "description": "Assess confidence in the current response and suggest followups if needed",
            "parameters": {
                "type": "object",
                "properties": {
                    "response": {"type": "string"},
                    "user_message": {"type": "string"},
                    "context": {"type": "object"},
                    "tool_calls": {"type": "array", "items": {"type": "string"}}
                },
                "required": ["response", "user_message"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "register_new_tool",
            "description": "Register a new reusable tool for future use",
            "parameters": {
                "type": "object",
                "properties": {
                    "tool_name": {"type": "string"},
                    "description": {"type": "string"},
                    "implementation": {"type": "string"}
                },
                "required": ["tool_name", "description", "implementation"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "predict_user_intent",
            "description": "Predict what the user will ask next",
            "parameters": {
                "type": "object",
                "properties": {
                    "current_intent": {"type": "string"},
                    "user_profile": {"type": "object"}
                },
                "required": ["current_intent"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "execute_parallel_agents",
            "description": "Execute multiple specialist agents in parallel for complex queries",
            "parameters": {
                "type": "object",
                "properties": {
                    "task": {"type": "string"},
                    "context": {"type": "object"},
                    "user_msg": {"type": "string"},
                    "session_id": {"type": "string"}
                },
                "required": ["task", "context", "user_msg", "session_id"]
            }
        }
    }
]

# ============================================================
# FASTAPI APPLICATION
# ============================================================
class ChatRequest(BaseModel):
    message: str
    session_id: str = "anonymous"
    quick_action: str = None
    deep_search: bool = False

class ChatResponse(BaseModel):
    response: str
    sources: List[str]

class TaskQueueRequest(BaseModel):
    session_id: str
    task_description: str
    priority: str = "medium"

class AdminStatsResponse(BaseModel):
    total_conversations: int
    unique_users: int
    avg_lead_score: float
    top_intents: List[dict]
    recent_tasks: List[dict]

# ============================================================
# APP STATE MANAGEMENT
# ============================================================
class AppState:
    def __init__(self):
        self.memory = AgenticMemory()
        self.self_improvement = SelfImprovementEngine()
        self.environment_monitor = EnvironmentMonitor()
        self.tool_registry = ToolRegistry()
        self.predictive_intent = PredictiveIntentEngine(self.memory)
        self.meta_cognitive = MetaCognitiveLayer(self.memory)
        self.orchestrator = AgentOrchestrator()

app_state = AppState()

# ============================================================
# LIFESPAN MANAGEMENT
# ============================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    asyncio.create_task(poll_agent_tasks())
    asyncio.create_task(proactive_followup_tasks())
    asyncio.create_task(app_state.environment_monitor.poll_sources())
    asyncio.create_task(daily_self_improvement())
    yield
    print("🛑 Shutting down...")

async def daily_self_improvement():
    while True:
        await asyncio.sleep(86400)
        await app_state.self_improvement.analyze_performance(days=7)

# ============================================================
# FASTAPI APP
# ============================================================
app = FastAPI(title="Sophia AI v7.1", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health_check():
    return {"status": "ok", "version": "7.1", "features": ["ReAct Pattern", "Weighted Consensus", "Self-Triggered Tasks"]}

@app.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest, background_tasks: BackgroundTasks, req: Request):
    # Use real visitor IP even behind Render/Cloudflare reverse proxy
    forwarded_for = req.headers.get("X-Forwarded-For")
    client_ip = forwarded_for.split(",")[0].strip() if forwarded_for else req.client.host
    if is_rate_limited(client_ip):
        return ChatResponse(response="Rate limit exceeded. Please try again later.", sources=[])
    user_profile = get_or_create_user_profile(request.session_id)
    response_text, sources = ask_groq(
        prompt=request.message,
        session_id=request.session_id,
        user_profile=user_profile,
        quick_action=request.quick_action,
        deep_search=request.deep_search,
        app_state=app_state
    )
    return ChatResponse(response=response_text, sources=sources)

@app.post("/queue-task")
async def queue_task(request: TaskQueueRequest):
    result = queue_autonomous_task(request.session_id, request.task_description, request.priority)
    return {"status": "queued", "message": result}

@app.get("/admin/stats")
async def admin_stats(password: str = ""):
    if password != ADMIN_PASSWORD:
        return {"error": "Unauthorized"}
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM conversations")
        total_conversations = c.fetchone()['count']
        c.execute("SELECT COUNT(DISTINCT session_id) FROM conversations")
        unique_users = c.fetchone()['count']
        c.execute("SELECT AVG(lead_score) FROM user_profiles")
        avg_lead_score = c.fetchone()['avg'] or 0
        c.execute("SELECT intent, COUNT(*) FROM conversations WHERE intent IS NOT NULL GROUP BY intent ORDER BY COUNT(*) DESC LIMIT 5")
        top_intents = [{"intent": r['intent'], "count": r['count']} for r in c.fetchall()]
        c.execute("SELECT id, session_id, task_description, status, created_at FROM agent_tasks ORDER BY created_at DESC LIMIT 10")
        recent_tasks = [{"id": r['id'], "session_id": r['session_id'][:8], "description": r['task_description'][:50], "status": r['status'], "created_at": r['created_at'].isoformat()} for r in c.fetchall()]
        conn.close()
        return AdminStatsResponse(
            total_conversations=total_conversations,
            unique_users=unique_users,
            avg_lead_score=round(avg_lead_score, 1),
            top_intents=top_intents,
            recent_tasks=recent_tasks
        )
    except Exception as e:
        return {"error": str(e)}

@app.get("/admin/conversations")
async def admin_conversations(password: str = "", limit: int = 50):
    if password != ADMIN_PASSWORD:
        return {"error": "Unauthorized"}
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT session_id, user_message, ai_response, timestamp, intent, reflection_score FROM conversations ORDER BY timestamp DESC LIMIT %s", (limit,))
        conversations = [{"session_id": r['session_id'][:8], "user_message": r['user_message'][:100], "ai_response": r['ai_response'][:100], "timestamp": r['timestamp'].isoformat(), "intent": r['intent'], "reflection_score": r['reflection_score']} for r in c.fetchall()]
        conn.close()
        return {"conversations": conversations}
    except Exception as e:
        return {"error": str(e)}

@app.api_route("/", methods=["GET", "HEAD"])
def root():
    return {"message": "Sophia AI v7.1 - Fully Agentic China-West Business Advisor", "docs": "/docs"}

# ------------------------------------------------------------------
# Compatibility stub: old widget versions POST to /new-session on load.
# This endpoint doesn't need to do anything — just return 200 so the
# browser doesn't log a CORS/404 error on every page view.
# ------------------------------------------------------------------
@app.post("/new-session")
async def new_session(req: Request):
    return {"status": "ok"}

# Explicit OPTIONS handler ensures preflight succeeds even if a CDN
# or WordPress security plugin intercepts the automatic CORS response.
@app.options("/chat")
async def options_chat():
    from fastapi.responses import Response as FastResponse
    return FastResponse(
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, Accept",
        }
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
