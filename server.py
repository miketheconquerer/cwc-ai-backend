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
from collections import defaultdict
from datetime import datetime, timedelta
from dotenv import load_dotenv
import asyncio
from contextlib import asynccontextmanager

load_dotenv()

# ============================================================
# CONFIGURATION
# ============================================================
BREVO_API_KEY   = os.getenv("BREVO_API_KEY", "")
GROQ_API_KEY    = os.getenv("GROQ_API_KEY")
TAVILY_API_KEY  = os.getenv("TAVILY_API_KEY")
ADMIN_PASSWORD  = os.getenv("ADMIN_PASSWORD", "")
SENDER_EMAIL    = os.getenv("SENDER_EMAIL", "888nv666@gmail.com")
RECIPIENT_EMAIL = "digkasm@proton.me"
DATABASE_URL    = os.getenv("DATABASE_URL")

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
# v5.1: PROCEDURAL MEMORY WORKFLOWS
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
    """Execute cached workflow without LLM reasoning - pure code execution for speed"""
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
# v5.1: HTN HIERARCHICAL TASK NETWORK PLANNING
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
    """HTN planning: decompose task using applicable methods with preconditions"""
    methods = HTN_METHODS.get(task, [])
    for method in methods:
        if method["precondition"](context):
            print(f"✓ HTN method selected: {method['name']} for {task}")
            return method["subtasks"]
    return []

def execute_htn_plan(plan: list, session_id: str, user_profile: dict, original_message: str) -> dict:
    """
    v6.0 UPGRADE: Execute HTN plan with proper output chaining and conditional re-planning.
    Step N outputs are parsed and fed as named inputs to step N+1.
    If fraud/risk is detected mid-plan, automatically switches to emergency plan.
    """
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
                # v6.0: Named outputs from previous steps for proper chaining
                "company_lookup_result": state["named_outputs"].get("company_lookup_result"),
                "risk_signals": state["named_outputs"].get("risk_signals"),
                "supplier_results": state["named_outputs"].get("supplier_results"),
            }
            
            if agent_type != "main":
                result = agent_delegate(agent_type, step["task"], json.dumps(step_context), depth=0)
            else:
                result = f"Main agent executed: {step['task']}"
                
            # v6.0: Parse and store named outputs for chaining
            result_str = result if isinstance(result, str) else json.dumps(result)
            if step["task"] in ("lookup_company", "immediate_lookup"):
                state["named_outputs"]["company_lookup_result"] = result_str
                # v6.0: Conditional re-planning — if fraud detected, escalate to emergency plan
                if any(flag in result_str.upper() for flag in ["FRAUD", "WARNING", "RED FLAG", "SCAM", "BLACKLIST"]):
                    print("🚨 v6.0 Re-planning triggered: fraud/risk detected mid-plan")
                    emergency_plan = htn_plan("urgent_due_diligence", {
                        "message": original_message + " urgent fraud detected",
                        "company_name": user_profile.get("key_facts", {}).get("company_name", "")
                    })
                    if emergency_plan:
                        state["completed"].append({
                            "task": step["task"], "agent": agent_type,
                            "result": result_str[:200], "replanned": True
                        })
                        state["outputs"].append(result)
                        # Switch to emergency plan for remaining steps
                        remaining_emergency = execute_htn_plan(emergency_plan, session_id, user_profile, original_message)
                        state["completed"].extend(remaining_emergency["completed"])
                        state["failed"].extend(remaining_emergency["failed"])
                        state["outputs"].extend(remaining_emergency["outputs"])
                        return state
                        
            elif step["task"] in ("generate_risk_report", "red_flag_check"):
                state["named_outputs"]["risk_signals"] = result_str
            elif step["task"] in ("search_suppliers", "present_options"):
                state["named_outputs"]["supplier_results"] = result_str
                
            state["completed"].append({
                "task": step["task"], 
                "agent": agent_type,
                "result": result_str[:200]
            })
            state["outputs"].append(result)
            
        except Exception as e:
            state["failed"].append({
                "task": step["task"],
                "agent": agent_type,
                "error": str(e)
            })
            break
            
    return state

# ============================================================
# v5.1: AGENT-TO-AGENT DELEGATION SYSTEM
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
    """Agents can delegate to other specialists (recursive, max depth 2)"""
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
    """Execute specialist agent with full delegation capabilities"""
    if not GROQ_API_KEY:
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
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": "llama-3.3-70b-versatile",
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
# v5.1: CONTINUOUS LEARNING FROM REFLECTION
# ============================================================
def normalize_term(word: str) -> str:
    """v6.0: Stem words to improve pattern matching accuracy"""
    for suffix in ['ing', 'tion', 'ations', 'ation', 'ed', 'ers', 'er', 'ness', 'ity', 'ies', 'es']:
        if word.endswith(suffix) and len(word) - len(suffix) > 3:
            return word[:-len(suffix)]
    return word

def learn_from_interaction(session_id: str, user_message: str, ai_response: str, 
                          reflection_score: int, intent: str, user_feedback: str = None):
    """Extract and store learnable patterns from each interaction"""
    try:
        conn = get_db()
        c = conn.cursor()
        
        c.execute('''CREATE TABLE IF NOT EXISTS learned_patterns (
            id SERIAL PRIMARY KEY,
            pattern_type TEXT,
            trigger_condition TEXT,
            action_recommendation TEXT,
            context_type TEXT,
            success_count INTEGER DEFAULT 0,
            failure_count INTEGER DEFAULT 0,
            avg_reflection_score REAL DEFAULT 0,
            last_used TIMESTAMP,
            created_at TIMESTAMP DEFAULT NOW()
        )''')
        
        if reflection_score >= 8:
            pattern_type = "successful_response"
            trigger = extract_trigger_pattern(user_message, intent)
            recommendation = extract_success_pattern(ai_response)
            
            c.execute("SELECT id, success_count FROM learned_patterns WHERE pattern_type=%s AND trigger_condition=%s",
                      (pattern_type, trigger))
            existing = c.fetchone()
            
            if existing:
                c.execute("UPDATE learned_patterns SET success_count=success_count+1, avg_reflection_score=%s, last_used=%s WHERE id=%s",
                          (reflection_score, datetime.now(), existing[0]))
            else:
                c.execute("INSERT INTO learned_patterns (pattern_type, trigger_condition, action_recommendation, context_type, success_count, avg_reflection_score, last_used) VALUES (%s, %s, %s, %s, 1, %s, %s)",
                          (pattern_type, trigger, recommendation, intent, reflection_score, datetime.now()))
        
        elif reflection_score <= 4:
            pattern_type = "failed_approach"
            trigger = extract_trigger_pattern(user_message, intent)
            recommendation = "Avoid: " + ai_response[:100]
            
            c.execute("SELECT id, failure_count FROM learned_patterns WHERE pattern_type=%s AND trigger_condition=%s",
                      (pattern_type, trigger))
            existing = c.fetchone()
            
            if existing:
                c.execute("UPDATE learned_patterns SET failure_count=failure_count+1, last_used=%s WHERE id=%s",
                          (datetime.now(), existing[0]))
            else:
                c.execute("INSERT INTO learned_patterns (pattern_type, trigger_condition, action_recommendation, context_type, failure_count, last_used) VALUES (%s, %s, %s, %s, 1, %s)",
                          (pattern_type, trigger, recommendation, intent, datetime.now()))
        
        if user_profile := get_user_profile_light(session_id):
            prefs = extract_user_preferences(user_message)
            if prefs:
                c.execute("INSERT INTO learned_patterns (pattern_type, trigger_condition, action_recommendation, context_type, success_count, last_used) VALUES (%s, %s, %s, %s, 1, %s) ON CONFLICT DO NOTHING",
                          ("user_preference", f"user:{session_id}", json.dumps(prefs), "preference", datetime.now()))
        
        conn.commit()
        conn.close()
        print(f"🧠 Learned from interaction (score: {reflection_score})")
        
    except Exception as e:
        print(f"Learning error: {e}")

def extract_trigger_pattern(message: str, intent: str) -> str:
    """v6.0: Improved pattern extraction with word normalization for better matching"""
    words = message.lower().split()
    stopwords = {"about", "would", "could", "should", "there", "their", "which", "where", "when", "have", "been", "that", "this", "with", "from", "they", "will"}
    key_terms = [normalize_term(w) for w in words if len(w) > 4 and w not in stopwords]
    return f"intent:{intent}|terms:{','.join(key_terms[:5])}"

def extract_success_pattern(response: str) -> str:
    """Extract what made this response successful"""
    has_structure = "1." in response or "①" in response
    has_numbers = any(char.isdigit() for char in response)
    has_cta = any(kw in response.lower() for kw in ["contact", "schedule", "book", "call", "michail"])
    
    patterns = []
    if has_structure: patterns.append("structured_list")
    if has_numbers: patterns.append("specific_numbers")
    if has_cta: patterns.append("clear_cta")
    
    return "|".join(patterns) if patterns else "general_quality"

def extract_user_preferences(message: str) -> dict:
    """Extract implicit user preferences from message"""
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
    """Lightweight profile fetch for learning"""
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
    """Retrieve relevant learned patterns to augment system prompt"""
    try:
        conn = get_db()
        c = conn.cursor()
        
        trigger_like = f"%intent:{intent}%"
        
        c.execute("""
            SELECT action_recommendation, success_count, avg_reflection_score 
            FROM learned_patterns 
            WHERE pattern_type='successful_response' 
            AND trigger_condition LIKE %s 
            AND success_count >= 2
            ORDER BY avg_reflection_score DESC, success_count DESC 
            LIMIT 3
        """, (trigger_like,))
        success_patterns = c.fetchall()
        
        c.execute("""
            SELECT action_recommendation 
            FROM learned_patterns 
            WHERE pattern_type='user_preference' 
            AND trigger_condition=%s
            ORDER BY last_used DESC 
            LIMIT 1
        """, (f"user:{session_id}",))
        user_pref = c.fetchone()
        
        conn.close()
        
        advice_parts = []
        
        if success_patterns:
            patterns_text = "\n".join([
                f"• Pattern used {p[1]} times (avg score: {p[2]:.1f}/10): {p[0]}" 
                for p in success_patterns
            ])
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
# v6.0: GOAL TRACKING — Long-horizon state across sessions
# ============================================================
def update_goal_state(session_id: str, goal: str, milestone: str, status: str):
    """
    v6.0: Track multi-session goals so Sophia continues the user's journey
    rather than starting fresh each visit. Stored in task_history JSONB.
    status: 'pending' | 'in_progress' | 'done' | 'blocked'
    """
    try:
        profile = get_or_create_user_profile(session_id)
        task_history = profile.get('task_history', []) or []
        
        # Update existing goal milestone if found, else append
        updated = False
        for entry in task_history:
            if entry.get("goal") == goal and entry.get("milestone") == milestone:
                entry["status"] = status
                entry["updated_at"] = datetime.now().isoformat()
                updated = True
                break
        
        if not updated:
            task_history.append({
                "goal": goal,
                "milestone": milestone,
                "status": status,
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            })
        
        # Keep only last 15 entries
        update_user_profile(session_id, task_history=task_history[-15:])
        print(f"🎯 Goal updated: {goal} → {milestone} [{status}]")
    except Exception as e:
        print(f"Goal tracking error: {e}")

def get_active_goals(session_id: str) -> str:
    """
    v6.0: Inject active/pending goals into system prompt so Sophia
    continues the user's journey across sessions.
    """
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
    """
    v6.0: Automatically infer a long-horizon goal from intent and save it.
    Maps intents to multi-step goal journeys.
    """
    goal_map = {
        "supplier_verification": ("Verify Chinese supplier", "Complete SAMR check + risk report"),
        "supplier_search": ("Source verified Chinese supplier", "Identify candidates + CWC matching"),
        "consultation_request": ("Book CWC consultation", "Connect with Michail Digkas"),
        "high_intent_lead": ("Engage CWC for business advisory", "Define scope + receive proposal"),
    }
    if intent in goal_map:
        goal, milestone = goal_map[intent]
        # Only create if not already tracked
        profile = get_or_create_user_profile(session_id)
        task_history = profile.get('task_history', []) or []
        existing_goals = [t.get("goal") for t in task_history]
        if goal not in existing_goals:
            update_goal_state(session_id, goal, milestone, "in_progress")

# ============================================================
# v6.0: AUTONOMOUS BACKGROUND TASK SYSTEM
# ============================================================
async def run_autonomous_task(task_id: int, session_id: str, task_description: str):
    """
    v6.0: Execute a queued autonomous task without waiting for user input.
    This is the core of true agentic autonomy — Sophia acts on the world
    independently and notifies via email when done.
    """
    if not GROQ_API_KEY:
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
        # Parse task to determine what actions to take
        task_lower = task_description.lower()
        
        if any(kw in task_lower for kw in ["watch", "monitor", "track"]):
            # Company monitoring task
            company_match = re.search(r'(?:watch|monitor|track)\s+([A-Za-z\s]+?)(?:\s+for|\s+company|$)', task_description, re.IGNORECASE)
            company_name = company_match.group(1).strip() if company_match else ""
            
            if company_name:
                lookup = lookup_chinese_company(company_name)
                risk_content, _ = search_web(f"{company_name} China news scam fraud complaints 2025 2026")
                
                status_changed = lookup.get("warning") is not None
                result = (
                    f"🤖 AUTONOMOUS MONITORING REPORT\n"
                    f"Company: {company_name}\n"
                    f"Status: {lookup['registration_status']}\n"
                    f"{'⚠️ WARNING: ' + lookup['warning'] if lookup.get('warning') else '✅ No red flags detected'}\n"
                    f"Recent signals: {risk_content[:300] if risk_content else 'None found'}\n"
                    f"Checked: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
                )
                
                if status_changed:
                    # Notify Michail immediately if risk detected
                    send_email_brevo(
                        RECIPIENT_EMAIL,
                        f"🚨 Sophia Alert: Risk detected for {company_name}",
                        f"Autonomous monitoring detected a change:\n\n{result}\n\nSession: {session_id}"
                    )
                    
        elif any(kw in task_lower for kw in ["find supplier", "source", "research supplier"]):
            # Supplier research task
            product_match = re.search(r'(?:find supplier|source|research supplier)\s+(?:for\s+)?([A-Za-z\s]+?)(?:\s+in|\s+from|$)', task_description, re.IGNORECASE)
            product = product_match.group(1).strip() if product_match else task_description[20:50]
            
            supplier_data = search_suppliers(product)
            result = (
                f"🤖 AUTONOMOUS SUPPLIER RESEARCH\n"
                f"Product: {product}\n"
                f"Market Context: {supplier_data.get('market_context', 'N/A')}\n"
                f"MOQ: {supplier_data.get('typical_moq', 'N/A')}\n"
                f"Price Range: {supplier_data.get('price_range', 'N/A')}\n"
                f"Key Considerations: {', '.join(supplier_data.get('key_considerations', []))}\n"
                f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            )
            
        elif any(kw in task_lower for kw in ["news", "update", "latest"]):
            # Market intelligence gathering
            topic_match = re.search(r'(?:news|update|latest)\s+(?:about|on)?\s*(.+?)(?:\s+for|$)', task_description, re.IGNORECASE)
            topic = topic_match.group(1).strip() if topic_match else "China business"
            
            content, sources = search_web(f"{topic} latest news 2026")
            result = (
                f"🤖 AUTONOMOUS MARKET INTELLIGENCE\n"
                f"Topic: {topic}\n"
                f"Summary: {content[:600] if content else 'No recent updates found'}\n"
                f"Sources: {', '.join(sources[:3])}\n"
                f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            )
            
        else:
            # Generic task — use Groq to determine approach
            if GROQ_API_KEY:
                try:
                    res = requests.post(
                        "https://api.groq.com/openai/v1/chat/completions",
                        headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
                        json={
                            "model": "llama-3.3-70b-versatile",
                            "messages": [
                                {"role": "system", "content": "You are Sophia, CWC's AI agent. Complete this autonomous research task and return a clear, factual report. Be concise and actionable."},
                                {"role": "user", "content": f"Task: {task_description}\nComplete this task and provide a structured report."}
                            ],
                            "temperature": 0.2,
                            "max_tokens": 600
                        },
                        timeout=25
                    )
                    result = res.json()["choices"][0]["message"]["content"]
                except Exception as e:
                    result = f"Task processing error: {str(e)}"

        # Save result and mark complete
        conn = get_db()
        c = conn.cursor()
        c.execute(
            "UPDATE agent_tasks SET status='completed', result=%s, completed_at=%s WHERE id=%s",
            (result, datetime.now(), task_id)
        )
        conn.commit()
        conn.close()
        
        # Notify user via email if we have their email
        try:
            profile = get_or_create_user_profile(session_id)
            user_email = profile.get("email")
            if user_email and result:
                send_email_brevo(
                    user_email,
                    f"✅ Sophia completed your task: {task_description[:50]}",
                    f"Hello {profile.get('name') or 'there'},\n\nSophia has completed your requested task:\n\n{result}\n\nFor deeper analysis, visit: https://www.chinawestconnector.com\n\nBest regards,\nSophia — CWC AI Advisor"
                )
        except Exception as e:
            print(f"Notification email error: {e}")

        # Also always notify Michail of completed autonomous tasks
        send_email_brevo(
            RECIPIENT_EMAIL,
            f"🤖 Sophia Autonomous Task Complete: {task_description[:40]}",
            f"Session: {session_id}\nTask: {task_description}\n\nResult:\n{result}"
        )
        
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

async def poll_autonomous_tasks():
    """
    v6.0: Background loop that polls for pending autonomous tasks every 5 minutes.
    Runs as an asyncio task within FastAPI's lifespan — zero new infrastructure needed.
    """
    while True:
        try:
            conn = get_db()
            c = conn.cursor()
            # Find tasks pending for > 30 seconds (newly queued)
            c.execute("""
                SELECT id, session_id, task_description 
                FROM agent_tasks 
                WHERE status='pending' 
                AND created_at < NOW() - INTERVAL '30 seconds'
                ORDER BY created_at ASC
                LIMIT 3
            """)
            tasks = c.fetchall()
            conn.close()
            
            for task_id, session_id, task_desc in tasks:
                # Run each task concurrently
                asyncio.create_task(run_autonomous_task(task_id, session_id, task_desc))
                
        except Exception as e:
            print(f"Task poller error: {e}")
        
        # Poll every 5 minutes
        await asyncio.sleep(300)

async def run_proactive_followup():
    """
    v6.0: Proactive outreach — notify Michail of hot leads gone cold.
    Runs daily. No new paid services needed — uses existing Brevo.
    """
    while True:
        try:
            conn = get_db()
            c = conn.cursor()
            # Hot leads (score >= 70) who visited 2-5 days ago and haven't returned
            c.execute("""
                SELECT up.session_id, up.email, up.name, up.company, up.lead_score, 
                       up.last_intent, up.region_interest, up.last_seen
                FROM user_profiles up
                WHERE up.lead_score >= 70
                AND up.last_seen < NOW() - INTERVAL '2 days'
                AND up.last_seen > NOW() - INTERVAL '5 days'
                AND up.email IS NOT NULL
                ORDER BY up.lead_score DESC
                LIMIT 10
            """)
            hot_leads = c.fetchall()
            conn.close()
            
            if hot_leads:
                lead_lines = []
                for lead in hot_leads:
                    _, email, name, company, score, intent, region, last_seen = lead
                    days_ago = (datetime.now() - last_seen).days if last_seen else "?"
                    lead_lines.append(
                        f"• {name or 'Unknown'} ({email}) | {company or '?'} | Score: {score}/100 | "
                        f"Intent: {intent or '?'} | Region: {region or '?'} | Last seen: {days_ago}d ago"
                    )
                
                send_email_brevo(
                    RECIPIENT_EMAIL,
                    f"🔥 Sophia Alert: {len(hot_leads)} Hot Lead(s) Need Follow-Up",
                    f"""━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🔥 HOT LEADS — FOLLOW UP NOW
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
These high-scoring leads visited recently but haven't returned:

{chr(10).join(lead_lines)}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Dashboard: https://cwc-ai-backend.onrender.com/analytics?password={ADMIN_PASSWORD}
Leads:     https://cwc-ai-backend.onrender.com/leads?password={ADMIN_PASSWORD}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"""
                )
                print(f"📧 Proactive followup: notified about {len(hot_leads)} hot leads")
                
        except Exception as e:
            print(f"Proactive followup error: {e}")
        
        # Run once every 24 hours
        await asyncio.sleep(86400)

# ============================================================
# SCHEDULER
# ============================================================
scheduler_running = False

async def schedule_weekly_report():
    global scheduler_running
    scheduler_running = True
    while scheduler_running:
        now = datetime.now()
        days_until_monday = (7 - now.weekday()) % 7
        if days_until_monday == 0 and now.hour >= 9:
            days_until_monday = 7
        next_monday = now + timedelta(days=days_until_monday)
        next_monday = next_monday.replace(hour=9, minute=0, second=0, microsecond=0)
        seconds_until = (next_monday - now).total_seconds()
        await asyncio.sleep(seconds_until)
        try:
            send_weekly_report()
        except Exception as e:
            print(f"Weekly report error: {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start all background async tasks
    asyncio.create_task(schedule_weekly_report())
    asyncio.create_task(poll_autonomous_tasks())    # v6.0: autonomous task poller
    asyncio.create_task(run_proactive_followup())   # v6.0: proactive lead followup
    yield
    global scheduler_running
    scheduler_running = False

app = FastAPI(
    lifespan=lifespan,
    title="CWC Sophia AI — China-West Business Intelligence",
    description="Sophia is CWC's agentic AI advisor for China-West cross-border business. v6.0 with autonomous task execution, goal tracking, iterative reflection, conditional re-planning, and proactive outreach.",
    version="6.0.0",
)

# ============================================================
# CORS
# ============================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://www.chinawestconnector.com",
        "https://chinawestconnector.com",
        "http://localhost:8000",
        "http://localhost:3000",
        "https://localhost",
    ],
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

    c.execute('''CREATE TABLE IF NOT EXISTS conversations
                 (id SERIAL PRIMARY KEY, session_id TEXT, user_message TEXT,
                  ai_response TEXT, timestamp TIMESTAMP, email TEXT, company TEXT,
                  region TEXT, intent TEXT, reflection_score INTEGER DEFAULT 0)''')

    c.execute('''CREATE TABLE IF NOT EXISTS leads
                 (id SERIAL PRIMARY KEY, name TEXT, email TEXT, company TEXT,
                  region TEXT, session_id TEXT, source TEXT, timestamp TEXT, status TEXT)''')

    c.execute('''CREATE TABLE IF NOT EXISTS user_profiles
                 (id SERIAL PRIMARY KEY, session_id TEXT UNIQUE, first_seen TIMESTAMP,
                  last_seen TIMESTAMP, visit_count INTEGER DEFAULT 1, name TEXT, email TEXT,
                  company TEXT, region_interest TEXT, topics_discussed TEXT,
                  lead_score INTEGER DEFAULT 0, last_intent TEXT, language TEXT DEFAULT 'en',
                  conversation_summary TEXT, key_facts JSONB DEFAULT '{}',
                  task_history JSONB DEFAULT '[]')''')

    c.execute('''CREATE TABLE IF NOT EXISTS response_cache
                 (cache_key TEXT PRIMARY KEY, response TEXT, sources TEXT, created_at TIMESTAMP)''')

    c.execute('''CREATE TABLE IF NOT EXISTS agent_tasks
                 (id SERIAL PRIMARY KEY, session_id TEXT, task_description TEXT,
                  sub_tasks JSONB DEFAULT '[]', status TEXT DEFAULT 'pending',
                  result TEXT, created_at TIMESTAMP, completed_at TIMESTAMP)''')

    c.execute('''CREATE TABLE IF NOT EXISTS supplier_searches
                 (id SERIAL PRIMARY KEY, session_id TEXT, query TEXT,
                  results JSONB DEFAULT '[]', created_at TIMESTAMP)''')

    c.execute('''CREATE TABLE IF NOT EXISTS learned_patterns (
        id SERIAL PRIMARY KEY,
        pattern_type TEXT,
        trigger_condition TEXT,
        action_recommendation TEXT,
        context_type TEXT,
        success_count INTEGER DEFAULT 0,
        failure_count INTEGER DEFAULT 0,
        avg_reflection_score REAL DEFAULT 0,
        last_used TIMESTAMP,
        created_at TIMESTAMP DEFAULT NOW()
    )''')

    migrations = [
        ("user_profiles",  "key_facts",        "JSONB DEFAULT '{}'"),
        ("user_profiles",  "task_history",      "JSONB DEFAULT '[]'"),
        ("conversations",  "reflection_score",  "INTEGER DEFAULT 0"),
    ]
    for table, col, definition in migrations:
        try:
            c.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col} {definition}")
        except Exception:
            conn.rollback()

    conn.commit()
    conn.close()

init_db()

# ============================================================
# PYDANTIC MODELS
# ============================================================
class ChatRequest(BaseModel):
    message: str
    session_id: str = "anonymous"
    deep_search: bool = False

class LeadCapture(BaseModel):
    name: str
    email: str
    company: str = ""
    region: str = ""
    session_id: str = ""
    source: str = "chat_widget"
    timestamp: str = ""

class QuickActionRequest(BaseModel):
    action: str
    session_id: str = "anonymous"

class SupplierSearchRequest(BaseModel):
    query: str
    sector: str = ""
    region: str = ""
    session_id: str = "anonymous"

# v6.0: New model for queuing autonomous tasks
class AutonomousTaskRequest(BaseModel):
    task_description: str
    session_id: str = "anonymous"

# ============================================================
# LANGUAGE DETECTION
# ============================================================
def detect_language(text: str) -> str:
    if not text: return "en"
    chinese_chars  = len(re.findall(r'[\u4e00-\u9fff\u3400-\u4dbf]', text))
    arabic_chars   = len(re.findall(r'[\u0600-\u06ff]', text))
    cyrillic_chars = len(re.findall(r'[\u0400-\u04ff]', text))
    total = max(len(text), 1)
    if chinese_chars  / total > 0.15: return "zh"
    if arabic_chars   / total > 0.15: return "ar"
    if cyrillic_chars / total > 0.15: return "ru"
    words = set(text.lower().split())
    if len(words & {"que","como","para","con","una","por","del","los"}) >= 2: return "es"
    if len(words & {"que","les","des","est","pour","dans","avec","vous"}) >= 2: return "fr"
    if len(words & {"und","die","der","das","ist","ich","mit","ein"}) >= 2:    return "de"
    return "en"

LANGUAGE_INSTRUCTIONS = {
    "zh": "用中文回复。这是高优先级中国企业客户。",
    "ar": "الرجاء الرد باللغة العربية.",
    "es": "Por favor responde en español.",
    "fr": "Veuillez répondre en français.",
    "de": "Bitte antworte auf Deutsch.",
    "ru": "Пожалуйста, отвечайте на русском языке.",
    "en": "",
}

# ============================================================
# USER PROFILE FUNCTIONS
# ============================================================
def get_or_create_user_profile(session_id: str, new_session: bool = False) -> dict:
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT * FROM user_profiles WHERE session_id = %s", (session_id,))
    profile = c.fetchone()
    if profile:
        if new_session:
            c.execute("UPDATE user_profiles SET last_seen=%s, visit_count=visit_count+1 WHERE session_id=%s",
                      (datetime.now(), session_id))
        else:
            c.execute("UPDATE user_profiles SET last_seen=%s WHERE session_id=%s",
                      (datetime.now(), session_id))
        conn.commit()
        raw_kf = profile[14] if len(profile) > 14 else {}
        raw_th = profile[15] if len(profile) > 15 else []
        user_profile = {
            "session_id": profile[1], "first_seen": profile[2],
            "last_seen": profile[3], "visit_count": profile[4] + (1 if new_session else 0),
            "name": profile[5], "email": profile[6], "company": profile[7],
            "region_interest": profile[8], "topics_discussed": profile[9],
            "lead_score": profile[10], "last_intent": profile[11],
            "language": profile[12] if len(profile) > 12 else "en",
            "conversation_summary": profile[13] if len(profile) > 13 else None,
            "key_facts":    raw_kf if isinstance(raw_kf, dict) else {},
            "task_history": raw_th if isinstance(raw_th, list) else [],
            "is_returning": True
        }
    else:
        c.execute("INSERT INTO user_profiles (session_id, first_seen, last_seen, visit_count, language, key_facts, task_history) VALUES (%s,%s,%s,1,'en','{}','[]')",
                  (session_id, datetime.now(), datetime.now()))
        conn.commit()
        user_profile = {
            "session_id": session_id, "first_seen": datetime.now(),
            "last_seen": datetime.now(), "visit_count": 1,
            "name": None, "email": None, "company": None, "region_interest": None,
            "topics_discussed": None, "lead_score": 0, "last_intent": None,
            "language": "en", "conversation_summary": None,
            "key_facts": {}, "task_history": [], "is_returning": False
        }
    conn.close()
    return user_profile


def update_user_profile(session_id: str, **kwargs):
    conn = get_db()
    c = conn.cursor()
    valid = ['name','email','company','region_interest','topics_discussed','lead_score',
             'last_intent','language','conversation_summary','key_facts','task_history']
    updates, values = [], []
    for key, value in kwargs.items():
        if key in valid and value is not None:
            if key in ('key_facts','task_history'):
                updates.append(f"{key} = %s::jsonb")
                values.append(json.dumps(value))
            else:
                updates.append(f"{key} = %s")
                values.append(value)
    if updates:
        values.append(session_id)
        c.execute(f"UPDATE user_profiles SET {', '.join(updates)} WHERE session_id = %s", values)
        conn.commit()
    conn.close()


def calculate_lead_score(user_profile: dict, message: str, intent: str) -> int:
    score = user_profile.get('lead_score', 0)
    score += {"high_intent_lead": 30, "consultation_request": 25,
              "supplier_verification": 20, "supplier_search": 15,
              "information_gathering": 5}.get(intent, 0)
    if user_profile.get('visit_count', 1) > 1: score += 10
    if any(kw in message.lower() for kw in ["budget","invest","contract","serious","start","hire","price"]): score += 15
    lang = user_profile.get('language','en')
    if lang == 'zh': score += 20
    elif lang != 'en': score += 10
    return min(score, 100)

# ============================================================
# DATABASE FUNCTIONS
# ============================================================
def save_conversation(session_id, user_msg, ai_response,
                      email=None, company=None, region=None, intent=None, reflection_score=0):
    conn = get_db()
    c = conn.cursor()
    c.execute("INSERT INTO conversations (session_id,user_message,ai_response,timestamp,email,company,region,intent,reflection_score) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
              (session_id, user_msg, ai_response, datetime.now(), email, company, region, intent, reflection_score))
    conn.commit()
    conn.close()


def get_conversation_history(session_id, limit=10):
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT user_message, ai_response FROM conversations WHERE session_id=%s ORDER BY timestamp DESC LIMIT %s",
              (session_id, limit))
    history = c.fetchall()
    conn.close()
    return history[::-1]


def get_message_count(session_id: str) -> int:
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM conversations WHERE session_id=%s", (session_id,))
    count = c.fetchone()[0]
    conn.close()
    return count

# ============================================================
# RESPONSE CACHE
# ============================================================
def get_cached_response(query: str):
    cache_key = hashlib.md5(query.strip().lower().encode()).hexdigest()
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT response,sources,created_at FROM response_cache WHERE cache_key=%s", (cache_key,))
    row = c.fetchone()
    conn.close()
    if row:
        created = row[2] if isinstance(row[2], datetime) else datetime.fromisoformat(str(row[2]))
        if datetime.now() - created < timedelta(hours=24):
            return row[0], (json.loads(row[1]) if row[1] else [])
    return None


def set_cached_response(query: str, response: str, sources: list):
    cache_key = hashlib.md5(query.strip().lower().encode()).hexdigest()
    conn = get_db()
    c = conn.cursor()
    c.execute("INSERT INTO response_cache (cache_key,response,sources,created_at) VALUES (%s,%s,%s,%s) ON CONFLICT (cache_key) DO UPDATE SET response=EXCLUDED.response,sources=EXCLUDED.sources,created_at=EXCLUDED.created_at",
              (cache_key, response, json.dumps(sources), datetime.now()))
    conn.commit()
    conn.close()


CACHEABLE_PATTERNS = ["hainan free trade","samr","wfoe","vat","fdi rules","what is cwc",
                      "what is china west","belt and road","how to register","free trade zone",
                      "import duties","nmpa","ce certification","iso certification"]

def is_cacheable(query: str) -> bool:
    return any(p in query.lower() for p in CACHEABLE_PATTERNS)

# ============================================================
# INTENT DETECTION
# ============================================================
def detect_intent(message: str) -> dict:
    msg = message.lower()
    detected = {"primary": "general", "region": None, "score": 0}
    if any(kw in msg for kw in ["price","cost","quote","proposal","start","begin","hire","contract","serious","budget","invest"]):
        detected.update({"primary": "high_intent_lead", "score": 90})
    elif any(kw in msg for kw in ["book","consultation","call","schedule","meet","contact","talk","discuss"]):
        detected.update({"primary": "consultation_request", "score": 85})
    elif any(kw in msg for kw in ["verify","check","audit","due diligence","factory","supplier","manufacturer"]):
        detected.update({"primary": "supplier_verification", "score": 80})
    elif any(kw in msg for kw in ["find supplier","find manufacturer","source","sourcing","who makes","find factory","best supplier"]):
        detected.update({"primary": "supplier_search", "score": 75})
    elif any(kw in msg for kw in ["how","what","tell me","explain","information"]):
        detected.update({"primary": "information_gathering", "score": 40})
    regions = {
        "africa":       ["africa","african","mining","infrastructure"],
        "middle_east":  ["middle east","mea","gcc","dubai","saudi","energy","oil","gas"],
        "latam":        ["latam","latin america","brazil","mexico","argentina","chile","lithium"],
        "europe":       ["europe","eu","germany","france","green tech","automotive"],
        "central_asia": ["central asia","kazakhstan","uzbekistan","belt and road","bri"],
        "china":        ["china","chinese","mainland","prc","shenzhen","shanghai","beijing","guangzhou"]
    }
    for region, keywords in regions.items():
        if any(kw in msg for kw in keywords):
            detected["region"] = region
            break
    return detected

# ============================================================
# SEARCH FUNCTIONS
# ============================================================
def search_duckduckgo(query: str) -> tuple:
    try:
        res = requests.get("https://api.duckduckgo.com/",
                           params={"q":query,"format":"json","no_html":"1","skip_disambig":"1"}, timeout=8)
        data = res.json()
        abstract = data.get("AbstractText","")
        related  = [r.get("Text","") for r in data.get("RelatedTopics",[])[:3] if r.get("Text")]
        sources  = [data.get("AbstractURL","DuckDuckGo")] if abstract else []
        return (abstract + ("\n" + "\n".join(related) if related else "")).strip(), sources
    except Exception as e:
        print(f"DDG error: {e}"); return "", []

def search_wikipedia(query: str) -> tuple:
    try:
        import urllib.parse
        res = requests.get(f"https://en.wikipedia.org/api/rest_v1/page/summary/{urllib.parse.quote(query.replace(' ','_'))}", timeout=8)
        if res.status_code == 200:
            data = res.json()
            extract = data.get("extract","")
            url = data.get("content_urls",{}).get("desktop",{}).get("page","Wikipedia")
            if extract: return extract[:600], [url]
        return "", []
    except Exception as e:
        print(f"Wiki error: {e}"); return "", []

def search_tavily(query: str) -> tuple:
    if not TAVILY_API_KEY: return "", []
    year = datetime.now().year
    is_news = any(kw in query.lower() for kw in ["news","latest","update","today","recent"])
    enhanced = f"{query} China business trade {year} latest news" if is_news else f"{query} China business {year}"
    try:
        res = requests.post("https://api.tavily.com/search",
                            json={"api_key":TAVILY_API_KEY,"query":enhanced,"max_results":3,"search_depth":"advanced","include_answer":True}, timeout=10)
        res.raise_for_status()
        data = res.json()
        answer = data.get("answer","")
        results = data.get("results",[])
        sources = [r.get("url","") for r in results if r.get("url")]
        return (answer + "\n" + "\n".join(r.get("content","") for r in results[:2])).strip(), sources
    except Exception as e:
        print(f"Tavily error: {e}"); return "", []

def search_web(query: str) -> tuple:
    all_content, all_sources = [], []
    tv, ts = search_tavily(query)
    if tv: all_content.append(tv); all_sources.extend(ts)
    if not tv:
        dq, ds = search_duckduckgo(query)
        if dq: all_content.append(dq); all_sources.extend(ds)
    wq, ws = search_wikipedia(query)
    if wq and len("\n".join(all_content)) < 400:
        all_content.append(f"Background: {wq}"); all_sources.extend(ws)
    return "\n\n".join(all_content), list(dict.fromkeys(s for s in all_sources if s))[:4]

# ============================================================
# NEWS — Tavily-powered, DB-persisted (survives Render cold starts)
# ============================================================
# NOTE: We do NOT use an in-memory cache here. Render's free tier spins the
# server down after ~15 min inactivity, so in-memory state is lost on every
# cold start. Instead we persist news in the response_cache DB table with a
# dedicated key, giving us fresh news that survives restarts.
# RSS feeds from SCMP/Caixin are often blocked on Render's outbound network,
# so Tavily is the primary source — it's already wired up and working.

NEWS_CACHE_KEY = "__sophia_china_news_v6__"
NEWS_TTL_HOURS = 3  # refresh every 3 hours

def _categorise_news_item(title: str) -> str:
    t = title.lower()
    if any(w in t for w in ["trade","tariff","export","import","wto","sanction"]): return "Trade"
    if any(w in t for w in ["invest","fdi","fund","deal","acquisition","merger"]): return "Investment"
    if any(w in t for w in ["policy","regulat","law","rule","government","ministry"]): return "Policy"
    if any(w in t for w in ["tech","ai","robot","digital","semiconductor","chip"]): return "Technology"
    if any(w in t for w in ["energy","solar","ev","battery","green","hydrogen","wind"]): return "Energy"
    if any(w in t for w in ["pharma","biotech","health","medical","drug","vaccine"]): return "Biotech"
    if any(w in t for w in ["ship","freight","logistics","port","cargo","supply chain"]): return "Logistics"
    return "China Business"

def _fetch_news_from_tavily() -> list:
    """Fetch China business news via Tavily search — reliable on Render."""
    if not TAVILY_API_KEY:
        return []

    # Run 3 targeted searches to get diverse, fresh news
    queries = [
        "China business trade investment news today 2026",
        "China economy policy FDI market entry news this week",
        "China supply chain logistics technology news latest",
    ]
    items = []
    seen_titles = set()

    for query in queries:
        try:
            res = requests.post(
                "https://api.tavily.com/search",
                json={
                    "api_key": TAVILY_API_KEY,
                    "query": query,
                    "max_results": 5,
                    "search_depth": "basic",
                    "include_answer": False,
                    "topic": "news",
                },
                timeout=10
            )
            if res.status_code != 200:
                continue
            results = res.json().get("results", [])
            for r in results:
                title = r.get("title", "").strip()
                url   = r.get("url", "")
                date  = r.get("published_date", datetime.now().strftime("%Y-%m-%d"))
                if not title or len(title) < 10:
                    continue
                # Deduplicate
                title_key = title.lower()[:60]
                if title_key in seen_titles:
                    continue
                seen_titles.add(title_key)
                # Filter noise
                if any(kw in title.lower() for kw in ["ukraine","russia","epstein","nato","israel","gaza","sport","football","cricket"]):
                    continue
                items.append({
                    "title": title,
                    "url": url,
                    "category": _categorise_news_item(title),
                    "date": str(date)[:10] if date else datetime.now().strftime("%Y-%m-%d"),
                })
            if len(items) >= 12:
                break
        except Exception as e:
            print(f"Tavily news error ({query[:30]}): {e}")

    # Sort by date descending, keep top 8
    try:
        items.sort(key=lambda x: x.get("date", ""), reverse=True)
    except Exception:
        pass
    return items[:8]

def _fetch_news_from_rss_fallback() -> list:
    """Secondary fallback: try RSS feeds. May fail on Render — that's OK."""
    feeds = [
        "https://www.chinadaily.com.cn/rss/bizchina_rss.xml",
        "https://www.xinhuanet.com/english/rss/financerss.xml",
        "https://rss.app/feeds/china-business.xml",
    ]
    items = []
    for feed_url in feeds:
        try:
            res = requests.get(feed_url, timeout=6, headers={"User-Agent": "Mozilla/5.0 CWC-Sophia/6.0"})
            if res.status_code != 200:
                continue
            for block in re.findall(r'<item[^>]*>(.*?)</item>', res.text, re.DOTALL)[:5]:
                tm = re.search(r'<title><!\[CDATA\[(.*?)\]\]></title>|<title>(.*?)</title>', block, re.DOTALL)
                if not tm: continue
                title = re.sub(r'<[^>]+>', '', (tm.group(1) or tm.group(2) or "")).strip()
                if not title or len(title) < 10: continue
                lm = re.search(r'<link>(https?://[^<]+)</link>', block) or re.search(r'<guid[^>]*>(https?://[^<]+)</guid>', block)
                link = lm.group(1).strip() if lm else feed_url
                dm = re.search(r'<pubDate>(.*?)</pubDate>', block)
                date = dm.group(1).strip()[:10] if dm else datetime.now().strftime("%Y-%m-%d")
                if any(kw in title.lower() for kw in ["ukraine","russia","epstein","nato","israel","gaza"]): continue
                items.append({"title": title, "url": link, "category": _categorise_news_item(title), "date": date})
            if len(items) >= 8: break
        except Exception as e:
            print(f"RSS fallback error ({feed_url}): {e}")
    return items[:8]

def fetch_china_news() -> list:
    """
    Fetch China business news.
    Strategy:
      1. Check DB cache — if < NEWS_TTL_HOURS old, return it (survives cold starts)
      2. Try Tavily (primary — reliable on Render)
      3. Try RSS feeds (secondary fallback)
      4. Return last known DB items if all else fails (never show static hardcoded list)
    """
    now = datetime.now()

    # ── Step 1: Check DB cache ────────────────────────────────────────────────
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT response, created_at FROM response_cache WHERE cache_key=%s", (NEWS_CACHE_KEY,))
        row = c.fetchone()
        conn.close()
        if row:
            created = row[1] if isinstance(row[1], datetime) else datetime.fromisoformat(str(row[1]))
            age_hours = (now - created).total_seconds() / 3600
            cached_items = json.loads(row[0]) if row[0] else []
            if age_hours < NEWS_TTL_HOURS and cached_items:
                print(f"📰 News: serving from DB cache ({age_hours:.1f}h old, {len(cached_items)} items)")
                return cached_items
            # Cache exists but stale — remember stale items as final fallback
            stale_items = cached_items
        else:
            stale_items = []
    except Exception as e:
        print(f"News DB cache read error: {e}")
        stale_items = []

    # ── Step 2: Fetch fresh news via Tavily ───────────────────────────────────
    fresh_items = []
    if TAVILY_API_KEY:
        fresh_items = _fetch_news_from_tavily()
        if fresh_items:
            print(f"📰 News: fetched {len(fresh_items)} items from Tavily")

    # ── Step 3: RSS fallback if Tavily got nothing ────────────────────────────
    if not fresh_items:
        fresh_items = _fetch_news_from_rss_fallback()
        if fresh_items:
            print(f"📰 News: fetched {len(fresh_items)} items from RSS fallback")

    # ── Step 4: Persist fresh items to DB (so cold starts use them) ──────────
    if fresh_items:
        try:
            conn = get_db()
            c = conn.cursor()
            c.execute(
                "INSERT INTO response_cache (cache_key, response, sources, created_at) VALUES (%s, %s, %s, %s) "
                "ON CONFLICT (cache_key) DO UPDATE SET response=EXCLUDED.response, created_at=EXCLUDED.created_at",
                (NEWS_CACHE_KEY, json.dumps(fresh_items), "[]", now)
            )
            conn.commit()
            conn.close()
            print(f"📰 News: persisted {len(fresh_items)} items to DB cache")
        except Exception as e:
            print(f"News DB cache write error: {e}")
        return fresh_items

    # ── Step 5: Return stale DB items if we couldn't refresh ─────────────────
    if stale_items:
        print(f"📰 News: returning {len(stale_items)} stale items (refresh failed)")
        return stale_items

    # ── Step 6: Absolute last resort — minimal placeholder (should rarely hit) 
    print("📰 News: all sources failed, returning placeholder")
    return [
        {"title": f"China business intelligence unavailable — check back shortly", "url": "", "category": "China Business", "date": now.strftime("%Y-%m-%d")},
    ]

# ============================================================
# COMPANY LOOKUP
# ============================================================
def lookup_chinese_company(company_name: str) -> dict:
    result = {"company":company_name,"found":False,"registration_status":"Unknown","details":"","sources":[],"warning":None}
    query = f"{company_name} China company registration SAMR business license"
    ddg, ddg_s = search_duckduckgo(query)
    tav, tav_s = search_tavily(query)
    combined = (tav or ddg or "").lower()
    if combined:
        result["found"] = True
        result["sources"] = (tav_s + ddg_s)[:3]
        flags = [f for f in ["scam","fraud","fake","blacklist","warning","complaint","dispute","lawsuit","suspended","revoked"] if f in combined]
        if flags:
            result["warning"] = f"⚠️ Red flags: {', '.join(flags)}"
            result["registration_status"] = "Requires Investigation"
        else:
            result["registration_status"] = "Preliminary search complete — full audit recommended"
        result["details"] = (tav or ddg)[:400]
    else:
        result["details"] = "No public data found. Full CWC Due Diligence strongly recommended."
        result["warning"] = "⚠️ No public data found — treat with caution"
    return result

# ============================================================
# v5: ACCIO-STYLE SUPPLIER SEARCH ENGINE
# ============================================================
def search_suppliers(product_or_sector: str, region: str = "") -> dict:
    year = datetime.now().year
    queries = [
        f"China {product_or_sector} manufacturer exporter verified {year}",
        f"{product_or_sector} Chinese supplier factory MOQ price certification",
    ]
    if region: queries.append(f"China {product_or_sector} export to {region}")
    all_raw, all_sources = [], []
    for q in queries[:2]:
        content, sources = search_web(q)
        if content:
            all_raw.append(content[:600]); all_sources.extend(sources)
    if not GROQ_API_KEY or not all_raw:
        return {"market_context":"","sources":[]}
    raw_text = "\n\n".join(all_raw)
    try:
        res = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization":f"Bearer {GROQ_API_KEY}","Content-Type":"application/json"},
            json={
                "model":"llama-3.3-70b-versatile",
                "messages":[
                    {"role":"system","content":(
                        "You are a China B2B sourcing analyst. Extract structured supplier intelligence from raw web data. "
                        "Return ONLY valid JSON: {\"market_context\":\"2-3 sentence overview\","
                        "\"key_considerations\":[\"3-4 important factors\"],"
                        "\"typical_moq\":\"MOQ range\",\"price_range\":\"price range if available\","
                        "\"certifications_required\":[\"list\"],\"top_regions\":[\"Chinese manufacturing regions\"],"
                        "\"red_flags\":[\"common fraud/quality risks\"],"
                        "\"cwc_recommendation\":\"one sentence how CWC can specifically help\"}"
                        " Return ONLY JSON."
                    )},
                    {"role":"user","content":f"Product/sector: {product_or_sector}\nRegion: {region or 'Global'}\n\n{raw_text}"}
                ],
                "temperature":0.1,"max_tokens":600
            }, timeout=15
        )
        raw = res.json()["choices"][0]["message"]["content"].strip()
        raw = re.sub(r"^```json\s*|```$","",raw.strip(),flags=re.MULTILINE).strip()
        structured = json.loads(raw)
        structured["sources"] = list(dict.fromkeys(s for s in all_sources if s))[:4]
        return structured
    except Exception as e:
        print(f"Supplier structuring error: {e}")
        return {"market_context":raw_text[:300],"sources":all_sources[:3]}

# ============================================================
# v5: TASK DECOMPOSITION ENGINE
# ============================================================
def decompose_task(user_message: str, user_profile: dict) -> dict | None:
    complex_triggers = ["help me find","i want to source","find suppliers for","market entry plan",
                        "full due diligence","compare","analyse","research and recommend",
                        "step by step","comprehensive","full report","everything about"]
    if not any(t in user_message.lower() for t in complex_triggers): return None
    if not GROQ_API_KEY: return None
    key_facts = user_profile.get('key_facts',{})
    try:
        res = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization":f"Bearer {GROQ_API_KEY}","Content-Type":"application/json"},
            json={
                "model":"llama-3.3-70b-versatile",
                "messages":[
                    {"role":"system","content":(
                        "You are a China trade intelligence task planner. Break complex requests into 2-4 sequential sub-tasks. "
                        "Return ONLY JSON: {\"is_complex\":true,\"task_summary\":\"one line\","
                        "\"sub_tasks\":[{\"step\":1,\"action\":\"search_market|lookup_company|search_suppliers|generate_risk_report\","
                        "\"query\":\"...\",\"reason\":\"...\"}],"
                        "\"expected_output\":\"what final response should contain\"}"
                    )},
                    {"role":"user","content":f"Request: {user_message}\nContext: {json.dumps(key_facts)}"}
                ],
                "temperature":0.1,"max_tokens":400
            }, timeout=10
        )
        raw = res.json()["choices"][0]["message"]["content"].strip()
        raw = re.sub(r"^```json\s*|```$","",raw.strip(),flags=re.MULTILINE).strip()
        plan = json.loads(raw)
        if plan.get('is_complex') and plan.get('sub_tasks'):
            print(f"📋 Decomposed: {plan['task_summary']} ({len(plan['sub_tasks'])} steps)")
            return plan
    except Exception as e:
        print(f"Decomposition error: {e}")
    return None

# ============================================================
# GROQ TOOL DEFINITIONS — v6.0
# ============================================================
GROQ_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "search_market_intelligence",
            "description": "Search live China business intel, market data, trade news, regulatory updates.",
            "parameters": {"type":"object","properties":{
                "query":{"type":"string"},
                "search_type":{"type":"string","enum":["market_news","company_lookup","regulation","general"]}
            },"required":["query","search_type"]}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "lookup_company",
            "description": "Look up a Chinese company in public registries for verification and due diligence.",
            "parameters": {"type":"object","properties":{"company_name":{"type":"string"}},"required":["company_name"]}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "generate_risk_report",
            "description": "Generate a full structured due diligence risk report for a Chinese company. Use when user wants safety assessment before payment or contract.",
            "parameters": {"type":"object","properties":{
                "company_name":{"type":"string"},
                "context":{"type":"string","description":"What user plans to do with this company"}
            },"required":["company_name"]}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "find_suppliers",
            "description": (
                "Accio-style supplier discovery. Search for Chinese manufacturers/suppliers for a product or sector. "
                "Returns structured market intel: MOQ ranges, price ranges, required certifications, top manufacturing regions, "
                "common red flags. Use when user wants to source products from China or find manufacturing partners."
            ),
            "parameters": {"type":"object","properties":{
                "product_or_sector":{"type":"string","description":"Product or sector to source"},
                "destination_region":{"type":"string","description":"Buyer country/region (e.g. Europe, USA)"},
                "additional_requirements":{"type":"string","description":"Certifications, MOQ, etc."}
            },"required":["product_or_sector"]}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "reflect_and_improve",
            "description": (
                "Self-reflection tool. After drafting a response, use this to evaluate quality. "
                "Checks: (1) answers real need not just literal question, (2) uses specific facts not generalities, "
                "(3) advances the CWC sales conversation, (4) has clear next step. "
                "Returns score 1-10 and improvement instructions if needed. "
                "ALWAYS use this before finalising a response to a complex or high-value user query."
            ),
            "parameters": {"type":"object","properties":{
                "draft_response":{"type":"string"},
                "user_need":{"type":"string"}
            },"required":["draft_response","user_need"]}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "update_semantic_memory",
            "description": (
                "Store important facts about the user for future personalization. "
                "Call when user reveals: company name, budget, timeline, preferences, constraints, or decision criteria. "
                "This creates long-term memory across sessions."
            ),
            "parameters": {"type":"object","properties":{
                "fact_type":{"type":"string","enum":["company","budget","timeline","preference","constraint","goal","sector","urgency"]},
                "fact_value":{"type":"string"},
                "importance":{"type":"integer","description":"1-10, store if >= 7"}
            },"required":["fact_type","fact_value"]}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "queue_autonomous_task",
            "description": (
                "Queue a background task for Sophia to complete autonomously — without waiting for the user. "
                "Use when user says 'watch this company', 'monitor for me', 'check back on', 'research later', "
                "'keep an eye on', or any request implying deferred or continuous work. "
                "Sophia will complete the task in the background and email the results."
            ),
            "parameters": {"type":"object","properties":{
                "task_description":{"type":"string","description":"Clear description of what to do autonomously"},
                "task_type":{"type":"string","enum":["monitor_company","find_supplier","market_research","news_watch","general"]}
            },"required":["task_description","task_type"]}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "update_goal_progress",
            "description": (
                "Update the user's long-term goal progress. Call when a milestone is completed or a goal changes. "
                "This ensures Sophia continues the user's journey on future visits rather than starting from scratch."
            ),
            "parameters": {"type":"object","properties":{
                "goal":{"type":"string","description":"The overarching goal (e.g. 'Verify supplier XYZ')"},
                "milestone":{"type":"string","description":"Current milestone reached"},
                "status":{"type":"string","enum":["pending","in_progress","done","blocked"]}
            },"required":["goal","milestone","status"]}
        }
    }
]

# ============================================================
# TOOL EXECUTION HANDLER — v6.0
# ============================================================
def run_tool_call(tool_name: str, tool_args: dict, session_id: str = None, user_profile: dict = None) -> tuple:
    """Execute tool calls with full context awareness"""
    
    if tool_name == "search_market_intelligence":
        query = tool_args.get("query","")
        st = tool_args.get("search_type","general")
        if st == "market_news":     query = f"{query} latest news 2025 2026"
        elif st == "company_lookup": query = f"{query} China company profile registration"
        elif st == "regulation":    query = f"{query} China regulation compliance 2026"
        return search_web(query)

    elif tool_name == "lookup_company":
        r = lookup_chinese_company(tool_args.get("company_name",""))
        summary = f"Company: {r['company']}\nStatus: {r['registration_status']}\nDetails: {r['details']}\n"
        if r.get('warning'): summary += f"WARNING: {r['warning']}\n"
        return summary, r.get('sources',[])

    elif tool_name == "generate_risk_report":
        name    = tool_args.get("company_name","")
        context = tool_args.get("context","business engagement")
        lookup  = lookup_chinese_company(name)
        risk, rs = search_web(f"{name} China fraud scam complaints blacklist 2024 2025")
        news, ns = search_web(f"{name} China company news recent 2025")
        all_src = lookup.get('sources',[]) + rs + ns
        report = (
            f"=== CWC RISK REPORT: {name} ===\n"
            f"Context: {context}\n"
            f"REGISTRATION: {lookup['registration_status']}\n"
            f"Details: {lookup['details'][:300]}\n"
            f"{('⚠️ ' + lookup['warning']) if lookup.get('warning') else '✅ No registry red flags.'}\n"
            f"RISK SIGNALS: {risk[:300] if risk else 'None found in open sources.'}\n"
            f"RECENT NEWS: {news[:300] if news else 'No recent news found.'}\n"
            f"VERDICT: {'⚠️ ESCALATE — Risk signals detected. Full CWC audit required.' if lookup.get('warning') else '✅ No critical signals. Standard CWC verification recommended.'}"
        )
        return report, list(dict.fromkeys(s for s in all_src if s))[:5]

    elif tool_name == "find_suppliers":
        product = tool_args.get("product_or_sector","")
        region  = tool_args.get("destination_region","")
        reqs    = tool_args.get("additional_requirements","")
        result  = search_suppliers(product, region)
        lines = [f"=== SUPPLIER INTELLIGENCE: {product} ===",
                 f"Target: {region or 'Global'} | Requirements: {reqs or 'Not specified'}",
                 f"\nMARKET CONTEXT: {result.get('market_context','N/A')}",
                 "\nKEY CONSIDERATIONS:"]
        lines += ["• " + k for k in result.get('key_considerations',[])]
        lines += [f"\nTYPICAL MOQ: {result.get('typical_moq','Varies')}",
                  f"PRICE RANGE: {result.get('price_range','Request quotes')}",
                  "\nCERTIFICATIONS REQUIRED:"]
        lines += ["• " + c for c in result.get('certifications_required',[])]
        lines += ["\nTOP MANUFACTURING REGIONS:"]
        lines += ["• " + r for r in result.get('top_regions',[])]
        lines += ["\nRED FLAGS:"]
        lines += ["⚠️ " + f for f in result.get('red_flags',[])]
        lines.append(f"\nCWC: {result.get('cwc_recommendation','Contact CWC for verified supplier matching.')}")
        return "\n".join(lines), result.get('sources',[])

    elif tool_name == "reflect_and_improve":
        draft = tool_args.get("draft_response","")
        need  = tool_args.get("user_need","")
        if not GROQ_API_KEY: return "Reflection unavailable.", []
        try:
            res = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization":f"Bearer {GROQ_API_KEY}","Content-Type":"application/json"},
                json={
                    "model":"llama-3.3-70b-versatile",
                    "messages":[
                        {"role":"system","content":(
                            "Senior CWC quality reviewer. Evaluate this draft against user need. Be harsh. "
                            "Return ONLY JSON: {\"score\":1-10,\"passes\":true/false,"
                            "\"issues\":[\"problems\"],\"improvement_instructions\":\"rewrite guidance or empty\"}"
                        )},
                        {"role":"user","content":f"User need: {need}\n\nDraft:\n{draft}"}
                    ],
                    "temperature":0.0,"max_tokens":300
                }, timeout=10
            )
            raw = re.sub(r"^```json\s*|```$","",res.json()["choices"][0]["message"]["content"].strip(),flags=re.MULTILINE).strip()
            ev = json.loads(raw)
            score = ev.get('score',5)
            if ev.get('passes',True) or score >= 7:
                return f"REFLECTION PASSED (score {score}/10). Response is good. Proceed.", []
            issues = "\n".join(ev.get('issues',[]))
            return (f"REFLECTION FAILED (score {score}/10).\nISSUES:\n{issues}\n"
                    f"REWRITE: {ev.get('improvement_instructions','')}"), []
        except Exception as e:
            print(f"Reflection error: {e}")
            return "Reflection unavailable — proceed with current response.", []

    elif tool_name == "update_semantic_memory":
        fact_type = tool_args.get("fact_type")
        fact_value = tool_args.get("fact_value")
        importance = tool_args.get("importance", 5)
        
        if importance >= 7 and session_id and user_profile is not None:
            current_facts = user_profile.get('key_facts', {})
            current_facts[fact_type] = fact_value
            update_user_profile(session_id, key_facts=current_facts)
            return f"Stored in long-term memory: {fact_type} = {fact_value}", []
        return f"Memory not stored (importance {importance} < 7 or missing context)", []

    elif tool_name == "queue_autonomous_task":
        # v6.0: Queue a background autonomous task
        task_desc = tool_args.get("task_description", "")
        if task_desc and session_id:
            try:
                conn = get_db()
                c = conn.cursor()
                c.execute(
                    "INSERT INTO agent_tasks (session_id, task_description, status, created_at) VALUES (%s, %s, 'pending', %s) RETURNING id",
                    (session_id, task_desc, datetime.now())
                )
                task_id = c.fetchone()[0]
                conn.commit()
                conn.close()
                print(f"🎯 Queued autonomous task #{task_id}: {task_desc[:50]}")
                return f"✅ Task queued (#{task_id}): '{task_desc}'. Sophia will complete this in the background and email you the results.", []
            except Exception as e:
                print(f"Task queue error: {e}")
                return "Task queuing failed. Please try again.", []
        return "Task description required.", []

    elif tool_name == "update_goal_progress":
        # v6.0: Update long-term goal state
        goal = tool_args.get("goal", "")
        milestone = tool_args.get("milestone", "")
        status = tool_args.get("status", "in_progress")
        if goal and session_id:
            update_goal_state(session_id, goal, milestone, status)
            return f"Goal updated: '{goal}' → '{milestone}' [{status}]", []
        return "Goal update skipped (missing data)", []

    return "", []

# ============================================================
# QUICK ACTION OPENERS
# ============================================================
QUICK_ACTION_OPENERS = {
    "robotics": (
        "Great choice — China is currently the world's largest industrial robotics market, "
        "producing over 70% of global units.\n\n"
        "Before I connect you with the right intelligence:\n\n"
        "Are you looking to **SOURCE** robotics technology from China, "
        "or are you a Chinese robotics company seeking **Western partners or markets**?"
    ),
    "energy": (
        "Energy is one of the most dynamic China-West collaboration areas right now. "
        "China accounts for over 80% of global solar production and leads in battery storage.\n\n"
        "What's your energy focus?\n\n"
        "① Solar PV — panels, inverters, mounting systems\n"
        "② Battery storage — utility-scale or commercial\n"
        "③ EV charging infrastructure\n"
        "④ Wind energy components\n"
        "⑤ Green hydrogen\n"
        "⑥ Energy trading or investment"
    ),
    "biotech": (
        "China is now the world's second-largest pharma market and leads in biosimilar manufacturing.\n\n"
        "What brings you here?\n\n"
        "① Western pharma seeking Chinese CMO/CDMO partners\n"
        "② Licensing Chinese biotech innovations for Western markets\n"
        "③ Entering the Chinese healthcare market with a Western product\n"
        "④ R&D or clinical trial partnerships\n"
        "⑤ Medical devices"
    ),
    "shipping": (
        "China handles over 30% of global container shipping volume.\n\n"
        "What's your shipping challenge?\n\n"
        "① Moving goods FROM China (import logistics)\n"
        "② Shipping TO China (export logistics)\n"
        "③ Optimising existing supply chain\n"
        "④ Customs clearance and documentation\n"
        "⑤ Maritime technology partnerships"
    ),
    "verify": (
        "Smart move. Verifying before contracts or payments is critical in China business.\n\n"
        "What do you need to verify?\n\n"
        "① A Chinese supplier or manufacturer\n"
        "② A business partner or JV candidate\n"
        "③ A Chinese investment target\n"
        "④ Certificates or documents from a Chinese company\n"
        "⑤ A Chinese individual's background"
    ),
    "market_entry": (
        "Market entry is CWC's core expertise.\n\n"
        "First — your direction:\n\n"
        "① We are a **Western company** entering the Chinese market\n"
        "② We are a **Chinese company** expanding into Western markets\n"
        "③ Bilateral partnership or trade\n"
        "④ Still exploring the opportunity"
    )
}

# ============================================================
# CONVERSATION SUMMARY & HANDOFF
# ============================================================
def generate_handoff_brief(session_id: str, user_profile: dict) -> str:
    history   = get_conversation_history(session_id, limit=20)
    conv_text = "\n".join([f"User: {u}\nSophia: {a}" for u, a in history])
    name      = user_profile.get('name') or 'Unknown'
    email     = user_profile.get('email') or 'Not captured'
    company   = user_profile.get('company') or 'Not provided'
    region    = user_profile.get('region_interest') or 'Not specified'
    score     = user_profile.get('lead_score', 0)
    visits    = user_profile.get('visit_count', 1)
    lang      = user_profile.get('language', 'en')
    intent    = user_profile.get('last_intent', 'Unknown')
    key_facts = user_profile.get('key_facts', {})
    facts_text = "\n".join([f"   {k}: {v}" for k, v in key_facts.items() if v]) or "   Not yet extracted."
    
    # v6.0: Include active goals in handoff
    task_history = user_profile.get('task_history', []) or []
    active_goals = [t for t in task_history if t.get("status") in ("pending", "in_progress")]
    goals_text = "\n".join([f"   [{t['status'].upper()}] {t['goal']} → {t['milestone']}" for t in active_goals]) or "   None tracked"
    
    priority  = "🔥 HOT — Contact within 24h" if score >= 70 else ("🟡 WARM — Follow up 48h" if score >= 40 else "🔵 COLD — Nurture")
    return f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📋 SOPHIA v6.0 HANDOFF BRIEF
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
👤 CONTACT
   Name:     {name}
   Email:    {email}
   Company:  {company}
   Region:   {region}
   Language: {lang.upper()}

📊 LEAD INTELLIGENCE
   Score:    {score}/100 | {priority}
   Visits:   {visits} | Intent: {intent}

🧠 AI KEY FACTS
{facts_text}

🎯 ACTIVE GOALS (v6.0)
{goals_text}

💬 CONVERSATION
{conv_text[:1500] if conv_text else 'No conversation recorded'}

⚡ ACTION: {_recommend_action(score, intent, region)}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""


def _recommend_action(score: int, intent: str, region: str) -> str:
    if intent == "supplier_verification": return "URGENT: Due diligence needed. SAMR check + factory audit proposal."
    elif intent == "supplier_search":     return f"User wants supplier matching in {region or 'target market'}. Prepare shortlist."
    elif intent == "high_intent_lead" and score >= 60: return f"HIGH VALUE: Prepare service proposal for {region or 'target market'}."
    elif intent == "consultation_request": return "Schedule discovery call ASAP."
    elif region: return f"Prepare {region} market brief."
    else: return "Send personalised intro email with CWC capabilities deck."


def check_qualification_gaps(user_profile: dict, message_count: int) -> str | None:
    if message_count < 3: return None
    missing = []
    if not user_profile.get('region_interest'): missing.append("direction (Western→China or Chinese→West)")
    if not user_profile.get('topics_discussed'): missing.append("sector/industry")
    if not user_profile.get('last_intent') or user_profile.get('last_intent') == 'general': missing.append("specific goal")
    if len(missing) >= 2:
        return (f"\n⚡ QUALIFY NOW: After {message_count} messages you still don't know: {', '.join(missing)}. "
                "Ask ONE direct qualifying question before answering. Warm tone: 'Before I go further — can I ask...'")
    return None


def check_escalation_trigger(user_profile: dict, message_count: int, current_message: str) -> bool:
    urgency = ["urgent","asap","immediately","today","deposit","already paid","already transferred","fraud","scam","lost money","emergency"]
    if any(w in current_message.lower() for w in urgency): return True
    if user_profile.get('lead_score',0) >= 75 and message_count >= 4: return True
    return False

# ============================================================
# MAIN AI FUNCTION — v6.0 FULLY AGENTIC
# ============================================================
def ask_groq(prompt: str, session_id: str = "anonymous",
             user_profile: dict = None, quick_action: str = None,
             deep_search: bool = False) -> tuple:

    if not GROQ_API_KEY:
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

    # ── v5.1: CHECK FOR PROCEDURAL WORKFLOW FIRST ────────────────────────────
    workflow_result = None
    if intent_data['primary'] == "supplier_verification" and any(kw in prompt.lower() for kw in ["company", "verify", "check"]):
        company_match = re.search(r'(?:company|verify|check)\s+([A-Z][A-Za-z\s]+)', prompt)
        if company_match:
            company_name = company_match.group(1).strip()
            workflow_context = {
                "company_name": company_name,
                "message": prompt,
                "intent": intent_data['primary']
            }
            workflow_result = execute_procedural_workflow("supplier_verification_checklist", workflow_context, session_id)
            print(f"⚡ Procedural workflow executed: {workflow_result['completed_steps']}/{workflow_result['total_steps']} steps")

    # ── v5.1: HTN PLANNING FOR COMPLEX QUERIES ───────────────────────────────
    htn_plan_result = None
    if deep_search or intent_data['score'] >= 75:
        htn_context = {
            "intent": intent_data['primary'],
            "message": prompt,
            "direction": user_profile.get('region_interest') if user_profile else None,
            "sector": user_profile.get('topics_discussed') if user_profile else None,
            "company_name": re.search(r'[A-Z][A-Za-z\s]{2,20}', prompt).group(0) if re.search(r'[A-Z][A-Za-z\s]{2,20}', prompt) else None
        }
        htn_subtasks = htn_plan("handle_supplier_request", htn_context) or htn_plan("market_entry_strategy", htn_context)
        if htn_subtasks:
            htn_plan_result = execute_htn_plan(htn_subtasks, session_id, user_profile or {}, prompt)
            print(f"🎯 HTN plan executed: {len(htn_plan_result['completed'])}/{len(htn_subtasks)} tasks")

    # ── v5: TASK DECOMPOSITION (fallback if HTN didn't match) ─────────────────
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
                specialist_context = (f"\n\n📋 TASK PLAN: {task_plan.get('task_summary','')}\n"
                                     + "\n\n".join(sub_results)
                                     + f"\n\nExpected: {task_plan.get('expected_output','')}")

        # ── v5: SPECIALIST SUB-AGENT (with v5.1 delegation) ────────────────────
        agent_map = {"supplier_verification":"due_diligence","supplier_search":"supplier_match","consultation_request":"market_entry"}
        if intent_data['primary'] in agent_map:
            agent_type = agent_map[intent_data['primary']]
            print(f"🤖 Specialist sub-agent: {agent_type}")
            output = agent_delegate(agent_type, intent_data['primary'], prompt, depth=0)
            if output:
                specialist_context += f"\n\n🎓 SPECIALIST ({agent_type.upper().replace('_',' ')}):\n{output}"

    # ── v5.1: APPLY LEARNED PATTERNS ─────────────────────────────────────────
    learned_context = ""
    if user_profile:
        learned_context = apply_learned_patterns(prompt, intent_data['primary'], session_id)
        if learned_context:
            print("📚 Applied learned patterns")

    returning_context = ""
    if user_profile and user_profile.get('is_returning'):
        kf = user_profile.get('key_facts',{})
        facts_str = ", ".join([f"{k}:{v}" for k,v in kf.items() if v]) if kf else "none"
        returning_context = (
            f"\nRETURNING USER: Visit #{user_profile.get('visit_count',1)} | "
            f"Name: {user_profile.get('name') or 'Unknown'} | "
            f"Intent: {user_profile.get('last_intent','?')} | "
            f"Region: {user_profile.get('region_interest','?')} | "
            f"Score: {user_profile.get('lead_score',0)}/100 | "
            f"Key facts: {facts_str}\n"
            f"Summary: {user_profile.get('conversation_summary') or 'First tracked session'}\n"
            "INSTRUCTION: Reference previous interest naturally. Don't re-introduce yourself."
        )

    # v6.0: Inject active goals for journey continuity
    active_goals_context = get_active_goals(session_id) if session_id else ""

    qualification_prompt   = check_qualification_gaps(user_profile or {}, message_count)
    should_escalate        = check_escalation_trigger(user_profile or {}, message_count, prompt)
    escalation_instruction = ("\n🚨 ESCALATION: Urgent/high intent. End response directing them to 'Speak with Michail' button."
                               if should_escalate else "")

    sector_context = ""
    if quick_action:
        sector_map = {
            "robotics":"ACTIVE: ROBOTICS — sourcing vs Chinese expansion. Factory audits, CE, IP.",
            "energy":"ACTIVE: ENERGY — solar/battery/EV/wind/hydrogen. Ask scale (MW) and deal structure.",
            "biotech":"ACTIVE: BIOTECH — CMO/CDMO, pharma entry, R&D. Ask molecule type, GMP.",
            "shipping":"ACTIVE: SHIPPING — import/export, customs. Ask volume (FCL/LCL/air).",
            "verify":"ACTIVE: DUE DILIGENCE — URGENT. Ask company name, amounts at risk.",
            "market_entry":"ACTIVE: MARKET ENTRY — Determine direction. Deliver phased roadmap.",
        }
        sector_context = sector_map.get(quick_action,"")

    # ── v5.1: WORKFLOW CONTEXT INJECTION ─────────────────────────────────────
    workflow_context = ""
    if workflow_result:
        wf_data = workflow_result['results'][-1]['data'] if workflow_result['results'] else {}
        workflow_context = f"\n⚡ PROCEDURAL RESULT: Verification workflow completed. Status: {wf_data.get('registration_status', 'Unknown')}. Warning: {wf_data.get('warning', 'None')}"

    # v6.0: HTN agent trace for transparency injection
    htn_context_str = ""
    if htn_plan_result and htn_plan_result.get('completed'):
        completed_tasks = [s['task'] for s in htn_plan_result['completed']]
        htn_context_str = f"\n🎯 AGENT TRACE: Completed sub-tasks: {', '.join(completed_tasks)}"

    _now = datetime.now()
    _today_str = _now.strftime('%A, %d %B %Y')   # e.g. "Friday, 28 February 2026"
    _time_str  = _now.strftime('%H:%M UTC')

    system_prompt = f"""You are Sophia — official AI advisor for China West Connector (CWC).
Version 6.0 | Deep Search: {'ON' if deep_search else 'OFF'}
TODAY: {_today_str} | TIME: {_time_str}
IMPORTANT: When asked the date, day, or time — use ONLY the values above. Never guess.

INTENT: {intent_data['primary']} | REGION: {intent_data['region'] or '?'} | MESSAGES: {message_count}
{lang_instruction}
{returning_context}
{active_goals_context}
{sector_context}
{qualification_prompt or ''}
{escalation_instruction}
{specialist_context}
{learned_context}
{workflow_context}
{htn_context_str}

━━━ v6.0 AUTONOMOUS CAPABILITIES ━━━
You can now queue background tasks with queue_autonomous_task.
Use this when user says things like:
  "watch this company for me" → queue company monitoring
  "research X and get back to me" → queue market research
  "keep an eye on..." → queue news monitoring
  "check back on..." → queue follow-up task
Always confirm what you've queued and that they'll receive email results.

━━━ CHAIN OF THOUGHT (always execute) ━━━
1. What does the user ACTUALLY need (beyond what they literally asked)?
2. What do I know from their profile, pre-gathered intel, active goals, and learned patterns?
3. Do I need more tools — or is pre-gathered intel sufficient?
4. Should any part of this request be queued as an autonomous background task?
5. Which CWC service maps most directly to their need?
6. What is the single most valuable next step?
7. After drafting, CALL reflect_and_improve. If score < 7, REWRITE and reflect again (max 2 attempts).
8. Call update_goal_progress to track milestones reached this conversation.

TOOLS: search_market_intelligence | lookup_company | generate_risk_report | find_suppliers | reflect_and_improve | update_semantic_memory | queue_autonomous_task | update_goal_progress

━━━ MISSION ━━━
You are NOT a Q&A bot. You are an active business advisor.
1. QUALIFY (direction, sector, goal, urgency)
2. PERSONALISE using everything known about this user + their active goals
3. RECOMMEND CWC service with clear reasoning
4. END with a concrete next step — never a dead end
5. USE update_semantic_memory to store important facts (importance >= 7)
6. USE queue_autonomous_task for any deferred/monitoring requests

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

    url     = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization":f"Bearer {GROQ_API_KEY}","Content-Type":"application/json"}
    all_sources      = []
    reflection_score = 5
    agent_trace      = []   # v6.0: track tool calls for transparency

    MAX_ITERATIONS   = 10  # Increased for v6.0 iterative reflection
    response_text    = ""
    current_messages = [{"role":"system","content":system_prompt}] + list(messages)
    
    # v6.0: Track reflection attempts for iterative improvement loop
    reflection_attempts = 0
    MAX_REFLECTION_ATTEMPTS = 2

    try:
        for iteration in range(MAX_ITERATIONS):
            data = {
                "model":"llama-3.3-70b-versatile",
                "messages":current_messages,
                "tools":GROQ_TOOLS,
                "tool_choice":"auto",
                "temperature":0.3,
                "max_tokens":1000
            }
            res = requests.post(url, headers=headers, json=data, timeout=30)
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
                agent_trace.append(fn_name)  # v6.0: track for transparency
                
                tool_result, sources = run_tool_call(fn_name, fn_args, session_id=session_id, user_profile=user_profile)
                all_sources.extend(sources)
                
                if fn_name == "reflect_and_improve":
                    m = re.search(r'score (\d+)/10', tool_result)
                    if m: 
                        reflection_score = int(m.group(1))
                    reflection_attempts += 1
                    
                    # v6.0: ITERATIVE REFLECTION LOOP
                    # If reflection fails and we haven't hit max attempts, inject improvement instructions
                    # back into the message stream so the LLM rewrites before reflecting again
                    if "REFLECTION FAILED" in tool_result and reflection_attempts < MAX_REFLECTION_ATTEMPTS:
                        print(f"🔄 Reflection failed (score {reflection_score}/10) — triggering rewrite (attempt {reflection_attempts}/{MAX_REFLECTION_ATTEMPTS})")
                        # The tool result with REWRITE instructions will be fed back automatically
                        # since it's appended to current_messages below — LLM will rewrite in next iteration
                        
                elif fn_name == "update_semantic_memory":
                    print(f"💾 Memory update: {tool_result}")
                elif fn_name == "queue_autonomous_task":
                    print(f"🤖 Autonomous task queued: {fn_args.get('task_description','')[:40]}")
                elif fn_name == "update_goal_progress":
                    print(f"🎯 Goal progress: {tool_result}")
                    
                current_messages.append({
                    "role":"tool","tool_call_id":tool_call["id"],
                    "content":tool_result or "No results found."
                })

        if not response_text:
            res2 = requests.post(url, headers=headers, json={**data,"tools":[],"tool_choice":"none"}, timeout=25)
            res2.raise_for_status()
            response_text = res2.json()["choices"][0]["message"]["content"]

        # v6.0: LEARN FROM THIS INTERACTION
        new_score = calculate_lead_score(user_profile or {}, prompt, intent_data['primary'])
        update_user_profile(session_id, last_intent=intent_data['primary'],
                            region_interest=intent_data['region'], lead_score=new_score, language=lang)
        save_conversation(session_id, prompt, response_text,
                          region=intent_data['region'], intent=intent_data['primary'],
                          reflection_score=reflection_score)
        
        learn_from_interaction(session_id, prompt, response_text, reflection_score, intent_data['primary'])

        if message_count > 0 and message_count % 5 == 0:
            _update_conversation_summary(session_id)
        if message_count > 0 and message_count % 3 == 0:
            _extract_and_save_key_facts(session_id, user_profile or {})

        return response_text, list(dict.fromkeys(s for s in all_sources if s))[:5]

    except Exception as e:
        print(f"Groq error: {e}")
        return "I apologise — connection trouble. Please reach out to the CWC team directly.", []


def _update_conversation_summary(session_id: str):
    if not GROQ_API_KEY: return
    history = get_conversation_history(session_id, limit=10)
    if not history: return
    conv_text = "\n".join([f"User: {u}\nSophia: {a[:100]}" for u, a in history])
    try:
        res = requests.post("https://api.groq.com/openai/v1/chat/completions",
                            headers={"Authorization":f"Bearer {GROQ_API_KEY}","Content-Type":"application/json"},
                            json={"model":"llama-3.3-70b-versatile","messages":[
                                {"role":"system","content":"Summarise this conversation in 2-3 sentences: what user wants, direction, sector, urgency. Factual."},
                                {"role":"user","content":conv_text}
                            ],"temperature":0.1,"max_tokens":150}, timeout=10)
        update_user_profile(session_id, conversation_summary=res.json()["choices"][0]["message"]["content"])
    except Exception as e: print(f"Summary error: {e}")


def _extract_and_save_key_facts(session_id: str, user_profile: dict):
    if not GROQ_API_KEY: return
    history = get_conversation_history(session_id, limit=6)
    if not history: return
    conv_text = "\n".join([f"User: {u}\nSophia: {a[:80]}" for u, a in history])
    existing  = user_profile.get('key_facts',{}) or {}
    try:
        res = requests.post("https://api.groq.com/openai/v1/chat/completions",
                            headers={"Authorization":f"Bearer {GROQ_API_KEY}","Content-Type":"application/json"},
                            json={"model":"llama-3.3-70b-versatile","messages":[
                                {"role":"system","content":(
                                    "Extract business facts from conversation. Return ONLY JSON with keys "
                                    "(null if unknown): direction, sector, goal, urgency, company_name, "
                                    "supplier_names (array), budget_mentioned (bool), target_market. No markdown."
                                )},
                                {"role":"user","content":conv_text}
                            ],"temperature":0.0,"max_tokens":200}, timeout=10)
        raw = re.sub(r"^```json\s*|```$","",res.json()["choices"][0]["message"]["content"].strip(),flags=re.MULTILINE).strip()
        extracted = json.loads(raw)
        for k, v in extracted.items():
            if v is not None and v != [] and v is not False: existing[k] = v
            elif k not in existing: existing[k] = v
        update_user_profile(session_id, key_facts=existing)
        print(f"🧠 Facts: {existing}")
    except Exception as e: print(f"Key facts error: {e}")

# ============================================================
# EMAIL FUNCTIONS
# ============================================================
def send_email_brevo(to_email: str, subject: str, body: str, from_name: str = "CWC AI") -> bool:
    try:
        res = requests.post("https://api.brevo.com/v3/smtp/email",
                            headers={"accept":"application/json","content-type":"application/json","api-key":BREVO_API_KEY},
                            json={"sender":{"name":from_name,"email":SENDER_EMAIL},"to":[{"email":to_email,"name":"Michail Digkas"}],
                                  "subject":subject,
                                  "htmlContent":f"<html><body><pre style='font-family:monospace;white-space:pre-wrap;'>{body}</pre></body></html>",
                                  "textContent":body}, timeout=10)
        if res.status_code == 201: print(f"✅ Email sent to {to_email}"); return True
        print(f"❌ Brevo error: {res.status_code}"); return False
    except Exception as e: print(f"❌ Email failed: {e}"); return False


def send_lead_notification(lead: LeadCapture):
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT * FROM user_profiles WHERE session_id=%s", (lead.session_id,))
    pr = c.fetchone(); conn.close()
    lead_score  = pr[10] if pr else 0
    visit_count = pr[4]  if pr else 1
    user_profile = {}
    if pr:
        raw_kf = pr[14] if len(pr) > 14 else {}
        user_profile = {"name":pr[5],"email":pr[6],"company":pr[7],"region_interest":pr[8],
                        "topics_discussed":pr[9],"lead_score":pr[10],"last_intent":pr[11],
                        "visit_count":pr[4],"language":pr[12] if len(pr)>12 else "en",
                        "conversation_summary":pr[13] if len(pr)>13 else None,
                        "key_facts":raw_kf if isinstance(raw_kf,dict) else {}}
    handoff = generate_handoff_brief(lead.session_id, user_profile)
    send_email_brevo(
        to_email=RECIPIENT_EMAIL,
        subject=f"🎯 New Lead: {lead.name} from {lead.company or 'Website'} (Score: {lead_score}/100)",
        body=f"""━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🎯 NEW LEAD — SOPHIA v6.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
NAME: {lead.name} | EMAIL: {lead.email}
COMPANY: {lead.company or '?'} | REGION: {lead.region or '?'}
SOURCE: {lead.source} | TIME: {lead.timestamp}
SCORE: {lead_score}/100 | VISITS: {visit_count}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{handoff}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Dashboard: https://cwc-ai-backend.onrender.com/analytics?password={ADMIN_PASSWORD}
Leads:     https://cwc-ai-backend.onrender.com/leads?password={ADMIN_PASSWORD}
Reply:     mailto:{lead.email}"""
    )


def send_weekly_report():
    conn = get_db()
    c = conn.cursor()
    w = (datetime.now() - timedelta(days=7)).isoformat()
    c.execute("SELECT COUNT(DISTINCT session_id) FROM conversations WHERE timestamp>%s",(w,)); uu=c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM conversations WHERE timestamp>%s",(w,)); tm=c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM leads WHERE timestamp>%s",(w,)); nl=c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM user_profiles WHERE visit_count>1 AND last_seen>%s",(w,)); ru=c.fetchone()[0]
    c.execute("SELECT intent,COUNT(*) FROM conversations WHERE timestamp>%s GROUP BY intent ORDER BY 2 DESC LIMIT 5",(w,)); ti=c.fetchall()
    c.execute("SELECT region,COUNT(*) FROM conversations WHERE timestamp>%s AND region IS NOT NULL GROUP BY region ORDER BY 2 DESC LIMIT 5",(w,)); tr=c.fetchall()
    c.execute("SELECT name,email,company,region,timestamp FROM leads WHERE timestamp>%s ORDER BY timestamp DESC LIMIT 10",(w,)); rl=c.fetchall()
    c.execute("SELECT name,email,company,lead_score FROM user_profiles WHERE lead_score>=50 ORDER BY lead_score DESC LIMIT 5"); hl=c.fetchall()
    c.execute("SELECT language,COUNT(*) FROM user_profiles WHERE last_seen>%s GROUP BY language ORDER BY 2 DESC",(w,)); lg=c.fetchall()
    c.execute("SELECT AVG(reflection_score) FROM conversations WHERE timestamp>%s",(w,)); ar=round(c.fetchone()[0] or 0,1)
    c.execute("SELECT COUNT(*) FROM supplier_searches WHERE created_at>%s",(w,)); ss=c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM learned_patterns WHERE created_at>%s",(w,)); lp=c.fetchone()[0]
    c.execute("SELECT pattern_type, COUNT(*) FROM learned_patterns GROUP BY pattern_type"); pt=c.fetchall()
    # v6.0: Autonomous tasks stats
    c.execute("SELECT COUNT(*) FROM agent_tasks WHERE created_at>%s",(w,)); at_total=c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM agent_tasks WHERE status='completed' AND created_at>%s",(w,)); at_done=c.fetchone()[0]
    conn.close()
    send_email_brevo(
        to_email=RECIPIENT_EMAIL,
        subject=f"📊 CWC AI Weekly — {uu} Users, {nl} Leads, Quality: {ar}/10",
        body=f"""━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📊 SOPHIA v6.0 WEEKLY REPORT
{w[:10]} → {datetime.now().strftime('%Y-%m-%d')}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
OVERVIEW: Users {uu} | Messages {tm} | Returning {ru} | Leads {nl}
Avg Response Quality: {ar}/10 | Supplier Searches: {ss}

INTENTS: {' | '.join([f"{i[0]}:{i[1]}" for i in ti]) or 'N/A'}
REGIONS: {' | '.join([f"{r[0]}:{r[1]}" for r in tr]) or 'N/A'}
LANGUAGES: {' | '.join([f"{l[0].upper()}:{l[1]}" for l in lg]) or 'N/A'}

HOT LEADS: {' | '.join([f"{h[0]}({h[2] or '?'}) {h[3]}/100" for h in hl if h[0]]) or 'None'}
RECENT LEADS: {chr(10).join([f"  • {l[0]} {l[1]} ({l[2] or '?'}) [{l[3] or '?'}]" for l in rl]) or 'None'}

🧠 LEARNED PATTERNS: {lp} new this week
{' | '.join([f"{p[0]}:{p[1]}" for p in pt]) or 'No patterns yet'}

🤖 AUTONOMOUS TASKS (v6.0): {at_total} queued, {at_done} completed
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Dashboard: https://cwc-ai-backend.onrender.com/analytics?password={ADMIN_PASSWORD}"""
    )
    print("✅ Weekly report sent!")

# ============================================================
# API ENDPOINTS
# ============================================================
@app.get("/")
def root():
    return {
        "service": "CWC Sophia AI — China-West Business Intelligence",
        "version": "6.0.0",
        "status": "operational",
        "features": [
            "procedural_memory", "htn_planning_with_conditional_replanning",
            "multi_agent_delegation", "continuous_learning_with_normalized_patterns",
            "iterative_self_reflection", "task_decomposition", "specialist_sub_agents",
            "accio_supplier_search", "autonomous_background_tasks",  # v6.0
            "long_horizon_goal_tracking",                            # v6.0
            "proactive_hot_lead_followup",                           # v6.0
            "agent_trace_transparency"                               # v6.0
        ],
        "public_api": "GET /api/sophia?q=your+question",
        "news_api": "GET /api/news",
        "docs": "/docs"
    }

@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "groq": bool(GROQ_API_KEY),
        "tavily": bool(TAVILY_API_KEY),
        "brevo": bool(BREVO_API_KEY),
        "db": bool(DATABASE_URL),
        "version": "6.0.0",
        "new_v6_features": ["autonomous_tasks", "goal_tracking", "iterative_reflection", "proactive_followup"]
    }

@app.get("/new-session")
@app.post("/new-session")
def new_session(req: ChatRequest):
    get_or_create_user_profile(req.session_id, new_session=True)
    return {"status":"session registered"}

@app.post("/chat")
async def chat(req: ChatRequest, request: Request):
    if is_rate_limited(request.client.host):
        return {"response":"Too many requests. Please wait a moment.","sources":[]}
    user_msg     = req.message.lower()
    user_profile = get_or_create_user_profile(req.session_id)
    if any(w in user_msg for w in ["stop","shorter","brief","short","too long"]):
        return {"response":"Got it — I'll keep answers concise. What would you like to know?","sources":[]}
    if is_cacheable(req.message):
        cached = get_cached_response(req.message)
        if cached:
            return {"response":cached[0],"sources":cached[1],"cached":True}
    consultation_kw = ["book","consultation","call","schedule","meet","contact","michail","digkas"]
    is_consultation = any(kw in user_msg for kw in consultation_kw)
    reply, sources = ask_groq(req.message, req.session_id, user_profile, deep_search=req.deep_search)
    if is_consultation and user_profile.get('lead_score',0) >= 20:
        brief = generate_handoff_brief(req.session_id, user_profile)
        send_email_brevo(RECIPIENT_EMAIL,
                         f"📋 Sophia v6.0 Handoff: {user_profile.get('name') or 'Prospect'} requested consultation",
                         brief)
    high_intent = ["price","cost","fee","how much","start","begin","help me","serious","interested","manufacturer","supplier","factory","invest"]
    if any(w in user_msg for w in high_intent):
        if "consultation" not in reply.lower() and "button" not in reply.lower():
            reply += "\n\nTo discuss next steps, click the 'Speak with Michail' button above."
    if is_cacheable(req.message) and reply:
        set_cached_response(req.message, reply, sources)
    return {"response":reply,"sources":sources}

@app.post("/quick-action")
def quick_action(req: QuickActionRequest):
    action = req.action.lower().strip()
    if action not in QUICK_ACTION_OPENERS:
        return {"response":"Hello! I'm Sophia, CWC's AI advisor. How can I help with China-West business today?","action":"general"}
    msg = QUICK_ACTION_OPENERS[action]
    save_conversation(session_id=req.session_id, user_msg=f"[Quick Action: {action}]", ai_response=msg, intent=action)
    update_user_profile(req.session_id, last_intent=action, topics_discussed=action)
    return {"response":msg,"action":action}

@app.post("/capture-lead")
async def capture_lead(lead: LeadCapture, background_tasks: BackgroundTasks):
    conn = get_db()
    c = conn.cursor()
    c.execute("INSERT INTO leads (name,email,company,region,session_id,source,timestamp,status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
              (lead.name,lead.email,lead.company,lead.region,lead.session_id,lead.source,lead.timestamp,'new'))
    conn.commit(); conn.close()
    update_user_profile(lead.session_id, name=lead.name, email=lead.email, company=lead.company, region_interest=lead.region)
    background_tasks.add_task(send_lead_notification, lead)
    return {"status":"success","message":"Lead captured"}

@app.post("/api/find-suppliers")
async def find_suppliers_endpoint(req: SupplierSearchRequest):
    if not req.query or len(req.query.strip()) < 2:
        return {"error":"Query required"}
    result = search_suppliers(req.query.strip(), req.region)
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("INSERT INTO supplier_searches (session_id,query,results,created_at) VALUES (%s,%s,%s::jsonb,%s)",
                  (req.session_id, req.query, json.dumps(result), datetime.now()))
        conn.commit(); conn.close()
    except Exception as e: print(f"Supplier search save error: {e}")
    return {"query":req.query,"sector":req.sector,"region":req.region,"intelligence":result,
            "powered_by":"Sophia — CWC Supplier Intelligence v6.0",
            "note":"For verified supplier matching with full due diligence, contact CWC.",
            "contact":"https://www.chinawestconnector.com"}

# v6.0: NEW ENDPOINTS
@app.post("/api/queue-task")
async def queue_task_endpoint(req: AutonomousTaskRequest):
    """
    v6.0: Directly queue an autonomous background task via API.
    The task will be picked up by the background poller within 5 minutes.
    """
    if not req.task_description or len(req.task_description.strip()) < 5:
        return {"error": "Task description required (min 5 chars)"}
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute(
            "INSERT INTO agent_tasks (session_id, task_description, status, created_at) VALUES (%s, %s, 'pending', %s) RETURNING id",
            (req.session_id, req.task_description.strip(), datetime.now())
        )
        task_id = c.fetchone()[0]
        conn.commit(); conn.close()
        return {
            "status": "queued",
            "task_id": task_id,
            "message": f"Task #{task_id} queued. Sophia will complete it in the background and notify via email.",
            "task": req.task_description
        }
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/task-status/{task_id}")
def get_task_status(task_id: int, password: str = None):
    """v6.0: Check status of an autonomous background task"""
    if password != ADMIN_PASSWORD:
        # Allow session-based checks without password (limited info)
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT id, status, created_at, completed_at FROM agent_tasks WHERE id=%s", (task_id,))
        row = c.fetchone(); conn.close()
        if not row: return {"error": "Task not found"}
        return {"task_id": row[0], "status": row[1], "created_at": str(row[2]), "completed_at": str(row[3])}
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT id, session_id, task_description, status, result, created_at, completed_at FROM agent_tasks WHERE id=%s", (task_id,))
    row = c.fetchone(); conn.close()
    if not row: return {"error": "Task not found"}
    return {"task_id": row[0], "session_id": row[1], "description": row[2], "status": row[3],
            "result": row[4], "created_at": str(row[5]), "completed_at": str(row[6])}

@app.get("/api/tasks")
def list_tasks(password: str = None):
    """v6.0: Admin view of all autonomous tasks"""
    if password != ADMIN_PASSWORD: return {"error": "Unauthorized"}
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT id, session_id, task_description, status, created_at, completed_at FROM agent_tasks ORDER BY created_at DESC LIMIT 50")
    tasks = c.fetchall(); conn.close()
    return {
        "tasks": [{"id":t[0],"session":t[1],"description":t[2],"status":t[3],"created":str(t[4]),"completed":str(t[5])} for t in tasks],
        "count": len(tasks)
    }

@app.get("/api/goals/{session_id}")
def get_session_goals(session_id: str):
    """v6.0: View active goals for a session"""
    profile = get_or_create_user_profile(session_id)
    task_history = profile.get('task_history', []) or []
    return {
        "session_id": session_id,
        "all_goals": task_history,
        "active_goals": [t for t in task_history if t.get("status") in ("pending", "in_progress")],
        "completed_goals": [t for t in task_history if t.get("status") == "done"],
    }

@app.get("/leads")
def view_leads(password: str = None):
    if password != ADMIN_PASSWORD: return {"error":"Unauthorized"}
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT * FROM leads ORDER BY timestamp DESC LIMIT 50")
    leads = c.fetchall(); conn.close()
    return {"leads":[{"id":l[0],"name":l[1],"email":l[2],"company":l[3],"region":l[4],"timestamp":l[7],"status":l[8]} for l in leads],"count":len(leads)}

@app.get("/analytics")
def get_analytics(password: str = None, days: int = 7):
    if password != ADMIN_PASSWORD: return {"error":"Unauthorized"}
    conn = get_db()
    c = conn.cursor()
    since = (datetime.now() - timedelta(days=days)).isoformat()
    c.execute("SELECT COUNT(DISTINCT session_id) FROM conversations WHERE timestamp>%s",(since,)); uu=c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM conversations WHERE timestamp>%s",(since,)); tc=c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM leads WHERE timestamp>%s",(since,)); nl=c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM user_profiles WHERE visit_count>1 AND last_seen>%s",(since,)); ru=c.fetchone()[0]
    c.execute("SELECT intent,COUNT(*) FROM conversations WHERE timestamp>%s GROUP BY intent ORDER BY 2 DESC LIMIT 5",(since,)); ti=[{"intent":r[0],"count":r[1]} for r in c.fetchall()]
    c.execute("SELECT region,COUNT(*) FROM conversations WHERE timestamp>%s AND region IS NOT NULL GROUP BY region ORDER BY 2 DESC LIMIT 5",(since,)); tr=[{"region":r[0],"count":r[1]} for r in c.fetchall()]
    c.execute("SELECT name,email,company,lead_score FROM user_profiles WHERE lead_score>=50 ORDER BY lead_score DESC LIMIT 10"); hl=[{"name":r[0],"email":r[1],"company":r[2],"score":r[3]} for r in c.fetchall() if r[0]]
    c.execute("SELECT language,COUNT(*) FROM user_profiles WHERE last_seen>%s GROUP BY language ORDER BY 2 DESC",(since,)); lg=[{"language":r[0],"count":r[1]} for r in c.fetchall()]
    c.execute("SELECT AVG(reflection_score) FROM conversations WHERE timestamp>%s",(since,)); aq=round(c.fetchone()[0] or 0,1)
    c.execute("SELECT COUNT(*) FROM supplier_searches WHERE created_at>%s",(since,)); ss=c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM learned_patterns WHERE created_at>%s",(since,)); lp=c.fetchone()[0]
    c.execute("SELECT pattern_type, COUNT(*) FROM learned_patterns GROUP BY pattern_type"); pt=[{"type":r[0],"count":r[1]} for r in c.fetchall()]
    # v6.0: Autonomous tasks analytics
    c.execute("SELECT COUNT(*) FROM agent_tasks WHERE created_at>%s",(since,)); at_total=c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM agent_tasks WHERE status='completed' AND created_at>%s",(since,)); at_done=c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM agent_tasks WHERE status='failed' AND created_at>%s",(since,)); at_fail=c.fetchone()[0]
    conn.close()
    return {
        "period_days":days,"unique_users":uu,"total_conversations":tc,"new_leads":nl,"returning_users":ru,
        "top_intents":ti,"top_regions":tr,"hot_leads":hl,"languages":lg,
        "avg_response_quality":aq,"supplier_searches":ss,
        "learned_patterns_count":lp,"pattern_types":pt,
        "autonomous_tasks": {"total": at_total, "completed": at_done, "failed": at_fail}  # v6.0
    }

@app.get("/trigger-report")
def trigger_report(password: str = None):
    if password != ADMIN_PASSWORD: return {"error":"Unauthorized"}
    try: send_weekly_report(); return {"status":"Report sent!","sent_to":RECIPIENT_EMAIL}
    except Exception as e: return {"error":str(e)}

@app.get("/test-email")
def test_email(password: str = None):
    if password != ADMIN_PASSWORD: return {"error":"Unauthorized"}
    ok = send_email_brevo(RECIPIENT_EMAIL,"✅ CWC AI v6.0 Email Test",
                          "Sophia v6.0 email working.\nNew: autonomous tasks, goal tracking, iterative reflection, conditional re-planning, proactive followup.")
    return {"status":"Sent!","sent_to":RECIPIENT_EMAIL} if ok else {"error":"Email failed"}

@app.get("/api/news")
def get_news(force_refresh: bool = False):
    """
    Returns fresh China business news.
    News is fetched via Tavily and persisted in the DB (survives server restarts).
    Cache TTL: {NEWS_TTL_HOURS} hours.
    Add ?force_refresh=true to bypass cache (admin use).
    """
    if force_refresh:
        # Wipe DB cache entry so next call fetches fresh
        try:
            conn = get_db()
            c = conn.cursor()
            c.execute("DELETE FROM response_cache WHERE cache_key=%s", (NEWS_CACHE_KEY,))
            conn.commit(); conn.close()
            print("📰 News cache cleared by force_refresh")
        except Exception as e:
            print(f"News cache clear error: {e}")

    news = fetch_china_news()

    # Report cache age for transparency
    cache_info = {}
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT created_at FROM response_cache WHERE cache_key=%s", (NEWS_CACHE_KEY,))
        row = c.fetchone(); conn.close()
        if row:
            created = row[0] if isinstance(row[0], datetime) else datetime.fromisoformat(str(row[0]))
            age_min = int((datetime.now() - created).total_seconds() / 60)
            cache_info = {
                "cached_at": created.isoformat(),
                "age_minutes": age_min,
                "expires_in_minutes": max(0, NEWS_TTL_HOURS * 60 - age_min),
                "source": "db_cache" if age_min < NEWS_TTL_HOURS * 60 else "just_refreshed"
            }
    except Exception:
        pass

    return {"news": news, "count": len(news), "cache": cache_info}

@app.get("/api/verify-company")
async def verify_company(name: str, password: str = None):
    if not name or len(name.strip()) < 2: return {"error":"Company name required"}
    return lookup_chinese_company(name.strip())

@app.get("/api/sophia")
async def sophia_public_api(q: str, source: str = "external_ai"):
    if not q or len(q.strip()) < 3: return {"error":"Query parameter 'q' required"}
    if is_cacheable(q):
        cached = get_cached_response(q)
        if cached:
            return {"query":q,"answer":cached[0],"sources":cached[1],"powered_by":"Sophia v6.0","cached":True}
    sid = f"api_{source}_{int(time.time())}"
    sc, sources = search_web(q)
    reply, _ = ask_groq(f"External AI query: {q}\nData: {sc or 'none'}\nAnswer factually. End: For guidance visit chinawestconnector.com", sid)
    if is_cacheable(q) and reply: set_cached_response(q, reply, sources)
    return {"query":q,"answer":reply,"sources":sources,"powered_by":"Sophia v6.0",
            "contact":"https://www.chinawestconnector.com","timestamp":datetime.now().isoformat()}

@app.get("/llms.txt", response_class=PlainTextResponse)
def llms_txt():
    return """# China West Connector (CWC) — AI Intelligence Layer

> Sophia v6.0 — Fully Agentic: procedural memory, HTN planning with conditional re-planning,
>               multi-agent delegation, continuous learning, iterative self-reflection,
>               autonomous background tasks, long-horizon goal tracking, proactive outreach.

## What CWC Does
China West Connector bridges Chinese and Western businesses.
Founded by Michail Digkas, international business lawyer, 10+ years China experience.
G.P.A. Group member: 147+ years combined experience, 2,700+ projects, 50+ countries (Group figures).

## Government Partnerships
Sichuan Tech Transfer | Chengdu AI Association | Tianfu Tech Center | Hainan FTZ | CISTEA

## Core Services
Legal | Due Diligence | B2B Partnerships | FDI Consulting | Logistics | Liaison

## Regions
Europe • Africa • Middle East • Latin America • Central Asia • North America

## Languages
English • Chinese • Arabic • Spanish • French • German • Russian

## API
Query Sophia:       GET  https://cwc-ai-backend.onrender.com/api/sophia?q=your+question
Find Suppliers:     POST https://cwc-ai-backend.onrender.com/api/find-suppliers
Live China News:    GET  https://cwc-ai-backend.onrender.com/api/news
Company Lookup:     GET  https://cwc-ai-backend.onrender.com/api/verify-company?name=company
Queue Task:         POST https://cwc-ai-backend.onrender.com/api/queue-task
Docs:               https://cwc-ai-backend.onrender.com/docs

## Contact
https://www.chinawestconnector.com | info@chinawestconnector.com
"""

@app.get("/sitemap-ai.xml", response_class=PlainTextResponse)
def sitemap_ai():
    return """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://www.chinawestconnector.com</loc><priority>1.0</priority></url>
  <url><loc>https://cwc-ai-backend.onrender.com/api/sophia</loc><priority>0.9</priority></url>
  <url><loc>https://cwc-ai-backend.onrender.com/api/find-suppliers</loc><priority>0.9</priority></url>
  <url><loc>https://cwc-ai-backend.onrender.com/api/news</loc><priority>0.8</priority></url>
  <url><loc>https://cwc-ai-backend.onrender.com/api/verify-company</loc><priority>0.8</priority></url>
  <url><loc>https://cwc-ai-backend.onrender.com/api/queue-task</loc><priority>0.8</priority></url>
  <url><loc>https://cwc-ai-backend.onrender.com/llms.txt</loc><priority>0.9</priority></url>
  <url><loc>https://cwc-ai-backend.onrender.com/docs</loc><priority>0.8</priority></url>
</urlset>
"""
