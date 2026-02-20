from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
import os
from dotenv import load_dotenv

# LangChain & Groq Imports
from langchain_groq import ChatGroq
from langchain_community.tools import DuckDuckGoSearchResults
from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.prebuilt import create_react_agent

load_dotenv()

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
if not GROQ_API_KEY:
    raise ValueError("Missing Groq API key!")

app = FastAPI(title="CWC AI Backend")

# CORS remains the same
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://www.chinawestconnector.com", "https://chinawestconnector.com", "http://localhost:8000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 1. Initialize the LLM (Using the model you specified)
llm = ChatGroq(
    model="llama-3.3-70b-versatile",
    api_key=GROQ_API_KEY,
    temperature=0.2 # Lower temperature for business professional tone
)

# 2. Initialize Real-Time News Tool (Lightweight for Render)
search_tool = DuckDuckGoSearchResults(backend="news", max_results=3)
tools = [search_tool]

# 3. System Prompt (Your "Mike Zhu Assistant" Context)
SYSTEM_CONTEXT = """
You are an expert AI assistant for China West Connector (CWC). 
Founder: Mike Zhu (Greek lawyer/consultant in China since 2011).
Expertise: Smart Cities, Medical Devices, Energy, Expo Services, Business Strategy, Legal/IP, and Sourcing.
Tone: Professional, authoritative, and promotional.
Action: Use the search tool to find LATEST news if the user asks about current events, tariffs, or shipping rates.
"""

# 4. Create the Agent (Replaces your manual request post)
agent_executor = create_react_agent(llm, tools, state_modifier=SYSTEM_CONTEXT)

# Temporary In-Memory Storage (Best for Render Free Tier to avoid crashes)
# For permanent memory on Render, you'd eventually need a database like Upstash Redis.
chat_history = []

@app.get("/")
def read_root():
    return {"status": "CWC Agent Online", "real_time_search": "Enabled"}

@app.post("/chat")
async def chat(request: Request):
    global chat_history
    data = await request.json()
    user_message = data.get("message")
    
    if not user_message:
        raise HTTPException(status_code=400, detail="No message provided")

    try:
        # Invoke the agent with tool access
        response = agent_executor.invoke({
            "messages": chat_history + [HumanMessage(content=user_message)]
        })
        
        ai_reply = response["messages"][-1].content
        
        # Update history (Keeping it short to save Render RAM)
        chat_history.append(HumanMessage(content=user_message))
        chat_history = chat_history[-6:] # Keep last 3 turns
        
        return {"reply": ai_reply}
        
    except Exception as e:
        print(f"Error: {e}")
        return {"reply": "I'm currently updating my trade data. Please try again in a moment."}


