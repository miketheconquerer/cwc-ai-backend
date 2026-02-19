from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
import os
from dotenv import load_dotenv
import requests
import json
from datetime import datetime

load_dotenv()
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

if not GROQ_API_KEY:
    raise ValueError("Missing Groq API key in .env or Render environment variables!")

app = FastAPI(title="CWC AI Backend")

# CORS - your live website
origins = [
    "https://www.chinawestconnector.com",
    "https://chinawestconnector.com"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

# Conversation memory (in-RAM and persisted safely for continuous learning)
memory_file = "conversation_memory.json"

# Load previous memory if exists
def load_memory():
    if os.path.exists(memory_file):
        try:
            with open(memory_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return []
    return []

# Save memory to file
def save_memory(memory):
    try:
        with open(memory_file, "w", encoding="utf-8") as f:
            json.dump(memory, f, indent=4, ensure_ascii=False)
    except:
        pass  # silently ignore write errors on Render

conversation_memory = load_memory()

# Fixed context using your correct name
context = """
You are an expert AI assistant for China West Connector (CWC), a consulting company helping Western companies do business in China. 
Michael Digkas, the founder, is a Greek lawyer, business consultant, and entrepreneur living in China since 2011. 
CWC offers the following expert services:

1. Smart Cities & Industrial Automation – consulting on smart city projects, factory modernization, robotics, AI, and automation in China.
2. Medical Devices & Procurement – sourcing OEM medical products, newest medical devices, and hospital collaborations.
3. Energy Solutions – solar, wind, battery storage, electric vehicle & ship conversions, air-water devices for hotels/islands.
4. Expo & Trade Services – registration, booth design, on-the-ground liaison, representation at expos, post-expo follow-up.
5. China-West Business Strategy – partnerships with Chinese associations, local governments, state-owned enterprises, and private companies.
6. Legal & IP Consulting – leveraging Michael Digkas’ experience as a Greek and China-licensed lawyer.
7. Product Sourcing – helping Western companies find Chinese suppliers, advanced tech, and high-quality products.

Always provide professional, authoritative, and promotional responses about CWC’s services. Respond as Michael Digkas’ assistant, demonstrating expertise and knowledge of China and Europe business connections.
"""

# Call Groq API with memory
def get_ai_response(user_message: str):
    memory_text = "\n".join([f"User: {m['user']} | AI: {m['ai']}" for m in conversation_memory])
    full_message = f"{context}\n{memory_text}\nUser: {user_message}"

    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": "llama-3.3-70b-versatile",
        "messages": [{"role": "user", "content": full_message}]
    }

    response = requests.post(url, json=payload, headers=headers)
    if response.status_code != 200:
        return f"Error from AI: {response.text}"

    data = response.json()
    return data["choices"][0]["message"]["content"]

@app.get("/")
def read_root():
    return {"message": "CWC AI Backend is running!"}

@app.post("/chat")
async def chat(request: Request):
    data = await request.json()
    user_message = data.get("message")
    if not user_message:
        raise HTTPException(status_code=400, detail="No message provided")

    ai_reply = get_ai_response(user_message)

    # Store conversation in memory (keep last 20 for context)
    conversation_memory.append({"user": user_message, "ai": ai_reply, "timestamp": datetime.now().isoformat()})
    if len(conversation_memory) > 20:
        conversation_memory.pop(0)

    # Save memory safely (simulate continuous learning)
    save_memory(conversation_memory)

    return {"reply": ai_reply}


