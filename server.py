from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
import os
from dotenv import load_dotenv
import requests
import json
from datetime import datetime

# Load .env locally
load_dotenv()

# Get Groq API key from environment variables
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
if not GROQ_API_KEY:
    raise ValueError(
        "Missing Groq API key! Set it in your .env file locally or in Render environment variables."
    )

# Initialize FastAPI
app = FastAPI(title="CWC AI Backend")

# Configure CORS
origins = [
    "https://www.chinawestconnector.com",
    "https://chinawestconnector.com",
    "http://localhost:8000",  # optional for local testing
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Load previous conversations for memory (stored as JSON file)
memory_file = "conversation_memory.json"

# Load previous memory, if exists
def load_memory():
    if os.path.exists(memory_file):
        with open(memory_file, "r") as f:
            return json.load(f)
    return {}

# Save memory to a file (for continuous learning simulation)
def save_memory(memory):
    with open(memory_file, "w") as f:
        json.dump(memory, f, indent=4)

# Initialize memory
memory = load_memory()

# Root endpoint
@app.get("/")
def read_root():
    return {"message": "CWC AI Backend is running!"}

# Function to call Groq API with context
def get_ai_response(user_message: str, memory: dict):
    context = """
You are an expert AI assistant for China West Connector (CWC), a consulting company helping Western companies do business in China. 
Mike Zhu, the founder, is a Greek lawyer, business consultant, and entrepreneur living in China since 2011. 
CWC offers the following expert services:

1. **Smart Cities & Industrial Automation** – consulting on smart city projects, factory modernization, robotics, AI, and automation in China.
2. **Medical Devices & Procurement** – sourcing OEM medical products, newest medical devices, and hospital collaborations.
3. **Energy Solutions** – solar, wind, battery storage, electric vehicle & ship conversions, air-water devices for hotels/islands.
4. **Expo & Trade Services** – registration, booth design, on-the-ground liaison, representation at expos, post-expo follow-up.
5. **China-West Business Strategy** – partnerships with Chinese associations, local governments, state-owned enterprises, and private companies.
6. **Legal & IP Consulting** – leveraging Mike’s experience as a Greek and China-licensed lawyer.
7. **Product Sourcing** – helping Western companies find Chinese suppliers, advanced tech, and high-quality products.

Always provide professional, authoritative, and promotional responses about CWC’s services. Tailor your advice for **Western companies seeking to connect with China**, highlighting business opportunities, partnerships, and innovation.

Respond **as if you are Mike’s assistant**, demonstrating expertise, knowledge of China, and ability to connect businesses efficiently.
"""

    # Add memory to the context for continuity
    memory_string = ""
    if memory:
        memory_string = "\n".join([f"User: {msg['user_message']} | AI: {msg['ai_reply']}" for msg in memory])

    full_context = f"{context}\n\nMemory:\n{memory_string}\n\nUser: {user_message}"

    # Call Groq API as usual
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": "llama-3.3-70b-versatile",
        "messages": [{"role": "user", "content": full_context}]
    }

    response = requests.post(url, json=payload, headers=headers)
    if response.status_code != 200:
        return f"Error from AI: {response.text}"

    data = response.json()
    return data["choices"][0]["message"]["content"]

# /chat endpoint to process user input
@app.post("/chat")
async def chat(request: Request):
    data = await request.json()
    user_message = data.get("message")
    if not user_message:
        raise HTTPException(status_code=400, detail="No message provided")

    # Get AI response based on memory and current message
    ai_reply = get_ai_response(user_message, memory)

    # Add this interaction to memory for future conversations
    memory.append({
        "user_message": user_message,
        "ai_reply": ai_reply,
        "timestamp": datetime.now().isoformat()
    })

    # Save memory to file for continuous learning
    save_memory(memory)

    return {"reply": ai_reply}

