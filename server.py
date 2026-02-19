
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
import os
from dotenv import load_dotenv
import requests

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

# Configure CORS so your WordPress Elementor website can call this API
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

# Root endpoint
@app.get("/")
def read_root():
    return {"message": "CWC AI Backend is running!"}

# Function to call Groq API
def get_ai_response(user_message: str):
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": "llama-3.3-70b-versatile",
        "messages": [{"role": "user", "content": user_message}]
    }

    response = requests.post(url, json=payload, headers=headers)
    
    if response.status_code != 200:
        return f"Error from AI: {response.text}"

    data = response.json()
    return data["choices"][0]["message"]["content"]

# /chat endpoint
@app.post("/chat")
async def chat(request: Request):
    data = await request.json()
    user_message = data.get("message")
    if not user_message:
        raise HTTPException(status_code=400, detail="No message provided")
    
    reply = get_ai_response(user_message)
    return {"reply": reply}
