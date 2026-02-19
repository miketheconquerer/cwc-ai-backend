from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
import os
from dotenv import load_dotenv

# Load .env locally
load_dotenv()

# Get your Groq API key from environment
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
if not GROQ_API_KEY:
    raise ValueError("Missing Groq API key! Set it in .env locally or in Render environment variables.")

# Initialize FastAPI
app = FastAPI(title="CWC AI Backend")

# Allow your website to access the API (CORS)
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

# Dummy AI function (replace with your LangChain / Groq call)
def get_ai_response(user_message: str):
    # TODO: replace with real Groq API call
    # Example:
    # import requests
    # headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    # payload = {"model": "llama-3.3-70b-versatile", "messages": [{"role": "user", "content": user_message}]}
    # response = requests.post("https://api.groq.com/openai/v1/chat/completions", json=payload, headers=headers)
    # return response.json()["choices"][0]["message"]["content"]
    
    # Temporary placeholder for deployment testing
    return f"Received your message: {user_message}"

# /chat endpoint
@app.post("/chat")
async def chat(request: Request):
    data = await request.json()
    user_message = data.get("message")
    if not user_message:
        raise HTTPException(status_code=400, detail="No message provided")
    
    reply = get_ai_response(user_message)
    return {"reply": reply}

