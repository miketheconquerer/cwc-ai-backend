from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from agent import get_ai_response  # your LangChain function

app = FastAPI()

# Allow your website domain
origins = [
    "https://www.chinawestconnector.com",  # replace with your actual domain
    "https://chinawestconnector.com",
    "http://localhost:8000"  # optional for testing locally
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/chat")
async def chat(request: dict):
    user_message = request.get("message")
    reply = get_ai_response(user_message)
    return {"reply": reply}

