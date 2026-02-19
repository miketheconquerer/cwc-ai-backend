# agent.py
import os
import requests
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("GROQ_API_KEY")

if not API_KEY:
    raise ValueError("Missing GROQ_API_KEY in .env file")

# Simple memory
conversation_history = []

def ask_agent(user_input: str) -> str:
    global conversation_history

    conversation_history.append({
        "role": "user",
        "content": user_input
    })

    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "model": "llama-3.3-70b-versatile",
        "messages": conversation_history
    }

    response = requests.post(url, headers=headers, json=payload)
    data = response.json()

    ai_reply = data["choices"][0]["message"]["content"]

    conversation_history.append({
        "role": "assistant",
        "content": ai_reply
    })

    return ai_reply

