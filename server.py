
import requests

def get_ai_response(user_message: str):
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": "llama-3.3-70b-versatile",
        "messages": [
            {"role": "user", "content": user_message}
        ]
    }

    response = requests.post(url, json=payload, headers=headers)
    
    if response.status_code != 200:
        return f"Error from AI: {response.text}"

    data = response.json()
    # Extract AI message from Groq response
    return data["choices"][0]["message"]["content"]
