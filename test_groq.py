
import os
from dotenv import load_dotenv
from groq import Groq

# Load environment variables from .env
load_dotenv()

# Read API key and model
api_key = os.getenv("GROQ_API_KEY")
model = os.getenv("GROQ_MODEL", "openai/gpt-oss-120b")

if not api_key:
    raise ValueError(
        "GROQ_API_KEY not found. Check your .env file."
    )

# Initialize Groq client
client = Groq(api_key=api_key)

print("[GROQ] Sending test request...")

# Send a test prompt
response = client.chat.completions.create(
    model=model,
    messages=[
        {
            "role": "system",
            "content": "You are a technical assistant."
        },
        {
            "role": "user",
            "content": (
                "Explain in simple words why a database "
                "connection failure occurs."
            )
        }
    ],
    max_completion_tokens=512
)

# Display response
answer = response.choices[0].message.content

print("\n========== GROQ API RESPONSE ==========")
print(answer)

print("\n[GROQ] API test completed successfully.")
