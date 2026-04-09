# tools.py — Groq wrapper + structured logging
import json
import hashlib
from datetime  import datetime
from groq      import Groq
from config    import GROQ_API_KEY, MODEL_PRIMARY, LOG_PATH

client = Groq(api_key=GROQ_API_KEY)


def call_groq(model: str, system: str, user: str, max_tokens: int = 800) -> str:
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system},
            {"role": "user",   "content": user}
        ],
        max_tokens=max_tokens,
        temperature=0.1,
    )
    return resp.choices[0].message.content.strip()


def call_groq_json(model: str, system: str, user: str,
                   max_tokens: int = 800) -> dict | list:
    raw = call_groq(model, system, user, max_tokens)
    clean = raw
    if "```" in clean:
        parts = clean.split("```")
        clean = parts[1] if len(parts) > 1 else parts[0]
        if clean.startswith("json"):
            clean = clean[4:]
    return json.loads(clean.strip())


def log_decision(agent: str, input_data: dict, output: dict) -> str:
    entry = {
        "ts":     datetime.utcnow().isoformat(),
        "agent":  agent,
        "input":  input_data,
        "output": output,
        "hash":   hashlib.md5(json.dumps(input_data, sort_keys=True)
                              .encode()).hexdigest()[:8]
    }
    with open(LOG_PATH, "a") as f:
        f.write(json.dumps(entry) + "\n")
    return entry["hash"]
