# config.py — Central configuration
import os

# API key: reads from Streamlit secrets (deploy) or env var (local)
try:
    import streamlit as st
    GROQ_API_KEY = st.secrets.get("GROQ_API_KEY", os.environ.get("GROQ_API_KEY", ""))
except Exception:
    GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")

MODEL_PRIMARY   = "llama-3.3-70b-versatile"    # fast, accurate
MODEL_VALIDATE  = "llama-3.1-8b-instant"         # dual-validation model
HITL_THRESHOLD  = 70     # below this % → human review required
AUTO_THRESHOLD  = 85     # above this % → auto-approve
DB_PATH         = "migration_memory.db"
LOG_PATH        = "agent_decisions.jsonl"
CUTOFF_DATE     = "2024-03-31"
