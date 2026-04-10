# AI Accounting Migration System

Automated pipeline for migrating financial data from **Reckon Desktop** to **Reckon One** — combining deterministic rules, dual-model LLM inference, semantic memory, and human-in-the-loop review.

> **For users:** See [`User_Manual_AI_Migration_System.pdf`](User_Manual_AI_Migration_System.pdf) — step-by-step guide for accountants and clients.  
> **For architects:** See [`Technical_Document_AI_Migration_System.pdf`](Technical_Document_AI_Migration_System.pdf) — full architecture, data models, database schema, deployment, and security reference.

---

## What it does

| Task | Without this system | With this system |
|---|---|---|
| COA account mapping | 3–5 days of manual spreadsheet work | Minutes — auto-approved for high-confidence accounts |
| Journal anomaly detection | Manual journal-by-journal review | Automated scan of 10+ Reckon-specific issue types |
| Trial balance reconciliation | Spreadsheet tick-off | Automated variance detection + CFO-ready memo |
| Knowledge retention | Lost when team changes | Grows in memory store — available to every future run |

After 3 migration runs on similar clients, typically **70–80% of accounts resolve from memory with zero LLM calls**.

---

## Architecture

```
Input CSVs (Reckon Desktop)
        │
        ▼
┌──────────────────────────────────────────┐
│           LangGraph State Machine        │
│  ┌──────────┐  ┌──────────┐  ┌────────┐ │
│  │ mapping  │→ │ anomaly  │→ │recon   │ │
│  │  node    │  │  node    │  │ node   │ │
│  └──────────┘  └──────────┘  └────────┘ │
│       ↓ (low confidence)                │
│  ┌──────────┐                           │
│  │   hitl   │  (human review via UI)    │
│  │   node   │                           │
│  └──────────┘                           │
└──────────────────────────────────────────┘
        │
        ├── Rules Engine (deterministic, zero LLM cost)
        ├── Memory lookup (SQLite — previously approved mappings)
        ├── RAG context (64-dim embeddings, cosine similarity)
        └── Dual-model LLM (primary 70B + validator 8B in parallel)
        │
        ▼
Output: account_mappings.csv · anomaly_report · TB reconciliation · audit log
```

**Resolution priority** (each level short-circuits the next):
1. **Rules engine** — 27 COA types, 19 tax codes, 30 entity transforms — zero LLM cost
2. **Memory lookup** — SQLite query by `(name, type)` — grows with every run
3. **Dual-model LLM** — primary + validator run in parallel via `asyncio.gather()`
4. **HITL queue** — low-confidence or model-disagreement items sent to human review

---

## Tech stack

| Layer | Technology | Purpose |
|---|---|---|
| Orchestration | LangGraph | State machine, conditional routing, MemorySaver checkpointing |
| LLM inference | Configurable LLM API | Dual-model validation — primary (70B) + validator (8B) |
| Structured output | Instructor | Pydantic-validated LLM responses, auto-retry on bad JSON |
| Semantic memory | Custom RAG (NumPy + SQLite) | 64-dim char embeddings stored as BLOBs — no external vector DB |
| Persistence | SQLite | Mappings, anomaly patterns, run history, RAG entries, audit log |
| Data processing | Pandas | CSV parsing, entity transforms, trial balance arithmetic |
| UI | Streamlit | File upload, pipeline runner, HITL review, results dashboard |
| Runtime | Python 3.12 | |

---

## Project structure

```
accounting_migration_system/
├── streamlit_app.py      # Main UI — entry point for Streamlit Cloud
├── graph.py              # LangGraph state machine (nodes + edges + routing)
├── agents.py             # Mapping, anomaly, and reconcile agent logic (async)
├── rules_engine.py       # Deterministic Reckon transforms (45-sheet specification)
├── memory.py             # SQLite CRUD — mappings, patterns, run history
├── rag_store.py          # Custom RAG — char embeddings, cosine similarity
├── autolearn.py          # Human correction feedback loop → improved prompt rules
├── tools.py              # LLM API wrapper, JSONL audit logger
├── config.py             # API key, model names, thresholds (env-aware)
├── models.py             # Pydantic schemas (AccountMapping, AnomalyReport, etc.)
├── main.py               # CLI entry point (non-Streamlit usage)
├── requirements.txt
├── migration_memory.db   # SQLite DB (auto-created, gitignored)
└── agent_decisions.jsonl # Audit log (auto-created, gitignored)
```

---

## Quick start

### 1. Clone

```bash
git clone https://github.com/sjain26/accounting-migration-system
cd accounting-migration-system
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Set the LLM API key

Create a `.env` file or export directly:

```bash
export GROQ_API_KEY="your-api-key-here"
```

Or for Streamlit Cloud deployment, add to **Settings → Secrets**:

```toml
GROQ_API_KEY = "your-api-key-here"
```

### 4. Run

**Web UI (recommended):**
```bash
streamlit run streamlit_app.py
```

**CLI (headless):**
```bash
python main.py
```

---

## Configuration

All settings in `config.py` — overridable via environment variables or Streamlit secrets:

```python
GROQ_API_KEY    = ""                        # LLM provider API key

MODEL_PRIMARY   = "llama-3.3-70b-versatile" # Primary reasoning model
MODEL_VALIDATE  = "llama-3.1-8b-instant"    # Validator model (dual-check)

HITL_THRESHOLD  = 70    # Confidence below this % → human review queue
AUTO_THRESHOLD  = 85    # Confidence above this % (both models agree) → auto-approve

DB_PATH         = "migration_memory.db"     # SQLite file
LOG_PATH        = "agent_decisions.jsonl"   # Audit log

CUTOFF_DATE     = "2024-03-31"              # Historical / YTD boundary
```

---

## Swapping the LLM provider

The LLM client lives in `tools.py`. Change the `_call()` function to use any provider:

```python
# Current: Groq-compatible client
from groq import Groq
client = Groq(api_key=config.GROQ_API_KEY)

# Swap to Anthropic
from anthropic import Anthropic
client = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

# Swap to OpenAI
from openai import OpenAI
client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
```

Instructor wraps any of the above — the Pydantic validation layer requires no changes.

---

## Adding a new entity type

1. Add `transform_<entity>_record(record)` in `rules_engine.py`
2. Register it in `batch_transform()`'s dispatch dict
3. Add to `ENTITY_OPTIONS` in `streamlit_app.py`
4. Add column hint to `COLUMN_HINTS` dict
5. Optionally add sample rows to `SAMPLE_DATA` dict

---

## Data models

```python
class AccountMapping(BaseModel):
    source_code : str
    source_name : str
    target_code : Optional[str]
    target_name : Optional[str]
    confidence  : int            # 0–100 average of dual models
    reasoning   : str
    status      : Literal["approved", "review", "error"]

class ReconciliationResult(BaseModel):
    overall_status : Literal["PASSED", "REVIEW", "FAILED"]
    risk_level     : Literal["low", "medium", "high"]
    net_variance   : int
    summary        : str         # CFO-ready 2–3 sentence memo
    next_steps     : list[str]
```

---

## Deployment (Streamlit Cloud)

1. Push to the `main` branch of your GitHub repository
2. Connect the repo on [share.streamlit.io](https://share.streamlit.io)
3. Set `GROQ_API_KEY` in **App Settings → Secrets**
4. Entry point: `streamlit_app.py`

Streamlit Cloud auto-deploys on every push to `main`.

---

## License

MIT — free to use and modify for commercial accounting migration projects.
