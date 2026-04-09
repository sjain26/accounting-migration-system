# streamlit_app.py — AI Accounting Migration System · Redesigned UI
import streamlit as st
import asyncio
import pandas as pd
import json
import io
from datetime import datetime, timezone

# ── Page config ─────────────────────────────────────────────
st.set_page_config(
    page_title="AI Migration System · Reckon",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Global CSS ───────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

html, body, [class*="css"] {
  font-family: 'Inter', sans-serif;
  background: #09090f;
  color: #c9c9d9;
}
.mono { font-family: 'JetBrains Mono', monospace; }

/* ── SIDEBAR ── */
[data-testid="stSidebar"] {
  background: #0d0d16 !important;
  border-right: 1px solid #1a1a28;
}
[data-testid="stSidebar"] * { color: #9090a8 !important; }
[data-testid="stSidebar"] .stRadio label { font-size: 13px !important; }

/* ── MAIN BG ── */
.main, .block-container { background: #09090f !important; }
.block-container { padding-top: 28px !important; padding-bottom: 40px !important; }

/* ── PAGE HEADER ── */
.page-header {
  display: flex; align-items: flex-start; gap: 16px;
  background: linear-gradient(135deg, #0f0e1e 0%, #130f2a 100%);
  border: 1px solid #1e1a38; border-radius: 14px;
  padding: 24px 28px; margin-bottom: 24px;
}
.page-header-icon {
  font-size: 32px; line-height: 1;
  background: rgba(99,102,241,0.12); border: 1px solid rgba(99,102,241,0.2);
  border-radius: 12px; padding: 10px 12px;
}
.page-header-text h1 { font-size: 20px; font-weight: 700; color: #e0e0f0; margin: 0 0 6px; }
.page-header-text p  { font-size: 12.5px; color: #5a5a78; margin: 0; line-height: 1.5; }

/* ── INSTRUCTION BOX ── */
.instr-box {
  background: #0c0c1a; border: 1px solid #1e1e35;
  border-left: 3px solid #6366f1;
  border-radius: 10px; padding: 18px 20px; margin-bottom: 20px;
}
.instr-title {
  font-size: 10px; font-weight: 700; letter-spacing: 0.14em;
  text-transform: uppercase; color: #6366f1; margin-bottom: 12px;
  display: flex; align-items: center; gap: 6px;
}
.instr-steps { list-style: none; padding: 0; margin: 0; }
.instr-steps li {
  display: flex; gap: 12px; align-items: flex-start;
  padding: 7px 0; border-bottom: 1px solid #141422;
  font-size: 12.5px; color: #8080a0; line-height: 1.55;
}
.instr-steps li:last-child { border-bottom: none; }
.instr-step-num {
  min-width: 20px; height: 20px; border-radius: 50%;
  background: rgba(99,102,241,0.15); border: 1px solid rgba(99,102,241,0.3);
  display: flex; align-items: center; justify-content: center;
  font-size: 10px; font-weight: 700; color: #818cf8; flex-shrink: 0;
  margin-top: 1px;
}
.instr-steps li strong { color: #b0b0c8; }

/* ── CALLOUT ── */
.callout {
  border-radius: 8px; padding: 12px 16px; margin: 12px 0;
  font-size: 12.5px; display: flex; gap: 10px;
  align-items: flex-start; line-height: 1.55;
}
.callout-info    { background: #080f1e; border: 1px solid #1e3a5f; color: #60a5fa; }
.callout-success { background: #060f09; border: 1px solid #1a3a22; color: #4ade80; }
.callout-warn    { background: #130f05; border: 1px solid #3a2a0a; color: #fbbf24; }
.callout-error   { background: #130608; border: 1px solid #3a1015; color: #f87171; }

/* ── STAT CARDS ── */
.stat-row { display: flex; gap: 12px; margin-bottom: 20px; flex-wrap: wrap; }
.stat-card {
  flex: 1; min-width: 120px;
  background: #0f0f1c; border: 1px solid #1a1a2c;
  border-radius: 10px; padding: 16px 18px;
}
.stat-label { font-size: 10px; color: #44445a; letter-spacing: 0.1em; text-transform: uppercase; margin-bottom: 6px; }
.stat-value { font-size: 26px; font-weight: 700; font-family: 'JetBrains Mono', monospace; color: #dcdcf0; }
.stat-sub   { font-size: 10.5px; color: #33334a; margin-top: 2px; }

/* ── SECTION DIVIDER ── */
.sdiv {
  font-size: 10px; font-weight: 700; letter-spacing: 0.14em;
  text-transform: uppercase; color: #333348;
  padding: 8px 0 10px; border-bottom: 1px solid #111120;
  margin: 20px 0 16px;
}

/* ── STATUS BADGES ── */
.badge {
  display: inline-block; font-size: 10.5px; font-weight: 600;
  padding: 2px 9px; border-radius: 20px;
  font-family: 'JetBrains Mono', monospace; vertical-align: middle;
}
.badge-approved { background: #071a0d; color: #4ade80; border: 1px solid #14532d; }
.badge-review   { background: #1a1205; color: #fbbf24; border: 1px solid #713f12; }
.badge-error    { background: #170608; color: #f87171; border: 1px solid #7f1d1d; }
.badge-memory   { background: #080820; color: #818cf8; border: 1px solid #312e81; }
.badge-rules    { background: #051209; color: #34d399; border: 1px solid #065f46; }

/* ── ANOMALY CARDS ── */
.anom-card {
  background: #100808; border: 1px solid #1e1010;
  border-left: 3px solid #dc2626; border-radius: 8px;
  padding: 14px 16px; margin-bottom: 10px;
}
.anom-card.medium { background: #100d06; border-left-color: #d97706; }
.anom-card.low    { background: #070f08; border-left-color: #15803d; }
.anom-ref  { font-family: 'JetBrains Mono', monospace; font-size: 10.5px; color: #444466; margin-bottom: 4px; }
.anom-type { font-size: 13px; font-weight: 600; color: #e0e0f0; margin-bottom: 4px; }
.anom-find { font-size: 12px; color: #7070a0; }
.anom-fix  { font-size: 11.5px; color: #4ade80; margin-top: 8px; padding-top: 8px; border-top: 1px solid #1a1a2e; }

/* ── HITL CARD ── */
.hitl-header {
  background: #120f07; border: 1px solid #2a2010;
  border-radius: 10px; padding: 16px 18px; margin-bottom: 8px;
}
.hitl-source { font-size: 12px; color: #555570; margin-bottom: 6px; }
.hitl-ai     { font-family: 'JetBrains Mono', monospace; font-size: 13px; color: #fbbf24; margin-bottom: 4px; }
.hitl-reason { font-size: 12px; color: #666688; margin-top: 6px; line-height: 1.5; }

/* ── PIPELINE LOG ── */
.log-console {
  background: #060609; border: 1px solid #111120;
  border-radius: 8px; padding: 14px 16px; margin: 12px 0;
  font-family: 'JetBrains Mono', monospace; font-size: 11.5px;
  color: #444466; max-height: 200px; overflow-y: auto;
}
.log-line-active { color: #4ade80; }

/* ── RECON BANNER ── */
.recon-banner {
  border-radius: 10px; padding: 20px 24px; margin: 16px 0;
}
.recon-banner.passed { background: #060f09; border: 1px solid #14532d; }
.recon-banner.review { background: #130f05; border: 1px solid #713f12; }
.recon-banner.failed { background: #130608; border: 1px solid #7f1d1d; }
.recon-status { font-size: 15px; font-weight: 700; margin-bottom: 10px; }
.recon-memo   { font-size: 12.5px; color: #7070a0; line-height: 1.65; }
.recon-steps  { margin-top: 12px; padding-left: 0; list-style: none; }
.recon-steps li {
  font-size: 12px; color: #606080; padding: 4px 0;
  display: flex; gap: 8px; align-items: flex-start;
}
.recon-steps li::before {
  content: "→"; color: #6366f1; font-weight: 700; flex-shrink: 0;
}

/* ── WELCOME ── */
.welcome-hero {
  text-align: center; padding: 48px 32px 40px;
  background: linear-gradient(160deg, #0e0c22, #120f2a, #0c1220);
  border: 1px solid #1e1a38; border-radius: 16px; margin-bottom: 28px;
}
.welcome-badge {
  display: inline-block; font-size: 10px; font-weight: 700;
  letter-spacing: 0.18em; text-transform: uppercase;
  color: #818cf8; background: rgba(99,102,241,0.1);
  border: 1px solid rgba(99,102,241,0.25);
  padding: 5px 14px; border-radius: 20px; margin-bottom: 20px;
}
.welcome-title {
  font-size: 36px; font-weight: 700; color: #e8e8ff;
  line-height: 1.15; margin-bottom: 14px;
}
.welcome-title span { color: #818cf8; }
.welcome-desc { font-size: 14px; color: #50507a; max-width: 560px; margin: 0 auto; line-height: 1.7; }

.feature-grid { display: grid; grid-template-columns: repeat(3,1fr); gap: 14px; margin: 28px 0; }
.feature-card {
  background: #0c0c1a; border: 1px solid #181828;
  border-radius: 10px; padding: 18px 20px;
}
.feature-icon { font-size: 22px; margin-bottom: 10px; }
.feature-title { font-size: 13px; font-weight: 600; color: #c0c0e0; margin-bottom: 6px; }
.feature-desc  { font-size: 12px; color: #44445a; line-height: 1.6; }

/* ── PIPELINE PROGRESS STEPS ── */
.pipe-track {
  display: flex; align-items: center; gap: 0;
  background: #0c0c1a; border: 1px solid #181828;
  border-radius: 10px; padding: 16px 20px; margin-bottom: 20px;
}
.pipe-node { display: flex; flex-direction: column; align-items: center; flex: 1; }
.pipe-dot {
  width: 30px; height: 30px; border-radius: 50%;
  background: #1a1a2c; border: 2px solid #2a2a40;
  display: flex; align-items: center; justify-content: center;
  font-size: 11px; font-weight: 700; color: #404060;
}
.pipe-dot.done   { background: rgba(74,222,128,0.1); border-color: #4ade80; color: #4ade80; }
.pipe-dot.active { background: rgba(251,191,36,0.1); border-color: #fbbf24; color: #fbbf24; box-shadow: 0 0 8px rgba(251,191,36,0.3); }
.pipe-label { font-size: 10px; color: #33334a; margin-top: 6px; text-align: center; }
.pipe-label.done   { color: #4ade80; }
.pipe-label.active { color: #fbbf24; }
.pipe-connector { flex: 1; height: 2px; background: #1a1a2c; max-width: 40px; }
.pipe-connector.done { background: #4ade80; }

/* ── UPLOAD ZONE ── */
[data-testid="stFileUploader"] {
  background: #0c0c1a !important; border: 1px dashed #1e1e35 !important;
  border-radius: 10px !important;
}
[data-testid="stFileUploader"] * { color: #50507a !important; }

/* ── BUTTONS ── */
.stButton > button[kind="primary"] {
  background: linear-gradient(135deg, #6366f1, #8b5cf6) !important;
  border: none !important; border-radius: 8px !important;
  font-weight: 600 !important; letter-spacing: 0.02em !important;
  box-shadow: 0 4px 16px rgba(99,102,241,0.25) !important;
}
.stButton > button {
  border-color: #1e1e35 !important; color: #8080a0 !important;
  border-radius: 8px !important; background: #0f0f1c !important;
}

/* ── TABS ── */
[data-testid="stTabs"] [data-baseweb="tab"] {
  font-size: 12.5px !important; color: #50507a !important;
  padding: 8px 18px !important;
}
[data-testid="stTabs"] [aria-selected="true"] { color: #818cf8 !important; }
[data-testid="stTabs"] [data-baseweb="tab-border"] { background: #6366f1 !important; }

/* ── DATAFRAME ── */
[data-testid="stDataFrame"] { border: 1px solid #1a1a28 !important; border-radius: 8px !important; }

/* ── METRICS ── */
[data-testid="stMetric"] { background: #0f0f1c; border: 1px solid #1a1a2c; border-radius: 8px; padding: 12px 16px; }
[data-testid="stMetricLabel"] { font-size: 10px !important; color: #44445a !important; text-transform: uppercase; letter-spacing: 0.08em; }
[data-testid="stMetricValue"] { font-family: 'JetBrains Mono', monospace !important; color: #dcdcf0 !important; }

/* ── EXPANDER ── */
[data-testid="stExpander"] {
  background: #0c0c1a !important; border: 1px solid #181828 !important;
  border-radius: 8px !important;
}

/* ── INPUT / SELECT / SLIDER ── */
[data-testid="stSlider"] * { color: #8080a0 !important; }
.stSelectbox > div > div { background: #0f0f1c !important; border-color: #1e1e35 !important; color: #9090b0 !important; }
.stMultiSelect > div > div { background: #0f0f1c !important; border-color: #1e1e35 !important; }

/* ── ALERTS ── */
[data-testid="stAlert"] { border-radius: 8px !important; }
</style>
""", unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════════
def stat_card(label, value, sub="", color="#dcdcf0"):
    return (f'<div class="stat-card">'
            f'<div class="stat-label">{label}</div>'
            f'<div class="stat-value" style="color:{color}">{value}</div>'
            f'<div class="stat-sub">{sub}</div></div>')

def callout(icon, text, kind="info"):
    return f'<div class="callout callout-{kind}">{icon}&nbsp;&nbsp;{text}</div>'

def badge(status):
    cls = {"approved":"badge-approved","review":"badge-review",
           "error":"badge-error","memory":"badge-memory","rules":"badge-rules"}.get(status,"badge-review")
    return f'<span class="badge {cls}">{status.upper()}</span>'

def page_header(icon, title, desc):
    st.markdown(
        f'<div class="page-header">'
        f'<div class="page-header-icon">{icon}</div>'
        f'<div class="page-header-text"><h1>{title}</h1><p>{desc}</p></div>'
        f'</div>', unsafe_allow_html=True)

def instruction_box(title, steps, tip=None):
    items = "".join(
        f'<li><span class="instr-step-num">{i+1}</span><span>{s}</span></li>'
        for i,s in enumerate(steps)
    )
    tip_html = ""
    if tip:
        tip_html = f'<div class="callout callout-info" style="margin-top:12px;">💡&nbsp;&nbsp;{tip}</div>'
    st.markdown(
        f'<div class="instr-box">'
        f'<div class="instr-title">📋 &nbsp;How to use this page</div>'
        f'<ul class="instr-steps">{items}</ul>'
        f'{tip_html}'
        f'</div>', unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════
# DEFAULT SAMPLE DATA
# ════════════════════════════════════════════════════════════
def get_default_source():
    return [
        {"code": "1-1000",    "name": "ANZ Business Cheque",          "type": "Bank"},
        {"code": "1-1100",    "name": "Accounts Receivable",           "type": "Accounts Receivable"},
        {"code": "2-0000",    "name": "Accounts Payable",              "type": "Accounts Payable"},
        {"code": "2-1000",    "name": "GST Payable",                   "type": "Other Current Liability"},
        {"code": "4-0000",    "name": "Sales – domestic operations",   "type": "Income"},
        {"code": "5-0000",    "name": "Cost of Goods Sold",            "type": "Cost of Goods Sold"},
        {"code": "6-0000",    "name": "Wages & Salaries",              "type": "Expense"},
        {"code": "6-0100",    "name": "Motor Vehicle Expenses",        "type": "Expense"},
        {"code": "AUG-25 7",  "name": "Suspense Account",              "type": "Suspense"},
        {"code": "693.00",    "name": "Loans from/to directors",       "type": "Long Term Liability"},
    ]

def get_default_target():
    return [
        {"code": "1001",  "name": "ANZ Business Cheque",                            "type": "Bank"},
        {"code": "1100",  "name": "Accounts Receivable (A/R)",                      "type": "Accounts Receivable (A/R)"},
        {"code": "2000",  "name": "Accounts Payable (A/P)",                         "type": "Accounts Payable (A/P)"},
        {"code": "2100",  "name": "GST Payable",                                    "type": "Other Current Liablity"},
        {"code": "4000",  "name": "Revenue from operations",                        "type": "Income"},
        {"code": "5000",  "name": "Cost of Goods Sold",                             "type": "Cost of Goods Sold"},
        {"code": "6000",  "name": "Wages & Salaries",                               "type": "Expense"},
        {"code": "6010",  "name": "Motor Vehicle Expenses",                         "type": "Expense"},
        {"code": "9990",  "name": "Non-Current Liability",                          "type": "Non-Current Liability"},
        {"code": "9991",  "name": "Retained Earnings Surplus/(Accumulated Losses)", "type": "Retained Earnings Surplus/(Accumulated Losses)"},
    ]

def get_default_journals():
    return [
        {"ref": "JE-001", "date": "2024-04-01", "desc": "Opening balance transfer",             "amount": 48235000, "type": "OB"},
        {"ref": "JE-002", "date": "2024-03-31", "desc": "Depreciation accrual Q4 FY23",          "amount": 1205000,  "type": "Accrual"},
        {"ref": "JE-003", "date": "2024-03-29", "desc": "Advance – delivery in Apr",             "amount": 21000000, "type": "Revenue"},
        {"ref": "JE-002", "date": "2024-03-31", "desc": "Depreciation accrual Q4 FY23",          "amount": 1205000,  "type": "Accrual"},
        {"ref": "JE-004", "date": "2024-03-31", "desc": "Intercompany recharge FY23",            "amount": 6540000,  "type": "Interco"},
        {"ref": "",       "date": "2024-03-31", "desc": "GST rounding adjustment",               "amount": 12,       "type": "GST"},
        {"ref": "JE-003", "date": "2024-03-20", "desc": "Inactive supplier used in transaction", "amount": 500000,   "type": "Payable"},
    ]

def get_default_source_tb():
    return {
        "0001-100010": ("Cash and bank",     12_45_82_310),
        "0001-110010": ("Trade receivables", 48_72_14_500),
        "0001-200010": ("Trade payables",    22_34_60_000),
        "0001-400010": ("Revenue",          184_52_30_000),
    }

def get_default_migrated_tb():
    return {
        "0001-100010": 12_45_82_310,
        "0001-110010": 48_72_14_500,
        "0001-200010": 22_34_60_000,
        "0001-400010": 184_52_28_500,
    }

def parse_all_data_csv(df: pd.DataFrame):
    acc_df = (df[["Account","Account Type"]].dropna(subset=["Account"])
              .drop_duplicates(subset=["Account"])
              .rename(columns={"Account":"name","Account Type":"type"}))
    acc_df["code"] = acc_df["name"].str[:20].str.strip()
    source_accounts = acc_df[["code","name","type"]].to_dict("records")

    je_df = df[df["Type"]=="General Journal"].copy()
    je_df = je_df.rename(columns={"Trans #":"ref","Date":"date","Num":"num",
                                   "Description":"desc","Account":"account",
                                   "Debit":"debit","Credit":"credit","Account Type":"account_type"})
    def to_num(v):
        try: return float(str(v).replace(",","").strip() or 0)
        except: return 0.0
    je_df["debit"]  = je_df["debit"].apply(to_num)
    je_df["credit"] = je_df["credit"].apply(to_num)
    je_df["amount"] = je_df["debit"] - je_df["credit"]
    je_df["ref"]    = je_df["ref"].apply(lambda x: f"JE-{int(x)}" if pd.notna(x) else "")
    je_df["type"]   = "Journal"
    journal_entries = je_df[["ref","date","desc","account","amount","type"]].to_dict("records")

    df2 = df.copy()
    df2["debit_n"]  = df2["Debit"].apply(to_num)
    df2["credit_n"] = df2["Credit"].apply(to_num)
    tb_df = (df2.groupby(["Account","Account Type"])[["debit_n","credit_n"]].sum().reset_index())
    tb_df["balance"] = tb_df["debit_n"] - tb_df["credit_n"]
    source_tb = {row["Account"]:(row["Account Type"],int(row["balance"])) for _,row in tb_df.iterrows()}
    return source_accounts, journal_entries, source_tb


# ════════════════════════════════════════════════════════════
# SIDEBAR
# ════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("""
    <div style="padding:16px 4px 12px;">
      <div style="font-size:11px;font-weight:700;letter-spacing:0.14em;text-transform:uppercase;color:#4444ff66;margin-bottom:2px;">Reckon</div>
      <div style="font-size:17px;font-weight:700;color:#c0c0e0;">AI Migration</div>
      <div style="font-size:11px;color:#333348;margin-top:2px;">Desktop → One</div>
    </div>
    <div style="border-top:1px solid #1a1a28;margin:4px 0 16px;"></div>
    """, unsafe_allow_html=True)

    st.markdown('<div style="font-size:10px;font-weight:600;letter-spacing:0.12em;text-transform:uppercase;color:#333348;margin-bottom:10px;">⚙ Pipeline Settings</div>', unsafe_allow_html=True)

    hitl_thresh = st.slider("HITL threshold %", 50, 90, 70,
                             help="Mappings below this confidence → sent to human review")
    auto_thresh = st.slider("Auto-approve threshold %", 70, 99, 85,
                             help="Mappings above this (both models agree) → auto-approved")
    cutoff_date = st.date_input("Cutoff date", value=datetime(2024,3,31),
                                 help="Journal entries before this = historical. After = YTD.")

    import config
    config.HITL_THRESHOLD = hitl_thresh
    config.AUTO_THRESHOLD = auto_thresh
    config.CUTOFF_DATE    = str(cutoff_date)

    st.markdown('<div style="border-top:1px solid #1a1a28;margin:16px 0;"></div>', unsafe_allow_html=True)
    st.markdown('<div style="font-size:10px;font-weight:600;letter-spacing:0.12em;text-transform:uppercase;color:#333348;margin-bottom:10px;">🗂 Navigation</div>', unsafe_allow_html=True)

    page = st.radio("", [
        "🏠 Home",
        "🚀 Run Pipeline",
        "🔄 Entity Transform",
        "📊 Results",
        "👤 HITL Review",
        "🧠 Memory & AutoLearn",
        "📜 Audit Log",
    ], label_visibility="collapsed")

    st.markdown('<div style="border-top:1px solid #1a1a28;margin:16px 0 8px;"></div>', unsafe_allow_html=True)

    # Memory quick stats
    try:
        from memory import get_memory_stats
        ms = get_memory_stats()
        st.markdown(f"""
        <div style="font-size:10px;color:#2a2a3a;line-height:2;">
          <div>Mappings in memory: <span style="color:#3a3a5a;">{ms['mappings']}</span></div>
          <div>Human-approved: <span style="color:#3a3a5a;">{ms['human_approved']}</span></div>
          <div>Patterns learned: <span style="color:#3a3a5a;">{ms['patterns']}</span></div>
        </div>""", unsafe_allow_html=True)
    except:
        pass

    st.markdown('<div style="margin-top:16px;font-size:10px;color:#222234;">LangGraph · Groq · RAG · Reckon Rules</div>', unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════
# PAGE: HOME
# ════════════════════════════════════════════════════════════
if page == "🏠 Home":
    st.markdown("""
    <div class="welcome-hero">
      <div class="welcome-badge">Reckon Desktop → Reckon One</div>
      <div class="welcome-title">AI Accounting<br><span>Migration System</span></div>
      <div class="welcome-desc">
        Automate your Chart of Accounts mapping, journal anomaly detection, and
        trial balance reconciliation — powered by deterministic rules + dual-model AI.
      </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="feature-grid">', unsafe_allow_html=True)
    features = [
        ("⚙️", "Rules Engine", "27 COA types, 19 tax codes, 30 entity types — deterministic, zero AI cost."),
        ("🤖", "Dual-Model AI", "Two Groq models validate each other. Disagreements trigger human review."),
        ("🧠", "AutoLearn Memory", "Every approved mapping is stored. Next run resolves same accounts instantly."),
        ("🔍", "Anomaly Detection", "10+ Reckon-specific checks: GST rounding, >75 lines, blank refs, cutoff dates."),
        ("👤", "Human-in-the-Loop", "Low-confidence items surfaced for accountant approval before import."),
        ("📊", "TB Reconciliation", "CFO-level trial balance comparison with variance root-cause analysis."),
    ]
    for icon, title, desc in features:
        st.markdown(
            f'<div class="feature-card"><div class="feature-icon">{icon}</div>'
            f'<div class="feature-title">{title}</div>'
            f'<div class="feature-desc">{desc}</div></div>',
            unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown("---")
    st.markdown('<div class="sdiv">Quick Start Guide</div>', unsafe_allow_html=True)
    cols = st.columns(4)
    steps = [
        ("1", "🚀", "Run Pipeline", "Upload your Reckon Desktop CSV and run the full AI migration pipeline."),
        ("2", "🔄", "Entity Transform", "Transform individual entity types (COA, Invoice, Customer…) separately."),
        ("3", "📊", "Results", "Review account mappings, anomaly report, and reconciliation status."),
        ("4", "👤", "HITL Review", "Approve or override uncertain AI mappings before final import."),
    ]
    for col, (num, icon, title, desc) in zip(cols, steps):
        with col:
            st.markdown(f"""
            <div style="background:#0c0c1a;border:1px solid #181828;border-radius:10px;padding:18px 16px;text-align:center;">
              <div style="font-size:10px;color:#333348;font-weight:700;margin-bottom:8px;">STEP {num}</div>
              <div style="font-size:24px;margin-bottom:10px;">{icon}</div>
              <div style="font-size:13px;font-weight:600;color:#b0b0d0;margin-bottom:6px;">{title}</div>
              <div style="font-size:11.5px;color:#3a3a5a;line-height:1.6;">{desc}</div>
            </div>""", unsafe_allow_html=True)

    st.markdown("---")
    st.markdown(callout("ℹ️", "<strong>First time?</strong> Go to <strong>🚀 Run Pipeline</strong> in the sidebar. No data upload needed — the system includes sample Reckon Desktop data so you can see the full pipeline in action immediately.", "info"), unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════
# PAGE: RUN PIPELINE
# ════════════════════════════════════════════════════════════
elif page == "🚀 Run Pipeline":
    page_header("🚀", "Run Migration Pipeline",
                "Upload your Reckon Desktop export and run the full AI migration pipeline end-to-end.")

    instruction_box("How to use this page", [
        "Choose your <strong>upload mode</strong> — Single file (full Reckon export) or Multiple files (separate CSVs).",
        "Upload your Reckon Desktop CSV file(s). Skip upload to run with built-in sample data.",
        "Optionally upload a <strong>Target accounts CSV</strong> (Reckon One format). Default targets are provided.",
        "Adjust <strong>Pipeline Settings</strong> in the sidebar if needed (HITL threshold, auto-approve %, cutoff date).",
        "Click <strong>▶ Run Pipeline</strong> and watch the live progress log.",
        "After completion, go to <strong>📊 Results</strong> to see mappings, anomalies, and reconciliation.",
    ], tip="No CSV file? Click Run Pipeline anyway — sample Reckon Desktop data is used automatically for demonstration.")

    # Active Reckon rules preview
    with st.expander("📋 View active Reckon migration rules (applied before any AI call)", expanded=False):
        st.markdown(callout("⚙️", "These <strong>deterministic rules</strong> are applied to every record before the LLM is called — reducing cost and ensuring Reckon-specific constraints are always met.", "info"), unsafe_allow_html=True)
        try:
            from rules_engine import build_reckon_mapping_context
            st.code(build_reckon_mapping_context(), language=None)
        except Exception as e:
            st.error(str(e))

    st.markdown('<div class="sdiv">Step 1 — Choose Upload Mode</div>', unsafe_allow_html=True)

    upload_mode = st.radio(
        "Upload mode",
        ["📄 Single file (ALL DATA CSV)", "📁 Multiple files (separate CSVs)"],
        horizontal=True,
        help="Single file: one complete Reckon Desktop export. Multiple: separate files for accounts, journals, TB."
    )

    st.markdown('<div class="sdiv">Step 2 — Upload Data</div>', unsafe_allow_html=True)

    # ── SINGLE FILE MODE ──
    if "Single" in upload_mode:
        st.markdown(callout("📄", "<strong>Expected format:</strong> Standard Reckon Desktop 'ALL DATA' export CSV containing columns: Type, Account, Account Type, Trans #, Date, Num, Description, Debit, Credit.", "info"), unsafe_allow_html=True)

        single_file = st.file_uploader(
            "Upload Reckon Desktop — ALL DATA CSV",
            type="csv", key="single_upload",
            help="Export from Reckon Desktop: Reports → Accountant & Taxes → Transaction Detail by Account → Export to CSV"
        )
        if single_file:
            try:
                df_all = pd.read_csv(single_file)
                src_accs, je_list, s_tb = parse_all_data_csv(df_all)
                st.session_state["source_accounts"] = src_accs
                st.session_state["journal_entries"] = je_list
                st.session_state["source_tb"]       = s_tb
                st.session_state["migrated_tb"]     = {k: v[1] for k, v in s_tb.items()}
                c1, c2, c3 = st.columns(3)
                c1.metric("Unique accounts",  len(src_accs))
                c2.metric("Journal entries",  len(je_list))
                c3.metric("TB accounts",      len(s_tb))
                with st.expander("Preview source accounts"):
                    st.dataframe(pd.DataFrame(src_accs), use_container_width=True, height=200)
                with st.expander("Preview journal entries"):
                    st.dataframe(pd.DataFrame(je_list), use_container_width=True, height=200)
                st.markdown(callout("✅", f"File parsed successfully — <strong>{len(src_accs)} accounts · {len(je_list)} journals · {len(s_tb)} TB entries</strong>", "success"), unsafe_allow_html=True)
            except Exception as e:
                st.markdown(callout("❌", f"Parse error: {e}", "error"), unsafe_allow_html=True)
        else:
            st.session_state.setdefault("source_accounts", get_default_source())
            st.session_state.setdefault("journal_entries", get_default_journals())
            st.session_state.setdefault("source_tb",       get_default_source_tb())
            st.session_state.setdefault("migrated_tb",     get_default_migrated_tb())
            st.markdown(callout("ℹ️", "No file uploaded — using <strong>10 sample Reckon Desktop accounts</strong> with 7 journal entries for demonstration.", "info"), unsafe_allow_html=True)

        st.markdown("**Target accounts (Reckon One)** — optional")
        uploaded_tgt = st.file_uploader(
            "Target accounts CSV (Reckon One format)", type="csv", key="tgt_single",
            help="Columns: code, name, type — representing your Reckon One chart of accounts"
        )
        if uploaded_tgt:
            df_tgt = pd.read_csv(uploaded_tgt)
            st.session_state["target_accounts"] = df_tgt.to_dict("records")
            st.markdown(callout("✅", f"Target accounts loaded: <strong>{len(df_tgt)} accounts</strong>", "success"), unsafe_allow_html=True)
        else:
            st.session_state.setdefault("target_accounts", get_default_target())
            st.caption(f"Using {len(get_default_target())} default Reckon One target accounts.")

    # ── MULTIPLE FILE MODE ──
    else:
        tab1, tab2, tab3 = st.tabs(["📋 Source Accounts", "📓 Journal Entries", "⚖ Trial Balance"])

        with tab1:
            st.markdown(callout("📋", "<strong>Required columns:</strong> name (or Account), type (or Account Type), code (or Accnt. #).", "info"), unsafe_allow_html=True)
            uploaded = st.file_uploader("Source accounts CSV", type="csv", key="src_upload")
            if uploaded:
                df_src = pd.read_csv(uploaded)
                st.session_state["source_accounts"] = df_src.to_dict("records")
                st.dataframe(df_src, use_container_width=True, height=200)
            else:
                df_src = pd.DataFrame(get_default_source())
                st.session_state["source_accounts"] = get_default_source()
                st.caption("Sample data — upload CSV to use your own accounts.")
                st.dataframe(df_src, use_container_width=True, height=200)
            uploaded_tgt2 = st.file_uploader("Target accounts CSV (Reckon One)", type="csv", key="tgt_upload")
            if uploaded_tgt2:
                st.session_state["target_accounts"] = pd.read_csv(uploaded_tgt2).to_dict("records")
            else:
                st.session_state.setdefault("target_accounts", get_default_target())
                st.caption(f"Using {len(get_default_target())} default Reckon One targets.")

        with tab2:
            st.markdown(callout("📓", "<strong>Required columns:</strong> ref, date, desc, amount, type.", "info"), unsafe_allow_html=True)
            uploaded_je = st.file_uploader("Journal entries CSV", type="csv", key="je_upload")
            if uploaded_je:
                df_je = pd.read_csv(uploaded_je)
                st.session_state["journal_entries"] = df_je.to_dict("records")
                st.dataframe(df_je, use_container_width=True, height=200)
            else:
                st.session_state["journal_entries"] = get_default_journals()
                st.caption("Sample data — upload CSV to use your own journals.")
                st.dataframe(pd.DataFrame(get_default_journals()), use_container_width=True, height=200)

        with tab3:
            col_src, col_mig = st.columns(2)
            src_tb  = get_default_source_tb()
            mig_tb  = get_default_migrated_tb()
            st.session_state["source_tb"]   = src_tb
            st.session_state["migrated_tb"] = mig_tb
            with col_src:
                st.caption("Source trial balance")
                st.dataframe(pd.DataFrame([
                    {"Account":code,"Name":name,"Balance":f"{bal:,}"} for code,(name,bal) in src_tb.items()
                ]), use_container_width=True)
            with col_mig:
                st.caption("Migrated trial balance")
                st.dataframe(pd.DataFrame([
                    {"Account":code,"Balance":f"{bal:,}"} for code,bal in mig_tb.items()
                ]), use_container_width=True)

    # ── RUN ──
    st.markdown('<div class="sdiv">Step 3 — Run the Pipeline</div>', unsafe_allow_html=True)

    c_run, c_info = st.columns([1,3])
    with c_run:
        run_btn = st.button("▶  Run Pipeline", type="primary", use_container_width=True)
    with c_info:
        st.markdown(
            f'<div style="padding:10px 0;font-size:12px;color:#333350;">'
            f'HITL threshold: <strong style="color:#fbbf24;">{hitl_thresh}%</strong> &nbsp;·&nbsp; '
            f'Auto-approve: <strong style="color:#4ade80;">{auto_thresh}%</strong> &nbsp;·&nbsp; '
            f'Cutoff: <strong style="color:#818cf8;">{cutoff_date}</strong></div>',
            unsafe_allow_html=True)

    if run_btn:
        started = datetime.now(timezone.utc).isoformat()

        # Pipeline tracker UI
        ph_track = st.empty()
        def render_track(stage):
            stages = ["Mapping","HITL Check","Anomaly","Reconcile","Done"]
            nodes = ""
            for i,s in enumerate(stages):
                idx = i+1
                cls = "done" if idx < stage else ("active" if idx==stage else "")
                sym = "✓" if idx < stage else str(idx)
                nodes += f'<div class="pipe-node"><div class="pipe-dot {cls}">{sym}</div><div class="pipe-label {cls}">{s}</div></div>'
                if i < len(stages)-1:
                    nodes += f'<div class="pipe-connector {"done" if idx<stage else ""}"></div>'
            ph_track.markdown(f'<div class="pipe-track">{nodes}</div>', unsafe_allow_html=True)

        render_track(1)
        pb = st.progress(0, text="Initialising...")
        log_ph = st.empty()
        logs = []

        def log(msg):
            logs.append(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}]  {msg}")
            html = "".join(f'<div class="log-line-active">▶ {l}</div>' for l in logs[-10:])
            log_ph.markdown(f'<div class="log-console">{html}</div>', unsafe_allow_html=True)

        try:
            log("Loading LangGraph pipeline...")
            pb.progress(5, "Loading graph...")
            from graph  import build_graph
            from memory import save_run

            graph  = build_graph()
            run_id = f"ui-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"

            initial_state = {
                "source_accounts": st.session_state.get("source_accounts", get_default_source()),
                "target_accounts": st.session_state.get("target_accounts", get_default_target()),
                "journal_entries": st.session_state.get("journal_entries", get_default_journals()),
                "source_tb":       st.session_state.get("source_tb",       get_default_source_tb()),
                "migrated_tb":     st.session_state.get("migrated_tb",     get_default_migrated_tb()),
                "cutoff_date":     str(cutoff_date),
                "mappings":[], "anomalies":[], "recon_result":{},
                "hitl_queue":[], "errors":[], "run_complete":False, "progress_log":[],
            }

            render_track(1)
            log("Running mapping agent (rules → memory → dual-model AI)...")
            pb.progress(20, "Mapping agent running...")
            cfg   = {"configurable": {"thread_id": run_id}}
            final = graph.invoke(initial_state, config=cfg)

            render_track(2)
            pb.progress(55, "Checking HITL queue...")
            log(f"Mapping complete — {len(final['mappings'])} accounts processed")
            if final.get("hitl_queue"):
                log(f"⚠  {len(final['hitl_queue'])} accounts queued for human review")

            render_track(3)
            pb.progress(70, "Anomaly agent running...")
            log(f"Anomaly scan complete — {len(final['anomalies'])} issues found")

            render_track(4)
            pb.progress(88, "Reconciliation running...")
            recon = final.get("recon_result", {})
            log(f"Reconciliation: {recon.get('overall_status','?')} | variance {recon.get('net_variance',0):,}")

            render_track(5)
            pb.progress(100, "Pipeline complete!")

            st.session_state["last_results"] = final
            st.session_state["run_id"]       = run_id

            approved = sum(1 for m in final["mappings"] if m.get("status")=="approved")
            review   = sum(1 for m in final["mappings"] if m.get("status")=="review")
            errors   = sum(1 for m in final["mappings"] if m.get("status")=="error")

            save_run(run_id,"complete",len(final["mappings"]),approved,review,errors,
                     len(final["anomalies"]),recon.get("overall_status","?"),started)
            log(f"Saved to memory. Run ID: {run_id}")

            st.markdown("---")
            st.markdown(
                f'<div class="stat-row">'
                f'{stat_card("Total accounts", len(final["mappings"]), "processed")}'
                f'{stat_card("Auto-approved",  approved,  "high confidence", "#4ade80")}'
                f'{stat_card("Needs review",   review,    "medium confidence", "#fbbf24")}'
                f'{stat_card("Errors",         errors,    "manual fix needed", "#f87171")}'
                f'{stat_card("Anomalies",      len(final["anomalies"]), "detected", "#818cf8")}'
                f'{stat_card("Recon status",   recon.get("overall_status","?"), "trial balance", "#34d399")}'
                f'</div>', unsafe_allow_html=True)

            if final.get("hitl_queue"):
                st.markdown(callout("⚠️", f'<strong>{len(final["hitl_queue"])} accounts need human review.</strong> Go to <strong>👤 HITL Review</strong> in the sidebar before proceeding to import.', "warn"), unsafe_allow_html=True)
            else:
                st.markdown(callout("✅", "All accounts resolved automatically. Go to <strong>📊 Results</strong> to review the full output.", "success"), unsafe_allow_html=True)

        except Exception as e:
            st.markdown(callout("❌", f"Pipeline error: {e}", "error"), unsafe_allow_html=True)
            st.exception(e)


# ════════════════════════════════════════════════════════════
# PAGE: ENTITY TRANSFORM
# ════════════════════════════════════════════════════════════
elif page == "🔄 Entity Transform":
    page_header("🔄", "Entity Transform",
                "Apply deterministic Reckon rules to individual entity types — no AI, instant results.")

    instruction_box("How to use this page", [
        "Select the <strong>entity type</strong> you want to transform from the dropdown (e.g. COA, Invoice, Customer).",
        "Read the <strong>column hint</strong> that appears — it shows required CSV column names.",
        "Upload your Reckon Desktop CSV for that entity type, or use sample data to preview.",
        "Click <strong>▶ Apply Reckon Rules</strong> to run all deterministic transformations instantly.",
        "Review results in 3 tabs: <strong>Transformed data</strong>, <strong>Rules applied</strong>, <strong>Type mappings reference</strong>.",
        "Download the transformed CSV — it is ready to import into Reckon One.",
    ], tip="COA transformation also runs pre-flight validation and shows the 3 mandatory accounts you must create first in Reckon One.")

    from rules_engine import (
        batch_transform, get_mandatory_coa_accounts, get_mandatory_items,
        validate_migration_readiness, MIGRATION_SEQUENCE,
        NOT_AVAILABLE_IN_RECKON_ONE, TAX_CODE_MAP, COA_TYPE_MAP,
        TRANSACTION_TYPE_MAP, JOB_STATUS_MAP, ITEM_SERVICE_TYPES, ITEM_PRODUCT_TYPES
    )

    with st.expander("📋 Reckon One migration sequence (from mapping spreadsheet)"):
        seq_df = pd.DataFrame(MIGRATION_SEQUENCE, columns=["Step","Reckon Desktop","Reckon One"])
        st.dataframe(seq_df, use_container_width=True, hide_index=True)
        st.caption(f"Not available in Reckon One: {', '.join(NOT_AVAILABLE_IN_RECKON_ONE)}")

    st.markdown('<div class="sdiv">Step 1 — Select Entity Type</div>', unsafe_allow_html=True)

    ENTITY_OPTIONS = {
        "── Master Data ──":          None,
        "COA (Chart of Accounts)":    "coa",
        "Bank Accounts":              "bank",
        "Tax Codes":                  "tax",
        "Terms (Payment Terms)":      "terms",
        "Customer":                   "customer",
        "Supplier":                   "supplier",
        "Item":                       "item",
        "Project (Customer Job)":     "project",
        "Class":                      "class",
        "── Transactions ──":         None,
        "Invoice / Tax Invoice":      "invoice",
        "Credit Memo (Adj. Note)":    "credit_memo",
        "Bill":                       "bill",
        "Bill Credit (Sup. Adj. Note)":"bill_credit",
        "Cheque / CC Charge":         "cheque",
        "Deposit / Receive Money":    "deposit",
        "C Card Credit":              "c_card_credit",
        "Sales Receipt":              "sales_receipt",
        "Transfer":                   "transfer",
        "Payment":                    "payment",
        "Bill Payment":               "bill_payment",
        "Journal Entry":              "journal",
        "── Journal-Based Types ──":  None,
        "Paycheque → Journal":        "paycheque",
        "Item Receipt → Journal":     "item_receipt",
        "Inventory Adjustment → Journal":"inventory_adjustment",
        "Liability Cheque → Journal": "liability_cheque",
        "Liability Adjustment → Journal":"liability_adjustment",
        "YTD Adjustment → Journal":   "ytd_adjustment",
        "C Card Refund → Journal":    "c_card_refund",
        "Statement Charge → Journal": "statement_charge",
        "Build Assembly → Journal":   "build_assembly",
    }

    valid_options = [k for k,v in ENTITY_OPTIONS.items() if v is not None and not k.startswith("──")]
    entity_type = st.selectbox("Select entity type", valid_options,
                                help="Each entity type has specific Reckon mapping rules from the 45-sheet specification.")
    etype = ENTITY_OPTIONS.get(entity_type)

    st.markdown('<div class="sdiv">Step 2 — Upload CSV</div>', unsafe_allow_html=True)

    COLUMN_HINTS = {
        "coa":      "Account · Account Type · Accnt. # · Description · Tax Code · Active Status",
        "tax":      "Name · Description · Tax Rate · Status",
        "customer": "Customer Name · Company Name · Line 1/Street1 · City · State · Post Code · Phone · Email · Active Status",
        "supplier": "Supplier · Company · Street1 · City · State · Post Code · Phone · Email · Account Number",
        "item":     "Item Name/ Number · Type · Income Account · Expense Account · Sales Price · Cost · Tax Code",
        "invoice":  "Customer Name/Customer Job · Tax Date · Due Date · Invoice No · Item · QTY · Sales Price · Tax · Amount",
        "journal":  "Date · Entry No · Account · Debit · Credit · Tax Item · Tax Amount · Memo",
    }

    col_up, col_hint = st.columns([2,1])
    with col_up:
        uploaded_entity = st.file_uploader(
            f"Upload {entity_type} CSV (Reckon Desktop export format)",
            type=["csv"], key=f"entity_{etype}"
        )
    with col_hint:
        hint = COLUMN_HINTS.get(etype, "Upload your Reckon Desktop CSV for this entity.")
        st.markdown(
            f'<div class="instr-box" style="height:100%;margin-bottom:0;">'
            f'<div class="instr-title">📋 Expected columns</div>'
            f'<div style="font-size:11.5px;color:#44445a;line-height:1.8;">{hint}</div>'
            f'</div>', unsafe_allow_html=True)

    SAMPLE_DATA = {
        "coa": [
            {"Account":"ANZ Business Cheque","Type":"Bank","Accnt. #":"1-1000","Description":"Main account","Tax Code":"GST","Active Status":"Active","Subaccount of (Parent Name)":""},
            {"Account":"Accounts Receivable","Type":"Accounts Receivable","Accnt. #":"1-1100","Description":"","Tax Code":"","Active Status":"Active","Subaccount of (Parent Name)":""},
            {"Account":"GST Payable","Type":"Other Current Liability","Accnt. #":"2-1000","Description":"GST control","Tax Code":"GST","Active Status":"Active","Subaccount of (Parent Name)":""},
            {"Account":"Sales – domestic$%","Type":"Income","Accnt. #":"4-0000","Description":"Revenue","Tax Code":"GST","Active Status":"Active","Subaccount of (Parent Name)":""},
            {"Account":"Suspense Account","Type":"Suspense","Accnt. #":"AUG-25 7","Description":"Clearing","Tax Code":"","Active Status":"Inactive","Subaccount of (Parent Name)":""},
            {"Account":"Loans from/to directors","Type":"Long Term Liability","Accnt. #":"693.00","Description":"Director loans","Tax Code":"","Active Status":"Active","Subaccount of (Parent Name)":""},
        ],
        "tax": [
            {"Name":"GST","Description":"10% GST","Tax Rate":10,"Status":"Active"},
            {"Name":"FRE","Description":"GST Free","Tax Rate":0,"Status":"Active"},
            {"Name":"ADJ-P","Description":"Tax Adjustments","Tax Rate":10,"Status":"Active"},
            {"Name":"N","Description":"Non-deductible","Tax Rate":0,"Status":"Active"},
        ],
        "customer": [
            {"Customer Name":"G & J Woodworks%%$$","Company Name":"G & J Woodworks","Line 1/Street1":"123 Main St","City":"Sydney","State":"NSW","Post Code":"2000","Phone":"0412345678","Email":"gj@example.com","Active Status":"Active"},
            {"Customer Name":"No Name","Company Name":"","Active Status":"Active"},
            {"Customer Name":"McDermott Aviations:Sunshine Coast","Company Name":"McDermott","Active Status":"Active"},
        ],
        "invoice": [
            {"Customer Name/Customer Job":"G & J Woodworks","Tax Date":"2024-03-15","Due Date":"2024-03-01","Invoice No":"V214757","Item":"B832083","QTY":2,"Sales Price":250,"Tax":"GST","Amount":550,"Amount Include Tax":"Inclusive"},
            {"Customer Name/Customer Job":"Berardos","Tax Date":"2024-03-20","Due Date":"2024-04-20","Invoice No":"","Item":"000114","QTY":0,"Sales Price":100,"Tax":"GST","Amount":100,"txn_id":"TXN-9001"},
        ],
        "journal": [
            {"Date":"2024-03-31","Entry No":"JE-001","Account":"Wages & Salaries","Debit":5000,"Credit":0,"Tax Item":"FRE","Memo":"March payroll"},
            {"Date":"2024-03-31","Entry No":"","Account":"GST Payable","Debit":0,"Credit":500,"Tax Item":"GST","Memo":"GST settlement","txn_id":"TXN-5001"},
        ],
    }

    if uploaded_entity:
        records = pd.read_csv(uploaded_entity).to_dict("records")
        st.markdown(callout("✅", f"Loaded <strong>{len(records)} records</strong> from CSV.", "success"), unsafe_allow_html=True)
    else:
        records = SAMPLE_DATA.get(etype, [])
        if records:
            st.markdown(callout("ℹ️", f"Using <strong>{len(records)} sample records</strong> — upload your CSV above to use your real data.", "info"), unsafe_allow_html=True)
            with st.expander("Preview sample data"):
                st.dataframe(pd.DataFrame(records), use_container_width=True, height=150)
        else:
            st.markdown(callout("ℹ️", "Upload a CSV to transform this entity type.", "info"), unsafe_allow_html=True)
            records = []

    st.markdown('<div class="sdiv">Step 3 — Apply Rules & Review Results</div>', unsafe_allow_html=True)

    if not records:
        st.markdown(callout("⚠️", "No records to transform. Upload a CSV file above.", "warn"), unsafe_allow_html=True)
    elif st.button("▶  Apply Reckon Rules", type="primary"):
        with st.spinner("Applying deterministic transformation rules..."):
            transformed = batch_transform(records, etype)
            total_rules = sum(len(r.get("_rules_applied",[])) for r in transformed)
            st.session_state[f"transformed_{etype}"] = transformed

        c1,c2,c3 = st.columns(3)
        c1.metric("Records processed", len(transformed))
        c2.metric("Rule applications", total_rules)
        c3.metric("Avg rules / record", round(total_rules/max(len(transformed),1),1))

        if etype == "coa":
            issues = validate_migration_readiness(transformed)
            if not issues:
                st.markdown(callout("✅", "All pre-flight validation checks passed.", "success"), unsafe_allow_html=True)
            else:
                for iss in issues:
                    kind = "error" if iss["type"]=="error" else "warn"
                    st.markdown(callout("⚠️" if kind=="warn" else "❌", f'<strong>[{iss["field"]}]</strong> {iss["message"]}', kind), unsafe_allow_html=True)

            st.markdown('<div class="sdiv">Mandatory accounts — create these first in Reckon One</div>', unsafe_allow_html=True)
            st.markdown(callout("ℹ️", "These 3 accounts must exist in Reckon One <strong>before</strong> importing any data.", "info"), unsafe_allow_html=True)
            st.dataframe(pd.DataFrame(get_mandatory_coa_accounts()), use_container_width=True, hide_index=True)

    key_name = f"transformed_{etype}"
    if key_name in st.session_state:
        transformed = st.session_state[key_name]
        st.markdown("---")
        t1, t2, t3 = st.tabs(["📤 Transformed Data", "📋 Rules Applied", "🗂 Type Mappings Reference"])

        with t1:
            display_rows = [{k:v for k,v in r.items() if not k.startswith("_")} for r in transformed]
            df_out = pd.DataFrame(display_rows)
            st.dataframe(df_out, use_container_width=True, height=320)
            buf = io.StringIO()
            df_out.to_csv(buf, index=False)
            st.download_button(f"⬇ Download {entity_type} CSV (Reckon One format)",
                               buf.getvalue(), f"reckon_one_{etype}.csv", "text/csv")
            st.markdown(callout("✅", "This CSV is in <strong>Reckon One import format</strong>. Download and import directly.", "success"), unsafe_allow_html=True)

        with t2:
            rules_rows = []
            for i,r in enumerate(transformed):
                name_key = {"coa":"ACCOUNT NAME*","customer":"Display name*","supplier":"Display name*",
                            "item":"Item Name*","invoice":"Reference Code","journal":"Summary *","tax":"Tax code name *"}.get(etype,"")
                nm = r.get(name_key, f"Record {i+1}")
                for rule in r.get("_rules_applied",[]):
                    rules_rows.append({"Record":nm,"Rule applied":rule})
            if rules_rows:
                st.dataframe(pd.DataFrame(rules_rows), use_container_width=True, height=320)
                st.caption(f"{len(rules_rows)} rule applications across {len(transformed)} records")
            else:
                st.markdown(callout("✅", "No transformations needed — all records passed through cleanly.", "success"), unsafe_allow_html=True)

        with t3:
            if etype == "coa":
                st.markdown("**COA Account Type Mapping**")
                st.dataframe(pd.DataFrame([{"Reckon Desktop":k.title(),"Reckon One":v} for k,v in COA_TYPE_MAP.items()]),
                             use_container_width=True, hide_index=True)
            elif etype == "tax":
                st.markdown("**Tax Code Mapping**")
                st.dataframe(pd.DataFrame([
                    {"Desktop Code":k,"Reckon One Code":v["code"],"Description":v["desc"],"Rate":f"{v['rate']*100:.2f}%"}
                    for k,v in TAX_CODE_MAP.items()
                ]), use_container_width=True, hide_index=True)
            else:
                st.markdown(callout("ℹ️", "Detailed type mapping reference available for COA and Tax entity types.", "info"), unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════
# PAGE: RESULTS
# ════════════════════════════════════════════════════════════
elif page == "📊 Results":
    page_header("📊", "Migration Results",
                "Review account mappings, detected anomalies, and trial balance reconciliation.")

    if "last_results" not in st.session_state:
        st.markdown(callout("ℹ️", "No results yet. Go to <strong>🚀 Run Pipeline</strong> and run the migration pipeline first.", "info"), unsafe_allow_html=True)
        st.stop()

    instruction_box("How to read this page", [
        "<strong>Account Mappings tab</strong> — every source account and its mapped Reckon One target with confidence score.",
        "<strong>Anomalies tab</strong> — issues found in journal entries. Fix HIGH severity issues before importing.",
        "<strong>Trial Balance tab</strong> — source vs migrated TB comparison. PASSED = ready to import.",
        "Use the <strong>filter dropdowns</strong> to focus on specific statuses or severities.",
        "Download buttons let you export each section as CSV for your records.",
    ])

    final = st.session_state["last_results"]
    maps  = final.get("mappings",  [])
    anom  = final.get("anomalies", [])
    recon = final.get("recon_result", {})

    approved   = sum(1 for m in maps if m.get("status")=="approved")
    review     = sum(1 for m in maps if m.get("status")=="review")
    errors     = sum(1 for m in maps if m.get("status")=="error")
    from_rules = sum(1 for m in maps if m.get("source")=="rules")
    from_mem   = sum(1 for m in maps if m.get("source")=="memory")

    st.markdown(
        f'<div class="stat-row">'
        f'{stat_card("Total", len(maps), "accounts")}'
        f'{stat_card("Approved", approved, "auto-resolved", "#4ade80")}'
        f'{stat_card("Review", review, "check needed", "#fbbf24")}'
        f'{stat_card("Errors", errors, "manual fix", "#f87171")}'
        f'{stat_card("Rules-based", from_rules, "0 LLM cost", "#34d399")}'
        f'{stat_card("From memory", from_mem, "cache hit", "#818cf8")}'
        f'{stat_card("Anomalies", len(anom), "detected", "#60a5fa")}'
        f'</div>', unsafe_allow_html=True)

    tab_map, tab_anom, tab_tb = st.tabs(["🗺 Account Mappings", "🔍 Anomalies", "⚖ Trial Balance"])

    with tab_map:
        col_f1, col_f2 = st.columns([2,1])
        with col_f1:
            status_filter = st.multiselect("Filter by status", ["approved","review","error"],
                                           default=["approved","review","error"])
        with col_f2:
            source_filter = st.multiselect("Filter by source", ["rules","memory","llm"],
                                           default=["rules","memory","llm"])
        filtered = [m for m in maps if m.get("status") in status_filter and m.get("source","llm") in source_filter]
        if filtered:
            df_map = pd.DataFrame([{
                "Source code":     m.get("source_code",""),
                "Source name":     m.get("source_name",""),
                "Reckon One type": m.get("_reckon_type",""),
                "Target code":     m.get("target_code") or "UNMAPPED",
                "Target name":     m.get("target_name") or "—",
                "Confidence %":    m.get("confidence",0),
                "Status":          m.get("status","?"),
                "Resolved by":     m.get("source","llm"),
                "Rules applied":   "; ".join(m.get("_rules",[])) or "—",
                "Reasoning":       m.get("reasoning",""),
            } for m in filtered])

            def hl(val):
                return {"approved":"color:#4ade80","review":"color:#fbbf24","error":"color:#f87171"}.get(val,"")

            st.dataframe(df_map.style.applymap(hl, subset=["Status"]),
                         use_container_width=True, height=340)
            buf = io.StringIO(); df_map.to_csv(buf,index=False)
            st.download_button("⬇ Download account mappings CSV", buf.getvalue(), "account_mappings.csv","text/csv")
        else:
            st.markdown(callout("ℹ️", "No accounts match the selected filters.", "info"), unsafe_allow_html=True)

    with tab_anom:
        if not anom:
            st.markdown(callout("✅", "No anomalies detected in the journal entries.", "success"), unsafe_allow_html=True)
        else:
            high = [a for a in anom if a.get("severity")=="high"]
            med  = [a for a in anom if a.get("severity")=="medium"]
            low  = [a for a in anom if a.get("severity")=="low"]

            c1,c2,c3 = st.columns(3)
            c1.metric("High severity", len(high))
            c2.metric("Medium severity", len(med))
            c3.metric("Low severity",  len(low))

            if high:
                st.markdown(callout("❌", f"<strong>{len(high)} HIGH severity issue(s)</strong> must be fixed before importing to Reckon One.", "error"), unsafe_allow_html=True)

            sev_f = st.multiselect("Filter severity",["high","medium","low"],default=["high","medium","low"])
            for a in [x for x in anom if x.get("severity") in sev_f]:
                sev = a.get("severity","low")
                cls = "medium" if sev=="medium" else ("low" if sev=="low" else "")
                sev_badge = {"high":"#f87171","medium":"#fbbf24","low":"#34d399"}.get(sev,"#888")
                st.markdown(f"""
                <div class="anom-card {cls}">
                  <div class="anom-ref">{a.get('ref','')} &nbsp;·&nbsp; {a.get('issue_type','')} &nbsp;·&nbsp; <span style="color:{sev_badge};font-weight:700;">{sev.upper()}</span></div>
                  <div class="anom-type">{a.get('finding','')}</div>
                  <div class="anom-fix">→ {a.get('recommended_action','')}</div>
                </div>""", unsafe_allow_html=True)

    with tab_tb:
        source_tb   = st.session_state.get("source_tb",   get_default_source_tb())
        migrated_tb = st.session_state.get("migrated_tb", get_default_migrated_tb())

        tb_rows = []
        for code,(name,src) in source_tb.items():
            mig  = migrated_tb.get(code,0)
            diff = mig - src
            tb_rows.append({
                "Account":code, "Name":name,
                "Source":f"{src:,}", "Migrated":f"{mig:,}",
                "Variance":f"{diff:+,}", "Match":"✓" if diff==0 else "✗"
            })
        st.dataframe(pd.DataFrame(tb_rows), use_container_width=True)

        status = recon.get("overall_status","REVIEW")
        cls    = status.lower()
        col_icon = {"PASSED":"✅","REVIEW":"⚠️","FAILED":"❌"}.get(status,"⚠️")
        col_clr  = {"PASSED":"#4ade80","REVIEW":"#fbbf24","FAILED":"#f87171"}.get(status,"#fbbf24")
        next_steps = "".join(f'<li>{s}</li>' for s in recon.get("next_steps",[]))
        st.markdown(f"""
        <div class="recon-banner {cls}">
          <div class="recon-status">{col_icon} &nbsp;Status: <span style="color:{col_clr};">{status}</span>
            &nbsp;·&nbsp; Risk: {recon.get('risk_level','?')}
            &nbsp;·&nbsp; Net variance: {recon.get('net_variance',0):,}
          </div>
          <div class="recon-memo">{recon.get('summary','')}</div>
          <ul class="recon-steps">{next_steps}</ul>
        </div>""", unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════
# PAGE: HITL REVIEW
# ════════════════════════════════════════════════════════════
elif page == "👤 HITL Review":
    page_header("👤", "Human-in-the-Loop Review",
                "Review AI-uncertain mappings and approve or override before final import.")

    instruction_box("How to use this page", [
        "Each card shows a <strong>source account</strong> the AI was not confident about.",
        "Read the <strong>AI suggestion</strong> — target account, confidence score, and reasoning.",
        "If the suggestion is <strong>correct</strong> — select it from the dropdown and click <strong>Approve & Save</strong>.",
        "If <strong>wrong</strong> — select the correct Reckon One account from the dropdown, then click <strong>Approve & Save</strong>.",
        "Every decision is <strong>saved to memory</strong>. Same account type next migration → auto-resolved, no review needed.",
    ], tip="Complete all HITL reviews before importing data into Reckon One. Unreviewed accounts remain in REVIEW status.")

    final    = st.session_state.get("last_results", {})
    hitl_q   = final.get("hitl_queue", [])
    tgt_accs = st.session_state.get("target_accounts", get_default_target())

    if not hitl_q:
        st.markdown(callout("✅", "No items pending human review. All accounts were resolved automatically by the rules engine or AI with high confidence.", "success"), unsafe_allow_html=True)
        st.markdown(callout("ℹ️", "If you run the pipeline again with new data, uncertain accounts will appear here.", "info"), unsafe_allow_html=True)
        st.stop()

    st.markdown(callout("⚠️", f"<strong>{len(hitl_q)} account(s) need your review.</strong> These were flagged because AI confidence was below {hitl_thresh}% or the two AI models disagreed.", "warn"), unsafe_allow_html=True)

    if "hitl_decisions" not in st.session_state:
        st.session_state["hitl_decisions"] = {}

    done = len(st.session_state["hitl_decisions"])
    st.progress(done / max(len(hitl_q),1), text=f"{done} of {len(hitl_q)} reviewed")

    for idx, item in enumerate(hitl_q):
        acc = item["account"]
        ai  = item["ai_result"]
        done_this = idx in st.session_state["hitl_decisions"]

        label = f"{'✅' if done_this else '⏳'}  #{idx+1}  {acc['code']} — {acc['name']}  (AI confidence: {ai.get('confidence',0)}%)"
        with st.expander(label, expanded=(idx==0 and not done_this)):
            c_left, c_right = st.columns([3,2])

            with c_left:
                agree_text = "✓ Both models agree" if ai.get("models_agree",True) else "✗ Models disagreed"
                agree_color = "#4ade80" if ai.get("models_agree",True) else "#f87171"
                st.markdown(f"""
                <div class="hitl-header">
                  <div class="hitl-source">Source: <strong style="color:#9090c0;">{acc.get('code','')} — {acc.get('name','')} — {acc.get('type','')}</strong></div>
                  <div style="font-size:10px;color:#333348;margin-bottom:8px;text-transform:uppercase;letter-spacing:0.1em;">AI Suggestion</div>
                  <div class="hitl-ai">{ai.get('target_code','—')} &nbsp;→&nbsp; {ai.get('target_name','—')}</div>
                  <div style="font-size:11.5px;color:#555570;margin-top:6px;">
                    Confidence: <strong style="color:#fbbf24;">{ai.get('confidence',0)}%</strong>
                    &nbsp;·&nbsp; <span style="color:{agree_color};">{agree_text}</span>
                  </div>
                  <div class="hitl-reason">{ai.get('reasoning','')}</div>
                </div>""", unsafe_allow_html=True)

            with c_right:
                target_options = [f"{t['code']} — {t['name']}" for t in tgt_accs]
                default_idx = next((i for i,opt in enumerate(target_options) if ai.get("target_code","") in opt), 0)
                chosen = st.selectbox("Select correct Reckon One account",
                                      target_options, index=default_idx,
                                      key=f"hitl_choice_{idx}",
                                      help="Select the correct Reckon One target account for this mapping.")
                if st.button("✓ Approve & Save to Memory", key=f"hitl_btn_{idx}", type="primary",
                             use_container_width=True, disabled=done_this):
                    from autolearn import accept_correction
                    chosen_code = chosen.split(" — ")[0]
                    chosen_name = chosen.split(" — ")[1]
                    msg = accept_correction(
                        agent="mapping_agent",
                        original_input={"name":acc["name"],"type":acc["type"],"code":acc["code"]},
                        original_output=ai,
                        correction={"target_code":chosen_code,"target_name":chosen_name,"confidence":99}
                    )
                    st.session_state["hitl_decisions"][idx] = {
                        "source_code":acc["code"],"target_code":chosen_code,"approved_by":"human"}
                    st.markdown(callout("✅", f"Saved. <strong>{acc['name']} → {chosen_name}</strong> added to memory — auto-resolved in future runs.", "success"), unsafe_allow_html=True)

    if len(st.session_state["hitl_decisions"]) == len(hitl_q):
        st.markdown(callout("✅", "All HITL items reviewed. Go to <strong>📊 Results</strong> for the final report.", "success"), unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════
# PAGE: MEMORY & AUTOLEARN
# ════════════════════════════════════════════════════════════
elif page == "🧠 Memory & AutoLearn":
    page_header("🧠", "Memory & AutoLearn",
                "The system's accumulated knowledge from all past migration runs.")

    instruction_box("How to use this page", [
        "<strong>Run history</strong> tab — see every past pipeline run with its results and reconciliation status.",
        "<strong>Anomaly patterns</strong> tab — learned patterns from past anomaly detections, used to improve future scans.",
        "<strong>Improved rules</strong> tab — after 50+ human corrections, click to generate AI-synthesised improved rules.",
        "Memory grows automatically with every run and every HITL approval. No action required.",
    ], tip="The more migrations you run through this system, the fewer LLM calls are needed — reducing cost and improving speed.")

    try:
        from memory import get_memory_stats, get_run_history, get_learned_patterns
        stats    = get_memory_stats()
        patterns = get_learned_patterns()
        history  = get_run_history()

        st.markdown(
            f'<div class="stat-row">'
            f'{stat_card("Mappings stored", stats["mappings"], "total in memory")}'
            f'{stat_card("Human-approved", stats["human_approved"], "HITL decisions", "#4ade80")}'
            f'{stat_card("Anomaly patterns", stats["patterns"], "auto-labelled", "#818cf8")}'
            f'{stat_card("Corrections", stats["feedback"], "feedback loops", "#fbbf24")}'
            f'</div>', unsafe_allow_html=True)

        mt1, mt2, mt3 = st.tabs(["📅 Run History", "🔍 Anomaly Patterns", "🤖 Improved Rules"])

        with mt1:
            if history:
                df_hist = pd.DataFrame(history)
                st.dataframe(df_hist, use_container_width=True, height=300)
            else:
                st.markdown(callout("ℹ️", "No run history yet. Run the pipeline to populate.", "info"), unsafe_allow_html=True)

        with mt2:
            if patterns:
                for p in patterns[:20]:
                    sev_color = {"high":"#f87171","medium":"#fbbf24","low":"#34d399"}.get(p.get("severity","low"),"#888")
                    st.markdown(f"""
                    <div style="background:#0c0c1a;border:1px solid #181828;border-radius:8px;padding:12px 16px;margin-bottom:8px;">
                      <div style="display:flex;justify-content:space-between;margin-bottom:4px;">
                        <span style="font-family:'JetBrains Mono',monospace;font-size:11px;color:#444466;">{p.get('pattern_key','')}</span>
                        <span style="font-size:10.5px;font-weight:700;color:{sev_color};">{p.get('severity','').upper()}</span>
                      </div>
                      <div style="font-size:12.5px;color:#8080a0;">{p.get('description','')}</div>
                    </div>""", unsafe_allow_html=True)
            else:
                st.markdown(callout("ℹ️", "No anomaly patterns stored yet. Run the pipeline to populate.", "info"), unsafe_allow_html=True)

        with mt3:
            st.markdown(callout("ℹ️", "After <strong>50+ human corrections</strong>, the AI synthesises improved prompt rules from your feedback. Click below to generate.", "info"), unsafe_allow_html=True)
            if st.button("🤖 Generate Improved Rules", type="primary"):
                with st.spinner("Asking AI to synthesise corrections into rules..."):
                    try:
                        from autolearn import generate_improved_prompt_rules
                        rules = generate_improved_prompt_rules()
                        st.json(rules)
                    except Exception as e:
                        st.markdown(callout("⚠️", f"{e}", "warn"), unsafe_allow_html=True)

    except Exception as e:
        st.markdown(callout("❌", f"Memory store not initialised: {e} — Run the pipeline at least once.", "error"), unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════
# PAGE: AUDIT LOG
# ════════════════════════════════════════════════════════════
elif page == "📜 Audit Log":
    page_header("📜", "Audit Log",
                "Complete trail of every AI decision made during the pipeline — for compliance and review.")

    instruction_box("How to use this page", [
        "Every mapping, anomaly detection, and reconciliation decision is logged here automatically.",
        "Use the <strong>agent filter</strong> to narrow down to specific pipeline stages.",
        "Click <strong>⬇ Download full audit log</strong> to export as JSONL for external audit tools.",
        "Log entries show: timestamp, agent, input data, and AI output with reasoning.",
    ])

    import os
    from config import LOG_PATH

    if not os.path.exists(LOG_PATH):
        st.markdown(callout("ℹ️", "No audit log yet. Run the pipeline to generate decision records.", "info"), unsafe_allow_html=True)
        st.stop()

    with open(LOG_PATH) as f:
        lines = f.readlines()

    c1,c2,c3 = st.columns(3)
    c1.metric("Total decisions logged", len(lines))
    mapping_count = sum(1 for l in lines if "mapping_agent" in l)
    anomaly_count = sum(1 for l in lines if "anomaly_agent" in l)
    c2.metric("Mapping decisions", mapping_count)
    c3.metric("Anomaly decisions", anomaly_count)

    agent_filter = st.multiselect("Filter by agent",
                                  ["mapping_agent","anomaly_agent","reconcile_agent"],
                                  default=["mapping_agent","anomaly_agent","reconcile_agent"])

    entries = []
    for line in reversed(lines[-200:]):
        try:
            e = json.loads(line)
            if e.get("agent") in agent_filter:
                entries.append({
                    "Timestamp": e.get("ts","")[:19],
                    "Agent":     e.get("agent",""),
                    "Input":     json.dumps(e.get("input",{}))[:60],
                    "Output":    json.dumps(e.get("output",{}))[:90],
                })
        except: pass

    if entries:
        st.dataframe(pd.DataFrame(entries), use_container_width=True, height=400)
        with open(LOG_PATH) as f:
            full_log = f.read()
        st.download_button("⬇ Download full audit log", full_log, "agent_decisions.jsonl","application/json")
    else:
        st.markdown(callout("ℹ️", "No entries match the selected filters.", "info"), unsafe_allow_html=True)
