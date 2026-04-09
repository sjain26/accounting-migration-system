# streamlit_app.py — Full Streamlit UI for AI Accounting Migration System
import streamlit as st
import asyncio
import pandas as pd
import json
import io
from datetime import datetime, timezone

# ── Page config (must be first Streamlit call) ──────────────
st.set_page_config(
    page_title="AI Migration System",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Custom CSS ──────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@300;400;500;600&display=swap');

html, body, [class*="css"] { font-family: 'IBM Plex Sans', sans-serif; }
code, .mono { font-family: 'IBM Plex Mono', monospace; }

/* Sidebar */
[data-testid="stSidebar"] {
    background: #0f0f14;
    border-right: 1px solid #1e1e2a;
}
[data-testid="stSidebar"] * { color: #c8c8d0 !important; }
[data-testid="stSidebar"] .stSelectbox label,
[data-testid="stSidebar"] .stTextInput label { color: #888 !important; font-size: 11px !important; }

/* Main bg */
.main { background: #0b0b10; }

/* Cards */
.stat-card {
    background: #13131a;
    border: 1px solid #1e1e2a;
    border-radius: 10px;
    padding: 18px 20px;
    margin-bottom: 10px;
}
.stat-card .label { font-size: 11px; color: #555; letter-spacing: 0.08em; text-transform: uppercase; margin-bottom: 4px; }
.stat-card .value { font-size: 28px; font-weight: 600; font-family: 'IBM Plex Mono'; color: #e8e8f0; }
.stat-card .sub   { font-size: 11px; color: #444; margin-top: 3px; }

/* Status badges */
.badge {
    display: inline-block;
    font-size: 11px;
    font-weight: 500;
    padding: 2px 9px;
    border-radius: 20px;
    font-family: 'IBM Plex Mono';
}
.badge-approved { background: #0a2015; color: #4ade80; border: 1px solid #15803d; }
.badge-review   { background: #1a1505; color: #fbbf24; border: 1px solid #854d0e; }
.badge-error    { background: #180a0a; color: #f87171; border: 1px solid #991b1b; }
.badge-memory   { background: #0a0a20; color: #818cf8; border: 1px solid #3730a3; }

/* Section headings */
.section-head {
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: #444;
    padding: 6px 0 10px;
    border-bottom: 1px solid #1a1a22;
    margin-bottom: 16px;
}

/* Pipeline step */
.pipeline-step {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 8px 0;
    font-size: 13px;
    color: #888;
    border-bottom: 1px solid #111118;
}
.pipeline-step .dot {
    width: 8px; height: 8px;
    border-radius: 50%;
    background: #2a2a38;
    flex-shrink: 0;
}
.pipeline-step.done   .dot { background: #4ade80; }
.pipeline-step.active .dot { background: #fbbf24; box-shadow: 0 0 6px #fbbf24; }
.pipeline-step.done   { color: #4ade80; }

/* Anomaly row */
.anomaly-row {
    background: #100a0a;
    border: 1px solid #1e1010;
    border-left: 3px solid #991b1b;
    border-radius: 6px;
    padding: 10px 14px;
    margin-bottom: 8px;
}
.anomaly-row.medium { border-left-color: #854d0e; background: #100e07; }
.anomaly-row.low    { border-left-color: #1e3a1e; background: #080f08; }
.anomaly-row .ar-ref  { font-family: 'IBM Plex Mono'; font-size: 11px; color: #555; }
.anomaly-row .ar-type { font-size: 12px; font-weight: 500; color: #e8e8f0; margin: 3px 0; }
.anomaly-row .ar-find { font-size: 12px; color: #888; }
.anomaly-row .ar-action { font-size: 11px; color: #4ade80; margin-top: 5px; }

/* HITL card */
.hitl-card {
    background: #121008;
    border: 1px solid #3d2f00;
    border-radius: 10px;
    padding: 16px 20px;
    margin-bottom: 12px;
}
.hitl-card .hc-title { font-size: 13px; font-weight: 500; color: #fbbf24; margin-bottom: 4px; }
.hitl-card .hc-sub   { font-size: 12px; color: #666; }
.hitl-card .hc-ai    { font-family: 'IBM Plex Mono'; font-size: 12px; color: #818cf8; }

/* TB variance */
.var-pass { color: #4ade80; font-family: 'IBM Plex Mono'; }
.var-fail { color: #f87171; font-family: 'IBM Plex Mono'; }

/* Scrollable log */
.log-box {
    background: #0a0a0f;
    border: 1px solid #1a1a22;
    border-radius: 8px;
    padding: 12px 16px;
    font-family: 'IBM Plex Mono';
    font-size: 11px;
    color: #555;
    max-height: 180px;
    overflow-y: auto;
}
.log-box .log-line { color: #4ade80; margin-bottom: 3px; }

/* Recon banner */
.recon-banner {
    border-radius: 10px;
    padding: 20px 24px;
    margin-bottom: 16px;
}
.recon-banner.passed { background: #091a0f; border: 1px solid #15803d; }
.recon-banner.review { background: #14100a; border: 1px solid #854d0e; }
.recon-banner.failed { background: #150808; border: 1px solid #991b1b; }
</style>
""", unsafe_allow_html=True)

# ── Lazy imports (only when API key is set) ─────────────────
@st.cache_resource
def get_memory():
    from memory import get_memory_stats, get_run_history
    return get_memory_stats, get_run_history

# ── Sidebar ──────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 🏦 Migration System")
    st.markdown("---")

    st.markdown("**Pipeline settings**")
    hitl_thresh = st.slider("HITL threshold %", 50, 90, 70,
                             help="Below this confidence → human review")
    auto_thresh = st.slider("Auto-approve threshold %", 70, 99, 85,
                             help="Above this → auto-approve")
    cutoff_date = st.date_input("Cutoff date", value=datetime(2024, 3, 31))

    import config
    config.HITL_THRESHOLD = hitl_thresh
    config.AUTO_THRESHOLD = auto_thresh
    config.CUTOFF_DATE    = str(cutoff_date)

    st.markdown("---")
    st.markdown("**Navigation**")
    page = st.radio("", ["🚀 Run pipeline", "🔄 Entity Transform", "📊 Results",
                         "👤 HITL Review", "🧠 Memory & AutoLearn", "📜 Audit log"],
                    label_visibility="collapsed")

    st.markdown("---")
    st.caption("LangGraph · Groq · Instructor · LlamaIndex · Reckon Rules")


# ── Helper: default sample data (Reckon Desktop style) ─────────────────────
def get_default_source():
    """Sample Reckon Desktop COA accounts."""
    return [
        {"code": "1-1000",   "name": "ANZ Business Cheque",         "type": "Bank"},
        {"code": "1-1100",   "name": "Accounts Receivable",         "type": "Accounts Receivable"},
        {"code": "2-0000",   "name": "Accounts Payable",            "type": "Accounts Payable"},
        {"code": "2-1000",   "name": "GST Payable",                 "type": "Other Current Liability"},
        {"code": "4-0000",   "name": "Sales – domestic operations", "type": "Income"},
        {"code": "5-0000",   "name": "Cost of Goods Sold",          "type": "Cost of Goods Sold"},
        {"code": "6-0000",   "name": "Wages & Salaries",            "type": "Expense"},
        {"code": "6-0100",   "name": "Motor Vehicle Expenses",      "type": "Expense"},
        {"code": "AUG-25 7", "name": "Suspense Account",            "type": "Suspense"},
        {"code": "693.00",   "name": "Loans from/to directors",     "type": "Long Term Liability"},
    ]

def get_default_target():
    """Sample Reckon One COA targets."""
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
        {"ref": "JE-001", "date": "2024-04-01", "desc": "Opening balance transfer",              "amount": 48235000, "type": "OB"},
        {"ref": "JE-002", "date": "2024-03-31", "desc": "Depreciation accrual Q4 FY23",           "amount": 1205000,  "type": "Accrual"},
        {"ref": "JE-003", "date": "2024-03-29", "desc": "Advance – delivery in Apr",              "amount": 21000000, "type": "Revenue"},
        {"ref": "JE-002", "date": "2024-03-31", "desc": "Depreciation accrual Q4 FY23",           "amount": 1205000,  "type": "Accrual"},
        {"ref": "JE-004", "date": "2024-03-31", "desc": "Intercompany recharge FY23",             "amount": 6540000,  "type": "Interco"},
        {"ref": "",       "date": "2024-03-31", "desc": "GST rounding adjustment",                "amount": 12,       "type": "GST"},
        {"ref": "JE-003", "date": "2024-03-20", "desc": "Inactive supplier used in transaction",  "amount": 500000,   "type": "Payable"},
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
        "0001-400010":184_52_28_500,
    }


# ════════════════════════════════════════════════════════════
# HELPERS: Parse ALL DATA CSV into source accounts / journals / TB
# ════════════════════════════════════════════════════════════
def parse_all_data_csv(df: pd.DataFrame):
    """
    Accepts a Reckon Desktop 'ALL DATA' export and returns
    (source_accounts, journal_entries, source_tb).
    """
    # ── Source accounts: unique Account + Account Type rows ──
    acc_df = (
        df[["Account", "Account Type"]]
        .dropna(subset=["Account"])
        .drop_duplicates(subset=["Account"])
        .rename(columns={"Account": "name", "Account Type": "type"})
    )
    acc_df["code"] = acc_df["name"].str[:20].str.strip()
    source_accounts = acc_df[["code", "name", "type"]].to_dict("records")

    # ── Journal entries: only General Journal rows ────────────
    je_df = df[df["Type"] == "General Journal"].copy()
    je_df = je_df.rename(columns={
        "Trans #":     "ref",
        "Date":        "date",
        "Num":         "num",
        "Description": "desc",
        "Account":     "account",
        "Debit":       "debit",
        "Credit":      "credit",
        "Account Type":"account_type",
    })

    def to_num(val):
        if pd.isna(val):
            return 0.0
        try:
            return float(str(val).replace(",", "").strip() or 0)
        except ValueError:
            return 0.0

    je_df["debit"]  = je_df["debit"].apply(to_num)
    je_df["credit"] = je_df["credit"].apply(to_num)
    je_df["amount"] = je_df["debit"] - je_df["credit"]
    je_df["ref"]    = je_df["ref"].apply(lambda x: f"JE-{int(x)}" if pd.notna(x) else "")
    je_df["type"]   = "Journal"
    journal_entries = je_df[["ref", "date", "desc", "account", "amount", "type"]].to_dict("records")

    # ── Trial balance: net balance per account ────────────────
    df2 = df.copy()
    df2["debit_n"]  = df2["Debit"].apply(to_num)
    df2["credit_n"] = df2["Credit"].apply(to_num)
    tb_df = (
        df2.groupby(["Account", "Account Type"])[["debit_n", "credit_n"]]
        .sum()
        .reset_index()
    )
    tb_df["balance"] = tb_df["debit_n"] - tb_df["credit_n"]
    tb_df = tb_df.rename(columns={"Account": "account", "Account Type": "account_type"})
    source_tb = {
        row["account"]: (row["account_type"], int(row["balance"]))
        for _, row in tb_df.iterrows()
    }

    return source_accounts, journal_entries, source_tb


# ════════════════════════════════════════════════════════════
# PAGE: RUN PIPELINE
# ════════════════════════════════════════════════════════════
if page == "🚀 Run pipeline":
    st.markdown('<div class="section-head">Configure & run migration pipeline · Reckon Desktop → Reckon One</div>',
                unsafe_allow_html=True)

    # Show Reckon rules summary
    with st.expander("Active Reckon migration rules (applied before LLM)", expanded=False):
        from rules_engine import build_reckon_mapping_context
        st.code(build_reckon_mapping_context(), language=None)
        st.caption("These deterministic rules are applied BEFORE any LLM call — reducing cost and latency.")

    # ── Upload mode toggle ────────────────────────────────────
    upload_mode = st.radio(
        "Upload mode",
        ["Single file (ALL DATA CSV)", "Multiple files (separate CSVs)"],
        horizontal=True,
        help="Single file: upload one Reckon export CSV — the system will split it automatically. Multiple: upload separate files."
    )

    st.markdown("---")

    # ════════════════════════════════════════════════════════
    # MODE 1 — SINGLE FILE
    # ════════════════════════════════════════════════════════
    if upload_mode == "Single file (ALL DATA CSV)":
        st.markdown("**Upload the full Reckon Desktop export CSV here** (e.g. ALL DATA CSV.csv)")
        single_file = st.file_uploader(
            "ALL DATA CSV", type="csv", key="single_upload",
            label_visibility="collapsed"
        )

        if single_file:
            df_all = pd.read_csv(single_file)
            src_accs, je_list, s_tb = parse_all_data_csv(df_all)

            st.session_state["source_accounts"] = src_accs
            st.session_state["journal_entries"] = je_list
            st.session_state["source_tb"]       = s_tb
            st.session_state["migrated_tb"]      = {k: v[1] for k, v in s_tb.items()}

            c1, c2, c3 = st.columns(3)
            c1.metric("Unique accounts",  len(src_accs))
            c2.metric("Journal entries",  len(je_list))
            c3.metric("TB accounts",      len(s_tb))

            with st.expander("Preview: Source Accounts", expanded=False):
                st.dataframe(pd.DataFrame(src_accs), use_container_width=True, height=200)
            with st.expander("Preview: Journal Entries", expanded=False):
                st.dataframe(pd.DataFrame(je_list), use_container_width=True, height=200)
            with st.expander("Preview: Trial Balance", expanded=False):
                tb_rows = [{"Account": k, "Type": v[0], "Balance": f"{v[1]:,}"} for k, v in s_tb.items()]
                st.dataframe(pd.DataFrame(tb_rows), use_container_width=True, height=200)

            st.success(f"File parsed — {len(src_accs)} accounts · {len(je_list)} journals · {len(s_tb)} TB entries")
        else:
            st.info("Upload a CSV or run the pipeline with sample data (if no file is provided, default sample data will be used).")

        uploaded_tgt = st.file_uploader("Target accounts CSV (optional — Reckon One format)", type="csv", key="tgt_single")
        if uploaded_tgt:
            df_tgt = pd.read_csv(uploaded_tgt)
            st.session_state["target_accounts"] = df_tgt.to_dict("records")
        else:
            st.session_state.setdefault("target_accounts", get_default_target())
            st.caption(f"Using {len(get_default_target())} default Reckon One target accounts")

    # ════════════════════════════════════════════════════════
    # MODE 2 — MULTIPLE FILES
    # ════════════════════════════════════════════════════════
    else:
        tab1, tab2, tab3 = st.tabs(["Source accounts", "Journal entries", "Trial balance"])

        with tab1:
            st.caption("Upload CSV or use sample data")
            uploaded = st.file_uploader("Source accounts CSV", type="csv", key="src_upload")
            if uploaded:
                df_src = pd.read_csv(uploaded)
                st.session_state["source_accounts"] = df_src.to_dict("records")
                st.dataframe(df_src, use_container_width=True)
            else:
                df_src = pd.DataFrame(get_default_source())
                st.session_state["source_accounts"] = get_default_source()
                st.dataframe(df_src, use_container_width=True)

            uploaded_tgt = st.file_uploader("Target accounts CSV", type="csv", key="tgt_upload")
            if uploaded_tgt:
                df_tgt = pd.read_csv(uploaded_tgt)
                st.session_state["target_accounts"] = df_tgt.to_dict("records")
            else:
                st.session_state["target_accounts"] = get_default_target()
                st.caption(f"Using {len(get_default_target())} default target accounts")

        with tab2:
            uploaded_je = st.file_uploader("Journal entries CSV", type="csv", key="je_upload")
            if uploaded_je:
                df_je = pd.read_csv(uploaded_je)
                st.session_state["journal_entries"] = df_je.to_dict("records")
                st.dataframe(df_je, use_container_width=True)
            else:
                df_je = pd.DataFrame(get_default_journals())
                st.session_state["journal_entries"] = get_default_journals()
                st.dataframe(df_je, use_container_width=True)

        with tab3:
            col_src, col_mig = st.columns(2)
            with col_src:
                st.caption("Source trial balance")
                src_tb = get_default_source_tb()
                df_stb = pd.DataFrame([
                    {"Account": code, "Name": name, "Balance (INR)": f"{bal:,}"}
                    for code, (name, bal) in src_tb.items()
                ])
                st.dataframe(df_stb, use_container_width=True)
                st.session_state["source_tb"] = src_tb
            with col_mig:
                st.caption("Migrated trial balance")
                mig_tb = get_default_migrated_tb()
                df_mtb = pd.DataFrame([
                    {"Account": code, "Balance (INR)": f"{bal:,}"}
                    for code, bal in mig_tb.items()
                ])
                st.dataframe(df_mtb, use_container_width=True)
                st.session_state["migrated_tb"] = mig_tb

    st.markdown("---")

    col_run, col_info = st.columns([1, 3])
    with col_run:
        run_btn = st.button("▶ Run pipeline", type="primary",
                            use_container_width=True)
    with col_info:
        st.caption(f"Will run: Mapping agent → Anomaly agent → Reconcile agent  "
                   f"|  HITL threshold: {hitl_thresh}%  |  Auto-approve: {auto_thresh}%")

    if run_btn:
        started = datetime.now(timezone.utc).isoformat()

        # Progress display
        progress_bar = st.progress(0, text="Initialising pipeline...")
        log_placeholder = st.empty()
        logs = []

        def add_log(msg):
            logs.append(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}")
            log_html = "".join(f'<div class="log-line">▶ {l}</div>' for l in logs[-8:])
            log_placeholder.markdown(
                f'<div class="log-box">{log_html}</div>',
                unsafe_allow_html=True
            )

        try:
            add_log("Loading graph...")
            progress_bar.progress(5, "Loading LangGraph...")
            from graph  import build_graph
            from memory import save_run

            graph = build_graph()
            run_id = f"ui-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"

            initial_state = {
                "source_accounts": st.session_state.get("source_accounts", get_default_source()),
                "target_accounts": st.session_state.get("target_accounts", get_default_target()),
                "journal_entries": st.session_state.get("journal_entries", get_default_journals()),
                "source_tb":       st.session_state.get("source_tb", get_default_source_tb()),
                "migrated_tb":     st.session_state.get("migrated_tb", get_default_migrated_tb()),
                "cutoff_date":     str(cutoff_date),
                "mappings":        [],
                "anomalies":       [],
                "recon_result":    {},
                "hitl_queue":      [],
                "errors":          [],
                "run_complete":    False,
                "progress_log":    [],
            }

            add_log("Running mapping agent (dual-model + RAG)...")
            progress_bar.progress(20, "Mapping agent running...")

            config = {"configurable": {"thread_id": run_id}}
            final  = graph.invoke(initial_state, config=config)

            add_log(f"Mapping done: {len(final['mappings'])} accounts")
            progress_bar.progress(60, "Anomaly agent running...")
            add_log(f"Anomaly scan: {len(final['anomalies'])} issues found")
            progress_bar.progress(80, "Reconciliation running...")
            recon = final.get("recon_result", {})
            add_log(f"Reconciliation: {recon.get('overall_status', '?')}")
            progress_bar.progress(100, "Complete!")

            # Store results
            st.session_state["last_results"] = final
            st.session_state["run_id"]       = run_id

            approved = sum(1 for m in final["mappings"] if m.get("status") == "approved")
            review   = sum(1 for m in final["mappings"] if m.get("status") == "review")
            errors   = sum(1 for m in final["mappings"] if m.get("status") == "error")

            save_run(run_id, "complete", len(final["mappings"]),
                     approved, review, errors, len(final["anomalies"]),
                     recon.get("overall_status", "?"), started)

            add_log(f"Pipeline complete — {approved} approved, {review} review, "
                    f"{errors} errors, {len(final['anomalies'])} anomalies")

            st.success(f"Pipeline complete! {approved} approved · {review} review · "
                       f"{errors} errors · {len(final['anomalies'])} anomalies")

            if final.get("hitl_queue"):
                st.warning(f"{len(final['hitl_queue'])} accounts need human review → go to HITL Review tab")

        except Exception as e:
            st.error(f"Pipeline error: {e}")
            st.exception(e)


# ════════════════════════════════════════════════════════════
# PAGE: ENTITY TRANSFORM (Reckon Rules Engine)
# ════════════════════════════════════════════════════════════
elif page == "🔄 Entity Transform":
    st.markdown('<div class="section-head">Reckon Desktop → Reckon One · Deterministic Rules Engine</div>',
                unsafe_allow_html=True)

    from rules_engine import (
        batch_transform, get_mandatory_coa_accounts, get_mandatory_items,
        validate_migration_readiness, MIGRATION_SEQUENCE,
        NOT_AVAILABLE_IN_RECKON_ONE, TAX_CODE_MAP, COA_TYPE_MAP,
        TRANSACTION_TYPE_MAP, JOB_STATUS_MAP, ITEM_SERVICE_TYPES, ITEM_PRODUCT_TYPES
    )

    # ── Migration sequence reference ──────────────────────────
    with st.expander("Migration sequence (from Reckon mapping spreadsheet)", expanded=False):
        seq_df = pd.DataFrame(MIGRATION_SEQUENCE, columns=["Step", "Reckon Desktop", "Reckon One"])
        st.dataframe(seq_df, use_container_width=True, hide_index=True)
        st.caption(f"Not available in Reckon One: {', '.join(NOT_AVAILABLE_IN_RECKON_ONE)}")

    st.markdown("---")

    # ── Entity type selector ──────────────────────────────────
    ENTITY_OPTIONS = {
        # Master data
        "COA (Chart of Accounts)":    "coa",
        "Bank Accounts":              "bank",
        "Tax Codes":                  "tax",
        "Terms (Payment Terms)":      "terms",
        "Customer":                   "customer",
        "Supplier":                   "supplier",
        "Item":                       "item",
        "Project (Customer Job)":     "project",
        "Class":                      "class",
        # Transactions
        "Invoice / Tax Invoice":      "invoice",
        "Credit Memo (Adj. Note)":    "credit_memo",
        "Bill":                       "bill",
        "Bill Credit (Sup. Adj. Note)": "bill_credit",
        "Cheque / CC Charge (Make Payment)": "cheque",
        "Deposit / Receive Money":    "deposit",
        "C Card Credit":              "c_card_credit",
        "Sales Receipt":              "sales_receipt",
        "Transfer":                   "transfer",
        "Payment":                    "payment",
        "Bill Payment":               "bill_payment",
        "Journal Entry":              "journal",
        # Journal-based types
        "Paycheque → Journal":        "paycheque",
        "Item Receipt → Journal":     "item_receipt",
        "Inventory Adjustment → Journal": "inventory_adjustment",
        "Liability Cheque → Journal": "liability_cheque",
        "Liability Adjustment → Journal": "liability_adjustment",
        "YTD Adjustment → Journal":   "ytd_adjustment",
        "C Card Refund → Journal":    "c_card_refund",
        "Statement Charge → Journal": "statement_charge",
        "Build Assembly → Journal":   "build_assembly",
    }

    entity_type = st.selectbox(
        "Select entity type to transform",
        list(ENTITY_OPTIONS.keys()),
        help="Each entity has specific field mapping and transformation rules from the Reckon spreadsheet (45 sheets)"
    )
    etype = ENTITY_OPTIONS[entity_type]

    # ── Upload CSV ────────────────────────────────────────────
    st.markdown(f"**Upload {entity_type} CSV** (Reckon Desktop export format)")
    col_up, col_help = st.columns([2, 1])
    with col_up:
        uploaded_entity = st.file_uploader(
            f"Upload {entity_type} CSV",
            type=["csv"],
            key=f"entity_upload_{etype}",
            label_visibility="collapsed"
        )
    with col_help:
        if etype == "coa":
            st.info("Columns: Account, Type, Accnt. #, Description, Tax Code, Subaccount of (Parent Name), Active Status, Bank No. / Note")
        elif etype == "tax":
            st.info("Columns: Name, Description, Tax Rate, Status, Use this item in sales transactions, Use this item in Purchase transactions")
        elif etype == "customer":
            st.info("Columns: Customer Name, Company Name, Line 1/Street1, City, State, Post Code, Phone, Email, Terms, Contact, Active Status")
        elif etype == "supplier":
            st.info("Columns: Supplier, Company, Street1, City, State, Post Code, Phone, Email, Note, Contact, Account Name, Account Number, Branch Code")
        elif etype == "item":
            st.info("Columns: Item Name/ Number, Type, Sub Item Of, Description On Sales Transactions, Income Account, Expense Account, Sales Price, Cost, Tax Code, Purchase Tax code")
        elif etype == "invoice":
            st.info("Columns: Customer Name/Customer Job, Tax Date, Due Date, Invoice No, Item, Account Code, Description, QTY, Sales Price, Tax, Amount, Amount Include Tax")
        elif etype == "journal":
            st.info("Columns: Date, Entry No, Account, Debit, Credit, Tax Item, Tax Amount, Memo, Name, Amount Include Tax")

    # ── Sample / use uploaded ─────────────────────────────────
    SAMPLE_DATA = {
        "coa": [
            {"Account": "ANZ Business Cheque", "Type": "Bank",                   "Accnt. #": "1-1000",   "Description": "Main operating account",   "Tax Code": "GST",  "Active Status": "Active",   "Subaccount of (Parent Name)": ""},
            {"Account": "Accounts Receivable",  "Type": "Accounts Receivable",   "Accnt. #": "1-1100",   "Description": "",                          "Tax Code": "",     "Active Status": "Active",   "Subaccount of (Parent Name)": ""},
            {"Account": "Accounts Payable",     "Type": "Accounts Payable",      "Accnt. #": "2-0000",   "Description": "",                          "Tax Code": "",     "Active Status": "Active",   "Subaccount of (Parent Name)": ""},
            {"Account": "GST Payable",          "Type": "Other Current Liability","Accnt. #": "2-1000",   "Description": "GST control account",       "Tax Code": "GST",  "Active Status": "Active",   "Subaccount of (Parent Name)": ""},
            {"Account": "Sales – domestic$%",   "Type": "Income",                "Accnt. #": "4-0000",   "Description": "Operating revenue",        "Tax Code": "GST",  "Active Status": "Active",   "Subaccount of (Parent Name)": ""},
            {"Account": "Motor Vehicle Expenses","Type": "Expense",              "Accnt. #": "6-0100",   "Description": "Vehicle running costs",     "Tax Code": "NCG",  "Active Status": "Active",   "Subaccount of (Parent Name)": ""},
            {"Account": "Suspense Account",     "Type": "Suspense",              "Accnt. #": "AUG-25 7", "Description": "Clearing/suspense",        "Tax Code": "",     "Active Status": "Inactive", "Subaccount of (Parent Name)": ""},
            {"Account": "Loans from/to directors","Type": "Long Term Liability", "Accnt. #": "693.00",   "Description": "Director loans",           "Tax Code": "",     "Active Status": "Active",   "Subaccount of (Parent Name)": ""},
        ],
        "tax": [
            {"Name": "GST",   "Description": "10% GST",                "Tax Rate": 10, "Status": "Active"},
            {"Name": "FRE",   "Description": "GST Free Supplies",       "Tax Rate": 0,  "Status": "Active"},
            {"Name": "NCG",   "Description": "Non-Cap. Acq. - Inc GST", "Tax Rate": 10, "Status": "Active"},
            {"Name": "ADJ-P", "Description": "Tax Adjustments",         "Tax Rate": 10, "Status": "Active"},
            {"Name": "N",     "Description": "Non-deductible",          "Tax Rate": 0,  "Status": "Active"},
        ],
        "customer": [
            {"Customer Name": "G & J Woodworks Timber Joinery%%$$", "Company Name": "G & J Woodworks", "Line 1/Street1": "123 Main St", "City": "Sydney", "State": "NSW", "Post Code": "2000", "Phone": "0412345678", "Email": "gj@example.com", "Active Status": "Active"},
            {"Customer Name": "No Name", "Company Name": "", "Active Status": "Active"},
            {"Customer Name": "McDermott Aviations:Sunshine Coast Airport", "Company Name": "McDermott", "Active Status": "Active"},
            {"Customer Name": "Berardos Restaurant", "Company Name": "", "Active Status": "Inactive"},
        ],
        "supplier": [
            {"Supplier": "Aims Medical Group$%^&*", "Company": "Aims Medical", "Street1": "45 Park Rd", "City": "Melbourne", "State": "VIC", "Post Code": "3000", "Phone": "0398765432", "Active Status": "Active"},
            {"Supplier": "ATO Payments", "Company": "Australian Tax Office", "Active Status": "Active", "Account Number": "123456789", "Branch Code": "063-000"},
        ],
        "item": [
            {"Item Name/ Number": "000114$%^&*()", "Type": "Service",            "Income Account": "Sales Revenue", "Expense Account": "Cost of Goods Sold", "Sales Price": 100, "Cost": 60},
            {"Item Name/ Number": "B832083",       "Type": "Inventory",          "Income Account": "Sales Revenue", "Expense Account": "Cost of Goods Sold", "Sales Price": 250, "Cost": 150},
            {"Item Name/ Number": "10% discount",  "Type": "Discount",           "Income Account": "Sales Revenue", "Expense Account": "Cost of Goods Sold", "Sales Price": 0,   "Cost": 0},
            {"Item Name/ Number": "BadItem",       "Type": "Service",            "Income Account": "ANZ Bank",      "Expense Account": "ANZ Bank",           "Sales Price": 50,  "Cost": 30},
        ],
        "invoice": [
            {"Customer Name/Customer Job": "G & J Woodworks", "Tax Date": "2024-03-15", "Due Date": "2024-03-01", "Invoice No": "V214757", "Item": "B832083", "QTY": 2, "Sales Price": 250, "Tax": "GST", "Amount": 550, "Amount Include Tax": "Inclusive"},
            {"Customer Name/Customer Job": "Berardos Restaurant", "Tax Date": "2024-03-20", "Due Date": "2024-04-20", "Invoice No": "", "Item": "000114", "QTY": 0, "Sales Price": 100, "Tax": "GST", "Amount": 100, "txn_id": "TXN-9001"},
            {"Customer Name/Customer Job": "McDermott Aviations", "Tax Date": "2024-03-25", "Due Date": "2024-04-25", "Invoice No": "V214757", "Item": "B832083", "QTY": 1, "Sales Price": 250, "Tax": "GST", "Amount": 275, "txn_id": "TXN-9002"},
        ],
        "journal": [
            {"Date": "2024-03-31", "Entry No": "JE-001", "Account": "Wages & Salaries", "Debit": 5000, "Credit": 0,    "Tax Item": "FRE", "Memo": "March payroll"},
            {"Date": "2024-03-31", "Entry No": "",        "Account": "GST Payable",      "Debit": 0,    "Credit": 500,  "Tax Item": "GST", "Memo": "GST settlement", "txn_id": "TXN-5001"},
            {"Date": "2024-03-31", "Entry No": "JE-001", "Account": "Bank",             "Debit": 0,    "Credit": 5000, "Tax Item": "",    "Memo": "Wages payment", "txn_id": "TXN-5002"},
        ],
    }

    if uploaded_entity:
        records = pd.read_csv(uploaded_entity).to_dict("records")
        st.caption(f"Loaded {len(records)} records from CSV")
    else:
        records = SAMPLE_DATA.get(etype, [])
        st.caption(f"Using {len(records)} sample records — upload CSV to use your own data")

    if not records:
        st.warning("No records to transform.")
        st.stop()

    # ── Run deterministic transform ───────────────────────────
    if st.button("▶ Apply Reckon Rules", type="primary"):
        with st.spinner("Applying deterministic transformation rules..."):
            transformed = batch_transform(records, etype)

            total_rules = sum(
                len(r.get("_rules_applied", [])) for r in transformed
            )
            st.session_state[f"transformed_{etype}"] = transformed

        c1, c2, c3 = st.columns(3)
        c1.metric("Records processed", len(transformed))
        c2.metric("Rule applications", total_rules)
        rules_per_rec = round(total_rules / len(transformed), 1) if transformed else 0
        c3.metric("Avg rules/record", rules_per_rec)

        # COA-specific validation
        if etype == "coa":
            st.markdown("---")
            st.markdown("**Pre-flight validation**")
            issues = validate_migration_readiness(transformed)
            if not issues:
                st.success("All validation checks passed.")
            else:
                for iss in issues:
                    if iss["type"] == "error":
                        st.error(f"[{iss['field']}] {iss['message']}")
                    else:
                        st.warning(f"[{iss['field']}] {iss['message']}")

            st.markdown("---")
            st.markdown("**Mandatory accounts to create first in Reckon One**")
            mand_df = pd.DataFrame(get_mandatory_coa_accounts())
            st.dataframe(mand_df, use_container_width=True, hide_index=True)

    # ── Show results ──────────────────────────────────────────
    key_name = f"transformed_{etype}"
    if key_name in st.session_state:
        transformed = st.session_state[key_name]
        st.markdown("---")
        st.markdown("**Transformation results**")

        t_tab1, t_tab2, t_tab3 = st.tabs(["Transformed data", "Rules applied", "Type mappings reference"])

        with t_tab1:
            # Clean display: drop private keys
            display_rows = []
            for r in transformed:
                row = {k: v for k, v in r.items() if not k.startswith("_")}
                display_rows.append(row)
            df_out = pd.DataFrame(display_rows)
            st.dataframe(df_out, use_container_width=True, height=350)

            csv_buf = io.StringIO()
            df_out.to_csv(csv_buf, index=False)
            fname = f"reckon_one_{etype}_transformed.csv"
            st.download_button(f"⬇ Download {entity_type} CSV (Reckon One format)",
                               csv_buf.getvalue(), fname, "text/csv")

        with t_tab2:
            rules_rows = []
            for i, r in enumerate(transformed):
                name_key = {
                    "coa": "ACCOUNT NAME*", "customer": "Display name*",
                    "supplier": "Display name*", "item": "Item Name*",
                    "invoice": "Reference Code", "journal": "Summary *",
                    "tax": "Tax code name *",
                }.get(etype, "")
                entity_name = r.get(name_key, f"Record {i+1}")
                for rule in r.get("_rules_applied", []):
                    rules_rows.append({"Record": entity_name, "Rule applied": rule})

            if rules_rows:
                df_rules = pd.DataFrame(rules_rows)
                st.dataframe(df_rules, use_container_width=True, height=350)
                st.caption(f"{len(rules_rows)} rule applications across {len(transformed)} records")
            else:
                st.success("No transformations needed — all records passed through cleanly.")

        with t_tab3:
            if etype in ("coa", "coa"):
                st.markdown("**COA Account Type Mapping**")
                type_rows = [{"Reckon Desktop": k.title(), "Reckon One": v} for k, v in COA_TYPE_MAP.items()]
                st.dataframe(pd.DataFrame(type_rows), use_container_width=True, hide_index=True)
            elif etype == "tax":
                st.markdown("**Tax Code Mapping**")
                tax_rows = [
                    {"Desktop Code": k, "Reckon One Code": v["code"],
                     "Description": v["desc"], "Rate": f"{v['rate']*100:.2f}%"}
                    for k, v in TAX_CODE_MAP.items()
                ]
                st.dataframe(pd.DataFrame(tax_rows), use_container_width=True, hide_index=True)
            else:
                st.info("Type mapping reference available for COA and Tax entities.")


# ════════════════════════════════════════════════════════════
# PAGE: RESULTS
# ════════════════════════════════════════════════════════════
elif page == "📊 Results":
    st.markdown('<div class="section-head">Pipeline results</div>',
                unsafe_allow_html=True)

    if "last_results" not in st.session_state:
        st.info("Run the pipeline first to see results here.")
        st.stop()

    final = st.session_state["last_results"]
    maps  = final.get("mappings", [])
    anom  = final.get("anomalies", [])
    recon = final.get("recon_result", {})

    approved   = sum(1 for m in maps if m.get("status") == "approved")
    review     = sum(1 for m in maps if m.get("status") == "review")
    errors     = sum(1 for m in maps if m.get("status") == "error")
    from_mem   = sum(1 for m in maps if m.get("source") == "memory")
    from_rules = sum(1 for m in maps if m.get("source") == "rules")

    # Metric cards
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    for col, label, val, sub in [
        (c1, "Total accounts",  len(maps),   "processed"),
        (c2, "Auto-approved",   approved,    "high confidence"),
        (c3, "Needs review",    review,      "medium confidence"),
        (c4, "Rules-based",     from_rules,  "0 LLM cost"),
        (c5, "From memory",     from_mem,    "RAG cache hit"),
        (c6, "Anomalies",       len(anom),   "detected"),
    ]:
        col.markdown(
            f'<div class="stat-card"><div class="label">{label}</div>'
            f'<div class="value">{val}</div><div class="sub">{sub}</div></div>',
            unsafe_allow_html=True
        )

    st.markdown("---")
    result_tab1, result_tab2, result_tab3 = st.tabs(
        ["Account mappings", "Anomalies", "Trial balance"]
    )

    with result_tab1:
        # Filter
        status_filter = st.multiselect(
            "Filter by status",
            ["approved", "review", "error"],
            default=["approved", "review", "error"]
        )
        filtered = [m for m in maps if m.get("status") in status_filter]

        if filtered:
            df_map = pd.DataFrame([{
                "Source code":       m.get("source_code", ""),
                "Source name":       m.get("source_name", ""),
                "Reckon One type":   m.get("_reckon_type", ""),
                "Target code":       m.get("target_code") or "UNMAPPED",
                "Target name":       m.get("target_name") or "—",
                "Confidence %":      m.get("confidence", 0),
                "Status":            m.get("status", "?"),
                "Source":            m.get("source", "llm"),
                "Rules applied":     "; ".join(m.get("_rules", [])) or "—",
                "Reasoning":         m.get("reasoning", ""),
            } for m in filtered])

            # Colour-code status column
            def highlight_status(val):
                colours = {"approved": "color: #4ade80", "review": "color: #fbbf24",
                           "error": "color: #f87171", "memory": "color: #818cf8"}
                return colours.get(val, "")

            st.dataframe(
                df_map.style.applymap(highlight_status, subset=["Status"]),
                use_container_width=True, height=320
            )

            # Download
            csv_buf = io.StringIO()
            df_map.to_csv(csv_buf, index=False)
            st.download_button("⬇ Download CSV", csv_buf.getvalue(),
                               "account_mappings.csv", "text/csv")

    with result_tab2:
        if not anom:
            st.success("No anomalies detected.")
        else:
            high_anom   = [a for a in anom if a.get("severity") == "high"]
            med_anom    = [a for a in anom if a.get("severity") == "medium"]
            low_anom    = [a for a in anom if a.get("severity") == "low"]

            sev_filter = st.multiselect("Filter severity",
                                         ["high", "medium", "low"],
                                         default=["high", "medium", "low"])
            show_anom = [a for a in anom if a.get("severity") in sev_filter]

            for a in show_anom:
                sev   = a.get("severity", "low")
                cls   = sev if sev in ["medium", "low"] else ""
                st.markdown(f"""
<div class="anomaly-row {cls}">
  <div class="ar-ref">{a.get('ref','')} &nbsp;·&nbsp; {a.get('issue_type','')}</div>
  <div class="ar-type">{a.get('finding','')}</div>
  <div class="ar-action">→ {a.get('recommended_action','')}</div>
</div>""", unsafe_allow_html=True)

    with result_tab3:
        source_tb  = st.session_state.get("source_tb",   get_default_source_tb())
        migrated_tb = st.session_state.get("migrated_tb", get_default_migrated_tb())

        tb_rows = []
        for code, (name, src) in source_tb.items():
            mig  = migrated_tb.get(code, 0)
            diff = mig - src
            tb_rows.append({
                "Account":         code,
                "Name":            name,
                "Source (INR)":    f"{src:,}",
                "Migrated (INR)":  f"{mig:,}",
                "Variance":        f"{diff:+,}",
                "Match":           "✓" if diff == 0 else "✗"
            })

        df_tb = pd.DataFrame(tb_rows)
        st.dataframe(df_tb, use_container_width=True)

        # Recon banner
        status = recon.get("overall_status", "REVIEW")
        cls    = status.lower()
        st.markdown(f"""
<div class="recon-banner {cls}">
  <strong>Status: {status}</strong> &nbsp;·&nbsp; Risk: {recon.get('risk_level','?')}
  &nbsp;·&nbsp; Net variance: INR {recon.get('net_variance', 0):,}<br/><br/>
  {recon.get('summary', '')}<br/><br/>
  <strong>Next steps:</strong><br/>
  {'<br/>'.join(f"  {i+1}. {s}" for i, s in enumerate(recon.get('next_steps', [])))}
</div>""", unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════
# PAGE: HITL REVIEW
# ════════════════════════════════════════════════════════════
elif page == "👤 HITL Review":
    st.markdown('<div class="section-head">Human-in-the-loop review</div>',
                unsafe_allow_html=True)

    final    = st.session_state.get("last_results", {})
    hitl_q   = final.get("hitl_queue", [])
    tgt_accs = st.session_state.get("target_accounts", get_default_target())

    if not hitl_q:
        st.success("No items pending human review. All accounts resolved automatically.")
        st.caption("Items appear here when AI confidence < HITL threshold or models disagree.")
        st.stop()

    st.info(f"{len(hitl_q)} accounts need your review. "
            f"Your decisions are saved to RAG memory and improve future runs.")

    if "hitl_decisions" not in st.session_state:
        st.session_state["hitl_decisions"] = {}

    for idx, item in enumerate(hitl_q):
        acc = item["account"]
        ai  = item["ai_result"]

        with st.expander(
            f"#{idx+1}  {acc['code']} — {acc['name']}  "
            f"(AI conf: {ai.get('confidence', 0)}%)",
            expanded=(idx == 0)
        ):
            col_info, col_action = st.columns([2, 1])

            with col_info:
                st.markdown(f"""
<div class="hitl-card">
  <div class="hc-title">AI suggestion</div>
  <div class="hc-ai">{ai.get('target_code', 'None')} — {ai.get('target_name', '—')}</div>
  <div class="hc-sub" style="margin-top:6px">Confidence: {ai.get('confidence', 0)}% &nbsp;·&nbsp;
  Models agree: {'Yes' if ai.get('models_agree', True) else '<span style="color:#f87171">No</span>'}</div>
  <div class="hc-sub" style="margin-top:4px">{ai.get('reasoning', '')}</div>
</div>""", unsafe_allow_html=True)

            with col_action:
                target_options = [f"{t['code']} — {t['name']}" for t in tgt_accs]
                default_idx = 0
                for i, opt in enumerate(target_options):
                    if ai.get("target_code", "") in opt:
                        default_idx = i
                        break

                chosen = st.selectbox(
                    "Confirm target account",
                    target_options,
                    index=default_idx,
                    key=f"hitl_choice_{idx}"
                )

                if st.button("Approve & save to memory", key=f"hitl_btn_{idx}",
                             type="primary"):
                    from autolearn import accept_correction
                    chosen_code = chosen.split(" — ")[0]
                    chosen_name = chosen.split(" — ")[1]

                    msg = accept_correction(
                        agent          = "mapping_agent",
                        original_input = {"name": acc["name"], "type": acc["type"],
                                          "code": acc["code"]},
                        original_output = ai,
                        correction      = {
                            "target_code": chosen_code,
                            "target_name": chosen_name,
                            "confidence":  99,
                        }
                    )
                    st.session_state["hitl_decisions"][idx] = {
                        "source_code": acc["code"],
                        "target_code": chosen_code,
                        "approved_by": "human"
                    }
                    st.success(msg)
                    st.caption("This decision is now in RAG memory. Same account → auto-resolved next run.")


# ════════════════════════════════════════════════════════════
# PAGE: MEMORY & AUTOLEARN
# ════════════════════════════════════════════════════════════
elif page == "🧠 Memory & AutoLearn":
    st.markdown('<div class="section-head">AutoLearn memory & pattern store</div>',
                unsafe_allow_html=True)

    try:
        from memory import get_memory_stats, get_run_history, get_learned_patterns
        stats    = get_memory_stats()
        patterns = get_learned_patterns()
        history  = get_run_history()

        c1, c2, c3, c4 = st.columns(4)
        for col, label, val, sub in [
            (c1, "Mapped rules",    stats["mappings"],       "in memory"),
            (c2, "Human-approved",  stats["human_approved"], "conf = 99%"),
            (c3, "Anomaly patterns",stats["patterns"],       "auto-labelled"),
            (c4, "Corrections",     stats["feedback"],       "feedback loops"),
        ]:
            col.markdown(
                f'<div class="stat-card"><div class="label">{label}</div>'
                f'<div class="value">{val}</div><div class="sub">{sub}</div></div>',
                unsafe_allow_html=True
            )

        st.markdown("---")

        mem_tab1, mem_tab2, mem_tab3 = st.tabs(
            ["Run history", "Anomaly patterns", "Improved prompt rules"]
        )

        with mem_tab1:
            if history:
                df_hist = pd.DataFrame(history)
                st.dataframe(df_hist, use_container_width=True)
            else:
                st.caption("No runs recorded yet.")

        with mem_tab2:
            if patterns:
                for p in patterns:
                    sev = p["severity"]
                    col = "#f87171" if sev == "high" else "#fbbf24" if sev == "medium" else "#4ade80"
                    st.markdown(
                        f'<div style="background:#111118;border:1px solid #1e1e2a;border-radius:8px;'
                        f'padding:10px 14px;margin-bottom:8px">'
                        f'<span style="color:{col};font-size:11px;font-family:IBM Plex Mono">'
                        f'[{sev.upper()}]</span>&nbsp;&nbsp;'
                        f'<strong style="color:#e8e8f0;font-size:13px">{p["key"]}</strong> '
                        f'<span style="color:#555;font-size:11px">({p["seen"]}x seen)</span><br/>'
                        f'<span style="color:#888;font-size:12px">{p["desc"]}</span><br/>'
                        f'<span style="color:#4ade80;font-size:11px">→ {p["action"]}</span>'
                        f'</div>',
                        unsafe_allow_html=True
                    )
            else:
                st.caption("No patterns stored yet. Run the pipeline to populate.")

        with mem_tab3:
            st.caption("After 50+ human corrections, AI synthesises improved prompt rules.")
            if st.button("Generate improved rules"):
                with st.spinner("Asking Groq to synthesise corrections..."):
                    try:
                        from autolearn import generate_improved_prompt_rules
                        rules = generate_improved_prompt_rules()
                        st.json(rules)
                    except Exception as e:
                        st.error(str(e))

    except Exception as e:
        st.error(f"Memory store not initialised: {e}")
        st.caption("Run the pipeline at least once to create the memory database.")


# ════════════════════════════════════════════════════════════
# PAGE: AUDIT LOG
# ════════════════════════════════════════════════════════════
elif page == "📜 Audit log":
    st.markdown('<div class="section-head">Audit trail — agent_decisions.jsonl</div>',
                unsafe_allow_html=True)

    import os
    from config import LOG_PATH

    if not os.path.exists(LOG_PATH):
        st.info("No audit log yet. Run the pipeline to generate decisions.")
        st.stop()

    with open(LOG_PATH) as f:
        lines = f.readlines()

    st.metric("Total decisions logged", len(lines))

    # Filter
    agent_filter = st.multiselect(
        "Filter by agent",
        ["mapping_agent", "anomaly_agent", "reconcile_agent"],
        default=["mapping_agent", "anomaly_agent", "reconcile_agent"]
    )

    entries = []
    for line in reversed(lines[-200:]):
        try:
            e = json.loads(line)
            if e.get("agent") in agent_filter:
                entries.append({
                    "Timestamp": e.get("ts", "")[:19],
                    "Agent":     e.get("agent", ""),
                    "Hash":      e.get("hash", ""),
                    "Input":     json.dumps(e.get("input", {}))[:60],
                    "Output":    json.dumps(e.get("output", {}))[:80],
                })
        except Exception:
            pass

    if entries:
        df_log = pd.DataFrame(entries)
        st.dataframe(df_log, use_container_width=True, height=400)

        # Download full log
        with open(LOG_PATH) as f:
            full_log = f.read()
        st.download_button("⬇ Download full audit log", full_log,
                           "agent_decisions.jsonl", "application/json")
    else:
        st.caption("No entries match the filter.")
