# agents.py — Mapping, Anomaly, Reconcile agents using Groqf
# Enhanced with Reckon Desktop → Reckon One specific rules (rules_engine.py)
import asyncio
import json
import time
from groq         import Groq
from groq         import RateLimitError, APIConnectionError, AuthenticationError
from config       import GROQ_API_KEY, MODEL_PRIMARY, MODEL_VALIDATE, HITL_THRESHOLD, AUTO_THRESHOLD
from memory       import (lookup_mapping, save_mapping,
                           save_anomaly_pattern, get_learned_patterns)
from rag_store    import rag
from tools        import log_decision
from rules_engine import (
    transform_coa_record, batch_transform,
    build_reckon_mapping_context, COA_TYPE_MAP
)

client = Groq(api_key=GROQ_API_KEY)

# Pre-built Reckon context injected once (no per-call cost)
_RECKON_CONTEXT = build_reckon_mapping_context()


def _get(d: dict, *keys, default="") -> str:
    """
    Flexible key lookup — handles CSV uploads with different column names.
    e.g. 'name' vs 'Account', 'type' vs 'Account Type', 'code' vs 'Accnt. #'
    """
    for k in keys:
        v = d.get(k)
        if v is not None and str(v).strip() not in ("", "nan", "None"):
            return str(v).strip()
    return default


def _normalize_account(acc: dict) -> dict:
    """
    Normalise an account dict to always have 'name', 'type', 'code' keys,
    regardless of whether it came from default sample data or a CSV upload.
    CSV columns vary: Account/Account Type/Accnt. # vs name/type/code.
    """
    name = _get(acc, "name", "Account", "account_name", "ACCOUNT NAME*")
    typ  = _get(acc, "type", "Account Type", "account_type", "Type", "Account TYPE*")
    code = _get(acc, "code", "Accnt. #", "account_code", "ACCOUNT CODE", "Trans #")
    return {**acc, "name": name, "type": typ, "code": code}


def _call(model: str, system: str, user: str, max_tokens: int = 800) -> dict | list:
    if not GROQ_API_KEY:
        return {}
    resp = None
    for attempt in range(4):
        try:
            resp = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user",   "content": user}
                ],
                max_tokens=max_tokens,
                temperature=0.1,
            )
            break
        except RateLimitError:
            if attempt == 3:
                return {}
            time.sleep(20 * (attempt + 1))
        except (APIConnectionError, AuthenticationError):
            return {}
        except Exception:
            return {}
    if resp is None:
        return {}
    raw = resp.choices[0].message.content.strip()
    if "```" in raw:
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    try:
        return json.loads(raw.strip())
    except Exception:
        return {}


# ── Mapping agent ──────────────────────────────────────────────────────────────
async def mapping_agent_async(source_accounts: list[dict],
                               target_accounts: list[dict],
                               progress_callback=None) -> tuple[list[dict], list[dict]]:
    """
    Returns (results, hitl_queue).
    Strategy:
      1. Apply deterministic Reckon rules (rules_engine) — 0 LLM cost
      2. Memory lookup for previously approved mappings
      3. Only call LLM for accounts that still need reasoning
      4. Dual-model validation for ambiguous cases
    """
    # Normalise both lists so 'name'/'type'/'code' always exist
    source_accounts = [_normalize_account(a) for a in source_accounts]
    target_accounts = [_normalize_account(a) for a in target_accounts]

    target_text = "\n".join(
        f"  {t['code']} | {t['name']} | {t['type']}"
        for t in target_accounts
    )
    results = []
    hitl_q  = []
    loop    = asyncio.get_event_loop()
    total   = len(source_accounts)
    _sem    = asyncio.Semaphore(2)

    # Pre-transform all source accounts with deterministic rules
    transformed_sources = []
    for acc in source_accounts:
        tr = transform_coa_record(acc)
        transformed_sources.append({
            **acc,
            "_reckon_type": tr.get("Account TYPE*", acc["type"]),
            "_clean_name":  tr.get("ACCOUNT NAME*", acc["name"]),
            "_clean_code":  tr.get("ACCOUNT CODE",  acc["code"]),
            "_rules":       tr.get("_rules_applied", []),
        })

    async def map_one(i: int, acc: dict):
        name = acc["name"]
        typ  = acc["type"]
        code = acc["code"]

        if progress_callback:
            progress_callback(i, total, name)

        reckon_type = acc.get("_reckon_type", typ)
        rules_info  = acc.get("_rules", [])

        # ── 1. Direct type match via rules_engine ────────────────────────
        type_key = typ.strip().lower()
        if type_key in COA_TYPE_MAP:
            mapped_type = COA_TYPE_MAP[type_key]
            direct_match = next(
                (t for t in target_accounts
                 if t["type"].lower() == mapped_type.lower()),
                None
            )
            if direct_match:
                entry = {
                    "source_code":  code,
                    "source_name":  name,
                    "target_code":  direct_match["code"],
                    "target_name":  direct_match["name"],
                    "confidence":   95,
                    "reasoning":    f"Direct type match: {typ} → {mapped_type}",
                    "models_agree": True,
                    "source":       "rules",
                    "status":       "approved",
                    "_reckon_type": reckon_type,
                    "_rules":       rules_info,
                }
                save_mapping(name, typ, direct_match["code"], direct_match["name"],
                             95, approved_by="rules_engine")
                rag.add(f"{name} {typ}", {**entry, "approved_by": "rules_engine"})
                log_decision("mapping_agent", acc, entry)
                results.append(entry)
                return

        # ── 2. Memory lookup ─────────────────────────────────────────────
        cached = lookup_mapping(name, typ)
        if cached:
            entry = {
                **cached,
                "source_code":  code,
                "source_name":  name,
                "status":       "approved",
                "source":       "memory",
                "_reckon_type": reckon_type,
            }
            results.append(entry)
            return

        # ── 3. LLM inference (ambiguous cases only) ──────────────────────
        rag_ctx = rag.build_context(f"{name} {typ}")

        system_prompt = (
            "You are an expert Reckon Desktop to Reckon One accounting migration specialist.\n"
            "Respond ONLY with valid JSON. No prose.\n\n"
            f"{_RECKON_CONTEXT}\n"
            f"{rag_ctx}"
        )
        rules_hint = f"\nPre-applied rules: {'; '.join(rules_info)}" if rules_info else ""
        user_prompt = f"""Map this Reckon Desktop account to the best Reckon One target account.

Source: {code} | {name} | {typ}
Reckon One type (pre-mapped): {reckon_type}{rules_hint}

Available targets:
{target_text}

Return JSON:
{{
  "target_code": "<code or null>",
  "target_name": "<name or null>",
  "confidence": <0-100>,
  "reasoning": "<one sentence using Reckon migration rules>"
}}"""

        async with _sem:
            primary_task   = loop.run_in_executor(None, _call, MODEL_PRIMARY,  system_prompt, user_prompt, 400)
            validator_task = loop.run_in_executor(None, _call, MODEL_VALIDATE, system_prompt, user_prompt, 300)
            primary, validator = await asyncio.gather(primary_task, validator_task)

        # Both models returned empty — LLM API unavailable or key missing
        if not primary and not validator:
            entry = {
                "source_code":  code,
                "source_name":  name,
                "target_code":  None,
                "target_name":  None,
                "confidence":   0,
                "reasoning":    "LLM API unavailable — check API key in Streamlit secrets.",
                "models_agree": False,
                "source":       "llm",
                "status":       "error",
                "_reckon_type": reckon_type,
                "_rules":       rules_info,
            }
            hitl_q.append({"type": "mapping", "account": acc, "ai_result": entry})
            results.append(entry)
            return

        agree    = (primary.get("target_code") == validator.get("target_code"))
        avg_conf = (int(primary.get("confidence", 0)) + int(validator.get("confidence", 0))) // 2

        entry = {
            "source_code":  code,
            "source_name":  name,
            "target_code":  primary.get("target_code"),
            "target_name":  primary.get("target_name"),
            "confidence":   avg_conf,
            "reasoning":    primary.get("reasoning", ""),
            "models_agree": agree,
            "source":       "llm",
            "_reckon_type": reckon_type,
            "_rules":       rules_info,
            "status": (
                "approved" if avg_conf >= AUTO_THRESHOLD and agree
                else "review" if avg_conf >= 60
                else "error"
            )
        }

        if avg_conf < HITL_THRESHOLD or not agree:
            hitl_q.append({"type": "mapping", "account": acc, "ai_result": entry})
        elif avg_conf >= AUTO_THRESHOLD and entry["target_code"]:
            save_mapping(name, typ, entry["target_code"], entry["target_name"],
                         avg_conf, approved_by="auto")
            rag.add(f"{name} {typ}", {**entry, "approved_by": "auto"})

        log_decision("mapping_agent", acc, entry)
        results.append(entry)

    tasks = [map_one(i, acc) for i, acc in enumerate(transformed_sources)]
    await asyncio.gather(*tasks)
    return results, hitl_q


# ── Anomaly agent ──────────────────────────────────────────────────────────────
async def anomaly_agent_async(journal_entries: list[dict],
                               cutoff_date: str) -> list[dict]:
    """
    Detects Reckon migration anomalies:
    - Standard: Duplicate, Cutoff risk, Period mismatch, Interco risk
    - Reckon-specific: Inactive account used in transactions, GST rounding,
      multi-currency, invoice >75 lines, qty=0, blank refs, special chars in refs
    """
    learned   = get_learned_patterns()
    learn_txt = ""
    if learned:
        learn_txt = "\nPreviously seen patterns:\n" + "\n".join(
            f"  [{p['severity'].upper()}] {p['desc']}" for p in learned[:8]
        )

    def _je(e: dict, *keys, default="?"):
        """Flexible key lookup for journal entry dicts (CSV vs sample data)."""
        for k in keys:
            v = e.get(k)
            if v is not None and str(v).strip() not in ("", "nan", "None"):
                return str(v).strip()
        return default

    def _je_amt(e: dict) -> str:
        for k in ("amount", "Debit", "Credit", "debit", "credit", "Tax Amount"):
            v = e.get(k)
            if v is not None:
                try:
                    return f"{float(str(v).replace(',','')):.2f}"
                except ValueError:
                    pass
        return "0"

    entries_text = "\n".join(
        f"  {_je(e,'ref','Num','num','Entry No')} | "
        f"{_je(e,'date','Date')} | "
        f"{_je(e,'desc','Description','description')} | "
        f"{_je_amt(e)} | "
        f"{_je(e,'type','Type','Account Type')}"
        for e in journal_entries
    )

    reckon_anomaly_rules = """
RECKON-SPECIFIC ANOMALY TYPES TO CHECK:
• Inactive_account_in_tx: Account marked inactive but used in a transaction during conversion period
• GST_rounding: Tax amount doesn't match expected GST calculation (rounding journal required)
• Multi_currency_unconverted: Foreign currency amounts not converted to base currency
• Invoice_over_75_lines: Invoice has >75 line items (needs consolidation to 1 liner)
• Blank_reference: Invoice/Journal reference is blank (use transaction ID instead)
• Duplicate_ref: Same reference number used for multiple transactions
• Special_chars_in_ref: Reference number contains special chars not accepted by Reckon One
• Due_before_invoice: Due date is earlier than invoice date
• Zero_qty_with_amount: QTY=0 but total amount is non-zero
• Bank_account_in_item: Bank/Credit card/Retained earnings used as item income/expense account
"""

    loop   = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        None, _call, MODEL_PRIMARY,
        f"You are a senior Reckon Desktop to Reckon One migration auditor.\n"
        f"Cutoff date: {cutoff_date}.\n"
        f"Return ONLY a JSON array of anomalies found.\n"
        f"{reckon_anomaly_rules}{learn_txt}",
        f"""Entries (before/on cutoff = historical, after = YTD):
{entries_text}

Each anomaly:
{{
  "ref": "",
  "issue_type": "Duplicate|Cutoff risk|Period mismatch|Interco risk|Inactive_account_in_tx|GST_rounding|Multi_currency_unconverted|Invoice_over_75_lines|Blank_reference|Duplicate_ref|Special_chars_in_ref|Due_before_invoice|Zero_qty_with_amount|Bank_account_in_item|Other",
  "severity": "high|medium|low",
  "finding": "",
  "recommended_action": "",
  "pattern_key": "<short_snake_case>"
}}
Return [] if none found.""",
        1200
    )

    anomalies = result if isinstance(result, list) else []
    for a in anomalies:
        if a.get("pattern_key"):
            save_anomaly_pattern(a["pattern_key"], a.get("finding", ""),
                                 a.get("severity", "low"),
                                 a.get("recommended_action", ""))
        log_decision("anomaly_agent", {"ref": a.get("ref")}, a)
    return anomalies


# ── Reconcile agent ────────────────────────────────────────────────────────────
async def reconcile_agent_async(source_tb: dict, migrated_tb: dict) -> dict:
    """
    CFO-level trial balance reconciliation.
    source_tb can be:
      {code: (name, amount)}   — default format
      {code: amount}           — flat format
    """
    variances = []

    def _parse_tb_val(v):
        """Accept (name, amount) tuple or plain amount."""
        if isinstance(v, (tuple, list)) and len(v) == 2:
            return str(v[0]), _to_float(v[1])
        return "", _to_float(v)

    def _to_float(x):
        try:
            return float(str(x).replace(",", ""))
        except (ValueError, TypeError):
            return 0.0

    for code, val in source_tb.items():
        name, src = _parse_tb_val(val)
        mig  = _to_float(migrated_tb.get(code, 0))
        diff = mig - src
        label = f"{name[:30]:30}" if name else f"{str(code)[:30]:30}"
        variances.append(
            f"  {code} | {label} | Src: {src:,.2f} | Mig: {mig:,.2f} | Diff: {diff:+,.2f}"
        )

    total_src = sum(_parse_tb_val(v)[1] for v in source_tb.values())
    total_mig = sum(_to_float(v) for v in migrated_tb.values())
    net_var   = round(total_mig - total_src, 2)

    loop   = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        None, _call, MODEL_PRIMARY,
        "You are a CFO-level Reckon migration reviewer. Return ONLY valid JSON.\n"
        "Common Reckon variance causes: GST rounding accounts, mandatory account creation "
        "(Item Sales/Item Purchase/Rounding), opening journal differences, multi-currency conversion.",
        f"""Trial balance reconciliation:
{chr(10).join(variances)}

Net variance: {net_var:+,.2f}

Return JSON:
{{
  "overall_status": "PASSED|REVIEW|FAILED",
  "risk_level": "low|medium|high",
  "net_variance": {net_var},
  "summary": "<2-3 sentence CFO-ready memo citing Reckon migration root cause>",
  "next_steps": ["<step1>", "<step2>", "<step3>"]
}}""",
        500
    )
    log_decision("reconcile_agent", {"net_var": net_var}, result)
    return result if isinstance(result, dict) else {
        "overall_status": "REVIEW", "risk_level": "medium",
        "net_variance": net_var, "summary": "Manual review required.",
        "next_steps": ["Review variances manually", "Check GST rounding accounts",
                       "Verify mandatory accounts (Item Sales, Item Purchase, Rounding)"]
    }
