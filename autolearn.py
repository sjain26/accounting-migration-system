# autolearn.py — feedback loop: corrections -> memory -> better future prompts
import hashlib
import json
from memory    import save_mapping, save_feedback, save_anomaly_pattern, get_learned_patterns
from rag_store import rag
from tools     import call_groq_json
from config    import MODEL_PRIMARY


def accept_correction(agent: str, original_input: dict,
                      original_output: dict, correction: dict) -> str:
    """
    Human reviewer overrides an AI decision.
    Persisted to memory + RAG with conf=99, approved_by='human'.
    Returns a status message.
    """
    input_hash = hashlib.md5(
        json.dumps(original_input, sort_keys=True).encode()
    ).hexdigest()[:8]

    save_feedback(agent, input_hash, json.dumps(correction))

    if agent == "mapping_agent" and "target_code" in correction:
        save_mapping(
            original_input.get("name", ""),
            original_input.get("type", ""),
            correction["target_code"],
            correction.get("target_name", ""),
            confidence=99,
            approved_by="human"
        )
        rag.add(
            f"{original_input.get('name', '')} {original_input.get('type', '')}",
            {**correction, "approved_by": "human", "confidence": 99}
        )
        return (f"Correction saved: {original_input.get('name')} -> "
                f"{correction['target_code']} (conf 99%, human-approved)")

    if agent == "anomaly_agent" and "pattern_key" in correction:
        save_anomaly_pattern(
            correction["pattern_key"],
            correction.get("finding", ""),
            correction.get("severity", "medium"),
            correction.get("recommended_action", "")
        )
        return f"Anomaly pattern saved: {correction['pattern_key']}"

    return "Feedback recorded."


def generate_improved_prompt_rules() -> dict:
    """
    After 50+ corrections, ask Groq to synthesise feedback
    into sharper system-prompt rules for each agent.
    """
    import sqlite3
    from config import DB_PATH

    con          = sqlite3.connect(DB_PATH)
    feedback_rows = con.execute(
        "SELECT agent, correction FROM agent_feedback ORDER BY learned_at DESC LIMIT 50"
    ).fetchall()
    con.close()

    if not feedback_rows:
        return {"message": "Not enough feedback yet (need 50+ corrections)."}

    feedback_text = "\n".join(f"  [{r[0]}] {r[1]}" for r in feedback_rows)
    patterns      = get_learned_patterns()
    patterns_text = "\n".join(f"  [{p['severity']}] {p['desc']}" for p in patterns)

    result = call_groq_json(
        model  = MODEL_PRIMARY,
        system = "You are an AI prompt engineer. Return ONLY valid JSON.",
        user   = f"""Based on human corrections and anomaly patterns,
generate improved system-prompt rules for each agent.

Human corrections:
{feedback_text}

Known anomaly patterns:
{patterns_text}

Return JSON:
{{
  "mapping_agent_rules":   ["<rule1>", "<rule2>", "<rule3>"],
  "anomaly_agent_rules":   ["<rule1>", "<rule2>", "<rule3>"],
  "reconcile_agent_rules": ["<rule1>", "<rule2>"]
}}"""
    )
    return result
