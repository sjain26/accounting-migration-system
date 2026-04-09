# memory.py — SQLite-backed autolearn store
import sqlite3
import json
from datetime import datetime
from config   import DB_PATH


def init_db():
    con = sqlite3.connect(DB_PATH)
    con.executescript("""
        CREATE TABLE IF NOT EXISTS mapping_rules (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            source_name TEXT,
            source_type TEXT,
            target_code TEXT,
            target_name TEXT,
            confidence  INTEGER,
            approved_by TEXT,
            times_used  INTEGER DEFAULT 1,
            created_at  TEXT
        );
        CREATE TABLE IF NOT EXISTS anomaly_patterns (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            pattern_key TEXT UNIQUE,
            description TEXT,
            severity    TEXT,
            action      TEXT,
            times_seen  INTEGER DEFAULT 1,
            created_at  TEXT
        );
        CREATE TABLE IF NOT EXISTS agent_feedback (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            agent       TEXT,
            input_hash  TEXT,
            correction  TEXT,
            learned_at  TEXT
        );
        CREATE TABLE IF NOT EXISTS run_history (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id       TEXT,
            status       TEXT,
            total        INTEGER,
            approved     INTEGER,
            review_count INTEGER,
            errors       INTEGER,
            anomalies    INTEGER,
            tb_status    TEXT,
            started_at   TEXT,
            finished_at  TEXT
        );
    """)
    con.commit()
    con.close()


def lookup_mapping(source_name: str, source_type: str) -> dict | None:
    con = sqlite3.connect(DB_PATH)
    row = con.execute("""
        SELECT target_code, target_name, confidence, times_used, approved_by
        FROM mapping_rules
        WHERE source_name = ? AND source_type = ? AND confidence >= 85
        ORDER BY times_used DESC LIMIT 1
    """, (source_name, source_type)).fetchone()
    con.close()
    if row:
        return {"target_code": row[0], "target_name": row[1],
                "confidence": row[2], "times_used": row[3],
                "approved_by": row[4], "source": "memory"}
    return None


def save_mapping(source_name, source_type, target_code,
                 target_name, confidence, approved_by="auto"):
    con = sqlite3.connect(DB_PATH)
    existing = con.execute(
        "SELECT id, times_used FROM mapping_rules WHERE source_name=? AND target_code=?",
        (source_name, target_code)
    ).fetchone()
    if existing:
        con.execute(
            "UPDATE mapping_rules SET times_used=?, confidence=? WHERE id=?",
            (existing[1] + 1, max(confidence, existing[1]), existing[0])
        )
    else:
        con.execute("""
            INSERT INTO mapping_rules
            (source_name, source_type, target_code, target_name, confidence, approved_by, created_at)
            VALUES (?,?,?,?,?,?,?)
        """, (source_name, source_type, target_code, target_name,
              confidence, approved_by, datetime.utcnow().isoformat()))
    con.commit()
    con.close()


def save_anomaly_pattern(pattern_key, description, severity, action):
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        INSERT INTO anomaly_patterns (pattern_key, description, severity, action, created_at)
        VALUES (?,?,?,?,?)
        ON CONFLICT(pattern_key) DO UPDATE SET times_seen = times_seen + 1
    """, (pattern_key, description, severity, action, datetime.utcnow().isoformat()))
    con.commit()
    con.close()


def get_learned_patterns() -> list[dict]:
    con = sqlite3.connect(DB_PATH)
    rows = con.execute(
        "SELECT pattern_key, description, severity, action, times_seen "
        "FROM anomaly_patterns ORDER BY times_seen DESC LIMIT 20"
    ).fetchall()
    con.close()
    return [{"key": r[0], "desc": r[1], "severity": r[2],
             "action": r[3], "seen": r[4]} for r in rows]


def save_feedback(agent, input_hash, correction):
    con = sqlite3.connect(DB_PATH)
    con.execute(
        "INSERT INTO agent_feedback (agent, input_hash, correction, learned_at) VALUES (?,?,?,?)",
        (agent, input_hash, correction, datetime.utcnow().isoformat())
    )
    con.commit()
    con.close()


def save_run(run_id, status, total, approved, review_count,
             errors, anomalies, tb_status, started_at):
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        INSERT INTO run_history
        (run_id, status, total, approved, review_count, errors, anomalies, tb_status, started_at, finished_at)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    """, (run_id, status, total, approved, review_count, errors,
          anomalies, tb_status, started_at, datetime.utcnow().isoformat()))
    con.commit()
    con.close()


def get_run_history() -> list[dict]:
    con = sqlite3.connect(DB_PATH)
    rows = con.execute(
        "SELECT run_id, status, total, approved, review_count, errors, anomalies, tb_status, started_at "
        "FROM run_history ORDER BY started_at DESC LIMIT 20"
    ).fetchall()
    con.close()
    return [{"run_id": r[0], "status": r[1], "total": r[2], "approved": r[3],
             "review": r[4], "errors": r[5], "anomalies": r[6],
             "tb_status": r[7], "started_at": r[8]} for r in rows]


def get_memory_stats() -> dict:
    con = sqlite3.connect(DB_PATH)
    mappings  = con.execute("SELECT COUNT(*) FROM mapping_rules").fetchone()[0]
    human     = con.execute("SELECT COUNT(*) FROM mapping_rules WHERE approved_by='human'").fetchone()[0]
    patterns  = con.execute("SELECT COUNT(*) FROM anomaly_patterns").fetchone()[0]
    feedback  = con.execute("SELECT COUNT(*) FROM agent_feedback").fetchone()[0]
    con.close()
    return {"mappings": mappings, "human_approved": human,
            "patterns": patterns, "feedback": feedback}


init_db()
