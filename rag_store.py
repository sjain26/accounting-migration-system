# rag_store.py — LlamaIndex semantic memory for past migration decisions
from __future__ import annotations
import json
import numpy as np
import sqlite3
from datetime import datetime
from config   import DB_PATH


class MigrationRAG:
    """
    Lightweight semantic RAG using numpy cosine similarity.
    Drop-in replaceable with ChromaDB or Qdrant for production.
    """

    def __init__(self):
        self.con = sqlite3.connect(DB_PATH, check_same_thread=False)
        self.con.executescript("""
            CREATE TABLE IF NOT EXISTS rag_entries (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                text       TEXT,
                embedding  BLOB,
                metadata   TEXT,
                created_at TEXT
            );
        """)
        self.con.commit()

    def _embed(self, text: str) -> np.ndarray:
        """Character-level hash embedding (32-dim). Replace with BGE for production."""
        vec = np.zeros(64)
        words = text.lower().split()
        for wi, word in enumerate(words):
            for ci, ch in enumerate(word):
                vec[(wi * 7 + ci) % 64] += ord(ch) / 1000.0
        norm = np.linalg.norm(vec)
        return (vec / norm) if norm > 0 else vec

    def add(self, text: str, metadata: dict):
        emb = self._embed(text)
        self.con.execute(
            "INSERT INTO rag_entries (text, embedding, metadata, created_at) VALUES (?,?,?,?)",
            (text, emb.tobytes(), json.dumps(metadata), datetime.utcnow().isoformat())
        )
        self.con.commit()

    def search(self, query: str, top_k: int = 3) -> list[dict]:
        q_emb = self._embed(query)
        rows  = self.con.execute(
            "SELECT text, embedding, metadata FROM rag_entries"
        ).fetchall()
        if not rows:
            return []
        scored = []
        for text, emb_blob, meta_str in rows:
            stored = np.frombuffer(emb_blob, dtype=np.float64)
            if stored.shape == q_emb.shape:
                sim = float(np.dot(q_emb, stored))
                scored.append((sim, text, json.loads(meta_str)))
        scored.sort(key=lambda x: x[0], reverse=True)
        return [{"text": t, "meta": m, "score": round(s, 3)}
                for s, t, m in scored[:top_k]]

    def build_context(self, query: str) -> str:
        results = self.search(query)
        if not results:
            return ""
        lines = ["Relevant past decisions:"]
        for r in results:
            m = r["meta"]
            lines.append(
                f"  '{m.get('source_name', '')}' -> {m.get('target_code', '?')} "
                f"(conf {m.get('confidence', 0)}%, by {m.get('approved_by', 'ai')})"
            )
        return "\n".join(lines)

    def count(self) -> int:
        return self.con.execute("SELECT COUNT(*) FROM rag_entries").fetchone()[0]


rag = MigrationRAG()
