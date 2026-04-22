import sqlite3
import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

DB_PATH = os.environ.get("DEDUP_DB_PATH", "/app/data/dedup_store.db")


class DedupStore:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _init_db(self):
        with self._get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS processed_events (
                    topic TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    processed_at TEXT NOT NULL,
                    source TEXT,
                    PRIMARY KEY (topic, event_id)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS event_payloads (
                    topic TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    source TEXT,
                    payload TEXT NOT NULL,
                    PRIMARY KEY (topic, event_id)
                )
            """)
            conn.commit()
        logger.info(f"DedupStore initialized: {self.db_path}")

    def is_duplicate(self, topic: str, event_id: str) -> bool:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT 1 FROM processed_events WHERE topic=? AND event_id=?",
                (topic, event_id)
            ).fetchone()
            return row is not None

    def mark_processed(self, topic: str, event_id: str, source: str, processed_at: str) -> bool:
        """Return True jika berhasil di-insert (bukan duplikat), False jika duplikat."""
        try:
            with self._get_conn() as conn:
                conn.execute(
                    "INSERT INTO processed_events (topic, event_id, processed_at, source) VALUES (?,?,?,?)",
                    (topic, event_id, processed_at, source)
                )
                conn.commit()
            return True
        except sqlite3.IntegrityError:
            logger.warning(f"Duplicate dropped: topic={topic} event_id={event_id}")
            return False

    def save_event_payload(self, topic: str, event_id: str, timestamp: str, source: str, payload: str):
        try:
            with self._get_conn() as conn:
                conn.execute(
                    "INSERT OR IGNORE INTO event_payloads (topic, event_id, timestamp, source, payload) VALUES (?,?,?,?,?)",
                    (topic, event_id, timestamp, source, payload)
                )
                conn.commit()
        except Exception as e:
            logger.error(f"Error saving payload: {e}")

    def get_events_by_topic(self, topic: str) -> list:
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT topic, event_id, timestamp, source, payload FROM event_payloads WHERE topic=? ORDER BY timestamp ASC",
                (topic,)
            ).fetchall()
        import json
        return [
            {
                "topic": r[0],
                "event_id": r[1],
                "timestamp": r[2],
                "source": r[3],
                "payload": json.loads(r[4])
            }
            for r in rows
        ]

    def get_all_topics(self) -> list:
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT DISTINCT topic FROM processed_events"
            ).fetchall()
        return [r[0] for r in rows]