"""
Unit Tests — Pub-Sub Log Aggregator
pytest tests/test_all.py -v
"""
import asyncio
import json
import os
import tempfile
import time
import pytest
from datetime import datetime, timezone
from fastapi.testclient import TestClient


# Gunakan DB sementara supaya tests terisolasi
@pytest.fixture(autouse=True)
def temp_db(tmp_path, monkeypatch):
    db_file = str(tmp_path / "test_dedup.db")
    monkeypatch.setenv("DEDUP_DB_PATH", db_file)
    yield db_file


@pytest.fixture
def client(temp_db):
    # Re-import setelah env var diset
    import importlib
    import src.dedup_store as ds_mod
    importlib.reload(ds_mod)
    import src.consumers as cons_mod
    importlib.reload(cons_mod)
    import src.main as main_mod
    importlib.reload(main_mod)

    with TestClient(main_mod.app) as c:
        yield c


# ─── TEST 1: Validasi skema event valid ───────────────────────────────────────
def test_valid_event_schema(client):
    payload = {
        "events": {
            "topic": "test.topic",
            "event_id": "evt-001",
            "timestamp": "2025-10-24T00:00:00Z",
            "source": "test",
            "payload": {"msg": "hello"}
        }
    }
    resp = client.post("/publish", json=payload)
    assert resp.status_code == 200
    assert resp.json()["count"] == 1


# ─── TEST 2: Validasi skema event invalid (missing field) ─────────────────────
def test_invalid_event_schema_missing_field(client):
    payload = {
        "events": {
            "topic": "test.topic",
            # event_id hilang
            "timestamp": "2025-10-24T00:00:00Z",
            "source": "test",
            "payload": {}
        }
    }
    resp = client.post("/publish", json=payload)
    assert resp.status_code == 422


# ─── TEST 3: Timestamp invalid ────────────────────────────────────────────────
def test_invalid_timestamp(client):
    payload = {
        "events": {
            "topic": "test.topic",
            "event_id": "evt-002",
            "timestamp": "bukan-timestamp",
            "source": "test",
            "payload": {}
        }
    }
    resp = client.post("/publish", json=payload)
    assert resp.status_code == 422


# ─── TEST 4: Dedup — duplikat hanya diproses sekali ───────────────────────────
def test_deduplication_logic(client):
    event = {
        "topic": "dedup.test",
        "event_id": "unique-dup-001",
        "timestamp": "2025-10-24T00:00:00Z",
        "source": "tester",
        "payload": {"x": 1}
    }
    # Kirim 3x
    for _ in range(3):
        client.post("/publish", json={"events": event})

    time.sleep(0.5)  # tunggu consumer

    stats = client.get("/stats").json()
    assert stats["unique_processed"] == 1
    assert stats["duplicate_dropped"] == 2


# ─── TEST 5: Batch publish ────────────────────────────────────────────────────
def test_batch_publish(client):
    events = [
        {
            "topic": "batch.test",
            "event_id": f"batch-{i}",
            "timestamp": "2025-10-24T00:00:00Z",
            "source": "batch_tester",
            "payload": {"i": i}
        }
        for i in range(10)
    ]
    resp = client.post("/publish", json={"events": events})
    assert resp.status_code == 200
    assert resp.json()["count"] == 10


# ─── TEST 6: GET /events konsisten dengan data ────────────────────────────────
def test_get_events_consistent(client):
    event = {
        "topic": "events.test",
        "event_id": "ev-get-001",
        "timestamp": "2025-10-24T00:00:00Z",
        "source": "tester",
        "payload": {"val": "test"}
    }
    client.post("/publish", json={"events": event})
    time.sleep(0.5)

    resp = client.get("/events?topic=events.test")
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 1
    assert data["events"][0]["event_id"] == "ev-get-001"


# ─── TEST 7: GET /stats konsisten ─────────────────────────────────────────────
def test_stats_consistency(client):
    for i in range(5):
        client.post("/publish", json={"events": {
            "topic": "stats.test",
            "event_id": f"stats-{i}",
            "timestamp": "2025-10-24T00:00:00Z",
            "source": "tester",
            "payload": {}
        }})
    # Kirim 2 duplikat
    for _ in range(2):
        client.post("/publish", json={"events": {
            "topic": "stats.test",
            "event_id": "stats-0",
            "timestamp": "2025-10-24T00:00:00Z",
            "source": "tester",
            "payload": {}
        }})

    time.sleep(0.5)
    stats = client.get("/stats").json()
    assert stats["received"] == 7
    assert stats["unique_processed"] == 5
    assert stats["duplicate_dropped"] == 2
    assert "stats.test" in stats["topics"]
    assert "uptime" in stats


# ─── TEST 8: Persistensi dedup store setelah simulasi restart ─────────────────
def test_dedup_persistence_across_restart(temp_db, monkeypatch):
    """Simulasi restart: buat DedupStore baru dengan DB yang sama, dedup tetap efektif."""
    from src.dedup_store import DedupStore
    monkeypatch.setenv("DEDUP_DB_PATH", temp_db)

    store1 = DedupStore(db_path=temp_db)
    result1 = store1.mark_processed("persist.test", "persist-001", "tester", "2025-10-24T00:00:00Z")
    assert result1 is True  # pertama kali: berhasil

    # Simulasi restart — buat instance baru dengan DB yang sama
    store2 = DedupStore(db_path=temp_db)
    result2 = store2.mark_processed("persist.test", "persist-001", "tester", "2025-10-24T01:00:00Z")
    assert result2 is False  # sudah ada: ditolak (dedup efektif)


# ─── TEST 9 (bonus): Stress — batch besar dalam waktu wajar ───────────────────
def test_stress_batch_performance(client):
    import time
    events = [
        {
            "topic": "stress.test",
            "event_id": f"stress-{i}",
            "timestamp": "2025-10-24T00:00:00Z",
            "source": "stress_tester",
            "payload": {"i": i}
        }
        for i in range(500)
    ]
    start = time.time()
    resp = client.post("/publish", json={"events": events})
    elapsed = time.time() - start

    assert resp.status_code == 200
    assert elapsed < 5.0, f"Terlalu lambat: {elapsed:.2f}s"
    assert resp.json()["count"] == 500