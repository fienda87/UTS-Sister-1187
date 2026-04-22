"""
Publisher script — simulasi at-least-once delivery dengan duplikasi event.
Kirim 5000 event ke aggregator dengan 20% duplikat.
"""
import uuid
import random
import time
import json
from datetime import datetime, timezone

try:
    import requests
except ImportError:
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "requests", "--quiet"])
    import requests

AGGREGATOR_URL = "http://aggregator:8080/publish"
TOTAL_EVENTS = 5000
DUPLICATE_RATE = 0.20
BATCH_SIZE = 100
TOPICS = [
    "auth-service.production.login",
    "payment-service.production.transaction",
    "inventory-service.production.update",
]


def generate_events():
    unique_ids = [str(uuid.uuid4()) for _ in range(int(TOTAL_EVENTS * (1 - DUPLICATE_RATE)))]
    events = []
    for i in range(TOTAL_EVENTS):
        if i < len(unique_ids):
            eid = unique_ids[i]
        else:
            eid = random.choice(unique_ids)  # duplikat
        events.append({
            "topic": random.choice(TOPICS),
            "event_id": eid,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "publisher-script",
            "payload": {"seq": i, "msg": f"event-{i}"}
        })
    random.shuffle(events)
    return events


def send_batch(batch):
    try:
        resp = requests.post(
            AGGREGATOR_URL,
            json={"events": batch},
            timeout=10
        )
        return resp.status_code == 200
    except Exception as e:
        print(f"[ERROR] {e}")
        return False


def main():
    print(f"[Publisher] Waiting for aggregator...")
    time.sleep(3)

    events = generate_events()
    print(f"[Publisher] Mengirim {len(events)} event ({int(DUPLICATE_RATE*100)}% duplikat)...")

    start = time.time()
    sent = 0
    for i in range(0, len(events), BATCH_SIZE):
        batch = events[i:i+BATCH_SIZE]
        ok = send_batch(batch)
        if ok:
            sent += len(batch)
        print(f"  Batch {i//BATCH_SIZE + 1}: {'OK' if ok else 'FAIL'} ({sent}/{len(events)})")

    elapsed = time.time() - start
    print(f"[Publisher] Selesai. {sent} event terkirim dalam {elapsed:.2f}s")

    # Cek stats
    try:
        r = requests.get("http://aggregator:8080/stats", timeout=5)
        print(f"[Stats] {json.dumps(r.json(), indent=2)}")
    except Exception as e:
        print(f"[Stats] Error: {e}")


if __name__ == "__main__":
    main()