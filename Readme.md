# UTS-Sister-1187: Pub-Sub Log Aggregator

Sistem Pub-Sub Log Aggregator dengan Idempotent Consumer dan Deduplication.
Dibangun dengan Python (FastAPI) + SQLite + Docker.

**Repository**: [UTS-Sister-1187](https://github.com/fienda87/UTS-Sister-1187)

## Arsitektur

```
Publisher(s) → POST /publish → asyncio.Queue → Consumer → SQLite DedupStore
                                                              ↓
                                             GET /events   GET /stats
```

## Struktur Direktori

```
├── src/
│   ├── main.py          # FastAPI app, endpoints
│   ├── models.py        # Pydantic Event schema
│   ├── consumer.py      # asyncio.Queue consumer + Stats
│   └── dedup_store.py   # SQLite persistent dedup store
├── tests/
│   └── test_all.py      # 9 unit tests (pytest)
├── scripts/
│   └── publisher.py     # Simulasi publisher (5000 event, 20% duplikat)
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

## Quick Start

### Build & Run dengan Docker

```bash
# Clone repository
git clone https://github.com/fienda87/UTS-Sister-1187.git
cd UTS-Sister-1187

# Build image
docker build -t uts-aggregator .

# Run container
docker run -p 8080:8080 uts-aggregator
```

### Build & Run dengan Docker Compose

```bash
docker compose up --build
```

### Run Unit Tests Lokal

```bash
pip install -r requirements.txt
pytest tests/test_all.py -v
```

## Endpoints

| Method | Path | Deskripsi |
|--------|------|-----------|
| POST | `/publish` | Kirim single atau batch event |
| GET | `/events?topic=...` | Daftar event unik per topic |
| GET | `/stats` | Statistik sistem |
| GET | `/health` | Health check |

### Contoh Publish

```bash
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{
    "events": {
      "topic": "auth-service.production.login",
      "event_id": "f47ac10b-58cc-4372-a567",
      "timestamp": "2025-10-24T00:00:00Z",
      "source": "auth-service",
      "payload": {"user": "zaki"}
    }
  }'
```

### Contoh Batch Publish

```bash
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{
    "events": [
      {"topic": "t1", "event_id": "e1", "timestamp": "2025-10-24T00:00:00Z", "source": "s1", "payload": {}},
      {"topic": "t1", "event_id": "e2", "timestamp": "2025-10-24T00:00:01Z", "source": "s1", "payload": {}}
    ]
  }'
```

## Testing Scenarios

### 1. Unit Tests (9 tests)
```bash
pytest tests/test_all.py -v
```
Expected: 9 passed, 2 warnings

### 2. Manual API Test (Single Event)
```bash
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{
    "events": {
      "topic": "audit.login",
      "event_id": "login-1001",
      "timestamp": "2025-10-24T08:15:00Z",
      "source": "web_portal",
      "payload": {"user": "andi", "action": "login"}
    }
  }'
```

### 3. Idempotency Test
Send same event twice → second request drops as duplicate
```bash
# Event 1
curl -X POST http://localhost:8080/publish ...

# Event 2 (same event_id) → duplicate_dropped +1
curl -X POST http://localhost:8080/publish ...
```

### 4. Stress Test (5000 events, 20% duplicate)
```powershell
$events = @()
for ($i = 1; $i -le 5000; $i++) {
  $id = if ($i % 5 -eq 0) { "evt$($i-1)" } else { "evt$i" }
  $events += @{
    topic = "stress.logs"
    event_id = $id
    timestamp = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
    source = "stress_tester"
    payload = @{ msg = "Event $i" }
  }
}

$body = @{ events = $events } | ConvertTo-Json -Depth 8
Invoke-RestMethod -Uri "http://localhost:8080/publish" -Method Post -ContentType "application/json" -Body $body
```

### 5. Dedup Persistence Test
```bash
# Restart container
docker compose restart aggregator

# Send old event (should be dropped)
# duplicate_dropped counter must increment
```

## Asumsi Desain

- **Ordering**: Partial ordering berbasis timestamp event. Total ordering tidak diterapkan antar topic karena overhead tidak sebanding dengan manfaatnya pada use case log aggregation.
- **Dedup Store**: SQLite dengan WAL mode untuk concurrency dan durability. DB disimpan di volume agar persisten setelah restart.
- **Idempotency**: Berdasarkan key `(topic, event_id)`. Satu kombinasi hanya diproses sekali.
- **Delivery semantics**: At-least-once di sisi publisher; exactly-once effect di sisi consumer via idempotency.