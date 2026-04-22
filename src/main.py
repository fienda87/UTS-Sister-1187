import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from src.consumers import Consumer, Stats
from src.dedup_store import DedupStore
from src.models import Event, PublishRequest

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

dedup_store = DedupStore()
stats = Stats()
consumer = Consumer(dedup_store=dedup_store, stats=stats)


@asynccontextmanager
async def lifespan(app: FastAPI):
    consumer.start()
    logger.info("Application startup complete.")
    yield
    await consumer.stop()
    logger.info("Application shutdown.")


app = FastAPI(
    title="Pub-Sub Log Aggregator",
    description="Idempotent consumer dengan deduplication berbasis SQLite",
    version="1.0.0",
    lifespan=lifespan
)


@app.post("/publish", status_code=200)
async def publish(request: PublishRequest):
    """Menerima single atau batch event; validasi skema dan enqueue."""
    events = request.get_events()
    if not events:
        raise HTTPException(status_code=400, detail="Tidak ada event yang dikirim.")

    queued = 0
    for event in events:
        await consumer.enqueue(event)
        queued += 1

    return {"status": "queued", "count": queued}


@app.get("/events")
async def get_events(topic: Optional[str] = Query(None, description="Filter berdasarkan topic")):
    """Mengembalikan daftar event unik yang telah diproses."""
    if not topic:
        raise HTTPException(status_code=400, detail="Parameter topic wajib diisi.")
    events = dedup_store.get_events_by_topic(topic)
    return {"topic": topic, "count": len(events), "events": events}


@app.get("/stats")
async def get_stats():
    """Menampilkan statistik sistem."""
    topics = dedup_store.get_all_topics()
    return {
        "received": stats.received,
        "unique_processed": stats.unique_processed,
        "duplicate_dropped": stats.duplicate_dropped,
        "topics": topics,
        "uptime": stats.uptime()
    }


@app.get("/health")
async def health():
    return {"status": "ok"}