import asyncio
import json
import logging
from datetime import datetime, timezone

from src.dedup_store import DedupStore
from src.models import Event

logger = logging.getLogger(__name__)


class Stats:
    def __init__(self):
        self.received: int = 0
        self.unique_processed: int = 0
        self.duplicate_dropped: int = 0
        self.start_time: datetime = datetime.now(timezone.utc)

    def uptime(self) -> float:
        delta = datetime.now(timezone.utc) - self.start_time
        return round(delta.total_seconds(), 3)


class Consumer:
    def __init__(self, dedup_store: DedupStore, stats: Stats):
        self.queue: asyncio.Queue = asyncio.Queue()
        self.dedup_store = dedup_store
        self.stats = stats
        self._task: asyncio.Task | None = None

    def start(self):
        self._task = asyncio.create_task(self._run())
        logger.info("Consumer started.")

    async def stop(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def enqueue(self, event: Event):
        self.stats.received += 1
        await self.queue.put(event)

    async def _run(self):
        while True:
            try:
                event: Event = await self.queue.get()
                await self._process(event)
                self.queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Consumer error: {e}")

    async def _process(self, event: Event):
        now_str = datetime.now(timezone.utc).isoformat()
        is_new = self.dedup_store.mark_processed(
            topic=event.topic,
            event_id=event.event_id,
            source=event.source,
            processed_at=now_str
        )
        if is_new:
            self.stats.unique_processed += 1
            payload_str = json.dumps(event.payload)
            self.dedup_store.save_event_payload(
                topic=event.topic,
                event_id=event.event_id,
                timestamp=event.timestamp.isoformat(),
                source=event.source,
                payload=payload_str
            )
            logger.info(f"Processed event: topic={event.topic} event_id={event.event_id}")
        else:
            self.stats.duplicate_dropped += 1
            logger.warning(f"Duplicate dropped: topic={event.topic} event_id={event.event_id}")