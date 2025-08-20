# app.py
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException

log = logging.getLogger("uvicorn.error")


class AppState:
    """Holds resources and task handles for coordinated shutdown."""
    def __init__(self) -> None:
        self.shutdown_event = asyncio.Event()
        self.queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.tasks: list[asyncio.Task] = []
        # put real resources here (db pool, httpx client, etc.)
        self.db = None


async def worker(name: str, state: AppState) -> None:
    """Background worker that processes jobs until asked to stop."""
    try:
        while not state.shutdown_event.is_set():
            try:
                job = await asyncio.wait_for(state.queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue  # check shutdown_event periodically

            try:
                # Simulate useful work
                log.info("[%s] processing: %s", name, job)
                await asyncio.sleep(job.get("duration", 1.0))
            except Exception as e:
                log.exception("[%s] job failed: %s", name, e)
            finally:
                state.queue.task_done()
    except asyncio.CancelledError:
        # Final cleanup for this worker
        log.info("[%s] cancelled; cleaning up...", name)
        raise


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Create shared resources and background tasks on startup.
    Ensure orderly cancellation and cleanup on shutdown.
    """
    app.state.state = AppState()
    state: AppState = app.state.state

    # Example: initialize external resources
    # state.db = await asyncpg.create_pool(...)

    # Start background workers
    for i in range(2):
        t = asyncio.create_task(worker(f"worker-{i+1}", state), name=f"worker-{i+1}")
        state.tasks.append(t)
    log.info("Background workers started.")

    try:
        yield
    finally:
        # Signal workers to stop accepting new work
        state.shutdown_event.set()

        # Optionally wait for the queue to drain (bounded wait)
        try:
            await asyncio.wait_for(state.queue.join(), timeout=5.0)
        except asyncio.TimeoutError:
            log.warning("Queue did not drain before shutdown; cancelling workers.")

        # Cancel any still-running tasks and wait for them to finish
        for t in state.tasks:
            t.cancel()
        results = await asyncio.gather(*state.tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, Exception) and not isinstance(r, asyncio.CancelledError):
                log.exception("Worker error during shutdown: %s", r)

        # Close external resources here
        # await state.db.close()

        log.info("Shutdown complete.")


app = FastAPI(title="Clean Lifespan + Graceful Cancellation", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"ok": True}


@app.post("/enqueue")
async def enqueue(job: dict[str, Any]):
    """
    Enqueue a unit of work, e.g.
    POST /enqueue
    {"task":"resize","duration":2.5}
    """
    state: AppState = app.state.state
    try:
        state.queue.put_nowait(job)
        size = state.queue.qsize()
        return {"status": "queued", "queue_size": size}
    except asyncio.QueueFull:
        raise HTTPException(status_code=503, detail="Queue full")


@app.get("/drain")
async def drain():
    """Wait (briefly) for the queue to drain – useful for graceful rollouts."""
    state: AppState = app.state.state
    try:
        await asyncio.wait_for(state.queue.join(), timeout=10.0)
        return {"drained": True}
    except asyncio.TimeoutError:
        return {"drained": False, "pending": state.queue.qsize()}


@app.get("/")
async def root():
    return {"message": "Send POST /enqueue with a JSON job payload."}
