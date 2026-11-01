import asyncio
import concurrent.futures
import logging
import uuid
import torch
from datetime import datetime, timedelta, timezone
from temporalio.client import Client
from temporalio.worker import Worker

from app.config import settings
from app.database.db import db
from app.temporal.workflows import (
    ConfigDrivenProcessingWorkflow,
    BatchProcessingWorkflow,
    BulkStepRetryWorkflow,
)
from app.temporal.activities import (
    update_status_activity,
    fetch_pending_submissions_activity,
    fetch_failed_submissions_activity,
)
from app.temporal.deface_blur_activity import deface_blur_activity
from app.temporal.pii_and_abusive_activity import pii_and_abusive_language_detection_activity
from app.temporal.thematic_activity import thematic_classification_activity
from app.temporal.environment_activity import environment_detection_activity
from app.temporal.story_rating_activity import story_rating_activity
from app.temporal.statement_category_activity import statement_category_activity
logger = logging.getLogger("analytics_service.temporal.worker")


def _compute_next_run_delay(cron_expr: str, now: datetime) -> tuple[datetime, float]:
    """
    Parse a daily cron expression (``MM HH * * *``) and return
    ``(target_utc_datetime, delay_seconds)`` for the next scheduled run.
    Advances by one day if the scheduled time has already passed today.
    """
    parts = cron_expr.strip().split()
    cron_minute = int(parts[0])
    cron_hour = int(parts[1])

    target = now.replace(hour=cron_hour, minute=cron_minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)

    delay = (target - now).total_seconds()
    return target, delay


async def _retry_cron_loop() -> None:
    """
    In-process retry cron: sleeps until the next RETRY_CRON_SCHEDULE time,
    then calls the same sweep the /v1/retry-step/all API uses.
    No parent workflow is created in Temporal — only individual step-retry-*
    entries appear (identical behaviour to the bulk retry API endpoint).
    Runs forever; catches and logs any exception so the loop never dies on its own.
    """
    # Deferred import to avoid circular imports at module load time
    from app.api.routes.uploads import _dispatch_all_retry_workflows

    logger.info(
        "Retry cron loop started (schedule=%s, UTC).", settings.RETRY_CRON_SCHEDULE
    )
    try:
        while True:
            now = datetime.now(timezone.utc)
            target, delay = _compute_next_run_delay(settings.RETRY_CRON_SCHEDULE, now)
            logger.info(
                "Retry cron: next run at %s UTC (in %.0f s).", target.isoformat(), delay
            )
            await asyncio.sleep(delay)

            sweep_id = f"cron-retry-sweep-{uuid.uuid4().hex[:8]}"
            cutoff = datetime.now(timezone.utc).isoformat()
            logger.info("Retry cron: starting sweep %s (cutoff=%s).", sweep_id, cutoff)
            try:
                await _dispatch_all_retry_workflows(
                    tenant_code=None,
                    cutoff=cutoff,
                    sweep_id=sweep_id,
                )
            except Exception as exc:
                logger.exception(
                    "Retry cron: sweep %s raised an unexpected error: %s", sweep_id, exc
                )
            # Loop continues to the next scheduled time regardless of outcome
    except asyncio.CancelledError:
        logger.info("Retry cron loop cancelled — shutting down cleanly.")
        raise

async def start_worker():
    """
    Connects to Temporal server and listens on the configured task queue.
    """
    # Python's default asyncio thread pool executor is capped at
    # min(32, cpu_count + 4) — far smaller than our configured worker
    # concurrency, and every activity's blocking work (LLM calls via urllib,
    # image download/blur/upload, PDF extraction, embeddings) runs through
    # asyncio.to_thread(), which uses this default executor unless overridden.
    # Without sizing it explicitly, WORKER_MAX_CONCURRENT_ACTIVITIES is a
    # ceiling Temporal never actually reaches — most "concurrent" activities
    # just queue for a free thread instead of doing real work (load-tested:
    # this silently capped real throughput to ~12-way parallelism on an
    # 8-core box, not the 40 configured at the Temporal level).
    asyncio.get_running_loop().set_default_executor(
        concurrent.futures.ThreadPoolExecutor(max_workers=settings.WORKER_MAX_CONCURRENT_ACTIVITIES)
    )

    # PyTorch defaults to using EVERY available CPU core for its own internal
    # BLAS/linear-algebra threading on each individual call (SentenceTransformer
    # embeddings here) — fine for a single request at a time, catastrophic once
    # many activities call into it concurrently. Load-tested: with this unset,
    # concurrent embedding calls under real load turned a sub-second operation
    # into 15+ minutes (dozens of activities each trying to claim all 8 cores
    # for their own inference call, thrashing on context switches instead of
    # doing work). Capping PyTorch's OWN thread count to 1 makes
    # WORKER_MAX_CONCURRENT_ACTIVITIES the only source of parallelism, instead
    # of the two multiplying against each other.
    torch.set_num_threads(1)

    # Initialize database connection pool
    await db.connect()

    try:
        logger.info(f"Connecting to Temporal Server at {settings.TEMPORAL_HOST}...")
        client = await Client.connect(settings.TEMPORAL_HOST, namespace=settings.TEMPORAL_NAMESPACE)
    except Exception as e:
        logger.error(f"Failed to connect to Temporal Server on {settings.TEMPORAL_HOST}: {e}")
        await db.disconnect()
        return

    # Define registered activities and workflows
    workflows = [
        ConfigDrivenProcessingWorkflow,
        BatchProcessingWorkflow,
        BulkStepRetryWorkflow,
    ]
    activities = [
        pii_and_abusive_language_detection_activity,
        environment_detection_activity,
        thematic_classification_activity,
        deface_blur_activity,
        story_rating_activity,
        statement_category_activity,
        update_status_activity,
        fetch_pending_submissions_activity,
        fetch_failed_submissions_activity,
    ]

    worker = Worker(
        client,
        task_queue=settings.TEMPORAL_QUEUE,
        workflows=workflows,
        activities=activities,
        max_concurrent_activities=settings.WORKER_MAX_CONCURRENT_ACTIVITIES,
    )

    from temporalio.client import (
        Schedule,
        ScheduleActionStartWorkflow,
        ScheduleSpec,
        ScheduleAlreadyRunningError,
        ScheduleUpdate,
    )

    # Register daily batch schedules if configured for batch mode
    if settings.PROCESSING_MODE.lower().strip() == "batch":
        batch_schedule = Schedule(
            action=ScheduleActionStartWorkflow(
                BatchProcessingWorkflow.run,
                settings.BATCH_SIZE,
                id="daily-batch-processing-run",
                task_queue=settings.TEMPORAL_QUEUE,
            ),
            spec=ScheduleSpec(
                cron_expressions=[settings.BATCH_SCHEDULE_CRON]
            ),
        )
        try:
            logger.info(f"Registering daily analysis batch schedule '{settings.BATCH_SCHEDULE_CRON}' in Temporal...")
            await client.create_schedule(
                id="daily-batch-processing",
                schedule=batch_schedule,
            )
            logger.info("Daily analysis batch schedule successfully registered.")
        except ScheduleAlreadyRunningError:
            # Schedule exists — update it so a changed cron expression or batch size takes effect
            logger.info("Daily analysis batch schedule already exists — updating with current config.")
            handle = client.get_schedule_handle("daily-batch-processing")
            await handle.update(lambda _: ScheduleUpdate(schedule=batch_schedule))
            logger.info("Daily analysis batch schedule updated.")
        except Exception as e:
            logger.error(f"Failed to register daily analysis batch schedule in Temporal: {e}")
    else:
        # Real-time mode: clean up any leftover batch schedules from Temporal Server
        # (prevents a schedule left behind from a prior batch-mode config from
        # silently retrying forever with outdated arguments).
        for sched_id in ("daily-batch-processing",):
            try:
                handle = client.get_schedule_handle(sched_id)
                await handle.delete()
                logger.info("Deleted stale batch schedule '%s' (PROCESSING_MODE=real-time).", sched_id)
            except Exception:
                pass  # schedule doesn't exist — nothing to clean up

    for sched_id in ("daily-failed-submissions-retry",):
        try:
            handle = client.get_schedule_handle(sched_id)
            await handle.delete()
            logger.info("Deleted stale retry Temporal schedule '%s'.", sched_id)
        except Exception:
            pass  # schedule doesn't exist — nothing to clean up

    # Start in-process retry cron if enabled.
    # This fires _dispatch_all_retry_workflows directly — the same function the
    # /v1/retry-step/all API uses — so only individual step-retry-* workflows
    # appear in Temporal UI, with no parent workflow at all.
    retry_cron_task = None
    if settings.ENABLE_RETRY_CRON:
        retry_cron_task = asyncio.create_task(_retry_cron_loop())
        logger.info(
            "In-process retry cron task started (schedule=%s).", settings.RETRY_CRON_SCHEDULE
        )
    else:
        logger.info("Retry cron disabled (ENABLE_RETRY_CRON=false).")

    logger.info(f"🚀 Temporal Worker started. Listening on task queue '{settings.TEMPORAL_QUEUE}'...")
    try:
        await worker.run()
    except asyncio.CancelledError:
        logger.info("Worker execution cancelled.")
    finally:
        # Cancel the retry cron loop before closing the DB pool so any
        # in-flight sweep can finish its current DB acquire cleanly.
        if retry_cron_task is not None and not retry_cron_task.done():
            retry_cron_task.cancel()
            try:
                await retry_cron_task
            except asyncio.CancelledError:
                pass
        await db.disconnect()

if __name__ == "__main__":
    from app.logging_config import configure_logging
    configure_logging("worker")
    asyncio.run(start_worker())
