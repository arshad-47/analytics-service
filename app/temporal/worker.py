import asyncio
import concurrent.futures
import logging
import torch
from temporalio.client import Client
from temporalio.worker import Worker

from app.config import settings
from app.database.db import db
from app.temporal.workflows import (
    ConfigDrivenProcessingWorkflow,
    BatchProcessingWorkflow,
)
from app.temporal.activities import (
    update_status_activity,
    fetch_pending_submissions_activity
)
from app.temporal.deface_blur_activity import deface_blur_activity
from app.temporal.pii_and_abusive_activity import pii_and_abusive_language_detection_activity
from app.temporal.thematic_activity import thematic_classification_activity
from app.temporal.environment_activity import environment_detection_activity
from app.temporal.story_rating_activity import story_rating_activity
from app.temporal.statement_category_activity import statement_category_activity
logger = logging.getLogger("analytics_service.temporal.worker")

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
    ]

    worker = Worker(
        client,
        task_queue=settings.TEMPORAL_QUEUE,
        workflows=workflows,
        activities=activities,
        max_concurrent_activities=settings.WORKER_MAX_CONCURRENT_ACTIVITIES,
    )

    # Register daily batch schedules if configured for batch mode
    if settings.PROCESSING_MODE.lower().strip() == "batch":
        from temporalio.client import (
            Schedule,
            ScheduleActionStartWorkflow,
            ScheduleSpec,
            ScheduleAlreadyRunningError,
        )

        # Register daily analysis batch processing schedule
        try:
            logger.info(f"Registering daily analysis batch schedule '{settings.BATCH_SCHEDULE_CRON}' in Temporal...")
            await client.create_schedule(
                id="daily-batch-processing",
                schedule=Schedule(
                    action=ScheduleActionStartWorkflow(
                        BatchProcessingWorkflow.run,
                        settings.BATCH_SIZE,
                        id="daily-batch-processing-run",
                        task_queue=settings.TEMPORAL_QUEUE,
                    ),
                    spec=ScheduleSpec(
                        cron_expressions=[settings.BATCH_SCHEDULE_CRON]
                    ),
                ),
            )
            logger.info("Daily analysis batch schedule successfully registered.")
        except ScheduleAlreadyRunningError:
            logger.info("Daily analysis batch schedule already exists in Temporal. Skipping registration.")
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

    logger.info(f"🚀 Temporal Worker started. Listening on task queue '{settings.TEMPORAL_QUEUE}'...")
    try:
        await worker.run()
    except asyncio.CancelledError:
        logger.info("Worker execution cancelled.")
    finally:
        await db.disconnect()

if __name__ == "__main__":
    from app.logging_config import configure_logging
    configure_logging("worker")
    asyncio.run(start_worker())
