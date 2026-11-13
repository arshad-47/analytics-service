import asyncio
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional
from temporalio import activity

logger = logging.getLogger("analytics_service.temporal.migration_activity")


@activity.defn
async def run_migration_activity(params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Temporal activity that executes the ShikshaLokam -> Analytics delta migration pipeline.
    Runs migrate-scripts/migrate.py as an isolated subprocess.
    """
    params = params or {}
    dry_run = bool(params.get("dry_run", False))

    activity.logger.info("Starting SL -> Analytics migration activity (dry_run=%s)...", dry_run)

    project_root = Path(__file__).resolve().parent.parent.parent
    script_path = project_root / "migrate-scripts" / "migrate.py"

    if not script_path.exists():
        error_msg = f"Migration script not found at expected path: {script_path}"
        activity.logger.error(error_msg)
        raise FileNotFoundError(error_msg)

    cmd = [sys.executable, str(script_path)]
    if dry_run:
        cmd.append("--dry-run")

    env = os.environ.copy()

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        cwd=str(project_root),
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    # Local deadline shorter than the 30-minute Temporal activity timeout (default: 25 minutes = 1500s)
    timeout_seconds = float(params.get("timeout_seconds", 1500.0))

    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout_seconds)
    except (asyncio.CancelledError, asyncio.TimeoutError) as exc:
        activity.logger.warning(
            "Migration activity interrupted (cancelled or timed out after %.1fs). Terminating child process (PID %s)...",
            timeout_seconds,
            proc.pid,
        )
        try:
            proc.terminate()
            await asyncio.wait_for(proc.wait(), timeout=5.0)
        except (asyncio.TimeoutError, ProcessLookupError):
            activity.logger.warning("Child process (PID %s) did not exit after SIGTERM; killing with SIGKILL...", proc.pid)
            try:
                proc.kill()
                await proc.wait()
            except ProcessLookupError:
                pass
        raise RuntimeError(f"Migration activity cancelled or timed out after {timeout_seconds}s") from exc

    stdout_text = stdout.decode("utf-8", errors="replace")
    stderr_text = stderr.decode("utf-8", errors="replace")

    if proc.returncode != 0:
        error_detail = stderr_text.strip() or stdout_text.strip()
        activity.logger.error("Migration failed (exit code %d):\n%s", proc.returncode, error_detail)
        raise RuntimeError(f"Migration script exited with code {proc.returncode}: {error_detail}")

    activity.logger.info("Migration finished successfully (exit code 0).")
    # Return output summary bounded to avoid exceeding Temporal's 2MB payload limit
    output_summary = stdout_text[-10000:] if len(stdout_text) > 10000 else stdout_text

    return {
        "status": "success",
        "returncode": proc.returncode,
        "dry_run": dry_run,
        "output_summary": output_summary,
    }
