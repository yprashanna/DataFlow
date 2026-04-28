"""APScheduler-based pipeline scheduler.

We use APScheduler because it's lightweight — no server, no daemon, just Python.
Airflow is overkill for most pipelines; this runs in a single process.

Usage:
    python -m orchestrator.scheduler
    # OR: make schedule
"""

import logging
import signal
import sys
import time
from pathlib import Path

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from orchestrator.config_parser import load_pipeline_config, list_pipeline_configs, get_pipeline_schedule
from orchestrator.runner import PipelineRunner

logger = logging.getLogger(__name__)

# How often to reload configs and re-register jobs (in seconds)
# This allows adding new pipelines without restarting the scheduler
CONFIG_RELOAD_INTERVAL = 300  # 5 minutes


def run_pipeline_job(config_path: str):
    """Job function called by APScheduler — loads config fresh each run."""
    try:
        pipeline_config = load_pipeline_config(config_path)
        runner = PipelineRunner(pipeline_config)
        result = runner.run()
        logger.info(
            "Scheduled job complete: %s — status=%s",
            pipeline_config["name"],
            result["status"],
        )
    except Exception as exc:
        logger.error("Scheduled job failed for %s: %s", config_path, exc)


def build_scheduler(configs_dir: str = "configs") -> BlockingScheduler:
    """Create and configure a BlockingScheduler from pipeline YAML configs."""
    scheduler = BlockingScheduler(timezone="UTC")

    config_files = list_pipeline_configs(configs_dir)
    registered = 0

    for config_path in config_files:
        try:
            pipeline_config = load_pipeline_config(config_path)
            cron_expr = get_pipeline_schedule(pipeline_config)

            if not cron_expr:
                logger.info(
                    "Pipeline '%s' has no schedule — skipping (run manually with make run-pipeline)",
                    pipeline_config.get("name"),
                )
                continue

            # Parse cron expression — standard 5-field cron
            cron_parts = cron_expr.strip().split()
            if len(cron_parts) != 5:
                logger.warning(
                    "Invalid cron expression '%s' for pipeline %s — expected 5 fields",
                    cron_expr,
                    pipeline_config.get("name"),
                )
                continue

            minute, hour, day, month, day_of_week = cron_parts
            trigger = CronTrigger(
                minute=minute,
                hour=hour,
                day=day,
                month=month,
                day_of_week=day_of_week,
                timezone="UTC",
            )

            job_id = f"pipeline_{pipeline_config['name']}"
            scheduler.add_job(
                func=run_pipeline_job,
                trigger=trigger,
                args=[str(config_path)],
                id=job_id,
                name=pipeline_config["name"],
                replace_existing=True,
                max_instances=1,  # prevent overlapping runs of the same pipeline
                misfire_grace_time=300,  # 5 min grace period for missed fires
            )

            logger.info(
                "Registered pipeline '%s' with schedule: %s",
                pipeline_config["name"],
                cron_expr,
            )
            registered += 1

        except Exception as exc:
            logger.error("Failed to register pipeline from %s: %s", config_path, exc)

    logger.info("Scheduler ready: %d pipeline(s) registered", registered)
    return scheduler


def main():
    """Entry point — starts the blocking scheduler."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    logger.info("DataFlow Scheduler starting up…")

    scheduler = build_scheduler()

    # Graceful shutdown on Ctrl+C or SIGTERM
    def _shutdown(signum, frame):
        logger.info("Shutdown signal received — stopping scheduler")
        scheduler.shutdown(wait=False)
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped")


if __name__ == "__main__":
    main()
