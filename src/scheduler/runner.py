"""
Scheduler example runner

Run with:
    python -m src.scheduler.runner
"""

from __future__ import annotations

import signal
import sys
from datetime import datetime, timedelta
from typing import Optional

from loguru import logger

from ..config.manager import ConfigManager
from .scheduler import TaskScheduler, ScheduledTask, TaskType


def next_weekday_time(target_weekday: int, hour: int, minute: int = 0, now: Optional[datetime] = None) -> datetime:
    """
    Get the next datetime for a specific weekday and time.
    target_weekday: Monday=0, Sunday=6
    hour/minute: 24h format
    """
    now = now or datetime.now()
    # Start from today at target time
    candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    days_ahead = (target_weekday - candidate.weekday()) % 7
    if days_ahead == 0 and candidate <= now:
        days_ahead = 7
    return candidate + timedelta(days=days_ahead)


def main():
    # 1) Load configuration
    config = ConfigManager.load_default()
    logger.info("Loaded configuration for scheduler runner")

    # 2) Create scheduler and default tasks
    scheduler = TaskScheduler(config)
    scheduler.setup_default_tasks()

    # 3) Add weekly analysis task: Monday 09:00, repeat every 7 days
    first_run = next_weekday_time(target_weekday=0, hour=9, minute=0)
    weekly_analysis = ScheduledTask(
        task_id="weekly_analysis_mon_0900",
        task_type=TaskType.ANALYSIS,
        name="每周一 09:00 量化分析",
        description="对优先基金执行每周分析并缓存结果",
        schedule_time=first_run,
        interval_minutes=7 * 24 * 60,  # every 7 days
    )
    scheduler.add_task(weekly_analysis)

    # 4) Start scheduler
    if not scheduler.start():
        logger.error("Failed to start scheduler")
        sys.exit(1)

    logger.info("Scheduler started. Press Ctrl+C to stop.")

    # 5) Graceful shutdown on SIGINT/SIGTERM
    stop = False

    def handle_signal(signum, frame):
        nonlocal stop
        logger.info(f"Received signal {signum}, stopping scheduler...")
        stop = True

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        while not stop:
            # Keep process alive; scheduler runs in a background thread
            signal.pause()  # wait for signals efficiently
    except AttributeError:
        # Windows may not have signal.pause; use a simple loop
        import time
        while not stop:
            time.sleep(1)
    finally:
        scheduler.stop()
        logger.info("Scheduler stopped. Bye.")


if __name__ == "__main__":
    main()
