"""Scheduling utilities for Sora."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from time import sleep
from typing import Callable

from src.sora.domain import RunSummary
from src.sora.orchestrator import SoraOrchestrator


@dataclass(slots=True)
class SchedulerState:
    interval_seconds: float
    next_run_at: datetime | None = None
    last_run_at: datetime | None = None
    run_count: int = 0


class IntervalScheduler:
    def __init__(
        self,
        orchestrator: SoraOrchestrator,
        interval_seconds: float,
        *,
        clock: Callable[[], datetime] = datetime.utcnow,
        sleep_fn: Callable[[float], None] = sleep,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be greater than 0")
        self.orchestrator = orchestrator
        self.clock = clock
        self.sleep_fn = sleep_fn
        self.state = SchedulerState(interval_seconds=interval_seconds)

    def is_due(self, now: datetime | None = None) -> bool:
        current = now or self.clock()
        if self.state.next_run_at is None:
            return True
        return current >= self.state.next_run_at

    def run_pending(self, now: datetime | None = None) -> RunSummary | None:
        current = now or self.clock()
        if not self.is_due(current):
            return None

        summary = self.orchestrator.run_once()
        self.state.last_run_at = current
        self.state.next_run_at = current + timedelta(seconds=self.state.interval_seconds)
        self.state.run_count += 1
        return summary

    def run_loop(self, *, max_iterations: int | None = None) -> int:
        completed_runs = 0
        while max_iterations is None or completed_runs < max_iterations:
            current = self.clock()
            if not self.is_due(current):
                assert self.state.next_run_at is not None
                remaining = (self.state.next_run_at - current).total_seconds()
                self.sleep_fn(max(remaining, 0.0))
                continue

            self.run_pending(current)
            completed_runs += 1
        return completed_runs
