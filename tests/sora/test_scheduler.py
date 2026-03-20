from datetime import datetime, timedelta

from src.sora.domain import RunSummary
from src.sora.scheduler import IntervalScheduler


class StubOrchestrator:
    def __init__(self) -> None:
        self.run_count = 0

    def run_once(self, asset_code: str | None = None) -> RunSummary:
        self.run_count += 1
        return RunSummary(
            run_id=f"run-{self.run_count}",
            total_assets=0,
            processed_assets=0,
            successful_assets=0,
            failed_assets=0,
            successes=[],
            failures=[],
        )


def test_interval_scheduler_runs_only_when_due():
    orchestrator = StubOrchestrator()
    scheduler = IntervalScheduler(orchestrator=orchestrator, interval_seconds=60)
    base = datetime(2025, 1, 1, 9, 0, 0)

    first = scheduler.run_pending(base)
    not_due = scheduler.run_pending(base + timedelta(seconds=30))
    second = scheduler.run_pending(base + timedelta(seconds=60))

    assert first is not None
    assert not_due is None
    assert second is not None
    assert orchestrator.run_count == 2
    assert scheduler.state.run_count == 2
    assert scheduler.state.last_run_at == base + timedelta(seconds=60)
    assert scheduler.state.next_run_at == base + timedelta(seconds=120)


def test_interval_scheduler_run_loop_respects_sleep_until_due():
    orchestrator = StubOrchestrator()
    current = datetime(2025, 1, 1, 9, 0, 0)
    sleep_calls: list[float] = []

    def clock() -> datetime:
        return current

    def sleep_fn(seconds: float) -> None:
        nonlocal current
        sleep_calls.append(seconds)
        current = current + timedelta(seconds=seconds)

    scheduler = IntervalScheduler(
        orchestrator=orchestrator,
        interval_seconds=60,
        clock=clock,
        sleep_fn=sleep_fn,
    )

    completed_runs = scheduler.run_loop(max_iterations=2)

    assert completed_runs == 2
    assert orchestrator.run_count == 2
    assert sleep_calls == [60.0]
