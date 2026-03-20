"""Run orchestration for Sora."""

from __future__ import annotations

from src.sora.analysis import AnalysisEngine
from src.sora.alerts import AlertEvaluator
from src.sora.domain import RunSummary
from src.sora.providers.base import MarketDataProvider
from src.sora.repository import SQLiteRepository


class RunAlreadyInProgressError(RuntimeError):
    """Raised when a new monitoring run starts while another run is still active."""


class SoraOrchestrator:
    def __init__(
        self,
        repository: SQLiteRepository,
        provider: MarketDataProvider,
        engine: AnalysisEngine,
        alert_evaluator: AlertEvaluator | None = None,
        lookback_days: int = 90,
    ) -> None:
        self.repository = repository
        self.provider = provider
        self.engine = engine
        self.alert_evaluator = alert_evaluator or AlertEvaluator()
        self.lookback_days = lookback_days

    def run_once(self, asset_code: str | None = None) -> RunSummary:
        running_run_id = self.repository.get_running_run_id()
        if running_run_id is not None:
            raise RunAlreadyInProgressError(
                f"Another monitoring run is already in progress: {running_run_id}"
            )

        assets = self.repository.list_assets(enabled_only=True, code=asset_code)
        rules = self.repository.list_alert_rules(enabled_only=True)
        run_id = self.repository.start_run(total_assets=len(assets))
        successes = []
        failures = []
        alert_events = []
        notification_events = []

        for asset in assets:
            try:
                supports = getattr(self.provider, "supports", None)
                if callable(supports) and not supports(asset):
                    raise ValueError(
                        f"Unsupported asset for current provider: "
                        f"{asset.code} ({asset.market.value}/{asset.asset_type.value})"
                    )
                series = self.provider.fetch_market_series(asset, self.lookback_days)
                result = self.engine.analyze(run_id, series)
                asset_rules = [
                    rule
                    for rule in rules
                    if rule.asset_code is None or rule.asset_code == asset.code
                ]
                result_alerts, result_notifications = self.alert_evaluator.evaluate(result, asset_rules)
                persisted_alerts, persisted_notifications = self.repository.save_run_artifacts(
                    result=result,
                    alert_events=result_alerts,
                    notification_events=result_notifications,
                )
                successes.append(result)
                alert_events.extend(persisted_alerts)
                notification_events.extend(persisted_notifications)
            except Exception as exc:  # noqa: BLE001
                failures.append({"code": asset.code, "error": str(exc)})

        if failures and not successes:
            status = "failed"
        elif failures:
            status = "partial_failed"
        else:
            status = "completed"

        self.repository.finish_run(
            run_id=run_id,
            processed_assets=len(assets),
            successful_assets=len(successes),
            failed_assets=len(failures),
            status=status,
            error_message=None if not failures else f"{len(failures)} assets failed",
        )
        return RunSummary(
            run_id=run_id,
            total_assets=len(assets),
            processed_assets=len(assets),
            successful_assets=len(successes),
            failed_assets=len(failures),
            successes=successes,
            failures=failures,
            alert_events=alert_events,
            notification_events=notification_events,
        )
