"""Alert evaluation for Sora."""

from __future__ import annotations

import uuid

from src.sora.domain import (
    AlertDirection,
    AlertEvent,
    AlertMetric,
    AlertRule,
    AnalysisResult,
    NotificationEvent,
)


class AlertEvaluator:
    """Evaluate alert rules against analysis results."""

    def evaluate(
        self,
        result: AnalysisResult,
        rules: list[AlertRule],
    ) -> tuple[list[AlertEvent], list[NotificationEvent]]:
        alert_events: list[AlertEvent] = []
        notification_events: list[NotificationEvent] = []

        for rule in rules:
            if not rule.enabled:
                continue
            if rule.asset_code and rule.asset_code != result.asset.code:
                continue

            metric_value = self._metric_value(result, rule.metric)
            if metric_value is None:
                continue
            if not self._matches(rule.direction, metric_value, rule.threshold):
                continue

            correlation_key = uuid.uuid4().hex
            alert_event = AlertEvent(
                run_id=result.run_id,
                asset_code=result.asset.code,
                asset_name=result.asset.name,
                rule_id=rule.rule_id,
                metric=rule.metric,
                direction=rule.direction,
                threshold=rule.threshold,
                metric_value=metric_value,
                message=self._message(result, rule, metric_value),
                correlation_key=correlation_key,
            )
            alert_events.append(alert_event)

            for channel in rule.channels:
                notification_events.append(
                    NotificationEvent(
                        channel=channel,
                        payload={
                            "run_id": result.run_id,
                            "asset_code": result.asset.code,
                            "asset_name": result.asset.name,
                            "metric": rule.metric.value,
                            "direction": rule.direction.value,
                            "threshold": rule.threshold,
                            "metric_value": metric_value,
                            "message": alert_event.message,
                        },
                        correlation_key=correlation_key,
                    )
                )

        return alert_events, notification_events

    def _metric_value(
        self,
        result: AnalysisResult,
        metric: AlertMetric,
    ) -> float | None:
        if metric == AlertMetric.DAILY_CHANGE_PCT:
            return result.snapshot.daily_change_pct
        if metric == AlertMetric.CHANGE_7D_PCT:
            return result.snapshot.change_7d_pct
        if metric == AlertMetric.CHANGE_30D_PCT:
            return result.snapshot.change_30d_pct
        if metric == AlertMetric.SCORE:
            return result.score
        return None

    @staticmethod
    def _matches(direction: AlertDirection, metric_value: float, threshold: float) -> bool:
        if direction == AlertDirection.ABOVE:
            return metric_value >= threshold
        return metric_value <= threshold

    @staticmethod
    def _message(result: AnalysisResult, rule: AlertRule, metric_value: float) -> str:
        relation = "above" if rule.direction == AlertDirection.ABOVE else "below"
        return (
            f"{result.asset.name} ({result.asset.code}) "
            f"{rule.metric.value} is {metric_value:.2f}, "
            f"{relation} threshold {rule.threshold:.2f}."
        )
