"""Rule-based analysis engine for Sora V1."""

from __future__ import annotations

from statistics import pstdev

from src.sora.domain import AnalysisResult, MarketSeries, Snapshot


class AnalysisEngine:
    def __init__(self, short_window: int = 7, long_window: int = 30) -> None:
        self.short_window = short_window
        self.long_window = long_window

    def analyze(self, run_id: str, series: MarketSeries) -> AnalysisResult:
        current = series.current_value
        previous_close = series.previous_close
        values = [point.value for point in series.history]

        snapshot = Snapshot(
            asset=series.asset,
            as_of=series.as_of,
            current_value=current,
            previous_close=previous_close,
            daily_change_pct=self._pct_change(current, previous_close),
            change_7d_pct=self._window_change(values, self.short_window),
            change_30d_pct=self._window_change(values, self.long_window),
            source=series.source,
        )

        sma5 = self._moving_average(values, 5)
        sma20 = self._moving_average(values, 20)
        trend = self._classify_trend(current, sma5, sma20)
        volatility = self._estimate_volatility(values)
        risk_level = self._classify_risk(volatility)
        score = self._score(snapshot.daily_change_pct, snapshot.change_7d_pct, trend)
        summary = self._build_summary(snapshot, trend, risk_level)

        return AnalysisResult(
            run_id=run_id,
            asset=series.asset,
            snapshot=snapshot,
            trend=trend,
            risk_level=risk_level,
            score=score,
            summary=summary,
            metrics={
                "sma5": sma5,
                "sma20": sma20,
                "volatility": volatility,
                "change_7d_pct": snapshot.change_7d_pct,
                "change_30d_pct": snapshot.change_30d_pct,
            },
        )

    @staticmethod
    def _pct_change(current: float, base: float) -> float:
        if not base:
            return 0.0
        return ((current - base) / base) * 100

    def _window_change(self, values: list[float], window: int) -> float | None:
        if len(values) <= window:
            return None
        return self._pct_change(values[-1], values[-window - 1])

    @staticmethod
    def _moving_average(values: list[float], window: int) -> float | None:
        if len(values) < window:
            return None
        subset = values[-window:]
        return sum(subset) / len(subset)

    @staticmethod
    def _classify_trend(current: float, sma5: float | None, sma20: float | None) -> str:
        if sma5 is None or sma20 is None:
            return "unknown"
        if current >= sma5 >= sma20:
            return "bullish"
        if current <= sma5 <= sma20:
            return "bearish"
        return "sideways"

    @staticmethod
    def _estimate_volatility(values: list[float]) -> float:
        if len(values) < 2:
            return 0.0
        returns: list[float] = []
        for previous, current in zip(values[:-1], values[1:]):
            if previous:
                returns.append((current - previous) / previous)
        if len(returns) < 2:
            return 0.0
        return pstdev(returns) * 100

    @staticmethod
    def _classify_risk(volatility: float) -> str:
        if volatility >= 3:
            return "high"
        if volatility >= 1.5:
            return "medium"
        return "low"

    @staticmethod
    def _score(
        daily_change_pct: float,
        change_7d_pct: float | None,
        trend: str,
    ) -> float:
        trend_bonus = {"bullish": 12, "sideways": 0, "bearish": -12, "unknown": -4}[trend]
        week_component = change_7d_pct or 0.0
        score = 50 + daily_change_pct * 1.5 + week_component * 0.8 + trend_bonus
        return max(0.0, min(100.0, score))

    @staticmethod
    def _build_summary(snapshot: Snapshot, trend: str, risk_level: str) -> str:
        week = (
            f"7日 {snapshot.change_7d_pct:.2f}%"
            if snapshot.change_7d_pct is not None
            else "7日 N/A"
        )
        month = (
            f"30日 {snapshot.change_30d_pct:.2f}%"
            if snapshot.change_30d_pct is not None
            else "30日 N/A"
        )
        return (
            f"{snapshot.asset.name} 当前 {snapshot.current_value:.4f}，"
            f"日涨跌 {snapshot.daily_change_pct:.2f}% ，趋势 {trend}，"
            f"{week}，{month}，风险 {risk_level}。"
        )
