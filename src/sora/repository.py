"""SQLite persistence for Sora."""

from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterator

from src.sora.domain import (
    AlertDirection,
    AlertEvent,
    AlertMetric,
    AlertRule,
    AnalysisRecord,
    AnalysisResult,
    Asset,
    AssetOverview,
    AssetType,
    Market,
    NotificationEvent,
    NotificationRecord,
    NotificationStatus,
    PortfolioOverview,
    PortfolioPositionOverview,
    RunRecord,
    Snapshot,
    SnapshotRecord,
)


class SQLiteRepository:
    def __init__(self, db_path: str) -> None:
        self.db_path = str(Path(db_path).resolve())
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def initialize(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS watchlist_assets (
                    code TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    asset_type TEXT NOT NULL,
                    market TEXT NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    baseline_value REAL,
                    baseline_at TEXT,
                    position_units REAL,
                    position_cost_amount REAL
                );

                CREATE TABLE IF NOT EXISTS monitoring_runs (
                    run_id TEXT PRIMARY KEY,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    total_assets INTEGER NOT NULL DEFAULT 0,
                    processed_assets INTEGER NOT NULL DEFAULT 0,
                    successful_assets INTEGER NOT NULL DEFAULT 0,
                    failed_assets INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL,
                    error_message TEXT
                );

                CREATE TABLE IF NOT EXISTS snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    asset_code TEXT NOT NULL,
                    as_of TEXT NOT NULL,
                    current_value REAL NOT NULL,
                    previous_close REAL NOT NULL,
                    daily_change_pct REAL NOT NULL,
                    change_7d_pct REAL,
                    change_30d_pct REAL,
                    source TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS analysis_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    asset_code TEXT NOT NULL,
                    trend TEXT NOT NULL,
                    risk_level TEXT NOT NULL,
                    score REAL NOT NULL,
                    summary TEXT NOT NULL,
                    metrics_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS alert_rules (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    asset_code TEXT,
                    metric TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    threshold REAL NOT NULL,
                    channels_json TEXT NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS alert_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    asset_code TEXT NOT NULL,
                    asset_name TEXT NOT NULL,
                    rule_id INTEGER,
                    metric TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    threshold REAL NOT NULL,
                    metric_value REAL NOT NULL,
                    message TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS notification_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    alert_event_id INTEGER,
                    channel TEXT NOT NULL,
                    status TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    sent_at TEXT,
                    error_message TEXT,
                    attempt_count INTEGER NOT NULL DEFAULT 0
                );

                CREATE INDEX IF NOT EXISTS idx_snapshots_run_id ON snapshots(run_id);
                CREATE INDEX IF NOT EXISTS idx_snapshots_asset_code ON snapshots(asset_code);
                CREATE INDEX IF NOT EXISTS idx_analysis_history_run_id ON analysis_history(run_id);
                CREATE INDEX IF NOT EXISTS idx_analysis_history_asset_code ON analysis_history(asset_code);
                CREATE INDEX IF NOT EXISTS idx_alert_rules_asset_code ON alert_rules(asset_code);
                CREATE INDEX IF NOT EXISTS idx_alert_events_run_id ON alert_events(run_id);
                CREATE INDEX IF NOT EXISTS idx_alert_events_asset_code ON alert_events(asset_code);
                CREATE INDEX IF NOT EXISTS idx_notification_events_alert_event_id ON notification_events(alert_event_id);
                """
            )
            self._ensure_column(
                conn,
                table_name="notification_events",
                column_name="sent_at",
                ddl="ALTER TABLE notification_events ADD COLUMN sent_at TEXT",
            )
            self._ensure_column(
                conn,
                table_name="notification_events",
                column_name="error_message",
                ddl="ALTER TABLE notification_events ADD COLUMN error_message TEXT",
            )
            self._ensure_column(
                conn,
                table_name="notification_events",
                column_name="attempt_count",
                ddl="ALTER TABLE notification_events ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0",
            )
            self._ensure_column(
                conn,
                table_name="watchlist_assets",
                column_name="baseline_value",
                ddl="ALTER TABLE watchlist_assets ADD COLUMN baseline_value REAL",
            )
            self._ensure_column(
                conn,
                table_name="watchlist_assets",
                column_name="baseline_at",
                ddl="ALTER TABLE watchlist_assets ADD COLUMN baseline_at TEXT",
            )
            self._ensure_column(
                conn,
                table_name="watchlist_assets",
                column_name="position_units",
                ddl="ALTER TABLE watchlist_assets ADD COLUMN position_units REAL",
            )
            self._ensure_column(
                conn,
                table_name="watchlist_assets",
                column_name="position_cost_amount",
                ddl="ALTER TABLE watchlist_assets ADD COLUMN position_cost_amount REAL",
            )
            self._ensure_column(
                conn,
                table_name="monitoring_runs",
                column_name="successful_assets",
                ddl="ALTER TABLE monitoring_runs ADD COLUMN successful_assets INTEGER NOT NULL DEFAULT 0",
            )
            self._ensure_column(
                conn,
                table_name="monitoring_runs",
                column_name="failed_assets",
                ddl="ALTER TABLE monitoring_runs ADD COLUMN failed_assets INTEGER NOT NULL DEFAULT 0",
            )

    def upsert_asset(self, asset: Asset) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO watchlist_assets (
                    code, name, asset_type, market, enabled, created_at,
                    baseline_value, baseline_at, position_units, position_cost_amount
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(code) DO UPDATE SET
                    name = excluded.name,
                    asset_type = excluded.asset_type,
                    market = excluded.market,
                    enabled = excluded.enabled,
                    baseline_value = COALESCE(excluded.baseline_value, baseline_value),
                    baseline_at = COALESCE(excluded.baseline_at, baseline_at),
                    position_units = COALESCE(excluded.position_units, position_units),
                    position_cost_amount = COALESCE(
                        excluded.position_cost_amount,
                        position_cost_amount
                    )
                """,
                (
                    asset.code,
                    asset.name,
                    asset.asset_type.value,
                    asset.market.value,
                    1 if asset.enabled else 0,
                    asset.created_at.isoformat(),
                    asset.baseline_value,
                    asset.baseline_at.isoformat() if asset.baseline_at else None,
                    asset.position_units,
                    asset.position_cost_amount,
                ),
            )

    def list_assets(self, enabled_only: bool = True, code: str | None = None) -> list[Asset]:
        query = """
            SELECT
                code,
                name,
                asset_type,
                market,
                enabled,
                created_at,
                baseline_value,
                baseline_at,
                position_units,
                position_cost_amount
            FROM watchlist_assets
        """
        conditions: list[str] = []
        params: list[object] = []
        if enabled_only:
            conditions.append("enabled = 1")
        if code:
            conditions.append("code = ?")
            params.append(code)
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY code"

        with self.connect() as conn:
            rows = conn.execute(query, params).fetchall()

        return [
            Asset(
                code=row["code"],
                name=row["name"],
                asset_type=AssetType(row["asset_type"]),
                market=Market(row["market"]),
                enabled=bool(row["enabled"]),
                created_at=datetime.fromisoformat(row["created_at"]),
                baseline_value=(
                    float(row["baseline_value"])
                    if row["baseline_value"] is not None
                    else None
                ),
                baseline_at=(
                    datetime.fromisoformat(row["baseline_at"])
                    if row["baseline_at"] is not None
                    else None
                ),
                position_units=(
                    float(row["position_units"])
                    if row["position_units"] is not None
                    else None
                ),
                position_cost_amount=(
                    float(row["position_cost_amount"])
                    if row["position_cost_amount"] is not None
                    else None
                ),
            )
            for row in rows
        ]

    def get_asset(self, code: str) -> Asset | None:
        assets = self.list_assets(enabled_only=False, code=code)
        if not assets:
            return None
        return assets[0]

    def set_asset_baseline(self, code: str, baseline_value: float, baseline_at: datetime) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE watchlist_assets
                SET baseline_value = ?, baseline_at = ?
                WHERE code = ?
                """,
                (
                    baseline_value,
                    baseline_at.isoformat(),
                    code,
                ),
            )

    def set_asset_position(self, code: str, position_units: float, position_cost_amount: float) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE watchlist_assets
                SET position_units = ?, position_cost_amount = ?
                WHERE code = ?
                """,
                (
                    position_units,
                    position_cost_amount,
                    code,
                ),
            )

    def add_alert_rule(self, rule: AlertRule) -> AlertRule:
        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO alert_rules (
                    asset_code, metric, direction, threshold, channels_json, enabled, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    rule.asset_code,
                    rule.metric.value,
                    rule.direction.value,
                    rule.threshold,
                    json.dumps(rule.channels, ensure_ascii=False),
                    1 if rule.enabled else 0,
                    rule.created_at.isoformat(),
                ),
            )
            return AlertRule(
                rule_id=int(cursor.lastrowid),
                asset_code=rule.asset_code,
                metric=rule.metric,
                direction=rule.direction,
                threshold=rule.threshold,
                channels=list(rule.channels),
                enabled=rule.enabled,
                created_at=rule.created_at,
            )

    def list_alert_rules(
        self,
        *,
        enabled_only: bool = True,
        asset_code: str | None = None,
    ) -> list[AlertRule]:
        query = """
            SELECT id, asset_code, metric, direction, threshold, channels_json, enabled, created_at
            FROM alert_rules
        """
        conditions: list[str] = []
        params: list[object] = []
        if enabled_only:
            conditions.append("enabled = 1")
        if asset_code:
            conditions.append("(asset_code = ? OR asset_code IS NULL)")
            params.append(asset_code)
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY id ASC"

        with self.connect() as conn:
            rows = conn.execute(query, params).fetchall()

        rules: list[AlertRule] = []
        for row in rows:
            channels = json.loads(row["channels_json"]) if row["channels_json"] else []
            rules.append(
                AlertRule(
                    rule_id=row["id"],
                    asset_code=row["asset_code"],
                    metric=AlertMetric(row["metric"]),
                    direction=AlertDirection(row["direction"]),
                    threshold=float(row["threshold"]),
                    channels=channels,
                    enabled=bool(row["enabled"]),
                    created_at=datetime.fromisoformat(row["created_at"]),
                )
            )
        return rules

    def get_run(self, run_id: str) -> RunRecord | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT
                    run_id,
                    started_at,
                    finished_at,
                    total_assets,
                    processed_assets,
                    successful_assets,
                    failed_assets,
                    status,
                    error_message
                FROM monitoring_runs
                WHERE run_id = ?
                """,
                (run_id,),
            ).fetchone()
        return self._build_run_record(row)

    def start_run(self, total_assets: int) -> str:
        run_id = uuid.uuid4().hex
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO monitoring_runs (run_id, started_at, total_assets, processed_assets, status)
                VALUES (?, ?, ?, 0, 'running')
                """,
                (run_id, datetime.utcnow().isoformat(), total_assets),
            )
        return run_id

    def get_running_run_id(self) -> str | None:
        run = self.get_running_run()
        if run is None:
            return None
        return run.run_id

    def get_running_run(self) -> RunRecord | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT
                    run_id,
                    started_at,
                    finished_at,
                    total_assets,
                    processed_assets,
                    successful_assets,
                    failed_assets,
                    status,
                    error_message
                FROM monitoring_runs
                WHERE status = 'running'
                ORDER BY started_at DESC
                LIMIT 1
                """
            ).fetchone()
        return self._build_run_record(row)

    def get_latest_run(self) -> RunRecord | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT
                    run_id,
                    started_at,
                    finished_at,
                    total_assets,
                    processed_assets,
                    successful_assets,
                    failed_assets,
                    status,
                    error_message
                FROM monitoring_runs
                ORDER BY started_at DESC
                LIMIT 1
                """
            ).fetchone()
        return self._build_run_record(row)

    def get_latest_finished_run(self) -> RunRecord | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT
                    run_id,
                    started_at,
                    finished_at,
                    total_assets,
                    processed_assets,
                    successful_assets,
                    failed_assets,
                    status,
                    error_message
                FROM monitoring_runs
                WHERE status != 'running'
                ORDER BY started_at DESC
                LIMIT 1
                """
            ).fetchone()
        return self._build_run_record(row)

    def list_runs(self, limit: int = 20) -> list[RunRecord]:
        if limit <= 0:
            raise ValueError("limit must be greater than 0")
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    run_id,
                    started_at,
                    finished_at,
                    total_assets,
                    processed_assets,
                    successful_assets,
                    failed_assets,
                    status,
                    error_message
                FROM monitoring_runs
                ORDER BY started_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [record for row in rows if (record := self._build_run_record(row)) is not None]

    def get_asset_overview(self, asset_code: str) -> AssetOverview | None:
        asset = self.get_asset(asset_code)
        if asset is None:
            return None

        snapshot = self.get_latest_snapshot(asset_code)
        analysis = self.get_latest_analysis(
            asset_code,
            run_id=snapshot.run_id if snapshot is not None else None,
        )
        run_id = snapshot.run_id if snapshot is not None else (analysis.run_id if analysis is not None else None)
        latest_run = self.get_run(run_id) if run_id is not None else None
        return AssetOverview(
            asset=asset,
            latest_run=latest_run,
            snapshot=snapshot,
            analysis=analysis,
        )

    def list_portfolio_positions(
        self,
        *,
        enabled_only: bool = True,
    ) -> list[PortfolioPositionOverview]:
        positions: list[PortfolioPositionOverview] = []
        for asset in self.list_assets(enabled_only=enabled_only):
            if asset.position_units is None or asset.position_cost_amount is None:
                continue
            snapshot = self.get_latest_snapshot(asset.code)
            analysis = self.get_latest_analysis(
                asset.code,
                run_id=snapshot.run_id if snapshot is not None else None,
            )
            positions.append(
                PortfolioPositionOverview(
                    asset=asset,
                    snapshot=snapshot,
                    analysis=analysis,
                )
            )
        return positions

    def get_portfolio_overview(
        self,
        *,
        enabled_only: bool = True,
    ) -> PortfolioOverview:
        positions = self.list_portfolio_positions(enabled_only=enabled_only)
        priced_positions = [
            position for position in positions if position.market_value() is not None
        ]
        baseline_positions = [
            position
            for position in priced_positions
            if position.entry_market_value() is not None
        ]

        total_cost_amount = sum(
            position.asset.position_cost_amount or 0.0 for position in positions
        )
        priced_cost_amount = sum(
            position.asset.position_cost_amount or 0.0 for position in priced_positions
        )
        total_market_value = sum(
            position.market_value() or 0.0 for position in priced_positions
        )
        total_unrealized_pnl_amount = sum(
            position.unrealized_pnl_amount() or 0.0 for position in priced_positions
        )
        total_unrealized_pnl_pct = (
            (total_unrealized_pnl_amount / priced_cost_amount) * 100
            if priced_cost_amount > 0
            else None
        )
        total_daily_pnl_amount = sum(
            position.daily_pnl_amount() or 0.0 for position in priced_positions
        )
        total_entry_value_amount = sum(
            position.entry_market_value() or 0.0 for position in baseline_positions
        )
        total_since_entry_pnl_amount = sum(
            position.since_entry_pnl_amount() or 0.0
            for position in baseline_positions
        )
        total_since_entry_pnl_pct = (
            (total_since_entry_pnl_amount / total_entry_value_amount) * 100
            if total_entry_value_amount > 0
            else None
        )
        return PortfolioOverview(
            positions=positions,
            total_positioned_assets=len(positions),
            assets_with_market_data=len(priced_positions),
            assets_with_entry_baseline=len(baseline_positions),
            total_cost_amount=total_cost_amount,
            total_market_value=total_market_value,
            total_unrealized_pnl_amount=total_unrealized_pnl_amount,
            total_unrealized_pnl_pct=total_unrealized_pnl_pct,
            total_daily_pnl_amount=total_daily_pnl_amount,
            total_entry_value_amount=total_entry_value_amount,
            total_since_entry_pnl_amount=total_since_entry_pnl_amount,
            total_since_entry_pnl_pct=total_since_entry_pnl_pct,
        )

    def get_latest_snapshot(self, asset_code: str) -> SnapshotRecord | None:
        snapshots = self.list_snapshots(asset_code=asset_code, limit=1)
        if not snapshots:
            return None
        return snapshots[0]

    def list_snapshots(self, *, asset_code: str, limit: int = 20) -> list[SnapshotRecord]:
        if limit <= 0:
            raise ValueError("limit must be greater than 0")
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    run_id,
                    asset_code,
                    as_of,
                    current_value,
                    previous_close,
                    daily_change_pct,
                    change_7d_pct,
                    change_30d_pct,
                    source,
                    created_at
                FROM snapshots
                WHERE asset_code = ?
                ORDER BY as_of DESC, id DESC
                LIMIT ?
                """,
                (asset_code, limit),
            ).fetchall()
        return [self._build_snapshot_record(row) for row in rows]

    def get_latest_analysis(self, asset_code: str, *, run_id: str | None = None) -> AnalysisRecord | None:
        query = """
            SELECT
                run_id,
                asset_code,
                trend,
                risk_level,
                score,
                summary,
                metrics_json,
                created_at
            FROM analysis_history
            WHERE asset_code = ?
        """
        params: list[object] = [asset_code]
        if run_id is not None:
            query += " AND run_id = ?"
            params.append(run_id)
        query += " ORDER BY created_at DESC, id DESC LIMIT 1"

        with self.connect() as conn:
            row = conn.execute(query, params).fetchone()
        if row is None:
            return None
        return self._build_analysis_record(row)

    def list_alert_events(
        self,
        *,
        asset_code: str | None = None,
        limit: int = 20,
    ) -> list[AlertEvent]:
        if limit <= 0:
            raise ValueError("limit must be greater than 0")

        query = """
            SELECT
                id,
                run_id,
                asset_code,
                asset_name,
                rule_id,
                metric,
                direction,
                threshold,
                metric_value,
                message,
                created_at
            FROM alert_events
        """
        params: list[object] = []
        if asset_code:
            query += " WHERE asset_code = ?"
            params.append(asset_code)
        query += " ORDER BY id DESC LIMIT ?"
        params.append(limit)

        with self.connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [self._build_alert_event(row) for row in rows]

    def list_notification_records(
        self,
        *,
        statuses: tuple[NotificationStatus, ...] | None = None,
        asset_code: str | None = None,
        limit: int = 100,
    ) -> list[NotificationRecord]:
        if limit <= 0:
            raise ValueError("limit must be greater than 0")

        resolved_statuses = statuses or tuple(NotificationStatus)
        if not resolved_statuses:
            raise ValueError("statuses must not be empty")

        placeholders = ", ".join("?" for _ in resolved_statuses)
        query = f"""
            SELECT
                ne.id,
                ne.alert_event_id,
                ae.run_id,
                ae.asset_code,
                ae.asset_name,
                ne.channel,
                ne.status,
                ne.payload_json,
                ne.created_at,
                ne.sent_at,
                ne.error_message,
                ne.attempt_count
            FROM notification_events ne
            LEFT JOIN alert_events ae ON ae.id = ne.alert_event_id
            WHERE ne.status IN ({placeholders})
        """
        params: list[object] = [*(status.value for status in resolved_statuses)]
        if asset_code:
            query += " AND ae.asset_code = ?"
            params.append(asset_code)
        query += " ORDER BY ne.id DESC LIMIT ?"
        params.append(limit)

        with self.connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [self._build_notification_record(row) for row in rows]

    def list_notification_events(
        self,
        *,
        statuses: tuple[NotificationStatus, ...] = (NotificationStatus.PENDING,),
        limit: int = 100,
    ) -> list[NotificationEvent]:
        if limit <= 0:
            raise ValueError("limit must be greater than 0")
        if not statuses:
            raise ValueError("statuses must not be empty")

        placeholders = ", ".join("?" for _ in statuses)
        with self.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT
                    id,
                    alert_event_id,
                    channel,
                    status,
                    payload_json,
                    created_at,
                    sent_at,
                    error_message,
                    attempt_count
                FROM notification_events
                WHERE status IN ({placeholders})
                ORDER BY id ASC
                LIMIT ?
                """,
                [*(status.value for status in statuses), limit],
            ).fetchall()
        return [self._build_notification_event(row) for row in rows]

    def mark_notification_sent(self, notification_id: int, *, sent_at: datetime | None = None) -> None:
        resolved_sent_at = sent_at or datetime.utcnow()
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE notification_events
                SET status = ?, sent_at = ?, error_message = NULL, attempt_count = attempt_count + 1
                WHERE id = ?
                """,
                (
                    NotificationStatus.SENT.value,
                    resolved_sent_at.isoformat(),
                    notification_id,
                ),
            )

    def mark_notification_failed(self, notification_id: int, error_message: str) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE notification_events
                SET status = ?, error_message = ?, attempt_count = attempt_count + 1
                WHERE id = ?
                """,
                (
                    NotificationStatus.FAILED.value,
                    error_message,
                    notification_id,
                ),
            )

    def finish_run(
        self,
        run_id: str,
        processed_assets: int,
        successful_assets: int,
        failed_assets: int,
        status: str,
        error_message: str | None = None,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE monitoring_runs
                SET finished_at = ?, processed_assets = ?, successful_assets = ?, failed_assets = ?,
                    status = ?, error_message = ?
                WHERE run_id = ?
                """,
                (
                    datetime.utcnow().isoformat(),
                    processed_assets,
                    successful_assets,
                    failed_assets,
                    status,
                    error_message,
                    run_id,
                ),
            )

    def save_snapshot(self, run_id: str, snapshot: Snapshot) -> None:
        with self.connect() as conn:
            self._insert_snapshot(conn, run_id, snapshot)

    def save_analysis(self, result: AnalysisResult) -> None:
        with self.connect() as conn:
            self._insert_analysis(conn, result)

    def save_notification_event(self, event: NotificationEvent) -> NotificationEvent:
        with self.connect() as conn:
            return self._insert_notification_event(conn, event)

    def save_result(self, result: AnalysisResult) -> None:
        with self.connect() as conn:
            self._insert_snapshot(conn, result.run_id, result.snapshot)
            self._insert_analysis(conn, result)

    def save_run_artifacts(
        self,
        result: AnalysisResult,
        alert_events: list[AlertEvent],
        notification_events: list[NotificationEvent],
    ) -> tuple[list[AlertEvent], list[NotificationEvent]]:
        with self.connect() as conn:
            self._insert_snapshot(conn, result.run_id, result.snapshot)
            self._insert_analysis(conn, result)

            persisted_alert_events: list[AlertEvent] = []
            for event in alert_events:
                persisted_alert_events.append(self._insert_alert_event(conn, event))

            alert_id_by_correlation_key = {
                event.correlation_key: event.event_id
                for event in persisted_alert_events
                if event.correlation_key and event.event_id is not None
            }
            default_alert_id = (
                persisted_alert_events[0].event_id
                if len(persisted_alert_events) == 1
                else None
            )
            persisted_notification_events: list[NotificationEvent] = []

            for notification in notification_events:
                resolved_alert_event_id = notification.alert_event_id
                if resolved_alert_event_id is None and notification.correlation_key:
                    resolved_alert_event_id = alert_id_by_correlation_key.get(
                        notification.correlation_key
                    )
                if resolved_alert_event_id is None:
                    resolved_alert_event_id = default_alert_id
                persisted_notification_events.append(
                    self._insert_notification_event(
                        conn,
                        NotificationEvent(
                            channel=notification.channel,
                            payload=dict(notification.payload),
                            status=notification.status,
                            alert_event_id=resolved_alert_event_id,
                            correlation_key=notification.correlation_key,
                            created_at=notification.created_at,
                            notification_id=notification.notification_id,
                        ),
                    )
                )

            return persisted_alert_events, persisted_notification_events

    def _insert_snapshot(self, conn: sqlite3.Connection, run_id: str, snapshot: Snapshot) -> None:
        conn.execute(
            """
            INSERT INTO snapshots (
                run_id, asset_code, as_of, current_value, previous_close,
                daily_change_pct, change_7d_pct, change_30d_pct, source, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                snapshot.asset.code,
                snapshot.as_of.isoformat(),
                snapshot.current_value,
                snapshot.previous_close,
                snapshot.daily_change_pct,
                snapshot.change_7d_pct,
                snapshot.change_30d_pct,
                snapshot.source,
                datetime.utcnow().isoformat(),
            ),
        )

    def _insert_analysis(self, conn: sqlite3.Connection, result: AnalysisResult) -> None:
        conn.execute(
            """
            INSERT INTO analysis_history (
                run_id, asset_code, trend, risk_level, score,
                summary, metrics_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                result.run_id,
                result.asset.code,
                result.trend,
                result.risk_level,
                result.score,
                result.summary,
                json.dumps(result.metrics, ensure_ascii=False),
                datetime.utcnow().isoformat(),
            ),
        )

    def _insert_alert_event(
        self,
        conn: sqlite3.Connection,
        event: AlertEvent,
    ) -> AlertEvent:
        cursor = conn.execute(
            """
            INSERT INTO alert_events (
                run_id, asset_code, asset_name, rule_id, metric, direction,
                threshold, metric_value, message, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.run_id,
                event.asset_code,
                event.asset_name,
                event.rule_id,
                event.metric.value,
                event.direction.value,
                event.threshold,
                event.metric_value,
                event.message,
                event.created_at.isoformat(),
            ),
        )
        return AlertEvent(
            event_id=int(cursor.lastrowid),
            run_id=event.run_id,
            asset_code=event.asset_code,
            asset_name=event.asset_name,
            rule_id=event.rule_id,
            metric=event.metric,
            direction=event.direction,
            threshold=event.threshold,
            metric_value=event.metric_value,
            message=event.message,
            correlation_key=event.correlation_key,
            created_at=event.created_at,
        )

    def _insert_notification_event(
        self,
        conn: sqlite3.Connection,
        event: NotificationEvent,
    ) -> NotificationEvent:
        cursor = conn.execute(
            """
            INSERT INTO notification_events (
                alert_event_id, channel, status, payload_json, created_at, sent_at, error_message, attempt_count
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.alert_event_id,
                event.channel,
                event.status.value,
                json.dumps(event.payload, ensure_ascii=False),
                event.created_at.isoformat(),
                event.sent_at.isoformat() if event.sent_at else None,
                event.error_message,
                event.attempt_count,
            ),
        )
        return NotificationEvent(
            notification_id=int(cursor.lastrowid),
            alert_event_id=event.alert_event_id,
            channel=event.channel,
            status=NotificationStatus(event.status),
            payload=dict(event.payload),
            correlation_key=event.correlation_key,
            created_at=event.created_at,
            sent_at=event.sent_at,
            error_message=event.error_message,
            attempt_count=event.attempt_count,
        )

    def _ensure_column(
        self,
        conn: sqlite3.Connection,
        table_name: str,
        column_name: str,
        ddl: str,
    ) -> None:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        columns = {row["name"] for row in rows}
        if column_name not in columns:
            conn.execute(ddl)

    @staticmethod
    def _build_run_record(row: sqlite3.Row | None) -> RunRecord | None:
        if row is None:
            return None
        return RunRecord(
            run_id=str(row["run_id"]),
            started_at=datetime.fromisoformat(row["started_at"]),
            finished_at=(
                datetime.fromisoformat(row["finished_at"])
                if row["finished_at"] is not None
                else None
            ),
            total_assets=int(row["total_assets"]),
            processed_assets=int(row["processed_assets"]),
            successful_assets=int(row["successful_assets"]),
            failed_assets=int(row["failed_assets"]),
            status=str(row["status"]),
            error_message=str(row["error_message"]) if row["error_message"] is not None else None,
        )

    @staticmethod
    def _build_notification_event(row: sqlite3.Row) -> NotificationEvent:
        return NotificationEvent(
            notification_id=int(row["id"]),
            alert_event_id=(
                int(row["alert_event_id"])
                if row["alert_event_id"] is not None
                else None
            ),
            channel=str(row["channel"]),
            status=NotificationStatus(row["status"]),
            payload=json.loads(row["payload_json"]) if row["payload_json"] else {},
            created_at=datetime.fromisoformat(row["created_at"]),
            sent_at=(
                datetime.fromisoformat(row["sent_at"])
                if row["sent_at"] is not None
                else None
            ),
            error_message=str(row["error_message"]) if row["error_message"] is not None else None,
            attempt_count=int(row["attempt_count"]),
        )

    @staticmethod
    def _build_snapshot_record(row: sqlite3.Row) -> SnapshotRecord:
        return SnapshotRecord(
            run_id=str(row["run_id"]),
            asset_code=str(row["asset_code"]),
            as_of=datetime.fromisoformat(row["as_of"]),
            current_value=float(row["current_value"]),
            previous_close=float(row["previous_close"]),
            daily_change_pct=float(row["daily_change_pct"]),
            change_7d_pct=(
                float(row["change_7d_pct"])
                if row["change_7d_pct"] is not None
                else None
            ),
            change_30d_pct=(
                float(row["change_30d_pct"])
                if row["change_30d_pct"] is not None
                else None
            ),
            source=str(row["source"]),
            created_at=datetime.fromisoformat(row["created_at"]),
        )

    @staticmethod
    def _build_analysis_record(row: sqlite3.Row) -> AnalysisRecord:
        return AnalysisRecord(
            run_id=str(row["run_id"]),
            asset_code=str(row["asset_code"]),
            trend=str(row["trend"]),
            risk_level=str(row["risk_level"]),
            score=float(row["score"]),
            summary=str(row["summary"]),
            metrics=json.loads(row["metrics_json"]) if row["metrics_json"] else {},
            created_at=datetime.fromisoformat(row["created_at"]),
        )

    @staticmethod
    def _build_alert_event(row: sqlite3.Row) -> AlertEvent:
        return AlertEvent(
            event_id=int(row["id"]),
            run_id=str(row["run_id"]),
            asset_code=str(row["asset_code"]),
            asset_name=str(row["asset_name"]),
            rule_id=int(row["rule_id"]) if row["rule_id"] is not None else None,
            metric=AlertMetric(row["metric"]),
            direction=AlertDirection(row["direction"]),
            threshold=float(row["threshold"]),
            metric_value=float(row["metric_value"]),
            message=str(row["message"]),
            created_at=datetime.fromisoformat(row["created_at"]),
        )

    @staticmethod
    def _build_notification_record(row: sqlite3.Row) -> NotificationRecord:
        return NotificationRecord(
            notification_id=int(row["id"]),
            alert_event_id=(
                int(row["alert_event_id"])
                if row["alert_event_id"] is not None
                else None
            ),
            run_id=str(row["run_id"]) if row["run_id"] is not None else None,
            asset_code=str(row["asset_code"]) if row["asset_code"] is not None else None,
            asset_name=str(row["asset_name"]) if row["asset_name"] is not None else None,
            channel=str(row["channel"]),
            status=NotificationStatus(row["status"]),
            payload=json.loads(row["payload_json"]) if row["payload_json"] else {},
            created_at=datetime.fromisoformat(row["created_at"]),
            sent_at=(
                datetime.fromisoformat(row["sent_at"])
                if row["sent_at"] is not None
                else None
            ),
            error_message=str(row["error_message"]) if row["error_message"] is not None else None,
            attempt_count=int(row["attempt_count"]),
        )
