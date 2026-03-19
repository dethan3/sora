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
    AnalysisResult,
    Asset,
    AssetType,
    Market,
    NotificationEvent,
    NotificationStatus,
    Snapshot,
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
                    created_at TEXT NOT NULL
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
                    created_at TEXT NOT NULL
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
                INSERT INTO watchlist_assets (code, name, asset_type, market, enabled, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(code) DO UPDATE SET
                    name = excluded.name,
                    asset_type = excluded.asset_type,
                    market = excluded.market,
                    enabled = excluded.enabled
                """,
                (
                    asset.code,
                    asset.name,
                    asset.asset_type.value,
                    asset.market.value,
                    1 if asset.enabled else 0,
                    asset.created_at.isoformat(),
                ),
            )

    def list_assets(self, enabled_only: bool = True, code: str | None = None) -> list[Asset]:
        query = """
            SELECT code, name, asset_type, market, enabled, created_at
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
            )
            for row in rows
        ]

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
                alert_event_id, channel, status, payload_json, created_at
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                event.alert_event_id,
                event.channel,
                event.status.value,
                json.dumps(event.payload, ensure_ascii=False),
                event.created_at.isoformat(),
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
