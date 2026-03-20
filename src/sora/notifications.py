"""Notification dispatch orchestration."""

from __future__ import annotations

from dataclasses import dataclass

from src.sora.domain import NotificationStatus
from src.sora.notifiers.base import Notifier
from src.sora.repository import SQLiteRepository


@dataclass(slots=True)
class NotificationDispatchSummary:
    requested: int
    sent: int
    failed: int


class NotificationDispatcher:
    def __init__(
        self,
        repository: SQLiteRepository,
        notifiers: list[Notifier],
    ) -> None:
        self.repository = repository
        self.notifiers = list(notifiers)

    def dispatch_pending(self, *, limit: int = 100) -> NotificationDispatchSummary:
        events = self.repository.list_notification_events(
            statuses=(NotificationStatus.PENDING,),
            limit=limit,
        )
        sent = 0
        failed = 0

        for event in events:
            try:
                notifier = self._resolve_notifier(event.channel)
                notifier.send(event)
                assert event.notification_id is not None
                self.repository.mark_notification_sent(event.notification_id)
                sent += 1
            except Exception as exc:  # noqa: BLE001
                assert event.notification_id is not None
                self.repository.mark_notification_failed(event.notification_id, str(exc))
                failed += 1

        return NotificationDispatchSummary(
            requested=len(events),
            sent=sent,
            failed=failed,
        )

    def _resolve_notifier(self, channel: str) -> Notifier:
        for notifier in self.notifiers:
            if notifier.supports(channel):
                return notifier
        raise ValueError(f"No notifier available for channel: {channel}")
