"""Notifier interfaces."""

from __future__ import annotations

from typing import Protocol

from src.sora.domain import NotificationEvent


class Notifier(Protocol):
    def supports(self, channel: str) -> bool:
        """Return whether the notifier can handle the given channel."""

    def send(self, event: NotificationEvent) -> None:
        """Deliver a notification event."""
