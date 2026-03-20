"""Webhook-based notification delivery."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, Callable
from urllib.request import Request, urlopen

from src.sora.domain import NotificationEvent


class WebhookNotifier:
    def __init__(
        self,
        webhook_urls: Mapping[str, str],
        *,
        timeout_seconds: float = 10.0,
        send_func: Callable[..., Any] = urlopen,
    ) -> None:
        self.webhook_urls = {str(channel): str(url) for channel, url in webhook_urls.items()}
        self.timeout_seconds = timeout_seconds
        self.send_func = send_func

    def supports(self, channel: str) -> bool:
        return channel in self.webhook_urls

    def send(self, event: NotificationEvent) -> None:
        if not self.supports(event.channel):
            raise ValueError(f"No webhook configured for channel: {event.channel}")

        request = Request(
            self.webhook_urls[event.channel],
            data=json.dumps(event.payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json; charset=utf-8"},
            method="POST",
        )
        response = self.send_func(request, timeout=self.timeout_seconds)
        if hasattr(response, "__enter__"):
            with response as handled_response:
                self._ensure_success(handled_response)
            return
        self._ensure_success(response)

    @staticmethod
    def _ensure_success(response: Any) -> None:
        status = getattr(response, "status", None)
        if status is None and hasattr(response, "getcode"):
            status = response.getcode()
        if status is None:
            return
        if int(status) >= 400:
            raise RuntimeError(f"Webhook responded with HTTP {status}")
