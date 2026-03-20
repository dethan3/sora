from urllib.error import URLError

import pytest

from src.sora.domain import NotificationEvent, NotificationStatus
from src.sora.notifications import NotificationDispatcher
from src.sora.notifiers import WebhookNotifier
from src.sora.repository import SQLiteRepository


class StubNotifier:
    def __init__(self, *, channels: set[str], should_fail: bool = False) -> None:
        self.channels = channels
        self.should_fail = should_fail
        self.sent_events: list[NotificationEvent] = []

    def supports(self, channel: str) -> bool:
        return channel in self.channels

    def send(self, event: NotificationEvent) -> None:
        self.sent_events.append(event)
        if self.should_fail:
            raise RuntimeError("send failed")


class DummyResponse:
    def __init__(self, status: int = 200) -> None:
        self.status = status

    def __enter__(self) -> "DummyResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


def test_notification_dispatcher_marks_sent_and_failed(tmp_path):
    repository = SQLiteRepository(str(tmp_path / "sora.db"))
    repository.initialize()
    first = repository.save_notification_event(
        NotificationEvent(channel="feishu", payload={"message": "ok"})
    )
    second = repository.save_notification_event(
        NotificationEvent(channel="telegram", payload={"message": "fail"})
    )
    assert first.notification_id is not None
    assert second.notification_id is not None

    dispatcher = NotificationDispatcher(
        repository=repository,
        notifiers=[
            StubNotifier(channels={"feishu"}),
            StubNotifier(channels={"telegram"}, should_fail=True),
        ],
    )

    summary = dispatcher.dispatch_pending(limit=10)
    sent = repository.list_notification_events(statuses=(NotificationStatus.SENT,))
    failed = repository.list_notification_events(statuses=(NotificationStatus.FAILED,))

    assert summary.requested == 2
    assert summary.sent == 1
    assert summary.failed == 1
    assert [event.channel for event in sent] == ["feishu"]
    assert [event.channel for event in failed] == ["telegram"]
    assert failed[0].error_message == "send failed"


def test_webhook_notifier_posts_json_payload():
    requests: list[tuple[str, bytes, dict[str, str], float]] = []

    def send_func(request, timeout: float):
        requests.append((request.full_url, request.data, dict(request.headers), timeout))
        return DummyResponse(status=200)

    notifier = WebhookNotifier(
        {"feishu": "https://example.com/webhook"},
        timeout_seconds=5.0,
        send_func=send_func,
    )

    notifier.send(NotificationEvent(channel="feishu", payload={"message": "hello"}))

    assert len(requests) == 1
    url, data, headers, timeout = requests[0]
    assert url == "https://example.com/webhook"
    assert data == b'{"message": "hello"}'
    assert headers["Content-type"] == "application/json; charset=utf-8"
    assert timeout == 5.0


def test_webhook_notifier_raises_on_transport_error():
    notifier = WebhookNotifier(
        {"feishu": "https://example.com/webhook"},
        send_func=lambda request, timeout: (_ for _ in ()).throw(URLError("down")),
    )

    with pytest.raises(URLError):
        notifier.send(NotificationEvent(channel="feishu", payload={"message": "hello"}))
