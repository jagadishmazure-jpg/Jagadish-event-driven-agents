"""In-process pub/sub event bus: the simulated event mesh.

Events are dicts with ``event_id``, ``timestamp``, ``topic`` and ``payload``.
Handlers that raise are isolated: the event plus the error go to the
dead-letter queue and the bus keeps running.

Production path: replace this bus with a durable broker (Kafka, Azure Event
Hubs, or a managed event-mesh product) for persistence, cross-process
delivery and exactly-once semantics. The subscriber/handler contract stays
the same.
"""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional


@dataclass
class DeadLetter:
    event: Dict[str, Any]
    error: str
    handler_name: str


@dataclass
class _Subscription:
    sub_id: str
    topic: str
    handler: Callable[[Dict[str, Any]], None]
    filter: Optional[Callable[[Dict[str, Any]], bool]] = None


class EventBus:
    """Synchronous in-process pub/sub bus with filters, replay and DLQ."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._subs: List[_Subscription] = []
        self._history: Dict[str, List[Dict[str, Any]]] = {}
        self._dead_letters: List[DeadLetter] = []

    # -- publishing -----------------------------------------------------
    def publish(self, topic: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Publish an event; returns the envelope delivered to subscribers."""
        event = {
            "event_id": str(uuid.uuid4()),
            "timestamp": time.time(),
            "topic": topic,
            "payload": dict(payload),
        }
        with self._lock:
            self._history.setdefault(topic, []).append(event)
            subs = list(self._subs)
        for sub in subs:
            if not self._topic_matches(sub.topic, topic):
                continue
            if sub.filter is not None:
                try:
                    if not sub.filter(event):
                        continue
                except Exception as exc:  # a broken filter must not kill the bus
                    self._to_dead_letter(event, sub, f"filter error: {exc}")
                    continue
            try:
                sub.handler(event)
            except Exception as exc:
                self._to_dead_letter(event, sub, f"handler error: {exc}")
        return event

    # -- subscribing ----------------------------------------------------
    def subscribe(
        self,
        topic: str,
        handler: Callable[[Dict[str, Any]], None],
        filter: Optional[Callable[[Dict[str, Any]], bool]] = None,
    ) -> str:
        """Subscribe; ``topic`` may end in ``.*`` as a single-level wildcard."""
        sub = _Subscription(sub_id=str(uuid.uuid4()), topic=topic,
                            handler=handler, filter=filter)
        with self._lock:
            self._subs.append(sub)
        return sub.sub_id

    def unsubscribe(self, sub_id: str) -> bool:
        with self._lock:
            before = len(self._subs)
            self._subs = [s for s in self._subs if s.sub_id != sub_id]
            return len(self._subs) < before

    # -- replay / history -----------------------------------------------
    def replay(self, topic: str) -> List[Dict[str, Any]]:
        """Return every event ever published on ``topic`` (late-subscriber catch-up).

        Wildcard topics are expanded: ``sensors.*`` replays all matching topics.
        """
        with self._lock:
            if topic.endswith(".*"):
                prefix = topic[:-2]
                out: List[Dict[str, Any]] = []
                for t, events in self._history.items():
                    if t == prefix or t.startswith(prefix + "."):
                        out.extend(events)
                return sorted(out, key=lambda e: e["timestamp"])
            return list(self._history.get(topic, []))

    def history(self, topic: str) -> List[Dict[str, Any]]:
        return self.replay(topic)

    @property
    def dead_letters(self) -> List[DeadLetter]:
        with self._lock:
            return list(self._dead_letters)

    # -- internals ------------------------------------------------------
    @staticmethod
    def _topic_matches(pattern: str, topic: str) -> bool:
        if pattern == topic:
            return True
        if pattern.endswith(".*"):
            prefix = pattern[:-2]
            return topic == prefix or topic.startswith(prefix + ".")
        return False

    def _to_dead_letter(
        self, event: Dict[str, Any], sub: _Subscription, error: str
    ) -> None:
        name = getattr(sub.handler, "__name__", type(sub.handler).__name__)
        with self._lock:
            self._dead_letters.append(
                DeadLetter(event=event, error=error, handler_name=name)
            )
