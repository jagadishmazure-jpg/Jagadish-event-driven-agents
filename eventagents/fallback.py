"""Degraded-mode fallback agents.

``with_fallback`` wraps a primary handler on a topic: if the primary
raises, the event still lands in the bus's dead-letter queue (the wrapper
re-raises, so the bus's own DLQ machinery records it under the agent's
name) and a fallback handler publishes a degraded-but-safe action instead
of leaving the event unhandled.

FallbackMonitor tracks primary vs fallback invocations per agent so
failover stats are visible: a fallback firing in production is a page,
not a silent save.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Callable, Dict, Optional

from .bus import EventBus


class FallbackMonitor:
    """Counts primary vs fallback invocations per agent name."""

    def __init__(self) -> None:
        self._counts: Dict[str, Dict[str, int]] = defaultdict(
            lambda: {"primary": 0, "fallback": 0})

    def record_primary(self, name: str) -> None:
        self._counts[name]["primary"] += 1

    def record_fallback(self, name: str) -> None:
        self._counts[name]["fallback"] += 1

    def report(self) -> Dict[str, Dict[str, int]]:
        """Failover stats: ``{agent: {"primary": n, "fallback": m}}``."""
        return {name: dict(counts)
                for name, counts in self._counts.items()}


def with_fallback(
    bus: EventBus,
    topic: str,
    primary_handler: Callable[[Dict], None],
    fallback_handler: Callable[[Dict, Exception], None],
    name: str,
    monitor: Optional[FallbackMonitor] = None,
    filter: Optional[Callable[[Dict], bool]] = None,
) -> str:
    """Subscribe ``topic`` with primary + degraded fallback.

    On primary exception the fallback runs first (publishing the safe
    degraded action), then the exception is re-raised so the bus records
    the dead letter. Returns the subscription id.
    """

    def wrapped(event: Dict) -> None:
        if monitor is not None:
            monitor.record_primary(name)
        try:
            primary_handler(event)
        except Exception as exc:
            if monitor is not None:
                monitor.record_fallback(name)
            fallback_handler(event, exc)
            raise

    wrapped.__name__ = f"fallback_wrapped[{name}]"
    return bus.subscribe(topic, wrapped, filter=filter)


def degraded_quality_fallback(bus: EventBus) -> Callable[[Dict, Exception], None]:
    """Fallback handler factory for the quality agent.

    If the quality agent crashes mid-stream, the safe degraded posture is:
    defer the automated action (publish ``quality.action_deferred``) and
    raise a manual-inspection work order so a human looks at the equipment.
    """

    def fallback(event: Dict, exc: Exception) -> None:
        p = event["payload"]
        bus.publish("quality.action_deferred", {
            "action": "quality_action",
            "equipment_id": p.get("equipment_id"),
            "batch_id": p.get("batch_id"),
            "reason": (f"primary quality handler failed ({exc}); automated "
                       f"action deferred, manual inspection required"),
            "deferral": "primary_failed",
            "decided_by": "fallback-agent",
        })
        bus.publish("maintenance.work_orders", {
            "equipment_id": p.get("equipment_id"),
            "failure_mode": "manual_inspection",
            "urgency": "medium",
            "trigger": f"quality agent failure on {p.get('sensor')} sensor",
            "recommended_action": ("Quality agent unavailable; manually "
                                   "inspect equipment before releasing batches"),
            "status": "open",
        })

    fallback.__name__ = "degraded_quality_fallback"
    return fallback
