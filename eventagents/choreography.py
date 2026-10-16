"""Agent-to-agent event cascades: choreography, not orchestration.

No central orchestrator dispatches work here. Agents react to each other's
events: a quality action fires a summary, the summary fires a comms routing
decision, and the CascadeTracker records the whole causal chain so any
sensor -> action -> summary -> page cascade is traceable end to end.

Manufacturing-flavored: the domain is the factory floor (sensors, batches,
lines). Every derived event carries ``root_id`` -- the sensor event that
started the cascade -- so the chain stays linked across topics.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Dict, List

from .bus import EventBus


class SummarizerAgent:
    """Turns quality actions into concise, routable summaries.

    Subscribes to ``quality.actions``; publishes to ``notify.summaries``.
    ``stop_line`` is critical, everything else is a warning.
    """

    def __init__(self, bus: EventBus) -> None:
        self.bus = bus
        self.summaries: List[Dict] = []
        bus.subscribe("quality.actions", self._on_action)

    def _on_action(self, event: Dict) -> None:
        p = event["payload"]
        severity = "critical" if p["action"] == "stop_line" else "warning"
        summary = {
            "original_event_id": event["event_id"],
            "root_id": p.get("root_id", event["event_id"]),
            "equipment_id": p["equipment_id"],
            "batch_id": p.get("batch_id"),
            "action": p["action"],
            "key_numbers": p["reason"],
            "severity": severity,
        }
        self.summaries.append(summary)
        self.bus.publish("notify.summaries", summary)


class CommsAgent:
    """Routes summaries to the right channel by severity.

    Subscribes to ``notify.summaries``. Critical -> ``comms.urgent``
    (paged); anything else -> ``comms.digest``. Every routed event cites
    what triggered it.
    """

    def __init__(self, bus: EventBus) -> None:
        self.bus = bus
        self.routed: List[Dict] = []
        bus.subscribe("notify.summaries", self._on_summary)

    def _on_summary(self, event: Dict) -> None:
        p = event["payload"]
        urgent = p["severity"] == "critical"
        routed = {
            "summary_event_id": event["event_id"],
            "root_id": p.get("root_id", event["event_id"]),
            "equipment_id": p["equipment_id"],
            "batch_id": p.get("batch_id"),
            "action": p["action"],
            "severity": p["severity"],
            "channel": "page" if urgent else "digest",
            "triggered_by": p["key_numbers"],
            "note": (f"{'PAGED' if urgent else 'digest'}: {p['action']} on "
                     f"{p['equipment_id']} (batch {p.get('batch_id')})"),
        }
        self.routed.append(routed)
        self.bus.publish("comms.urgent" if urgent else "comms.digest", routed)


class CascadeTracker:
    """Records the causal chain of every cascade by root event.

    Subscribes to ``sensors.*``, ``quality.*``, ``notify.*`` and
    ``comms.*``. Each event is filed under its ``root_id`` (the sensor
    event that started the cascade; sensor events are their own root), so
    ``chain_for(root_id)`` returns the full ordered chain, e.g.
    sensor -> quarantine_batch -> summary -> page.
    """

    TRACKED = ("sensors.*", "quality.*", "notify.*", "comms.*")

    def __init__(self, bus: EventBus) -> None:
        self._chains: Dict[str, List[Dict]] = defaultdict(list)
        for pattern in self.TRACKED:
            bus.subscribe(pattern, self._on_event)

    def _on_event(self, event: Dict) -> None:
        root_id = event["payload"].get("root_id", event["event_id"])
        self._chains[root_id].append(event)

    def chain_for(self, root_id: str) -> List[Dict]:
        """Return the cascade chain for ``root_id`` in causal order.

        Events are stored in handler-invocation order (a tracker's own
        sensor subscription may fire after an agent's nested publishes),
        so the chain is sorted by event timestamp -- which always
        increases along the causal path -- before returning.
        """
        return sorted(self._chains.get(root_id, []),
                      key=lambda e: e["timestamp"])

    def roots(self) -> List[str]:
        """All root event ids seen so far."""
        return list(self._chains.keys())
