"""Human-in-the-loop as an event participant.

Approvals are events like anything else: the ApprovalGateway publishes
``approval.requested`` and blocks (with a timeout) until a matching
``approval.granted`` or ``approval.denied`` arrives. A human is just another
subscriber/publisher on the bus -- in tests, SimulatedHuman stands in.

Production path: replace SimulatedHuman with a real human reached through
a UI, chat, or paging integration that publishes the decision event back
onto the bus. The gateway contract (request event in, decision event out)
stays the same.

Wire-up: QualityAgent takes an optional ``approval_gateway``; when wired,
its severe ``stop_line`` action goes through approval first (quarantines
stay automatic). Denial or timeout publishes ``quality.action_deferred``
with the reason instead of stopping the line.
"""

from __future__ import annotations

import time
import uuid
from typing import Callable, Dict, List, Tuple

from .bus import EventBus

Decision = Tuple[str, str, str]  # (granted|denied, decided_by, reason)


class ApprovalGateway:
    """Request human approval over events; block until decided or timed out."""

    def __init__(self, bus: EventBus) -> None:
        self.bus = bus
        self._decisions: Dict[str, Dict] = {}
        bus.subscribe("approval.granted", self._on_decision)
        bus.subscribe("approval.denied", self._on_decision)

    def _on_decision(self, event: Dict) -> None:
        p = event["payload"]
        self._decisions[p["approval_id"]] = {
            "decision": "granted" if event["topic"] == "approval.granted"
                        else "denied",
            "approval_id": p["approval_id"],
            "decided_by": p.get("decided_by"),
            "reason": p.get("reason"),
        }

    def request_approval(
        self,
        action: str,
        context: Dict,
        timeout_s: float = 30.0,
    ) -> Dict:
        """Publish ``approval.requested``; return the decision.

        Returns ``{"decision": "granted"|"denied", ...}`` when a human
        answers, or ``{"decision": "timeout", ...}`` (and publishes
        ``approval.timed_out``) when nobody answers in time.
        """
        approval_id = str(uuid.uuid4())
        self.bus.publish("approval.requested", {
            "approval_id": approval_id,
            "action": action,
            "context": dict(context),
            "requested_at": time.time(),
        })
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if approval_id in self._decisions:
                return self._decisions.pop(approval_id)
            time.sleep(0.005)
        self.bus.publish("approval.timed_out", {
            "approval_id": approval_id,
            "action": action,
            "waited_s": timeout_s,
        })
        return {
            "decision": "timeout",
            "approval_id": approval_id,
            "decided_by": None,
            "reason": f"no decision within {timeout_s}s",
        }


def _resolve_policy(policy) -> Callable[[Dict], Decision]:
    if callable(policy):
        return policy
    if policy == "approve_all":
        return lambda p: ("granted", "simulated-human", "auto-approved")
    if policy == "deny_all":
        return lambda p: ("denied", "simulated-human", "auto-denied")
    if policy == "approve_quarantine_only":
        def pol(p: Dict) -> Decision:
            if p["action"] == "quarantine_batch":
                return ("granted", "simulated-human",
                        "quarantine is low-risk; auto-approved")
            return ("denied", "simulated-human",
                    "line stop needs a real human")
        return pol
    raise ValueError(f"unknown approval policy: {policy!r}")


def approve_below_severity(max_severity: str = "warning") -> Callable[[Dict], Decision]:
    """Policy factory: auto-approve at or below a severity threshold.

    Severity order: warning < critical. Useful when low-severity actions
    can flow but critical ones must wait for a person.
    """
    order = {"warning": 0, "critical": 1}
    limit = order[max_severity]

    def pol(payload: Dict) -> Decision:
        sev = payload.get("context", {}).get("severity", "warning")
        if order.get(sev, 0) <= limit:
            return ("granted", "simulated-human",
                    f"severity {sev} within auto-approve threshold")
        return ("denied", "simulated-human",
                f"severity {sev} exceeds auto-approve threshold")
    return pol


class SimulatedHuman:
    """Test stand-in for a human approver.

    Subscribes to ``approval.requested`` and immediately publishes the
    matching ``approval.granted`` / ``approval.denied`` per its policy.
    """

    def __init__(self, bus: EventBus, policy="approve_all") -> None:
        self.bus = bus
        self._policy = _resolve_policy(policy)
        self.decisions: List[Dict] = []
        bus.subscribe("approval.requested", self._on_request)

    def _on_request(self, event: Dict) -> None:
        p = event["payload"]
        decision, decided_by, reason = self._policy(p)
        record = {
            "approval_id": p["approval_id"],
            "action": p["action"],
            "decision": decision,
            "decided_by": decided_by,
            "reason": reason,
        }
        self.decisions.append(record)
        self.bus.publish(f"approval.{decision}", {
            "approval_id": p["approval_id"],
            "decided_by": decided_by,
            "reason": reason,
            "decided_at": time.time(),
        })
