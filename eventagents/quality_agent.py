"""Quality agent: subscribes to sensor events, acts on quality deviations.

Maintains a rolling per-(equipment, sensor) baseline using robust
statistics (median and MAD, so a sustained anomaly can't drag the
baseline toward itself until it dominates the window). A reading beyond
``z_warn`` robust sigmas raises a warning; beyond ``z_act`` triggers a
corrective action published to ``quality.actions``:

- single-sensor breach        -> ``quarantine_batch`` (hold the batch)
- 2+ sensors breaching at once -> ``stop_line`` (severe; halt the line)

"At once" means breached within the last ``co_breach_seqs`` readings:
severity is about concurrent deviation, not all-time history.

Pass an ``approval_gateway`` (see ``eventagents.hitl``) to route the
severe ``stop_line`` through human approval first; quarantines stay
automatic. Denial or timeout publishes ``quality.action_deferred``
instead of stopping the line.

Every action cites its evidence: reading vs baseline and z-score, plus
``trigger_event_id`` / ``root_id`` linking it back to the sensor event
that started the cascade.
"""

from __future__ import annotations

from collections import defaultdict, deque
from typing import Dict, List, Optional

from .bus import EventBus

# For normal data MAD * 1.4826 approximates the standard deviation.
_MAD_TO_SIGMA = 1.4826


def robust_stats(values: List[float], minimum: int = 30) -> Optional[tuple]:
    """Return (median, robust_sigma) or None if too few values.

    The minimum is deliberately higher than for mean/std: MAD estimates
    from a handful of samples are noisy and underestimate spread, which
    turns ordinary outliers into false alarms.
    """
    if len(values) < minimum:
        return None
    ordered = sorted(values)
    median = ordered[len(ordered) // 2]
    deviations = sorted(abs(v - median) for v in values)
    mad = deviations[len(deviations) // 2]
    sigma = mad * _MAD_TO_SIGMA
    return median, sigma if sigma > 1e-9 else 1e-9


class QualityAgent:
    def __init__(
        self,
        bus: EventBus,
        window: int = 60,
        z_warn: float = 3.0,
        z_act: float = 5.0,
        co_breach_seqs: int = 5,
        approval_gateway=None,
        approval_timeout: float = 30.0,
    ) -> None:
        self.bus = bus
        self.window = window
        self.z_warn = z_warn
        self.z_act = z_act
        self.co_breach_seqs = co_breach_seqs
        self.approval_gateway = approval_gateway
        self.approval_timeout = approval_timeout
        self._hist: Dict[tuple, deque] = defaultdict(lambda: deque(maxlen=window))
        self._last_breach: Dict[tuple, int] = {}
        self.warnings: List[Dict] = []
        self.actions: List[Dict] = []
        self.deferred: List[Dict] = []
        bus.subscribe("sensors.*", self._on_sensor)

    # -- event handling -------------------------------------------------
    def _on_sensor(self, event: Dict) -> None:
        p = event["payload"]
        key = (p["equipment_id"], p["sensor"])
        stats = robust_stats(list(self._hist[key]))
        self._hist[key].append(p["value"])
        if stats is None:
            return  # still warming up the baseline
        median, sigma = stats
        z = abs(p["value"] - median) / sigma

        if z >= self.z_warn:
            self._last_breach[key] = p["seq"]
            self.warnings.append({
                "equipment_id": p["equipment_id"],
                "sensor": p["sensor"],
                "value": p["value"],
                "baseline_median": round(median, 2),
                "z": round(z, 2),
                "batch_id": p.get("batch_id"),
            })

        if z >= self.z_act:
            self._act(event, median, sigma, z)

    def _act(self, event: Dict, median: float, sigma: float, z: float) -> None:
        p = event["payload"]
        # severe only if another sensor on the same equipment breached
        # within the recent window -- concurrent deviation, not history
        others = {
            sensor for (eq, sensor), seq in self._last_breach.items()
            if eq == p["equipment_id"] and sensor != p["sensor"]
            and 0 <= p["seq"] - seq <= self.co_breach_seqs
        }
        evidence = (
            f"{p['sensor']} reading {p['value']}{p['unit']} vs baseline "
            f"{median:.2f}{p['unit']} (z={z:.1f})"
        )
        action: Dict = {
            "equipment_id": p["equipment_id"],
            "batch_id": p.get("batch_id"),
            "trigger_event_id": event["event_id"],
            "root_id": event["event_id"],
        }
        if others:
            action["action"] = "stop_line"
            action["reason"] = (
                f"multi-sensor deviation: {evidence}; "
                f"also breaching: {sorted(others)}"
            )
            self._act_stop_line(action)
        else:
            action["action"] = "quarantine_batch"
            action["reason"] = f"quality deviation: {evidence}"
            self.actions.append(action)
            self.bus.publish("quality.actions", action)

    def _act_stop_line(self, action: Dict) -> None:
        """Severe action: goes through the approval gateway when wired.

        Quarantines stay automatic; only the line stop needs a human.
        Denial or timeout defers the action (``quality.action_deferred``)
        instead of stopping the line.
        """
        if self.approval_gateway is None:
            self.actions.append(action)
            self.bus.publish("quality.actions", action)
            return
        decision = self.approval_gateway.request_approval(
            action="stop_line",
            context={
                "equipment_id": action["equipment_id"],
                "batch_id": action["batch_id"],
                "reason": action["reason"],
                "severity": "critical",
            },
            timeout_s=self.approval_timeout,
        )
        if decision["decision"] == "granted":
            action["approved_by"] = decision.get("decided_by")
            self.actions.append(action)
            self.bus.publish("quality.actions", action)
        else:
            deferred = {
                "action": "stop_line",
                "equipment_id": action["equipment_id"],
                "batch_id": action["batch_id"],
                "reason": action["reason"],
                "deferral": decision["decision"],  # denied | timeout
                "decided_by": decision.get("decided_by"),
                "defer_reason": decision.get("reason"),
                "trigger_event_id": action["trigger_event_id"],
                "root_id": action["root_id"],
            }
            self.deferred.append(deferred)
            self.bus.publish("quality.action_deferred", deferred)
