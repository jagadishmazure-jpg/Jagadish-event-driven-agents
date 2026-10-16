"""Maintenance agent: subscribes to sensor events, predicts equipment failure.

Each equipment gets a health score (0-100, starts at 100). Readings that
deviate beyond ``z_bad`` robust sigmas from the rolling median/MAD baseline
decay health; clean readings recover it slowly. When health drops below
``threshold`` a work order is published to ``maintenance.work_orders`` with
the predicted failure mode, urgency and recommended action.

De-duplication: at most one open work order per (equipment, failure mode).
When health recovers above ``threshold + hysteresis``, the order closes and
a new one may be raised later.
"""

from __future__ import annotations

from collections import defaultdict, deque
from typing import Dict, List, Optional

from .bus import EventBus
from .quality_agent import robust_stats

FAILURE_MODES = {
    "temperature": ("bearing_overheat", "high",
                    "Inspect bearings and cooling; schedule replacement"),
    "vibration": ("mechanical_loosening", "high",
                  "Check mounts, belts and alignment; torque fasteners"),
    "pressure": ("seal_leak", "medium",
                "Inspect seals and fittings; pressure-test the line"),
}


class MaintenanceAgent:
    def __init__(
        self,
        bus: EventBus,
        window: int = 60,
        z_bad: float = 4.0,
        threshold: float = 60.0,
        decay: float = 6.0,
        recover: float = 0.4,
    ) -> None:
        self.bus = bus
        self.window = window
        self.z_bad = z_bad
        self.threshold = threshold
        self.decay = decay
        self.recover = recover
        self._hist: Dict[tuple, deque] = defaultdict(lambda: deque(maxlen=window))
        self.health: Dict[str, float] = defaultdict(lambda: 100.0)
        self.open_orders: Dict[tuple, Dict] = {}   # (equipment, mode) -> order
        self.work_orders: List[Dict] = []
        self.closed_orders: List[Dict] = []
        bus.subscribe("sensors.*", self._on_sensor)

    def _on_sensor(self, event: Dict) -> None:
        p = event["payload"]
        eq, sensor = p["equipment_id"], p["sensor"]
        key = (eq, sensor)
        stats = robust_stats(list(self._hist[key]))
        self._hist[key].append(p["value"])
        if stats is None:
            return
        median, sigma = stats
        z = abs(p["value"] - median) / sigma

        if z >= self.z_bad:
            self.health[eq] = max(0.0, self.health[eq] - self.decay)
            self._maybe_raise(eq, sensor, p, median, z)
        else:
            self.health[eq] = min(100.0, self.health[eq] + self.recover)
            self._maybe_close(eq)

    def _maybe_raise(self, eq: str, sensor: str, p: Dict,
                     median: float, z: float) -> None:
        mode, urgency, recommendation = FAILURE_MODES[sensor]
        key = (eq, mode)
        if key in self.open_orders:
            return  # de-dup: one open order per equipment+failure mode
        if self.health[eq] < self.threshold:
            order = {
                "equipment_id": eq,
                "failure_mode": mode,
                "urgency": urgency,
                "health_score": round(self.health[eq], 1),
                "trigger": (f"{sensor} reading {p['value']}{p['unit']} vs "
                            f"baseline {median:.2f}{p['unit']} (z={z:.1f})"),
                "recommended_action": recommendation,
                "status": "open",
            }
            self.open_orders[key] = order
            self.work_orders.append(order)
            self.bus.publish("maintenance.work_orders", order)

    def _maybe_close(self, eq: str) -> None:
        if self.health[eq] >= self.threshold + 15:
            for key in [k for k in self.open_orders if k[0] == eq]:
                order = self.open_orders.pop(key)
                order["status"] = "closed"
                self.closed_orders.append(order)
