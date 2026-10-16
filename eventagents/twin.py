"""Digital twin: event-sourced read model of the factory.

Subscribes to every topic and materializes current state: per-equipment
latest readings, open work orders, active quarantines, line status and
recipes issued. ``snapshot()`` returns the twin state -- the same physical
events, viewed as "what is true right now".
"""

from __future__ import annotations

from typing import Any, Dict

from .bus import EventBus


class DigitalTwin:
    def __init__(self, bus: EventBus) -> None:
        self.equipment: Dict[str, Dict[str, Any]] = {}
        self.open_work_orders: Dict[str, Dict] = {}
        self.quarantines: Dict[str, Dict] = {}
        self.line_status: Dict[str, str] = {}
        self.recipes: Dict[str, Dict] = {}
        bus.subscribe("sensors.*", self._on_sensor)
        bus.subscribe("maintenance.work_orders", self._on_work_order)
        bus.subscribe("quality.actions", self._on_quality_action)
        bus.subscribe("production.recipes", self._on_recipe)

    def _on_sensor(self, event: Dict) -> None:
        p = event["payload"]
        eq = self.equipment.setdefault(p["equipment_id"], {"readings": {}})
        eq["readings"][p["sensor"]] = {
            "value": p["value"], "unit": p["unit"], "seq": p["seq"],
        }
        eq["last_batch"] = p.get("batch_id")
        self.line_status.setdefault(p["equipment_id"], "running")

    def _on_work_order(self, event: Dict) -> None:
        p = event["payload"]
        key = f"{p['equipment_id']}:{p['failure_mode']}"
        if p.get("status") == "open":
            self.open_work_orders[key] = p
        else:
            self.open_work_orders.pop(key, None)

    def _on_quality_action(self, event: Dict) -> None:
        p = event["payload"]
        if p["action"] == "quarantine_batch":
            self.quarantines[p["batch_id"]] = p
        elif p["action"] == "stop_line":
            self.line_status[p["equipment_id"]] = "stopped"

    def _on_recipe(self, event: Dict) -> None:
        p = event["payload"]
        self.recipes[p["order_id"]] = p

    def snapshot(self) -> Dict[str, Any]:
        return {
            "equipment": self.equipment,
            "open_work_orders": self.open_work_orders,
            "quarantines": self.quarantines,
            "line_status": self.line_status,
            "recipes": self.recipes,
        }
