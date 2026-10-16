"""Production agent: order events -> dynamic manufacturing recipes.

Subscribes to ``orders.new``. Each order carries its variant requirements
(product, options, firmware channel, quantity); the agent generates a
tailored recipe published to ``production.recipes`` -- no fixed recipe per
line. Steps, parameters and firmware are derived from the order, so two
orders for the same product with different options get different recipes.
"""

from __future__ import annotations

from typing import Dict, List

from .bus import EventBus

BASE_STEPS = ["stage_materials", "mix", "heat_treat", "fill", "seal", "label", "qa_sample"]

FIRMWARE = {
    "stable": "fw-4.2.1",
    "beta": "fw-5.0.0-rc3",
    "lts": "fw-3.9.8",
}


class ProductionAgent:
    def __init__(self, bus: EventBus) -> None:
        self.bus = bus
        self.recipes: List[Dict] = []
        bus.subscribe("orders.new", self._on_order)

    def _on_order(self, event: Dict) -> None:
        order = event["payload"]
        recipe = self.build_recipe(order)
        self.recipes.append(recipe)
        self.bus.publish("production.recipes", recipe)

    def build_recipe(self, order: Dict) -> Dict:
        options = order.get("options", {})
        steps = list(BASE_STEPS)
        parameters: Dict[str, object] = {
            "mix_time_s": 300 + 60 * len(options),
            "heat_temp_c": options.get("heat_temp_c", 165),
            "fill_volume_ml": options.get("fill_volume_ml", 500),
        }
        if options.get("extra_qa"):
            steps.insert(steps.index("qa_sample") + 1, "extended_qa")
        if options.get("allergen_free"):
            steps.insert(0, "allergen_sanitize")
            parameters["sanitize_temp_c"] = 90
        if options.get("express"):
            parameters["mix_time_s"] = int(parameters["mix_time_s"] * 0.8)

        return {
            "order_id": order["order_id"],
            "product": order["product"],
            "variant": order.get("variant", "standard"),
            "steps": steps,
            "parameters": parameters,
            "firmware": FIRMWARE.get(order.get("firmware_channel", "stable"),
                                     FIRMWARE["stable"]),
            "quantity": order.get("quantity", 1),
        }
