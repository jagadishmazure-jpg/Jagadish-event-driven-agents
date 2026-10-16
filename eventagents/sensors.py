"""Deterministic seeded sensor-event generators.

Each reading becomes an event on ``sensors.{equipment_id}`` with payload:
``equipment_id, sensor ("temperature"|"vibration"|"pressure"),
value, unit, batch_id, seq``.

Normal readings drift realistically around a per-equipment nominal value.
Anomaly injectors return modified copies (never mutate the input) so a test
can build a clean baseline stream and an attacked stream from the same seed.
"""

from __future__ import annotations

import random
from typing import Dict, List

NOMINALS = {
    "temperature": (72.0, 1.5, "C"),      # nominal, stddev, unit
    "vibration": (2.0, 0.3, "mm/s"),
    "pressure": (101.3, 1.0, "kPa"),
}

EQUIPMENT = ["mixer-01", "oven-02", "filler-03"]


def _reading(rng: random.Random, equipment_id: str, sensor: str, seq: int) -> Dict:
    nominal, std, unit = NOMINALS[sensor]
    # slow realistic drift plus noise
    drift = 0.15 * std * seq / 50.0
    value = rng.gauss(nominal + drift, std)
    return {
        "equipment_id": equipment_id,
        "sensor": sensor,
        "value": round(value, 2),
        "unit": unit,
        "batch_id": f"batch-{equipment_id}-{seq // 20:03d}",
        "seq": seq,
    }


def gen_sensor_stream(
    n: int = 200,
    seed: int = 42,
    equipment: List[str] | None = None,
) -> List[Dict]:
    """Generate ``n`` sensor readings per equipment, interleaved by seq."""
    rng = random.Random(seed)
    equipment = equipment or EQUIPMENT
    out: List[Dict] = []
    for seq in range(n):
        for eq in equipment:
            for sensor in ("temperature", "vibration", "pressure"):
                out.append(_reading(rng, eq, sensor, seq))
    return out


def inject_overheat(
    readings: List[Dict],
    equipment_id: str,
    start_seq: int = 120,
    delta: float = 18.0,
) -> List[Dict]:
    """Temperature runaway: readings climb ``delta`` degrees from ``start_seq``."""
    out = []
    for r in readings:
        r2 = dict(r)
        if r2["equipment_id"] == equipment_id and r2["sensor"] == "temperature" \
                and r2["seq"] >= start_seq:
            ramp = min(1.0, (r2["seq"] - start_seq) / 10.0 + 0.2)
            r2["value"] = round(r2["value"] + delta * ramp, 2)
            r2["injected"] = "overheat"
        out.append(r2)
    return out


def inject_vibration_spike(
    readings: List[Dict],
    equipment_id: str,
    start_seq: int = 120,
    factor: float = 6.0,
) -> List[Dict]:
    """Vibration spike: readings multiplied by ``factor`` from ``start_seq``."""
    out = []
    for r in readings:
        r2 = dict(r)
        if r2["equipment_id"] == equipment_id and r2["sensor"] == "vibration" \
                and r2["seq"] >= start_seq:
            r2["value"] = round(r2["value"] * factor, 2)
            r2["injected"] = "vibration_spike"
        out.append(r2)
    return out


def inject_pressure_drop(
    readings: List[Dict],
    equipment_id: str,
    start_seq: int = 120,
    delta: float = -25.0,
) -> List[Dict]:
    """Pressure drop: readings fall by ``delta`` kPa from ``start_seq``."""
    out = []
    for r in readings:
        r2 = dict(r)
        if r2["equipment_id"] == equipment_id and r2["sensor"] == "pressure" \
                and r2["seq"] >= start_seq:
            r2["value"] = round(r2["value"] + delta, 2)
            r2["injected"] = "pressure_drop"
        out.append(r2)
    return out
