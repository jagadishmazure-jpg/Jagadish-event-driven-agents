# Topic catalog

Wildcard subscriptions are supported: `sensors.*` matches
`sensors.mixer-01`, `sensors.oven-02`, etc.

## `sensors.{equipment_id}`

Published by: sensor stream generators (`eventagents.sensors`).
Subscribed by: `QualityAgent`, `MaintenanceAgent`, `DigitalTwin`.

```json
{
  "equipment_id": "mixer-01",
  "sensor": "temperature",
  "value": 72.4,
  "unit": "C",
  "batch_id": "batch-mixer-01-006",
  "seq": 121
}
```

Sensors: `temperature` (C), `vibration` (mm/s), `pressure` (kPa).

## `quality.actions`

Published by: `QualityAgent`.
Subscribed by: `DigitalTwin` (quarantines, line stops).

```json
{
  "action": "quarantine_batch",
  "equipment_id": "mixer-01",
  "batch_id": "batch-mixer-01-006",
  "reason": "quality deviation: temperature reading 90.4C vs baseline 72.1C (z=11.8)"
}
```

Actions: `quarantine_batch` (single-sensor breach), `stop_line`
(multi-sensor breach — severe).

## `maintenance.work_orders`

Published by: `MaintenanceAgent`.
Subscribed by: `DigitalTwin`.

```json
{
  "equipment_id": "oven-02",
  "failure_mode": "bearing_overheat",
  "urgency": "high",
  "health_score": 42.0,
  "trigger": "temperature reading 91.2C vs baseline 72.0C (z=12.6)",
  "recommended_action": "Inspect bearings and cooling; schedule replacement",
  "status": "open"
}
```

Failure modes: `bearing_overheat` (temperature), `mechanical_loosening`
(vibration), `seal_leak` (pressure). One open order per
equipment + failure mode (de-duplicated).

## `orders.new`

Published by: order source (demo / ERP adapter in production).
Subscribed by: `ProductionAgent`.

```json
{
  "order_id": "ord-1042",
  "product": "oat-drink",
  "variant": "barista",
  "options": {"fill_volume_ml": 1000, "extra_qa": true},
  "firmware_channel": "stable",
  "quantity": 5000
}
```

## `production.recipes`

Published by: `ProductionAgent`.
Subscribed by: `DigitalTwin`.

```json
{
  "order_id": "ord-1042",
  "product": "oat-drink",
  "variant": "barista",
  "steps": ["stage_materials", "mix", "heat_treat", "fill", "seal", "label",
            "qa_sample", "extended_qa"],
  "parameters": {"mix_time_s": 420, "heat_temp_c": 165, "fill_volume_ml": 1000},
  "firmware": "fw-4.2.1",
  "quantity": 5000
}
```
