# Event-driven agents for smart manufacturing

Every agent in a typical portfolio answers requests: ask a question, get an
answer. The physical world doesn't work that way. A temperature spike
mid-batch doesn't file a request — it happens, and something has to react
*now*. Polling a database every minute to notice is wasteful; reacting to
the event the moment it fires is the architecture.

This project is a **simulated event mesh** — an in-process pub/sub event bus
with autonomous agents subscribed to event streams:

- **Quality agent** — watches sensor events, detects deviations from rolling
  baselines, and publishes corrective actions (quarantine the batch, stop
  the line). Every action cites its evidence: reading vs baseline, z-score.
- **Maintenance agent** — tracks equipment health from the same stream,
  predicts failure modes, and raises de-duplicated work orders.
- **Production agent** — turns order events into dynamic manufacturing
  recipes: each order variant gets a tailored recipe, not a fixed one.
- **Digital twin** — an event-sourced read model: the same events,
  materialized as "what is true right now."
- **Event ledger** — every published event hash-chained for a tamper-evident
  audit trail.

## v2: choreography, HITL, fallback

The v1 agents react to *sensor* events. v2 adds agents reacting to
*each other* — no central orchestrator:

- **Choreography cascades** — `SummarizerAgent` turns quality actions into
  `notify.summaries` (critical for line stops, warning otherwise);
  `CommsAgent` routes critical summaries to `comms.urgent` (paged) and the
  rest to `comms.digest`. `CascadeTracker` records the causal chain per
  root sensor event, so any sensor → action → summary → page cascade is
  traceable end to end via `chain_for(root_id)`.
- **Human-in-the-loop as an event participant** — `ApprovalGateway`
  publishes `approval.requested` and blocks (with timeout) for a matching
  `approval.granted` / `approval.denied`. The quality agent's severe
  `stop_line` goes through approval when a gateway is wired; quarantines
  stay automatic. Denial or timeout publishes `quality.action_deferred`
  instead of stopping the line. `SimulatedHuman` stands in for a real
  approver in tests (approve-all, deny-all, quarantine-only, or
  severity-threshold policies).
- **Fallback agents** — `with_fallback` wraps a primary handler: on
  exception the event still lands in the dead-letter queue and a fallback
  handler publishes a degraded-but-safe action (defer + manual-inspection
  work order). `FallbackMonitor.report()` shows primary vs fallback
  counts per agent.

Vendor-neutral: the pattern, not the product. The bus documents its
production path (Kafka / Azure Event Hubs); no streaming vendor is required
to run this.

## The food-manufacturing story

A temperature sensor on `mixer-01` starts climbing mid-batch. The quality
agent's rolling baseline says 72.0°C ± 1.5; the reading hits 90°C (z > 10).
Before the batch ships, a `quarantine_batch` event fires on
`quality.actions` with the batch id and the cited numbers — and if a second
sensor breaches at the same time, the line stops. No polling, no human in
the loop, no defective product leaving the plant.

## Architecture

```
  sensors ──publish──▶  ┌───────────┐  subscribe ┌──────────────────┐
  (temp/vib/press)      │ EventBus  │◀───────────│ QualityAgent     │──▶ quality.actions ──▶ SummarizerAgent ──▶ notify.summaries ──▶ CommsAgent ──▶ comms.urgent / digest
  orders.new ───────────│ (topics,  │◀───────────│ MaintenanceAgent │──▶ maintenance.work_orders
                        │ filters,  │◀───────────│ ProductionAgent  │──▶ production.recipes
                        │ DLQ,      │◀───────────│ DigitalTwin      │    (read model)
                        │ replay)   │◀───────────│ CascadeTracker   │    (causal chains)
                        └─────┬─────┘            └──────────────────┘
                              │ record
                              ▼
                       ┌──────────────┐         approval.requested ⇄ human
                       │ EventLedger  │◀── stop_line ──▶ ApprovalGateway ──▶ granted ──▶ stop_line
                       └──────────────┘  hash-chained                     denied/timeout ──▶ quality.action_deferred
                                         audit trail                     (fallback agents wrap primaries:
                                                                          exception ──▶ DLQ + degraded safe action)
```

## Quickstart

```python
from eventagents import (
    EventBus, EventLedger, QualityAgent, MaintenanceAgent,
    ProductionAgent, DigitalTwin, gen_sensor_stream, inject_overheat,
)

bus = EventBus()
ledger = EventLedger()
quality = QualityAgent(bus)
maintenance = MaintenanceAgent(bus)
production = ProductionAgent(bus)
twin = DigitalTwin(bus)

# every published event also lands in the tamper-evident ledger
orig_publish = bus.publish
def publish(topic, payload):
    event = orig_publish(topic, payload)
    ledger.record(event)
    return event
bus.publish = publish

# clean baseline, then an overheat mid-run
stream = gen_sensor_stream(n=200, seed=7)
attacked = inject_overheat(stream, "mixer-01", start_seq=120)
for reading in attacked:
    bus.publish(f"sensors.{reading['equipment_id']}", reading)

print(next(a for a in quality.actions if a["action"] == "quarantine_batch"))
# {'action': 'quarantine_batch', 'equipment_id': 'mixer-01',
#  'batch_id': 'batch-mixer-01-006',
#  'reason': 'quality deviation: temperature reading 82.06C vs baseline 72.36C (z=6.0)',
#  'trigger_event_id': '…', 'root_id': '…'}  # cascade linkage
print(twin.snapshot()["quarantines"].keys())
print("ledger ok:", ledger.verify_chain())
```

## Topic catalog

See [docs/topics.md](docs/topics.md) for the full topic list, event schemas,
publishers and subscribers.

## Knowledge base

Distilled study notes from the source material behind this repo's design,
plus the architecture diagrams redrawn as Mermaid:
[docs/knowledge/](docs/knowledge/).

## Production path

- Replace `EventBus` with a durable broker (Kafka, Azure Event Hubs, or a
  managed event mesh) for persistence, cross-process delivery and replay.
  The subscriber/handler contract is unchanged.
- Exactly-once semantics: make handlers idempotent (the maintenance agent's
  work-order de-dup is the in-repo example) and let the broker own offsets.
- Schema registry for event payloads; the topic catalog in `docs/topics.md`
  is the starting point.

## Tests

```bash
pytest -q
```

51 tests, deterministic, no network or API keys. This project is
independent and unaffiliated; all data is synthetic.
