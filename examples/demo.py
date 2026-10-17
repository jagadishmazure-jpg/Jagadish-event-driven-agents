"""End-to-end run: overheat on mixer-01, human approval, a failing agent, an order."""
from collections import Counter

from eventagents import (
    ApprovalGateway,
    CascadeTracker,
    CommsAgent,
    DigitalTwin,
    EventBus,
    EventLedger,
    MaintenanceAgent,
    ProductionAgent,
    QualityAgent,
    SimulatedHuman,
    SummarizerAgent,
    gen_sensor_stream,
    inject_overheat,
    inject_vibration_spike,
)


def wire(policy):
    bus = EventBus()
    ledger = EventLedger()
    raw_publish = bus.publish

    def publish(topic, payload):
        event = raw_publish(topic, payload)
        ledger.record(event)
        return event

    bus.publish = publish
    SimulatedHuman(bus, policy=policy)
    agents = dict(
        quality=QualityAgent(bus, approval_gateway=ApprovalGateway(bus), approval_timeout=1.0),
        maintenance=MaintenanceAgent(bus),
        production=ProductionAgent(bus),
        twin=DigitalTwin(bus),
        tracker=CascadeTracker(bus),
    )
    SummarizerAgent(bus)
    CommsAgent(bus)
    return bus, ledger, agents


stream = gen_sensor_stream(n=200, seed=7)
stream = inject_overheat(stream, "mixer-01", start_seq=120)
stream = inject_vibration_spike(stream, "mixer-01", start_seq=120)

for policy in ("approve_quarantine_only", "approve_all"):
    bus, ledger, a = wire(policy)
    for r in stream:
        bus.publish(f"sensors.{r['equipment_id']}", r)
    q = a["quality"]
    print(f"--- approver policy: {policy}")
    print(f"sensor events: {len(stream)}")
    print("quality actions:", dict(Counter(x["action"] for x in q.actions)))
    print("deferred (approval denied):", len(q.deferred))
    first = next(x for x in q.actions if x["action"] == "quarantine_batch")
    print("first quarantine:", first["batch_id"], "|", first["reason"])
    print("work orders:", len(a["maintenance"].work_orders))
    print("pages (comms.urgent):", len(bus.history("comms.urgent")),
          "| digest:", len(bus.history("comms.digest")))
    chain = a["tracker"].chain_for(first["root_id"])
    print("cascade from the triggering reading:", " -> ".join(e["topic"] for e in chain))
    print("ledger events:", len(ledger.export_jsonl().splitlines()), "| chain verifies:", ledger.verify_chain())

bus, _, a = wire("approve_all")
bus.publish("orders.new", {"order_id": "ORD-1", "product": "sauce-base", "variant": "allergen-free",
                           "options": {"allergen_free": True, "extra_qa": True}})
recipe = a["production"].recipes[0]
print("--- order ORD-1 recipe steps:", recipe["steps"])
