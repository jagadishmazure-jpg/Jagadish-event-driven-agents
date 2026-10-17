"""Tests for eventagents. All deterministic; no network, no API keys."""


from eventagents import (
    DigitalTwin,
    EventBus,
    EventLedger,
    MaintenanceAgent,
    ProductionAgent,
    QualityAgent,
    gen_sensor_stream,
    inject_overheat,
    inject_pressure_drop,
    inject_vibration_spike,
)


def make_reading(equipment="mixer-01", sensor="temperature", value=72.0,
                 unit="C", seq=0, batch="batch-mixer-01-000"):
    return {"equipment_id": equipment, "sensor": sensor, "value": value,
            "unit": unit, "seq": seq, "batch_id": batch}


def publish_stream(bus, readings):
    for r in readings:
        bus.publish(f"sensors.{r['equipment_id']}", r)


# ---------------------------------------------------------------- bus
def test_publish_subscribe_receives_event():
    bus = EventBus()
    got = []
    bus.subscribe("alerts", got.append)
    event = bus.publish("alerts", {"msg": "hi"})
    assert len(got) == 1
    assert got[0]["payload"] == {"msg": "hi"}
    assert got[0]["event_id"] == event["event_id"]
    assert got[0]["topic"] == "alerts"
    assert "timestamp" in got[0]


def test_wildcard_subscription():
    bus = EventBus()
    got = []
    bus.subscribe("sensors.*", got.append)
    bus.publish("sensors.mixer-01", {"v": 1})
    bus.publish("sensors.oven-02", {"v": 2})
    bus.publish("other.topic", {"v": 3})
    assert len(got) == 2


def test_filter_predicate():
    bus = EventBus()
    got = []
    bus.subscribe("sensors.*", got.append,
                  filter=lambda e: e["payload"].get("sensor") == "temperature")
    bus.publish("sensors.mixer-01", {"sensor": "temperature"})
    bus.publish("sensors.mixer-01", {"sensor": "vibration"})
    assert len(got) == 1


def test_unsubscribe_stops_delivery():
    bus = EventBus()
    got = []
    sub_id = bus.subscribe("t", got.append)
    assert bus.unsubscribe(sub_id) is True
    bus.publish("t", {"v": 1})
    assert got == []
    assert bus.unsubscribe("nope") is False


def test_handler_exception_goes_to_dead_letter_and_bus_continues():
    bus = EventBus()
    good = []

    def bad_handler(event):
        raise RuntimeError("boom")

    bus.subscribe("t", bad_handler)
    bus.subscribe("t", good.append)
    bus.publish("t", {"v": 1})
    bus.publish("t", {"v": 2})
    assert len(good) == 2  # bus kept running
    assert len(bus.dead_letters) == 2
    dl = bus.dead_letters[0]
    assert "boom" in dl.error
    assert dl.event["payload"] == {"v": 1}


def test_broken_filter_isolated_to_dead_letter():
    bus = EventBus()
    got = []
    bus.subscribe("t", got.append, filter=lambda e: 1 / 0)
    bus.publish("t", {"v": 1})
    assert got == []
    assert len(bus.dead_letters) == 1
    assert "filter error" in bus.dead_letters[0].error


def test_replay_returns_history_including_wildcard():
    bus = EventBus()
    bus.publish("sensors.a", {"v": 1})
    bus.publish("sensors.b", {"v": 2})
    bus.publish("other", {"v": 3})
    assert len(bus.replay("sensors.a")) == 1
    assert len(bus.replay("sensors.*")) == 2
    assert bus.replay("missing") == []


# ------------------------------------------------------------- sensors
def test_seeded_determinism():
    a = gen_sensor_stream(n=20, seed=7)
    b = gen_sensor_stream(n=20, seed=7)
    assert a == b
    c = gen_sensor_stream(n=20, seed=8)
    assert a != c


def test_inject_overheat_out_of_baseline():
    clean = gen_sensor_stream(n=60, seed=3)
    attacked = inject_overheat(clean, "mixer-01", start_seq=30, delta=18.0)
    # input not mutated
    assert all("injected" not in r for r in clean)
    temps = [r["value"] for r in attacked
             if r["equipment_id"] == "mixer-01"
             and r["sensor"] == "temperature" and r["seq"] >= 40]
    assert min(temps) > 80.0  # nominal ~72


def test_inject_vibration_spike():
    clean = gen_sensor_stream(n=60, seed=3)
    attacked = inject_vibration_spike(clean, "oven-02", start_seq=30, factor=6.0)
    vibes = [r["value"] for r in attacked
             if r["equipment_id"] == "oven-02"
             and r["sensor"] == "vibration" and r["seq"] >= 30]
    assert min(vibes) > 6.0  # nominal ~2.0


def test_inject_pressure_drop():
    clean = gen_sensor_stream(n=60, seed=3)
    attacked = inject_pressure_drop(clean, "filler-03", start_seq=30, delta=-25.0)
    press = [r["value"] for r in attacked
             if r["equipment_id"] == "filler-03"
             and r["sensor"] == "pressure" and r["seq"] >= 30]
    assert max(press) < 90.0  # nominal ~101.3


# -------------------------------------------------------- quality agent
def test_clean_stream_no_actions():
    bus = EventBus()
    qa = QualityAgent(bus)
    publish_stream(bus, gen_sensor_stream(n=150, seed=11))
    assert qa.actions == []


def test_baseline_warmup_no_false_positives():
    bus = EventBus()
    qa = QualityAgent(bus)
    publish_stream(bus, gen_sensor_stream(n=5, seed=11))
    assert qa.actions == [] and qa.warnings == []


def test_overheat_triggers_quarantine_with_cited_numbers():
    bus = EventBus()
    qa = QualityAgent(bus)
    actions_seen = []
    bus.subscribe("quality.actions", actions_seen.append)
    stream = gen_sensor_stream(n=200, seed=7)
    attacked = inject_overheat(stream, "mixer-01", start_seq=120, delta=18.0)
    publish_stream(bus, attacked)
    quarantines = [a for a in qa.actions if a["action"] == "quarantine_batch"]
    assert quarantines, "expected a quarantine_batch action"
    q = quarantines[0]
    assert q["equipment_id"] == "mixer-01"
    assert q["batch_id"].startswith("batch-mixer-01-")
    # evidence cited: reading, baseline, z-score
    assert "90" in q["reason"] or "8" in q["reason"]
    assert "baseline" in q["reason"] and "z=" in q["reason"]
    assert len(actions_seen) == len(qa.actions)


def test_multisensor_deviation_triggers_stop_line():
    bus = EventBus()
    qa = QualityAgent(bus)
    stream = gen_sensor_stream(n=200, seed=7)
    attacked = inject_overheat(stream, "mixer-01", start_seq=120, delta=18.0)
    attacked = inject_vibration_spike(attacked, "mixer-01", start_seq=120,
                                      factor=6.0)
    publish_stream(bus, attacked)
    stops = [a for a in qa.actions if a["action"] == "stop_line"]
    assert stops, "expected a stop_line action on multi-sensor breach"
    assert "multi-sensor" in stops[0]["reason"]


def test_warnings_recorded_before_actions():
    bus = EventBus()
    qa = QualityAgent(bus)
    stream = gen_sensor_stream(n=200, seed=7)
    attacked = inject_overheat(stream, "mixer-01", start_seq=120, delta=18.0)
    publish_stream(bus, attacked)
    assert len(qa.warnings) >= len(qa.actions) > 0


# ---------------------------------------------------- maintenance agent
def test_degraded_health_raises_work_order():
    bus = EventBus()
    ma = MaintenanceAgent(bus)
    orders_seen = []
    bus.subscribe("maintenance.work_orders", orders_seen.append)
    stream = gen_sensor_stream(n=200, seed=7)
    attacked = inject_overheat(stream, "oven-02", start_seq=120, delta=18.0)
    publish_stream(bus, attacked)
    assert len(ma.work_orders) == 1
    wo = ma.work_orders[0]
    assert wo["equipment_id"] == "oven-02"
    assert wo["failure_mode"] == "bearing_overheat"
    assert wo["urgency"] == "high"
    assert wo["status"] == "open"
    assert "baseline" in wo["trigger"] and "z=" in wo["trigger"]
    assert wo["recommended_action"]
    assert len(orders_seen) == 1


def test_dedup_no_duplicate_work_orders():
    bus = EventBus()
    ma = MaintenanceAgent(bus)
    stream = gen_sensor_stream(n=400, seed=7)
    attacked = inject_overheat(stream, "oven-02", start_seq=120, delta=18.0)
    publish_stream(bus, attacked)
    assert len(ma.work_orders) == 1  # still one despite continued anomalies


def test_recovery_closes_order():
    bus = EventBus()
    ma = MaintenanceAgent(bus, recover=2.0)
    stream = gen_sensor_stream(n=200, seed=7)
    attacked = inject_overheat(stream, "oven-02", start_seq=100, delta=18.0)
    # only the bad window, then clean readings to recover
    publish_stream(bus, [r for r in attacked if r["seq"] < 140])
    assert len(ma.work_orders) == 1
    publish_stream(bus, gen_sensor_stream(n=120, seed=99,
                                          equipment=["oven-02"]))
    assert len(ma.open_orders) == 0
    assert len(ma.closed_orders) == 1


def test_clean_stream_no_work_orders():
    bus = EventBus()
    ma = MaintenanceAgent(bus)
    publish_stream(bus, gen_sensor_stream(n=150, seed=11))
    assert ma.work_orders == []
    assert all(h == 100.0 for h in ma.health.values())


# ----------------------------------------------------- production agent
def test_order_variant_gets_tailored_recipe():
    bus = EventBus()
    pa = ProductionAgent(bus)
    recipes_seen = []
    bus.subscribe("production.recipes", recipes_seen.append)
    bus.publish("orders.new", {
        "order_id": "ord-1", "product": "oat-drink", "variant": "barista",
        "options": {"fill_volume_ml": 1000, "extra_qa": True,
                    "allergen_free": True},
        "firmware_channel": "stable", "quantity": 5000,
    })
    assert len(pa.recipes) == 1
    r = pa.recipes[0]
    assert r["order_id"] == "ord-1"
    assert "allergen_sanitize" in r["steps"]  # tailored to allergen_free
    assert "extended_qa" in r["steps"]        # tailored to extra_qa
    assert r["parameters"]["fill_volume_ml"] == 1000
    assert r["firmware"] == "fw-4.2.1"
    assert len(recipes_seen) == 1


def test_different_variants_get_different_recipes():
    bus = EventBus()
    pa = ProductionAgent(bus)
    bus.publish("orders.new", {"order_id": "a", "product": "oat-drink",
                               "options": {}})
    bus.publish("orders.new", {"order_id": "b", "product": "oat-drink",
                               "firmware_channel": "beta",
                               "options": {"express": True}})
    ra = pa.recipes[0]
    rb = pa.recipes[1]
    assert ra["steps"] != rb["steps"] or \
        ra["parameters"] != rb["parameters"] or \
        ra["firmware"] != rb["firmware"]
    assert rb["firmware"] == "fw-5.0.0-rc3"


# ---------------------------------------------------------------- twin
def test_snapshot_reflects_sensors_and_quarantine():
    bus = EventBus()
    qa = QualityAgent(bus)
    twin = DigitalTwin(bus)
    stream = gen_sensor_stream(n=200, seed=7)
    attacked = inject_overheat(stream, "mixer-01", start_seq=120, delta=18.0)
    publish_stream(bus, attacked)
    snap = twin.snapshot()
    assert "mixer-01" in snap["equipment"]
    assert snap["equipment"]["mixer-01"]["readings"]["temperature"]["value"] > 80
    assert len(snap["quarantines"]) >= 1
    assert qa.actions, "expected quality actions"
    assert qa.actions[0]["batch_id"] in snap["quarantines"]


def test_line_stop_reflected_in_twin():
    bus = EventBus()
    QualityAgent(bus)
    twin = DigitalTwin(bus)
    stream = gen_sensor_stream(n=200, seed=7)
    attacked = inject_overheat(stream, "mixer-01", start_seq=120, delta=18.0)
    attacked = inject_vibration_spike(attacked, "mixer-01", start_seq=120,
                                      factor=6.0)
    publish_stream(bus, attacked)
    assert twin.snapshot()["line_status"]["mixer-01"] == "stopped"


# --------------------------------------------------------------- ledger
def test_chain_verifies():
    ledger = EventLedger()
    bus = EventBus()
    for i in range(5):
        ledger.record(bus.publish("t", {"i": i}))
    assert ledger.verify_chain() is True
    assert len(ledger.entries) == 5


def test_tamper_detected():
    ledger = EventLedger()
    bus = EventBus()
    for i in range(3):
        ledger.record(bus.publish("t", {"i": i}))
    ledger.entries[1]["body"] = ledger.entries[1]["body"].replace('"t"', '"evil"')
    assert ledger.verify_chain() is False


def test_export_jsonl_roundtrip():
    import json
    ledger = EventLedger()
    bus = EventBus()
    ledger.record(bus.publish("t", {"i": 1}))
    lines = ledger.export_jsonl().strip().split("\n")
    assert len(lines) == 1
    entry = json.loads(lines[0])
    assert json.loads(entry["body"])["payload"]["i"] == 1


# ------------------------------------------------------------ end-to-end
def test_full_factory_overheat_scenario():
    bus = EventBus()
    ledger = EventLedger()
    orig_publish = bus.publish

    def publish(topic, payload):
        event = orig_publish(topic, payload)
        ledger.record(event)
        return event
    bus.publish = publish

    QualityAgent(bus)  # subscribes on construction
    ma = MaintenanceAgent(bus)
    pa = ProductionAgent(bus)
    twin = DigitalTwin(bus)

    bus.publish("orders.new", {"order_id": "ord-9", "product": "oat-drink",
                               "options": {"fill_volume_ml": 750}})
    stream = gen_sensor_stream(n=200, seed=7)
    attacked = inject_overheat(stream, "mixer-01", start_seq=120, delta=18.0)
    publish_stream(bus, attacked)

    quality_events = bus.history("quality.actions")
    assert any(e["payload"]["action"] == "quarantine_batch"
               for e in quality_events)
    assert len(twin.snapshot()["quarantines"]) >= 1
    assert len(ma.work_orders) >= 1
    assert len(pa.recipes) == 1
    assert ledger.verify_chain() is True
    assert len(bus.dead_letters) == 0


def test_agents_coexist_on_same_bus_without_interference():
    bus = EventBus()
    QualityAgent(bus)
    MaintenanceAgent(bus)
    ProductionAgent(bus)
    DigitalTwin(bus)
    publish_stream(bus, gen_sensor_stream(n=150, seed=11))
    bus.publish("orders.new", {"order_id": "x", "product": "p", "options": {}})
    assert len(bus.dead_letters) == 0
