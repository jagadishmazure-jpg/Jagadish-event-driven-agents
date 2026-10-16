"""Tests for v2: choreography cascades, HITL approvals, fallback agents.

All deterministic; no network, no API keys.
"""

import pytest

from eventagents import (
    ApprovalGateway,
    CascadeTracker,
    CommsAgent,
    EventBus,
    FallbackMonitor,
    QualityAgent,
    SimulatedHuman,
    SummarizerAgent,
    approve_below_severity,
    degraded_quality_fallback,
    gen_sensor_stream,
    inject_overheat,
    inject_vibration_spike,
    with_fallback,
)


def publish_stream(bus, readings):
    for r in readings:
        bus.publish(f"sensors.{r['equipment_id']}", r)


def attacked_stream(seed=7, n=200, multi=True):
    stream = gen_sensor_stream(n=n, seed=seed)
    attacked = inject_overheat(stream, "mixer-01", start_seq=120, delta=18.0)
    if multi:
        attacked = inject_vibration_spike(attacked, "mixer-01",
                                          start_seq=120, factor=6.0)
    return attacked


def make_factory():
    """Bus wired with quality + summarizer + comms + tracker."""
    bus = EventBus()
    qa = QualityAgent(bus)
    summ = SummarizerAgent(bus)
    comms = CommsAgent(bus)
    tracker = CascadeTracker(bus)
    return bus, qa, summ, comms, tracker


# ------------------------------------------------------- choreography
def test_quarantine_cascades_to_warning_summary():
    bus, qa, summ, comms, tracker = make_factory()
    actions_seen = []
    bus.subscribe("quality.actions", actions_seen.append)
    publish_stream(bus, attacked_stream(multi=False))
    assert summ.summaries, "expected summaries from quarantine actions"
    s = summ.summaries[0]
    assert s["action"] == "quarantine_batch"
    assert s["severity"] == "warning"
    assert s["original_event_id"] == actions_seen[0]["event_id"]
    assert s["equipment_id"] == "mixer-01"


def test_stopline_produces_critical_summary():
    bus, qa, summ, comms, tracker = make_factory()
    publish_stream(bus, attacked_stream(multi=True))
    crit = [s for s in summ.summaries if s["severity"] == "critical"]
    assert crit, "expected a critical summary for stop_line"
    assert crit[0]["action"] == "stop_line"


def test_critical_summary_pages_urgent():
    bus, qa, summ, comms, tracker = make_factory()
    urgent = []
    bus.subscribe("comms.urgent", urgent.append)
    publish_stream(bus, attacked_stream(multi=True))
    assert urgent, "expected a page on comms.urgent"
    assert urgent[0]["payload"]["channel"] == "page"
    assert urgent[0]["payload"]["severity"] == "critical"


def test_warning_summary_goes_to_digest():
    bus, qa, summ, comms, tracker = make_factory()
    digest = []
    urgent = []
    bus.subscribe("comms.digest", digest.append)
    bus.subscribe("comms.urgent", urgent.append)
    publish_stream(bus, attacked_stream(multi=False))
    assert digest, "expected quarantine summaries on comms.digest"
    assert digest[0]["payload"]["channel"] == "digest"
    # routing is severity-correct even if a coincidental vibration outlier
    # escalates one cascade to critical (multi-sensor by design)
    assert all(e["payload"]["severity"] == "critical" for e in urgent)
    assert all(e["payload"]["severity"] == "warning" for e in digest)


def test_routed_event_cites_trigger():
    bus, qa, summ, comms, tracker = make_factory()
    urgent = []
    bus.subscribe("comms.urgent", urgent.append)
    publish_stream(bus, attacked_stream(multi=True))
    u = urgent[0]["payload"]
    assert "z=" in u["triggered_by"] and "baseline" in u["triggered_by"]
    assert u["equipment_id"] == "mixer-01"
    assert "stop_line" in u["note"]


def test_full_cascade_four_link_chain_in_order():
    bus, qa, summ, comms, tracker = make_factory()
    publish_stream(bus, attacked_stream(multi=True))
    stops = [a for a in qa.actions if a["action"] == "stop_line"]
    assert stops
    chain = tracker.chain_for(stops[0]["root_id"])
    topics = [e["topic"] for e in chain]
    assert topics == ["sensors.mixer-01", "quality.actions",
                      "notify.summaries", "comms.urgent"]
    # causal linkage: every link shares the root
    roots = {e["payload"].get("root_id", e["event_id"]) for e in chain}
    assert roots == {stops[0]["root_id"]}


def test_no_crosstalk_from_unrelated_events():
    bus, qa, summ, comms, tracker = make_factory()
    bus.publish("orders.new", {"order_id": "ord-1"})
    bus.publish("random.topic", {"v": 1})
    publish_stream(bus, gen_sensor_stream(n=150, seed=11))  # clean
    assert summ.summaries == []
    assert comms.routed == []
    assert bus.history("notify.summaries") == []
    assert bus.history("comms.urgent") == []
    assert bus.history("comms.digest") == []


def test_summary_key_numbers_cite_evidence():
    bus, qa, summ, comms, tracker = make_factory()
    publish_stream(bus, attacked_stream(multi=False))
    s = summ.summaries[0]
    assert "z=" in s["key_numbers"] and "baseline" in s["key_numbers"]


# ---------------------------------------------------------------- hitl
def make_gated(multi=True, policy="approve_all", timeout=5.0):
    bus = EventBus()
    gateway = ApprovalGateway(bus)
    human = SimulatedHuman(bus, policy=policy)
    qa = QualityAgent(bus, approval_gateway=gateway,
                      approval_timeout=timeout)
    return bus, gateway, human, qa


def test_approved_stop_line_proceeds():
    bus, gateway, human, qa = make_gated(policy="approve_all")
    actions = []
    bus.subscribe("quality.actions", actions.append)
    publish_stream(bus, attacked_stream(multi=True))
    stops = [a["payload"] for a in actions if a["payload"]["action"] == "stop_line"]
    assert stops, "approved stop_line should be published"
    assert stops[0]["approved_by"] == "simulated-human"
    assert qa.deferred == []


def test_denied_stop_line_defers_with_reason():
    bus, gateway, human, qa = make_gated(policy="deny_all")
    actions = []
    deferred = []
    bus.subscribe("quality.actions", actions.append)
    bus.subscribe("quality.action_deferred", deferred.append)
    publish_stream(bus, attacked_stream(multi=True))
    assert [a for a in actions if a["payload"]["action"] == "stop_line"] == []
    assert deferred, "expected quality.action_deferred on denial"
    d = deferred[0]["payload"]
    assert d["deferral"] == "denied"
    assert d["decided_by"] == "simulated-human"
    assert d["defer_reason"]
    assert d["equipment_id"] == "mixer-01"


def test_timeout_defers_and_publishes_timed_out():
    bus = EventBus()
    gateway = ApprovalGateway(bus)  # no human subscribed
    timed_out = []
    bus.subscribe("approval.timed_out", timed_out.append)
    decision = gateway.request_approval("stop_line", {"severity": "critical"},
                                        timeout_s=0.05)
    assert decision["decision"] == "timeout"
    assert timed_out and timed_out[0]["payload"]["action"] == "stop_line"


def test_quarantine_stays_automatic_with_gateway():
    bus, gateway, human, qa = make_gated(policy="deny_all")
    requested = []
    bus.subscribe("approval.requested", requested.append)
    publish_stream(bus, attacked_stream(multi=False))
    quarantines = [a for a in qa.actions if a["action"] == "quarantine_batch"]
    assert quarantines, "quarantine must not need approval"
    requested_actions = {e["payload"]["action"] for e in requested}
    assert "quarantine_batch" not in requested_actions, \
        "quarantine must never go through approval"
    assert qa.deferred == [] or all(
        d["action"] == "stop_line" for d in qa.deferred)


def test_approve_quarantine_only_policy():
    bus = EventBus()
    gateway = ApprovalGateway(bus)
    SimulatedHuman(bus, policy="approve_quarantine_only")
    q = gateway.request_approval("quarantine_batch", {}, timeout_s=2.0)
    s = gateway.request_approval("stop_line", {}, timeout_s=2.0)
    assert q["decision"] == "granted"
    assert s["decision"] == "denied"
    assert "line stop" in s["reason"]


def test_approval_request_payload_shape():
    bus = EventBus()
    gateway = ApprovalGateway(bus)
    SimulatedHuman(bus, policy="approve_all")
    requested = []
    bus.subscribe("approval.requested", requested.append)
    gateway.request_approval("stop_line",
                             {"equipment_id": "mixer-01", "severity": "critical"},
                             timeout_s=2.0)
    r = requested[0]["payload"]
    assert r["approval_id"] and r["action"] == "stop_line"
    assert r["context"]["equipment_id"] == "mixer-01"
    assert "requested_at" in r


def test_severity_threshold_policy():
    bus = EventBus()
    gateway = ApprovalGateway(bus)
    SimulatedHuman(bus, policy=approve_below_severity("warning"))
    low = gateway.request_approval("quarantine_batch",
                                   {"severity": "warning"}, timeout_s=2.0)
    high = gateway.request_approval("stop_line",
                                    {"severity": "critical"}, timeout_s=2.0)
    assert low["decision"] == "granted"
    assert high["decision"] == "denied"


def test_simulated_human_records_decisions():
    bus = EventBus()
    gateway = ApprovalGateway(bus)
    human = SimulatedHuman(bus, policy="deny_all")
    gateway.request_approval("stop_line", {}, timeout_s=2.0)
    assert len(human.decisions) == 1
    assert human.decisions[0]["decision"] == "denied"
    assert human.decisions[0]["decided_by"] == "simulated-human"


# ------------------------------------------------------------- fallback
def test_primary_exception_triggers_fallback_dlq_and_monitor():
    bus = EventBus()
    monitor = FallbackMonitor()
    deferred = []
    orders = []
    bus.subscribe("quality.action_deferred", deferred.append)
    bus.subscribe("maintenance.work_orders", orders.append)

    def primary(event):
        raise RuntimeError("quality agent crashed")

    with_fallback(bus, "sensors.mixer-01", primary,
                  degraded_quality_fallback(bus),
                  name="quality-agent", monitor=monitor)
    bus.publish("sensors.mixer-01", {"equipment_id": "mixer-01",
                                    "sensor": "temperature",
                                    "batch_id": "batch-mixer-01-006"})
    assert deferred, "fallback must defer the automated action"
    assert "manual inspection" in deferred[0]["payload"]["reason"]
    assert orders and orders[0]["payload"]["failure_mode"] == "manual_inspection"
    assert len(bus.dead_letters) == 1
    assert monitor.report() == {"quality-agent": {"primary": 1, "fallback": 1}}


def test_primary_success_skips_fallback():
    bus = EventBus()
    monitor = FallbackMonitor()
    handled = []
    fallback_calls = []

    def primary(event):
        handled.append(event)

    def fallback(event, exc):
        fallback_calls.append(event)

    with_fallback(bus, "t", primary, fallback, name="agent-a",
                  monitor=monitor)
    bus.publish("t", {"v": 1})
    assert len(handled) == 1 and fallback_calls == []
    assert bus.dead_letters == []
    assert monitor.report() == {"agent-a": {"primary": 1, "fallback": 0}}


def test_fallback_monitor_report_mixed_outcomes():
    bus = EventBus()
    monitor = FallbackMonitor()
    calls = {"n": 0}

    def flaky(event):
        calls["n"] += 1
        if calls["n"] % 2 == 0:
            raise RuntimeError("intermittent")

    with_fallback(bus, "t", flaky, lambda e, x: None,
                  name="flaky", monitor=monitor)
    with_fallback(bus, "t", lambda e: None, lambda e, x: None,
                  name="steady", monitor=monitor)
    for i in range(4):
        bus.publish("t", {"i": i})
    report = monitor.report()
    assert report["flaky"] == {"primary": 4, "fallback": 2}
    assert report["steady"] == {"primary": 4, "fallback": 0}


def test_dlq_names_the_failing_agent():
    bus = EventBus()

    def primary(event):
        raise RuntimeError("boom")

    with_fallback(bus, "t", primary, lambda e, x: None,
                  name="quality-agent")
    bus.publish("t", {"v": 1})
    assert len(bus.dead_letters) == 1
    assert "quality-agent" in bus.dead_letters[0].handler_name


def test_fallback_wrapper_ignores_other_topics():
    bus = EventBus()
    monitor = FallbackMonitor()
    with_fallback(bus, "t", lambda e: None, lambda e, x: None,
                  name="agent-a", monitor=monitor)
    bus.publish("other", {"v": 1})
    assert monitor.report() == {}


def test_degraded_fallback_cites_manual_inspection():
    bus = EventBus()
    orders = []
    bus.subscribe("maintenance.work_orders", orders.append)
    handler = degraded_quality_fallback(bus)
    handler({"payload": {"equipment_id": "oven-02", "sensor": "vibration"}},
            RuntimeError("crash"))
    assert orders[0]["payload"]["equipment_id"] == "oven-02"
    assert "manually inspect" in orders[0]["payload"]["recommended_action"]
