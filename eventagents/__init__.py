"""eventagents: event-driven agent architecture for smart manufacturing.

A simulated event mesh (in-process pub/sub bus) with autonomous agents that
subscribe to event streams and react: quality deviations trigger corrective
actions, equipment anomalies raise work orders, order events produce dynamic
manufacturing recipes. Vendor-neutral: the pattern, not any product.
"""

from .audit import EventLedger
from .bus import EventBus
from .choreography import CascadeTracker, CommsAgent, SummarizerAgent
from .fallback import (
    FallbackMonitor,
    degraded_quality_fallback,
    with_fallback,
)
from .hitl import (
    ApprovalGateway,
    SimulatedHuman,
    approve_below_severity,
)
from .maintenance_agent import MaintenanceAgent
from .production_agent import ProductionAgent
from .quality_agent import QualityAgent
from .sensors import (
    gen_sensor_stream,
    inject_overheat,
    inject_pressure_drop,
    inject_vibration_spike,
)
from .twin import DigitalTwin

__all__ = [
    "EventBus",
    "EventLedger",
    "QualityAgent",
    "MaintenanceAgent",
    "ProductionAgent",
    "DigitalTwin",
    "SummarizerAgent",
    "CommsAgent",
    "CascadeTracker",
    "ApprovalGateway",
    "SimulatedHuman",
    "approve_below_severity",
    "FallbackMonitor",
    "with_fallback",
    "degraded_quality_fallback",
    "gen_sensor_stream",
    "inject_overheat",
    "inject_vibration_spike",
    "inject_pressure_drop",
]
