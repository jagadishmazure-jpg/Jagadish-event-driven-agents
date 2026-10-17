# 03 — Architecture diagrams

All diagrams are Mermaid (they render on GitHub). Sections 1 to 3 show general
event-driven patterns in my own drawing; sections 4 to 7 show this repo. The
notes in `01` and `02` explain them.

## 1. Smart factory event flow

Sensors publish; each consumer subscribes independently. New sensors or
new consumers join without changes on the other side.

```mermaid
flowchart LR
    T["Temperature sensor"] --> M["Event mesh"]
    V["Vibration sensor"] --> M
    P["Pressure sensor"] --> M
    AQ["Air-quality sensor"] --> M
    M --> Q["Quality app\n(deviation → rework / stop line)"]
    M --> FD["Fault detection"]
    M --> OC["Operator console"]
    M --> DW["Data warehouse"]
    Q --> QI["Quality inspection events"]
    QI --> M
```

## 2. Event mesh across the value chain

Only the needed subset crosses expensive WAN links — filter, buffer,
secure, observe.

```mermaid
flowchart TB
    subgraph Plant["Plant floor (LAN)"]
        S["Sensors / MES / SCADA"]
    end
    subgraph Mesh["Event mesh"]
        F["Filter: what must travel?"]
        B["Buffer: decouple sender/receiver"]
    end
    subgraph Cloud["Cloud / HQ"]
        AN["Analytics"]
        DT["Digital twin"]
        ERP["ERP / planning"]
    end
    S --> F --> B --> AN
    B --> DT
    B --> ERP
```

## 3. Event-driven agentic AI reference architecture

A general shape for event-driven agents: gateways ingest triggers, the mesh
distributes, agents act, the orchestrator coordinates, humans stay in
the loop.

```mermaid
flowchart TB
    subgraph Gateways["Gateways — trigger ingestion"]
        CH["Chat / web form"]
        CRM["CRM / ERP change"]
        IOT["IoT sensor spike"]
        TM["Time-based trigger"]
    end
    GW["Event gateways"] --> MESH["Event mesh\n(routing, filtering, persistence)"]
    CH --> GW
    CRM --> GW
    IOT --> GW
    TM --> GW
    MESH <--> ORCH["Orchestrator\n(task breakdown, dispatch,\nauthorization scopes)"]
    MESH <--> A1["Agent: language /\nretrieval"]
    MESH <--> A2["Agent: planning"]
    MESH <--> A3["Agent: API execution"]
    MESH <--> MCP["MCP servers\n(tools ↔ enterprise APIs)"]
    MESH <-.-> A2A["A2A\n(agent ↔ agent)"]
    ORCH --> HITL["Human review queue\n(validation / escalation)"]
    HITL --> MESH
    MESH --> OBS["Observability\n(decisions, rationale, lineage)"]
```

## 4. Choreography cascade (agent-to-agent, no orchestrator)

Agent-to-agent cascade in this repo: each agent
reacts to the previous agent's event. Implemented in
`eventagents/choreography.py`.

```mermaid
sequenceDiagram
    participant S as Sensor stream
    participant QA as QualityAgent
    participant SA as SummarizerAgent
    participant CA as CommsAgent
    participant TR as CascadeTracker
    S->>QA: sensors.mixer-01 (overheat)
    QA->>SA: quality.actions: quarantine_batch
    SA->>CA: notify.summaries (severity: critical)
    CA->>CA: comms.urgent (paged)
    TR->>TR: chain_for(root_id) → 4-link causal chain
```

## 5. Human-in-the-loop as an event participant

Approvals are events; the human is just another subscriber/publisher.
Implemented in `eventagents/hitl.py`.

```mermaid
sequenceDiagram
    participant QA as QualityAgent
    participant B as Event bus
    participant H as Human (UI / chat)
    QA->>B: approval.requested (stop_line)
    B->>H: approval.requested
    alt granted
        H->>B: approval.granted
        B->>QA: approval.granted
        QA->>B: quality.actions: stop_line (approved_by: human)
    else denied / timeout
        H->>B: approval.denied
        B->>QA: approval.denied
        QA->>B: quality.action_deferred (reason)
    end
```

## 6. Fallback agents

When the primary handler fails, a degraded-but-safe action keeps the
factory protected. Implemented in `eventagents/fallback.py`.

```mermaid
flowchart TB
    E["Event (e.g. sensor spike)"] --> PH["Primary handler"]
    PH -- "success" --> A["Normal action"]
    PH -- "exception" --> DLQ["Dead-letter queue\n(event + error recorded)"]
    PH -- "exception" --> FB["Fallback handler\n(degraded safe action)"]
    FB --> DA["quality.action_deferred +\nmanual-inspection work order"]
    MON["FallbackMonitor\n(primary vs fallback counts)"] -.-> PH
    MON -.-> FB
```

## 7. How this repo maps to the architectures

```mermaid
flowchart TB
    subgraph Sources["Event sources"]
        SE["eventagents.sensors\n(seedable streams + injectors)"]
        OR["orders.new"]
    end
    subgraph Bus["eventagents.bus — EventBus"]
        T["topics · filters · DLQ · replay"]
    end
    subgraph Agents["Agents (subscribers)"]
        QA["QualityAgent\n→ quarantine / stop_line"]
        MA["MaintenanceAgent\n→ work orders"]
        PA["ProductionAgent\n→ dynamic recipes"]
        SA["SummarizerAgent → notify.summaries"]
        CA2["CommsAgent → comms.urgent / digest"]
        FB2["Fallback agents"]
    end
    subgraph Humans["Humans"]
        HG["ApprovalGateway +\nSimulatedHuman"]
    end
    subgraph Views["Read models"]
        TW["DigitalTwin.snapshot()"]
        CT["CascadeTracker.chain_for()"]
        LG["EventLedger.verify_chain()"]
    end
    Sources --> Bus --> Agents
    Agents --> Bus
    HG <--> Bus
    Bus --> Views
```
