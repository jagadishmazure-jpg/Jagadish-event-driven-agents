# Implementation guide: from offline build to Azure

> Status: **planned**. Nothing here has been deployed. The offline build in this
> repository is what exists today; this guide is how it would be taken to a
> real Azure environment.

## Azure service mapping

| Offline component | Azure service (planned) | Notes |
|---|---|---|
| `EventBus` | Azure Event Hubs (Kafka protocol) or Service Bus topics | Consumer groups per agent |
| Agents | Azure Container Apps or Functions with Event Hubs triggers | Scale on lag |
| Dead letters | Service Bus dead-letter queue |  |
| Digital twin | Azure Digital Twins or Cosmos DB read model |  |
| Approvals | Logic Apps or Teams approvals publishing back to the bus |  |
| Ledger | Immutable Blob storage or Azure Confidential Ledger |  |
| Schemas | Event Hubs schema registry, starting from docs/topics.md |  |

## Steps

1. Swap `EventBus` for an Event Hubs client behind the same publish/subscribe contract.
2. Make every handler idempotent (the maintenance agent's de-dup is the model) and let the broker own offsets.
3. Register event schemas from docs/topics.md.
4. Wire `ApprovalGateway` to Teams approvals with asynchronous resumption.
5. Add incident-level de-duplication for line stops.

## Guardrails to keep when going live

- Line stops (the costly action) need human approval when a gateway is wired; quarantines stay automatic.
- Denied or timed-out approvals become `quality.action_deferred`, never a silent drop.
- A failing agent triggers a degraded safe action (defer plus manual-inspection work order) and a dead letter.
- Every action carries its evidence and a `root_id` back to the triggering reading.
- Hash-chained ledger of every event.

## What to measure before calling it production-ready

- Time from fault reading to quarantine.
- False quarantines per week against operator review.
- Approval turnaround and the share of deferred actions.
