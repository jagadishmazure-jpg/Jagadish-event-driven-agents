# 02: Why agents and events fit together

My own notes on applying event-driven design to AI agents, and how this repo
does it. Background reading is credited at the end; nothing here is copied from it.

## The hard part is not the agent

A single agent is easy to demo. The difficulty comes when there are many: how
they learn that something happened, how they hand work to each other, how a
person can step in, and how you explain afterwards why something was done. Those
are the same problems microservices had, and events are a proven answer.

## What agents need from the platform

| Need | How this repo meets it |
|---|---|
| React quickly to change | Agents subscribe to sensor and order topics instead of polling |
| Loose coupling | Agents only know topics, never each other; `choreography.py` adds agents that react to other agents' events |
| A person in the loop | `hitl.py`: `ApprovalGateway` publishes `approval.requested` and waits, with a timeout, for granted or denied |
| Failure handling | Dead-letter queue on the bus; `fallback.py` gives a default action when an agent fails |
| Explainability | Every action cites its evidence (reading, baseline, z-score); `CascadeTracker` links each cascade back to the sensor event that started it |
| Audit | Hash-chained event ledger |
| Open protocols to tools and other agents (MCP, A2A) | Not in this repo; see Jagadish-azure-ai-integration-platform and Jagadish-food-ingredients-agentic-ai |

## Three jobs for real-time data

1. **Trigger** work: an event starts an agent.
2. **Enrich** decisions: fresh events keep retrieval stores and baselines current.
3. **Speed up** action: an agent with current context and the right tools acts sooner.

## Choreography versus orchestration

This repo leans on choreography: the quality agent publishes an action, a
summariser turns it into a notification, and a comms agent decides whether it
pages someone. Nobody coordinates the chain, yet `CascadeTracker.chain_for(root_id)`
can still show it end to end. Orchestration is still right when a business
process needs a fixed order of steps and a single owner; the two can coexist on
the same bus.

## Where I would start in a real programme

Prototype with one team, map the events that already exist across domains, pilot
a multi-agent flow on one business-critical case, set trust and approval rules
for agents early, and measure business outcomes rather than only uptime.

