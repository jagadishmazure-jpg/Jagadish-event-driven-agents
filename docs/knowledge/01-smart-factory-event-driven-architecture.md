# 01: Event-driven design for a smart factory

My own notes on why this repo is event-driven, written for interview prep and
for anyone adopting the code. Background reading that shaped them is credited at
the end; nothing here is copied from it.

## The problem with polling

Factory systems (scheduling, execution, quality, maintenance) usually keep their
own database. To know what is happening right now, every system has to ask every
other one, and the answers are already old. Decisions made on old data are the
real cost: a bad batch keeps running until someone queries the right table.

The alternative is to turn each change into an event the moment it happens. A
sensor reading, a new order or a quality verdict is published once; anything that
cares subscribes. Producers do not know who listens, so adding a consumer never
touches the producer.

## Three patterns, three agents in this repo

| Pattern | What it means | Where it lives |
|---|---|---|
| Build to order | Each order event produces its own recipe instead of a fixed production run | `eventagents/production_agent.py` (`orders.new` to recipe) |
| Quality in the moment | Readings are compared with a rolling baseline; a deviation quarantines the batch or asks to stop the line | `eventagents/quality_agent.py` |
| Maintenance before failure | Equipment health is scored from the same stream and raises de-duplicated work orders | `eventagents/maintenance_agent.py` |

A **digital twin** (`eventagents/twin.py`) is the same events folded into a view
of what is true now. Because it is built from events, it can be rebuilt or
replayed at any time.

## What the event bus has to provide

`eventagents/bus.py` is an in-process stand-in for a real broker. The properties
that matter, and that a broker such as Azure Event Hubs would provide in
production:

- topics and filtered subscriptions, so each consumer sees only what it needs
- a dead-letter queue for handlers that fail
- replay, so a new consumer or a rebuilt twin can catch up
- a hash-chained ledger of every event (`eventagents/audit.py`) for audit

## Taking it beyond one plant

When events cross sites, bandwidth and trust become the constraints: send only
the subset another site needs, buffer so senders and receivers are decoupled,
lock down who can publish and subscribe, and keep one view of the whole flow for
operations.

