# Interview points

Short, honest talking points for Jagadish-event-driven-agents. Every number quoted is from the
real run shown in the README.

- Agents only know topics, so a new agent joins without changing anyone else.
- Costly actions go through a human as events: in the demo, denied line stops become 46 deferred actions instead of stops.
- Traceability: `chain_for(root_id)` shows reading, quality action, summary and comms hop by hop.
- Honest gap: line-stop requests are not de-duplicated per incident yet.

## Likely follow-up questions

- Choreography or orchestration? (Choreography for reactions; orchestration when a process needs a fixed order and owner.)
- How do you avoid duplicate actions on redelivery? (Idempotent handlers keyed on event ids.)
- What if the approver never answers? (Timeout publishes a deferral; the line keeps running and the batch stays quarantined.)
