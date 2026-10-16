"""Hash-chained event ledger: tamper-evident record of published events.

Each entry chains to the previous entry's SHA-256 hash, so any alteration
of history breaks ``verify_chain()``.
"""

from __future__ import annotations

import hashlib
import json
from typing import Dict, List


class EventLedger:
    def __init__(self) -> None:
        self.entries: List[Dict] = []
        self._last_hash = "GENESIS"

    def record(self, event: Dict) -> Dict:
        body = json.dumps(event, sort_keys=True, default=str)
        entry_hash = hashlib.sha256(
            (self._last_hash + body).encode("utf-8")
        ).hexdigest()
        entry = {
            "seq": len(self.entries),
            "prev_hash": self._last_hash,
            "hash": entry_hash,
            # body is the only event copy: no redundant fields to tamper with
            "body": body,
        }
        self.entries.append(entry)
        self._last_hash = entry_hash
        return entry

    def verify_chain(self) -> bool:
        prev = "GENESIS"
        for i, entry in enumerate(self.entries):
            if entry["seq"] != i or entry["prev_hash"] != prev:
                return False
            recomputed = hashlib.sha256(
                (entry["prev_hash"] + entry["body"]).encode("utf-8")
            ).hexdigest()
            if recomputed != entry["hash"]:
                return False
            prev = entry["hash"]
        return True

    def export_jsonl(self) -> str:
        return "\n".join(json.dumps(e, sort_keys=True) for e in self.entries)
