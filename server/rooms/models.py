"""Domain models for rooms, participants, cursors, and revision history."""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from .ot import SpliceOp, compute_content_hash


@dataclass
class CursorPosition:
    position: int = 0
    selection_end: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "position": self.position,
            "selection_end": self.selection_end,
        }


@dataclass
class Participant:
    user_id: str
    connection_id: str
    display_name: str
    color: str
    cursor: CursorPosition = field(default_factory=CursorPosition)
    joined_at: float = field(default_factory=time.time)
    last_seen: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        return {
            "user_id": self.user_id,
            "display_name": self.display_name,
            "color": self.color,
            "cursor": self.cursor.to_dict(),
            "joined_at": self.joined_at,
            "last_seen": self.last_seen,
        }


@dataclass(frozen=True)
class RevisionRecord:
    version: int
    user_id: str
    client_seq: int
    ops: tuple[SpliceOp, ...]
    timestamp: float


@dataclass
class RoomState:
    room_id: str
    content: str = ""
    version: int = 0
    max_history: int = 500
    participants: dict[str, Participant] = field(default_factory=dict)
    history: deque[RevisionRecord] = field(default_factory=deque)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)

    @property
    def content_hash(self) -> str:
        return compute_content_hash(self.content)

    @property
    def min_history_version(self) -> int:
        if not self.history:
            return self.version
        return self.history[0].version - 1

    def record_revision(self, record: RevisionRecord) -> None:
        self.history.append(record)
        while len(self.history) > self.max_history:
            self.history.popleft()

    def ops_since(self, base_version: int) -> list[SpliceOp]:
        """Return flattened committed ops applied strictly after base_version."""
        ops: list[SpliceOp] = []
        for rec in self.history:
            if rec.version > base_version:
                ops.extend(rec.ops)
        return ops

    def participant_list(self) -> list[dict[str, Any]]:
        return [
            p.to_dict()
            for p in sorted(
                self.participants.values(), key=lambda item: (item.joined_at, item.user_id)
            )
        ]
