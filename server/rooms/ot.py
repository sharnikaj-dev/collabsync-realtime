"""Deterministic Operational Transform (OT) and LWW tie-breaking for text splices."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Sequence


@dataclass(frozen=True)
class SpliceOp:
    """Canonical text splice: delete `delete_len` chars at `position`, then insert `insert_text`."""

    position: int
    delete_len: int
    insert_text: str
    user_id: str = ""
    timestamp: float = 0.0

    @classmethod
    def from_protocol(
        cls, op_dict: dict[str, Any], user_id: str = "", default_ts: float = 0.0
    ) -> "SpliceOp":
        kind = op_dict["kind"]
        position = int(op_dict["position"])
        text = str(op_dict.get("text", ""))
        length = int(op_dict.get("length", 0))
        ts = float(op_dict.get("timestamp", default_ts))

        if kind == "insert":
            return cls(
                position=position,
                delete_len=0,
                insert_text=text,
                user_id=user_id,
                timestamp=ts,
            )
        if kind == "delete":
            return cls(
                position=position,
                delete_len=length,
                insert_text="",
                user_id=user_id,
                timestamp=ts,
            )
        return cls(
            position=position,
            delete_len=length,
            insert_text=text,
            user_id=user_id,
            timestamp=ts,
        )

    def to_protocol(self) -> dict[str, Any]:
        if self.delete_len == 0:
            kind = "insert"
        elif self.insert_text == "":
            kind = "delete"
        else:
            kind = "replace"
        return {
            "kind": kind,
            "position": self.position,
            "text": self.insert_text,
            "length": self.delete_len,
            "timestamp": self.timestamp,
        }

    @property
    def delta(self) -> int:
        return len(self.insert_text) - self.delete_len

    def is_noop(self) -> bool:
        return self.delete_len == 0 and self.insert_text == ""


def compute_content_hash(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()[:16]


def apply_op(content: str, op: SpliceOp) -> str:
    """Apply a single SpliceOp safely with deterministic boundary clamping."""
    doc_len = len(content)
    pos = max(0, min(op.position, doc_len))
    del_len = max(0, min(op.delete_len, doc_len - pos))
    return content[:pos] + op.insert_text + content[pos + del_len :]


def apply_ops(content: str, ops: Sequence[SpliceOp]) -> str:
    result = content
    for op in ops:
        result = apply_op(result, op)
    return result


def _tie_break_before(op_a: SpliceOp, op_b: SpliceOp) -> bool:
    """Deterministic tie-breaker when two concurrent inserts target the exact same offset.

    Returns True if op_a should appear BEFORE op_b's inserted text.
    Uses timestamp then user_id lexicographical order.
    """
    return (op_a.timestamp, op_a.user_id) < (op_b.timestamp, op_b.user_id)


def transform_single(op_a: SpliceOp, op_b: SpliceOp) -> list[SpliceOp]:
    """Transform `op_a` so it applies cleanly after committed `op_b` (Inclusion Transform)."""
    if op_a.is_noop():
        return []
    if op_b.is_noop():
        return [op_a]

    a_start = op_a.position
    a_end = op_a.position + op_a.delete_len
    b_start = op_b.position
    b_end = op_b.position + op_b.delete_len
    b_ins = len(op_b.insert_text)
    b_delta = b_ins - op_b.delete_len

    # Case 1: Both are pure inserts at the exact same position -> deterministic tie-break
    if (
        op_a.delete_len == 0
        and op_b.delete_len == 0
        and a_start == b_start
    ):
        pos = b_start if _tie_break_before(op_a, op_b) else b_start + b_ins
        return [
            SpliceOp(
                position=pos,
                delete_len=0,
                insert_text=op_a.insert_text,
                user_id=op_a.user_id,
                timestamp=op_a.timestamp,
            )
        ]

    # Case 2: op_a is strictly before op_b's affected range
    if a_end <= b_start:
        return [op_a]

    # Case 3: op_a starts at or after op_b's deleted range
    if a_start >= b_end:
        return [
            SpliceOp(
                position=max(0, a_start + b_delta),
                delete_len=op_a.delete_len,
                insert_text=op_a.insert_text,
                user_id=op_a.user_id,
                timestamp=op_a.timestamp,
            )
        ]

    # Case 4: op_a is a pure insert strictly inside op_b's deleted range (b_start < a_start < b_end)
    if op_a.delete_len == 0:
        return [
            SpliceOp(
                position=b_start + b_ins,
                delete_len=0,
                insert_text=op_a.insert_text,
                user_id=op_a.user_id,
                timestamp=op_a.timestamp,
            )
        ]

    # Case 5: op_a encloses op_b completely (a_start < b_start and a_end > b_end)
    if a_start < b_start and a_end > b_end:
        left_del = b_start - a_start
        right_del = a_end - b_end
        if b_ins == 0:
            return [
                SpliceOp(
                    position=a_start,
                    delete_len=left_del + right_del,
                    insert_text=op_a.insert_text,
                    user_id=op_a.user_id,
                    timestamp=op_a.timestamp,
                )
            ]
        # Preserve op_b's inserted text by splitting deletion around b_ins
        op1 = SpliceOp(
            position=a_start,
            delete_len=left_del,
            insert_text=op_a.insert_text,
            user_id=op_a.user_id,
            timestamp=op_a.timestamp,
        )
        op2_pos = a_start + len(op_a.insert_text) + b_ins
        op2 = SpliceOp(
            position=op2_pos,
            delete_len=right_del,
            insert_text="",
            user_id=op_a.user_id,
            timestamp=op_a.timestamp,
        )
        return [op for op in (op1, op2) if not op.is_noop()]

    # Case 6: op_a's deleted range is completely enclosed within [b_start, b_end]
    if a_start >= b_start and a_end <= b_end:
        if not op_a.insert_text:
            return []
        return [
            SpliceOp(
                position=b_start + b_ins,
                delete_len=0,
                insert_text=op_a.insert_text,
                user_id=op_a.user_id,
                timestamp=op_a.timestamp,
            )
        ]

    # Case 7: op_a overlaps the left boundary of op_b (a_start < b_start < a_end <= b_end)
    if a_start < b_start < a_end <= b_end:
        transformed = SpliceOp(
            position=a_start,
            delete_len=b_start - a_start,
            insert_text=op_a.insert_text,
            user_id=op_a.user_id,
            timestamp=op_a.timestamp,
        )
        return [] if transformed.is_noop() else [transformed]

    # Case 8: op_a overlaps the right boundary of op_b (b_start <= a_start < b_end < a_end)
    transformed = SpliceOp(
        position=b_start + b_ins,
        delete_len=a_end - b_end,
        insert_text=op_a.insert_text,
        user_id=op_a.user_id,
        timestamp=op_a.timestamp,
    )
    return [] if transformed.is_noop() else [transformed]


def transform_against_history(
    op: SpliceOp, committed_history: Sequence[SpliceOp]
) -> list[SpliceOp]:
    """Transform `op` against a sequence of already-committed operations."""
    ops: list[SpliceOp] = [op]
    for committed in committed_history:
        next_ops: list[SpliceOp] = []
        c_frame = committed
        for sub_op in ops:
            if sub_op.is_noop():
                continue
            transformed_subs = transform_single(sub_op, c_frame)
            next_ops.extend(transformed_subs)
            # Advance c_frame into the post-sub_op coordinate frame for any split tail ops
            c_shifted = transform_single(c_frame, sub_op)
            if c_shifted:
                c_frame = c_shifted[0]
        ops = next_ops
        if not ops:
            break
    return ops


def transform_cursor(cursor_pos: int, committed_history: Sequence[SpliceOp]) -> int:
    """Rebase a cursor offset across a sequence of committed operations."""
    pos = max(0, cursor_pos)
    for op in committed_history:
        b_start = op.position
        b_end = op.position + op.delete_len
        b_ins = len(op.insert_text)
        b_delta = b_ins - op.delete_len
        if pos <= b_start:
            continue
        if pos <= b_end:
            pos = b_start + b_ins
        else:
            pos = max(0, pos + b_delta)
    return pos
