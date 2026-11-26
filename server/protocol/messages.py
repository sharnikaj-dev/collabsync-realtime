"""Structured JSON WebSocket message protocol for CollabSync Phase 1."""

from __future__ import annotations

import json
import time
from enum import Enum
from typing import Any


class MessageType(str, Enum):
    JOIN_ROOM = "JOIN_ROOM"
    LEAVE_ROOM = "LEAVE_ROOM"
    STATE_PATCH = "STATE_PATCH"
    CURSOR_MOVE = "CURSOR_MOVE"
    SYNC_ACK = "SYNC_ACK"
    PRESENCE_UPDATE = "PRESENCE_UPDATE"
    PING = "PING"
    PONG = "PONG"
    ERROR = "ERROR"


VALID_OP_KINDS = frozenset({"insert", "delete", "replace"})
MAX_ROOM_ID_LEN = 128
MAX_DISPLAY_NAME_LEN = 64
MAX_PATCH_TEXT_LEN = 1_000_000


class ProtocolError(ValueError):
    """Raised when an incoming WebSocket message fails schema validation."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def _require_non_empty_str(
    payload: dict[str, Any], field: str, max_len: int = 256
) -> str:
    value = payload.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ProtocolError("INVALID_FIELD", f"Field '{field}' must be a non-empty string.")
    trimmed = value.strip()
    if len(trimmed) > max_len:
        raise ProtocolError(
            "FIELD_TOO_LONG", f"Field '{field}' exceeds max length of {max_len}."
        )
    return trimmed


def _require_non_neg_int(payload: dict[str, Any], field: str) -> int:
    value = payload.get(field)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ProtocolError(
            "INVALID_FIELD", f"Field '{field}' must be a non-negative integer."
        )
    return value


def parse_client_message(raw: str | bytes | dict[str, Any]) -> dict[str, Any]:
    """Parse and validate an incoming JSON protocol message from a client."""
    if isinstance(raw, (str, bytes)):
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ProtocolError("INVALID_JSON", f"Malformed JSON payload: {exc}") from exc
    elif isinstance(raw, dict):
        data = dict(raw)
    else:
        raise ProtocolError("INVALID_PAYLOAD", "Payload must be a JSON object.")

    if not isinstance(data, dict):
        raise ProtocolError("INVALID_PAYLOAD", "Top-level JSON value must be an object.")

    raw_type = data.get("type")
    if not isinstance(raw_type, str):
        raise ProtocolError("MISSING_TYPE", "Message must include a string 'type' field.")

    try:
        msg_type = MessageType(raw_type)
    except ValueError as exc:
        raise ProtocolError("UNKNOWN_TYPE", f"Unsupported message type '{raw_type}'.") from exc

    if msg_type in (MessageType.PING, MessageType.PONG):
        return {
            "type": msg_type.value,
            "ts": float(data.get("ts") or time.time()),
        }

    if msg_type == MessageType.JOIN_ROOM:
        room_id = _require_non_empty_str(data, "room_id", MAX_ROOM_ID_LEN)
        display_name = data.get("display_name")
        if display_name is not None:
            if not isinstance(display_name, str) or not display_name.strip():
                raise ProtocolError(
                    "INVALID_FIELD", "Field 'display_name' must be a non-empty string."
                )
            display_name = display_name.strip()[:MAX_DISPLAY_NAME_LEN]
        user_id = data.get("user_id")
        if user_id is not None and (not isinstance(user_id, str) or not user_id.strip()):
            raise ProtocolError("INVALID_FIELD", "Field 'user_id' must be a non-empty string.")
        color = data.get("color")
        if color is not None and not isinstance(color, str):
            raise ProtocolError("INVALID_FIELD", "Field 'color' must be a string.")
        return {
            "type": msg_type.value,
            "room_id": room_id,
            "user_id": user_id.strip() if isinstance(user_id, str) else None,
            "display_name": display_name,
            "color": color,
        }

    if msg_type == MessageType.LEAVE_ROOM:
        room_id = _require_non_empty_str(data, "room_id", MAX_ROOM_ID_LEN)
        return {
            "type": msg_type.value,
            "room_id": room_id,
            "user_id": data.get("user_id"),
        }

    if msg_type == MessageType.STATE_PATCH:
        room_id = _require_non_empty_str(data, "room_id", MAX_ROOM_ID_LEN)
        base_version = _require_non_neg_int(data, "base_version")
        client_seq = data.get("client_seq", 0)
        if isinstance(client_seq, bool) or not isinstance(client_seq, int) or client_seq < 0:
            raise ProtocolError(
                "INVALID_FIELD", "Field 'client_seq' must be a non-negative integer."
            )

        raw_op = data.get("op")
        if not isinstance(raw_op, dict):
            raise ProtocolError("INVALID_FIELD", "Field 'op' must be an object.")

        kind = raw_op.get("kind")
        if kind not in VALID_OP_KINDS:
            raise ProtocolError(
                "INVALID_OP",
                f"Operation 'kind' must be one of {sorted(VALID_OP_KINDS)}.",
            )

        position = _require_non_neg_int(raw_op, "position")
        text = raw_op.get("text", "")
        if not isinstance(text, str):
            raise ProtocolError("INVALID_OP", "Operation 'text' must be a string.")
        if len(text) > MAX_PATCH_TEXT_LEN:
            raise ProtocolError("OP_TOO_LARGE", "Operation 'text' exceeds maximum allowed size.")

        length = raw_op.get("length", 0)
        if isinstance(length, bool) or not isinstance(length, int) or length < 0:
            raise ProtocolError("INVALID_OP", "Operation 'length' must be a non-negative integer.")

        if kind == "insert" and length != 0:
            raise ProtocolError("INVALID_OP", "Insert operation must have length=0.")
        if kind == "delete" and text != "":
            raise ProtocolError("INVALID_OP", "Delete operation must have empty text.")

        ts_raw = raw_op.get("timestamp")
        if ts_raw is not None:
            if isinstance(ts_raw, bool) or not isinstance(ts_raw, (int, float)):
                raise ProtocolError("INVALID_OP", "Operation 'timestamp' must be numeric.")
            op_ts = float(ts_raw)
        else:
            op_ts = time.time()

        return {
            "type": msg_type.value,
            "room_id": room_id,
            "user_id": data.get("user_id"),
            "client_seq": client_seq,
            "base_version": base_version,
            "op": {
                "kind": kind,
                "position": position,
                "text": text,
                "length": length,
                "timestamp": op_ts,
            },
        }

    if msg_type == MessageType.CURSOR_MOVE:
        room_id = _require_non_empty_str(data, "room_id", MAX_ROOM_ID_LEN)
        position = _require_non_neg_int(data, "position")
        selection_end = data.get("selection_end", position)
        if (
            isinstance(selection_end, bool)
            or not isinstance(selection_end, int)
            or selection_end < 0
        ):
            raise ProtocolError(
                "INVALID_FIELD", "Field 'selection_end' must be a non-negative integer."
            )
        base_version = data.get("base_version", 0)
        if (
            isinstance(base_version, bool)
            or not isinstance(base_version, int)
            or base_version < 0
        ):
            raise ProtocolError(
                "INVALID_FIELD", "Field 'base_version' must be a non-negative integer."
            )
        return {
            "type": msg_type.value,
            "room_id": room_id,
            "user_id": data.get("user_id"),
            "base_version": base_version,
            "position": position,
            "selection_end": selection_end,
        }

    raise ProtocolError("UNHANDLED_TYPE", f"Client cannot send message type '{msg_type.value}'.")


def build_join_room_ack(
    *,
    room_id: str,
    user_id: str,
    display_name: str,
    color: str,
    version: int,
    content: str,
    content_hash: str,
    participants: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "type": MessageType.JOIN_ROOM.value,
        "room_id": room_id,
        "user_id": user_id,
        "display_name": display_name,
        "color": color,
        "version": version,
        "content": content,
        "content_hash": content_hash,
        "participants": participants,
    }


def build_leave_room_event(
    *,
    room_id: str,
    user_id: str,
    display_name: str,
    reason: str,
    version: int,
) -> dict[str, Any]:
    return {
        "type": MessageType.LEAVE_ROOM.value,
        "room_id": room_id,
        "user_id": user_id,
        "display_name": display_name,
        "reason": reason,
        "version": version,
    }


def build_state_patch_broadcast(
    *,
    room_id: str,
    user_id: str,
    client_seq: int,
    base_version: int,
    version: int,
    ops: list[dict[str, Any]],
    content_hash: str,
) -> dict[str, Any]:
    return {
        "type": MessageType.STATE_PATCH.value,
        "room_id": room_id,
        "user_id": user_id,
        "client_seq": client_seq,
        "base_version": base_version,
        "version": version,
        "op": ops[0] if len(ops) == 1 else None,
        "ops": ops,
        "content_hash": content_hash,
    }


def build_cursor_move_broadcast(
    *,
    room_id: str,
    user_id: str,
    version: int,
    position: int,
    selection_end: int,
) -> dict[str, Any]:
    return {
        "type": MessageType.CURSOR_MOVE.value,
        "room_id": room_id,
        "user_id": user_id,
        "version": version,
        "position": position,
        "selection_end": selection_end,
    }


def build_sync_ack(
    *,
    room_id: str,
    user_id: str,
    client_seq: int,
    base_version: int,
    version: int,
    status: str,
    ops: list[dict[str, Any]] | None,
    content: str | None,
    content_hash: str,
) -> dict[str, Any]:
    return {
        "type": MessageType.SYNC_ACK.value,
        "room_id": room_id,
        "user_id": user_id,
        "client_seq": client_seq,
        "base_version": base_version,
        "version": version,
        "status": status,
        "op": ops[0] if ops and len(ops) == 1 else None,
        "ops": ops,
        "content": content,
        "content_hash": content_hash,
    }


def build_presence_update(
    *,
    room_id: str,
    version: int,
    participants: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "type": MessageType.PRESENCE_UPDATE.value,
        "room_id": room_id,
        "version": version,
        "participants": participants,
    }


def build_ping(ts: float | None = None) -> dict[str, Any]:
    return {
        "type": MessageType.PING.value,
        "ts": time.time() if ts is None else ts,
    }


def build_pong(ts: float | None = None) -> dict[str, Any]:
    return {
        "type": MessageType.PONG.value,
        "ts": time.time() if ts is None else ts,
    }


def build_error(code: str, message: str) -> dict[str, Any]:
    return {
        "type": MessageType.ERROR.value,
        "code": code,
        "message": message,
    }
