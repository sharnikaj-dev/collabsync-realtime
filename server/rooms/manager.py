"""Room & State Manager: room isolation, monotonic versioning, OT resolution, and heartbeat cleanup."""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from typing import Any

from server.protocol import (
    MessageType,
    ProtocolError,
    build_cursor_move_broadcast,
    build_error,
    build_join_room_ack,
    build_leave_room_event,
    build_pong,
    build_presence_update,
    build_state_patch_broadcast,
    build_sync_ack,
    parse_client_message,
)
from server.transport.base import ClientConnection

from .models import CursorPosition, Participant, RevisionRecord, RoomState
from .ot import SpliceOp, apply_ops, transform_against_history, transform_cursor

PARTICIPANT_PALETTE = (
    "#2563EB",  # Blue
    "#059669",  # Emerald
    "#D97706",  # Amber
    "#DC2626",  # Crimson
    "#7C3AED",  # Violet
    "#0891B2",  # Cyan
    "#DB2777",  # Pink
    "#4F46E5",  # Indigo
)


class RoomManager:
    """Authoritative in-memory room, document revision, and presence coordinator."""

    def __init__(
        self,
        *,
        max_history: int = 500,
        heartbeat_interval: float = 10.0,
        heartbeat_timeout: float = 30.0,
        evict_empty_rooms: bool = False,
        default_content: str = "",
    ) -> None:
        self.max_history = max_history
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_timeout = heartbeat_timeout
        self.evict_empty_rooms = evict_empty_rooms
        self.default_content = default_content

        self._rooms: dict[str, RoomState] = {}
        self._room_locks: dict[str, asyncio.Lock] = {}
        self._connections: dict[str, ClientConnection] = {}
        # connection_id -> (room_id, user_id)
        self._conn_membership: dict[str, tuple[str, str]] = {}
        self._last_seen: dict[str, float] = {}
        self._heartbeat_task: asyncio.Task[None] | None = None

    def get_room(self, room_id: str) -> RoomState | None:
        return self._rooms.get(room_id)

    def list_room_ids(self) -> list[str]:
        return list(self._rooms.keys())

    def _get_or_create_room(self, room_id: str) -> tuple[RoomState, asyncio.Lock]:
        if room_id not in self._rooms:
            self._rooms[room_id] = RoomState(
                room_id=room_id,
                content=self.default_content,
                max_history=self.max_history,
            )
        if room_id not in self._room_locks:
            self._room_locks[room_id] = asyncio.Lock()
        return self._rooms[room_id], self._room_locks[room_id]

    def _pick_color(self, room: RoomState, preferred: str | None) -> str:
        if preferred and preferred.startswith("#") and len(preferred) in (4, 7):
            return preferred
        used = {p.color for p in room.participants.values()}
        for color in PARTICIPANT_PALETTE:
            if color not in used:
                return color
        return PARTICIPANT_PALETTE[len(room.participants) % len(PARTICIPANT_PALETTE)]

    def register_connection(self, conn: ClientConnection) -> None:
        self._connections[conn.connection_id] = conn
        self._last_seen[conn.connection_id] = time.monotonic()

    def touch_heartbeat(self, connection_id: str) -> None:
        now_mono = time.monotonic()
        self._last_seen[connection_id] = now_mono
        membership = self._conn_membership.get(connection_id)
        if membership:
            room_id, user_id = membership
            room = self._rooms.get(room_id)
            if room and user_id in room.participants:
                room.participants[user_id].last_seen = time.time()

    async def handle_raw_message(self, conn: ClientConnection, raw: str | bytes) -> None:
        """Parse, validate, and dispatch an incoming JSON message from a client."""
        if conn.connection_id not in self._connections:
            self.register_connection(conn)
        self.touch_heartbeat(conn.connection_id)

        try:
            msg = parse_client_message(raw)
        except ProtocolError as exc:
            await self._safe_send(conn, build_error(exc.code, exc.message))
            return

        msg_type = msg["type"]
        if msg_type == MessageType.PING.value:
            await self._safe_send(conn, build_pong(msg["ts"]))
            return
        if msg_type == MessageType.PONG.value:
            return
        if msg_type == MessageType.JOIN_ROOM.value:
            await self.join_room(
                conn=conn,
                room_id=msg["room_id"],
                user_id=msg["user_id"],
                display_name=msg["display_name"],
                color=msg["color"],
            )
            return
        if msg_type == MessageType.LEAVE_ROOM.value:
            await self.leave_room(conn, msg["room_id"], reason="left")
            return
        if msg_type == MessageType.STATE_PATCH.value:
            await self.apply_patch(
                conn=conn,
                room_id=msg["room_id"],
                base_version=msg["base_version"],
                client_seq=msg["client_seq"],
                op_dict=msg["op"],
            )
            return
        if msg_type == MessageType.CURSOR_MOVE.value:
            await self.move_cursor(
                conn=conn,
                room_id=msg["room_id"],
                base_version=msg["base_version"],
                position=msg["position"],
                selection_end=msg["selection_end"],
            )
            return

    async def join_room(
        self,
        *,
        conn: ClientConnection,
        room_id: str,
        user_id: str | None = None,
        display_name: str | None = None,
        color: str | None = None,
    ) -> Participant:
        """Add or move a connection into `room_id`, sending initial sync & presence."""
        if conn.connection_id not in self._connections:
            self.register_connection(conn)
        self.touch_heartbeat(conn.connection_id)

        # If this connection is already in a different room, leave it first
        existing = self._conn_membership.get(conn.connection_id)
        if existing and existing[0] != room_id:
            await self.leave_room(conn, existing[0], reason="left")

        room, lock = self._get_or_create_room(room_id)
        async with lock:
            # Reuse existing user_id if same connection rejoins same room
            if existing and existing[0] == room_id:
                assigned_uid = existing[1]
            else:
                candidate = user_id or f"user-{uuid.uuid4().hex[:8]}"
                if candidate in room.participants and (
                    room.participants[candidate].connection_id != conn.connection_id
                ):
                    assigned_uid = f"{candidate}-{uuid.uuid4().hex[:4]}"
                else:
                    assigned_uid = candidate

            resolved_name = display_name or f"User-{assigned_uid[-4:]}"
            resolved_color = self._pick_color(room, color)
            now_wall = time.time()

            participant = Participant(
                user_id=assigned_uid,
                connection_id=conn.connection_id,
                display_name=resolved_name,
                color=resolved_color,
                cursor=CursorPosition(position=0, selection_end=0),
                joined_at=now_wall,
                last_seen=now_wall,
            )
            room.participants[assigned_uid] = participant
            self._conn_membership[conn.connection_id] = (room_id, assigned_uid)

            participants_snapshot = room.participant_list()
            join_ack = build_join_room_ack(
                room_id=room_id,
                user_id=assigned_uid,
                display_name=resolved_name,
                color=resolved_color,
                version=room.version,
                content=room.content,
                content_hash=room.content_hash,
                participants=participants_snapshot,
            )
            presence_msg = build_presence_update(
                room_id=room_id,
                version=room.version,
                participants=participants_snapshot,
            )
            peer_targets = [
                c
                for c in self._room_connections(room)
                if c.connection_id != conn.connection_id
            ]

        await self._safe_send(conn, join_ack)
        await self._broadcast_many(peer_targets, presence_msg)
        return participant

    async def leave_room(
        self,
        conn: ClientConnection,
        room_id: str | None = None,
        *,
        reason: str = "left",
    ) -> None:
        """Remove a connection from its room and notify remaining participants."""
        membership = self._conn_membership.get(conn.connection_id)
        if not membership:
            return
        current_room_id, user_id = membership
        if room_id is not None and current_room_id != room_id:
            return

        room = self._rooms.get(current_room_id)
        lock = self._room_locks.get(current_room_id)
        if not room or not lock:
            self._conn_membership.pop(conn.connection_id, None)
            return

        async with lock:
            self._conn_membership.pop(conn.connection_id, None)
            removed = room.participants.pop(user_id, None)
            if not removed:
                return
            remaining_targets = self._room_connections(room)
            leave_msg = build_leave_room_event(
                room_id=current_room_id,
                user_id=removed.user_id,
                display_name=removed.display_name,
                reason=reason,
                version=room.version,
            )
            presence_msg = build_presence_update(
                room_id=current_room_id,
                version=room.version,
                participants=room.participant_list(),
            )
            if self.evict_empty_rooms and not room.participants:
                self._rooms.pop(current_room_id, None)
                self._room_locks.pop(current_room_id, None)

        await self._broadcast_many(remaining_targets, leave_msg)
        await self._broadcast_many(remaining_targets, presence_msg)

    async def disconnect(
        self, conn: ClientConnection, *, reason: str = "disconnected"
    ) -> None:
        """Clean up a dropped or closed connection across all rooms."""
        await self.leave_room(conn, reason=reason)
        self._connections.pop(conn.connection_id, None)
        self._last_seen.pop(conn.connection_id, None)

    async def apply_patch(
        self,
        *,
        conn: ClientConnection,
        room_id: str,
        base_version: int,
        client_seq: int,
        op_dict: dict[str, Any],
    ) -> None:
        """Validate version, transform concurrent edits, mutate state, and broadcast."""
        membership = self._conn_membership.get(conn.connection_id)
        if not membership or membership[0] != room_id:
            await self._safe_send(
                conn,
                build_error("NOT_IN_ROOM", f"Connection is not joined to room '{room_id}'."),
            )
            return

        user_id = membership[1]
        room, lock = self._get_or_create_room(room_id)

        async with lock:
            if base_version > room.version or base_version < room.min_history_version:
                resync_ack = build_sync_ack(
                    room_id=room_id,
                    user_id=user_id,
                    client_seq=client_seq,
                    base_version=base_version,
                    version=room.version,
                    status="resync",
                    ops=None,
                    content=room.content,
                    content_hash=room.content_hash,
                )
                await self._safe_send(conn, resync_ack)
                return

            incoming_op = SpliceOp.from_protocol(
                op_dict, user_id=user_id, default_ts=time.time()
            )
            concurrent_ops = room.ops_since(base_version)
            if not concurrent_ops:
                transformed_ops = [] if incoming_op.is_noop() else [incoming_op]
                status = "applied"
            else:
                transformed_ops = transform_against_history(incoming_op, concurrent_ops)
                status = "transformed"

            if transformed_ops:
                room.content = apply_ops(room.content, transformed_ops)

            room.version += 1
            room.updated_at = time.time()
            room.record_revision(
                RevisionRecord(
                    version=room.version,
                    user_id=user_id,
                    client_seq=client_seq,
                    ops=tuple(transformed_ops),
                    timestamp=room.updated_at,
                )
            )

            # Rebase other participants' stored cursor coordinates to the new version
            if transformed_ops:
                doc_len = len(room.content)
                for pid, participant in room.participants.items():
                    if pid == user_id:
                        continue
                    participant.cursor.position = min(
                        doc_len,
                        transform_cursor(participant.cursor.position, transformed_ops),
                    )
                    participant.cursor.selection_end = min(
                        doc_len,
                        transform_cursor(participant.cursor.selection_end, transformed_ops),
                    )

            serialized_ops = [op.to_protocol() for op in transformed_ops]
            new_version = room.version
            new_hash = room.content_hash

            ack_msg = build_sync_ack(
                room_id=room_id,
                user_id=user_id,
                client_seq=client_seq,
                base_version=base_version,
                version=new_version,
                status=status,
                ops=serialized_ops,
                content=None,
                content_hash=new_hash,
            )
            broadcast_msg = build_state_patch_broadcast(
                room_id=room_id,
                user_id=user_id,
                client_seq=client_seq,
                base_version=base_version,
                version=new_version,
                ops=serialized_ops,
                content_hash=new_hash,
            )
            peer_targets = [
                c
                for c in self._room_connections(room)
                if c.connection_id != conn.connection_id
            ]

        await self._safe_send(conn, ack_msg)
        await self._broadcast_many(peer_targets, broadcast_msg)

    async def move_cursor(
        self,
        *,
        conn: ClientConnection,
        room_id: str,
        base_version: int,
        position: int,
        selection_end: int,
    ) -> None:
        """Update and broadcast a participant's cursor position, rebased to current version."""
        membership = self._conn_membership.get(conn.connection_id)
        if not membership or membership[0] != room_id:
            await self._safe_send(
                conn,
                build_error("NOT_IN_ROOM", f"Connection is not joined to room '{room_id}'."),
            )
            return

        user_id = membership[1]
        room = self._rooms.get(room_id)
        lock = self._room_locks.get(room_id)
        if not room or not lock:
            return

        async with lock:
            participant = room.participants.get(user_id)
            if not participant:
                return

            doc_len = len(room.content)
            rebased_pos = position
            rebased_end = selection_end
            if 0 <= base_version < room.version and base_version >= room.min_history_version:
                concurrent_ops = room.ops_since(base_version)
                rebased_pos = transform_cursor(position, concurrent_ops)
                rebased_end = transform_cursor(selection_end, concurrent_ops)

            rebased_pos = max(0, min(rebased_pos, doc_len))
            rebased_end = max(0, min(rebased_end, doc_len))

            participant.cursor = CursorPosition(
                position=rebased_pos, selection_end=rebased_end
            )
            participant.last_seen = time.time()

            cursor_msg = build_cursor_move_broadcast(
                room_id=room_id,
                user_id=user_id,
                version=room.version,
                position=rebased_pos,
                selection_end=rebased_end,
            )
            peer_targets = [
                c
                for c in self._room_connections(room)
                if c.connection_id != conn.connection_id
            ]

        await self._broadcast_many(peer_targets, cursor_msg)

    async def reap_stale_connections(self, now_monotonic: float | None = None) -> list[str]:
        """Ping active clients and disconnect sockets exceeding `heartbeat_timeout`."""
        now = time.monotonic() if now_monotonic is None else now_monotonic
        stale_ids: list[str] = []
        active_conns: list[ClientConnection] = []

        for cid, conn in list(self._connections.items()):
            last_seen = self._last_seen.get(cid, now)
            if now - last_seen > self.heartbeat_timeout:
                stale_ids.append(cid)
            else:
                active_conns.append(conn)

        for cid in stale_ids:
            conn = self._connections.get(cid)
            if conn is not None:
                await self.disconnect(conn, reason="timeout")
                with contextlib.suppress(Exception):
                    await conn.close(code=1001, reason="Heartbeat timeout")

        for conn in active_conns:
            with contextlib.suppress(Exception):
                await conn.ping()

        return stale_ids

    def start_heartbeat(self) -> None:
        """Start the background heartbeat/reaper task if not already running."""
        if self._heartbeat_task is None or self._heartbeat_task.done():
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def stop_heartbeat(self) -> None:
        if self._heartbeat_task is not None and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._heartbeat_task
        self._heartbeat_task = None

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self.heartbeat_interval)
            await self.reap_stale_connections()

    def _room_connections(self, room: RoomState) -> list[ClientConnection]:
        conns: list[ClientConnection] = []
        for participant in room.participants.values():
            conn = self._connections.get(participant.connection_id)
            if conn is not None:
                conns.append(conn)
        return conns

    async def _safe_send(self, conn: ClientConnection, payload: dict[str, Any]) -> None:
        try:
            await conn.send_json(payload)
        except Exception:
            await self.disconnect(conn, reason="disconnected")

    async def _broadcast_many(
        self, targets: list[ClientConnection], payload: dict[str, Any]
    ) -> None:
        if not targets:
            return
        results = await asyncio.gather(
            *(conn.send_json(payload) for conn in targets),
            return_exceptions=True,
        )
        for conn, result in zip(targets, results):
            if isinstance(result, Exception):
                await self.disconnect(conn, reason="disconnected")
