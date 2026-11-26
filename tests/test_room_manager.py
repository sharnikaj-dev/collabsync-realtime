"""RoomManager unit tests covering isolation, concurrency, resync, and heartbeat cleanup."""

import asyncio
import json
import time
import unittest
from typing import Any

from server.rooms.manager import RoomManager


class MemoryConnection:
    def __init__(self, connection_id: str) -> None:
        self.connection_id = connection_id
        self.sent: list[dict[str, Any]] = []
        self.closed = False
        self.ping_count = 0

    async def send_json(self, payload: dict[str, Any]) -> None:
        if self.closed:
            raise ConnectionResetError("closed")
        self.sent.append(payload)

    async def ping(self) -> None:
        if self.closed:
            raise ConnectionResetError("closed")
        self.ping_count += 1

    async def close(self, code: int = 1000, reason: str = "") -> None:
        self.closed = True

    def messages_of_type(self, msg_type: str) -> list[dict[str, Any]]:
        return [m for m in self.sent if m.get("type") == msg_type]


class TestRoomManager(unittest.IsolatedAsyncioTestCase):
    async def test_room_isolation(self) -> None:
        mgr = RoomManager()
        c_alpha = MemoryConnection("c1")
        c_beta = MemoryConnection("c2")

        await mgr.join_room(conn=c_alpha, room_id="room-alpha", display_name="Alice")
        await mgr.join_room(conn=c_beta, room_id="room-beta", display_name="Bob")

        await mgr.handle_raw_message(
            c_alpha,
            json.dumps(
                {
                    "type": "STATE_PATCH",
                    "room_id": "room-alpha",
                    "base_version": 0,
                    "client_seq": 1,
                    "op": {"kind": "insert", "position": 0, "text": "Alpha secret"},
                }
            ),
        )

        self.assertEqual(mgr.get_room("room-alpha").content, "Alpha secret")
        self.assertEqual(mgr.get_room("room-beta").content, "")
        self.assertEqual(len(c_beta.messages_of_type("STATE_PATCH")), 0)

    async def test_concurrent_edits_transform_and_increment_version_monotonically(self) -> None:
        mgr = RoomManager(default_content="world")
        c1 = MemoryConnection("c1")
        c2 = MemoryConnection("c2")

        p1 = await mgr.join_room(conn=c1, room_id="doc-1", user_id="u1", display_name="User 1")
        p2 = await mgr.join_room(conn=c2, room_id="doc-1", user_id="u2", display_name="User 2")
        self.assertEqual(p1.user_id, "u1")
        self.assertEqual(p2.user_id, "u2")

        # Both clients start at base_version=0 ("world")
        await mgr.handle_raw_message(
            c1,
            json.dumps(
                {
                    "type": "STATE_PATCH",
                    "room_id": "doc-1",
                    "base_version": 0,
                    "client_seq": 1,
                    "op": {"kind": "insert", "position": 0, "text": "Hello ", "timestamp": 1.0},
                }
            ),
        )
        # Client 2 didn't see version 1 yet, still sends base_version=0 appending "!" at index 5
        await mgr.handle_raw_message(
            c2,
            json.dumps(
                {
                    "type": "STATE_PATCH",
                    "room_id": "doc-1",
                    "base_version": 0,
                    "client_seq": 1,
                    "op": {"kind": "insert", "position": 5, "text": "!", "timestamp": 2.0},
                }
            ),
        )

        room = mgr.get_room("doc-1")
        self.assertEqual(room.version, 2)
        self.assertEqual(room.content, "Hello world!")

        ack_c2 = c2.messages_of_type("SYNC_ACK")[-1]
        self.assertEqual(ack_c2["status"], "transformed")
        self.assertEqual(ack_c2["version"], 2)
        self.assertEqual(ack_c2["op"]["position"], 11)

    async def test_out_of_sync_client_receives_resync_payload(self) -> None:
        mgr = RoomManager(max_history=3)
        c1 = MemoryConnection("c1")
        c2 = MemoryConnection("c2")

        await mgr.join_room(conn=c1, room_id="doc-2", display_name="Fast")
        await mgr.join_room(conn=c2, room_id="doc-2", display_name="Slow")

        # Advance 5 revisions so base_version=0 falls out of history window (max_history=3)
        for idx in range(5):
            await mgr.handle_raw_message(
                c1,
                json.dumps(
                    {
                        "type": "STATE_PATCH",
                        "room_id": "doc-2",
                        "base_version": idx,
                        "client_seq": idx + 1,
                        "op": {"kind": "insert", "position": idx, "text": str(idx)},
                    }
                ),
            )

        # Slow client sends patch with stale base_version=0
        await mgr.handle_raw_message(
            c2,
            json.dumps(
                {
                    "type": "STATE_PATCH",
                    "room_id": "doc-2",
                    "base_version": 0,
                    "client_seq": 1,
                    "op": {"kind": "insert", "position": 0, "text": "X"},
                }
            ),
        )

        ack = c2.messages_of_type("SYNC_ACK")[-1]
        self.assertEqual(ack["status"], "resync")
        self.assertEqual(ack["version"], 5)
        self.assertEqual(ack["content"], "01234")

    async def test_cursor_move_rebased_on_stale_version(self) -> None:
        mgr = RoomManager(default_content="hello")
        c1 = MemoryConnection("c1")
        c2 = MemoryConnection("c2")

        await mgr.join_room(conn=c1, room_id="doc-3", display_name="Editor")
        await mgr.join_room(conn=c2, room_id="doc-3", display_name="Reader")

        # c1 inserts ">> " at pos 0 -> version=1, content=">> hello"
        await mgr.handle_raw_message(
            c1,
            json.dumps(
                {
                    "type": "STATE_PATCH",
                    "room_id": "doc-3",
                    "base_version": 0,
                    "client_seq": 1,
                    "op": {"kind": "insert", "position": 0, "text": ">> "},
                }
            ),
        )

        # c2 moves cursor to pos 5 based on version 0 -> should rebase to 8
        await mgr.handle_raw_message(
            c2,
            json.dumps(
                {
                    "type": "CURSOR_MOVE",
                    "room_id": "doc-3",
                    "base_version": 0,
                    "position": 5,
                    "selection_end": 5,
                }
            ),
        )

        cursor_event = c1.messages_of_type("CURSOR_MOVE")[-1]
        self.assertEqual(cursor_event["version"], 1)
        self.assertEqual(cursor_event["position"], 8)

    async def test_heartbeat_reaps_stale_connections_and_broadcasts_leave(self) -> None:
        mgr = RoomManager(heartbeat_interval=0.05, heartbeat_timeout=0.1)
        c1 = MemoryConnection("c1")
        c2 = MemoryConnection("c2")

        await mgr.join_room(conn=c1, room_id="doc-hb", display_name="Active")
        p2 = await mgr.join_room(conn=c2, room_id="doc-hb", display_name="Silent")

        # Simulate c2 stopping heartbeats while c1 remains active
        future_mono = time.monotonic() + 0.25
        mgr._last_seen[c1.connection_id] = future_mono - 0.01

        reaped = await mgr.reap_stale_connections(now_monotonic=future_mono)
        self.assertEqual(reaped, ["c2"])
        self.assertTrue(c2.closed)
        self.assertNotIn(p2.user_id, mgr.get_room("doc-hb").participants)

        leave_events = c1.messages_of_type("LEAVE_ROOM")
        self.assertEqual(len(leave_events), 1)
        self.assertEqual(leave_events[0]["reason"], "timeout")
        self.assertEqual(leave_events[0]["user_id"], p2.user_id)


if __name__ == "__main__":
    unittest.main()
