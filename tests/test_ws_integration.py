"""End-to-end WebSocket integration tests using the real asyncio RFC 6455 server."""

import asyncio
import base64
import json
import os
import struct
import unittest
from typing import Any

from server.rooms.manager import RoomManager
from server.transport.asyncio_ws import AsyncioWebSocketServer


class MinimalWSClient:
    """Lightweight RFC 6455 WebSocket test client over asyncio streams."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.reader = reader
        self.writer = writer

    @classmethod
    async def connect(cls, host: str, port: int, path: str = "/ws") -> "MinimalWSClient":
        reader, writer = await asyncio.open_connection(host, port)
        key = base64.b64encode(os.urandom(16)).decode("ascii")
        req = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {host}:{port}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n"
            "\r\n"
        ).encode("ascii")
        writer.write(req)
        await writer.drain()
        head = await reader.readuntil(b"\r\n\r\n")
        if b"101 Switching Protocols" not in head:
            raise AssertionError(f"Unexpected handshake response: {head!r}")
        return cls(reader, writer)

    async def send_json(self, payload: dict[str, Any]) -> None:
        data = json.dumps(payload).encode("utf-8")
        mask = os.urandom(4)
        header = bytearray([0x81])
        n = len(data)
        if n < 126:
            header.append(0x80 | n)
        elif n <= 0xFFFF:
            header.append(0x80 | 126)
            header.extend(struct.pack("!H", n))
        else:
            header.append(0x80 | 127)
            header.extend(struct.pack("!Q", n))
        header.extend(mask)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
        self.writer.write(bytes(header) + masked)
        await self.writer.drain()

    async def recv_json(self, timeout: float = 2.0) -> dict[str, Any]:
        while True:
            b1, b2 = await asyncio.wait_for(self.reader.readexactly(2), timeout=timeout)
            opcode = b1 & 0x0F
            length = b2 & 0x7F
            if length == 126:
                (length,) = struct.unpack("!H", await self.reader.readexactly(2))
            elif length == 127:
                (length,) = struct.unpack("!Q", await self.reader.readexactly(8))
            payload = await self.reader.readexactly(length) if length else b""
            if opcode == 0x9:  # ping -> reply pong
                mask = os.urandom(4)
                masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
                self.writer.write(bytes([0x8A, 0x80 | len(payload)]) + mask + masked)
                await self.writer.drain()
                continue
            if opcode == 0x1:
                return json.loads(payload.decode("utf-8"))
            if opcode == 0x8:
                raise EOFError("Server closed socket")

    async def recv_until(self, msg_type: str, timeout: float = 2.0) -> dict[str, Any]:
        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            remaining = deadline - asyncio.get_running_loop().time()
            msg = await self.recv_json(timeout=max(0.05, remaining))
            if msg.get("type") == msg_type:
                return msg

    async def close(self) -> None:
        self.writer.close()
        await self.writer.wait_closed()


class TestWebSocketIntegration(unittest.IsolatedAsyncioTestCase):
    async def test_two_clients_sync_and_disconnect_cleanup(self) -> None:
        manager = RoomManager(heartbeat_interval=5.0, heartbeat_timeout=15.0)
        server = AsyncioWebSocketServer(manager=manager, host="127.0.0.1", port=0)
        port = await server.start()

        c1 = await MinimalWSClient.connect("127.0.0.1", port)
        c2 = await MinimalWSClient.connect("127.0.0.1", port)
        try:
            await c1.send_json({"type": "JOIN_ROOM", "room_id": "shared-pad", "display_name": "Tab 1"})
            join1 = await c1.recv_until("JOIN_ROOM")
            self.assertEqual(join1["version"], 0)

            await c2.send_json({"type": "JOIN_ROOM", "room_id": "shared-pad", "display_name": "Tab 2"})
            join2 = await c2.recv_until("JOIN_ROOM")
            self.assertEqual(len(join2["participants"]), 2)

            presence1 = await c1.recv_until("PRESENCE_UPDATE")
            self.assertEqual(len(presence1["participants"]), 2)

            # Tab 1 inserts text
            await c1.send_json(
                {
                    "type": "STATE_PATCH",
                    "room_id": "shared-pad",
                    "base_version": 0,
                    "client_seq": 1,
                    "op": {"kind": "insert", "position": 0, "text": "Sync works!"},
                }
            )
            ack1 = await c1.recv_until("SYNC_ACK")
            self.assertEqual(ack1["version"], 1)

            patch2 = await c2.recv_until("STATE_PATCH")
            self.assertEqual(patch2["version"], 1)
            self.assertEqual(patch2["op"]["text"], "Sync works!")

            # Tab 2 closes socket abruptly -> Tab 1 sees LEAVE_ROOM & updated PRESENCE_UPDATE
            await c2.close()
            leave1 = await c1.recv_until("LEAVE_ROOM")
            self.assertEqual(leave1["reason"], "disconnected")
            presence_after = await c1.recv_until("PRESENCE_UPDATE")
            self.assertEqual(len(presence_after["participants"]), 1)
        finally:
            await c1.close()
            await server.stop()


if __name__ == "__main__":
    unittest.main()
