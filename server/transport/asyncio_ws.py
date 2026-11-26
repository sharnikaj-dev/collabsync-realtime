"""Pure Python stdlib asyncio RFC 6455 WebSocket server transport."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import json
import struct
import uuid
from typing import Any

from server.rooms.manager import RoomManager

WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
MAX_FRAME_SIZE = 4 * 1024 * 1024  # 4 MiB

OP_CONTINUATION = 0x0
OP_TEXT = 0x1
OP_BINARY = 0x2
OP_CLOSE = 0x8
OP_PING = 0x9
OP_PONG = 0xA


class WebSocketClosed(Exception):
    """Raised when the remote peer closes the WebSocket connection."""


class AsyncioWSConnection:
    """RFC 6455 WebSocket connection implementing ClientConnection."""

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        connection_id: str | None = None,
    ) -> None:
        self.connection_id = connection_id or f"ws-{uuid.uuid4().hex[:12]}"
        self._reader = reader
        self._writer = writer
        self._write_lock = asyncio.Lock()
        self._closed = False

    async def send_json(self, payload: dict[str, Any]) -> None:
        encoded = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        await self._send_frame(OP_TEXT, encoded)

    async def ping(self) -> None:
        await self._send_frame(OP_PING, b"hb")

    async def pong(self, payload: bytes = b"") -> None:
        await self._send_frame(OP_PONG, payload[:125])

    async def close(self, code: int = 1000, reason: str = "") -> None:
        if self._closed:
            return
        self._closed = True
        with contextlib.suppress(Exception):
            body = struct.pack("!H", code) + reason.encode("utf-8")[:123]
            await self._send_frame(OP_CLOSE, body, ignore_closed=True)
        with contextlib.suppress(Exception):
            self._writer.close()
            await self._writer.wait_closed()

    async def _send_frame(
        self, opcode: int, payload: bytes, *, ignore_closed: bool = False
    ) -> None:
        if self._closed and not ignore_closed:
            raise WebSocketClosed("Connection is closed.")
        length = len(payload)
        header = bytearray()
        header.append(0x80 | opcode)
        if length < 126:
            header.append(length)
        elif length <= 0xFFFF:
            header.append(126)
            header.extend(struct.pack("!H", length))
        else:
            header.append(127)
            header.extend(struct.pack("!Q", length))

        async with self._write_lock:
            if self._writer.is_closing():
                raise WebSocketClosed("Writer is closing.")
            self._writer.write(bytes(header) + payload)
            await self._writer.drain()

    async def read_message(self) -> tuple[int, bytes]:
        """Read a complete WebSocket message, handling fragmented frames and control frames."""
        fragments: list[bytes] = []
        message_opcode: int | None = None

        while True:
            fin, opcode, payload = await self._read_frame()
            if opcode == OP_CLOSE:
                raise WebSocketClosed("Peer sent close frame.")
            if opcode == OP_PING:
                await self.pong(payload)
                return OP_PING, payload
            if opcode == OP_PONG:
                return OP_PONG, payload

            if opcode in (OP_TEXT, OP_BINARY):
                if message_opcode is not None:
                    raise WebSocketClosed("Unexpected data frame during fragmented message.")
                message_opcode = opcode
                fragments.append(payload)
            elif opcode == OP_CONTINUATION:
                if message_opcode is None:
                    raise WebSocketClosed("Unexpected continuation frame.")
                fragments.append(payload)
            else:
                raise WebSocketClosed(f"Unsupported opcode {opcode}")

            if fin:
                assert message_opcode is not None
                return message_opcode, b"".join(fragments)

    async def _read_frame(self) -> tuple[bool, int, bytes]:
        try:
            b1, b2 = await self._reader.readexactly(2)
        except (asyncio.IncompleteReadError, ConnectionError) as exc:
            raise WebSocketClosed("EOF while reading frame header.") from exc

        fin = bool(b1 & 0x80)
        opcode = b1 & 0x0F
        masked = bool(b2 & 0x80)
        length = b2 & 0x7F

        if length == 126:
            raw_len = await self._reader.readexactly(2)
            (length,) = struct.unpack("!H", raw_len)
        elif length == 127:
            raw_len = await self._reader.readexactly(8)
            (length,) = struct.unpack("!Q", raw_len)

        if length > MAX_FRAME_SIZE:
            raise WebSocketClosed("Frame exceeds maximum allowed size.")

        mask_key = b""
        if masked:
            mask_key = await self._reader.readexactly(4)

        payload = await self._reader.readexactly(length) if length else b""
        if masked and payload:
            # Fast XOR unmasking using 4-byte slice stepping
            unmasked = bytearray(payload)
            for i in range(4):
                unmasked[i::4] = bytes(b ^ mask_key[i] for b in unmasked[i::4])
            payload = bytes(unmasked)

        return fin, opcode, payload


class AsyncioWebSocketServer:
    """Zero-dependency asyncio HTTP/WebSocket server backed by RoomManager."""

    def __init__(
        self,
        manager: RoomManager,
        host: str = "127.0.0.1",
        port: int = 8765,
    ) -> None:
        self.manager = manager
        self.host = host
        self.port = port
        self._server: asyncio.Server | None = None

    async def start(self) -> int:
        self.manager.start_heartbeat()
        self._server = await asyncio.start_server(
            self._handle_client, self.host, self.port
        )
        sockets = self._server.sockets
        if sockets:
            self.port = sockets[0].getsockname()[1]
        return self.port

    async def serve_forever(self) -> None:
        if self._server is None:
            await self.start()
        assert self._server is not None
        async with self._server:
            await self._server.serve_forever()

    async def stop(self) -> None:
        await self.manager.stop_heartbeat()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            raw_head = await asyncio.wait_for(
                reader.readuntil(b"\r\n\r\n"), timeout=10.0
            )
        except Exception:
            writer.close()
            return

        lines = raw_head.decode("latin-1").split("\r\n")
        request_line = lines[0] if lines else ""
        parts = request_line.split(" ")
        path = parts[1].split("?")[0] if len(parts) >= 2 else "/"

        headers: dict[str, str] = {}
        for line in lines[1:]:
            if ":" in line:
                k, v = line.split(":", 1)
                headers[k.strip().lower()] = v.strip()

        upgrade = headers.get("upgrade", "").lower()
        ws_key = headers.get("sec-websocket-key")

        if upgrade != "websocket" or not ws_key:
            await self._handle_http(path, writer)
            return

        accept_digest = hashlib.sha1((ws_key + WS_GUID).encode("ascii")).digest()
        accept_key = base64.b64encode(accept_digest).decode("ascii")
        response = (
            "HTTP/1.1 101 Switching Protocols\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Accept: {accept_key}\r\n"
            "\r\n"
        ).encode("ascii")
        writer.write(response)
        await writer.drain()

        conn = AsyncioWSConnection(reader, writer)
        self.manager.register_connection(conn)
        try:
            while True:
                opcode, payload = await conn.read_message()
                if opcode in (OP_PING, OP_PONG):
                    self.manager.touch_heartbeat(conn.connection_id)
                    continue
                if opcode == OP_TEXT:
                    await self.manager.handle_raw_message(conn, payload.decode("utf-8"))
        except (WebSocketClosed, ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            await self.manager.disconnect(conn, reason="disconnected")
            await conn.close()

    async def _handle_http(
        self, path: str, writer: asyncio.StreamWriter
    ) -> None:
        if path == "/health":
            body = json.dumps(
                {"status": "ok", "rooms": len(self.manager.list_room_ids())}
            ).encode("utf-8")
            resp = (
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Type: application/json\r\n"
                + f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode("ascii")
                + body
            )
        else:
            body = b"CollabSync WebSocket endpoint active at /ws"
            resp = (
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Type: text/plain\r\n"
                + f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode("ascii")
                + body
            )
        with contextlib.suppress(Exception):
            writer.write(resp)
            await writer.drain()
            writer.close()
            await writer.wait_closed()
