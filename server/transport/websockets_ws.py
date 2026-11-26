"""Optional `websockets` library adapter for CollabSync RoomManager."""

from __future__ import annotations

import contextlib
import json
import uuid
from typing import Any

from server.rooms.manager import RoomManager


class WebsocketsLibConnection:
    """Adapts a `websockets` ServerConnection to ClientConnection."""

    def __init__(self, ws: Any, connection_id: str | None = None) -> None:
        self.connection_id = connection_id or f"wsl-{uuid.uuid4().hex[:12]}"
        self._ws = ws

    async def send_json(self, payload: dict[str, Any]) -> None:
        await self._ws.send(json.dumps(payload, separators=(",", ":")))

    async def ping(self) -> None:
        await self._ws.ping()

    async def close(self, code: int = 1000, reason: str = "") -> None:
        with contextlib.suppress(Exception):
            await self._ws.close(code=code, reason=reason)


async def run_websockets_server(
    manager: RoomManager, host: str = "127.0.0.1", port: int = 8765
) -> None:
    import websockets  # type: ignore[import-not-found]

    async def handler(ws: Any) -> None:
        conn = WebsocketsLibConnection(ws)
        manager.register_connection(conn)
        try:
            async for message in ws:
                await manager.handle_raw_message(conn, message)
        finally:
            await manager.disconnect(conn, reason="disconnected")

    manager.start_heartbeat()
    try:
        async with websockets.serve(handler, host, port):
            await __import__("asyncio").Future()
    finally:
        await manager.stop_heartbeat()
