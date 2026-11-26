"""Optional FastAPI WebSocket adapter for CollabSync RoomManager."""

from __future__ import annotations

import contextlib
import json
import uuid
from pathlib import Path
from typing import Any

from server.rooms.manager import RoomManager


class FastAPIWSConnection:
    """Adapts a FastAPI/Starlette WebSocket to ClientConnection."""

    def __init__(self, ws: Any, connection_id: str | None = None) -> None:
        self.connection_id = connection_id or f"fapi-{uuid.uuid4().hex[:12]}"
        self._ws = ws

    async def send_json(self, payload: dict[str, Any]) -> None:
        await self._ws.send_text(json.dumps(payload, separators=(",", ":")))

    async def ping(self) -> None:
        # FastAPI/Starlette uses application-level PING frame via JSON
        await self.send_json({"type": "PING"})

    async def close(self, code: int = 1000, reason: str = "") -> None:
        with contextlib.suppress(Exception):
            await self._ws.close(code=code, reason=reason)


def create_fastapi_app(manager: RoomManager | None = None) -> Any:
    """Create a FastAPI application serving `/ws`, `/health`, and static `client/`."""
    from fastapi import FastAPI, WebSocket, WebSocketDisconnect  # type: ignore[import-not-found]
    from fastapi.staticfiles import StaticFiles  # type: ignore[import-not-found]

    room_manager = manager or RoomManager()
    app = FastAPI(title="CollabSync Phase 1")

    @app.on_event("startup")
    async def _startup() -> None:
        room_manager.start_heartbeat()

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        await room_manager.stop_heartbeat()

    @app.get("/health")
    async def health() -> dict[str, Any]:
        return {"status": "ok", "rooms": len(room_manager.list_room_ids())}

    @app.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        conn = FastAPIWSConnection(websocket)
        room_manager.register_connection(conn)
        try:
            while True:
                data = await websocket.receive_text()
                await room_manager.handle_raw_message(conn, data)
        except WebSocketDisconnect:
            pass
        finally:
            await room_manager.disconnect(conn, reason="disconnected")

    client_dir = Path(__file__).resolve().parents[2] / "client"
    if client_dir.exists():
        app.mount("/", StaticFiles(directory=str(client_dir), html=True), name="client")

    return app
