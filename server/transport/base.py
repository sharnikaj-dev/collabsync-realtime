"""Transport-agnostic connection interface for CollabSync."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class ClientConnection(Protocol):
    """Minimal transport contract consumed by RoomManager."""

    connection_id: str

    async def send_json(self, payload: dict[str, Any]) -> None:
        """Serialize and transmit a JSON protocol message to the client."""
        ...

    async def ping(self) -> None:
        """Send a transport or protocol heartbeat probe."""
        ...

    async def close(self, code: int = 1000, reason: str = "") -> None:
        """Close the underlying client connection gracefully."""
        ...
