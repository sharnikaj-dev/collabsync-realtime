"""CollabSync Phase-1 server entrypoint."""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
import signal
import sys

from server.rooms.manager import RoomManager
from server.transport.asyncio_ws import AsyncioWebSocketServer


def _has_module(name: str) -> bool:
    return importlib.util.find_spec(name) is not None


async def run_asyncio_server(host: str, port: int, heartbeat_interval: float, heartbeat_timeout: float) -> None:
    manager = RoomManager(
        heartbeat_interval=heartbeat_interval,
        heartbeat_timeout=heartbeat_timeout,
    )
    server = AsyncioWebSocketServer(manager=manager, host=host, port=port)
    bound_port = await server.start()
    print(f"CollabSync WebSocket server listening on ws://{host}:{bound_port}/ws (engine=asyncio)", flush=True)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    try:
        await stop_event.wait()
    finally:
        await server.stop()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="CollabSync Phase-1 Real-Time Server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument(
        "--engine",
        choices=("auto", "asyncio", "websockets", "fastapi"),
        default="auto",
    )
    parser.add_argument("--heartbeat-interval", type=float, default=10.0)
    parser.add_argument("--heartbeat-timeout", type=float, default=30.0)
    args = parser.parse_args(argv)

    engine = args.engine
    if engine == "auto":
        if _has_module("fastapi") and _has_module("uvicorn"):
            engine = "fastapi"
        elif _has_module("websockets"):
            engine = "websockets"
        else:
            engine = "asyncio"

    if engine == "fastapi":
        import uvicorn  # type: ignore[import-not-found]
        from server.transport.fastapi_ws import create_fastapi_app

        manager = RoomManager(
            heartbeat_interval=args.heartbeat_interval,
            heartbeat_timeout=args.heartbeat_timeout,
        )
        app = create_fastapi_app(manager)
        uvicorn.run(app, host=args.host, port=args.port, log_level="info")
        return 0

    if engine == "websockets":
        from server.transport.websockets_ws import run_websockets_server

        manager = RoomManager(
            heartbeat_interval=args.heartbeat_interval,
            heartbeat_timeout=args.heartbeat_timeout,
        )
        asyncio.run(run_websockets_server(manager, host=args.host, port=args.port))
        return 0

    try:
        asyncio.run(
            run_asyncio_server(
                args.host,
                args.port,
                args.heartbeat_interval,
                args.heartbeat_timeout,
            )
        )
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
