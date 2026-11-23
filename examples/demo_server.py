#!/usr/bin/env python3
"""A tiny WebSocket server that streams sample captions, for trying out the scanner.

python examples/demo_server.py            # serves ws://127.0.0.1:6677
python ws_listener.py --gui               # then press Find, double-click, Connect
"""

from __future__ import annotations

import argparse
import asyncio
import itertools
import json

from websockets.asyncio.server import ServerConnection, serve

CAPTIONS = (
    "Welcome back. Let's pick up where we left off.",
    "The listener connects, then reads every frame the server sends.",
    "JSON messages are searched for keys like text, transcript and caption.",
    "Plain-text frames are printed exactly as they arrive.",
    "Use the filter box to keep only the lines you care about.",
    "Everything is also appended to a JSON Lines log for later.",
)


async def stream(ws: ServerConnection, interval: float) -> None:
    for index in itertools.count(1):
        caption = CAPTIONS[(index - 1) % len(CAPTIONS)]
        if index % 3 == 0:
            await ws.send(caption)
        else:
            await ws.send(json.dumps({"type": "caption", "id": index, "data": {"text": caption}}))
        await asyncio.sleep(interval)


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6677)
    parser.add_argument("--interval", type=float, default=1.5, help="seconds between messages")
    args = parser.parse_args()

    async with serve(lambda ws: stream(ws, args.interval), args.host, args.port) as server:
        print(f"Demo server on ws://{args.host}:{args.port} (Ctrl+C to stop)")
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
