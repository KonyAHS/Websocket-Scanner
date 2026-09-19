#!/usr/bin/env python3
"""Listen to a WebSocket and print or log useful text from its messages.

Install the optional dependency first:

    python -m pip install -r requirements-ws-listener.txt

Run ``python ws_listener.py --help`` for all options.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import csv
import io
import json
import os
import re
import signal
import socket
import ssl
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Empty, Queue
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence


DEFAULT_URL = "ws://127.0.0.1:6677"
DEFAULT_TEXT_KEYS = (
    "text",
    "transcript",
    "content",
    "caption",
    "message",
    "utterance",
    "delta",
    "partial",
)
DISCOVERY_PATHS = ("/", "/ws", "/websocket", "/socket.io/?EIO=4&transport=websocket")


@dataclass(frozen=True)
class SocketCandidate:
    kind: str
    local: str
    remote: str
    state: str
    pid: int | None
    process: str
    url: str = ""

    @property
    def search_text(self) -> str:
        return " ".join(
            (self.kind, self.local, self.remote, self.state, self.process, str(self.pid or ""), self.url)
        ).casefold()


def split_endpoint(value: str) -> tuple[str, int | None]:
    """Split netstat's IPv4 or bracketed IPv6 endpoint format."""
    value = value.strip()
    if value in {"*:*", "*"}:
        return "*", None
    if value.startswith("[") and "]:" in value:
        host, _, port = value[1:].rpartition("]:")
    else:
        host, separator, port = value.rpartition(":")
        if not separator:
            return value, None
    try:
        return host, int(port)
    except ValueError:
        return host, None


def display_endpoint(host: str, port: int | None) -> str:
    if port is None:
        return host
    return f"[{host}]:{port}" if ":" in host else f"{host}:{port}"


def parse_netstat_connections(
    output: str, process_names: dict[int, str] | None = None
) -> list[SocketCandidate]:
    """Parse Windows ``netstat -ano -p tcp`` output."""
    names = process_names or {}
    found: list[SocketCandidate] = []
    for line in output.splitlines():
        parts = line.split()
        if len(parts) < 5 or parts[0].upper() not in {"TCP", "TCPV6"}:
            continue
        local, remote, state, raw_pid = parts[1], parts[2], parts[3].upper(), parts[4]
        try:
            pid = int(raw_pid)
        except ValueError:
            pid = None
        found.append(
            SocketCandidate(
                kind="Listening" if state == "LISTENING" else "Active",
                local=local,
                remote="" if remote == "0.0.0.0:0" else remote,
                state=state.title(),
                pid=pid,
                process=names.get(pid or -1, ""),
            )
        )
    return found


def windows_process_names() -> dict[int, str]:
    if not sys.platform.startswith("win"):
        return {}
    creation_flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    try:
        completed = subprocess.run(
            ["tasklist", "/fo", "csv", "/nh"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=8,
            creationflags=creation_flags,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return {}
    names: dict[int, str] = {}
    for row in csv.reader(io.StringIO(completed.stdout)):
        if len(row) < 2:
            continue
        try:
            names[int(row[1])] = row[0]
        except ValueError:
            continue
    if names:
        return names

    # Some managed Windows setups deny tasklist while Get-Process remains available.
    try:
        completed = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "Get-Process | Select-Object Id,ProcessName | ConvertTo-Csv -NoTypeInformation",
            ],
            capture_output=True,
            text=True,
            encoding="utf-8-sig",
            errors="replace",
            timeout=10,
            creationflags=creation_flags,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return names
    for row in csv.DictReader(io.StringIO(completed.stdout)):
        try:
            names[int(row["Id"])] = row["ProcessName"]
        except (KeyError, TypeError, ValueError):
            continue
    return names


def list_tcp_connections() -> list[SocketCandidate]:
    """Return local listeners and established TCP connections without admin rights."""
    if not sys.platform.startswith("win"):
        raise RuntimeError("Socket discovery is currently available on Windows.")
    creation_flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    try:
        completed = subprocess.run(
            ["netstat", "-ano", "-p", "tcp"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=10,
            creationflags=creation_flags,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeError(f"Could not read active connections: {exc}") from exc
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or "Could not read active connections.")
    connections = parse_netstat_connections(completed.stdout, windows_process_names())
    return [item for item in connections if item.state in {"Listening", "Established"}]


def probe_websocket(host: str, port: int, timeout: float = 0.35) -> str | None:
    """Return a working ws/wss URL when a local endpoint accepts an upgrade."""
    connect_host = "127.0.0.1" if host in {"0.0.0.0", "*"} else host
    if connect_host in {"::", "[::]"}:
        connect_host = "::1"
    display_host = f"[{connect_host}]" if ":" in connect_host else connect_host
    key = base64.b64encode(os.urandom(16)).decode("ascii")
    for secure in (False, True):
        for path in DISCOVERY_PATHS:
            try:
                raw = socket.create_connection((connect_host, port), timeout=timeout)
                raw.settimeout(timeout)
                connection: Any = raw
                if secure:
                    context = ssl.create_default_context()
                    context.check_hostname = False
                    context.verify_mode = ssl.CERT_NONE
                    connection = context.wrap_socket(raw, server_hostname="localhost")
                with connection:
                    request = (
                        f"GET {path} HTTP/1.1\r\n"
                        f"Host: {display_host}:{port}\r\n"
                        "Connection: Upgrade\r\n"
                        "Upgrade: websocket\r\n"
                        f"Sec-WebSocket-Key: {key}\r\n"
                        "Sec-WebSocket-Version: 13\r\n\r\n"
                    )
                    connection.sendall(request.encode("ascii"))
                    chunks: list[bytes] = []
                    size = 0
                    while size < 8192:
                        chunk = connection.recv(min(2048, 8192 - size))
                        if not chunk:
                            break
                        chunks.append(chunk)
                        size += len(chunk)
                        if b"\r\n\r\n" in chunk or b"\r\n\r\n" in b"".join(chunks):
                            break
                    response = b"".join(chunks).lower()
                headers = response.split(b"\r\n\r\n", 1)[0]
                if headers.startswith(b"http/1.1 101") and b"upgrade: websocket" in headers:
                    scheme = "wss" if secure else "ws"
                    return f"{scheme}://{display_host}:{port}{path}"
            except (OSError, ssl.SSLError):
                continue
    return None


def discover_sockets() -> list[SocketCandidate]:
    """Find active TCP sockets and verify local websocket listeners."""
    connections = list_tcp_connections()
    listeners = [item for item in connections if item.state == "Listening"]
    probe_targets: dict[tuple[str, int], SocketCandidate] = {}
    for item in listeners:
        host, port = split_endpoint(item.local)
        if port is not None:
            normalized_host = (
                "127.0.0.1"
                if host in {"0.0.0.0", "*"}
                else "::1"
                if host == "::"
                else host
            )
            probe_targets.setdefault((normalized_host, port), item)

    verified: dict[tuple[int | None, int], str] = {}
    if probe_targets:
        with ThreadPoolExecutor(max_workers=min(24, len(probe_targets))) as executor:
            pending = {
                executor.submit(probe_websocket, host, port): (item.pid, port)
                for (host, port), item in probe_targets.items()
            }
            for future in as_completed(pending):
                try:
                    url = future.result()
                except Exception:
                    url = None
                if url:
                    verified[pending[future]] = url

    results: list[SocketCandidate] = []
    for item in connections:
        _host, port = split_endpoint(item.local)
        url = verified.get((item.pid, port or -1), "")
        if url and item.state == "Established":
            # The verified listening row already represents this endpoint. Hiding its
            # individual TCP sessions keeps the finder useful on busy servers.
            continue
        if url and item.state == "Listening":
            item = SocketCandidate(
                kind="WebSocket",
                local=item.local,
                remote=item.remote,
                state=item.state,
                pid=item.pid,
                process=item.process,
                url=url,
            )
        results.append(item)
    priority = {"WebSocket": 0, "Listening": 1, "Active": 2}
    return sorted(results, key=lambda item: (priority[item.kind], item.process.casefold(), item.local))


def timestamp() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def parse_header(value: str) -> tuple[str, str]:
    name, separator, header_value = value.partition(":")
    if not separator or not name.strip() or not header_value.strip():
        raise argparse.ArgumentTypeError("headers must look like 'Name: value'")
    return name.strip(), header_value.strip()


def parse_header_env(value: str) -> tuple[str, str]:
    name, separator, variable = value.partition("=")
    if not separator or not name.strip() or not variable.strip():
        raise argparse.ArgumentTypeError(
            "environment headers must look like 'Header-Name=ENV_VARIABLE'"
        )
    if variable.strip() not in os.environ:
        raise argparse.ArgumentTypeError(
            f"environment variable {variable.strip()!r} isn't set"
        )
    return name.strip(), os.environ[variable.strip()]


def _display_path(parent: str, part: str | int) -> str:
    if isinstance(part, int):
        return f"{parent}[{part}]"
    return f"{parent}.{part}"


def _strings_in(value: Any, path: str) -> Iterable[tuple[str, str]]:
    if isinstance(value, str):
        if value.strip():
            yield path, value
    elif isinstance(value, list):
        for index, item in enumerate(value):
            yield from _strings_in(item, _display_path(path, index))
    elif isinstance(value, dict):
        for key, item in value.items():
            yield from _strings_in(item, _display_path(path, str(key)))


def lookup_field(value: Any, field: str) -> tuple[str, Any] | None:
    """Resolve a simple dotted field such as choices.0.delta.content."""
    current = value
    path = "$"
    for part in field.split("."):
        if not part:
            return None
        if isinstance(current, dict) and part in current:
            current = current[part]
            path = _display_path(path, part)
        elif isinstance(current, list) and part.isdecimal():
            index = int(part)
            if index >= len(current):
                return None
            current = current[index]
            path = _display_path(path, index)
        else:
            return None
    return path, current


def extract_text(
    value: Any,
    *,
    fields: Sequence[str] = (),
    text_keys: Sequence[str] = DEFAULT_TEXT_KEYS,
) -> list[tuple[str, str]]:
    """Return ``(JSON path, text)`` pairs without repeating identical text."""
    found: list[tuple[str, str]] = []

    if fields:
        for field in fields:
            result = lookup_field(value, field)
            if result is not None:
                path, item = result
                found.extend(_strings_in(item, path))
    elif isinstance(value, str):
        found.append(("$", value))
    else:
        wanted = {key.casefold().replace("-", "_") for key in text_keys}

        def visit(item: Any, path: str) -> None:
            if isinstance(item, dict):
                for key, child in item.items():
                    child_path = _display_path(path, str(key))
                    normalized = str(key).casefold().replace("-", "_")
                    if normalized in wanted and isinstance(child, str):
                        if child.strip():
                            found.append((child_path, child))
                    elif normalized in wanted and isinstance(child, list):
                        for index, list_item in enumerate(child):
                            item_path = _display_path(child_path, index)
                            if isinstance(list_item, str) and list_item.strip():
                                found.append((item_path, list_item))
                            else:
                                visit(list_item, item_path)
                    else:
                        visit(child, child_path)
            elif isinstance(item, list):
                for index, child in enumerate(item):
                    visit(child, _display_path(path, index))

        visit(value, "$")

    unique: list[tuple[str, str]] = []
    seen: set[str] = set()
    for path, text in found:
        if text not in seen:
            seen.add(text)
            unique.append((path, text))
    return unique


@dataclass(frozen=True)
class DecodedMessage:
    raw: str
    data: Any | None
    is_json: bool
    is_binary: bool


def decode_message(message: str | bytes, binary_mode: str) -> DecodedMessage | None:
    is_binary = isinstance(message, bytes)
    if is_binary:
        if binary_mode == "skip":
            return None
        if binary_mode == "base64":
            raw = base64.b64encode(message).decode("ascii")
        else:
            raw = message.decode("utf-8", errors="replace")
    else:
        raw = message

    try:
        return DecodedMessage(raw, json.loads(raw), True, is_binary)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return DecodedMessage(raw, None, False, is_binary)


class JsonlLog:
    def __init__(self, path: Path | None) -> None:
        self.path = path
        self._file = None

    def __enter__(self) -> "JsonlLog":
        if self.path is not None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._file = self.path.open("a", encoding="utf-8")
        return self

    def write(self, record: dict[str, Any]) -> None:
        if self._file is not None:
            json.dump(record, self._file, ensure_ascii=False, separators=(",", ":"))
            self._file.write("\n")
            self._file.flush()

    def __exit__(self, *_: object) -> None:
        if self._file is not None:
            self._file.close()


def render_message(
    message: str | bytes,
    args: argparse.Namespace,
    log: JsonlLog,
    emit: Callable[[str], None] | None = None,
) -> None:
    decoded = decode_message(message, args.binary)
    if decoded is None:
        return

    when = timestamp()
    source = decoded.data if decoded.is_json else decoded.raw
    extracted = extract_text(source, fields=args.field, text_keys=args.text_keys)
    if args.match is not None:
        extracted = [item for item in extracted if args.match.search(item[1])]

    record: dict[str, Any] = {
        "timestamp": when,
        "binary": decoded.is_binary,
        "json": decoded.is_json,
        "extracted": [{"path": path, "text": text} for path, text in extracted],
    }
    record["data" if decoded.is_json else "raw"] = source
    log.write(record)

    prefix = "" if args.plain else f"[{when}] "
    if args.output == "raw":
        lines = [f"{prefix}{decoded.raw}"]
    elif args.output == "json":
        rendered = (
            json.dumps(decoded.data, ensure_ascii=False, indent=2)
            if decoded.is_json
            else decoded.raw
        )
        lines = [f"{prefix}{rendered}"]
    else:
        lines = []
        for path, text in extracted:
            label = "" if args.plain or len(extracted) == 1 else f"{path}: "
            lines.append(f"{prefix}{label}{text}")

    for line in lines:
        if emit is None:
            print(line, flush=True)
        else:
            emit(line)


async def wait_or_stop(stop: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except TimeoutError:
        pass


async def receive_messages(
    ws: Any,
    args: argparse.Namespace,
    log: JsonlLog,
    emit: Callable[[str], None] | None,
) -> None:
    async for message in ws:
        render_message(message, args, log, emit)


async def listen(
    args: argparse.Namespace,
    *,
    stop: asyncio.Event | None = None,
    emit: Callable[[str], None] | None = None,
    status: Callable[[str], None] | None = None,
    manage_signals: bool = True,
) -> None:
    if status is None:
        status = lambda text: print(text, file=sys.stderr, flush=True)
    try:
        from websockets.asyncio.client import connect
    except ImportError as exc:
        raise RuntimeError(
            'Missing dependency. Install it with: python -m pip install "websockets>=15,<18"'
        ) from exc

    stop = stop or asyncio.Event()
    loop = asyncio.get_running_loop()

    def request_stop() -> None:
        if not stop.is_set():
            status("Stopping...")
            stop.set()

    if manage_signals:
        for signal_name in ("SIGINT", "SIGTERM", "SIGBREAK"):
            sig = getattr(signal, signal_name, None)
            if sig is None:
                continue
            try:
                loop.add_signal_handler(sig, request_stop)
            except (NotImplementedError, RuntimeError):
                signal.signal(sig, lambda *_: loop.call_soon_threadsafe(request_stop))

    headers = dict(args.header + args.header_env)
    ssl_context = None
    if args.insecure:
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

    connect_options: dict[str, Any] = {
        "additional_headers": headers or None,
        "origin": args.origin,
        "subprotocols": args.subprotocol or None,
        "open_timeout": args.open_timeout,
        "ping_interval": args.ping_interval,
        "ping_timeout": args.ping_timeout,
        "max_size": None if args.max_size_mb == 0 else args.max_size_mb * 1024 * 1024,
    }
    if ssl_context is not None:
        connect_options["ssl"] = ssl_context

    startup_messages = list(args.send)
    for path in args.send_file:
        startup_messages.append(path.read_text(encoding="utf-8"))

    delay = args.retry_delay
    with JsonlLog(args.log) as log:
        while not stop.is_set():
            connected = False
            try:
                async with connect(args.url, **connect_options) as ws:
                    connected = True
                    delay = args.retry_delay
                    status(f"[{timestamp()}] Connected -> {args.url}")
                    for message in startup_messages:
                        await ws.send(message)

                    receiver = asyncio.create_task(receive_messages(ws, args, log, emit))
                    stopper = asyncio.create_task(stop.wait())
                    done, pending = await asyncio.wait(
                        (receiver, stopper), return_when=asyncio.FIRST_COMPLETED
                    )
                    for task in pending:
                        task.cancel()
                    await asyncio.gather(*pending, return_exceptions=True)
                    if receiver in done:
                        receiver.result()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if stop.is_set():
                    break
                status(f"[{timestamp()}] WebSocket error: {exc}")

            if stop.is_set() or not args.reconnect:
                break
            status(f"[{timestamp()}] Reconnecting in {delay:g}s...")
            await wait_or_stop(stop, delay)
            if not connected:
                delay = min(delay * 2, args.retry_max)


def run_gui(base_args: argparse.Namespace) -> int:
    """Run the listener and local socket finder in a compact Tk interface."""
    try:
        import tkinter as tk
        from tkinter import filedialog, messagebox, scrolledtext, ttk
    except ImportError as exc:
        raise SystemExit("Tkinter isn't available in this Python installation.") from exc

    class ListenerWindow:
        def __init__(self, root: Any) -> None:
            self.root = root
            self.base_args = base_args
            self.events: Queue[tuple[str, Any]] = Queue()
            self.worker: threading.Thread | None = None
            self.worker_loop: asyncio.AbstractEventLoop | None = None
            self.worker_stop: asyncio.Event | None = None
            self.disconnect_requested = threading.Event()
            self.discovery_worker: threading.Thread | None = None
            self.candidates: list[SocketCandidate] = []
            self.visible_candidates: dict[str, SocketCandidate] = {}
            self.more_visible = False

            root.title("WebSocket Finder")
            root.geometry("1000x760")
            root.minsize(780, 600)

            self.url = tk.StringVar(value=base_args.url)
            self.output_mode = tk.StringVar(value=base_args.output)
            self.fields = tk.StringVar(value=", ".join(base_args.field))
            self.match = tk.StringVar(value=base_args.match.pattern if base_args.match else "")
            self.log_enabled = tk.BooleanVar(value=base_args.log is not None)
            self.log_path = tk.StringVar(value=str(base_args.log or "ws_messages.jsonl"))
            self.reconnect = tk.BooleanVar(value=base_args.reconnect)
            self.plain = tk.BooleanVar(value=base_args.plain)
            self.insecure = tk.BooleanVar(value=base_args.insecure)
            self.binary = tk.StringVar(value=base_args.binary)
            self.origin = tk.StringVar(value=base_args.origin or "")
            self.subprotocols = tk.StringVar(value=", ".join(base_args.subprotocol))
            self.connection_status = tk.StringVar(value="Disconnected")
            self.socket_search = tk.StringVar()
            self.find_status = tk.StringVar(value="Find local websocket listeners and active connections.")

            frame = ttk.Frame(root, padding=12)
            frame.grid(sticky="nsew")
            root.rowconfigure(0, weight=1)
            root.columnconfigure(0, weight=1)
            frame.columnconfigure(1, weight=1)
            frame.columnconfigure(3, weight=1)

            ttk.Label(frame, text="URL").grid(row=0, column=0, sticky="w")
            ttk.Entry(frame, textvariable=self.url).grid(
                row=0, column=1, columnspan=3, padx=(8, 8), sticky="ew"
            )
            self.connect_button = ttk.Button(frame, text="Connect", command=self.connect)
            self.connect_button.grid(row=0, column=4, padx=(0, 6))
            self.disconnect_button = ttk.Button(
                frame, text="Disconnect", command=self.disconnect, state="disabled"
            )
            self.disconnect_button.grid(row=0, column=5)

            self.find_button = ttk.Button(frame, text="Find", command=self.find_sockets)
            self.find_button.grid(row=1, column=0, pady=(12, 6), sticky="w")
            ttk.Label(frame, text="Search").grid(row=1, column=1, padx=(10, 6), pady=(12, 6), sticky="e")
            ttk.Entry(frame, textvariable=self.socket_search).grid(
                row=1, column=2, columnspan=4, pady=(12, 6), sticky="ew"
            )

            socket_table = ttk.Frame(frame)
            socket_table.grid(row=2, column=0, columnspan=6, sticky="nsew")
            socket_table.rowconfigure(0, weight=1)
            socket_table.columnconfigure(0, weight=1)
            self.socket_list = ttk.Treeview(
                socket_table,
                columns=("kind", "address", "process"),
                show="headings",
                height=7,
                selectmode="browse",
            )
            self.socket_list.heading("kind", text="Type")
            self.socket_list.heading("address", text="Address")
            self.socket_list.heading("process", text="Process")
            self.socket_list.column("kind", width=95, minwidth=80, stretch=False)
            self.socket_list.column("address", width=520, minwidth=260)
            self.socket_list.column("process", width=180, minwidth=100)
            self.socket_list.grid(row=0, column=0, sticky="nsew")
            socket_scroll = ttk.Scrollbar(
                socket_table, orient="vertical", command=self.socket_list.yview
            )
            socket_scroll.grid(row=0, column=1, sticky="ns")
            self.socket_list.configure(yscrollcommand=socket_scroll.set)
            self.socket_list.tag_configure("verified", foreground="#08783e")
            self.socket_list.bind("<Double-1>", self.choose_socket)
            frame.rowconfigure(2, weight=1)

            ttk.Label(frame, textvariable=self.find_status).grid(
                row=3, column=0, columnspan=6, pady=(4, 8), sticky="w"
            )

            ttk.Label(frame, text="Show").grid(row=4, column=0, sticky="w")
            ttk.Combobox(
                frame,
                textvariable=self.output_mode,
                values=("text", "json", "raw"),
                state="readonly",
                width=9,
            ).grid(row=4, column=1, padx=(8, 18), sticky="w")
            ttk.Label(frame, text="Filter").grid(row=4, column=2, sticky="e")
            ttk.Entry(frame, textvariable=self.match).grid(
                row=4, column=3, padx=(8, 8), sticky="ew"
            )
            self.more_button = ttk.Button(frame, text="More", command=self.toggle_more)
            self.more_button.grid(row=4, column=4, columnspan=2, sticky="e")

            self.more = ttk.Frame(frame, padding=(0, 8, 0, 0))
            self.more.columnconfigure(1, weight=1)
            self.more.columnconfigure(3, weight=1)

            ttk.Label(self.more, text="Fields").grid(row=0, column=0, sticky="w")
            ttk.Entry(self.more, textvariable=self.fields).grid(
                row=0, column=1, padx=(8, 18), sticky="ew"
            )
            ttk.Label(self.more, text="Binary").grid(row=0, column=2, sticky="e")
            ttk.Combobox(
                self.more,
                textvariable=self.binary,
                values=("decode", "base64", "skip"),
                state="readonly",
                width=9,
            ).grid(row=0, column=3, padx=(8, 0), sticky="w")
            ttk.Checkbutton(self.more, text="Plain", variable=self.plain).grid(
                row=0, column=4, padx=(14, 0), sticky="w"
            )
            ttk.Checkbutton(self.more, text="Reconnect", variable=self.reconnect).grid(
                row=0, column=5, padx=(10, 0), sticky="w"
            )

            ttk.Checkbutton(self.more, text="Log", variable=self.log_enabled).grid(
                row=1, column=0, pady=(8, 0), sticky="w"
            )
            ttk.Entry(self.more, textvariable=self.log_path).grid(
                row=1, column=1, columnspan=4, padx=(8, 8), pady=(8, 0), sticky="ew"
            )
            ttk.Button(self.more, text="Browse", command=self.browse_log).grid(
                row=1, column=5, pady=(8, 0)
            )

            ttk.Label(self.more, text="Headers").grid(row=2, column=0, pady=(8, 0), sticky="nw")
            self.headers = tk.Text(self.more, height=3, width=38, wrap="none")
            self.headers.grid(row=2, column=1, rowspan=2, padx=(8, 18), pady=(8, 0), sticky="ew")
            for name, value in base_args.header:
                self.headers.insert("end", f"{name}: {value}\n")
            ttk.Label(self.more, text="Origin").grid(row=2, column=2, pady=(8, 0), sticky="e")
            ttk.Entry(self.more, textvariable=self.origin).grid(
                row=2, column=3, columnspan=3, padx=(8, 0), pady=(8, 0), sticky="ew"
            )
            ttk.Label(self.more, text="Protocols").grid(row=3, column=2, pady=(8, 0), sticky="e")
            ttk.Entry(self.more, textvariable=self.subprotocols).grid(
                row=3, column=3, columnspan=3, padx=(8, 0), pady=(8, 0), sticky="ew"
            )
            ttk.Checkbutton(
                self.more,
                text="Allow untrusted certificate",
                variable=self.insecure,
            ).grid(row=4, column=1, columnspan=2, pady=(8, 0), sticky="w")

            ttk.Label(self.more, text="Send once").grid(
                row=5, column=0, pady=(8, 0), sticky="nw"
            )
            self.startup_message = tk.Text(self.more, height=3, wrap="word")
            self.startup_message.grid(
                row=5, column=1, columnspan=5, padx=(8, 0), pady=(8, 0), sticky="ew"
            )
            if base_args.send:
                self.startup_message.insert("1.0", base_args.send[0])

            output_bar = ttk.Frame(frame)
            output_bar.grid(row=6, column=0, columnspan=6, pady=(10, 4), sticky="new")
            output_bar.columnconfigure(0, weight=1)
            ttk.Label(output_bar, textvariable=self.connection_status).grid(row=0, column=0, sticky="w")
            ttk.Button(output_bar, text="Clear", command=self.clear).grid(row=0, column=1)

            self.output = scrolledtext.ScrolledText(frame, wrap="word", state="disabled")
            self.output.grid(row=7, column=0, columnspan=6, sticky="nsew")
            frame.rowconfigure(7, weight=2)
            self.output.tag_configure("status", foreground="#666666")
            self.output.tag_configure("error", foreground="#b00020")

            self.socket_search.trace_add("write", lambda *_: self.refresh_socket_list())
            root.protocol("WM_DELETE_WINDOW", self.close)
            root.after(75, self.drain_events)
            root.after(250, self.find_sockets)

        @staticmethod
        def comma_list(value: str) -> list[str]:
            return [part.strip() for part in value.split(",") if part.strip()]

        def build_args(self) -> argparse.Namespace:
            args = argparse.Namespace(**vars(self.base_args))
            args.url = self.url.get().strip()
            if not args.url.startswith(("ws://", "wss://")):
                raise ValueError("URL must start with ws:// or wss://")
            args.output = self.output_mode.get()
            args.field = self.comma_list(self.fields.get())
            args.match = re.compile(self.match.get()) if self.match.get() else None
            raw_log_path = self.log_path.get().strip()
            if self.log_enabled.get() and not raw_log_path:
                raise ValueError("Choose a log file or turn logging off")
            args.log = Path(raw_log_path).expanduser() if self.log_enabled.get() else None
            args.reconnect = self.reconnect.get()
            args.plain = self.plain.get()
            args.binary = self.binary.get()
            args.origin = self.origin.get().strip() or None
            args.subprotocol = self.comma_list(self.subprotocols.get())
            args.insecure = self.insecure.get()
            if args.insecure and not args.url.startswith("wss://"):
                raise ValueError("The untrusted-certificate option only applies to wss:// URLs")
            args.header = []
            for line in self.headers.get("1.0", "end").splitlines():
                if line.strip():
                    args.header.append(parse_header(line))
            startup = self.startup_message.get("1.0", "end-1c")
            args.send = [startup] if startup else []
            return args

        def toggle_more(self) -> None:
            self.more_visible = not self.more_visible
            if self.more_visible:
                self.more.grid(row=5, column=0, columnspan=6, sticky="ew")
                self.more_button.configure(text="Less")
            else:
                self.more.grid_remove()
                self.more_button.configure(text="More")

        def find_sockets(self) -> None:
            if self.discovery_worker and self.discovery_worker.is_alive():
                return
            self.find_button.configure(state="disabled")
            self.find_status.set("Finding sockets…")

            def run() -> None:
                try:
                    self.events.put(("discovered", discover_sockets()))
                except Exception as exc:
                    self.events.put(("discover_error", str(exc)))

            self.discovery_worker = threading.Thread(target=run, daemon=True)
            self.discovery_worker.start()

        def refresh_socket_list(self) -> None:
            query = self.socket_search.get().casefold().split()
            selected = self.socket_list.selection()
            selected_candidate = self.visible_candidates.get(selected[0]) if selected else None
            self.socket_list.delete(*self.socket_list.get_children())
            self.visible_candidates.clear()
            shown = 0
            for index, candidate in enumerate(self.candidates):
                if query and not all(token in candidate.search_text for token in query):
                    continue
                address = candidate.url or candidate.local
                if candidate.remote:
                    address += f"  →  {candidate.remote}"
                process = candidate.process
                if candidate.pid is not None:
                    process = f"{process or 'PID'} ({candidate.pid})"
                item_id = f"socket-{index}"
                tags = ("verified",) if candidate.kind == "WebSocket" else ()
                self.socket_list.insert(
                    "", "end", iid=item_id, values=(candidate.kind, address, process), tags=tags
                )
                self.visible_candidates[item_id] = candidate
                shown += 1
                if candidate == selected_candidate:
                    self.socket_list.selection_set(item_id)
            if selected_candidate is None:
                self.socket_list.yview_moveto(0)
            if query:
                self.find_status.set(f"{shown} of {len(self.candidates)} shown. Double-click to use an address.")
            else:
                self.set_socket_summary()

        def set_socket_summary(self) -> None:
            verified = sum(item.kind == "WebSocket" for item in self.candidates)
            listeners = sum(item.kind == "Listening" for item in self.candidates)
            active = sum(item.kind == "Active" for item in self.candidates)
            self.find_status.set(
                f"{verified} verified websockets · {listeners} listeners · {active} active TCP connections"
            )

        def choose_socket(self, _event: Any = None) -> None:
            selected = self.socket_list.selection()
            if not selected:
                return
            candidate = self.visible_candidates.get(selected[0])
            if candidate is None:
                return
            url = candidate.url
            if not url and candidate.state == "Listening":
                host, port = split_endpoint(candidate.local)
                if port is not None:
                    host = "127.0.0.1" if host in {"0.0.0.0", "*"} else "::1" if host == "::" else host
                    url = f"ws://{display_endpoint(host, port)}"
            if url:
                self.url.set(url)
                self.connection_status.set("Address selected. Press Connect.")
            else:
                self.connection_status.set("This active connection does not expose its websocket URL.")

        def connect(self) -> None:
            try:
                args = self.build_args()
            except (ValueError, re.error, argparse.ArgumentTypeError) as exc:
                messagebox.showerror("Invalid setting", str(exc), parent=self.root)
                return

            self.connect_button.configure(state="disabled")
            self.disconnect_button.configure(state="normal")
            self.connection_status.set("Connecting…")
            self.disconnect_requested.clear()
            self.worker = threading.Thread(target=self.run_worker, args=(args,), daemon=True)
            self.worker.start()

        def run_worker(self, args: argparse.Namespace) -> None:
            async def runner() -> None:
                self.worker_loop = asyncio.get_running_loop()
                self.worker_stop = asyncio.Event()
                if self.disconnect_requested.is_set():
                    self.worker_stop.set()
                await listen(
                    args,
                    stop=self.worker_stop,
                    emit=lambda text: self.events.put(("output", text)),
                    status=lambda text: self.events.put(("status", text)),
                    manage_signals=False,
                )

            try:
                asyncio.run(runner())
            except Exception as exc:
                self.events.put(("error", str(exc)))
            finally:
                self.worker_loop = None
                self.worker_stop = None
                self.events.put(("done", "Disconnected"))

        def disconnect(self) -> None:
            self.connection_status.set("Disconnecting…")
            self.disconnect_requested.set()
            if self.worker_loop is not None and self.worker_stop is not None:
                self.worker_loop.call_soon_threadsafe(self.worker_stop.set)

        def append(self, text: str, tag: str = "output") -> None:
            self.output.configure(state="normal")
            self.output.insert("end", text + "\n", tag)
            self.output.see("end")
            self.output.configure(state="disabled")

        def drain_events(self) -> None:
            try:
                while True:
                    kind, text = self.events.get_nowait()
                    if kind == "output":
                        self.append(text)
                    elif kind == "status":
                        self.connection_status.set(text)
                        self.append(text, "status")
                    elif kind == "error":
                        self.connection_status.set("Error")
                        self.append(text, "error")
                    elif kind == "done":
                        self.connection_status.set(text)
                        self.connect_button.configure(state="normal")
                        self.disconnect_button.configure(state="disabled")
                    elif kind == "discovered":
                        self.candidates = text
                        self.find_button.configure(state="normal")
                        self.refresh_socket_list()
                        self.set_socket_summary()
                    elif kind == "discover_error":
                        self.find_button.configure(state="normal")
                        self.find_status.set(text)
            except Empty:
                pass
            self.root.after(75, self.drain_events)

        def browse_log(self) -> None:
            chosen = filedialog.asksaveasfilename(
                parent=self.root,
                title="Choose JSONL log",
                defaultextension=".jsonl",
                filetypes=(("JSON Lines", "*.jsonl"), ("All files", "*.*")),
            )
            if chosen:
                self.log_path.set(chosen)
                self.log_enabled.set(True)

        def clear(self) -> None:
            self.output.configure(state="normal")
            self.output.delete("1.0", "end")
            self.output.configure(state="disabled")

        def close(self) -> None:
            self.disconnect_requested.set()
            if self.worker_loop is not None and self.worker_stop is not None:
                self.worker_loop.call_soon_threadsafe(self.worker_stop.set)
            self.root.destroy()

    root = tk.Tk()
    try:
        ttk.Style(root).theme_use("vista" if sys.platform.startswith("win") else "clam")
    except tk.TclError:
        pass
    ListenerWindow(root)
    root.mainloop()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Listen to a WebSocket and extract text from plain-text or JSON messages.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "url",
        nargs="?",
        default=os.getenv("WS_URL", DEFAULT_URL),
        help="ws:// or wss:// address (or set WS_URL)",
    )
    parser.add_argument("--gui", action="store_true", help="open the graphical listener")
    parser.add_argument(
        "--output",
        choices=("text", "json", "raw"),
        default="text",
        help="what to print to standard output",
    )
    parser.add_argument(
        "--field",
        action="append",
        default=[],
        metavar="PATH",
        help="extract an exact dotted JSON path; repeat for multiple paths",
    )
    parser.add_argument(
        "--text-key",
        action="append",
        default=[],
        metavar="KEY",
        help="add a JSON key to automatic text detection",
    )
    parser.add_argument("--match", metavar="REGEX", help="print only matching extracted text")
    parser.add_argument("--plain", action="store_true", help="omit timestamps and JSON paths")
    parser.add_argument(
        "--binary",
        choices=("decode", "base64", "skip"),
        default="decode",
        help="how to handle binary messages",
    )
    parser.add_argument(
        "--log",
        type=Path,
        default=Path(os.getenv("WS_LOG_FILE", "ws_messages.jsonl")),
        help="append complete structured messages to this JSONL file",
    )
    parser.add_argument(
        "--no-log", dest="log", action="store_const", const=None, help="disable file logging"
    )
    parser.add_argument(
        "--header",
        action="append",
        type=parse_header,
        default=[],
        metavar="NAME: VALUE",
        help="handshake header; repeat as needed",
    )
    parser.add_argument(
        "--header-env",
        action="append",
        type=parse_header_env,
        default=[],
        metavar="NAME=ENV_VAR",
        help="read a secret handshake header from an environment variable",
    )
    parser.add_argument("--origin", help="Origin handshake value")
    parser.add_argument(
        "--subprotocol", action="append", default=[], help="subprotocol; repeat in preference order"
    )
    parser.add_argument(
        "--send", action="append", default=[], metavar="MESSAGE", help="send after connecting"
    )
    parser.add_argument(
        "--send-file",
        action="append",
        type=Path,
        default=[],
        metavar="PATH",
        help="send a UTF-8 file after connecting",
    )
    parser.add_argument("--no-reconnect", dest="reconnect", action="store_false")
    parser.add_argument("--retry-delay", type=float, default=1.0)
    parser.add_argument("--retry-max", type=float, default=30.0)
    parser.add_argument("--open-timeout", type=float, default=10.0)
    parser.add_argument("--ping-interval", type=float, default=20.0)
    parser.add_argument("--ping-timeout", type=float, default=20.0)
    parser.add_argument(
        "--max-size-mb", type=int, default=32, help="maximum message size; 0 disables the limit"
    )
    parser.add_argument(
        "--insecure",
        action="store_true",
        help="disable wss:// certificate validation (testing only)",
    )
    parser.set_defaults(reconnect=True)
    return parser


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.retry_delay <= 0 or args.retry_max <= 0:
        parser.error("retry delays must be greater than zero")
    if args.max_size_mb < 0:
        parser.error("--max-size-mb cannot be negative")
    if args.insecure and not args.gui and not args.url.startswith("wss://"):
        parser.error("--insecure only applies to wss:// URLs")
    try:
        args.match = re.compile(args.match) if args.match else None
    except re.error as exc:
        parser.error(f"invalid --match expression: {exc}")
    args.text_keys = (*DEFAULT_TEXT_KEYS, *args.text_key)
    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if args.gui:
        return run_gui(args)
    try:
        asyncio.run(listen(args))
    except RuntimeError as exc:
        print(exc, file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
