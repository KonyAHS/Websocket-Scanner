import argparse
import asyncio
import json
import socket
import tempfile
import threading
import unittest
from pathlib import Path

import ws_listener


class DiscoveryTests(unittest.TestCase):
    def test_split_endpoint_handles_ipv4_and_ipv6(self) -> None:
        self.assertEqual(ws_listener.split_endpoint("127.0.0.1:6677"), ("127.0.0.1", 6677))
        self.assertEqual(ws_listener.split_endpoint("[::1]:8080"), ("::1", 8080))
        self.assertEqual(ws_listener.split_endpoint("*:*"), ("*", None))

    def test_display_endpoint_brackets_ipv6(self) -> None:
        self.assertEqual(ws_listener.display_endpoint("::1", 80), "[::1]:80")
        self.assertEqual(ws_listener.display_endpoint("127.0.0.1", 80), "127.0.0.1:80")

    def test_parse_netstat_keeps_listeners_and_active_connections(self) -> None:
        sample = """
          TCP    0.0.0.0:6677        0.0.0.0:0          LISTENING       42
          TCP    127.0.0.1:6677      127.0.0.1:51000    ESTABLISHED     42
        """
        found = ws_listener.parse_netstat_connections(sample, {42: "listener.exe"})
        self.assertEqual([item.kind for item in found], ["Listening", "Active"])
        self.assertEqual(found[0].process, "listener.exe")
        self.assertEqual(found[1].remote, "127.0.0.1:51000")

    def test_probe_finds_websocket_path(self) -> None:
        server = socket.socket()
        server.bind(("127.0.0.1", 0))
        server.listen()
        server.settimeout(2)
        port = server.getsockname()[1]

        def serve() -> None:
            try:
                for _ in range(4):
                    connection, _address = server.accept()
                    with connection:
                        request = connection.recv(4096)
                        if request.startswith(b"GET /ws "):
                            connection.sendall(
                                b"HTTP/1.1 101 Switching Protocols\r\n"
                                b"Connection: Upgrade\r\n"
                                b"Upgrade: websocket\r\n\r\n"
                            )
                            return
                        connection.sendall(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")
            finally:
                server.close()

        worker = threading.Thread(target=serve, daemon=True)
        worker.start()
        self.assertEqual(
            ws_listener.probe_websocket("127.0.0.1", port, timeout=0.5),
            f"ws://127.0.0.1:{port}/ws",
        )
        worker.join(timeout=2)


class ExtractionTests(unittest.TestCase):
    def test_finds_known_keys_anywhere_and_deduplicates(self) -> None:
        payload = {
            "data": {"text": "hello", "meta": {"caption": "hello"}},
            "items": [{"content": "bye"}],
        }
        self.assertEqual(
            ws_listener.extract_text(payload),
            [("$.data.text", "hello"), ("$.items[0].content", "bye")],
        )

    def test_exact_fields_override_automatic_detection(self) -> None:
        payload = {"choices": [{"delta": {"content": "token"}}], "text": "ignored"}
        self.assertEqual(
            ws_listener.extract_text(payload, fields=["choices.0.delta.content"]),
            [("$.choices[0].delta.content", "token")],
        )

    def test_missing_field_returns_nothing(self) -> None:
        self.assertIsNone(ws_listener.lookup_field({"a": [1]}, "a.5"))
        self.assertEqual(ws_listener.extract_text({"a": 1}, fields=["b"]), [])

    def test_plain_string_is_returned_as_is(self) -> None:
        self.assertEqual(ws_listener.extract_text("just text"), [("$", "just text")])

    def test_decode_message_modes(self) -> None:
        self.assertIsNone(ws_listener.decode_message(b"\x00", "skip"))
        self.assertEqual(ws_listener.decode_message(b"hi", "base64").raw, "aGk=")
        decoded = ws_listener.decode_message('{"text": "x"}', "decode")
        self.assertTrue(decoded.is_json)
        self.assertEqual(decoded.data, {"text": "x"})


class ArgumentTests(unittest.TestCase):
    def test_header_parsing(self) -> None:
        self.assertEqual(
            ws_listener.parse_header("Authorization: Bearer x"), ("Authorization", "Bearer x")
        )
        with self.assertRaises(argparse.ArgumentTypeError):
            ws_listener.parse_header("no-colon")

    def test_defaults_and_validation(self) -> None:
        args = ws_listener.parse_args(["ws://example.test", "--match", "hi", "--text-key", "body"])
        self.assertEqual(args.url, "ws://example.test")
        self.assertTrue(args.reconnect)
        self.assertIn("body", args.text_keys)
        self.assertIsNotNone(args.match.search("oh hi"))
        with self.assertRaises(SystemExit):
            ws_listener.parse_args(["ws://example.test", "--insecure"])


class ListenTests(unittest.TestCase):
    def test_listen_prints_and_logs_messages(self) -> None:
        from websockets.asyncio.server import serve

        async def handler(ws) -> None:
            await ws.send(json.dumps({"data": {"text": "first"}}))
            await ws.send("second")

        lines: list[str] = []
        with tempfile.TemporaryDirectory() as folder:
            log_path = Path(folder) / "log.jsonl"

            async def scenario() -> None:
                async with serve(handler, "127.0.0.1", 0) as server:
                    port = server.sockets[0].getsockname()[1]
                    args = ws_listener.parse_args(
                        [
                            f"ws://127.0.0.1:{port}",
                            "--plain",
                            "--no-reconnect",
                            "--log",
                            str(log_path),
                        ]
                    )
                    await asyncio.wait_for(
                        ws_listener.listen(
                            args, emit=lines.append, status=lambda _text: None, manage_signals=False
                        ),
                        timeout=10,
                    )

            asyncio.run(scenario())
            records = [
                json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines()
            ]

        self.assertEqual(lines, ["first", "second"])
        self.assertEqual([record["json"] for record in records], [True, False])
        self.assertEqual(records[0]["extracted"], [{"path": "$.data.text", "text": "first"}])


if __name__ == "__main__":
    unittest.main()
