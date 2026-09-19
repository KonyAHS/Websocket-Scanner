import socket
import threading
import unittest

import ws_listener


class DiscoveryTests(unittest.TestCase):
    def test_split_endpoint_handles_ipv4_and_ipv6(self) -> None:
        self.assertEqual(ws_listener.split_endpoint("127.0.0.1:6677"), ("127.0.0.1", 6677))
        self.assertEqual(ws_listener.split_endpoint("[::1]:8080"), ("::1", 8080))
        self.assertEqual(ws_listener.split_endpoint("*:*"), ("*", None))

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


if __name__ == "__main__":
    unittest.main()
