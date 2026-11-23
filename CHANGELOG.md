# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [1.0.0] - 2026-11-23

First tagged release.

### Added

- Standalone Windows executable (`WebSocketScanner-v1.0.0-windows-x64.exe`), no Python needed.
- `pip install` support via `pyproject.toml`, with `ws-scanner` and `ws-scanner-gui` commands.
- App icon (`assets/icon.svg`, plus PNG and ICO) used for the window and the executable.
- `--version` flag and `--connect` (connect on launch when used with `--gui`).
- `examples/demo_server.py`, a local server that streams sample captions.
- Sharper rendering on high-DPI Windows displays; the window scales with the display.
- README with screenshots, MIT license, CI on Windows and Linux (Python 3.10 to 3.13), and an automated release workflow.
- Tests for text extraction, argument parsing and a full connect, receive and log round trip.

### Changed

- The window is now titled "WebSocket Scanner".
- `requirements-ws-listener.txt` is now `requirements.txt`; `start_ws_listener.cmd` is merged into `run.bat`.
- Python 3.10 is supported (reconnect waits no longer depend on 3.11 timeout semantics).

### Removed

- The committed `ws_messages.jsonl` capture. `*.jsonl` logs are now ignored by git.

[1.0.0]: https://github.com/KonyAHS/Websocket-Scanner/releases/tag/v1.0.0
