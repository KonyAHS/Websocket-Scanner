@echo off
rem Launch the WebSocket Scanner window from this folder.
cd /d "%~dp0"
where py >nul 2>nul
if %errorlevel%==0 (py ws_listener.py --gui %*) else (python ws_listener.py --gui %*)
if errorlevel 1 pause
