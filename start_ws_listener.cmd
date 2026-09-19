@echo off
cd /d "%~dp0"
py ws_listener.py --gui
if errorlevel 1 pause
