@echo off
cd /d "%~dp0"

if exist "%~dp0python\python.exe" (
    "%~dp0python\python.exe" main.py
) else (
    python main.py
)
pause
