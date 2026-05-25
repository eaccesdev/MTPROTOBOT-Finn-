@echo off
:: Auto-Proxy Daemon launcher (Windows 7+)
title Telegram Auto-Proxy Daemon
cd /d "%~dp0"

if not exist daemon_config.json (
    copy daemon_config.example.json daemon_config.json
    echo.
    echo [!] daemon_config.json created.
    echo     Fill in your api_id and api_hash from https://my.telegram.org
    echo     then run this script again.
    pause
    exit /b 1
)

:loop
echo Starting auto-proxy daemon...
python auto_proxy_daemon.py
echo.
echo Daemon stopped. Restarting in 5s... (Ctrl+C to quit)
timeout /t 5 /nobreak >NUL
goto loop
