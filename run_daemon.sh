#!/usr/bin/env bash
# Auto-Proxy Daemon launcher (Linux / Termux)
cd "$(dirname "$0")"

# Use the venv python if present, otherwise fall back to system python
if [ -f venv/bin/python ]; then
    PYC="venv/bin/python"
else
    PYC=$(command -v python3 || command -v python)
fi

if [ ! -f daemon_config.json ]; then
    cp daemon_config.example.json daemon_config.json
    echo ""
    echo "[!] daemon_config.json created."
    echo "    Fill in your api_id and api_hash from https://my.telegram.org"
    echo "    then run this script again."
    exit 1
fi

echo "Starting auto-proxy daemon..."
while true; do
    $PYC auto_proxy_daemon.py
    echo "Daemon stopped. Restarting in 5s... (Ctrl+C to quit)"
    sleep 5
done
