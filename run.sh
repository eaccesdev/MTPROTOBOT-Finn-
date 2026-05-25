#!/usr/bin/env bash
# Telegram Proxy Manager Bot — Linux / Termux (Android) Launcher
cd "$(dirname "$0")"

if [ -f venv/bin/python ]; then
    PYC="venv/bin/python"
else
    PYC=$(command -v python3 || command -v python)
fi

while true; do
    echo "Starting bot..."
    $PYC bot.py
    echo "Bot stopped. Restarting in 5 seconds... (Ctrl+C to exit)"
    sleep 5
done
