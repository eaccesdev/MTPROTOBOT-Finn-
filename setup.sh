#!/usr/bin/env bash
# ============================================================
# Telegram Proxy Manager Bot — Linux / Termux (Android) Setup
# ============================================================

set -e

echo "=========================================="
echo " Telegram Proxy Manager Bot — Setup"
echo "=========================================="

# Termux: install Python if missing
if command -v pkg &>/dev/null; then
    echo "[Termux detected] Installing Python & pip…"
    pkg install -y python libexpat openssl
fi

# Check Python 3.8+
PY=$(python3 --version 2>&1 || python --version 2>&1)
echo "Python: $PY"

PYC=$(command -v python3 || command -v python)

# Upgrade pip
$PYC -m pip install --upgrade pip --quiet

# Install deps
echo "Installing dependencies…"
$PYC -m pip install -r requirements.txt

# Create config if missing
if [ ! -f config.json ]; then
    cp config.example.json config.json
    echo ""
    echo "[!] config.json created from template."
fi

echo ""
echo "=========================================="
echo " Setup complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "  1. Edit config.json with your bot token and admin ID"
echo "  2. Run the bot with:  bash run.sh"
echo "     or:                python3 bot.py"
