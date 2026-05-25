#!/usr/bin/env bash
# install.sh — Install (or update) the systemd service units.
#
# Usage:
#   bash systemd/install.sh          — install + enable both services
#   bash systemd/install.sh --user   — install as user services (no sudo)
#
# After install:
#   sudo systemctl start  telegram-proxy-bot     (or: systemctl --user start ...)
#   sudo systemctl status telegram-proxy-bot
#   journalctl -u telegram-proxy-bot -f

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BOT_SVC="telegram-proxy-bot.service"
DAEMON_SVC="telegram-proxy-daemon.service"

if [[ "${1:-}" == "--user" ]]; then
    TARGET_DIR="${HOME}/.config/systemd/user"
    mkdir -p "${TARGET_DIR}"
    cp "${SCRIPT_DIR}/${BOT_SVC}"    "${TARGET_DIR}/${BOT_SVC}"
    cp "${SCRIPT_DIR}/${DAEMON_SVC}" "${TARGET_DIR}/${DAEMON_SVC}"
    systemctl --user daemon-reload
    systemctl --user enable --now "${BOT_SVC}"
    systemctl --user enable --now "${DAEMON_SVC}"
    echo ""
    echo "Services installed as user units."
    echo "View logs: journalctl --user -u telegram-proxy-bot -f"
else
    # System-wide install (requires sudo)
    TARGET_DIR="/etc/systemd/system"
    sudo cp "${SCRIPT_DIR}/${BOT_SVC}"    "${TARGET_DIR}/${BOT_SVC}"
    sudo cp "${SCRIPT_DIR}/${DAEMON_SVC}" "${TARGET_DIR}/${DAEMON_SVC}"
    sudo systemctl daemon-reload
    sudo systemctl enable --now "${BOT_SVC}"
    sudo systemctl enable --now "${DAEMON_SVC}"
    echo ""
    echo "Services installed system-wide."
    echo "View logs: journalctl -u telegram-proxy-bot -f"
fi
