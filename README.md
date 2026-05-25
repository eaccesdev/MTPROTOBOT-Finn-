# MTPROTO Proxy Manager Bot

A self-hosted Telegram bot that automatically discovers, tests, scores, and rotates working
MTProto, SOCKS5, and HTTP proxies — keeping your Telegram connection alive with zero manual
intervention after the initial setup.

> **Fully automated pipeline:** Scrape → TCP-check → Score → Connect → Monitor → Rotate → Notify

---

## What Is This?

This is a two-process system that manages Telegram proxies on your behalf:

```
┌─────────────────────────────────┐   ┌────────────────────────────────────────┐
│  Bot  (bot.py)                  │   │  Daemon  (auto_proxy_daemon.py)         │
│                                 │   │                                          │
│  • 27 admin commands            │   │  • Runs a live Telegram (Telethon)       │
│  • Scrapes proxy channels       │   │    connection through the best proxy     │
│  • Fetches from URL sources     │   │  • Pings every 30 s to verify health     │
│  • TCP-checks + geo-locates     │   │  • Rotates instantly when proxy dies     │
│  • Scores by latency + uptime   │   │  • Pushes new proxy to Telegram Desktop  │
│  • Inline "Apply" buttons       │   │  • Tries verified proxies first (~6 s)   │
│  • Daily digest to admins       │   │                                          │
└──────────────┬──────────────────┘   └──────────────────┬─────────────────────┘
               │                                          │
               └──────────── proxies.json ────────────────┘
                             connection.json
```

You can run the **bot only**, the **daemon only**, or **both together** for the full experience.

---

## Features

### Proxy Management
- Add proxies manually via command or by pasting a link directly into chat
- Bulk-import from `.json` or `.txt` files
- Export the full pool as a downloadable file
- One-tap **inline connect buttons** — tap to open Telegram's proxy dialog, pre-filled
- Remove, clean dead proxies, or purge stale ones automatically

### Scraping & Discovery
- Scrapes public Telegram proxy channels via `t.me/s/` (no Telegram account needed)
- Fetches from any external URL: GitHub raw files, Geonode-compatible JSON APIs, plain text lists
- Supports MTProto (`tg://proxy?...`), SOCKS5 (`socks5://...`), and HTTP proxies
- Blocklist filters out fake/honeypot proxy entries

### Checking & Scoring
- Concurrent TCP reachability checks (up to 50 at once) with DNS pre-caching
- Latency measured in milliseconds per proxy
- Uptime tracking (check count + success count) across restarts
- Score formula: `latency_ms + (1 − uptime) × 2000` — lower is better
- Geo-location via ip-api.com (country flag + name, zero API key required)

### Connection & Auto-Rotation
- **Bot** monitors the active proxy every 2 minutes; rotates and notifies on failure
- **Daemon** (Telethon) pings every 30 seconds; rotates within seconds of a failure
- Best-scoring proxy is always chosen; previously verified proxies are tried first
- After first successful MTProto connection, reconnect time drops to ~6 seconds

### Notifications & Reporting
- Instant rotation alerts sent to all configured admin IDs
- Daily digest at a configurable UTC hour with pool stats + top proxies
- Inline "Apply Proxy in Telegram" button on every rotation alert

### Supported Platforms
- **Linux** (Ubuntu/Debian, Arch, headless servers)
- **Android** via Termux
- **Windows 7+** (`.bat` launchers included)
- Systemd service units included for boot-time auto-start (Linux)

---

## Quick Start

### 1. Clone the repository

```bash
git clone https://github.com/eaccesdev/MTPROTOBOT-Finn-.git
cd MTPROTOBOT-Finn-
```

### 2. Run setup

**Linux / Termux:**
```bash
bash setup.sh
```

**Windows:**
```
setup.bat
```

This creates a virtual environment and installs all dependencies.

### 3. Configure the bot

```bash
cp config.example.json config.json
```

Edit `config.json`:

```json
{
  "bot_token": "YOUR:BOT_TOKEN_HERE",
  "admin_ids": [YOUR_TELEGRAM_USER_ID]
}
```

- Get a bot token from [@BotFather](https://t.me/BotFather) → `/newbot`
- Get your user ID from [@userinfobot](https://t.me/userinfobot)

### 4. Start the bot

**Linux / Termux:**
```bash
bash run.sh
```

**Windows:**
```
run.bat
```

### 5. Send your first commands

```
/fetch      ← scrape all configured channels and URL sources for proxies
/check      ← TCP-test every proxy (marks alive/dead, records latency)
/connect    ← connect to the best-scoring alive proxy and start monitoring
```

After these three commands the system runs **fully automatically**. The bot fetches, checks, and
rotates without any further input.

---

## Configuration Reference

### `config.json`

| Field | Default | Description |
|-------|---------|-------------|
| `bot_token` | *(required)* | Token from [@BotFather](https://t.me/BotFather) |
| `admin_ids` | *(required)* | Array of Telegram user IDs with admin access |
| `source_channels` | `[]` | Telegram channel usernames to scrape (no `@`) |
| `source_urls` | `[]` | External URLs to fetch proxy lists from |
| `check_interval_minutes` | `30` | Background TCP-check cycle interval |
| `auto_fetch_interval_minutes` | `60` | Background scrape cycle interval |
| `monitor_interval_minutes` | `2` | Active-proxy health check interval |
| `daily_digest_hour` | `9` | UTC hour to send daily stats (0–23) |
| `check_timeout_seconds` | `8` | TCP connect timeout per proxy |
| `max_proxies` | `1000` | Maximum proxies to keep in the pool |
| `auto_remove_blocked` | `true` | Delete confirmed-dead proxies after a check run |
| `stale_days` | `7` | Days before a persistently-dead proxy is eligible for `/purge` |

### `daemon_config.json` (optional — for the daemon only)

```bash
cp daemon_config.example.json daemon_config.json
```

| Field | Default | Description |
|-------|---------|-------------|
| `api_id` | *(required)* | From [my.telegram.org](https://my.telegram.org) → App Configuration |
| `api_hash` | *(required)* | From [my.telegram.org](https://my.telegram.org) → App Configuration |
| `check_interval_seconds` | `30` | How often the daemon pings the active proxy |
| `reconnect_delay_seconds` | `5` | Wait between rotation attempts |
| `max_rotate_attempts` | `10` | Max rotation retries before giving up |

---

## Running

### Bot only
```bash
bash run.sh        # Linux / Termux
run.bat            # Windows
```

### Daemon only (requires Telegram account login on first run)
```bash
bash run_daemon.sh        # Linux / Termux
run_daemon.bat            # Windows
```

On first run you will be prompted for your phone number and a verification code. After that a
`daemon_session.session` file is saved and you will **never be prompted again**.

### Both (recommended)

Run `run.sh` and `run_daemon.sh` in separate terminals.

### systemd (Linux — runs on boot)

```bash
bash systemd/install.sh --user     # user-level, no sudo
# or
bash systemd/install.sh            # system-wide, requires sudo
```

View logs:
```bash
journalctl -u telegram-proxy-bot -f
journalctl -u telegram-proxy-daemon -f
```

---

## Bot Commands

All commands require your user ID to be in `admin_ids`.

### Connection

| Command | Description |
|---------|-------------|
| `/connect` | Pick the best-scoring alive proxy and start monitoring |
| `/connect 3` | Connect specifically to proxy #3 from `/list` |
| `/disconnect` | Stop monitoring and clear the active proxy |
| `/status` | Show active proxy — latency, uptime, country, inline apply button |

### Proxy Pool

| Command | Description |
|---------|-------------|
| `/list` | Paginated proxy list (10 per page) with inline connect buttons |
| `/list 2` | Go to page 2 |
| `/top` | Top 10 proxies ranked by score with inline connect buttons |
| `/add <proxy>` | Add a proxy manually |
| `/remove <n>` | Remove proxy by list number or `host:port` |
| `/check` | Run a full TCP check right now |
| `/clean` | Remove all confirmed-dead proxies |
| `/purge` | Remove proxies dead for longer than `stale_days` |
| `/export` | Download the pool as `proxies.json` |
| `/import` | Reply with a `.json` or `.txt` file to bulk-import |

**Accepted formats for `/add` and paste-detection:**
```
https://t.me/proxy?server=1.2.3.4&port=443&secret=abc123
tg://proxy?server=1.2.3.4&port=443&secret=abc123
socks5://user:pass@1.2.3.4:1080
http://1.2.3.4:8080
1.2.3.4:443:abc123secret
```

### Sources

| Command | Description |
|---------|-------------|
| `/channels` | List configured Telegram source channels |
| `/addchannel @Name` | Add a channel to scrape |
| `/removechannel @Name` | Remove a channel |
| `/sources` | List configured URL sources |
| `/addsource <url>` | Add an external URL source |
| `/removesource <url>` | Remove a URL source |
| `/fetch` | Scrape all sources immediately |

### Settings & System

| Command | Description |
|---------|-------------|
| `/settings` | Show all current config values |
| `/setinterval <min>` | Change background check interval (min: 5) |
| `/setfetch <min>` | Change background fetch interval (min: 10) |
| `/setmonitor <min>` | Change active-proxy monitor interval (min: 1) |
| `/reload` | Hot-reload `config.json` without restarting |
| `/daemon` | Show daemon process status and its active proxy |
| `/help` | Full command reference |

---

## How Proxy Scoring Works

Every proxy carries a combined score based on speed and reliability:

```
score = latency_ms + (1 − uptime_fraction) × 2000
```

- **Lower score = better**
- `latency_ms` — TCP round-trip time measured at each check
- `uptime_fraction` — fraction of checks that returned alive (range 0.0 – 1.0)
- New proxies (no history) get full uptime credit — no penalty for being new
- `/connect` always picks the lowest score; `/top` shows the top 10

**Example:**

| Proxy | Latency | Uptime | Score |
|-------|---------|--------|-------|
| A | 100 ms | 80% | 100 + 400 = **500** |
| B | 200 ms | 100% | 200 + 0 = **200** ← wins |
| C | 50 ms | 50% | 50 + 1000 = **1050** |

---

## Inline Connect Buttons

Every proxy displayed by the bot — in `/list`, `/top`, `/status`, `/connect`, and auto-rotation
alerts — includes a **"🔗 Apply Proxy in Telegram"** button.

Tapping the button on mobile opens Telegram's built-in proxy settings dialog with the server,
port, and secret already filled in. Just tap **Enable** — no copy-pasting required.

- MTProto proxies → `tg://proxy?server=…&port=…&secret=…`
- SOCKS5 / HTTP proxies → `tg://socks?server=…&port=…`

---

## Daemon Output

```
====================================================
  Telegram Auto-Proxy Daemon
  Press Ctrl+C to stop
====================================================

[10:00:01] ↺ Probing 83 alive proxies (1 previously verified)…
[10:00:07] 🔌 Active proxy → 37.1.212.47:443 [MTPROTO]
           Link : https://t.me/proxy?server=37.1.212.47&port=443&secret=ea93…

[10:00:37] ✅ Ping OK (112 ms)
[10:01:37] ❌ Ping failed — rotating…
[10:01:43] 🔌 Active proxy → 91.108.4.1:443 [MTPROTO]
```

After the first successful connection the working proxy is flagged as `mtproto_ok: true`.
On the next start the daemon skips untested proxies and goes straight to the verified ones —
reconnection takes ~6 seconds instead of scanning the whole pool.

---

## File Reference

| File | Description |
|------|-------------|
| `bot.py` | Main bot — all 27 commands and 4 background jobs |
| `auto_proxy_daemon.py` | Telethon-based MTProto auto-switcher daemon |
| `fetcher.py` | Channel + URL scraper (t.me/s/, GitHub raw, Geonode API) |
| `checker.py` | Concurrent TCP checker with DNS caching + latency measurement |
| `geoip.py` | Async geo-lookup via ip-api.com (no API key needed) |
| `proxy_manager.py` | Pool I/O, scoring, formatting, field migration helpers |
| `connection.py` | Active-proxy state, best-proxy picker, auto-rotate logic |
| `config.example.json` | Config template — copy to `config.json` |
| `daemon_config.example.json` | Daemon config template — copy to `daemon_config.json` |
| `requirements.txt` | Python dependencies |
| `run.sh` / `run.bat` | Bot launcher (uses venv if present) |
| `run_daemon.sh` / `run_daemon.bat` | Daemon launcher |
| `setup.sh` / `setup.bat` | One-time dependency installer |
| `systemd/` | systemd service units + install script |

**Files created at runtime (not in the repo):**

| File | Description |
|------|-------------|
| `config.json` | Your personal bot configuration |
| `daemon_config.json` | Your daemon credentials |
| `proxies.json` | Live proxy pool (shared by bot and daemon) |
| `connection.json` | Active proxy state |
| `daemon_session.session` | Saved Telethon login — do not delete |

---

## Troubleshooting

**"No alive proxies" after `/fetch`:**
Run `/check` after `/fetch`. Fetching only imports proxies — checking tests reachability.

**`/check` is slow:**
Each proxy gets a TCP attempt with up to 10-second timeout. 100+ proxies may take 2–3 minutes
total. Checks run concurrently (50 at once) so in practice it is usually much faster.

**Daemon takes a long time to start:**
It scans in batches of 5, each with a 6-second MTProto timeout. With ~80 alive proxies this
takes up to ~2 minutes worst case. On subsequent starts verified proxies are tried first (~6 s).

**Daemon login prompt appears every start:**
The `daemon_session.session` file is missing. Run `bash run_daemon.sh` and log in once. After
that the session is saved and you will never be prompted again.

**Bot sends "Telegram API error":**
Your bot token is wrong or the bot has been revoked. Get a new token from [@BotFather](https://t.me/BotFather).

**Flatpak Telegram Desktop does not open from `tg://` links:**
Register the URL handler once:
```bash
xdg-mime default org.telegram.desktop.desktop x-scheme-handler/tg
```

**Running on a headless server:**
The bot and daemon both work fine headlessly. The Telegram Desktop auto-apply step is silently
skipped when no display is available.

**`/purge` removed proxies unexpectedly:**
`/purge` only touches proxies dead for longer than `stale_days` (default 7). Raise `stale_days`
in `config.json` and run `/reload` if you want to be less aggressive.

---

## Requirements

- Python **3.11** or newer
- Linux (Ubuntu/Debian/Arch), Android (Termux), or Windows 7+
- A Telegram bot token from [@BotFather](https://t.me/BotFather)

**Daemon additionally requires:**
- A Telegram account (personal account, not a bot)
- `api_id` and `api_hash` from [my.telegram.org](https://my.telegram.org)

**For Telegram Desktop auto-confirmation (optional, Linux/X11 only):**
```bash
sudo apt install xdotool
```

---

## Dependencies

```
python-telegram-bot[job-queue]>=20.0   # Bot framework
aiohttp>=3.8.0                         # Async HTTP for fetching + geo-lookup
telethon>=1.34.0                       # MTProto client for the daemon
python-socks[asyncio]>=2.0.0          # SOCKS5 support for Telethon
PySocks>=1.7.1                         # Fallback SOCKS5
```

Install all at once:
```bash
pip install -r requirements.txt
```
