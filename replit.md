# Telegram OTP Bot

A Python Telegram bot that allocates phone numbers and forwards OTP codes to users.

## Run & Operate

- `python bot.py` — run the bot (Telegram Bot workflow)
- Required secrets: `TELEGRAM_BOT_TOKEN`, `CRAPI_KEY`, `OTP_FORWARD_CHAT_ID`

## Stack

- Python 3.11
- `python-telegram-bot[job-queue]` (polling)
- `httpx` for HTTP requests, `pyotp` for OTP
- SQLite (`bot_data.db`) for local state
- Dependencies declared in `pyproject.toml`

## Deployment (CURRENT — Railway via GitHub)

- **Code lives on GitHub:** `rijviold/telegram-bot` (public repo, `main` branch).
- **Hosted on Railway.app** (project "believable-creativity", service "telegram-bot") as an always-on worker for 24/7 uptime.
- **Auto-deploy:** Railway watches the GitHub `main` branch. Any new commit to `main` triggers an automatic redeploy. No manual step needed.
- **Railway Variables (set in Railway dashboard → Variables):** `TELEGRAM_BOT_TOKEN`, `CRAPI_KEY`, `OTP_FORWARD_CHAT_ID`, and optionally `ADMIN_IDS` (comma-separated Telegram user IDs).
- **Process config:** `railway.toml` (`[deploy] startCommand = "python bot.py"`, restart always) and `Procfile` (`worker: python bot.py`).
- **How to make a change:** edit the file (e.g. on github.com directly, or via an agent that pushes to GitHub) → commit to `main` → Railway auto-deploys in 1-2 minutes.

### Railway gotcha (learned the hard way)
- **Never add a Railway Volume** unless you intend to keep it permanently. Adding then deleting a volume corrupts the build pipeline ("secret ID missing for empty environment variable" + OCI runtime errors). The only fix is to delete the whole service and recreate it from the same GitHub repo + same env vars.

## Legacy deployment note (NOT in use)

- Was originally intended as a Replit Reserved VM, but the Replit Publish UI fails to load from Bangladesh (network/region issue), so the bot was moved to Railway instead.
- A minimal health server in `bot.py` listens on `PORT` (default 5000) and responds "OK" — harmless on Railway.

## Where things live

- `bot.py` — the entire bot (handlers, scheduler jobs, health server)
- `services.json` — service configuration used by the bot
- `bot_data.db` — SQLite database (local runtime state)

## Architecture decisions

- This was originally bootstrapped from a pnpm/JS monorepo template; that scaffolding (artifacts, lib, scripts, pnpm config) was removed since the project is a single Python bot.
- Uses polling (not webhooks), so the bot only makes outbound connections — no inbound web routes needed.

## User preferences

- Communicate in Bengali (Bangla).

## Gotchas

- `.replit` cannot be edited directly — use the appropriate tool/callback.
- The Publish UI requires opening over a VPN from some regions (Bangladesh) due to network/region issues loading the publish pane.
