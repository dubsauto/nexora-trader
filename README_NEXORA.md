# NEXORA AI TRADER — Phase 2 Platform

Centralized MetaApi copy-trading platform + admin dashboard. The server reads
your Telegram signal channels (Trial + VIP) and copies GOLD signals to every
eligible client's MT5 account via MetaApi, using on-demand deploy/undeploy to
keep costs low.

## Architecture

```
Telegram (Trial + VIP channels)
        │  getUpdates (one bot, admin of both)
        ▼
run_worker.py  ──►  nexora/telegram.py   (parse + store signals)
                    nexora/engine.py      (deploy → entry zone → 3 trades → TP1 → undeploy)
                    nexora/expiry.py       (auto-expire trials/licenses)
        │
        ▼  MetaApi (on-demand)
   Client MT5 accounts

app/server.py (FastAPI)  ──►  static/ dashboard  (admin login, clients, signals, activity)
   shared PostgreSQL database
```

Two processes share one database:
- **Web** (`uvicorn app.server:app`) — the admin dashboard + API.
- **Worker** (`python run_worker.py`) — the listener + trade engine + expiry.

## Key modules

| Path | Purpose |
|------|---------|
| `nexora/config.py` | All settings from `.env`. |
| `nexora/signal_parser.py` | Parses BUY/SELL signals (port of the Phase-1 EA). |
| `nexora/telegram.py` | Polls both channels, dedups, stores `Signal` rows. |
| `nexora/engine.py` | Per-signal lifecycle: eligible clients → deploy → entry-zone fill → 3 positions + SL → TP1 (close 2, break-even 1) → undeploy. |
| `nexora/expiry.py` | Flips trials/licenses to `expired` on schedule. |
| `nexora/operations.py` | Close Runner / Close All (deploy → close → undeploy). |
| `app/model.py` | `AdminUser`, `Client`, `Signal`, `TradeGroup`, `ActivityLog`, `Setting`. |
| `app/api/admin_routes.py` | Dashboard API. |
| `app/services/account_management.py` | MetaApi add/deploy/undeploy/remove (reused). |
| `app/services/trading.py` | Order place/close/modify on a connection (reused). |
| `hedgebridge/rpc_pool.py` | Robust MetaApi RPC connection pool (reused). |

## Local run

1. `pip install -r requirements.txt`
2. Fill `.env` (MetaApi `ACCESS_TOKEN`, `DATABASE_URL`, Telegram token + channel
   ids, admin password). For a quick local DB you can set
   `DATABASE_URL=sqlite:///./nexora.db`.
3. Terminal 1 (dashboard): `uvicorn app.server:app --reload`
4. Terminal 2 (worker): `python run_worker.py`
5. Open http://localhost:8000 and log in with `ADMIN_USERNAME` / `ADMIN_PASSWORD`.

## Deploy on Render

Use `render.yaml` (web + worker + Postgres). After the first deploy, set the
secret env vars on BOTH services in the Render dashboard:
`ACCESS_TOKEN`, `SECRET_KEY`, `ADMIN_PASSWORD`, `TELEGRAM_BOT_TOKEN`,
`TRIAL_CHANNEL_ID`, `VIP_CHANNEL_ID`.

## Onboarding a client (admin flow)

1. Client sends MT5 **login + server + password**.
2. **Add Client** on the dashboard → a MetaApi account is auto-provisioned.
3. **Start Trial** → 3-day trial on the Trial channel.
4. After the trial, **Activate** (sets a license + moves to VIP), or let it
   auto-expire.

## Notes / open items to confirm with the client

- **Risk multipliers** default to 0.5 / 1 / 2 (`RISK_*` in `.env`).
- **On expiry**: default blocks only NEW trades; set
  `CLOSE_POSITIONS_ON_EXPIRY=true` to force-close.
- **Symbol**: `TRADE_SYMBOL=XAUUSD` — adjust if brokers use a suffix (e.g. `XAUUSD.m`).
- End-to-end live trading requires a funded/demo MT5 account connected via
  MetaApi and the bot added as admin to both channels.
