# run_worker.py
#
# NEXORA AI TRADER background worker. Runs three cooperating loops in one
# asyncio event loop:
#
#   1. Telegram listener  — polls Trial + VIP channels, stores new signals.
#   2. Trade engine       — picks up waiting signals and works them
#                           (deploy -> entry -> 3 trades -> TP1 -> undeploy).
#   3. Expiry checker     — flips trials/licenses to expired on schedule.
#
# Run this as a SEPARATE process from the web dashboard (own Render worker).

import asyncio
from dotenv import load_dotenv

load_dotenv()

from nexora import config
from nexora import symbol_resolver
from nexora.telegram import listener
from nexora.engine import engine
from nexora.expiry import check_expiries
from nexora.commands import process_pending, reset_interrupted_on_startup
from app.init_db import init_database

EXPIRY_INTERVAL = 60          # seconds between expiry checks
ENGINE_INTERVAL = 2           # seconds between engine ticks
COMMAND_INTERVAL = 3          # seconds between command-queue checks


async def _telegram_loop():
    if not listener.enabled():
        print("[Worker] Telegram not configured (set TELEGRAM_BOT_TOKEN + channel ids) — listener idle")
        while True:
            await asyncio.sleep(30)
    await listener.prime_offset()
    print("[Worker] Telegram listener running")
    warned_conflict = False
    while True:
        try:
            status = await listener.poll_once()
            if status == -409:
                if not warned_conflict:
                    print("[Worker] Telegram 409 CONFLICT — another process is polling "
                          "this SAME bot token (e.g. the Phase-1 EA, a second worker, or "
                          "a browser getUpdates). Use a DEDICATED bot for this platform or "
                          "stop the other poller. Backing off 15s.")
                    warned_conflict = True
                await asyncio.sleep(15)
                continue
            warned_conflict = False
            if status < 0:
                await asyncio.sleep(config.TELEGRAM_POLL_SECONDS)
                continue
            # small gap between long-poll cycles
            await asyncio.sleep(config.TELEGRAM_POLL_SECONDS)
        except Exception as e:
            print(f"[Worker] telegram loop error: {e}")
            await asyncio.sleep(config.TELEGRAM_POLL_SECONDS)


async def _engine_loop():
    print("[Worker] Trade engine running")
    rpc_pool_started = False
    while True:
        try:
            await engine.tick()
        except Exception as e:
            print(f"[Worker] engine loop error: {e}")
        await asyncio.sleep(ENGINE_INTERVAL)


async def _expiry_loop():
    print("[Worker] Expiry checker running")
    while True:
        try:
            n = await check_expiries()
            if n:
                print(f"[Worker] {n} client(s) expired")
        except Exception as e:
            print(f"[Worker] expiry loop error: {e}")
        await asyncio.sleep(EXPIRY_INTERVAL)


async def _command_loop():
    print("[Worker] Command processor running")
    reset_interrupted_on_startup()
    while True:
        try:
            n = await process_pending()
            if n:
                print(f"[Worker] processed {n} queued command(s)")
        except Exception as e:
            print(f"[Worker] command loop error: {e}")
        await asyncio.sleep(COMMAND_INTERVAL)


async def main():
    await init_database()
    symbol_resolver.prime_from_db()   # resolve once, remember forever across restarts
    print("[Worker] NEXORA worker starting…")
    await asyncio.gather(
        _telegram_loop(),
        _engine_loop(),
        _expiry_loop(),
        _command_loop(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("[Worker] stopped")
