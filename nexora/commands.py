# nexora/commands.py
#
# The WORKER processes trade-action commands queued by the dashboard, so all
# MetaApi deploy/close/undeploy for a given account happens in ONE process.
# This avoids the web and worker fighting over an account's deploy state.

import json
from datetime import datetime, timedelta

from app.database import SessionLocal
from app.model import Command
from nexora import operations

STALE_SECONDS = 300   # a command older than this is considered stale


def reset_interrupted_on_startup():
    """Any command left 'running' when the worker starts was interrupted by a
    restart/crash — mark it error so it can't block the dedup forever."""
    db = SessionLocal()
    try:
        n = 0
        for c in db.query(Command).filter(Command.status == "running").all():
            c.status = "error"
            c.result = "interrupted by worker restart"
            n += 1
        if n:
            db.commit()
            print(f"[Commands] reset {n} interrupted command(s) on startup")
    finally:
        db.close()


def _expire_stale(db):
    """Mark very old pending/running commands as error (self-healing)."""
    cutoff = datetime.utcnow() - timedelta(seconds=STALE_SECONDS)
    stale = db.query(Command).filter(
        Command.status.in_(["pending", "running"]),
        Command.created_at < cutoff).all()
    for c in stale:
        c.status = "error"
        c.result = "stale — expired before completion"
    if stale:
        db.commit()


async def _run(action: str, client_id, payload=None):
    payload = payload or {}
    if action == "close_all":
        return await operations.close_all_for_client(client_id)
    if action == "close_runner":
        return await operations.close_runner_for_client(client_id)
    if action == "close_all_bulk":
        return await operations.close_all_for_all()
    if action == "close_runner_bulk":
        return await operations.close_runner_for_all()
    if action == "refresh_account":
        return await operations.refresh_account(client_id)
    if action == "update_sl":
        return await operations.update_sl_for_signal(payload.get("signal_id"))
    return {"success": False, "message": f"unknown action: {action}"}


async def process_pending() -> int:
    """Execute all pending commands. Returns how many were processed."""
    db = SessionLocal()
    try:
        _expire_stale(db)
        cmds = db.query(Command).filter(Command.status == "pending").all()
        jobs = [(c.id, c.action, c.client_id, c.payload) for c in cmds]
        for c in cmds:
            c.status = "running"
        if cmds:
            db.commit()
    finally:
        db.close()

    for cid, action, client_id, payload in jobs:
        try:
            result = await _run(action, client_id, payload)
        except Exception as e:
            result = {"success": False, "message": str(e)}

        db = SessionLocal()
        try:
            c = db.query(Command).get(cid)
            if c:
                c.status = "done" if result.get("success") else "error"
                c.result = json.dumps(result)[:500]
                db.commit()
        finally:
            db.close()

    return len(jobs)
