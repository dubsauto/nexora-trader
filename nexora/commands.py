# nexora/commands.py
#
# The WORKER processes trade-action commands queued by the dashboard, so all
# MetaApi deploy/close/undeploy for a given account happens in ONE process.
# This avoids the web and worker fighting over an account's deploy state.

import json

from app.database import SessionLocal
from app.model import Command
from nexora import operations


async def _run(action: str, client_id):
    if action == "close_all":
        return await operations.close_all_for_client(client_id)
    if action == "close_runner":
        return await operations.close_runner_for_client(client_id)
    if action == "close_all_bulk":
        return await operations.close_all_for_all()
    if action == "close_runner_bulk":
        return await operations.close_runner_for_all()
    return {"success": False, "message": f"unknown action: {action}"}


async def process_pending() -> int:
    """Execute all pending commands. Returns how many were processed."""
    db = SessionLocal()
    try:
        cmds = db.query(Command).filter(Command.status == "pending").all()
        jobs = [(c.id, c.action, c.client_id) for c in cmds]
        for c in cmds:
            c.status = "running"
        if cmds:
            db.commit()
    finally:
        db.close()

    for cid, action, client_id in jobs:
        try:
            result = await _run(action, client_id)
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
