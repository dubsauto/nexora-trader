# nexora/operations.py
#
# Manual dashboard actions that touch the broker: "Close Runner" and
# "Close All" for a client. Each follows the on-demand pattern:
# deploy -> act -> undeploy, so no account stays connected afterwards.

from nexora import config
from app.database import SessionLocal
from app.model import Client, TradeGroup, ActivityLog
from app.services.account_management import account_manager
from app.services.trading import trader
from hedgebridge.rpc_pool import rpc_pool


def _log(db, action, message, client_id=None):
    db.add(ActivityLog(actor="admin", category="trade", action=action,
                       message=message, client_id=client_id))
    db.commit()


async def _with_connection(account_id, fn):
    """Deploy, run fn(conn), then undeploy — always cleans up."""
    dep = await account_manager.deploy_and_wait(account_id)
    if not dep.get("success"):
        return {"success": False, "message": f"deploy failed: {dep.get('message')}"}
    try:
        conn = await rpc_pool.get_connection(account_id, force=True)
        return await fn(conn)
    finally:
        try:
            await rpc_pool.invalidate(account_id)
        except Exception:
            pass
        await account_manager.undeploy(account_id)


async def _nexora_positions(conn):
    try:
        positions = await conn.get_positions()
    except Exception:
        return []
    prefix = config.ORDER_COMMENT_PREFIX
    out = []
    for p in positions:
        comment = (p.get("comment") or "")
        magic = int(p.get("magic", 0) or 0)
        if comment.startswith(prefix) or magic >= config.MAGIC_BASE:
            out.append(p)
    return out


async def close_all_for_client(client_id: int) -> dict:
    db = SessionLocal()
    try:
        client = db.query(Client).get(client_id)
        if not client or not client.metaapi_account_id:
            return {"success": False, "message": "client not provisioned"}
        name = client.name
        acc_id = client.metaapi_account_id
    finally:
        db.close()

    async def _do(conn):
        positions = await _nexora_positions(conn)
        closed = 0
        for p in positions:
            r = await trader.close_position(conn, p.get("id"))
            if r.get("success"):
                closed += 1
        return {"success": True, "closed": closed}

    result = await _with_connection(acc_id, _do)

    db = SessionLocal()
    try:
        # mark this client's groups closed
        for g in db.query(TradeGroup).filter(TradeGroup.client_id == client_id,
                                             TradeGroup.state != "closed").all():
            g.state = "closed"
        db.commit()
        _log(db, "close_all",
             f"{name}: Close All — {result.get('closed', 0)} position(s) closed",
             client_id=client_id)
    finally:
        db.close()
    return result


async def close_runner_for_client(client_id: int) -> dict:
    """Close the remaining runner position(s) for a client."""
    db = SessionLocal()
    try:
        client = db.query(Client).get(client_id)
        if not client or not client.metaapi_account_id:
            return {"success": False, "message": "client not provisioned"}
        name = client.name
        acc_id = client.metaapi_account_id
    finally:
        db.close()

    async def _do(conn):
        positions = await _nexora_positions(conn)
        closed = 0
        for p in positions:
            r = await trader.close_position(conn, p.get("id"))
            if r.get("success"):
                closed += 1
        return {"success": True, "closed": closed}

    result = await _with_connection(acc_id, _do)

    db = SessionLocal()
    try:
        for g in db.query(TradeGroup).filter(TradeGroup.client_id == client_id,
                                             TradeGroup.state == "tp1_done").all():
            g.state = "closed"
        db.commit()
        _log(db, "close_runner",
             f"{name}: Close Runner — {result.get('closed', 0)} position(s) closed",
             client_id=client_id)
    finally:
        db.close()
    return result


async def close_all_for_expired():
    """Force-close positions for clients that just expired (opt-in)."""
    db = SessionLocal()
    try:
        ids = [c.id for c in db.query(Client).filter(Client.status == "expired").all()]
    finally:
        db.close()
    for cid in ids:
        try:
            await close_all_for_client(cid)
        except Exception as e:
            print(f"[Operations] expiry close error for {cid}: {e}")
