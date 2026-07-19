# nexora/operations.py
#
# Manual dashboard actions that touch the broker: "Close Runner" and
# "Close All" for a client. Each follows the on-demand pattern:
# deploy -> act -> undeploy, so no account stays connected afterwards.

from nexora import config
from app.database import SessionLocal
from app.model import Client, TradeGroup, ActivityLog
from app.services.trading import trader
from nexora.deploy_manager import deploy_manager


def _log(db, action, message, client_id=None):
    db.add(ActivityLog(actor="admin", category="trade", action=action,
                       message=message, client_id=client_id))
    db.commit()


async def _with_connection(account_id, fn):
    """Acquire (deploy+connect) via the shared reference-counted manager, run
    fn(conn), then release. If a signal is also using the account it stays
    deployed until both are done — no undeploy out from under an active signal."""
    try:
        conn = await deploy_manager.acquire(account_id)
    except Exception as e:
        return {"success": False, "message": f"deploy/connect failed: {e}"}
    try:
        return await fn(conn)
    finally:
        await deploy_manager.release(account_id)


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
        # capture the just-closed trades' P/L while still connected
        try:
            from nexora import trade_history
            await trade_history.sync_client_history(conn, client_id)
        except Exception:
            pass
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
    """Close ONLY the runner — the single break-even position left after TP1.

    A runner only exists once TP1 has been reached (2 closed, 1 left running).
    If the 3 positions are still open (no TP1 yet) there is no runner, so this
    closes nothing and says so — it will NOT close the whole set (that's what
    Close All is for)."""
    db = SessionLocal()
    try:
        client = db.query(Client).get(client_id)
        if not client or not client.metaapi_account_id:
            return {"success": False, "message": "client not provisioned"}
        name = client.name
        acc_id = client.metaapi_account_id
        # Only groups that have reached TP1 have a runner. Collect their magics.
        runner_magics = {int(g.magic) for g in db.query(TradeGroup).filter(
            TradeGroup.client_id == client_id,
            TradeGroup.state == "tp1_done").all()}
    finally:
        db.close()

    if not runner_magics:
        return {"success": True, "closed": 0,
                "message": "No runner to close — TP1 has not been reached yet "
                           "(use Close All to close all 3 open positions)."}

    async def _do(conn):
        positions = await _nexora_positions(conn)
        closed = 0
        for p in positions:
            if int(p.get("magic", 0) or 0) in runner_magics:   # only runner positions
                r = await trader.close_position(conn, p.get("id"))
                if r.get("success"):
                    closed += 1
        try:
            from nexora import trade_history
            await trade_history.sync_client_history(conn, client_id)
        except Exception:
            pass
        return {"success": True, "closed": closed}

    result = await _with_connection(acc_id, _do)

    db = SessionLocal()
    try:
        for g in db.query(TradeGroup).filter(TradeGroup.client_id == client_id,
                                             TradeGroup.state == "tp1_done").all():
            g.state = "closed"
        db.commit()
        _log(db, "close_runner",
             f"{name}: Close Runner — {result.get('closed', 0)} runner position(s) closed",
             client_id=client_id)
    finally:
        db.close()
    return result


async def close_all_for_all() -> dict:
    """Emergency: Close All for every provisioned client (sequential)."""
    db = SessionLocal()
    try:
        ids = [c.id for c in db.query(Client)
               .filter(Client.metaapi_account_id.isnot(None)).all()]
    finally:
        db.close()
    total = 0
    for cid in ids:
        try:
            r = await close_all_for_client(cid)
            total += r.get("closed", 0) or 0
        except Exception as e:
            print(f"[Operations] bulk close-all error for {cid}: {e}")
    return {"success": True, "clients": len(ids), "closed": total}


async def close_runner_for_all() -> dict:
    """Close the runner for every provisioned client (sequential)."""
    db = SessionLocal()
    try:
        ids = [c.id for c in db.query(Client)
               .filter(Client.metaapi_account_id.isnot(None)).all()]
    finally:
        db.close()
    total = 0
    for cid in ids:
        try:
            r = await close_runner_for_client(cid)
            total += r.get("closed", 0) or 0
        except Exception as e:
            print(f"[Operations] bulk close-runner error for {cid}: {e}")
    return {"success": True, "clients": len(ids), "closed": total}


async def refresh_account(client_id: int) -> dict:
    """Client-requested balance/equity refresh: deploy → read → undeploy, via
    the shared reference-counted manager (safe alongside an active signal)."""
    from nexora import metrics
    db = SessionLocal()
    try:
        c = db.query(Client).get(client_id)
        if not c or not c.metaapi_account_id:
            return {"success": False, "message": "client not provisioned"}
        acc_id = c.metaapi_account_id
    finally:
        db.close()

    async def _do(conn):
        info = await conn.get_account_information()
        # opportunistic history refresh while connected
        try:
            from nexora import trade_history
            await trade_history.sync_client_history(conn, client_id)
        except Exception:
            pass
        return {"success": True, "balance": info.get("balance"),
                "equity": info.get("equity")}

    result = await _with_connection(acc_id, _do)
    if result.get("success"):
        metrics.record_metrics(client_id, result.get("balance"), result.get("equity"))
    return result


async def update_sl_for_signal(signal_id) -> dict:
    """Modify the stop-loss on every OPEN position of a signal, across all
    clients, to the signal's current SL value. Runs per-account: deploy → modify
    → undeploy (shared with the engine if it still holds the account).
    Groups already at TP1 (runner at break-even) are left untouched."""
    from app.model import Signal
    db = SessionLocal()
    try:
        sig = db.query(Signal).get(signal_id)
        if not sig:
            return {"success": False, "message": "signal not found"}
        new_sl = sig.sl
        # only groups still fully open (pre-TP1) — don't override break-even runners
        groups = (db.query(TradeGroup)
                  .filter(TradeGroup.signal_id == signal_id,
                          TradeGroup.state == "open").all())
        targets = []
        for g in groups:
            client = db.query(Client).get(g.client_id)
            if client and client.metaapi_account_id:
                targets.append((client.id, client.name, client.metaapi_account_id, g.magic))
    finally:
        db.close()

    if not targets:
        return {"success": True, "updated": 0, "clients": 0,
                "message": "no open positions to update"}

    total = 0
    for client_id, name, acc_id, magic in targets:
        async def _do(conn, magic=magic):
            try:
                positions = await conn.get_positions()
            except Exception as e:
                return {"success": False, "message": str(e)}
            updated = 0
            for p in positions:
                if int(p.get("magic", 0) or 0) != int(magic):
                    continue
                r = await trader.modify_position(conn, p.get("id"), sl=new_sl,
                                                 tp=p.get("takeProfit"))
                if r.get("success"):
                    updated += 1
            return {"success": True, "updated": updated}
        res = await _with_connection(acc_id, _do)
        if res.get("success"):
            total += res.get("updated", 0)

    db = SessionLocal()
    try:
        _log(db, "update_sl",
             f"Signal #{signal_id}: SL updated to {new_sl} on {total} position(s) "
             f"across {len(targets)} client(s)")
    finally:
        db.close()
    return {"success": True, "updated": total, "clients": len(targets), "new_sl": new_sl}


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
