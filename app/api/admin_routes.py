# app/api/admin_routes.py
#
# NEXORA admin API — everything the dashboard needs:
#   clients CRUD, start trial, activate license, move channel, lot/risk,
#   trading on/off, close runner / close all, signals feed, activity log.

from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import desc

from app.database import get_db
from app.auth import get_current_user
from app.model import Client, Signal, TradeGroup, ActivityLog
from app.services.account_management import account_manager
from nexora import config
from nexora import operations

router = APIRouter(prefix="/api", tags=["Admin"])


# ─────────────────────────────────────────────────────────────
# helpers
# ─────────────────────────────────────────────────────────────
def _actor(user) -> str:
    return (user or {}).get("username", "admin")


def _log(db, actor, action, message, client_id=None):
    db.add(ActivityLog(actor=actor, category="client", action=action,
                       message=message, client_id=client_id))


def _client_dict(c: Client) -> dict:
    return {
        "id": c.id,
        "name": c.name,
        "note": c.note,
        "login": c.login,
        "server": c.server,
        "status": c.status,
        "channel": c.channel,
        "trading_enabled": c.trading_enabled,
        "lot_size": c.lot_size,
        "risk_profile": c.risk_profile,
        "deposit": c.deposit,
        "deploy_state": c.deploy_state,
        "metaapi_account_id": c.metaapi_account_id,
        "provisioned": bool(c.metaapi_account_id),
        "trial_expires_at": c.trial_expires_at.isoformat() if c.trial_expires_at else None,
        "license_expires_at": c.license_expires_at.isoformat() if c.license_expires_at else None,
        "connection_note": c.connection_note,
        "created_at": c.created_at.isoformat() if c.created_at else None,
    }


# ─────────────────────────────────────────────────────────────
# CONFIG / STATUS
# ─────────────────────────────────────────────────────────────
@router.get("/config")
async def get_config(user=Depends(get_current_user)):
    return config.as_dict()


@router.get("/stats")
async def stats(db: Session = Depends(get_db), user=Depends(get_current_user)):
    clients = db.query(Client).all()
    return {
        "total": len(clients),
        "trial": sum(1 for c in clients if c.status == "trial"),
        "active": sum(1 for c in clients if c.status == "active"),
        "expired": sum(1 for c in clients if c.status == "expired"),
        "inactive": sum(1 for c in clients if c.status == "inactive"),
        "trading_on": sum(1 for c in clients
                          if c.trading_enabled and c.status in ("trial", "active")),
    }


# ─────────────────────────────────────────────────────────────
# CLIENTS — list / create / update / delete
# ─────────────────────────────────────────────────────────────
@router.get("/clients")
async def list_clients(db: Session = Depends(get_db), user=Depends(get_current_user)):
    clients = db.query(Client).order_by(desc(Client.created_at)).all()
    return [_client_dict(c) for c in clients]


@router.get("/clients/{client_id}")
async def client_detail(client_id: int, db: Session = Depends(get_db),
                        user=Depends(get_current_user)):
    c = db.query(Client).get(client_id)
    if not c:
        raise HTTPException(404, "Client not found")

    d = _client_dict(c)

    # last signal on this client's channel
    last_sig = (db.query(Signal)
                .filter(Signal.channel == c.channel)
                .order_by(desc(Signal.created_at)).first())
    d["last_signal"] = {
        "direction": last_sig.direction,
        "entry_low": last_sig.entry_low,
        "entry_high": last_sig.entry_high,
        "sl": last_sig.sl,
        "tp1": last_sig.tp1,
        "state": last_sig.state,
        "posted_at": last_sig.posted_at.isoformat() if last_sig.posted_at else None,
    } if last_sig else None

    # this client's most recent trade group
    last_group = (db.query(TradeGroup)
                  .filter(TradeGroup.client_id == c.id)
                  .order_by(desc(TradeGroup.created_at)).first())
    d["last_trade"] = {
        "state": last_group.state,
        "lot": last_group.lot,
        "opened_at": last_group.opened_at.isoformat() if last_group.opened_at else None,
        "tp1_at": last_group.tp1_at.isoformat() if last_group.tp1_at else None,
    } if last_group else None

    d["risk_multiplier"] = config.risk_multiplier(c.risk_profile)
    d["effective_lot"] = c.effective_lot(config.risk_multiplier(c.risk_profile))
    return d


@router.post("/clients")
async def create_client(data: dict, db: Session = Depends(get_db), user=Depends(get_current_user)):
    for f in ["name", "login", "password", "server"]:
        if not data.get(f):
            raise HTTPException(400, f"Missing field: {f}")

    if db.query(Client).filter(Client.login == str(data["login"])).first():
        raise HTTPException(400, "A client with this MT5 login already exists")

    c = Client(
        name=data["name"],
        note=data.get("note"),
        login=str(data["login"]),
        password=data["password"],
        server=data["server"],
        status="inactive",
        channel="trial",
        trading_enabled=True,
        lot_size=float(data.get("lot_size", 0.01)),
        risk_profile=data.get("risk_profile", "balanced"),
        deposit=float(data.get("deposit", 0) or 0),
    )
    db.add(c)
    db.flush()
    c.magic = config.MAGIC_BASE + c.id
    db.commit()

    # provision on MetaApi (create the account so it can be deployed later)
    prov = await account_manager.add_account(
        name=f"NEXORA-{c.id}-{c.name}",
        server=c.server,
        login=str(c.login),
        password=c.password,
        manual_trades=False,
        use_dedicated_ip=config.USE_DEDICATED_IP,
        magic=c.magic,
    )
    if prov.get("success"):
        c.metaapi_account_id = prov["account_id"]
        c.connection_note = "provisioned"
        # On-demand cost model: keep the account UNDEPLOYED until a signal needs
        # it. MetaApi auto-deploys a freshly created account, so undeploy it now.
        try:
            await account_manager.undeploy(c.metaapi_account_id)
            c.deploy_state = "undeployed"
        except Exception as e:
            print(f"[Admin] undeploy after provisioning failed: {e}")
    else:
        c.connection_note = f"provision failed: {prov.get('message')}"
    db.commit()

    _log(db, _actor(user), "create", f"Created client {c.name} (login {c.login})", c.id)
    db.commit()
    return _client_dict(c)


@router.put("/clients/{client_id}")
async def update_client(client_id: int, data: dict, db: Session = Depends(get_db),
                        user=Depends(get_current_user)):
    c = db.query(Client).get(client_id)
    if not c:
        raise HTTPException(404, "Client not found")

    for field in ["name", "note", "server"]:
        if field in data and data[field] is not None:
            setattr(c, field, data[field])
    if "lot_size" in data:
        c.lot_size = float(data["lot_size"])
    if "deposit" in data:
        c.deposit = float(data["deposit"] or 0)
    if "risk_profile" in data and data["risk_profile"] in config.RISK_MULTIPLIERS:
        c.risk_profile = data["risk_profile"]
    if data.get("password"):
        c.password = data["password"]

    db.commit()
    _log(db, _actor(user), "update", f"Updated client {c.name}", c.id)
    db.commit()
    return _client_dict(c)


@router.delete("/clients/{client_id}")
async def delete_client(client_id: int, db: Session = Depends(get_db),
                        user=Depends(get_current_user)):
    c = db.query(Client).get(client_id)
    if not c:
        raise HTTPException(404, "Client not found")
    name, acc_id = c.name, c.metaapi_account_id
    db.delete(c)
    db.commit()
    if acc_id:
        try:
            await account_manager.remove_account(acc_id)
        except Exception as e:
            print(f"[Admin] remove_account error: {e}")
    _log(db, _actor(user), "delete", f"Deleted client {name}", client_id)
    db.commit()
    return {"success": True}


# ─────────────────────────────────────────────────────────────
# LIFECYCLE ACTIONS
# ─────────────────────────────────────────────────────────────
@router.post("/clients/{client_id}/start-trial")
async def start_trial(client_id: int, db: Session = Depends(get_db),
                      user=Depends(get_current_user)):
    c = db.query(Client).get(client_id)
    if not c:
        raise HTTPException(404, "Client not found")
    now = datetime.utcnow()
    c.status = "trial"
    c.channel = "trial"
    c.trading_enabled = True
    c.trial_started_at = now
    c.trial_expires_at = now + timedelta(days=config.TRIAL_DAYS)
    db.commit()
    _log(db, _actor(user), "start_trial",
         f"{c.name}: {config.TRIAL_DAYS}-day trial started (expires {c.trial_expires_at})", c.id)
    db.commit()
    return _client_dict(c)


@router.post("/clients/{client_id}/activate")
async def activate_license(client_id: int, data: dict = None, db: Session = Depends(get_db),
                           user=Depends(get_current_user)):
    c = db.query(Client).get(client_id)
    if not c:
        raise HTTPException(404, "Client not found")
    data = data or {}
    days = int(data.get("days", config.DEFAULT_LICENSE_DAYS))
    now = datetime.utcnow()
    c.status = "active"
    c.channel = "vip"                 # promote to VIP channel
    c.trading_enabled = True
    c.license_expires_at = now + timedelta(days=days)
    db.commit()
    _log(db, _actor(user), "activate",
         f"{c.name}: full license activated for {days} days, moved to VIP", c.id)
    db.commit()
    return _client_dict(c)


@router.post("/clients/{client_id}/deactivate")
async def deactivate(client_id: int, db: Session = Depends(get_db),
                     user=Depends(get_current_user)):
    c = db.query(Client).get(client_id)
    if not c:
        raise HTTPException(404, "Client not found")
    c.status = "inactive"
    db.commit()
    _log(db, _actor(user), "deactivate", f"{c.name}: deactivated", c.id)
    db.commit()
    return _client_dict(c)


@router.post("/clients/{client_id}/channel")
async def set_channel(client_id: int, data: dict, db: Session = Depends(get_db),
                      user=Depends(get_current_user)):
    c = db.query(Client).get(client_id)
    if not c:
        raise HTTPException(404, "Client not found")
    channel = data.get("channel")
    if channel not in ("trial", "vip"):
        raise HTTPException(400, "channel must be 'trial' or 'vip'")
    c.channel = channel
    db.commit()
    _log(db, _actor(user), "channel", f"{c.name}: moved to {channel.upper()} channel", c.id)
    db.commit()
    return _client_dict(c)


@router.post("/clients/{client_id}/trading")
async def set_trading(client_id: int, data: dict, db: Session = Depends(get_db),
                      user=Depends(get_current_user)):
    c = db.query(Client).get(client_id)
    if not c:
        raise HTTPException(404, "Client not found")
    c.trading_enabled = bool(data.get("enabled", True))
    db.commit()
    _log(db, _actor(user), "trading",
         f"{c.name}: trading {'ON' if c.trading_enabled else 'OFF'}", c.id)
    db.commit()
    return _client_dict(c)


# ─────────────────────────────────────────────────────────────
# TRADE CONTROLS (deploy → act → undeploy)
# ─────────────────────────────────────────────────────────────
@router.post("/clients/{client_id}/close-runner")
async def close_runner(client_id: int, user=Depends(get_current_user)):
    return await operations.close_runner_for_client(client_id)


@router.post("/clients/{client_id}/close-all")
async def close_all(client_id: int, user=Depends(get_current_user)):
    return await operations.close_all_for_client(client_id)


# ─────────────────────────────────────────────────────────────
# QUICK ACTIONS (bulk — apply to all clients)
# ─────────────────────────────────────────────────────────────
@router.post("/bulk/trading")
async def bulk_trading(data: dict, db: Session = Depends(get_db),
                       user=Depends(get_current_user)):
    enabled = bool(data.get("enabled", True))
    clients = db.query(Client).all()
    n = 0
    for c in clients:
        if c.trading_enabled != enabled:
            c.trading_enabled = enabled
            n += 1
    db.commit()
    _log(db, _actor(user), "bulk_trading",
         f"Trading {'ON' if enabled else 'OFF'} for all clients ({n} changed)")
    db.commit()
    return {"success": True, "changed": n, "enabled": enabled}


@router.post("/bulk/close-runner")
async def bulk_close_runner(user=Depends(get_current_user)):
    return await operations.close_runner_for_all()


@router.post("/bulk/close-all")
async def bulk_close_all(user=Depends(get_current_user)):
    return await operations.close_all_for_all()


# ─────────────────────────────────────────────────────────────
# SIGNALS + ACTIVITY
# ─────────────────────────────────────────────────────────────
@router.get("/signals")
async def list_signals(limit: int = 50, db: Session = Depends(get_db),
                       user=Depends(get_current_user)):
    rows = db.query(Signal).order_by(desc(Signal.created_at)).limit(limit).all()
    return [{
        "id": s.id,
        "channel": s.channel,
        "direction": s.direction,
        "entry_low": s.entry_low,
        "entry_high": s.entry_high,
        "sl": s.sl,
        "tp1": s.tp1,
        "state": s.state,
        "posted_at": s.posted_at.isoformat() if s.posted_at else None,
        "created_at": s.created_at.isoformat() if s.created_at else None,
        "groups": db.query(TradeGroup).filter(TradeGroup.signal_id == s.id).count(),
    } for s in rows]


@router.get("/activity")
async def list_activity(limit: int = 100, db: Session = Depends(get_db),
                        user=Depends(get_current_user)):
    rows = db.query(ActivityLog).order_by(desc(ActivityLog.ts)).limit(limit).all()
    return [{
        "id": r.id,
        "ts": r.ts.isoformat() if r.ts else None,
        "actor": r.actor,
        "category": r.category,
        "action": r.action,
        "message": r.message,
        "client_id": r.client_id,
        "signal_id": r.signal_id,
    } for r in rows]
