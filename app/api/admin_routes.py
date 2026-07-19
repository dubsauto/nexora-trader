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
from app.auth import get_current_user, hash_password
from app.model import AdminUser
from app.model import (Client, Signal, TradeGroup, ActivityLog, Command,
                       Symbol, Setting, Notification, Ticket, TicketMessage)
from nexora import emailer
from app.services.account_management import account_manager
from nexora import config

router = APIRouter(prefix="/api", tags=["Admin"])


# ─────────────────────────────────────────────────────────────
# helpers
# ─────────────────────────────────────────────────────────────
def _actor(user) -> str:
    return (user or {}).get("username", "admin")


def _clamp_positions(value):
    """Positions-per-signal: 1-10, or None to fall back to the global default."""
    if value in (None, "", 0):
        return None
    try:
        return max(1, min(10, int(value)))
    except (TypeError, ValueError):
        return None


def _log(db, actor, action, message, client_id=None):
    db.add(ActivityLog(actor=actor, category="client", action=action,
                       message=message, client_id=client_id))


def _client_dict(c: Client) -> dict:
    return {
        "id": c.id,
        "name": c.name,
        "note": c.note,
        "email": c.email,
        "phone": c.phone,
        "approval_status": c.approval_status or "approved",
        "login": c.login,
        "server": c.server,
        "status": c.status,
        "channel": c.channel,
        "trading_enabled": c.trading_enabled,
        "lot_size": c.lot_size,
        "risk_profile": c.risk_profile,
        "positions_per_signal": c.positions_per_signal,
        "effective_positions": int(c.positions_per_signal or config.POSITIONS_PER_SIGNAL),
        "deposit": c.deposit,
        "symbol_overrides": c.symbol_overrides or {},
        "resolved_symbols": c.resolved_symbols or {},
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


# ─────────────────────────────────────────────────────────────
# ACCOUNT — the logged-in user edits their OWN login only
# ─────────────────────────────────────────────────────────────
@router.get("/account")
async def get_account(db: Session = Depends(get_db), user=Depends(get_current_user)):
    u = db.query(AdminUser).get(user.get("user_id"))
    if not u:
        raise HTTPException(404, "Account not found")
    return {"username": u.username, "email": u.email, "role": u.role}


@router.put("/account")
async def update_account(data: dict, db: Session = Depends(get_db),
                         user=Depends(get_current_user)):
    u = db.query(AdminUser).get(user.get("user_id"))
    if not u:
        raise HTTPException(404, "Account not found")

    new_username = (data.get("username") or "").strip()
    if new_username and new_username != u.username:
        clash = db.query(AdminUser).filter(AdminUser.username == new_username).first()
        if clash:
            raise HTTPException(400, "That username is already taken")
        u.username = new_username

    if "email" in data:
        u.email = (data.get("email") or "").strip() or None

    if data.get("password"):
        if len(data["password"]) < 6:
            raise HTTPException(400, "Password must be at least 6 characters")
        u.password_hash = hash_password(data["password"])

    db.commit()
    return {"success": True, "username": u.username, "email": u.email}


@router.get("/listener")
async def listener_health(db: Session = Depends(get_db), user=Depends(get_current_user)):
    """Telegram listener heartbeat for the dashboard health badge."""
    enabled = bool(config.TELEGRAM_BOT_TOKEN and
                   (config.TRIAL_CHANNEL_ID or config.VIP_CHANNEL_ID))
    hb = db.query(Setting).filter(Setting.key == "listener_heartbeat").first()
    st = db.query(Setting).filter(Setting.key == "listener_status").first()
    heartbeat = hb.value if hb else None
    status = st.value if st else None

    age = None
    healthy = False
    if heartbeat:
        try:
            age = (datetime.utcnow() - datetime.fromisoformat(heartbeat)).total_seconds()
            # long-poll can be ~28s; allow generous margin before "stale"
            healthy = age is not None and age < 90 and status != "conflict"
        except Exception:
            pass

    return {"enabled": enabled, "heartbeat": heartbeat, "status": status,
            "age_seconds": round(age) if age is not None else None,
            "healthy": healthy}


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
        "pending_approval": sum(1 for c in clients
                                if (c.approval_status or "approved") == "pending"),
        "open_tickets": db.query(Ticket).filter(Ticket.status == "open").count(),
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
        positions_per_signal=_clamp_positions(data.get("positions_per_signal")),
        deposit=float(data.get("deposit", 0) or 0),
        symbol_overrides=data.get("symbol_overrides") or {},
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

    old_server = c.server
    for field in ["name", "note", "server"]:
        if field in data and data[field] is not None:
            setattr(c, field, data[field])
    # Broker changed → the remembered broker-symbol mappings may be wrong; clear
    # them so they re-detect (takes effect after the next worker cycle/restart).
    if "server" in data and data["server"] and data["server"] != old_server:
        c.resolved_symbols = {}
    if "lot_size" in data:
        c.lot_size = float(data["lot_size"])
    if "positions_per_signal" in data:
        c.positions_per_signal = _clamp_positions(data["positions_per_signal"])
    if "deposit" in data:
        c.deposit = float(data["deposit"] or 0)
    if "symbol_overrides" in data:
        c.symbol_overrides = data["symbol_overrides"] or {}
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
@router.post("/clients/{client_id}/approve")
async def approve_client(client_id: int, db: Session = Depends(get_db),
                         user=Depends(get_current_user)):
    c = db.query(Client).get(client_id)
    if not c:
        raise HTTPException(404, "Client not found")
    if (c.approval_status or "approved") != "pending":
        return _client_dict(c)

    c.approval_status = "approved"
    db.commit()

    # Provision the MetaApi account now (signups are not provisioned until approved)
    if not c.metaapi_account_id:
        prov = await account_manager.add_account(
            name=f"NEXORA-{c.id}-{c.name}", server=c.server,
            login=str(c.login), password=c.password,
            manual_trades=False, use_dedicated_ip=config.USE_DEDICATED_IP,
            magic=c.magic or 0)
        if prov.get("success"):
            c.metaapi_account_id = prov["account_id"]
            c.connection_note = "provisioned"
            try:
                await account_manager.undeploy(c.metaapi_account_id)
                c.deploy_state = "undeployed"
            except Exception as e:
                print(f"[Admin] undeploy after approval failed: {e}")
        else:
            c.connection_note = f"provision failed: {prov.get('message')}"
        db.commit()

    # In-portal notification + approval email
    db.add(Notification(client_id=c.id, title="Account approved 🎉",
                        body="Your account has been approved. Your dashboard is now unlocked."))
    _log(db, _actor(user), "approve", f"{c.name}: signup approved", c.id)
    db.commit()
    if c.email:
        first = (c.name or "").split(" ")[0] or "there"
        await emailer.send_approval_email(c.email, first)
    return _client_dict(c)


@router.post("/clients/{client_id}/decline")
async def decline_client(client_id: int, db: Session = Depends(get_db),
                         user=Depends(get_current_user)):
    """Decline a signup: the account is destroyed entirely — the client can no
    longer log in and must sign up again."""
    c = db.query(Client).get(client_id)
    if not c:
        raise HTTPException(404, "Client not found")
    if (c.approval_status or "approved") != "pending":
        raise HTTPException(400, "Only pending signups can be declined")
    name, acc_id = c.name, c.metaapi_account_id
    db.delete(c)
    db.commit()
    if acc_id:
        try:
            await account_manager.remove_account(acc_id)
        except Exception as e:
            print(f"[Admin] remove_account on decline error: {e}")
    _log(db, _actor(user), "decline", f"{name}: signup declined and removed", client_id)
    db.commit()
    return {"success": True}


@router.post("/clients/{client_id}/start-trial")
async def start_trial(client_id: int, db: Session = Depends(get_db),
                      user=Depends(get_current_user)):
    c = db.query(Client).get(client_id)
    if not c:
        raise HTTPException(404, "Client not found")
    if (c.approval_status or "approved") == "pending":
        raise HTTPException(400, "Approve this client first")
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
    if (c.approval_status or "approved") == "pending":
        raise HTTPException(400, "Approve this client first")
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
# TRADE CONTROLS — QUEUED to the worker (single owner of MetaApi)
# ─────────────────────────────────────────────────────────────
def _queue(db, action, actor, client_id=None):
    # Dedup: if an identical command is already pending/running, don't stack
    # another. Collapses accidental double/triple clicks into one and avoids
    # pointless deploy/undeploy churn on the same account. Only RECENT commands
    # count — a stale one (worker died mid-run) must never block forever.
    cutoff = datetime.utcnow() - timedelta(seconds=180)
    existing = db.query(Command).filter(
        Command.action == action,
        Command.client_id == client_id,
        Command.status.in_(["pending", "running"]),
        Command.created_at >= cutoff).first()
    if existing:
        return {"queued": False, "duplicate": True, "command_id": existing.id,
                "message": "Already queued — please wait for it to finish."}

    cmd = Command(action=action, client_id=client_id, requested_by=actor, status="pending")
    db.add(cmd)
    db.commit()
    return {"queued": True, "command_id": cmd.id,
            "message": "Queued — the worker will process it in a few seconds."}


@router.post("/clients/{client_id}/close-runner")
async def close_runner(client_id: int, db: Session = Depends(get_db),
                       user=Depends(get_current_user)):
    if not db.query(Client).get(client_id):
        raise HTTPException(404, "Client not found")
    # A runner only exists after TP1 (group in tp1_done). Check before queuing.
    has_runner = db.query(TradeGroup).filter(
        TradeGroup.client_id == client_id,
        TradeGroup.state == "tp1_done").first()
    if not has_runner:
        return {"queued": False, "no_runner": True,
                "message": "No runner to close yet — TP1 has not been reached. "
                           "Use Close All to close the open trades."}
    return _queue(db, "close_runner", _actor(user), client_id)


@router.post("/clients/{client_id}/close-all")
async def close_all(client_id: int, db: Session = Depends(get_db),
                    user=Depends(get_current_user)):
    if not db.query(Client).get(client_id):
        raise HTTPException(404, "Client not found")
    return _queue(db, "close_all", _actor(user), client_id)


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
async def bulk_close_runner(db: Session = Depends(get_db), user=Depends(get_current_user)):
    has_runner = db.query(TradeGroup).filter(TradeGroup.state == "tp1_done").first()
    if not has_runner:
        return {"queued": False, "no_runner": True,
                "message": "No runners to close yet — no client has reached TP1."}
    return _queue(db, "close_runner_bulk", _actor(user))


@router.post("/bulk/close-all")
async def bulk_close_all(db: Session = Depends(get_db), user=Depends(get_current_user)):
    return _queue(db, "close_all_bulk", _actor(user))


# ─────────────────────────────────────────────────────────────
# SIGNALS + ACTIVITY
# ─────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────
# SYMBOLS (tradable instruments)
# ─────────────────────────────────────────────────────────────
def _symbol_dict(s: Symbol) -> dict:
    return {"id": s.id, "name": s.name, "aliases": s.alias_list(),
            "aliases_raw": s.aliases or "", "enabled": s.enabled}


@router.get("/symbols")
async def list_symbols(db: Session = Depends(get_db), user=Depends(get_current_user)):
    return [_symbol_dict(s) for s in db.query(Symbol).order_by(Symbol.name).all()]


@router.post("/symbols")
async def create_symbol(data: dict, db: Session = Depends(get_db), user=Depends(get_current_user)):
    name = (data.get("name") or "").strip().upper()
    if not name:
        raise HTTPException(400, "Symbol name is required")
    if db.query(Symbol).filter(Symbol.name == name).first():
        raise HTTPException(400, "Symbol already exists")
    aliases = (data.get("aliases") or "").strip()
    s = Symbol(name=name, aliases=aliases, enabled=bool(data.get("enabled", True)))
    db.add(s)
    db.commit()
    _log(db, _actor(user), "symbol_add", f"Added symbol {name}")
    db.commit()
    return _symbol_dict(s)


@router.put("/symbols/{symbol_id}")
async def update_symbol(symbol_id: int, data: dict, db: Session = Depends(get_db),
                        user=Depends(get_current_user)):
    s = db.query(Symbol).get(symbol_id)
    if not s:
        raise HTTPException(404, "Symbol not found")
    if "name" in data and data["name"]:
        s.name = data["name"].strip().upper()
    if "aliases" in data:
        s.aliases = (data["aliases"] or "").strip()
    if "enabled" in data:
        s.enabled = bool(data["enabled"])
    db.commit()
    _log(db, _actor(user), "symbol_update", f"Updated symbol {s.name}")
    db.commit()
    return _symbol_dict(s)


@router.delete("/symbols/{symbol_id}")
async def delete_symbol(symbol_id: int, db: Session = Depends(get_db),
                        user=Depends(get_current_user)):
    s = db.query(Symbol).get(symbol_id)
    if not s:
        raise HTTPException(404, "Symbol not found")
    name = s.name
    db.delete(s)
    db.commit()
    _log(db, _actor(user), "symbol_delete", f"Deleted symbol {name}")
    db.commit()
    return {"success": True}


@router.get("/signals")
async def list_signals(limit: int = 50, db: Session = Depends(get_db),
                       user=Depends(get_current_user)):
    rows = db.query(Signal).order_by(desc(Signal.created_at)).limit(limit).all()
    return [{
        "id": s.id,
        "channel": s.channel,
        "symbol": s.symbol,
        "direction": s.direction,
        "immediate": bool(s.immediate),
        "entry_low": s.entry_low,
        "entry_high": s.entry_high,
        "sl": s.sl,
        "tp1": s.tp1,
        "state": s.state,
        "posted_at": s.posted_at.isoformat() if s.posted_at else None,
        "created_at": s.created_at.isoformat() if s.created_at else None,
        "groups": db.query(TradeGroup).filter(TradeGroup.signal_id == s.id).count(),
        "sl_editable": s.state == "filled",   # SL editable only while filled
    } for s in rows]


@router.post("/signals/{signal_id}/sl")
async def update_signal_sl(signal_id: int, data: dict, db: Session = Depends(get_db),
                           user=Depends(get_current_user)):
    """Manually change a filled signal's stop-loss and push it to every open
    client position for that signal. Only allowed while the signal is 'filled'
    (once it's 'done', the SL is locked)."""
    s = db.query(Signal).get(signal_id)
    if not s:
        raise HTTPException(404, "Signal not found")
    if s.state != "filled":
        raise HTTPException(400, "Stop-loss can only be edited while the signal is 'filled'")
    try:
        new_sl = float(data.get("sl"))
    except (TypeError, ValueError):
        raise HTTPException(400, "Invalid stop-loss value")
    if new_sl <= 0:
        raise HTTPException(400, "Stop-loss must be greater than 0")

    s.sl = new_sl
    db.commit()

    # dedup any pending SL update already queued for this signal
    existing = db.query(Command).filter(
        Command.action == "update_sl", Command.status.in_(["pending", "running"])).all()
    for cmd in existing:
        if (cmd.payload or {}).get("signal_id") == signal_id:
            cmd.result = None  # let it re-run with the newest SL (already in DB)
    db.add(Command(action="update_sl", client_id=None,
                   payload={"signal_id": signal_id}, requested_by=_actor(user),
                   status="pending"))
    _log(db, _actor(user), "update_sl",
         f"Signal #{signal_id}: SL changed to {new_sl} — pushing to open trades", None)
    db.commit()
    return {"queued": True, "sl": new_sl,
            "message": "SL updated — applying to all open trades in a few seconds."}


# ─────────────────────────────────────────────────────────────
# SUPPORT TICKETS (admin side)
# ─────────────────────────────────────────────────────────────
@router.get("/tickets")
async def admin_tickets(db: Session = Depends(get_db), user=Depends(get_current_user)):
    rows = db.query(Ticket).order_by(desc(Ticket.updated_at)).limit(100).all()
    names = {c.id: c.name for c in db.query(Client).filter(
        Client.id.in_([t.client_id for t in rows])).all()} if rows else {}
    out = []
    for t in rows:
        last = t.messages[-1] if t.messages else None
        out.append({"id": t.id, "client_id": t.client_id,
                    "client_name": names.get(t.client_id, f"#{t.client_id}"),
                    "subject": t.subject, "status": t.status,
                    "messages": len(t.messages),
                    "last_sender": last.sender if last else None,
                    "updated_at": t.updated_at.isoformat() if t.updated_at else None})
    return out


@router.get("/tickets/{ticket_id}")
async def admin_ticket_detail(ticket_id: int, db: Session = Depends(get_db),
                              user=Depends(get_current_user)):
    t = db.query(Ticket).get(ticket_id)
    if not t:
        raise HTTPException(404, "Ticket not found")
    client = db.query(Client).get(t.client_id)
    return {"id": t.id, "subject": t.subject, "status": t.status,
            "client_name": client.name if client else f"#{t.client_id}",
            "messages": [{"sender": m.sender, "body": m.body,
                          "images": m.images or [],
                          "time": m.created_at.isoformat() if m.created_at else None}
                         for m in t.messages]}


@router.post("/tickets/{ticket_id}/reply")
async def admin_ticket_reply(ticket_id: int, data: dict, db: Session = Depends(get_db),
                             user=Depends(get_current_user)):
    t = db.query(Ticket).get(ticket_id)
    if not t:
        raise HTTPException(404, "Ticket not found")
    body = (data.get("body") or "").strip()
    if not body:
        raise HTTPException(400, "Reply cannot be empty")
    db.add(TicketMessage(ticket_id=t.id, sender="admin", body=body, images=[]))
    t.status = "open"
    t.updated_at = datetime.utcnow()
    # notify the client (bell counter + email)
    db.add(Notification(client_id=t.client_id,
                        title=f"Support replied: {t.subject[:60]}",
                        body=body[:300]))
    db.commit()
    client = db.query(Client).get(t.client_id)
    if client and client.email:
        first = (client.name or "").split(" ")[0] or "there"
        await emailer.send_ticket_reply_email(client.email, first, t.subject)
    return {"success": True}


@router.post("/tickets/{ticket_id}/close")
async def admin_ticket_close(ticket_id: int, db: Session = Depends(get_db),
                             user=Depends(get_current_user)):
    t = db.query(Ticket).get(ticket_id)
    if not t:
        raise HTTPException(404, "Ticket not found")
    t.status = "closed"
    db.commit()
    return {"success": True}


# ─────────────────────────────────────────────────────────────
# NOTIFICATION CENTER — broadcast announcements to clients' bells
# ─────────────────────────────────────────────────────────────
@router.post("/notifications/broadcast")
async def broadcast_notification(data: dict, db: Session = Depends(get_db),
                                 user=Depends(get_current_user)):
    """Send an announcement (maintenance, update, notice) to clients. It lands
    in each client's in-portal notification bell. Audience:
      all    -> every client
      active -> currently on a trial or VIP license
      trial  -> clients on the trial channel
      vip    -> clients on the VIP channel
    """
    title = (data.get("title") or "").strip()
    body = (data.get("body") or "").strip()
    audience = (data.get("audience") or "all").strip().lower()
    if not title:
        raise HTTPException(400, "Title is required")
    if not body:
        raise HTTPException(400, "Message is required")

    q = db.query(Client)
    if audience == "active":
        q = q.filter(Client.status.in_(["trial", "active"]))
    elif audience == "trial":
        q = q.filter(Client.channel == "trial")
    elif audience == "vip":
        q = q.filter(Client.channel == "vip")
    else:
        audience = "all"

    clients = q.all()
    for c in clients:
        db.add(Notification(client_id=c.id, title=title[:120], body=body[:2000]))
    _log(db, _actor(user), "broadcast",
         f"Announcement '{title[:60]}' sent to {len(clients)} client(s) [{audience}]")
    db.commit()
    return {"success": True, "sent": len(clients), "audience": audience}


@router.get("/notifications/broadcasts")
async def list_broadcasts(limit: int = 20, db: Session = Depends(get_db),
                          user=Depends(get_current_user)):
    """Recent announcement history (from the activity log)."""
    rows = (db.query(ActivityLog)
            .filter(ActivityLog.action == "broadcast")
            .order_by(desc(ActivityLog.ts)).limit(min(limit, 100)).all())
    return [{"ts": r.ts.isoformat() if r.ts else None,
             "actor": r.actor, "message": r.message} for r in rows]


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
