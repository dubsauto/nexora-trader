# app/api/client_routes.py
#
# Client portal API:
#   /client/*      — auth: signup, login, forgot password, reset password
#   /client-api/*  — portal data (JWT role=client): profile, dashboard,
#                    connection update, signals, trades, notifications, tickets
#
# Signup clients start as approval_status='pending' (portal locked) until the
# admin approves them from the dashboard. Admin-created clients are 'approved'.

import secrets
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import desc

from app.database import get_db
from app.auth import hash_password, verify_password, create_access_token, get_current_user
from app.model import (Client, Signal, TradeGroup, Notification, Ticket,
                       TicketMessage, PasswordReset, ActivityLog, Command)
from nexora import config
from nexora import emailer
from nexora import metrics

router = APIRouter(prefix="", tags=["Client Portal"])

MAX_IMAGES = 3
MAX_IMAGE_CHARS = 2_000_000     # ~1.5 MB per image as a data URL
MAX_VERIFY_CHARS = 6_000_000    # ~4 MB for the XM verification screenshot


# ─────────────────────────────────────────────────────────────
# helpers
# ─────────────────────────────────────────────────────────────
def get_current_client(payload=Depends(get_current_user)):
    if payload.get("role") != "client" or not payload.get("client_id"):
        raise HTTPException(status_code=403, detail="Client access only")
    return payload


def _client_or_404(db, payload) -> Client:
    c = db.query(Client).get(payload["client_id"])
    if not c:
        raise HTTPException(status_code=401, detail="Account no longer exists")
    return c


def _first_name(c: Client) -> str:
    return (c.name or "").strip().split(" ")[0] or "there"


def _log(db, action, message, client_id=None):
    db.add(ActivityLog(actor="portal", category="client", action=action,
                       message=message, client_id=client_id))


# ─────────────────────────────────────────────────────────────
# PUBLIC (no auth) — landing-page live statistics
# ─────────────────────────────────────────────────────────────
@router.get("/public/stats")
async def public_stats(db: Session = Depends(get_db)):
    """Real platform numbers for the landing page (used when live stats are
    switched on there). Only aggregate counts — nothing sensitive."""
    active = db.query(Client).filter(Client.status.in_(["trial", "active"])).count()
    executed = db.query(Signal).filter(Signal.state.in_(["filled", "done"])).count()
    total = db.query(Signal).count()
    return {"active_clients": active, "signals_executed": executed,
            "live_signals": total}


# ─────────────────────────────────────────────────────────────
# AUTH
# ─────────────────────────────────────────────────────────────
@router.post("/client/signup")
async def client_signup(data: dict, db: Session = Depends(get_db)):
    required = ["full_name", "gender", "email", "phone", "password",
                "mt5_login", "mt5_password", "server"]
    for f in required:
        if not (data.get(f) or "").strip():
            raise HTTPException(400, f"Missing field: {f.replace('_', ' ')}")

    email = data["email"].strip().lower()
    if db.query(Client).filter(Client.email == email).first():
        raise HTTPException(400, "An account with this email already exists")
    if db.query(Client).filter(Client.login == str(data["mt5_login"]).strip()).first():
        raise HTTPException(400, "An account with this MT5 login already exists")
    if len(data["password"]) < 6:
        raise HTTPException(400, "Password must be at least 6 characters")
    gender = data["gender"].strip().lower()
    if gender not in ("male", "female"):
        raise HTTPException(400, "Gender must be male or female")

    # XM verification screenshot (Account ID + balance) — required
    image = data.get("verification_image") or ""
    if not isinstance(image, str) or not image.startswith("data:image/"):
        raise HTTPException(400, "Please upload a screenshot of your XM account "
                                 "showing your Account ID and current balance")
    if len(image) > MAX_VERIFY_CHARS:
        raise HTTPException(400, "Screenshot is too large — please upload an image under ~4 MB")

    c = Client(
        name=data["full_name"].strip(),
        email=email,
        phone=data["phone"].strip(),
        gender=gender,
        verification_image=image,
        client_password_hash=hash_password(data["password"]),
        login=str(data["mt5_login"]).strip(),
        password=data["mt5_password"],
        server=data["server"].strip(),
        status="inactive",
        channel="trial",
        approval_status="pending",      # admin must approve before anything
        trading_enabled=True,
        lot_size=0.01,
        risk_profile="balanced",
    )
    db.add(c)
    db.flush()
    c.magic = config.MAGIC_BASE + c.id
    _log(db, "signup", f"New signup: {c.name} ({email}) — pending approval", c.id)
    db.commit()
    cname = c.name
    # notify the admin inbox of the new signup awaiting approval
    await emailer.send_admin_signup_email(cname, email)
    return {"success": True,
            "message": "Account created! You can log in, but your profile is "
                       "pending and waiting for admin approval."}


@router.post("/client/login")
async def client_login(data: dict, db: Session = Depends(get_db)):
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""
    if not email or not password:
        raise HTTPException(400, "Missing credentials")

    c = db.query(Client).filter(Client.email == email).first()
    if not c or not c.client_password_hash or \
            not verify_password(password, c.client_password_hash):
        raise HTTPException(401, "Invalid email or password")

    token = create_access_token({"client_id": c.id, "role": "client",
                                 "username": c.email})
    return {"access_token": token, "role": "client",
            "approval_status": c.approval_status, "name": c.name}


@router.post("/client/forgot")
async def client_forgot(data: dict, db: Session = Depends(get_db)):
    email = (data.get("email") or "").strip().lower()
    c = db.query(Client).filter(Client.email == email).first()
    # Always answer the same, so emails can't be enumerated.
    generic = {"success": True,
               "message": "If that email exists, a reset link has been sent."}
    if not c:
        return generic
    token = secrets.token_urlsafe(32)
    db.add(PasswordReset(token=token, client_id=c.id,
                         expires_at=datetime.utcnow() + timedelta(hours=1)))
    db.commit()
    await emailer.send_reset_email(c.email, _first_name(c), token)
    return generic


@router.post("/client/reset")
async def client_reset(data: dict, db: Session = Depends(get_db)):
    token = data.get("token") or ""
    password = data.get("password") or ""
    if len(password) < 6:
        raise HTTPException(400, "Password must be at least 6 characters")
    pr = db.query(PasswordReset).filter(PasswordReset.token == token).first()
    if not pr or pr.used or pr.expires_at < datetime.utcnow():
        raise HTTPException(400, "This reset link is invalid or has expired")
    c = db.query(Client).get(pr.client_id)
    if not c:
        raise HTTPException(400, "Account no longer exists")
    c.client_password_hash = hash_password(password)
    pr.used = True
    db.commit()
    return {"success": True, "message": "Password updated — you can log in now."}


# ─────────────────────────────────────────────────────────────
# PROFILE / DASHBOARD
# ─────────────────────────────────────────────────────────────
@router.get("/client-api/me")
async def me(db: Session = Depends(get_db), payload=Depends(get_current_client)):
    c = _client_or_404(db, payload)
    unread = db.query(Notification).filter(Notification.client_id == c.id,
                                           Notification.read == False).count()  # noqa: E712
    return {
        "id": c.id, "name": c.name, "first_name": _first_name(c),
        "email": c.email, "gender": c.gender,
        "approval_status": c.approval_status,
        "status": c.status, "channel": c.channel,
        "unread_notifications": unread,
    }


@router.get("/client-api/dashboard")
async def dashboard(db: Session = Depends(get_db), payload=Depends(get_current_client)):
    c = _client_or_404(db, payload)
    # Pending clients CAN load the dashboard: they see their MT5 connection
    # details (login/server) and a "waiting for approval" status, but no live
    # account metrics (the account is not provisioned until approval).
    pending = c.approval_status != "approved"

    expiry = None
    if c.status == "trial" and c.trial_expires_at:
        expiry = c.trial_expires_at.isoformat()
    elif c.status == "active" and c.license_expires_at:
        expiry = c.license_expires_at.isoformat()

    bot_active = bool(c.trading_enabled and c.status in ("trial", "active"))

    # Most recent trade this client's account actually opened
    last_group = (db.query(TradeGroup)
                  .filter(TradeGroup.client_id == c.id,
                          TradeGroup.opened_at.isnot(None))
                  .order_by(desc(TradeGroup.opened_at)).first())
    last_trade_at = last_group.opened_at.isoformat() if last_group else None

    if pending:
        conn_status = "waiting_approval"
    elif c.status not in ("trial", "active"):
        # trial ended / VIP expired / admin-paused -> not connected for trading
        conn_status = "disconnected"
    elif c.metaapi_account_id:
        conn_status = "connected"
    else:
        conn_status = "disconnected"

    return {
        "balance": None if pending else c.last_balance,
        "equity": None if pending else c.last_equity,
        "profit_today": None if pending else metrics.profit_today(db, c),
        "synced_at": c.last_synced_at.isoformat() if (c.last_synced_at and not pending) else None,
        "bot_status": "active" if bot_active else "paused",
        "broker": c.server, "server": c.server, "mt5_login": c.login,
        "status": c.status, "channel": c.channel, "license_expiry": expiry,
        "connected": bool(c.metaapi_account_id),
        "connection_status": conn_status, "pending": pending,
        "last_trade": last_trade_at,
        "trading_enabled": bool(c.trading_enabled),
        "lot_size": c.lot_size, "risk_profile": c.risk_profile,
    }


@router.post("/client-api/refresh")
async def refresh_metrics(db: Session = Depends(get_db),
                          payload=Depends(get_current_client)):
    """Queue a balance/equity refresh to the worker (deploy → read → undeploy).
    Deduped so repeated clicks don't stack deployments."""
    c = _client_or_404(db, payload)
    if c.approval_status != "approved":
        raise HTTPException(403, "Account pending approval")
    if not c.metaapi_account_id:
        raise HTTPException(400, "Your MT5 account isn't connected yet")
    cutoff = datetime.utcnow() - timedelta(seconds=120)
    existing = db.query(Command).filter(
        Command.action == "refresh_account", Command.client_id == c.id,
        Command.status.in_(["pending", "running"]),
        Command.created_at >= cutoff).first()
    if existing:
        return {"queued": False, "message": "A refresh is already in progress…"}
    db.add(Command(action="refresh_account", client_id=c.id,
                   requested_by="client", status="pending"))
    db.commit()
    return {"queued": True,
            "message": "Refreshing your account — this can take up to a minute."}


@router.put("/client-api/connection")
async def update_connection(data: dict, db: Session = Depends(get_db),
                            payload=Depends(get_current_client)):
    c = _client_or_404(db, payload)

    new_login = str(data.get("login") or c.login).strip()
    new_server = (data.get("server") or c.server).strip()
    new_password = data.get("password") or None

    login_changed = new_login != c.login
    server_changed = new_server != c.server

    if login_changed and db.query(Client).filter(Client.login == new_login,
                                                 Client.id != c.id).first():
        raise HTTPException(400, "That MT5 login is already registered")

    c.login = new_login
    c.server = new_server
    if new_password:
        c.password = new_password

    # Pending accounts are NOT provisioned on MetaApi until an admin approves
    # them (approval provisions using these fields). Just persist the details.
    if c.approval_status != "approved":
        _log(db, "connection_update",
             f"{c.name}: MT5 details updated by client (pending approval)", c.id)
        db.commit()
        return {"success": True, "connected": False,
                "message": "Saved. Your account will be connected once your "
                           "profile is approved."}

    # Re-provision when the account identity changed; update creds otherwise.
    from app.services.account_management import account_manager
    try:
        if (login_changed or server_changed) and c.metaapi_account_id:
            old = c.metaapi_account_id
            c.metaapi_account_id = None
            db.commit()
            await account_manager.remove_account(old)
        if not c.metaapi_account_id:
            prov = await account_manager.add_account(
                name=f"NEXORA-{c.id}-{c.name}", server=c.server,
                login=str(c.login), password=c.password,
                manual_trades=False, use_dedicated_ip=config.USE_DEDICATED_IP,
                magic=c.magic or 0)
            if prov.get("success"):
                c.metaapi_account_id = prov["account_id"]
                c.connection_note = "provisioned"
                await account_manager.undeploy(c.metaapi_account_id)
                c.deploy_state = "undeployed"
                # broker may have changed -> stale symbol mappings
                c.resolved_symbols = {}
            else:
                c.connection_note = f"provision failed: {prov.get('message')}"
        elif new_password:
            await account_manager.update_account(c.metaapi_account_id,
                                                 {"password": new_password})
    except Exception as e:
        c.connection_note = f"connection update error: {e}"

    _log(db, "connection_update", f"{c.name}: MT5 connection updated by client", c.id)
    db.commit()
    return {"success": True, "connected": bool(c.metaapi_account_id),
            "message": "Connection updated"
            if c.metaapi_account_id else
            f"Saved, but connection failed: {c.connection_note}"}


# ─────────────────────────────────────────────────────────────
# SIGNALS / TRADES
# ─────────────────────────────────────────────────────────────
@router.get("/client-api/signals")
async def client_signals(limit: int = 5, db: Session = Depends(get_db),
                         payload=Depends(get_current_client)):
    c = _client_or_404(db, payload)
    if c.approval_status != "approved":
        raise HTTPException(403, "Account pending approval")
    rows = (db.query(Signal).filter(Signal.channel == c.channel)
            .order_by(desc(Signal.created_at)).limit(min(limit, 50)).all())
    lot = round((c.lot_size or 0.01) * config.risk_multiplier(c.risk_profile), 2)
    return [{
        "symbol": s.symbol, "direction": s.direction,
        "immediate": bool(s.immediate), "lot": lot,
        "entry": s.entry_low if not s.immediate else None,
        "state": s.state,
        "time": (s.posted_at or s.created_at).isoformat()
                if (s.posted_at or s.created_at) else None,
    } for s in rows]


def _trade_row(g: TradeGroup, sig: Signal):
    return {
        "symbol": sig.symbol if sig else None,
        "direction": sig.direction if sig else None,
        "lot": g.lot, "state": g.state,
        "entry_price": g.entry_price,
        "close_price": g.close_price,
        "profit": g.profit,
        "opened_at": g.opened_at.isoformat() if g.opened_at else None,
        "closed_at": g.closed_at.isoformat() if g.closed_at else None,
        "tp1_at": g.tp1_at.isoformat() if g.tp1_at else None,
    }


@router.get("/client-api/trades")
async def client_trades(limit: int = 5, live: bool = False,
                        db: Session = Depends(get_db),
                        payload=Depends(get_current_client)):
    c = _client_or_404(db, payload)
    if c.approval_status != "approved":
        raise HTTPException(403, "Account pending approval")
    q = db.query(TradeGroup).filter(TradeGroup.client_id == c.id)
    if live:
        q = q.filter(TradeGroup.state.in_(["open", "tp1_done"]))
    rows = q.order_by(desc(TradeGroup.created_at)).limit(min(limit, 50)).all()
    sigs = {s.id: s for s in db.query(Signal).filter(
        Signal.id.in_([g.signal_id for g in rows])).all()} if rows else {}
    return [_trade_row(g, sigs.get(g.signal_id)) for g in rows]


# ─────────────────────────────────────────────────────────────
# NOTIFICATIONS
# ─────────────────────────────────────────────────────────────
@router.get("/client-api/notifications")
async def notifications(db: Session = Depends(get_db),
                        payload=Depends(get_current_client)):
    c = _client_or_404(db, payload)
    rows = (db.query(Notification).filter(Notification.client_id == c.id)
            .order_by(desc(Notification.created_at)).limit(30).all())
    return [{"id": n.id, "title": n.title, "body": n.body, "read": n.read,
             "time": n.created_at.isoformat() if n.created_at else None}
            for n in rows]


@router.post("/client-api/notifications/read")
async def mark_read(db: Session = Depends(get_db),
                    payload=Depends(get_current_client)):
    c = _client_or_404(db, payload)
    db.query(Notification).filter(Notification.client_id == c.id,
                                  Notification.read == False)\
        .update({Notification.read: True})  # noqa: E712
    db.commit()
    return {"success": True}


# ─────────────────────────────────────────────────────────────
# SUPPORT TICKETS
# ─────────────────────────────────────────────────────────────
def _validate_images(images):
    images = images or []
    if len(images) > MAX_IMAGES:
        raise HTTPException(400, f"Maximum {MAX_IMAGES} images per message")
    for img in images:
        if not isinstance(img, str) or not img.startswith("data:image/"):
            raise HTTPException(400, "Images must be image files")
        if len(img) > MAX_IMAGE_CHARS:
            raise HTTPException(400, "Each image must be under ~1.5 MB")
    return images


@router.get("/client-api/tickets")
async def list_tickets(db: Session = Depends(get_db),
                       payload=Depends(get_current_client)):
    c = _client_or_404(db, payload)
    rows = (db.query(Ticket).filter(Ticket.client_id == c.id)
            .order_by(desc(Ticket.updated_at)).all())
    return [{"id": t.id, "subject": t.subject, "status": t.status,
             "updated_at": t.updated_at.isoformat() if t.updated_at else None,
             "messages": len(t.messages)} for t in rows]


@router.post("/client-api/tickets")
async def create_ticket(data: dict, db: Session = Depends(get_db),
                        payload=Depends(get_current_client)):
    c = _client_or_404(db, payload)
    subject = (data.get("subject") or "").strip()
    body = (data.get("body") or "").strip()
    if not subject or not body:
        raise HTTPException(400, "Subject and message are required")
    images = _validate_images(data.get("images"))

    t = Ticket(client_id=c.id, subject=subject[:200], status="open")
    db.add(t)
    db.flush()
    db.add(TicketMessage(ticket_id=t.id, sender="client", body=body, images=images))
    _log(db, "ticket_open", f"{c.name}: opened ticket '{subject[:60]}'", c.id)
    db.commit()
    cname, tid = c.name, t.id
    await emailer.send_admin_ticket_email(cname, subject[:120])
    return {"success": True, "ticket_id": tid}


@router.get("/client-api/tickets/{ticket_id}/messages")
async def ticket_messages(ticket_id: int, db: Session = Depends(get_db),
                          payload=Depends(get_current_client)):
    c = _client_or_404(db, payload)
    t = db.query(Ticket).filter(Ticket.id == ticket_id,
                                Ticket.client_id == c.id).first()
    if not t:
        raise HTTPException(404, "Ticket not found")
    return {"id": t.id, "subject": t.subject, "status": t.status,
            "messages": [{"sender": m.sender, "body": m.body,
                          "images": m.images or [],
                          "time": m.created_at.isoformat() if m.created_at else None}
                         for m in t.messages]}


@router.post("/client-api/tickets/{ticket_id}/messages")
async def ticket_reply(ticket_id: int, data: dict, db: Session = Depends(get_db),
                       payload=Depends(get_current_client)):
    c = _client_or_404(db, payload)
    t = db.query(Ticket).filter(Ticket.id == ticket_id,
                                Ticket.client_id == c.id).first()
    if not t:
        raise HTTPException(404, "Ticket not found")
    body = (data.get("body") or "").strip()
    images = _validate_images(data.get("images"))
    if not body and not images:
        raise HTTPException(400, "Message cannot be empty")
    db.add(TicketMessage(ticket_id=t.id, sender="client", body=body, images=images))
    t.status = "open"
    t.updated_at = datetime.utcnow()
    db.commit()
    return {"success": True}
