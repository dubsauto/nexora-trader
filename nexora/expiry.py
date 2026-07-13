# nexora/expiry.py
#
# Background automation: flips trial/active clients to "expired" the moment
# their trial or license date passes. No admin action needed. By default it
# only blocks NEW trades (open positions finish naturally); set
# CLOSE_POSITIONS_ON_EXPIRY=true to force-close everything on expiry.

from datetime import datetime

from nexora import config
from nexora import emailer
from app.database import SessionLocal
from app.model import Client, ActivityLog, Notification


def _log(db, action, message, client_id=None):
    db.add(ActivityLog(actor="engine", category="client", action=action,
                       message=message, client_id=client_id))


async def check_expiries():
    """One pass. Returns the number of clients newly expired."""
    db = SessionLocal()
    expired = 0
    to_notify = []   # (email, first_name, kind) for clients that just expired
    try:
        now = datetime.utcnow()
        clients = db.query(Client).filter(Client.status.in_(["trial", "active"])).all()
        for c in clients:
            kind = None
            if c.status == "trial" and c.trial_expires_at and now >= c.trial_expires_at:
                kind = "trial"
            elif c.status == "active" and c.license_expires_at and now >= c.license_expires_at:
                kind = "license"

            if kind:
                c.status = "expired"
                expired += 1
                _log(db, "expired",
                     f"{c.name}: {kind} expired — trading stopped", client_id=c.id)
                # in-portal notification for the client
                db.add(Notification(
                    client_id=c.id, title="Your plan has expired",
                    body=f"Your {kind} has expired and trading has stopped. "
                         f"Contact us to renew and continue trading."))
                if c.email:
                    first = (c.name or "").split(" ")[0] or "there"
                    to_notify.append((c.email, first, kind))
        if expired:
            db.commit()
    except Exception as e:
        print(f"[Expiry] error: {e}")
        db.rollback()
    finally:
        db.close()

    # send expiry emails outside the DB session
    for email, first, kind in to_notify:
        try:
            await emailer.send_expiry_email(email, first, kind)
        except Exception as e:
            print(f"[Expiry] email error to {email}: {e}")

    if expired and config.CLOSE_POSITIONS_ON_EXPIRY:
        # optional: force-close positions for the just-expired clients
        from nexora.operations import close_all_for_expired
        await close_all_for_expired()

    # housekeeping: keep the snapshot table from growing unbounded
    try:
        from nexora.metrics import prune_snapshots
        prune_snapshots()
    except Exception as e:
        print(f"[Expiry] snapshot prune error: {e}")

    return expired
