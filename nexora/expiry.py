# nexora/expiry.py
#
# Background automation: flips trial/active clients to "expired" the moment
# their trial or license date passes. No admin action needed. By default it
# only blocks NEW trades (open positions finish naturally); set
# CLOSE_POSITIONS_ON_EXPIRY=true to force-close everything on expiry.

from datetime import datetime

from nexora import config
from app.database import SessionLocal
from app.model import Client, ActivityLog


def _log(db, action, message, client_id=None):
    db.add(ActivityLog(actor="engine", category="client", action=action,
                       message=message, client_id=client_id))


async def check_expiries():
    """One pass. Returns the number of clients newly expired."""
    db = SessionLocal()
    expired = 0
    try:
        now = datetime.utcnow()
        clients = db.query(Client).filter(Client.status.in_(["trial", "active"])).all()
        for c in clients:
            lapsed = False
            if c.status == "trial" and c.trial_expires_at and now >= c.trial_expires_at:
                lapsed = True
            elif c.status == "active" and c.license_expires_at and now >= c.license_expires_at:
                lapsed = True

            if lapsed:
                c.status = "expired"
                expired += 1
                _log(db, "expired",
                     f"{c.name}: {'trial' if c.channel == 'trial' else 'license'} "
                     f"expired — trading stopped", client_id=c.id)
        if expired:
            db.commit()
    except Exception as e:
        print(f"[Expiry] error: {e}")
        db.rollback()
    finally:
        db.close()

    if expired and config.CLOSE_POSITIONS_ON_EXPIRY:
        # optional: force-close positions for the just-expired clients
        from nexora.operations import close_all_for_expired
        await close_all_for_expired()

    return expired
