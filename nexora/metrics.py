# nexora/metrics.py
#
# Records account balance/equity to the client row (last-known values) AND to
# the account_snapshots history whenever a live connection is available.
# Shared by the trade engine (opportunistic on deploy) and the client Refresh.

from datetime import datetime, timedelta

from app.database import SessionLocal
from app.model import Client, AccountSnapshot

SNAPSHOT_RETENTION_DAYS = 120


def record_metrics(client_id, balance, equity):
    """Update the client's last-known balance/equity and append a snapshot."""
    if balance is None and equity is None:
        return
    db = SessionLocal()
    try:
        c = db.query(Client).get(client_id)
        if not c:
            return
        c.last_balance = balance
        c.last_equity = equity
        c.last_synced_at = datetime.utcnow()
        db.add(AccountSnapshot(client_id=client_id, balance=balance, equity=equity))
        db.commit()
    except Exception:
        db.rollback()
    finally:
        db.close()


def profit_today(db, client) -> float | None:
    """Current equity minus the balance at the first snapshot of the UTC day.
    Falls back to floating P/L (equity - last balance) if no snapshot today."""
    if client.last_equity is None:
        return None
    day_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    first = (db.query(AccountSnapshot)
             .filter(AccountSnapshot.client_id == client.id,
                     AccountSnapshot.taken_at >= day_start)
             .order_by(AccountSnapshot.taken_at).first())
    baseline = first.balance if (first and first.balance is not None) else client.last_balance
    if baseline is None:
        return None
    return round(client.last_equity - baseline, 2)


def prune_snapshots():
    """Delete snapshots older than the retention window (keeps the table small)."""
    db = SessionLocal()
    try:
        cutoff = datetime.utcnow() - timedelta(days=SNAPSHOT_RETENTION_DAYS)
        n = db.query(AccountSnapshot).filter(AccountSnapshot.taken_at < cutoff)\
            .delete(synchronize_session=False)
        if n:
            db.commit()
            print(f"[Metrics] pruned {n} old snapshot(s)")
        else:
            db.rollback()
    except Exception:
        db.rollback()
    finally:
        db.close()
