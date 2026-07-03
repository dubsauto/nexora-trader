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
]


def _ensure_columns():
    for table, column, ddl in _COLUMN_ADDITIONS:
        try:
            with engine.begin() as conn:
                conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}"))
            print(f"[init] Added column {table}.{column}")
        except Exception:
            # column already exists (or table not created yet) — safe to ignore
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


def _seed_default_symbol(db):
    if db.query(Symbol).count() > 0:
        return
    db.add(Symbol(name=config.DEFAULT_SYMBOL,
                  aliases=config.DEFAULT_SYMBOL_ALIASES,
                  enabled=True))
    db.commit()
    print(f"[init] Seeded default symbol '{config.DEFAULT_SYMBOL}'")


async def init_database():
    print("[init] Initializing NEXORA database...")
    Base.metadata.create_all(bind=engine)
    _ensure_columns()
    db = SessionLocal()
    try:
        _seed_admin(db)
        _seed_default_symbol(db)
    finally:
        db.close()
    print("[init] Database ready.")
