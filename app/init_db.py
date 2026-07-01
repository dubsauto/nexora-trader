# app/init_db.py
#
# Creates all NEXORA tables and seeds a default admin user.
# Admin credentials come from the environment:
#   ADMIN_USERNAME (default "admin")
#   ADMIN_PASSWORD (default "changeme" — CHANGE THIS in .env)

import os
from datetime import datetime

from sqlalchemy.orm import sessionmaker

from app.model import Base, AdminUser
from app.auth import hash_password
from app.database import engine

SessionLocal = sessionmaker(bind=engine)


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


async def init_database():
    print("[init] Initializing NEXORA database...")
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        _seed_admin(db)
    finally:
        db.close()
    print("[init] Database ready.")
