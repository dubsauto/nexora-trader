# app/database.py

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


# Use same DATABASE_URL as model.py
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://nexora_trader:nexora001@localhost:5432/nexora_trader_db"
)

# Create engine. For Postgres we tune the pool so leaked/idle connections are
# recycled and checkouts fail fast instead of hanging (SQLite ignores these).
_engine_kwargs = {"pool_pre_ping": True}   # drop stale connections
if DATABASE_URL.startswith("postgresql"):
    _engine_kwargs.update(
        pool_size=5,
        max_overflow=10,
        pool_timeout=15,      # fail fast instead of hanging 30s on checkout
        pool_recycle=900,     # recycle connections every 15 min
    )

engine = create_engine(DATABASE_URL, **_engine_kwargs)

# Session factory
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)


# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
