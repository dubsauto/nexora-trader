# app/database.py

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


# Use same DATABASE_URL as model.py
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://nexora_trader:nexora001@localhost:5432/nexora_trader_db"
)

# Create engine
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True  # avoids stale connections
)

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
