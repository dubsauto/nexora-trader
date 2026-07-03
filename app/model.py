# app/model.py
#
# NEXORA AI TRADER — data models.
#
#   AdminUser   : dashboard login (you, the admin).
#   Client      : one trading customer (MT5 creds + trial/license + settings).
#   Signal      : a parsed BUY/SELL signal from a Telegram channel.
#   TradeGroup  : the 3 positions opened for ONE client from ONE signal.
#   ActivityLog : audit trail of admin actions and engine events.
#   Setting     : simple key/value store for global settings.

from datetime import datetime
from sqlalchemy import (
    Column, Integer, BigInteger, String, Float, DateTime, Text,
    Boolean, ForeignKey, JSON
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


# =========================================================
# ADMIN USER (dashboard login)
# =========================================================
class AdminUser(Base):
    __tablename__ = "admin_users"

    id = Column(Integer, primary_key=True)
    username = Column(String(64), unique=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    role = Column(String(16), default="admin")
    created_at = Column(DateTime, default=datetime.utcnow)


# =========================================================
# CLIENT
# =========================================================
# status values:
#   trial     -> on a free trial (channel = "trial")
#   active    -> full paid license (channel = "vip")
#   inactive  -> manually disabled by admin (never trades)
#   expired   -> trial or license lapsed (never trades until re-activated)
#
# channel values: "trial" | "vip"  (which signal channel this client copies)
class Client(Base):
    __tablename__ = "clients"

    id = Column(Integer, primary_key=True)

    # Who this is
    name = Column(String(255), nullable=False)
    note = Column(Text, nullable=True)

    # MT5 credentials (provided by the client once)
    login = Column(String(64), nullable=False)          # MT5 account number
    password = Column(Text, nullable=False)             # investor/master password
    server = Column(String(255), nullable=False)        # broker server name

    # MetaApi linkage
    metaapi_account_id = Column(String(255), unique=True, nullable=True)
    deploy_state = Column(String(20), default="undeployed")  # undeployed/deploying/deployed
    connection_note = Column(Text, nullable=True)

    # Lifecycle
    status = Column(String(16), default="inactive")     # trial/active/inactive/expired
    channel = Column(String(8), default="trial")        # trial/vip
    trading_enabled = Column(Boolean, default=True)     # pause/resume without changing license

    # Trial + license dates (UTC)
    trial_started_at = Column(DateTime, nullable=True)
    trial_expires_at = Column(DateTime, nullable=True)
    license_expires_at = Column(DateTime, nullable=True)

    # Trading settings
    lot_size = Column(Float, default=0.01)              # base lot PER position
    risk_profile = Column(String(16), default="balanced")  # conservative/balanced/aggressive
    deposit = Column(Float, default=0.0)                # deposit amount (admin-entered)

    # Unique magic base per client so positions never collide across clients
    magic = Column(Integer, default=0)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    trade_groups = relationship("TradeGroup", back_populates="client",
                                cascade="all, delete-orphan")

    # ---- computed helpers -------------------------------------------------
    def is_eligible(self, signal_channel: str) -> bool:
        """A client trades only if active/trial, trading ON, not expired,
        and the signal's channel matches the client's assigned channel."""
        if not self.trading_enabled:
            return False
        if self.status not in ("trial", "active"):
            return False
        if self.channel != signal_channel:
            return False
        now = datetime.utcnow()
        if self.status == "trial" and self.trial_expires_at and now >= self.trial_expires_at:
            return False
        if self.status == "active" and self.license_expires_at and now >= self.license_expires_at:
            return False
        return True

    def effective_lot(self, multiplier: float) -> float:
        return round((self.lot_size or 0.01) * multiplier, 2)


# =========================================================
# SIGNAL
# =========================================================
class Signal(Base):
    __tablename__ = "signals"

    id = Column(Integer, primary_key=True)

    # Telegram provenance
    update_id = Column(BigInteger, unique=True, nullable=True)  # dedup key
    channel = Column(String(8), nullable=False)                 # trial/vip
    raw_text = Column(Text, nullable=True)
    posted_at = Column(DateTime, nullable=True)                 # Telegram post time (UTC)

    # Parsed values
    direction = Column(String(4), nullable=False)   # BUY/SELL
    entry_low = Column(Float, nullable=False)
    entry_high = Column(Float, nullable=False)
    sl = Column(Float, nullable=False)
    tp1 = Column(Float, nullable=False)

    # Engine state:
    #   waiting  -> watching for price to enter the zone (5-min window)
    #   filled   -> positions opened for eligible clients
    #   done     -> TP1 handled / finished
    #   expired  -> window passed without entry
    state = Column(String(12), default="waiting")

    magic = Column(Integer, default=0)              # base magic for this signal

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    trade_groups = relationship("TradeGroup", back_populates="signal",
                                cascade="all, delete-orphan")


# =========================================================
# TRADE GROUP  (3 positions for ONE client from ONE signal)
# =========================================================
class TradeGroup(Base):
    __tablename__ = "trade_groups"

    id = Column(Integer, primary_key=True)

    signal_id = Column(Integer, ForeignKey("signals.id", ondelete="CASCADE"))
    client_id = Column(Integer, ForeignKey("clients.id", ondelete="CASCADE"))

    magic = Column(Integer, default=0)              # unique magic for these positions
    lot = Column(Float, default=0.0)                # lot actually used per position

    # tickets of the opened positions (JSON list of strings)
    tickets = Column(JSON, default=list)

    # open -> positions live; tp1_done -> 2 closed + runner at BE; closed -> none left
    state = Column(String(12), default="open")

    opened_at = Column(DateTime, nullable=True)
    tp1_at = Column(DateTime, nullable=True)
    last_error = Column(Text, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    signal = relationship("Signal", back_populates="trade_groups")
    client = relationship("Client", back_populates="trade_groups")


# =========================================================
# ACTIVITY LOG
# =========================================================
class ActivityLog(Base):
    __tablename__ = "activity_log"

    id = Column(Integer, primary_key=True)
    ts = Column(DateTime, default=datetime.utcnow)

    actor = Column(String(64), default="system")    # admin username or "engine"
    category = Column(String(24), default="system") # client/signal/trade/system
    action = Column(String(64), nullable=False)
    message = Column(Text, nullable=True)

    client_id = Column(Integer, nullable=True)
    signal_id = Column(Integer, nullable=True)


# =========================================================
# COMMANDS (dashboard → worker queue)
# =========================================================
# Trade actions (Close Runner / Close All, bulk versions) are QUEUED by the
# web dashboard and executed by the WORKER, so only one process ever touches
# a MetaApi account. This prevents the web and worker fighting over deploy
# state (which caused endless "no accounts deployed yet" subscription errors).
class Command(Base):
    __tablename__ = "commands"

    id = Column(Integer, primary_key=True)
    action = Column(String(32), nullable=False)   # close_all / close_runner / close_all_bulk / close_runner_bulk
    client_id = Column(Integer, nullable=True)     # null for bulk actions
    status = Column(String(12), default="pending") # pending / running / done / error
    result = Column(Text, nullable=True)
    requested_by = Column(String(64), default="admin")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


# =========================================================
# SETTINGS (key/value)
# =========================================================
class Setting(Base):
    __tablename__ = "settings"

    key = Column(String(64), primary_key=True)
    value = Column(Text, nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
