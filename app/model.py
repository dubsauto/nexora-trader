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
    email = Column(String(190), nullable=True)
    password_hash = Column(String(255), nullable=False)
    role = Column(String(16), default="admin")   # admin | developer (maintenance)
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

    # Client-portal identity (signups). Admin-created clients may leave these
    # empty until the client registers.
    email = Column(String(190), nullable=True)
    phone = Column(String(32), nullable=True)              # contact number (for admin outreach)
    client_password_hash = Column(String(255), nullable=True)
    gender = Column(String(10), nullable=True)            # male/female

    # XM account screenshot (Account ID + balance) uploaded at signup, so the
    # admin can verify a genuine XM client before approving. Stored as a data URL.
    verification_image = Column(Text, nullable=True)

    # 'approved'  -> full access (admin-created default)
    # 'pending'   -> self-signup awaiting admin approval (portal locked)
    approval_status = Column(String(16), default="approved")

    # Last-known account metrics (synced opportunistically by the engine
    # whenever it holds a live connection) for the portal overview.
    last_balance = Column(Float, nullable=True)
    last_equity = Column(Float, nullable=True)
    last_synced_at = Column(DateTime, nullable=True)

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
    # Positions opened per signal for this client. NULL -> use the global
    # POSITIONS_PER_SIGNAL default (3). Engine closes all-but-one at TP1.
    positions_per_signal = Column(Integer, nullable=True)
    deposit = Column(Float, default=0.0)                # deposit amount (admin-entered)

    # Optional per-client symbol overrides for rare brokers the auto-resolver
    # can't match, e.g. {"XAUUSD": "GOLD.spot", "BTCUSD": "BTC/USD"}.
    symbol_overrides = Column(JSON, default=dict)

    # Auto-detected broker symbol per instrument, populated by the engine the
    # first time it trades that instrument for this client (for dashboard view).
    resolved_symbols = Column(JSON, default=dict)

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
    symbol = Column(String(32), nullable=True)      # broker trading symbol (e.g. XAUUSD)
    direction = Column(String(4), nullable=False)   # BUY/SELL
    immediate = Column(Boolean, default=False)      # BUY NOW/SELL NOW -> market entry
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
    closed_at = Column(DateTime, nullable=True)
    last_error = Column(Text, nullable=True)

    # Trade-history fields, captured opportunistically while the account is
    # deployed (during a signal or a manual op). Not real-time by design.
    entry_price = Column(Float, nullable=True)   # avg fill price of the group
    close_price = Column(Float, nullable=True)   # exit price once fully closed
    profit = Column(Float, nullable=True)        # realized (+ floating) P/L in account currency

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
    action = Column(String(32), nullable=False)   # close_all / close_runner / *_bulk / refresh_account / update_sl
    client_id = Column(Integer, nullable=True)     # null for bulk/signal actions
    payload = Column(JSON, nullable=True)          # extra args, e.g. {"signal_id": 12}
    status = Column(String(12), default="pending") # pending / running / done / error
    result = Column(Text, nullable=True)
    requested_by = Column(String(64), default="admin")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


# =========================================================
# SYMBOLS (tradable instruments)
# =========================================================
# Admin-managed list of symbols the platform trades. Each has a broker
# trading `name` (e.g. XAUUSD) used to place orders, plus `aliases` — the
# keywords that may appear in a signal message (e.g. "GOLD,XAUUSD") used to
# detect which instrument a signal is for.
class Symbol(Base):
    __tablename__ = "symbols"

    id = Column(Integer, primary_key=True)
    name = Column(String(32), unique=True, nullable=False)   # broker symbol to trade
    aliases = Column(Text, default="")                       # comma-separated keywords
    enabled = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    def alias_list(self):
        return [a.strip() for a in (self.aliases or "").split(",") if a.strip()]


# =========================================================
# CLIENT PORTAL — notifications, support tickets, pw reset
# =========================================================
class Notification(Base):
    __tablename__ = "notifications"

    id = Column(Integer, primary_key=True)
    client_id = Column(Integer, ForeignKey("clients.id", ondelete="CASCADE"),
                       nullable=False, index=True)
    title = Column(String(120), nullable=False)
    body = Column(Text, nullable=True)
    read = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)


class Ticket(Base):
    __tablename__ = "tickets"

    id = Column(Integer, primary_key=True)
    client_id = Column(Integer, ForeignKey("clients.id", ondelete="CASCADE"),
                       nullable=False, index=True)
    subject = Column(String(200), nullable=False)
    status = Column(String(12), default="open")      # open / closed
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    messages = relationship("TicketMessage", back_populates="ticket",
                            cascade="all, delete-orphan",
                            order_by="TicketMessage.created_at")


class TicketMessage(Base):
    __tablename__ = "ticket_messages"

    id = Column(Integer, primary_key=True)
    ticket_id = Column(Integer, ForeignKey("tickets.id", ondelete="CASCADE"),
                       nullable=False, index=True)
    sender = Column(String(8), nullable=False)       # client / admin
    body = Column(Text, nullable=True)
    images = Column(JSON, default=list)              # up to 3 data-URLs
    created_at = Column(DateTime, default=datetime.utcnow)

    ticket = relationship("Ticket", back_populates="messages")


class PasswordReset(Base):
    __tablename__ = "password_resets"

    token = Column(String(64), primary_key=True)
    client_id = Column(Integer, ForeignKey("clients.id", ondelete="CASCADE"),
                       nullable=False)
    expires_at = Column(DateTime, nullable=False)
    used = Column(Boolean, default=False)


# =========================================================
# ACCOUNT SNAPSHOTS — balance/equity captured whenever the engine (or a
# client Refresh) has a live connection. Powers "Profit Today" (vs the first
# snapshot of the UTC day) and future balance/equity charts.
# =========================================================
class AccountSnapshot(Base):
    __tablename__ = "account_snapshots"

    id = Column(Integer, primary_key=True)
    client_id = Column(Integer, ForeignKey("clients.id", ondelete="CASCADE"),
                       nullable=False, index=True)
    balance = Column(Float, nullable=True)
    equity = Column(Float, nullable=True)
    taken_at = Column(DateTime, default=datetime.utcnow, index=True)


# =========================================================
# SETTINGS (key/value)
# =========================================================
class Setting(Base):
    __tablename__ = "settings"

    key = Column(String(64), primary_key=True)
    value = Column(Text, nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
