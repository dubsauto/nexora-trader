# nexora/config.py
#
# Central configuration for the NEXORA AI TRADER platform.
# All values are read from environment variables (.env) with sensible
# defaults so the client only has to fill the .env once.

import os
from dotenv import load_dotenv

load_dotenv()


def _int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def _float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


# ─────────────────────────────────────────────────────────────
# Telegram
# ─────────────────────────────────────────────────────────────
TELEGRAM_API_URL = "https://api.telegram.org"

# One bot that is an ADMIN of BOTH channels (Trial + VIP).
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")

# Channel ids (negative, e.g. -1001234567890). Same bot reads both.
TRIAL_CHANNEL_ID = os.getenv("TRIAL_CHANNEL_ID", "")
VIP_CHANNEL_ID = os.getenv("VIP_CHANNEL_ID", "")

# How often (seconds) to poll Telegram for new posts.
TELEGRAM_POLL_SECONDS = _int("TELEGRAM_POLL_SECONDS", 3)


# ─────────────────────────────────────────────────────────────
# Trading / signal rules (mirror the Phase-1 EA)
# ─────────────────────────────────────────────────────────────
# Symbol name as it exists on the brokers used by MetaApi.
# Most MT5 gold symbols are "XAUUSD"; override per deployment if needed.
TRADE_SYMBOL = os.getenv("TRADE_SYMBOL", "XAUUSD")

# Minutes allowed for price to enter the entry zone before a signal is
# discarded (Phase-1 rule = 5 minutes, measured in UTC).
ENTRY_WINDOW_SECONDS = _int("ENTRY_WINDOW_SECONDS", 300)

# Number of positions opened per client per signal.
POSITIONS_PER_SIGNAL = _int("POSITIONS_PER_SIGNAL", 3)

# Base magic number; each signal gets a unique magic derived from this.
MAGIC_BASE = _int("MAGIC_BASE", 990000)

# Order comment prefix (also used to identify NEXORA positions).
ORDER_COMMENT_PREFIX = os.getenv("ORDER_COMMENT_PREFIX", "NEXORA")


# ─────────────────────────────────────────────────────────────
# Risk profiles — lot multiplier applied on top of a client's lot size.
# ─────────────────────────────────────────────────────────────
RISK_MULTIPLIERS = {
    "conservative": _float("RISK_CONSERVATIVE", 0.5),
    "balanced": _float("RISK_BALANCED", 1.0),
    "aggressive": _float("RISK_AGGRESSIVE", 2.0),
}


def risk_multiplier(profile: str) -> float:
    return RISK_MULTIPLIERS.get((profile or "balanced").lower(), 1.0)


# ─────────────────────────────────────────────────────────────
# Trial / license
# ─────────────────────────────────────────────────────────────
TRIAL_DAYS = _int("TRIAL_DAYS", 3)                 # 3-day free trial
DEFAULT_LICENSE_DAYS = _int("DEFAULT_LICENSE_DAYS", 30)

# On trial/license expiry: block only NEW trades, let open trades finish
# naturally (TP1/SL). Set to True to force-close everything on expiry.
CLOSE_POSITIONS_ON_EXPIRY = os.getenv("CLOSE_POSITIONS_ON_EXPIRY", "false").lower() == "true"


# ─────────────────────────────────────────────────────────────
# MetaApi account provisioning
# ─────────────────────────────────────────────────────────────
# Give each managed account a dedicated IP? Costs more on MetaApi.
USE_DEDICATED_IP = os.getenv("USE_DEDICATED_IP", "false").lower() == "true"


def as_dict() -> dict:
    """Snapshot of non-secret config for the dashboard / logs."""
    return {
        "trade_symbol": TRADE_SYMBOL,
        "entry_window_seconds": ENTRY_WINDOW_SECONDS,
        "positions_per_signal": POSITIONS_PER_SIGNAL,
        "risk_multipliers": RISK_MULTIPLIERS,
        "trial_days": TRIAL_DAYS,
        "default_license_days": DEFAULT_LICENSE_DAYS,
        "close_positions_on_expiry": CLOSE_POSITIONS_ON_EXPIRY,
        "trial_channel_set": bool(TRIAL_CHANNEL_ID),
        "vip_channel_set": bool(VIP_CHANNEL_ID),
        "bot_token_set": bool(TELEGRAM_BOT_TOKEN),
    }
