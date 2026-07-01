# nexora/signal_parser.py
#
# Parses a Telegram signal message into a structured signal.
# This is a direct port of the Phase-1 MQL5 EA logic:
#
#   GOLD (XAUUSD) – BUY
#   ✅Entry: 4610 – 4606
#   ❗️SL: 4605 ( 50 pips )
#   Targets:
#   4620 ( 100 Pips )
#   4630 ...
#
# It reads the direction (BUY/SELL), the two entry numbers, the SL,
# and the FIRST target (TP1 = nearest). Emojis / bars / dashes are
# ignored — only the ASCII anchors and numbers matter. The signal is
# rejected if SL/TP are on the wrong side of the entry (sanity check).

import re
from dataclasses import dataclass
from typing import Optional


@dataclass
class ParsedSignal:
    direction: str          # "BUY" | "SELL"
    entry_low: float
    entry_high: float
    sl: float
    tp1: float


_NUMBER_RE = re.compile(r"\d+(?:\.\d+)?")


def _first_number(text: str) -> Optional[float]:
    m = _NUMBER_RE.search(text)
    return float(m.group()) if m else None


def _first_two_numbers(text: str):
    nums = _NUMBER_RE.findall(text)
    if len(nums) < 2:
        return None, None
    return float(nums[0]), float(nums[1])


def parse_signal(text: str) -> Optional[ParsedSignal]:
    """Return a ParsedSignal, or None if the message is not a valid signal."""
    if not text:
        return None

    upper = text.upper()

    # --- direction ---------------------------------------------------------
    if "SELL" in upper:
        direction = "SELL"
    elif "BUY" in upper:
        direction = "BUY"
    else:
        return None

    # --- locate section anchors (case-insensitive) -------------------------
    e_idx = upper.find("ENTRY:")
    s_idx = upper.find("SL:")
    t_idx = upper.find("TARGET")   # matches "Targets:" / "Target:"
    if e_idx == -1 or s_idx == -1 or t_idx == -1:
        return None

    # order must be Entry -> SL -> Targets for the slicing to be meaningful
    e_end = e_idx + len("ENTRY:")
    if not (e_end <= s_idx < t_idx):
        return None

    entry_part = text[e_end:s_idx]
    sl_part = text[s_idx + len("SL:"):t_idx]
    tgt_part = text[t_idx:]
    # skip the word "Targets:" itself before reading the first number
    colon = tgt_part.find(":")
    if colon != -1:
        tgt_part = tgt_part[colon + 1:]

    a, b = _first_two_numbers(entry_part)
    sl = _first_number(sl_part)
    tp1 = _first_number(tgt_part)

    if a is None or b is None or sl is None or tp1 is None:
        return None
    if a <= 0 or b <= 0 or sl <= 0 or tp1 <= 0:
        return None

    entry_low, entry_high = min(a, b), max(a, b)

    # --- direction consistency (SL/TP on the correct side) -----------------
    if direction == "BUY":
        if not (sl < entry_low and tp1 > entry_high):
            return None
    else:  # SELL
        if not (sl > entry_high and tp1 < entry_low):
            return None

    return ParsedSignal(
        direction=direction,
        entry_low=entry_low,
        entry_high=entry_high,
        sl=sl,
        tp1=tp1,
    )
