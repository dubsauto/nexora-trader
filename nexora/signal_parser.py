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
    direction: str              # "BUY" | "SELL"
    entry_low: float            # 0 for immediate (market) signals
    entry_high: float           # 0 for immediate (market) signals
    sl: float
    tp1: float
    immediate: bool = False     # BUY NOW / SELL NOW -> open at market immediately


_NOW_RE = re.compile(r"\b(BUY|SELL)\s+NOW\b")


_NUMBER_RE = re.compile(r"\d+(?:\.\d+)?")


def _first_number(text: str) -> Optional[float]:
    m = _NUMBER_RE.search(text)
    return float(m.group()) if m else None


def _first_two_numbers(text: str):
    nums = _NUMBER_RE.findall(text)
    if len(nums) < 2:
        return None, None
    return float(nums[0]), float(nums[1])


def detect_symbol(text: str, symbols) -> Optional[str]:
    """Return the broker symbol name whose alias/name appears in the message.

    `symbols` is an iterable of (name, [aliases]). First match wins, so list
    more specific symbols first if aliases overlap. Returns None if nothing
    matches — the caller should then skip the signal (unknown instrument).
    """
    if not text:
        return None
    up = text.upper()
    for name, aliases in symbols:
        candidates = [name] + list(aliases or [])
        for token in candidates:
            if token and token.upper() in up:
                return name
    return None


def parse_signal(text: str) -> Optional[ParsedSignal]:
    """Return a ParsedSignal, or None if the message is not a valid signal.

    Two shapes are supported:
      - Zone signal:  Entry: A – B  +  SL  +  Targets  (wait for price in zone)
      - Market signal: "BUY NOW" / "SELL NOW"  +  SL  +  Targets  (open at once,
        no Entry line).
    """
    if not text:
        return None

    upper = text.upper()

    # --- immediate (market) signal? ---------------------------------------
    now_match = _NOW_RE.search(upper)
    if now_match:
        direction = now_match.group(1)   # "BUY" or "SELL"
        s_idx = upper.find("SL:")
        t_idx = upper.find("TARGET")
        if s_idx == -1 or t_idx == -1 or not (s_idx < t_idx):
            return None
        sl = _first_number(text[s_idx + len("SL:"):t_idx])
        tgt_part = text[t_idx:]
        colon = tgt_part.find(":")
        if colon != -1:
            tgt_part = tgt_part[colon + 1:]
        tp1 = _first_number(tgt_part)
        if sl is None or tp1 is None or sl <= 0 or tp1 <= 0:
            return None
        # sanity: SL protects the correct side (SELL: SL above TP; BUY: SL below TP)
        if direction == "SELL" and not (sl > tp1):
            return None
        if direction == "BUY" and not (sl < tp1):
            return None
        return ParsedSignal(direction=direction, entry_low=0.0, entry_high=0.0,
                            sl=sl, tp1=tp1, immediate=True)

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
