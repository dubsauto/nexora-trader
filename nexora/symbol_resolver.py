# nexora/symbol_resolver.py
#
# Automatically resolves the broker-specific symbol name on each account.
#
# Signals name an instrument generically (e.g. "GOLD (XAUUSD)"), but every
# broker names it slightly differently — XAUUSD, XAUUSD.m, XAUUSDm, GOLD#,
# GOLD.spot, BTCUSD, BTCUSD#, BTCUSD.pro … This module looks at the symbols
# actually available on a client's account and picks the matching one, so the
# admin never has to map symbols per broker by hand.
#
# Matching: a broker symbol matches a candidate (the instrument base or one of
# its aliases) when it equals it, or starts with it followed only by a
# "suffix" — a separator (. # _ - /) or a short lowercase tag (m, pro, c…).
# This accepts BTCUSD# / BTCUSD.m / BTCUSDm but rejects BTCUSDT (Tether).

_SEP = set(".#_-/ ")


def _match_score(broker_symbol: str, candidate: str) -> int:
    """2 = exact, 1 = suffix match, 0 = no match."""
    if not broker_symbol or not candidate:
        return 0
    s, c = broker_symbol, candidate
    if s.upper() == c.upper():
        return 2
    if s.upper().startswith(c.upper()):
        rem = s[len(c):]
        if not rem:
            return 2
        if rem[0] in _SEP:
            return 1
        if rem.isalpha() and rem.islower() and len(rem) <= 4:
            return 1
    return 0


def resolve_symbol(broker_symbols, base, aliases):
    """Pick the best broker symbol for the instrument (base + aliases).
    Prefers an exact match, then the shortest suffix match. Returns None."""
    candidates = [base] + [a for a in (aliases or []) if a]
    best, best_score = None, 0
    for s in broker_symbols or []:
        sc = 0
        for cand in candidates:
            v = _match_score(s, cand)
            if v > sc:
                sc = v
        if sc == 0:
            continue
        if sc > best_score or (sc == best_score and (best is None or len(s) < len(best))):
            best, best_score = s, sc
    return best


# Cache resolved names per (account_id, base) — broker symbol lists don't change
# within a session, and get_symbols() is a relatively heavy call.
_cache: dict = {}


async def resolve_for_account(conn, account_id, base, aliases, override=None):
    """Return the broker symbol to trade for `base` on this account.
    Order: explicit override → cache → live lookup on the account. None if the
    broker has no matching symbol."""
    if override:
        return override

    key = (account_id, base)
    if key in _cache:
        return _cache[key]

    try:
        broker_symbols = await conn.get_symbols()
    except Exception as e:
        print(f"[SymbolResolver] get_symbols failed for {account_id}: {e}")
        return None

    resolved = resolve_symbol(broker_symbols, base, aliases)
    if resolved:
        _cache[key] = resolved
    return resolved


def clear_cache(account_id=None):
    if account_id is None:
        _cache.clear()
    else:
        for k in list(_cache.keys()):
            if k[0] == account_id:
                _cache.pop(k, None)


def prime_from_db():
    """Load previously-resolved symbols from the database into the cache on
    worker startup, so a restart does NOT re-resolve every account (which would
    be a bottleneck as the client base grows). Resolve once, remember forever —
    a live lookup only happens for instrument/account pairs not yet stored."""
    from app.database import SessionLocal
    from app.model import Client
    db = SessionLocal()
    try:
        n = 0
        rows = db.query(Client).filter(Client.metaapi_account_id.isnot(None)).all()
        for c in rows:
            for base, broker in (c.resolved_symbols or {}).items():
                if broker:
                    _cache[(c.metaapi_account_id, base)] = broker
                    n += 1
        print(f"[SymbolResolver] primed {n} symbol mapping(s) from DB")
    except Exception as e:
        print(f"[SymbolResolver] prime_from_db error: {e}")
    finally:
        db.close()
