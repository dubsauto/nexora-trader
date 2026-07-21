# nexora/engine.py
#
# The heart of NEXORA. For each new signal it runs the AGREED on-demand flow:
#
#   1. Pick ELIGIBLE clients for the signal's channel.
#   2. Deploy their MetaApi accounts IN PARALLEL (on-demand — saves cost).
#   3. Watch the entry zone for up to 5 minutes (UTC).
#   4. When price enters the zone, open 3 positions per client (all with SL).
#   5. When TP1 is reached, close 2 positions and move the 3rd to break-even.
#   6. Undeploy all accounts (SL/BE are held broker-side, so this is safe).
#
# Each signal is handled in its own asyncio task. IMPORTANT: DB sessions are
# SHORT-LIVED — opened, used, and closed for each read/write. A session is NEVER
# held across an await (deploy, price wait, TP1 loop). Holding a connection for a
# signal's whole lifecycle exhausted the connection pool and held table locks.

import asyncio
import time
from dataclasses import dataclass
from datetime import datetime

from nexora import config
from app.database import SessionLocal
from app.model import Signal, Client, TradeGroup, ActivityLog, Symbol
from app.services.trading import trader
from nexora.deploy_manager import deploy_manager
from nexora import symbol_resolver
from nexora import metrics
from nexora import trade_history


# ─────────────────────────────────────────────────────────────
# Short-lived DB helpers (each opens + closes its own session)
# ─────────────────────────────────────────────────────────────
def _log(category, action, message, client_id=None, signal_id=None):
    # Print to stdout (visible in Render logs) AND persist to the Activity log.
    sig = f" sig#{signal_id}" if signal_id else ""
    print(f"[Engine]{sig} {category}/{action}: {message}", flush=True)
    db = SessionLocal()
    try:
        db.add(ActivityLog(actor="engine", category=category, action=action,
                           message=message, client_id=client_id, signal_id=signal_id))
        db.commit()
    except Exception:
        db.rollback()
    finally:
        db.close()


def _p(msg):
    """Lightweight stdout print for step-by-step tracing in Render logs."""
    print(f"[Engine] {msg}", flush=True)


def _set_signal_state(signal_id, state):
    db = SessionLocal()
    try:
        s = db.query(Signal).get(signal_id)
        if s:
            s.state = state
            db.commit()
    except Exception:
        db.rollback()
    finally:
        db.close()


def _persist_resolved(client_id, base, broker):
    db = SessionLocal()
    try:
        c = db.query(Client).get(client_id)
        if c:
            rs = dict(c.resolved_symbols or {})
            rs[base] = broker
            c.resolved_symbols = rs
            db.commit()
    except Exception:
        db.rollback()
    finally:
        db.close()


def _update_group_state(group_id, state, tp1_at=None):
    db = SessionLocal()
    try:
        g = db.query(TradeGroup).get(group_id)
        if g:
            g.state = state
            if tp1_at:
                g.tp1_at = tp1_at
            db.commit()
    except Exception:
        db.rollback()
    finally:
        db.close()


# ─────────────────────────────────────────────────────────────
# Plain data holders so we never carry ORM objects across awaits
# ─────────────────────────────────────────────────────────────
@dataclass
class SigData:
    id: int
    symbol: str
    direction: str
    entry_low: float
    entry_high: float
    sl: float
    tp1: float
    channel: str
    posted_at: object
    immediate: bool = False


@dataclass
class CliData:
    id: int
    name: str
    account_id: str
    risk_profile: str
    lot_size: float
    symbol_overrides: dict
    resolved_symbols: dict
    positions: int = 3      # trades opened per signal for this client


class TradeEngine:
    def __init__(self):
        self._active: set[int] = set()      # signal ids currently being handled
        self._lock = asyncio.Lock()

    # ============================================================
    # MAIN LOOP — spawn a handler task per new waiting signal
    # ============================================================
    async def tick(self):
        db = SessionLocal()
        try:
            ids = [s.id for s in db.query(Signal).filter(Signal.state == "waiting").all()]
        finally:
            db.close()

        for sid in ids:
            async with self._lock:
                if sid in self._active:
                    continue
                self._active.add(sid)
            _p(f"picking up signal #{sid}")
            asyncio.create_task(self._handle_signal(sid))

    # ============================================================
    # SIGNAL LIFECYCLE
    # ============================================================
    async def _handle_signal(self, signal_id: int):
        connections: dict[str, object] = {}   # account_id -> connection
        resolved: dict[str, str] = {}          # account_id -> broker symbol
        try:
            # ---- load signal + eligible clients (SHORT session) ----
            db = SessionLocal()
            try:
                sig = db.query(Signal).get(signal_id)
                if not sig or sig.state != "waiting":
                    return
                sd = SigData(sig.id, sig.symbol, sig.direction, sig.entry_low,
                             sig.entry_high, sig.sl, sig.tp1, sig.channel,
                             sig.posted_at, bool(sig.immediate))
                clients = db.query(Client).filter(Client.channel == sd.channel).all()
                eligible = [
                    CliData(c.id, c.name, c.metaapi_account_id, c.risk_profile,
                            c.lot_size or 0.01, dict(c.symbol_overrides or {}),
                            dict(c.resolved_symbols or {}),
                            int(c.positions_per_signal or config.POSITIONS_PER_SIGNAL))
                    for c in clients
                    if c.is_eligible(sd.channel) and c.metaapi_account_id
                ]
                sym_row = db.query(Symbol).filter(Symbol.name == sd.symbol).first()
                aliases = sym_row.alias_list() if sym_row else []
            finally:
                db.close()

            if not eligible:
                _set_signal_state(signal_id, "done")
                _log("signal", "no_clients",
                     f"No eligible {sd.channel} clients — signal skipped", signal_id=signal_id)
                return

            _log("signal", "processing",
                 f"{sd.symbol} {sd.direction} {sd.channel} — "
                 f"{len(eligible)} eligible client(s), deploying", signal_id=signal_id)

            # ---- acquire (deploy + connect) all in parallel ----
            acq = await asyncio.gather(
                *[deploy_manager.acquire(c.account_id) for c in eligible],
                return_exceptions=True)
            active = []
            for c, res in zip(eligible, acq):
                if isinstance(res, Exception) or res is None:
                    _log("client", "connect_failed",
                         f"Deploy/connect failed: {res}", client_id=c.id, signal_id=signal_id)
                    continue
                connections[c.account_id] = res
                active.append(c)
                _p(f"sig#{signal_id} connected {c.name} ({c.account_id})")
            _p(f"sig#{signal_id} {len(active)}/{len(eligible)} account(s) connected")
            if not active:
                _set_signal_state(signal_id, "expired")
                _log("signal", "deploy_failed",
                     "No accounts could be deployed/connected — signal dropped",
                     signal_id=signal_id)
                return

            # ---- resolve each broker's symbol name ----
            usable = []
            for c in active:
                override = c.symbol_overrides.get(sd.symbol)
                bs = await symbol_resolver.resolve_for_account(
                    connections[c.account_id], c.account_id, sd.symbol, aliases, override)
                if not bs:
                    _log("client", "symbol_error",
                         f"{c.name}: '{sd.symbol}' not found on this broker — skipped",
                         client_id=c.id, signal_id=signal_id)
                    continue
                resolved[c.account_id] = bs
                _p(f"sig#{signal_id} {c.name}: '{sd.symbol}' -> broker symbol '{bs}'")
                if c.resolved_symbols.get(sd.symbol) != bs:
                    _persist_resolved(c.id, sd.symbol, bs)
                usable.append(c)
            active = usable
            if not active:
                _set_signal_state(signal_id, "expired")
                _log("signal", "symbol_error",
                     f"No eligible broker offers '{sd.symbol}' — signal dropped",
                     signal_id=signal_id)
                return

            # ---- opportunistic account sync for the client portal (best-effort,
            #      we already hold live connections so this is nearly free) ----
            for c in active:
                try:
                    info = await asyncio.wait_for(
                        connections[c.account_id].get_account_information(), timeout=5)
                    metrics.record_metrics(c.id, info.get("balance"), info.get("equity"))
                except Exception:
                    pass
                # capture outcomes of PRIOR trades (e.g. a runner that closed
                # while the account was undeployed) now that we're connected
                try:
                    await trade_history.sync_client_history(connections[c.account_id], c.id)
                except Exception:
                    pass

            # ---- entry: immediate (market) opens right away; zone signals wait ----
            if sd.immediate:
                _p(f"sig#{signal_id} MARKET NOW — opening at current price immediately")
            elif not await self._wait_for_entry(sd, connections, resolved):
                _set_signal_state(signal_id, "expired")
                _log("signal", "window_expired",
                     "Price did not enter the zone within the window — discarded",
                     signal_id=signal_id)
                return

            # ---- open 3 positions per client ----
            await asyncio.gather(
                *[self._open_positions(sd, c, connections[c.account_id], resolved[c.account_id])
                  for c in active],
                return_exceptions=True)
            _set_signal_state(signal_id, "filled")

            # ---- manage TP1 (no DB session held across the loop) ----
            await self._manage_tp1(sd, active, connections, resolved)

            # capture this signal's trade outcomes (entry price + realized P/L of
            # the closed legs) while we still hold the connections
            for c in active:
                try:
                    await trade_history.sync_client_history(connections[c.account_id], c.id)
                except Exception:
                    pass
            _set_signal_state(signal_id, "done")

        except Exception as e:
            print(f"[Engine] signal {signal_id} error: {e}")
        finally:
            for acc_id in list(connections.keys()):
                try:
                    await deploy_manager.release(acc_id)
                except Exception as e:
                    print(f"[Engine] release {acc_id} error: {e}")
            if connections:
                _log("signal", "released",
                     f"Released {len(connections)} account(s)", signal_id=signal_id)
            async with self._lock:
                self._active.discard(signal_id)

    # ============================================================
    # ENTRY ZONE
    # ============================================================
    async def _wait_for_entry(self, sd, connections, resolved) -> bool:
        got_price = False
        last_err = None
        warned = False
        last_print = 0.0
        _p(f"sig#{sd.id} watching entry zone [{sd.entry_low}-{sd.entry_high}] for up to "
           f"{config.ENTRY_WINDOW_SECONDS}s")
        while True:
            if sd.posted_at:
                elapsed = (datetime.utcnow() - sd.posted_at).total_seconds()
                if elapsed > config.ENTRY_WINDOW_SECONDS:
                    if not got_price:
                        _log("signal", "price_error",
                             f"Window expired but the {sd.symbol} price was never read. "
                             f"The broker symbol name is likely wrong or the symbol is "
                             f"not available on the account. Last error: {last_err}",
                             signal_id=sd.id)
                    return False

            price, err = await self._price_or_error(connections, resolved)
            now = time.monotonic()
            if price is not None:
                got_price = True
                ref = price["ask"] if sd.direction == "BUY" else price["bid"]
                if now - last_print > 8:
                    last_print = now
                    _p(f"sig#{sd.id} {sd.symbol} price {ref} | zone [{sd.entry_low}-{sd.entry_high}] "
                       f"| waiting to enter")
                if ref is not None and sd.entry_low <= ref <= sd.entry_high:
                    _log("signal", "entered_zone",
                         f"Price {ref} entered zone [{sd.entry_low}-{sd.entry_high}]",
                         signal_id=sd.id)
                    return True
            else:
                last_err = err
                if now - last_print > 8:
                    last_print = now
                    _p(f"sig#{sd.id} {sd.symbol} price unavailable: {err}")
                if not warned:
                    warned = True
                    _log("signal", "price_error",
                         f"Cannot read {sd.symbol} price: {err}. If this persists, "
                         f"correct the broker symbol name in the Symbols tab.",
                         signal_id=sd.id)

            await asyncio.sleep(1.5)

    # ============================================================
    # OPEN N POSITIONS FOR ONE CLIENT (per-client count, default 3)
    # ============================================================
    async def _open_positions(self, sd, client, conn, broker_symbol):
        mult = config.risk_multiplier(client.risk_profile)
        lot = round((client.lot_size or 0.01) * mult, 2)
        count = max(1, int(client.positions or config.POSITIONS_PER_SIGNAL))

        # create the group first (short session) to get a unique per-group magic
        db = SessionLocal()
        try:
            group = TradeGroup(signal_id=sd.id, client_id=client.id, magic=0,
                               account_id=client.account_id,
                               lot=lot, tickets=[], state="open",
                               opened_at=datetime.utcnow())
            db.add(group)
            db.commit()
            group_id = group.id
            magic = config.MAGIC_BASE + group_id
            group.magic = magic
            db.commit()
        finally:
            db.close()

        # place orders (NO db session held during the network calls)
        tickets, opened, last_error = [], 0, None
        for i in range(count):
            comment = f"{config.ORDER_COMMENT_PREFIX}_{sd.id}_{i+1}"
            fn = trader.buy if sd.direction == "BUY" else trader.sell
            res = await fn(conn, broker_symbol, lot, sl=sd.sl, tp=None,
                           comment=comment, magic=magic)
            if res.get("success"):
                opened += 1
                r = res.get("result") or {}
                tid = r.get("positionId") or r.get("orderId")
                if tid:
                    tickets.append(str(tid))
            else:
                last_error = str(res.get("error"))

        # record the result (short session)
        db = SessionLocal()
        try:
            g = db.query(TradeGroup).get(group_id)
            if g:
                g.tickets = tickets
                g.state = "closed" if opened == 0 else "open"
                if last_error:
                    g.last_error = last_error
                db.commit()
        finally:
            db.close()

        if opened == 0:
            _log("trade", "open_failed",
                 f"{client.name}: could not open any position ({last_error})",
                 client_id=client.id, signal_id=sd.id)
        else:
            _log("trade", "opened",
                 f"{client.name}: opened {opened}/{count} "
                 f"{sd.direction} @ lot {lot} (magic {magic})",
                 client_id=client.id, signal_id=sd.id)

    # ============================================================
    # TP1 MANAGEMENT — close 2, break-even the 3rd
    # ============================================================
    async def _manage_tp1(self, sd, clients, connections, resolved):
        pending = {c.id: c for c in clients}
        unhealthy_since = None
        last_print = 0.0
        _p(f"sig#{sd.id} managing TP1 at {sd.tp1} for {len(pending)} client(s)")

        while pending:
            price = await self._price_with_reconnect(connections, resolved)
            now = time.monotonic()
            if price is not None and now - last_print > 8:
                last_print = now
                cur = price["bid"] if sd.direction == "BUY" else price["ask"]
                _p(f"sig#{sd.id} {sd.symbol} price {cur} | TP1 {sd.tp1} | {len(pending)} runner(s) pending")
            if price is None:
                now = time.monotonic()
                unhealthy_since = unhealthy_since or now
                if now - unhealthy_since > config.TP1_GIVEUP_SECONDS:
                    _log("signal", "tp1_giveup",
                         "Broker unreachable too long — stopped TP1 management. "
                         "Open positions remain protected by the broker stop-loss; "
                         "close manually if needed.", signal_id=sd.id)
                    break
                await asyncio.sleep(3)
                continue
            unhealthy_since = None

            if sd.direction == "BUY":
                tp1_hit = price["bid"] is not None and price["bid"] >= sd.tp1
            else:
                tp1_hit = price["ask"] is not None and price["ask"] <= sd.tp1

            # snapshot current group states (short session)
            db = SessionLocal()
            try:
                groups = {g.client_id: {"id": g.id, "magic": g.magic, "state": g.state}
                          for g in db.query(TradeGroup)
                          .filter(TradeGroup.signal_id == sd.id).all()}
            finally:
                db.close()

            done_ids = []
            for cid, client in list(pending.items()):
                g = groups.get(cid)
                if not g or g["state"] != "open":
                    done_ids.append(cid)
                    continue
                conn = connections.get(client.account_id)
                if conn is None:
                    continue

                try:
                    positions = await self._positions_for_magic(conn, g["magic"])
                except Exception:
                    fresh = await self._safe_reconnect(client.account_id, connections)
                    if fresh is None:
                        continue
                    try:
                        positions = await self._positions_for_magic(fresh, g["magic"])
                        conn = fresh
                    except Exception:
                        continue

                if not positions:
                    _update_group_state(g["id"], "closed")
                    done_ids.append(cid)
                    continue

                if tp1_hit:
                    ok = await self._close_two_breakeven_one(sd, client, conn, positions, g["id"])
                    if ok:
                        done_ids.append(cid)

            for cid in done_ids:
                pending.pop(cid, None)

            if pending:
                await asyncio.sleep(2)

    async def _close_two_breakeven_one(self, sd, client, conn, positions, group_id) -> bool:
        keep = positions[-1]
        to_close = positions[:-1]
        all_ok = True
        for p in to_close:
            r = await trader.close_position(conn, p.get("id"))
            if not r.get("success"):
                all_ok = False
        open_price = keep.get("openPrice")
        if open_price is not None:
            r = await trader.modify_position(conn, keep.get("id"), sl=open_price, tp=None)
            if not r.get("success"):
                all_ok = False

        if all_ok:
            _update_group_state(group_id, "tp1_done", tp1_at=datetime.utcnow())
            _log("trade", "tp1",
                 f"{client.name}: TP1 hit — closed {len(to_close)}, runner at break-even",
                 client_id=client.id, signal_id=sd.id)
            return True
        _log("trade", "tp1_retry",
             f"{client.name}: TP1 close hit a connection error — will retry",
             client_id=client.id, signal_id=sd.id)
        return False

    # ============================================================
    # PRICE / RECONNECT HELPERS
    # ============================================================
    async def _price_or_error(self, connections, resolved):
        last = None
        for acc_id, conn in list(connections.items()):
            sym = resolved.get(acc_id)
            if not sym:
                continue
            try:
                return await trader.get_price(conn, sym), None
            except Exception as e:
                last = str(e)
        for acc_id in list(connections.keys()):
            sym = resolved.get(acc_id)
            if not sym:
                continue
            fresh = await self._safe_reconnect(acc_id, connections)
            if fresh is None:
                continue
            try:
                return await trader.get_price(fresh, sym), None
            except Exception as e:
                last = str(e)
        return None, last

    async def _price_with_reconnect(self, connections, resolved):
        price, _ = await self._price_or_error(connections, resolved)
        return price

    async def _safe_reconnect(self, account_id, connections):
        try:
            fresh = await deploy_manager.reconnect(account_id)
            connections[account_id] = fresh
            return fresh
        except Exception as e:
            print(f"[Engine] reconnect {account_id} failed: {e}")
            return None

    async def _positions_for_magic(self, conn, magic):
        # Lets exceptions propagate — a fetch failure means a stale connection
        # (caller reconnects); it must NOT be read as "no positions".
        positions = await conn.get_positions()
        return [p for p in positions if int(p.get("magic", 0) or 0) == int(magic)]


engine = TradeEngine()
