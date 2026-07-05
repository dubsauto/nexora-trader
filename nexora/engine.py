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
# Each signal is handled in its own asyncio task, so multiple signals run
# independently and never mix up.

import asyncio
import time
from datetime import datetime

from nexora import config
from app.database import SessionLocal
from app.model import Signal, Client, TradeGroup, ActivityLog
from app.services.trading import trader
from nexora.deploy_manager import deploy_manager


def _log(db, category, action, message, client_id=None, signal_id=None):
    db.add(ActivityLog(actor="engine", category=category, action=action,
                       message=message, client_id=client_id, signal_id=signal_id))
    db.commit()


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
            waiting = db.query(Signal).filter(Signal.state == "waiting").all()
            ids = [s.id for s in waiting]
        finally:
            db.close()

        for sid in ids:
            async with self._lock:
                if sid in self._active:
                    continue
                self._active.add(sid)
            asyncio.create_task(self._handle_signal(sid))

    # ============================================================
    # SIGNAL LIFECYCLE
    # ============================================================
    async def _handle_signal(self, signal_id: int):
        db = SessionLocal()
        connections: dict[str, object] = {}   # metaapi_account_id -> connection
        try:
            sig = db.query(Signal).get(signal_id)
            if not sig or sig.state != "waiting":
                return

            # 1) eligible clients
            clients = db.query(Client).filter(Client.channel == sig.channel).all()
            eligible = [c for c in clients
                        if c.is_eligible(sig.channel) and c.metaapi_account_id]

            if not eligible:
                sig.state = "done"
                db.commit()
                _log(db, "signal", "no_clients",
                     f"No eligible {sig.channel} clients — signal skipped",
                     signal_id=sig.id)
                return

            _log(db, "signal", "processing",
                 f"{sig.symbol} {sig.direction} {sig.channel} — "
                 f"{len(eligible)} eligible client(s), deploying",
                 signal_id=sig.id)

            # 2) acquire (deploy + connect) all accounts in parallel. The
            #    deploy_manager reference-counts, so accounts shared with other
            #    concurrent signals are deployed once and undeployed only when
            #    the last signal releases them.
            acq_results = await asyncio.gather(
                *[deploy_manager.acquire(c.metaapi_account_id) for c in eligible],
                return_exceptions=True,
            )
            active_clients = []
            for c, res in zip(eligible, acq_results):
                if isinstance(res, Exception) or res is None:
                    _log(db, "client", "connect_failed",
                         f"Deploy/connect failed: {res}", client_id=c.id, signal_id=sig.id)
                    continue
                connections[c.metaapi_account_id] = res   # account_id -> connection
                active_clients.append(c)
            if not active_clients:
                sig.state = "expired"
                db.commit()
                _log(db, "signal", "deploy_failed",
                     "No accounts could be deployed/connected — signal dropped",
                     signal_id=sig.id)
                return

            # 3) watch entry zone within the 5-minute window
            filled = await self._wait_for_entry(db, sig, active_clients, connections)
            if not filled:
                sig.state = "expired"
                db.commit()
                _log(db, "signal", "window_expired",
                     "Price did not enter the zone within the window — discarded",
                     signal_id=sig.id)
                return

            # 4) place 3 positions per client (parallel)
            await asyncio.gather(
                *[self._open_positions(db, sig, c, connections[c.metaapi_account_id])
                  for c in active_clients],
                return_exceptions=True,
            )
            sig.state = "filled"
            db.commit()

            # 5) manage TP1 for all groups
            await self._manage_tp1(db, sig, active_clients, connections)

            sig.state = "done"
            db.commit()

        except Exception as e:
            print(f"[Engine] signal {signal_id} error: {e}")
        finally:
            # release every account we acquired (undeploy happens only when the
            # last concurrent signal on that account releases it)
            for acc_id in list(connections.keys()):
                try:
                    await deploy_manager.release(acc_id)
                except Exception as e:
                    print(f"[Engine] release {acc_id} error: {e}")
            if connections:
                try:
                    db.rollback()
                    _log(db, "signal", "released",
                         f"Released {len(connections)} account(s)", signal_id=signal_id)
                except Exception:
                    pass
            db.close()
            async with self._lock:
                self._active.discard(signal_id)

    # ============================================================
    # ENTRY ZONE
    # ============================================================
    async def _wait_for_entry(self, db, sig, clients, connections) -> bool:
        """Poll price until it enters [entry_low, entry_high] or the UTC
        window expires. Price is read via any healthy connection, reconnecting
        stale ones automatically. The window timeout bounds this loop."""
        got_price = False
        last_err = None
        warned = False
        while True:
            # window check (posted_at is UTC; utcnow is UTC)
            if sig.posted_at:
                elapsed = (datetime.utcnow() - sig.posted_at).total_seconds()
                if elapsed > config.ENTRY_WINDOW_SECONDS:
                    if not got_price:
                        # Never read a single price → almost always a wrong/missing
                        # broker symbol name. Make it visible instead of silent.
                        _log(db, "signal", "price_error",
                             f"Window expired but the {sig.symbol} price was never read. "
                             f"The broker symbol name is likely wrong or the symbol is "
                             f"not available on the account. Last error: {last_err}",
                             signal_id=sig.id)
                    return False

            price, err = await self._price_or_error(sig, connections)
            if price is not None:
                got_price = True
                ref = price["ask"] if sig.direction == "BUY" else price["bid"]
                if ref is not None and sig.entry_low <= ref <= sig.entry_high:
                    _log(db, "signal", "entered_zone",
                         f"Price {ref} entered zone [{sig.entry_low}-{sig.entry_high}]",
                         signal_id=sig.id)
                    return True
            else:
                last_err = err
                if not warned:
                    warned = True
                    _log(db, "signal", "price_error",
                         f"Cannot read {sig.symbol} price: {err}. If this persists, "
                         f"correct the broker symbol name in the Symbols tab.",
                         signal_id=sig.id)

            await asyncio.sleep(1.5)

    # ============================================================
    # OPEN 3 POSITIONS FOR ONE CLIENT
    # ============================================================
    async def _open_positions(self, db, sig, client, conn):
        symbol = sig.symbol
        mult = config.risk_multiplier(client.risk_profile)
        lot = client.effective_lot(mult)

        group = TradeGroup(signal_id=sig.id, client_id=client.id, magic=0,
                           lot=lot, tickets=[], state="open",
                           opened_at=datetime.utcnow())
        db.add(group)
        db.flush()
        # Unique magic PER GROUP (per client+signal) so concurrent signals on
        # the same account never collide during TP1 management or closing.
        magic = config.MAGIC_BASE + group.id
        group.magic = magic

        tickets = []
        opened = 0
        for i in range(config.POSITIONS_PER_SIGNAL):
            comment = f"{config.ORDER_COMMENT_PREFIX}_{sig.id}_{i+1}"
            fn = trader.buy if sig.direction == "BUY" else trader.sell
            res = await fn(conn, symbol, lot, sl=sig.sl, tp=None,
                           comment=comment, magic=magic)
            if res.get("success"):
                opened += 1
                r = res.get("result") or {}
                tid = r.get("positionId") or r.get("orderId")
                if tid:
                    tickets.append(str(tid))
            else:
                group.last_error = str(res.get("error"))

        group.tickets = tickets
        if opened == 0:
            group.state = "closed"
            db.commit()
            _log(db, "trade", "open_failed",
                 f"{client.name}: could not open any position ({group.last_error})",
                 client_id=client.id, signal_id=sig.id)
        else:
            db.commit()
            _log(db, "trade", "opened",
                 f"{client.name}: opened {opened}/{config.POSITIONS_PER_SIGNAL} "
                 f"{sig.direction} @ lot {lot} (magic {magic})",
                 client_id=client.id, signal_id=sig.id)

    # ============================================================
    # TP1 MANAGEMENT — close 2, break-even the 3rd
    # ============================================================
    async def _manage_tp1(self, db, sig, clients, connections):
        symbol = sig.symbol
        pending = {c.id: c for c in clients}
        unhealthy_since = None   # monotonic ts of first continuous connection failure

        while pending:
            done_ids = []

            # 1) price drives the TP1 decision — reconnect stale connections
            price = await self._price_with_reconnect(sig, connections)
            if price is None:
                # broker unreachable — start/continue the give-up watchdog
                now = time.monotonic()
                unhealthy_since = unhealthy_since or now
                if now - unhealthy_since > config.TP1_GIVEUP_SECONDS:
                    _log(db, "signal", "tp1_giveup",
                         "Broker unreachable too long — stopped TP1 management. "
                         "Open positions remain protected by the broker stop-loss; "
                         "close manually if needed.", signal_id=sig.id)
                    break
                await asyncio.sleep(3)
                continue
            unhealthy_since = None   # healthy again

            if sig.direction == "BUY":
                tp1_hit = price["bid"] is not None and price["bid"] >= sig.tp1
            else:
                tp1_hit = price["ask"] is not None and price["ask"] <= sig.tp1

            for cid, client in list(pending.items()):
                acc_id = client.metaapi_account_id
                group = (db.query(TradeGroup)
                         .filter(TradeGroup.signal_id == sig.id,
                                 TradeGroup.client_id == cid).first())
                if not group or group.state != "open":
                    done_ids.append(cid)
                    continue

                conn = connections.get(acc_id)
                if conn is None:
                    continue   # leave pending; watchdog handles persistent failure

                # positions for THIS group's unique magic + symbol.
                # A fetch error means the connection is stale — NOT that the
                # positions are gone — so we reconnect and retry, and never
                # mark the group closed on a fetch failure.
                try:
                    positions = await self._positions_for_magic(conn, group.magic, symbol)
                except Exception:
                    fresh = await self._safe_reconnect(acc_id, connections)
                    if fresh is None:
                        continue   # keep it pending; try again next loop
                    try:
                        positions = await self._positions_for_magic(fresh, group.magic, symbol)
                        conn = fresh
                    except Exception:
                        continue

                if not positions:
                    group.state = "closed"     # genuinely gone (all hit SL, etc.)
                    db.commit()
                    done_ids.append(cid)
                    continue

                if tp1_hit:
                    ok = await self._close_two_breakeven_one(db, sig, client, conn, positions, group)
                    if ok:
                        done_ids.append(cid)
                    # if the close failed (stale mid-close), leave it pending to retry

            for cid in done_ids:
                pending.pop(cid, None)

            if pending:
                await asyncio.sleep(2)

    async def _close_two_breakeven_one(self, db, sig, client, conn, positions, group) -> bool:
        """Close all but one position and move the runner to break-even.
        Returns True only if every step succeeded — the caller retries on False
        (safe: re-running just finishes whatever is left)."""
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
            group.state = "tp1_done"
            group.tp1_at = datetime.utcnow()
            db.commit()
            _log(db, "trade", "tp1",
                 f"{client.name}: TP1 hit — closed {len(to_close)}, runner at break-even",
                 client_id=client.id, signal_id=sig.id)
            return True
        else:
            _log(db, "trade", "tp1_retry",
                 f"{client.name}: TP1 close hit a connection error — will retry",
                 client_id=client.id, signal_id=sig.id)
            return False

    # ============================================================
    # PRICE / RECONNECT HELPERS
    # ============================================================
    async def _price_or_error(self, sig, connections):
        """Return (price, None) from any healthy connection, or (None, error)
        with the last error seen — reconnecting stale connections once."""
        symbol = sig.symbol
        last = None
        for conn in list(connections.values()):
            try:
                return await trader.get_price(conn, symbol), None
            except Exception as e:
                last = str(e)
        for acc_id in list(connections.keys()):
            fresh = await self._safe_reconnect(acc_id, connections)
            if fresh is None:
                continue
            try:
                return await trader.get_price(fresh, symbol), None
            except Exception as e:
                last = str(e)
        return None, last

    async def _price_with_reconnect(self, sig, connections):
        price, _ = await self._price_or_error(sig, connections)
        return price

    async def _safe_reconnect(self, account_id, connections):
        """Rebuild a fresh connection for an already-acquired account."""
        try:
            fresh = await deploy_manager.reconnect(account_id)
            connections[account_id] = fresh
            return fresh
        except Exception as e:
            print(f"[Engine] reconnect {account_id} failed: {e}")
            return None

    async def _positions_for_magic(self, conn, magic, symbol=None):
        # NOTE: intentionally lets exceptions propagate — a fetch failure means
        # a stale connection, which the caller handles by reconnecting. It must
        # NOT be swallowed as "no positions" or a live group would be dropped.
        positions = await conn.get_positions()
        out = []
        for p in positions:
            if int(p.get("magic", 0) or 0) != int(magic):
                continue
            if symbol is not None and p.get("symbol") != symbol:
                continue
            out.append(p)
        return out


engine = TradeEngine()
