# nexora/trade_history.py
#
# Opportunistic trade-history capture. We do NOT deploy accounts just to read
# history — that would defeat the low-cost on-demand model. Instead, whenever
# the platform ALREADY holds a live connection (a signal is running, or the
# admin/client triggered a manual op that deploys the account), we take the
# chance to fill in entry/close/profit for that client's trade groups.
#
# As a result, a trade's final P&L is captured the next time its account is
# deployed after it closes — not in real time. This is by design.

from datetime import datetime, timedelta

from app.database import SessionLocal
from app.model import TradeGroup, Client


def _num(x) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return 0.0


async def sync_client_history(conn, client_id: int):
    """Best-effort: update this client's recent TradeGroups from the live
    connection `conn`. Reads open positions (floating P/L + entry price) and
    the account's recent deals (realized P/L + entry/close price/time), keyed
    back to each group by its stored position tickets. Never raises."""
    try:
        # ---- snapshot recent groups (short session) ----
        db = SessionLocal()
        try:
            client = db.query(Client).get(client_id)
            acc_id = client.metaapi_account_id if client else None
            if not acc_id:
                return
            # only groups on the currently-connected account (avoids mapping
            # this account's deals onto a previous/test account's trades)
            groups = (db.query(TradeGroup)
                      .filter(TradeGroup.client_id == client_id,
                              TradeGroup.account_id == acc_id)
                      .order_by(TradeGroup.id.desc()).limit(80).all())
            snap = [{"id": g.id, "magic": int(g.magic or 0),
                     "tickets": [str(t) for t in (g.tickets or [])],
                     "opened_at": g.opened_at, "entry_price": g.entry_price}
                    for g in groups]
        finally:
            db.close()
        if not snap:
            return

        ticket_to_gid = {t: g["id"] for g in snap for t in g["tickets"]}
        magic_to_gid = {g["magic"]: g["id"] for g in snap if g["magic"]}

        def _gid_for(position_id, magic):
            gid = ticket_to_gid.get(str(position_id or ""))
            if gid is None and magic:
                gid = magic_to_gid.get(int(magic))
            return gid

        # ---- 1) open positions: floating P/L, entry price, still-open flag ----
        try:
            positions = await conn.get_positions()
        except Exception:
            positions = []
        floating, entry_sum, open_count = {}, {}, {}
        for p in positions:
            gid = _gid_for(p.get("id"), p.get("magic", 0))
            if gid is None:
                continue
            floating[gid] = floating.get(gid, 0.0) + _num(p.get("profit")) + _num(p.get("swap"))
            op = p.get("openPrice")
            if op is not None:
                s, cnt = entry_sum.get(gid, (0.0, 0))
                entry_sum[gid] = (s + _num(op), cnt + 1)
            open_count[gid] = open_count.get(gid, 0) + 1

        # ---- 2) recent deals: realized P/L, entry (IN) + close (OUT) ----
        since = min((g["opened_at"] for g in snap if g["opened_at"]), default=None)
        since = (since or datetime.utcnow() - timedelta(days=7)) - timedelta(minutes=5)
        deals = []
        try:
            res = await conn.get_deals_by_time_range(since, datetime.utcnow())
            deals = (res.get("deals") if isinstance(res, dict) else res) or []
        except Exception:
            deals = []
        realized, in_sum, out_last = {}, {}, {}
        for d in deals:
            gid = _gid_for(d.get("positionId"), d.get("magic", 0))
            if gid is None:
                continue
            realized[gid] = (realized.get(gid, 0.0) + _num(d.get("profit"))
                             + _num(d.get("commission")) + _num(d.get("swap")))
            et = d.get("entryType") or ""
            price = d.get("price")
            if et == "DEAL_ENTRY_IN" and price is not None:
                s, cnt = in_sum.get(gid, (0.0, 0))
                in_sum[gid] = (s + _num(price), cnt + 1)
            elif et in ("DEAL_ENTRY_OUT", "DEAL_ENTRY_OUT_BY", "DEAL_ENTRY_INOUT"):
                t = d.get("time")
                prev = out_last.get(gid)
                if prev is None or (t and prev[0] and t > prev[0]) or (t and not prev[0]):
                    out_last[gid] = (t, price)

        if not (floating or realized):
            return

        # ---- 3) write updates (short session) ----
        db = SessionLocal()
        try:
            for g in snap:
                gid = g["id"]
                grp = db.query(TradeGroup).get(gid)
                if not grp:
                    continue

                if gid in realized or gid in floating:
                    grp.profit = round(realized.get(gid, 0.0) + floating.get(gid, 0.0), 2)

                if grp.entry_price is None:
                    if entry_sum.get(gid, (0, 0))[1]:
                        grp.entry_price = round(entry_sum[gid][0] / entry_sum[gid][1], 5)
                    elif in_sum.get(gid, (0, 0))[1]:
                        grp.entry_price = round(in_sum[gid][0] / in_sum[gid][1], 5)

                # Fully closed: no open positions AND we have an exit deal.
                if open_count.get(gid, 0) == 0 and gid in out_last:
                    t, price = out_last[gid]
                    if price is not None:
                        grp.close_price = round(_num(price), 5)
                    if isinstance(t, datetime) and not grp.closed_at:
                        grp.closed_at = t
                    grp.state = "closed"
            db.commit()
        finally:
            db.close()
    except Exception as e:
        print(f"[TradeHistory] sync error for client {client_id}: {e}", flush=True)
