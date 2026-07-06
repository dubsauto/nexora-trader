# nexora/telegram.py
#
# Reads BOTH signal channels (Trial + VIP) with ONE bot token via the
# Telegram getUpdates long-poll, deduplicates by update_id, maps each
# channel_post to "trial"/"vip", parses it, and stores new Signal rows
# (state="waiting") for the trade engine to pick up.
#
# The bot must be an ADMIN of both channels.

import asyncio
from datetime import datetime, timezone

import httpx

from nexora import config
from nexora.signal_parser import parse_signal, detect_symbol
from app.database import SessionLocal
from app.model import Signal, Setting, ActivityLog, Symbol


def _enabled_symbols(db):
    """Return [(name, [aliases]), ...] for enabled symbols."""
    rows = db.query(Symbol).filter(Symbol.enabled == True).all()  # noqa: E712
    return [(s.name, s.alias_list()) for s in rows]

_OFFSET_KEY = "tg_offset"


def _get_offset(db) -> int:
    row = db.query(Setting).filter(Setting.key == _OFFSET_KEY).first()
    if row and row.value:
        try:
            return int(row.value)
        except ValueError:
            return 0
    return 0


def _set_offset(db, value: int):
    row = db.query(Setting).filter(Setting.key == _OFFSET_KEY).first()
    if not row:
        row = Setting(key=_OFFSET_KEY, value=str(value))
        db.add(row)
    else:
        row.value = str(value)
    db.commit()


def _set_setting(db, key, value):
    row = db.query(Setting).filter(Setting.key == key).first()
    if not row:
        db.add(Setting(key=key, value=str(value)))
    else:
        row.value = str(value)


def _write_heartbeat(status: str):
    """Record that the listener is alive (isolated session so it never
    interferes with the update-processing transaction)."""
    db = SessionLocal()
    try:
        _set_setting(db, "listener_heartbeat", datetime.utcnow().isoformat())
        _set_setting(db, "listener_status", status)
        db.commit()
    except Exception:
        db.rollback()
    finally:
        db.close()


def _channel_for_chat(chat_id: str) -> str | None:
    cid = str(chat_id)
    if config.VIP_CHANNEL_ID and cid == str(config.VIP_CHANNEL_ID):
        return "vip"
    if config.TRIAL_CHANNEL_ID and cid == str(config.TRIAL_CHANNEL_ID):
        return "trial"
    return None


class TelegramListener:
    def __init__(self):
        self._base = f"{config.TELEGRAM_API_URL}/bot{config.TELEGRAM_BOT_TOKEN}"

    def enabled(self) -> bool:
        return bool(config.TELEGRAM_BOT_TOKEN and
                    (config.TRIAL_CHANNEL_ID or config.VIP_CHANNEL_ID))

    async def prime_offset(self):
        """On first ever run, skip whatever is already sitting in the channel
        so old posts are not traded. Only runs if no offset is stored yet."""
        db = SessionLocal()
        try:
            if _get_offset(db) > 0:
                return
            async with httpx.AsyncClient(timeout=15) as client:
                r = await client.get(f"{self._base}/getUpdates",
                                     params={"offset": -1})
            data = r.json()
            if data.get("ok") and data.get("result"):
                last = data["result"][-1]["update_id"]
                _set_offset(db, last)
                print(f"[Telegram] Primed offset to {last} (skipping backlog)")
            else:
                _set_offset(db, 0)
        except Exception as e:
            print(f"[Telegram] prime_offset error: {e}")
        finally:
            db.close()

    async def poll_once(self) -> int:
        """Fetch new updates, store new signals.

        Returns the number of new signals on success, or a negative status:
          -409 -> another poller holds this bot token (conflict)
          -1   -> other transient error
        """
        if not self.enabled():
            return 0

        # Read the offset in a short session, then do the (up to 25s) long-poll
        # WITHOUT holding a DB connection open the whole time.
        db0 = SessionLocal()
        try:
            offset = _get_offset(db0)
        finally:
            db0.close()

        new_signals = 0
        status = "ok"
        db = None
        try:
            async with httpx.AsyncClient(timeout=35) as client:
                r = await client.get(
                    f"{self._base}/getUpdates",
                    params={"offset": offset + 1, "timeout": 25,
                            "allowed_updates": '["channel_post"]'},
                )
            data = r.json()
            # open a session only now, to process/store the results
            db = SessionLocal()
            if not data.get("ok"):
                if data.get("error_code") == 409:
                    status = "conflict"
                    return -409
                if data.get("error_code") == 429:
                    retry = int((data.get("parameters") or {}).get("retry_after", 5))
                    status = "rate_limited"
                    print(f"[Telegram] rate limited (429) — waiting {retry}s as requested")
                    await asyncio.sleep(retry)
                    return -1
                status = "error"
                print(f"[Telegram] getUpdates not ok: {data}")
                return -1

            highest = offset
            for upd in data.get("result", []):
                uid = upd.get("update_id", 0)
                if uid > highest:
                    highest = uid

                post = upd.get("channel_post")
                if not post:
                    continue

                chat_id = post.get("chat", {}).get("id")
                channel = _channel_for_chat(chat_id)
                if channel is None:
                    continue   # some other chat — ignore

                text = post.get("text", "")
                posted_at = datetime.fromtimestamp(
                    post.get("date", 0), tz=timezone.utc
                ).replace(tzinfo=None)

                parsed = parse_signal(text)
                if not parsed:
                    self._log(db, "signal", "ignored",
                              f"[{channel}] non-signal message ignored")
                    continue

                # which instrument is this signal for?
                symbol = detect_symbol(text, _enabled_symbols(db))
                if not symbol:
                    self._log(db, "signal", "ignored",
                              f"[{channel}] signal ignored — symbol not recognized "
                              f"(add it in the Symbols tab)")
                    continue

                # dedup
                exists = db.query(Signal).filter(Signal.update_id == uid).first()
                if exists:
                    continue

                sig = Signal(
                    update_id=uid,
                    channel=channel,
                    raw_text=text,
                    posted_at=posted_at,
                    symbol=symbol,
                    direction=parsed.direction,
                    entry_low=parsed.entry_low,
                    entry_high=parsed.entry_high,
                    sl=parsed.sl,
                    tp1=parsed.tp1,
                    state="waiting",
                )
                db.add(sig)
                db.flush()
                sig.magic = config.MAGIC_BASE + (sig.id % 100000)
                self._log(db, "signal", "received",
                          f"[{channel}] {symbol} {parsed.direction} zone "
                          f"{parsed.entry_low}-{parsed.entry_high} "
                          f"SL {parsed.sl} TP1 {parsed.tp1}",
                          signal_id=sig.id)
                new_signals += 1

            if highest > offset:
                _set_offset(db, highest)
            db.commit()
        except Exception as e:
            status = "error"
            print(f"[Telegram] poll error: {repr(e)}")
            if db is not None:
                db.rollback()
        finally:
            if db is not None:
                db.close()
            _write_heartbeat(status)   # mark listener alive regardless of outcome
        return new_signals

    def _log(self, db, category, action, message, signal_id=None):
        # "ignored" (non-signal chatter) is noisy — keep it in the Activity log
        # but don't print it to the Render logs.
        if action != "ignored":
            print(f"[Telegram] {category}/{action}: {message}", flush=True)
        db.add(ActivityLog(actor="engine", category=category,
                           action=action, message=message, signal_id=signal_id))


listener = TelegramListener()
