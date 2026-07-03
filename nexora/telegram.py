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
from nexora.signal_parser import parse_signal
from app.database import SessionLocal
from app.model import Signal, Setting, ActivityLog

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

        db = SessionLocal()
        new_signals = 0
        try:
            offset = _get_offset(db)
            async with httpx.AsyncClient(timeout=35) as client:
                r = await client.get(
                    f"{self._base}/getUpdates",
                    params={"offset": offset + 1, "timeout": 25,
                            "allowed_updates": '["channel_post"]'},
                )
            data = r.json()
            if not data.get("ok"):
                if data.get("error_code") == 409:
                    return -409
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

                # dedup
                exists = db.query(Signal).filter(Signal.update_id == uid).first()
                if exists:
                    continue

                sig = Signal(
                    update_id=uid,
                    channel=channel,
                    raw_text=text,
                    posted_at=posted_at,
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
                          f"[{channel}] {parsed.direction} zone "
                          f"{parsed.entry_low}-{parsed.entry_high} "
                          f"SL {parsed.sl} TP1 {parsed.tp1}",
                          signal_id=sig.id)
                new_signals += 1

            if highest > offset:
                _set_offset(db, highest)
            db.commit()
        except Exception as e:
            print(f"[Telegram] poll error: {e}")
            db.rollback()
        finally:
            db.close()
        return new_signals

    def _log(self, db, category, action, message, signal_id=None):
        db.add(ActivityLog(actor="engine", category=category,
                           action=action, message=message, signal_id=signal_id))


listener = TelegramListener()
