# -*- coding: utf-8 -*-
# E-GIRLZ Telegram Bot — full version with robust logging
# Aiogram v3

import os, json, base64, logging, time, html, re, aiohttp, sqlite3, asyncio, tempfile, csv, traceback, ssl
from datetime import datetime, timedelta
from urllib.parse import quote_plus
from typing import Any, Dict, List, Optional, Tuple
from contextlib import suppress

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    InputMediaPhoto, FSInputFile, WebAppInfo
)
from aiogram.filters import CommandStart, CommandObject, Command
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest
from dotenv import load_dotenv

import random
try:
    import certifi
except Exception:
    certifi = None

# ─── ENV ──────────────────────────────────────────────────────────────────────
load_dotenv(override=True)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("egirlz-bot")

BOT_TOKEN   = os.getenv("BOT_TOKEN")
SHOP_URL    = os.getenv("SHOP_URL", "https://egirlz.chat")
COUPON_CODE = os.getenv("COUPON_CODE", "LEAVE10")
GIRLS_MANIFEST_URL = os.getenv("GIRLS_MANIFEST_URL")
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0") or 0)
SLOT_NEWS_CHAT_ID = int(os.getenv("SLOT_NEWS_CHAT_ID", "0") or 0)
SUBSCRIBE_CHANNEL_ID = os.getenv("SUBSCRIBE_CHANNEL_ID", "").strip()
SUBSCRIBE_CHANNEL_URL = os.getenv("SUBSCRIBE_CHANNEL_URL", "").strip()
SUBSCRIBE_COUPON = (os.getenv("SUBSCRIBE_COUPON", "WELCOME20") or "WELCOME20").strip()
WC_API_URL = os.getenv("WC_API_URL", "").strip()  # e.g. https://site.com/wp-json/wc/v3
WC_CONSUMER_KEY = os.getenv("WC_CONSUMER_KEY", "").strip()
WC_CONSUMER_SECRET = os.getenv("WC_CONSUMER_SECRET", "").strip()
WC_DEFAULT_PRODUCT_ID = int(os.getenv("WC_DEFAULT_PRODUCT_ID", "0") or 0)
WC_FORCE_FEE_ONLY = str(os.getenv("WC_FORCE_FEE_ONLY", "0")).strip().lower() in {"1", "true", "yes", "on"}
TG_OPEN_URLS_AS_WEBAPP = str(os.getenv("TG_OPEN_URLS_AS_WEBAPP", "1")).strip().lower() in {"1", "true", "yes", "on"}
PLATEGA_BASE_URL = (os.getenv("PLATEGA_BASE_URL", "https://app.platega.io") or "https://app.platega.io").strip().rstrip("/")
PLATEGA_MERCHANT_ID = os.getenv("PLATEGA_MERCHANT_ID", "").strip()
PLATEGA_SECRET = os.getenv("PLATEGA_SECRET", "").strip()
VIP_PRICE = float(os.getenv("VIP_PRICE", "990") or 990)
VIP_CURRENCY = (os.getenv("VIP_CURRENCY", "RUB") or "RUB").strip().upper()
VIP_PAYMENT_METHOD = int(os.getenv("VIP_PAYMENT_METHOD", "2") or 2)  # 2 = SBP QR
VIP_RETURN_URL = (os.getenv("VIP_RETURN_URL", f"{SHOP_URL.rstrip('/')}/vip-success") or f"{SHOP_URL.rstrip('/')}/vip-success").strip()
VIP_FAILED_URL = (os.getenv("VIP_FAILED_URL", f"{SHOP_URL.rstrip('/')}/vip-fail") or f"{SHOP_URL.rstrip('/')}/vip-fail").strip()
DB_PATH = os.getenv("DB_PATH", "egirlz_bot.db")  # SQLite файл

COUPON_20 = (os.getenv("COUPON_20", "TODAY20") or "TODAY20").strip()
TRIAL_PRICE = (os.getenv("TRIAL_PRICE", "99₽") or "99₽").strip()
CAMPAIGN_COOLDOWN_HOURS = int(os.getenv("CAMPAIGN_COOLDOWN_HOURS", "24"))

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing. Set env var or use .env")
if not GIRLS_MANIFEST_URL:
    raise RuntimeError("GIRLS_MANIFEST_URL is missing")

# один админ — как просил
def is_admin(uid: int) -> bool:
    return uid == ADMIN_CHAT_ID and ADMIN_CHAT_ID != 0

# ─── AIOGRAM CORE ────────────────────────────────────────────────────────────
bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp  = Dispatcher()
rt  = Router()
dp.include_router(rt)

# ─── DATABASE (SQLite) ───────────────────────────────────────────────────────
def db_init():
    con = sqlite3.connect(DB_PATH)
    # users
    con.execute("""
        CREATE TABLE IF NOT EXISTS users (
            chat_id     INTEGER PRIMARY KEY,
            username    TEXT,
            first_name  TEXT,
            last_name   TEXT,
            added_at    INTEGER,
            last_reason TEXT,
            last_coupon TEXT,
            last_seen   INTEGER
        )
    """)
    # миграция last_seen на старую базу
    try:
        con.execute("ALTER TABLE users ADD COLUMN last_seen INTEGER")
    except Exception:
        pass
    # interest_once — фиксация, что админу уже слали "Интерес к анкете" для (user,girl)
    con.execute("""
        CREATE TABLE IF NOT EXISTS interest_once (
            chat_id    INTEGER NOT NULL,
            girl_id    INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            PRIMARY KEY(chat_id, girl_id)
        )
    """)

    # interests
    con.execute("""
        CREATE TABLE IF NOT EXISTS interests (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id    INTEGER NOT NULL,
            girl_id    INTEGER NOT NULL,
            source     TEXT,
            created_at INTEGER NOT NULL
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_interests_cg ON interests(chat_id, girl_id, created_at)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_interests_chat ON interests(chat_id, created_at)")

    # favorites
    con.execute("""
        CREATE TABLE IF NOT EXISTS favorites (
            chat_id    INTEGER NOT NULL,
            girl_id    INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            PRIMARY KEY(chat_id, girl_id)
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_favorites_chat ON favorites(chat_id, created_at)")
    # slot subscriptions
    con.execute("""
        CREATE TABLE IF NOT EXISTS slot_subscriptions (
            chat_id         INTEGER NOT NULL,
            girl_id         INTEGER NOT NULL,
            known_slots     TEXT NOT NULL DEFAULT '[]',
            created_at      INTEGER NOT NULL,
            last_notified_at INTEGER,
            PRIMARY KEY(chat_id, girl_id)
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_slot_subs_chat ON slot_subscriptions(chat_id, created_at)")
    # per-girl channel state for "new slots" autopost
    con.execute("""
        CREATE TABLE IF NOT EXISTS slot_channel_state (
            girl_id        INTEGER PRIMARY KEY,
            known_slots    TEXT NOT NULL DEFAULT '[]',
            last_posted_at INTEGER
        )
    """)
    # recommendation push log (anti-spam)
    con.execute("""
        CREATE TABLE IF NOT EXISTS reco_push_log (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id    INTEGER NOT NULL,
            created_at INTEGER NOT NULL
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_reco_push_chat ON reco_push_log(chat_id, created_at)")
    # one-time reward for channel subscription
    con.execute("""
        CREATE TABLE IF NOT EXISTS subscribe_rewards (
            chat_id     INTEGER PRIMARY KEY,
            coupon      TEXT,
            created_at  INTEGER NOT NULL
        )
    """)
    # VIP payments created via Platega
    con.execute("""
        CREATE TABLE IF NOT EXISTS vip_payments (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id         INTEGER NOT NULL,
            transaction_id  TEXT,
            amount          REAL NOT NULL,
            currency        TEXT NOT NULL,
            status          TEXT,
            redirect_url    TEXT,
            created_at      INTEGER NOT NULL
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_vip_payments_chat ON vip_payments(chat_id, created_at)")

    # campaign log
    con.execute("""
        CREATE TABLE IF NOT EXISTS campaign_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            campaign TEXT NOT NULL,
            step_idx INTEGER NOT NULL,
            reason TEXT,
            girl_id INTEGER,
            payload_hash TEXT,
            sent_at INTEGER NOT NULL
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_campaign_log_key ON campaign_log(chat_id, campaign, step_idx, sent_at)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_campaign_log_payload ON campaign_log(chat_id, campaign, payload_hash, step_idx, sent_at)")

    # campaigns & steps (редактируемые в админке)
    con.execute("""
        CREATE TABLE IF NOT EXISTS campaigns (
            name TEXT PRIMARY KEY,
            title TEXT,
            enabled INTEGER DEFAULT 1,
            cooldown_hours INTEGER DEFAULT 24,
            updated_at INTEGER,
            created_at INTEGER
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS campaign_steps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            campaign_name TEXT NOT NULL,
            step_idx INTEGER NOT NULL,
            kind TEXT NOT NULL,          -- 'text' | 'photo'
            delay INTEGER NOT NULL,      -- seconds
            text TEXT,
            caption TEXT,
            image TEXT,
            buttons_json TEXT,           -- JSON [[{text,url?cb?},...],...]
            FOREIGN KEY(campaign_name) REFERENCES campaigns(name)
        )
    """)

    # editable settings (для купонов/прайсов)
    con.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)

    con.commit()
    con.close()

# безопасный “мгновенный” ответ на callback
async def ack(cb: CallbackQuery, text: str | None = None, alert: bool = False):
    with suppress(TelegramBadRequest):
        await cb.answer(text, show_alert=alert)

async def db_upsert_user(chat_id: int, username: str | None, first: str | None,
                         last: str | None, reason: str | None, coupon: str | None):
    now = int(time.time())
    def _op():
        con = sqlite3.connect(DB_PATH)
        con.execute("""
            INSERT INTO users (chat_id, username, first_name, last_name, added_at, last_reason, last_coupon, last_seen)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                username=excluded.username,
                first_name=excluded.first_name,
                last_name=excluded.last_name,
                last_reason=excluded.last_reason,
                last_coupon=excluded.last_coupon,
                last_seen=excluded.last_seen
        """, (chat_id, username, first, last, now, reason, coupon, now))
        con.commit()
        con.close()
    await asyncio.to_thread(_op)

def _touch_user(chat_id: int):
    def _op():
        con = sqlite3.connect(DB_PATH)
        con.execute("UPDATE users SET last_seen=? WHERE chat_id=?", (int(time.time()), chat_id))
        con.commit()
        con.close()
    return asyncio.to_thread(_op)

async def db_add_interest(chat_id: int, girl_id: int, source: str = "deeplink"):
    def _op():
        con = sqlite3.connect(DB_PATH)
        con.execute(
            "INSERT INTO interests (chat_id, girl_id, source, created_at) VALUES (?, ?, ?, ?)",
            (chat_id, girl_id, source, int(time.time()))
        )
        con.commit()
        con.close()
    await asyncio.to_thread(_op)

async def db_recent_interest_exists(chat_id: int, girl_id: int, within_sec: int = 24*3600) -> bool:
    """Есть ли у юзера интерес к этой девочке за последние within_sec секунд?"""
    cutoff = int(time.time()) - int(within_sec)
    def _op():
        con = sqlite3.connect(DB_PATH)
        try:
            cur = con.execute(
                "SELECT MAX(created_at) FROM interests WHERE chat_id=? AND girl_id=?",
                (chat_id, girl_id)
            )
            row = cur.fetchone()
            if not row or row[0] is None:
                return False
            last = int(row[0])
            # если вдруг в БД лежат миллисекунды — нормализуем
            if last > 10**12:  # ~ миллисекунды
                last //= 1000
            return last >= cutoff
        finally:
            con.close()
    return await asyncio.to_thread(_op)

async def db_interest_seen_once(chat_id: int, girl_id: int) -> bool:
    """
    True  — если для (chat_id, girl_id) уже когда-то слали "Интерес к анкете".
    False — если ещё не слали (и в этом случае помечаем как слали — один раз и навсегда).
    """
    def _op():
        con = sqlite3.connect(DB_PATH)
        try:
            cur = con.execute(
                "SELECT 1 FROM interest_once WHERE chat_id=? AND girl_id=?",
                (chat_id, girl_id)
            )
            exists = cur.fetchone() is not None
            if not exists:
                con.execute(
                    "INSERT INTO interest_once(chat_id, girl_id, created_at) VALUES (?,?,?)",
                    (chat_id, girl_id, int(time.time()))
                )
                con.commit()
            return exists
        finally:
            con.close()
    return await asyncio.to_thread(_op)

# ─── CAMPAIGN DB OPS ─────────────────────────────────────────────────────────
async def seed_campaigns_if_empty():
    """
    Умное сидирование:
    - создаёт записи кампаний из CAMPAIGNS, если их нет;
    - если кампания есть, но В БД НЕТ ШАГОВ — подсаживает дефолтные шаги;
    - не трогает существующие кастомные шаги.
    - сидирует дефолтные settings.
    """
    def _op():
        con = sqlite3.connect(DB_PATH)
        now = int(time.time())

        for name, steps in CAMPAIGNS.items():
            # ensure campaign exists
            cur = con.execute("SELECT COUNT(*), COALESCE(enabled,1), COALESCE(cooldown_hours,?) FROM campaigns WHERE name=?",
                              (CAMPAIGN_COOLDOWN_HOURS, name))
            row = cur.fetchone()
            if not row or row[0] == 0:
                con.execute(
                    "INSERT INTO campaigns(name,title,enabled,cooldown_hours,updated_at,created_at) VALUES(?,?,?,?,?,?)",
                    (name, name, 1, CAMPAIGN_COOLDOWN_HOURS, now, now)
                )
                log.info("SEED: inserted campaign '%s'", name)
            # ensure steps exist
            cur = con.execute("SELECT COUNT(*) FROM campaign_steps WHERE campaign_name=?", (name,))
            step_cnt = cur.fetchone()[0] or 0
            if step_cnt == 0:
                for i, st in enumerate(steps):
                    con.execute("""
                        INSERT INTO campaign_steps(campaign_name, step_idx, kind, delay, text, caption, image, buttons_json)
                        VALUES(?,?,?,?,?,?,?,?)
                    """, (
                        name, i,
                        st.get("kind","text"),
                        int(st.get("delay",0)),
                        st.get("text"),
                        st.get("caption"),
                        st.get("image"),
                        json.dumps(st.get("buttons",[]), ensure_ascii=False)
                    ))
                log.info("SEED: added %d default steps for '%s'", len(steps), name)

        # defaults for settings
        for k, v in (("COUPON_20", COUPON_20), ("TRIAL_PRICE", TRIAL_PRICE), ("CAMPAIGN_COOLDOWN_HOURS", str(CAMPAIGN_COOLDOWN_HOURS))):
            con.execute("INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)", (k, v))

        con.commit()
        con.close()
    await asyncio.to_thread(_op)

async def load_campaign_steps_from_db(campaign: str) -> Optional[List[Dict[str, Any]]]:
    def _op():
        con = sqlite3.connect(DB_PATH)
        try:
            cur = con.execute("SELECT enabled FROM campaigns WHERE name=?", (campaign,))
            row = cur.fetchone()
            if not row:
                # кампании нет в БД → None (значит будем fallback на дефолт)
                return None
            enabled = int(row[0] or 0)
            if not enabled:
                # выключена → пустой список (ничего слать)
                return []
            cur = con.execute("""
                SELECT step_idx, kind, delay, text, caption, image, buttons_json
                FROM campaign_steps
                WHERE campaign_name=?
                ORDER BY step_idx ASC
            """, (campaign,))
            steps = []
            for step_idx, kind, delay, text, caption, image, buttons_json in cur.fetchall():
                try:
                    buttons = json.loads(buttons_json) if buttons_json else []
                except Exception:
                    buttons = []
                steps.append({
                    "delay": int(delay or 0),
                    "kind": kind or "text",
                    "text": text,
                    "caption": caption,
                    "image": image,
                    "buttons": buttons
                })
            return steps
        finally:
            con.close()
    steps = await asyncio.to_thread(_op)
    if steps is None:
        log.info("CAMP '%s': not in DB, will use defaults", campaign)
    elif len(steps) == 0:
        log.warning("CAMP '%s': disabled or 0 steps in DB → nothing will be sent", campaign)
    else:
        log.debug("CAMP '%s': loaded %d steps from DB", campaign, len(steps))
    return steps

async def db_campaigns_list() -> List[Tuple[str,str,int,int]]:
    def _op():
        con = sqlite3.connect(DB_PATH)
        cur = con.execute("SELECT name,title,enabled,cooldown_hours FROM campaigns ORDER BY name")
        rows = cur.fetchall()
        con.close()
        return rows
    return await asyncio.to_thread(_op)

async def db_campaign_toggle(name: str, enable: Optional[bool]=None):
    def _op():
        con = sqlite3.connect(DB_PATH)
        if enable is None:
            con.execute("UPDATE campaigns SET enabled=1-enabled, updated_at=? WHERE name=?", (int(time.time()), name))
        else:
            con.execute("UPDATE campaigns SET enabled=?, updated_at=? WHERE name=?", (1 if enable else 0, int(time.time()), name))
        con.commit()
        con.close()
    await asyncio.to_thread(_op)

async def db_campaign_set_cooldown(name: str, hours: int):
    def _op():
        con = sqlite3.connect(DB_PATH)
        con.execute("UPDATE campaigns SET cooldown_hours=?, updated_at=? WHERE name=?", (hours, int(time.time()), name))
        con.commit()
        con.close()
    await asyncio.to_thread(_op)

async def db_campaign_steps(name: str) -> List[Dict[str, Any]]:
    def _op():
        con = sqlite3.connect(DB_PATH)
        cur = con.execute("""
            SELECT id, step_idx, kind, delay, text, caption, image, buttons_json
            FROM campaign_steps WHERE campaign_name=? ORDER BY step_idx
        """, (name,))
        rows = []
        for rid, idx, kind, delay, text, caption, image, buttons_json in cur.fetchall():
            rows.append({
                "id": rid, "step_idx": idx, "kind": kind, "delay": delay,
                "text": text, "caption": caption, "image": image,
                "buttons": json.loads(buttons_json) if buttons_json else []
            })
        con.close()
        return rows
    return await asyncio.to_thread(_op)

async def db_campaign_step_update(name: str, step_idx: int, fields: Dict[str, Any]):
    def _op():
        sets, vals = [], []
        if "kind" in fields: sets.append("kind=?"); vals.append(fields["kind"])
        if "delay" in fields: sets.append("delay=?"); vals.append(int(fields["delay"]))
        if "text" in fields: sets.append("text=?"); vals.append(fields["text"])
        if "caption" in fields: sets.append("caption=?"); vals.append(fields["caption"])
        if "image" in fields: sets.append("image=?"); vals.append(fields["image"])
        if "buttons" in fields: sets.append("buttons_json=?"); vals.append(json.dumps(fields["buttons"], ensure_ascii=False))
        vals.extend([name, step_idx])
        con = sqlite3.connect(DB_PATH)
        con.execute(f"UPDATE campaign_steps SET {', '.join(sets)} WHERE campaign_name=? AND step_idx=?", vals)
        con.commit()
        con.close()
    await asyncio.to_thread(_op)

async def db_campaign_step_add(name: str):
    def _op():
        con = sqlite3.connect(DB_PATH)
        cur = con.execute("SELECT COALESCE(MAX(step_idx),-1) FROM campaign_steps WHERE campaign_name=?", (name,))
        mx = cur.fetchone()[0]
        new_idx = (mx if mx is not None else -1) + 1
        con.execute("""
            INSERT INTO campaign_steps(campaign_name, step_idx, kind, delay, text, caption, image, buttons_json)
            VALUES(?,?,?,?,?,?,?,?)
        """, (name, new_idx, "text", 0, "Новый шаг", "", "", "[]"))
        con.commit()
        con.close()
    await asyncio.to_thread(_op)

async def db_campaign_step_delete(name: str, step_idx: int):
    def _op():
        con = sqlite3.connect(DB_PATH)
        con.execute("DELETE FROM campaign_steps WHERE campaign_name=? AND step_idx=?", (name, step_idx))
        # переиндексация
        cur = con.execute("SELECT id FROM campaign_steps WHERE campaign_name=? ORDER BY step_idx", (name,))
        ids = [r[0] for r in cur.fetchall()]
        for i, rid in enumerate(ids):
            con.execute("UPDATE campaign_steps SET step_idx=? WHERE id=?", (i, rid))
        con.commit()
        con.close()
    await asyncio.to_thread(_op)

async def db_campaign_step_move(name: str, step_idx: int, delta: int):
    def _op():
        con = sqlite3.connect(DB_PATH)
        cur = con.execute("SELECT id, step_idx FROM campaign_steps WHERE campaign_name=? ORDER BY step_idx", (name,))
        rows = cur.fetchall()
        n = len(rows)
        if n == 0 or step_idx < 0 or step_idx >= n:
            con.close(); return
        new_idx = max(0, min(n-1, step_idx + delta))
        if new_idx == step_idx:
            con.close(); return
        # swap indices
        id_a = rows[step_idx][0]
        id_b = rows[new_idx][0]
        con.execute("UPDATE campaign_steps SET step_idx=? WHERE id=?", (new_idx, id_a))
        con.execute("UPDATE campaign_steps SET step_idx=? WHERE id=?", (step_idx, id_b))
        con.commit()
        con.close()
    await asyncio.to_thread(_op)

# SETTINGS
async def settings_get(key: str, default: str) -> str:
    def _op():
        con = sqlite3.connect(DB_PATH)
        cur = con.execute("SELECT value FROM settings WHERE key=?", (key,))
        r = cur.fetchone()
        con.close()
        return r[0] if r and r[0] is not None else default
    return await asyncio.to_thread(_op)

async def settings_set(key: str, value: str):
    def _op():
        con = sqlite3.connect(DB_PATH)
        con.execute("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))
        con.commit()
        con.close()
    await asyncio.to_thread(_op)

# USERS STATS / LIST / EXPORT
async def db_users_stats() -> Dict[str,int]:
    now = int(time.time())
    def _op():
        con = sqlite3.connect(DB_PATH)
        cur = con.execute("SELECT COUNT(*) FROM users")
        total = cur.fetchone()[0]
        cur = con.execute("SELECT COUNT(*) FROM users WHERE last_seen>=?", (now-7*86400,))
        active7 = cur.fetchone()[0]
        cur = con.execute("SELECT COUNT(*) FROM users WHERE last_seen>=?", (now-30*86400,))
        active30 = cur.fetchone()[0]
        cur = con.execute("SELECT COUNT(*) FROM users WHERE added_at>=?", (now-24*3600,))
        new24 = cur.fetchone()[0]
        con.close()
        return {"total": total, "active7": active7, "active30": active30, "new24": new24}
    return await asyncio.to_thread(_op)

async def db_user_ids(segment: str) -> List[int]:
    now = int(time.time())
    def _op():
        con = sqlite3.connect(DB_PATH)
        if segment == "all":
            cur = con.execute("SELECT chat_id FROM users")
        elif segment == "active7":
            cur = con.execute("SELECT chat_id FROM users WHERE last_seen>=?", (now-7*86400,))
        else:  # active30
            cur = con.execute("SELECT chat_id FROM users WHERE last_seen>=?", (now-30*86400,))
        ids = [r[0] for r in cur.fetchall()]
        con.close()
        return ids
    return await asyncio.to_thread(_op)

async def db_users_list(limit: int = 50) -> List[Dict[str,Any]]:
    def _op():
        con = sqlite3.connect(DB_PATH)
        cur = con.execute("""
            SELECT chat_id, username, first_name, last_name, added_at, last_seen, last_reason, last_coupon
            FROM users ORDER BY last_seen DESC LIMIT ?
        """, (limit,))
        rows = []
        for r in cur.fetchall():
            rows.append({
                "chat_id": r[0], "username": r[1], "first_name": r[2], "last_name": r[3],
                "added_at": r[4], "last_seen": r[5], "last_reason": r[6], "last_coupon": r[7]
            })
        con.close()
        return rows
    return await asyncio.to_thread(_op)

async def db_favorite_add(chat_id: int, girl_id: int):
    def _op():
        con = sqlite3.connect(DB_PATH)
        con.execute(
            "INSERT OR IGNORE INTO favorites(chat_id, girl_id, created_at) VALUES (?,?,?)",
            (chat_id, girl_id, int(time.time()))
        )
        con.commit()
        con.close()
    await asyncio.to_thread(_op)

async def db_favorite_remove(chat_id: int, girl_id: int):
    def _op():
        con = sqlite3.connect(DB_PATH)
        con.execute("DELETE FROM favorites WHERE chat_id=? AND girl_id=?", (chat_id, girl_id))
        con.commit()
        con.close()
    await asyncio.to_thread(_op)

async def db_favorite_exists(chat_id: int, girl_id: int) -> bool:
    def _op():
        con = sqlite3.connect(DB_PATH)
        try:
            cur = con.execute("SELECT 1 FROM favorites WHERE chat_id=? AND girl_id=? LIMIT 1", (chat_id, girl_id))
            return cur.fetchone() is not None
        finally:
            con.close()
    return await asyncio.to_thread(_op)

async def db_favorites_list(chat_id: int) -> List[int]:
    def _op():
        con = sqlite3.connect(DB_PATH)
        try:
            cur = con.execute(
                "SELECT girl_id FROM favorites WHERE chat_id=? ORDER BY created_at DESC",
                (chat_id,)
            )
            return [int(r[0]) for r in cur.fetchall()]
        finally:
            con.close()
    return await asyncio.to_thread(_op)

async def db_user_last_seen(chat_id: int) -> Optional[int]:
    def _op():
        con = sqlite3.connect(DB_PATH)
        try:
            cur = con.execute("SELECT last_seen FROM users WHERE chat_id=?", (chat_id,))
            row = cur.fetchone()
            if not row or row[0] is None:
                return None
            return int(row[0])
        finally:
            con.close()
    return await asyncio.to_thread(_op)

async def db_slot_subscribe(chat_id: int, girl_id: int, known_slots: List[str]):
    payload = json.dumps(known_slots, ensure_ascii=False)
    def _op():
        con = sqlite3.connect(DB_PATH)
        con.execute(
            """
            INSERT INTO slot_subscriptions(chat_id, girl_id, known_slots, created_at, last_notified_at)
            VALUES (?,?,?,?,NULL)
            ON CONFLICT(chat_id, girl_id) DO UPDATE SET
                known_slots=excluded.known_slots
            """,
            (chat_id, girl_id, payload, int(time.time()))
        )
        con.commit()
        con.close()
    await asyncio.to_thread(_op)

async def db_slot_unsubscribe(chat_id: int, girl_id: int):
    def _op():
        con = sqlite3.connect(DB_PATH)
        con.execute("DELETE FROM slot_subscriptions WHERE chat_id=? AND girl_id=?", (chat_id, girl_id))
        con.commit()
        con.close()
    await asyncio.to_thread(_op)

async def db_slot_sub_exists(chat_id: int, girl_id: int) -> bool:
    def _op():
        con = sqlite3.connect(DB_PATH)
        try:
            cur = con.execute(
                "SELECT 1 FROM slot_subscriptions WHERE chat_id=? AND girl_id=? LIMIT 1",
                (chat_id, girl_id)
            )
            return cur.fetchone() is not None
        finally:
            con.close()
    return await asyncio.to_thread(_op)

async def db_slot_subscriptions() -> List[Dict[str, Any]]:
    def _op():
        con = sqlite3.connect(DB_PATH)
        try:
            cur = con.execute(
                "SELECT chat_id, girl_id, known_slots FROM slot_subscriptions"
            )
            rows = []
            for chat_id, girl_id, known_slots in cur.fetchall():
                rows.append({
                    "chat_id": int(chat_id),
                    "girl_id": int(girl_id),
                    "known_slots": str(known_slots or "[]")
                })
            return rows
        finally:
            con.close()
    return await asyncio.to_thread(_op)

async def db_slot_sub_update_known(chat_id: int, girl_id: int, known_slots: List[str], touched_notify: bool):
    payload = json.dumps(known_slots, ensure_ascii=False)
    def _op():
        con = sqlite3.connect(DB_PATH)
        if touched_notify:
            con.execute(
                "UPDATE slot_subscriptions SET known_slots=?, last_notified_at=? WHERE chat_id=? AND girl_id=?",
                (payload, int(time.time()), chat_id, girl_id)
            )
        else:
            con.execute(
                "UPDATE slot_subscriptions SET known_slots=? WHERE chat_id=? AND girl_id=?",
                (payload, chat_id, girl_id)
            )
        con.commit()
        con.close()
    await asyncio.to_thread(_op)

async def db_recent_interest_girl_ids(chat_id: int, limit: int = 20, within_sec: int = 7 * 24 * 3600) -> List[int]:
    cutoff = int(time.time()) - int(within_sec)
    def _op():
        con = sqlite3.connect(DB_PATH)
        try:
            cur = con.execute(
                """
                SELECT girl_id
                FROM interests
                WHERE chat_id=? AND created_at>=?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (chat_id, cutoff, int(limit))
            )
            out: List[int] = []
            for (gid,) in cur.fetchall():
                try:
                    out.append(int(gid))
                except Exception:
                    pass
            return out
        finally:
            con.close()
    return await asyncio.to_thread(_op)

async def db_recent_reco_sent(chat_id: int, within_sec: int = 6 * 3600) -> bool:
    cutoff = int(time.time()) - int(within_sec)
    def _op():
        con = sqlite3.connect(DB_PATH)
        try:
            cur = con.execute(
                "SELECT 1 FROM reco_push_log WHERE chat_id=? AND created_at>=? LIMIT 1",
                (chat_id, cutoff)
            )
            return cur.fetchone() is not None
        finally:
            con.close()
    return await asyncio.to_thread(_op)

async def db_mark_reco_sent(chat_id: int):
    def _op():
        con = sqlite3.connect(DB_PATH)
        con.execute(
            "INSERT INTO reco_push_log(chat_id, created_at) VALUES (?,?)",
            (chat_id, int(time.time()))
        )
        con.commit()
        con.close()
    await asyncio.to_thread(_op)

async def db_channel_state_get(girl_id: int) -> Optional[List[str]]:
    def _op():
        con = sqlite3.connect(DB_PATH)
        try:
            cur = con.execute("SELECT known_slots FROM slot_channel_state WHERE girl_id=?", (girl_id,))
            row = cur.fetchone()
            if not row:
                return None
            try:
                data = json.loads(row[0] or "[]")
                if isinstance(data, list):
                    return [str(x) for x in data]
                return []
            except Exception:
                return []
        finally:
            con.close()
    return await asyncio.to_thread(_op)

async def db_channel_state_set(girl_id: int, known_slots: List[str], posted_now: bool = False):
    payload = json.dumps(known_slots, ensure_ascii=False)
    ts = int(time.time()) if posted_now else None
    def _op():
        con = sqlite3.connect(DB_PATH)
        if ts is None:
            con.execute(
                """
                INSERT INTO slot_channel_state(girl_id, known_slots, last_posted_at)
                VALUES (?, ?, COALESCE((SELECT last_posted_at FROM slot_channel_state WHERE girl_id=?), NULL))
                ON CONFLICT(girl_id) DO UPDATE SET
                    known_slots=excluded.known_slots
                """,
                (girl_id, payload, girl_id)
            )
        else:
            con.execute(
                """
                INSERT INTO slot_channel_state(girl_id, known_slots, last_posted_at)
                VALUES (?, ?, ?)
                ON CONFLICT(girl_id) DO UPDATE SET
                    known_slots=excluded.known_slots,
                    last_posted_at=excluded.last_posted_at
                """,
                (girl_id, payload, ts)
            )
        con.commit()
        con.close()
    await asyncio.to_thread(_op)

async def db_sub_reward_exists(chat_id: int) -> bool:
    def _op():
        con = sqlite3.connect(DB_PATH)
        try:
            cur = con.execute("SELECT 1 FROM subscribe_rewards WHERE chat_id=? LIMIT 1", (chat_id,))
            return cur.fetchone() is not None
        finally:
            con.close()
    return await asyncio.to_thread(_op)

async def db_mark_sub_reward(chat_id: int, coupon: str):
    def _op():
        con = sqlite3.connect(DB_PATH)
        con.execute(
            "INSERT OR IGNORE INTO subscribe_rewards(chat_id, coupon, created_at) VALUES (?,?,?)",
            (chat_id, coupon, int(time.time()))
        )
        con.commit()
        con.close()
    await asyncio.to_thread(_op)

async def is_user_subscribed(user_id: int) -> bool:
    if not SUBSCRIBE_CHANNEL_ID:
        return False
    try:
        member = await bot.get_chat_member(SUBSCRIBE_CHANNEL_ID, user_id)
        return member.status in {"member", "administrator", "creator", "restricted"}
    except Exception as e:
        log.warning("is_user_subscribed failed: %s", e)
        return False

def platega_enabled() -> bool:
    return bool(PLATEGA_MERCHANT_ID and PLATEGA_SECRET)

async def db_add_vip_payment(chat_id: int, transaction_id: str | None, amount: float, currency: str, status: str | None, redirect_url: str | None):
    def _op():
        con = sqlite3.connect(DB_PATH)
        con.execute(
            """
            INSERT INTO vip_payments(chat_id, transaction_id, amount, currency, status, redirect_url, created_at)
            VALUES (?,?,?,?,?,?,?)
            """,
            (chat_id, transaction_id, float(amount), currency, status, redirect_url, int(time.time()))
        )
        con.commit()
        con.close()
    await asyncio.to_thread(_op)

async def platega_create_vip_payment(user) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Returns: (redirect_url, transaction_id, status)
    """
    if not platega_enabled():
        return None, None, None

    payload_info = {
        "source": "telegram_bot",
        "kind": "vip_subscription",
        "tg_user_id": str(user.id),
        "tg_username": str(getattr(user, "username", "") or ""),
    }
    body = {
        "paymentMethod": VIP_PAYMENT_METHOD,
        "paymentDetails": {
            "amount": float(VIP_PRICE),
            "currency": VIP_CURRENCY
        },
        "description": f"VIP подписка EGIRLZ для Telegram user {user.id}",
        "return": VIP_RETURN_URL,
        "failedUrl": VIP_FAILED_URL,
        "payload": json.dumps(payload_info, ensure_ascii=False),
    }

    headers = {
        "Content-Type": "application/json",
        "X-MerchantId": PLATEGA_MERCHANT_ID,
        "X-Secret": PLATEGA_SECRET,
    }
    url = f"{PLATEGA_BASE_URL}/transaction/process"
    try:
        async with aiohttp.ClientSession(connector=build_http_connector()) as s:
            async with s.post(url, json=body, headers=headers, timeout=25) as r:
                txt = await r.text()
                if r.status >= 400:
                    log.warning("Platega create failed status=%s body=%s", r.status, txt[:1000])
                    return None, None, None
                data = json.loads(txt)
    except Exception as e:
        log.warning("Platega create exception: %s", e)
        return None, None, None

    redirect = data.get("redirect")
    transaction_id = data.get("transactionId")
    status = data.get("status")
    return (str(redirect) if redirect else None, str(transaction_id) if transaction_id else None, str(status) if status else None)

def slot_key_to_human(key: str) -> str:
    try:
        date_s, start, end = key.split("|", 2)
        day = datetime.strptime(date_s, "%Y-%m-%d").strftime("%d.%m")
        return f"{day} {start} - {end}"
    except Exception:
        return key

# ─── HELPERS ─────────────────────────────────────────────────────────────────
def apply_link(coupon: str | None = None) -> str:
    base = SHOP_URL.rstrip("/")
    code = (coupon or COUPON_CODE).strip()
    return f"{base}/?coupon={quote_plus(code)}"

def wc_enabled() -> bool:
    return bool(WC_API_URL and WC_CONSUMER_KEY and WC_CONSUMER_SECRET)

def wc_store_base_url() -> str:
    if WC_API_URL and "/wp-json/" in WC_API_URL:
        return WC_API_URL.split("/wp-json/", 1)[0].rstrip("/")
    return SHOP_URL.rstrip("/")

def _girl_checkout_price(g: Dict[str, Any]) -> float:
    for k in ("from_price", "price"):
        v = g.get(k)
        if v is None:
            continue
        try:
            return float(v)
        except Exception:
            pass
    return 0.0

async def wc_create_order_for_girl(user, g: Dict[str, Any]) -> Tuple[Optional[int], Optional[str]]:
    """
    Creates WooCommerce order and returns (order_id, payment_url).
    payment_url can be None when order creation failed.
    """
    if not wc_enabled():
        return None, None

    price = _girl_checkout_price(g)
    currency = str(g.get("currency") or "RUB")
    product_id = int(g.get("wc_product_id") or WC_DEFAULT_PRODUCT_ID or 0)
    gname = str(g.get("name", "E-Girl"))

    payload: Dict[str, Any] = {
        "status": "pending",
        "currency": currency,
        "set_paid": False,
        "billing": {
            "first_name": str(getattr(user, "first_name", "") or ""),
            "last_name": str(getattr(user, "last_name", "") or ""),
            "email": f"tg_{int(user.id)}@example.local",
        },
        "customer_note": f"Telegram order for {gname}",
        "meta_data": [
            {"key": "source", "value": "telegram_bot"},
            {"key": "tg_user_id", "value": str(user.id)},
            {"key": "tg_username", "value": str(getattr(user, "username", "") or "")},
            {"key": "girl_id", "value": str(g.get("id", ""))},
            {"key": "girl_name", "value": gname},
        ],
    }

    if price > 0:
        if product_id > 0 and not WC_FORCE_FEE_ONLY:
            payload["line_items"] = [{
                "product_id": product_id,
                "quantity": 1,
                "subtotal": f"{price:.2f}",
                "total": f"{price:.2f}",
                "name": f"E-Girl booking: {gname}",
            }]
        else:
            payload["fee_lines"] = [{
                "name": f"E-Girl booking: {gname}",
                "total": f"{price:.2f}",
                "tax_status": "none",
            }]

    try:
        auth = aiohttp.BasicAuth(WC_CONSUMER_KEY, WC_CONSUMER_SECRET)
        async with aiohttp.ClientSession(auth=auth, connector=build_http_connector()) as s:
            async with s.post(f"{WC_API_URL.rstrip('/')}/orders", json=payload, timeout=25) as r:
                txt = await r.text()
                if r.status >= 400:
                    log.warning("Woo create order failed status=%s body=%s", r.status, txt[:1000])
                    return None, None
                data = json.loads(txt)
    except Exception as e:
        log.warning("Woo create order exception: %s", e)
        return None, None

    order_id = int(data.get("id")) if str(data.get("id", "")).isdigit() else None
    payment_url = data.get("payment_url") or ""
    if not payment_url and order_id and data.get("order_key"):
        payment_url = (
            f"{wc_store_base_url()}/checkout/order-pay/{order_id}/"
            f"?pay_for_order=true&key={quote_plus(str(data.get('order_key')))}"
        )
    return order_id, (str(payment_url) if payment_url else None)

def b64url_decode(s: str) -> bytes:
    s = s.replace('-', '+').replace('_', '/')
    s += '=' * (-len(s) % 4)
    return base64.b64decode(s)

CURRENCY = {"RUB": "₽", "USD": "$", "EUR": "€"}
def money(val: Any, cur: str | None) -> str:
    sym = CURRENCY.get((cur or "").upper(), cur or "")
    try:
        n = float(val)
    except Exception:
        return str(val)
    s = f"{int(n)}" if abs(n - int(n)) < 1e-9 else f"{n:.2f}"
    return f"{s} {sym}".strip()

def tg_user_link(u) -> str:
    if getattr(u, "username", None):
        return f"https://t.me/{u.username}"
    return f"tg://user?id={u.id}"

# ─── CACHE & HTTP ────────────────────────────────────────────────────────────
_manifest_cache: Dict[str, Any] = {}
_manifest_ts = 0.0
_slots_cache: Dict[str, Any] = {}
_slots_ts: Dict[str, float] = {}
TTL = 60  # сек

def girls_list(mf: Dict[str, Any]) -> List[Dict[str, Any]]:
    return list(mf.get("girls", []))

def girl_by_id(mf: Dict[str, Any], gid: int) -> Optional[Dict[str, Any]]:
    arr = girls_list(mf)
    total = len(arr)
    for i, x in enumerate(arr):
        try:
            if int(x.get("id")) == int(gid):
                y = x.copy()
                y["_index"] = i
                y["_total"] = total
                return y
        except Exception:
            continue
    return None

def girl_by_index(mf: Dict[str, Any], idx: int) -> Optional[Dict[str, Any]]:
    arr = girls_list(mf)
    if not arr: return None
    idx = idx % len(arr)
    g = arr[idx].copy()
    g["_index"] = idx
    g["_total"] = len(arr)
    return g

async def http_get_json(url: str) -> Any:
    connector = build_http_connector()
    async with aiohttp.ClientSession(connector=connector) as s:
        async with s.get(url, timeout=20) as r:
            r.raise_for_status()
            return await r.json()

def build_http_connector() -> aiohttp.TCPConnector:
    ssl_no_verify = str(os.getenv("SSL_NO_VERIFY", "0")).strip().lower() in {"1", "true", "yes", "on"}
    ssl_ctx: ssl.SSLContext | bool = True
    if ssl_no_verify:
        ssl_ctx = False
    elif certifi is not None:
        # Prefer certifi CA bundle on macOS/Python builds with broken system trust store.
        ssl_ctx = ssl.create_default_context(cafile=certifi.where())
    return aiohttp.TCPConnector(ssl=ssl_ctx)

async def get_manifest(force=False) -> Dict[str, Any]:
    global _manifest_cache, _manifest_ts
    now = time.time()
    if not force and _manifest_cache and now - _manifest_ts < TTL:
        return _manifest_cache
    try:
        data = await http_get_json(GIRLS_MANIFEST_URL)
        _manifest_cache = data or {}
        _manifest_ts = now
        log.info("MANIFEST: fetched %d girls", len(girls_list(_manifest_cache)))
        return _manifest_cache
    except Exception as e:
        log.exception("MANIFEST fetch failed: %s", e)
        return _manifest_cache or {}

async def get_slots(slot_json_url: str) -> Dict[str, Any]:
    now = time.time()
    if slot_json_url in _slots_cache and now - _slots_ts.get(slot_json_url, 0) < TTL:
        return _slots_cache[slot_json_url]
    try:
        data = await http_get_json(slot_json_url)
        _slots_cache[slot_json_url] = data or {}
        _slots_ts[slot_json_url] = now
        log.debug("SLOTS: fetched '%s'", slot_json_url)
        return _slots_cache[slot_json_url]
    except Exception as e:
        log.warning("SLOTS fetch failed for '%s': %s", slot_json_url, e)
        return {}

def profile_text(g: Dict[str, Any], slots: Dict[str, Any]) -> str:
    name = html.escape(str(g.get("name","")))
    cur = g.get("currency")
    if g.get("from_price"):
        price_line = f"от <b>{money(g['from_price'], cur)}</b>"
    elif g.get("price"):
        price_line = f"<b>{money(g['price'], cur)}</b>"
    else:
        price_line = "цена на сайте"
    desc = (g.get("acf", {}) or {}).get("description") or ""
    desc = html.escape(desc.strip())
    if len(desc) > 450:
        desc = desc[:447].rsplit(" ", 1)[0] + "…"

    slot_text = slots.get("slot_text") or g.get("slot_text") or ""
    lines = [f"<b>{name}</b>",
             f"{html.escape(slot_text)}",
             f"💸 {price_line}"]

    social = build_social_proof(g)
    if social:
        lines.append("")
        lines.extend(social)

    s_list = collect_available_slots(g, slots)
    if s_list:
        lines.append("\n<b>Ближайшие 7 часов:</b>")
        for s in s_list[:7]:
            lines.append(f"• {html.escape(s)}")

    if desc:
        lines += ["", desc]
    return "\n".join(lines)

def build_social_proof(g: Dict[str, Any]) -> List[str]:
    acf = (g.get("acf") or {})

    rating_raw = g.get("rating") or acf.get("rating")
    bookings_raw = g.get("bookings_count") or acf.get("bookings_count") or g.get("orders_count")
    review_raw = acf.get("review_short") or acf.get("review") or g.get("review")
    best_for_raw = acf.get("best_for") or g.get("best_for")

    cats = set(_cat_slugs(g))
    if rating_raw is None:
        if "bestseller" in cats:
            rating = "в топе каталога"
        elif "main" in cats:
            rating = "рекомендуемая анкета"
        else:
            rating = "новые отзывы добавляются"
    else:
        rating = str(rating_raw)

    if bookings_raw is None:
        bookings = "по запросу"
    else:
        bookings = str(bookings_raw)

    if not review_raw:
        review = "Приятный вайб и комфортное общение."
    else:
        review = str(review_raw).strip()
        if len(review) > 120:
            review = review[:117].rsplit(" ", 1)[0] + "..."

    if not best_for_raw:
        games = acf.get("favorite_games") or []
        if isinstance(games, list) and games:
            best_for = "совместных игр и тёплого общения"
        else:
            best_for = "уютного общения и приятного вечера"
    else:
        best_for = str(best_for_raw).strip()

    return [
        f"⭐ Рейтинг: {html.escape(rating)}",
        f"🏆 Бронирований: {html.escape(bookings)}",
        f"💬 Отзыв: {html.escape(review)}",
        f"🎯 Лучший выбор для: {html.escape(best_for)}",
    ]

def _parse_slot_dt(date_s: str | None, time_s: str | None) -> Optional[datetime]:
    if not date_s or not time_s:
        return None
    try:
        return datetime.strptime(f"{date_s} {time_s}", "%Y-%m-%d %H:%M")
    except Exception:
        return None

def _msk_now_naive() -> datetime:
    return datetime.utcnow() + timedelta(hours=3)

def _today_slots_from_ops_calendar(g: Dict[str, Any]) -> List[Tuple[datetime, str]]:
    now = _msk_now_naive()
    today_s = now.strftime("%Y-%m-%d")
    out: List[Tuple[datetime, str]] = []
    seen: set[str] = set()
    for day in (g.get("ops_calendar") or []):
        if str(day.get("date")) != today_s:
            continue
        for s in (day.get("slots") or []):
            if s.get("available", True) is False:
                continue
            start = str(s.get("start") or "").strip()
            end = str(s.get("end") or "").strip()
            if not start or not end or start == end:
                continue
            dt = _parse_slot_dt(today_s, start)
            if not dt or dt < now:
                continue
            key = f"{today_s}|{start}|{end}"
            if key in seen:
                continue
            seen.add(key)
            out.append((dt, f"{start} - {end}"))
    out.sort(key=lambda x: x[0])
    return out

def pick_free_today(mf: Dict[str, Any], limit: int = 5) -> List[Dict[str, Any]]:
    arr = girls_list(mf) or []
    ranked: List[Tuple[datetime, Dict[str, Any], str]] = []
    for g in arr:
        slots_today = _today_slots_from_ops_calendar(g)
        if not slots_today:
            continue
        first_dt, first_label = slots_today[0]
        ranked.append((first_dt, g, first_label))
    ranked.sort(key=lambda x: x[0])
    out: List[Dict[str, Any]] = []
    for first_dt, g, first_label in ranked[:limit]:
        item = g.copy()
        item["_first_today_slot"] = first_label
        item["_first_today_dt"] = first_dt
        out.append(item)
    return out

def _has_future_slots(g: Dict[str, Any]) -> bool:
    return len(collect_available_slots(g, {})) > 0

def _filter_pick_girls(
    mf: Dict[str, Any],
    budget: str = "any",
    style: str = "any",
    date_pref: str = "any",
    rating_pref: str = "any",
    limit: int = 7
) -> List[Dict[str, Any]]:
    arr = girls_list(mf) or []
    out: List[Tuple[float, Dict[str, Any]]] = []
    now = _msk_now_naive()
    today_s = now.strftime("%Y-%m-%d")

    for g in arr:
        p = _girl_price_num(g)
        if budget == "low" and (p is None or p > 500):
            continue
        if budget == "mid" and (p is None or p < 500 or p > 1000):
            continue
        if budget == "high" and (p is None or p < 1000):
            continue

        cats = set(_cat_slugs(g))
        if style == "popular" and not ({"bestseller", "main"} & cats):
            continue
        if style == "gamer":
            games = (g.get("acf") or {}).get("favorite_games") or []
            if not isinstance(games, list) or len(games) == 0:
                continue
        if style == "new":
            ach = (g.get("acf") or {}).get("achievements") or []
            if not isinstance(ach, list) or len(ach) == 0:
                continue

        today_slots = _today_slots_from_ops_calendar(g)
        has_today = len(today_slots) > 0
        has_any = _has_future_slots(g)
        if date_pref == "today" and not has_today:
            continue
        if date_pref == "soon" and not has_any:
            continue

        score = 0.0
        if rating_pref == "top":
            if "bestseller" in cats:
                score += 3.0
            if "main" in cats:
                score += 2.0
        elif rating_pref == "safe":
            if "main" in cats:
                score += 2.0
            if "bestseller" in cats:
                score += 1.0

        if has_today:
            score += 1.5
            score += max(0.0, 0.6 - min(0.6, (today_slots[0][0] - now).total_seconds() / 36000.0))
        elif has_any:
            score += 0.4

        if p is not None:
            score += max(0.0, 1.0 - min(1.0, p / 2500.0))

        out.append((score, g))

    out.sort(key=lambda x: x[0], reverse=True)
    return [g for _, g in out[:limit]]

def collect_available_slots(g: Dict[str, Any], slots: Dict[str, Any]) -> List[str]:
    now = _msk_now_naive()
    items: List[Tuple[datetime, str]] = []
    seen: set[str] = set()

    def _push(date_s: str | None, start: str | None, end: str | None, label: str | None, available: Any = True):
        if available is False:
            return
        start = (start or "").strip()
        end = (end or "").strip()
        if not start or not end or start == end:
            return
        dt = _parse_slot_dt(date_s, start)
        if not dt or dt < now:
            return
        label_core = (label or f"{start} - {end}").strip()
        try:
            day = datetime.strptime(date_s or "", "%Y-%m-%d").strftime("%d.%m")
            pretty = f"{day} {label_core}"
        except Exception:
            pretty = label_core
        key = f"{dt.isoformat()}::{pretty}"
        if key in seen:
            return
        seen.add(key)
        items.append((dt, pretty))

    # New format from girls.json
    for day in (g.get("ops_calendar") or []):
        date_s = day.get("date")
        for s in (day.get("slots") or []):
            _push(
                date_s=s.get("date") or date_s,
                start=s.get("start"),
                end=s.get("end"),
                label=s.get("label"),
                available=s.get("available", True)
            )

    # Backward-compatible format from slot_json
    for s in (slots.get("slots") or []):
        _push(
            date_s=s.get("date"),
            start=s.get("start"),
            end=s.get("end"),
            label=s.get("label"),
            available=s.get("available", True)
        )

    items.sort(key=lambda x: x[0])
    return [lbl for _, lbl in items]

def collect_available_slot_keys(g: Dict[str, Any], slots: Dict[str, Any]) -> List[str]:
    now = _msk_now_naive()
    keys: List[Tuple[datetime, str]] = []
    seen: set[str] = set()

    def _push(date_s: str | None, start: str | None, end: str | None, available: Any = True):
        if available is False:
            return
        start = (start or "").strip()
        end = (end or "").strip()
        if not start or not end or start == end:
            return
        dt = _parse_slot_dt(date_s, start)
        if not dt or dt < now:
            return
        key = f"{date_s}|{start}|{end}"
        if key in seen:
            return
        seen.add(key)
        keys.append((dt, key))

    for day in (g.get("ops_calendar") or []):
        date_s = day.get("date")
        for s in (day.get("slots") or []):
            _push(
                date_s=s.get("date") or date_s,
                start=s.get("start"),
                end=s.get("end"),
                available=s.get("available", True)
            )

    for s in (slots.get("slots") or []):
        _push(
            date_s=s.get("date"),
            start=s.get("start"),
            end=s.get("end"),
            available=s.get("available", True)
        )

    keys.sort(key=lambda x: x[0])
    return [k for _, k in keys]

def all_slots_text(g: Dict[str, Any], slots: Dict[str, Any]) -> str:
    name = html.escape(str(g.get("name", "")))
    s_list = collect_available_slots(g, slots)
    if not s_list:
        return f"<b>{name}</b>\n\nСвободного времени пока нет."
    lines = [f"<b>{name}</b>", "", "<b>Все доступные окна:</b>"]
    lines.extend([f"• {html.escape(s)}" for s in s_list])
    return "\n".join(lines)

def _girl_price_num(g: Dict[str, Any]) -> Optional[float]:
    for k in ("from_price", "price"):
        v = g.get(k)
        if v is None:
            continue
        try:
            return float(v)
        except Exception:
            pass
    return None

def _girl_style_tokens(g: Dict[str, Any]) -> set[str]:
    tokens: set[str] = set()
    for s in _cat_slugs(g):
        tokens.add(f"cat:{s}")
    for gimg in ((g.get("acf") or {}).get("favorite_games") or []):
        if isinstance(gimg, str) and gimg:
            tokens.add(f"game:{gimg.split('/')[-1].lower()}")
    return tokens

def _pick_recommendations(mf: Dict[str, Any], viewed_ids: List[int], limit: int = 3) -> List[Dict[str, Any]]:
    arr = girls_list(mf) or []
    if not arr:
        return []

    # Keep unique order by recency.
    uniq_viewed: List[int] = []
    seen_ids: set[int] = set()
    for gid in viewed_ids:
        if gid not in seen_ids:
            uniq_viewed.append(gid)
            seen_ids.add(gid)
    if len(uniq_viewed) < 2:
        return []

    viewed_girls = [girl_by_id(mf, gid) for gid in uniq_viewed]
    viewed_girls = [g for g in viewed_girls if g]
    if len(viewed_girls) < 2:
        return []

    pref_tokens: Dict[str, int] = {}
    prices: List[float] = []
    for vg in viewed_girls:
        for t in _girl_style_tokens(vg):
            pref_tokens[t] = pref_tokens.get(t, 0) + 1
        p = _girl_price_num(vg)
        if p is not None:
            prices.append(p)

    target_price = (sum(prices) / len(prices)) if prices else None
    viewed_set = set(uniq_viewed)

    scored: List[Tuple[float, Dict[str, Any]]] = []
    for cand in arr:
        try:
            cid = int(cand.get("id"))
        except Exception:
            continue
        if cid in viewed_set:
            continue

        score = 0.0
        cand_tokens = _girl_style_tokens(cand)
        for t in cand_tokens:
            score += pref_tokens.get(t, 0) * 1.5

        if target_price is not None:
            cp = _girl_price_num(cand)
            if cp is not None:
                diff = abs(cp - target_price)
                if diff <= 100:
                    score += 3.0
                elif diff <= 300:
                    score += 2.0
                elif diff <= 600:
                    score += 1.0

        # Small boost for availability today.
        if collect_available_slots(cand, {}):
            score += 0.5

        scored.append((score, cand))

    scored.sort(key=lambda x: x[0], reverse=True)
    out: List[Dict[str, Any]] = []
    for score, cand in scored:
        if score <= 0:
            continue
        out.append(cand)
        if len(out) >= limit:
            break
    return out

async def maybe_send_personal_reco(chat_id: int):
    viewed = await db_recent_interest_girl_ids(chat_id, limit=24, within_sec=7 * 24 * 3600)
    # Trigger after browsing at least 2-3 profiles.
    uniq_count = len(set(viewed))
    if uniq_count < 2:
        return
    if await db_recent_reco_sent(chat_id, within_sec=6 * 3600):
        return

    mf = await get_manifest()
    recs = _pick_recommendations(mf, viewed, limit=3)
    if not recs:
        return

    rows = []
    for g in recs:
        name = str(g.get("name", f"#{g.get('id')}"))
        url = g.get("bot_deeplink") or g.get("url") or SHOP_URL
        rows.append([link_button(f"💜 {name}", url)])
    rows.append([InlineKeyboardButton(text="👩 Смотреть всех", callback_data="girls:0")])

    text = (
        "💎 Похоже, тебе заходит похожий стиль.\n"
        "Вот ещё 3 девушки, которые могут понравиться:"
    )
    with suppress(Exception):
        await bot.send_message(chat_id, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
        await db_mark_reco_sent(chat_id)

# ─── BUTTON/KB HELPERS ───────────────────────────────────────────────────────
def btn_url(text: str, url: str) -> Dict[str, str]:
    return {"text": text, "url": url}
def btn_cb(text: str, cb: str) -> Dict[str, str]:
    return {"text": text, "cb": cb}

def link_button(text: str, url: str) -> InlineKeyboardButton:
    # Open regular HTTPS store links inside Telegram WebApp.
    can_webapp = (
        TG_OPEN_URLS_AS_WEBAPP
        and isinstance(url, str)
        and url.startswith("http")
        and "t.me/" not in url
    )
    if can_webapp:
        return InlineKeyboardButton(text=text, web_app=WebAppInfo(url=url))
    return InlineKeyboardButton(text=text, url=url)

def kb_from(rows: List[List[Dict[str, str]]]) -> InlineKeyboardMarkup:
    _rows = []
    for row in rows or []:
        btns = []
        for b in row:
            if "url" in b:
                btns.append(link_button(b["text"], b["url"]))
            elif "cb" in b:
                btns.append(InlineKeyboardButton(text=b["text"], callback_data=b["cb"]))
        if btns:
            _rows.append(btns)
    return InlineKeyboardMarkup(inline_keyboard=_rows) if _rows else None
def render_html(tpl: str, ctx: Dict[str, Any]) -> str:
    safe = {k: html.escape(str(v)) if v is not None else "" for k, v in ctx.items()}
    try:
        return tpl.format_map(safe)
    except Exception:
        return tpl

def render_raw(tpl: str, ctx: Dict[str, Any]) -> str:
    raw = {k: ("" if v is None else str(v)) for k, v in ctx.items()}
    try:
        return tpl.format_map(raw)
    except Exception:
        return tpl

def render(tpl: str, ctx: Dict[str, Any]) -> str:
    safe = {k: html.escape(str(v)) if v is not None else "" for k, v in ctx.items()}
    try:
        return tpl.format_map(safe)
    except Exception:
        return tpl
def _parse_id_list(s: str | None) -> List[int]:
    if not s:
        return []
    out = []
    for p in re.split(r"[,\s;|]+", s.strip()):
        try:
            out.append(int(p))
        except Exception:
            pass
    return out

def _cat_slugs(item: Dict[str, Any]) -> List[str]:
    cats = item.get("category_slugs") or item.get("categories") or item.get("category") or []
    if isinstance(cats, str):
        cats = re.split(r"[,\s;|]+", cats)
    return [str(c).strip().lower() for c in (cats or []) if str(c).strip()]

def _girl_order(item: Dict[str, Any]) -> int:
    for k in ("position", "menu_order", "order", "_index"):
        if k in item and item[k] is not None:
            try:
                return int(k in item and item[k])
            except Exception:
                pass
    return 10**9

async def get_featured_ids() -> List[int]:
    env_val = os.getenv("BESTSELLER_IDS") or os.getenv("BESTSELLER_ID") or ""
    val = await settings_get("BESTSELLER_IDS", env_val)
    return _parse_id_list(val)

def girl_image(g: Dict[str, Any]) -> str:
    imgs = g.get("images")
    if isinstance(imgs, list) and imgs:
        return imgs[0]
    return g.get("image") or g.get("url") or ""

async def find_bestseller(mf: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    arr = girls_list(mf)
    if not arr:
        log.warning("BESTSELLER: manifest empty")
        return None

    # 0) ЖЁСТКИЙ оверрайд по ID: settings → ENV
    featured_env = os.getenv("BESTSELLER_IDS") or os.getenv("BESTSELLER_ID") or ""
    featured_str = await settings_get("BESTSELLER_IDS", featured_env)
    forced_ids   = _parse_id_list(featured_str)
    if forced_ids:
        order = {gid: i for i, gid in enumerate(forced_ids)}
        cand = []
        for x in arr:
            try:
                gid = int(x.get("id"))
                if gid in order:
                    cand.append(x)
            except Exception:
                pass
        if cand:
            cand.sort(key=lambda x: order.get(int(x.get("id")), 10**9))
            chosen = cand[0]
            log.info("BESTSELLER: forced_ids matched id=%s name=%s", chosen.get("id"), chosen.get("name"))
            return chosen

    # 1) Категории: bestseller → main
    for slug in ("bestseller", "main"):
        cand = [x for x in arr if slug in set(_cat_slugs(x))]
        if cand:
            cand.sort(key=_girl_order)
            chosen = cand[0]
            log.info("BESTSELLER: chosen by category '%s' id=%s name=%s", slug, chosen.get("id"), chosen.get("name"))
            return chosen

    # 2) Легаси флаги
    for key in ("bestseller", "top", "featured"):
        for x in arr:
            v = x.get(key)
            if (isinstance(v, bool) and v) or (isinstance(v, (int, str)) and str(v).lower() in ("1","true","yes","y")):
                log.info("BESTSELLER: chosen by legacy flag '%s' id=%s name=%s", key, x.get("id"), x.get("name"))
                return x

    # 3) Фоллбэк
    log.info("BESTSELLER: fallback first id=%s name=%s", arr[0].get("id"), arr[0].get("name"))
    return arr[0]

def _log_campaign_step(chat_id: int, campaign: str, step_idx: int, reason: str|None, girl_id: int|None, payload_hash: str|None):
    def _op():
        con = sqlite3.connect(DB_PATH)
        con.execute(
            "INSERT INTO campaign_log (chat_id,campaign,step_idx,reason,girl_id,payload_hash,sent_at) VALUES (?,?,?,?,?,?,?)",
            (chat_id, campaign, step_idx, reason, girl_id, payload_hash, int(time.time()))
        )
        con.commit()
        con.close()
    return asyncio.to_thread(_op)

async def _campaign_throttled(chat_id: int, campaign: str, cooldown_hours: int, payload_hash: str | None = None) -> bool:
    cutoff = int(time.time()) - cooldown_hours*3600
    def _op():
        con = sqlite3.connect(DB_PATH)
        try:
            if payload_hash is not None:
                cur = con.execute(
                    "SELECT 1 FROM campaign_log "
                    "WHERE chat_id=? AND campaign=? AND payload_hash=? "
                    "AND step_idx=0 AND sent_at>=? LIMIT 1",
                    (chat_id, campaign, payload_hash, cutoff)
                )
            else:
                cur = con.execute(
                    "SELECT 1 FROM campaign_log "
                    "WHERE chat_id=? AND campaign=? AND step_idx=0 AND sent_at>=? LIMIT 1",
                    (chat_id, campaign, cutoff)
                )
            return cur.fetchone() is not None
        finally:
            con.close()
    return await asyncio.to_thread(_op)

# ─── DEFAULT CAMPAIGNS ───────────────────────────────────────────────────────
CAMPAIGNS: Dict[str, List[Dict[str, Any]]] = {
    "price": [
        {
            "delay": 0,
            "kind": "text",
            "text": (
                "Вижу, смущает цена. Держи жирный подгон 🔥\n\n"
                "👉 Только <b>сегодня</b>: <b>-20%</b> по купону <code>{coupon20}</code>\n"
                "и ещё можешь <b>попробовать за {trial_price}</b> — не зайдёт, <b>вернём деньги</b>."
            ),
            "buttons": [
                [btn_url("✅ Забрать −20% сейчас", "{apply_coupon20}")],
                [btn_url("🎁 10 мин за {trial_price}", "{shop_url}")],
                [btn_cb("🆘 Поддержка", "support")]
            ],
        },
        {
            "delay": 15*60,
            "kind": "text",
            "text": (
                "Напомню про -20% по <code>{coupon20}</code> — действует сегодня. "
                "И да, <b>пробник за {trial_price}</b> с гарантией возврата 😉"
            ),
            "buttons": [
                [btn_url("Применить купон", "{apply_coupon20}")],
                [btn_url("Перейти в магазин", "{shop_url}")]
            ],
        },
        {
            "delay": 2*60*60,
            "kind": "text",
            "text": (
                "⚠️ Финальный пинг: -20% по <code>{coupon20}</code> и пробник за {trial_price}. "
                "Завтра оффер уже не тот."
            ),
            "buttons": [
                [btn_url("Успеть сейчас", "{apply_coupon20}")]
            ],
        },
    ],
    "just-browsing": [
        {
            "delay": 0,
            "kind": "photo",
            "image": "{bestseller_image}",
            "caption": (
                "Если просто присматриваешься — глянь бестселлер: <b>{bestseller_name}</b> 💜\n"
                "У неё часто свободны ближайшие слоты.\n\n"
                "И да, есть <b>10 минут за {trial_price}</b> — без риска."
            ),
            "buttons": [
                [btn_url("⚡ Забронировать", "{bestseller_url}")],
                [btn_url("🎁 Пробник за {trial_price}", "{shop_url}")]
            ],
        },
        {
            "delay": 10*60,
            "kind": "text",
            "text": (
                "По-хорошему, пока есть окна — бронируй. Потом выберешь другую, если не зайдёт 👌"
            ),
            "buttons": [
                [btn_url("Открыть анкеты", "{shop_url}")]
            ],
        },
        {
            "delay": 60*60,
            "kind": "text",
            "text": (
                "Чисто чтобы не потерялось: промо на пробник за {trial_price} ещё действует."
            ),
            "buttons": [
                [btn_url("Забрать пробник", "{shop_url}")]
            ],
        },
    ],
    "no-match": [
        {
            "delay": 0,
            "kind": "text",
            "text": (
                "Не нашёл идеальную? У нас <b>постоянно появляются новые</b> 😎\n"
                "Можешь стартануть с <b>10 минут за {trial_price}</b> — если не зайдёт, вернём деньги."
            ),
            "buttons": [
                [btn_url("Посмотреть новеньких", "{shop_url}")],
                [btn_cb("🆘 Подобрать под меня", "support")]
            ],
        },
        {
            "delay": 6*60*60,
            "kind": "text",
            "text": "Апдейт: уже добавили пару свежих анкет. Забегай посмотреть.",
            "buttons": [
                [btn_url("Открыть анкеты", "{shop_url}")]
            ],
        },
    ],
    "schedule": [
        {
            "delay": 0,
            "kind": "text",
            "text": (
                "Если у тебя особый график — напиши в поддержку. "
                "Соберём <b>спец-оффер под тебя</b> и подгоним по времени 💬"
            ),
            "buttons": [
                [btn_cb("Написать в поддержку", "support")]
            ],
        },
    ],
    "other": [
        {
            "delay": 0,
            "kind": "text",
            "text": (
                "Лови оффер без лишней воды: <b>-20%</b> по <code>{coupon20}</code> сегодня "
                "и <b>пробник за {trial_price}</b> с гарантией возврата."
            ),
            "buttons": [
                [btn_url("Забрать −20%", "{apply_coupon20}")],
                [btn_url("Пробник {trial_price}", "{shop_url}")]
            ],
        },
        {
            "delay": 30*60,
            "kind": "text",
            "text": "Пока действует — лучше успеть 😉",
            "buttons": [
                [btn_url("Оформить со скидкой", "{apply_coupon20}")]
            ],
        },
    ],
    "girl_interest": [
        {
            "delay": 60,  # осознанно: чтобы не выглядело как мгновенный спам
            "kind": "text",
            "text": (
                "<b>{girl_name}</b> сегодня как раз свободна — можно быстро забронировать ⚡\n"
                "Если хочешь мягко начать — <b>10 минут за {trial_price}</b> без риска."
            ),
            "buttons": [
                [btn_url("⚡ Забронировать {girl_name}", "{girl_url}")],
                [btn_url("🎁 Пробник за {trial_price}", "{shop_url}")]
            ],
        },
        {
            "delay": 20*60,
            "kind": "text",
            "text": (
                "Пока окна у <b>{girl_name}</b> есть — лучше взять сейчас. "
                "Если не зайдёт вайб — заменим или вернём деньги."
            ),
            "buttons": [
                [btn_url("Открыть {girl_name}", "{girl_url}")]
            ],
        },
    ],
    "timesall_followup": [
        {
            "delay": 0,
            "kind": "text",
            "text": (
                "Ты ещё думаешь? 😏\n"
                "Этот слот могут забрать в любой момент.\n\n"
                "Лови <b>-10%</b> по купону <code>{coupon}</code>, если бронируешь в течение часа."
            ),
            "buttons": [
                [btn_url("⚡ Забронировать сейчас", "{girl_url}")],
                [btn_url("🎟 Применить -10%", "{apply_coupon}")]
            ],
        }
    ],
}

async def schedule_timesall_followup(chat_id: int, girl_id: int, viewed_at: int):
    # Random 15-30 min delay as requested.
    await asyncio.sleep(random.randint(15 * 60, 30 * 60))

    # If user had activity after opening full slots, skip follow-up.
    last_seen = await db_user_last_seen(chat_id)
    if last_seen is not None and last_seen > viewed_at + 5:
        log.info("timesall_followup skipped: chat=%s girl=%s last_seen=%s viewed_at=%s", chat_id, girl_id, last_seen, viewed_at)
        return

    mf = await get_manifest()
    g = girl_by_id(mf, girl_id)
    if not g:
        log.info("timesall_followup skipped: girl not found girl_id=%s", girl_id)
        return

    coupon = COUPON_CODE
    ctx = {
        "coupon": coupon,
        "apply_coupon": apply_link(coupon),
        "girl_name": g.get("name", ""),
        "girl_url": g.get("url") or SHOP_URL,
    }
    payload_hash = f"timesall:{girl_id}:{viewed_at // 3600}"
    await run_campaign(
        chat_id=chat_id,
        campaign="timesall_followup",
        ctx=ctx,
        reason="timesall_followup",
        girl_id=girl_id,
        payload_hash=payload_hash
    )

# ─── SEND STEPS / RUN CAMPAIGN ───────────────────────────────────────────────
async def _send_step(chat_id: int, step: Dict[str, Any], ctx: Dict[str, Any], step_idx: int,
                     campaign: str, reason: str|None, girl_id: int|None, payload_hash: str|None):
    try:
        if step.get("kind") == "photo":
            img = render_raw(step.get("image",""), ctx)
            caption = render_html(step.get("caption","") or step.get("text",""), ctx)
            kb = kb_from([
                [
                    {
                        **b,
                        "text": render_html(b["text"], ctx),
                        **({"url": render_raw(b["url"], ctx)} if "url" in b else {"cb": b["cb"]})
                    } for b in row
                ] for row in step.get("buttons",[])
            ])
            await bot.send_photo(chat_id, photo=img, caption=caption, reply_markup=kb)
            log.info("SEND step ok: chat=%s camp=%s idx=%s kind=photo", chat_id, campaign, step_idx)
        else:
            text = render_html(step.get("text",""), ctx)
            kb = kb_from([
                [
                    {
                        **b,
                        "text": render_html(b["text"], ctx),
                        **({"url": render_raw(b["url"], ctx)} if "url" in b else {"cb": b["cb"]})
                    } for b in row
                ] for row in step.get("buttons",[])
            ])
            await bot.send_message(chat_id, text, reply_markup=kb)
            log.info("SEND step ok: chat=%s camp=%s idx=%s kind=text", chat_id, campaign, step_idx)
    except Exception as e:
        log.exception("SEND step failed: chat=%s camp=%s idx=%s err=%s", chat_id, campaign, step_idx, e)

    await _log_campaign_step(chat_id, campaign, step_idx, reason, girl_id, payload_hash)

async def run_campaign(chat_id: int, campaign: str, ctx: Dict[str, Any], reason: str|None = None,
                       girl_id: int|None = None, payload_hash: str|None = None):
    steps = await load_campaign_steps_from_db(campaign)
    if steps is None:  # нет в БД — fallback на дефолт
        steps = CAMPAIGNS.get(campaign) or []
        log.info("CAMP '%s': using defaults, steps=%d", campaign, len(steps))
    if not steps:
        log.warning("CAMP '%s': no steps to send → abort", campaign)
        return

    # кастомный кулдаун из БД
    def _get_cooldown():
        con = sqlite3.connect(DB_PATH)
        try:
            cur = con.execute("SELECT cooldown_hours FROM campaigns WHERE name=?", (campaign,))
            r = cur.fetchone()
            if r and r[0] is not None:
                return int(r[0])
            return CAMPAIGN_COOLDOWN_HOURS
        finally:
            con.close()
    cooldown_hours = await asyncio.to_thread(_get_cooldown)

    throttled = await _campaign_throttled(chat_id, campaign, cooldown_hours, payload_hash)
    log.info("CAMP '%s': chat=%s steps=%d cooldown=%sh payload_hash=%r throttled=%s reason=%r girl_id=%r",
             campaign, chat_id, len(steps), cooldown_hours, payload_hash, throttled, reason, girl_id)
    if throttled:
        return

    async def _runner():
        for i, step in enumerate(steps):
            d = max(0, int(step.get("delay",0)))
            if d:
                log.debug("CAMP '%s': sleep %ss before step %s", campaign, d, i)
            await asyncio.sleep(d)
            try:
                await _send_step(chat_id, step, ctx, i, campaign, reason, girl_id, payload_hash)
            except Exception as e:
                log.exception("CAMP '%s': step %s failed: %s", campaign, i, e)
    asyncio.create_task(_runner())

# ─── KEYBOARDS (user) ────────────────────────────────────────────────────────
def kb_home() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="🔥 Свободны сегодня", callback_data="free:today")],
        [InlineKeyboardButton(text="👩‍🦰 Посмотреть всех девушек", callback_data="girls:0")],
        [InlineKeyboardButton(text="🔎 Подобрать под себя", callback_data="find:start")],
        [InlineKeyboardButton(text="──────────────", callback_data="noop")],
        [InlineKeyboardButton(text="❤️ Моё избранное", callback_data="fav:list")],
        [InlineKeyboardButton(text="🎁 -20% для новых", callback_data="new20")],
        [InlineKeyboardButton(text="👑 VIP-клуб", callback_data="vip")],
        [InlineKeyboardButton(text="──────────────", callback_data="noop")],
        [InlineKeyboardButton(text="📲 Поддержка", callback_data="support")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_profile(g: dict, slots: dict, is_favorite: bool = False, is_slot_subscribed: bool = False) -> InlineKeyboardMarkup:
    idx = g.get("_index", 0)
    total = max(1, g.get("_total", 1))
    prev_idx = (idx - 1) % total
    next_idx = (idx + 1) % total
    booking_url = slots.get("scheduling_url") or g.get("url")
    has_slots = len(collect_available_slots(g, slots)) > 0

    if wc_enabled():
        booking_btn = InlineKeyboardButton(text="⚡ Забронировать E-Girl", callback_data=f"pay:start:{g['id']}")
    else:
        booking_btn = link_button("⚡ Забронировать E-Girl", booking_url)
    rows = [[booking_btn]]
    rows.append([
        InlineKeyboardButton(
            text=("💔 Убрать из избранного" if is_favorite else "❤️ Добавить в избранное"),
            callback_data=(f"fav:del:{g['id']}" if is_favorite else f"fav:add:{g['id']}")
        )
    ])
    if has_slots:
        rows.append([
            InlineKeyboardButton(
                text=("🔕 Не уведомлять о слотах" if is_slot_subscribed else "🔔 Уведомлять о новых слотах"),
                callback_data=(f"slotsub:del:{g['id']}" if is_slot_subscribed else f"slotsub:add:{g['id']}")
            )
        ])
        rows.append([InlineKeyboardButton(text="🗓 Показать всё время", callback_data=f"timesall:{g['id']}")])
    rows += [
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"girls:{prev_idx}"),
         InlineKeyboardButton(text="Вперёд ➡️", callback_data=f"girls:{next_idx}")],
        [InlineKeyboardButton(text="🏠 В меню", callback_data="home")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

# ─── SIMPLE FSMs ─────────────────────────────────────────────────────────────
PENDING_SUGGEST: Dict[int, int] = {}   # user_id -> girl_id
PENDING_SUPPORT: Dict[int, bool] = {}  # user_id -> waiting for support message
FIND_STATE: Dict[int, Dict[str, str]] = {}  # user_id -> filter wizard state

# ─── ADMIN STATE (простая FSM в памяти) ──────────────────────────────────────
ADMIN_STATE: Dict[int, Dict[str, Any]] = {}  # admin_id -> {mode, ...}
BCAST_STATE: Dict[int, Dict[str, Any]] = {}

# ─── ADMIN KEYBOARDS ─────────────────────────────────────────────────────────
def kb_admin_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🧩 Шаблоны", callback_data="adm:camps")],
        [InlineKeyboardButton(text="👥 Юзеры", callback_data="adm:users")],
        [InlineKeyboardButton(text="📣 Рассылка", callback_data="adm:bcast")],
        [InlineKeyboardButton(text="⚙️ Настройки", callback_data="adm:settings")]
    ])

def kb_campaign_line(name, title, enabled):
    flag = "🟢" if enabled else "🔴"
    return [InlineKeyboardButton(text=f"{flag} {title}", callback_data=f"adm:camp:{name}")]

def kb_campaign_actions(name, enabled, cooldown):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=("🔕 Выключить" if enabled else "🔔 Включить"), callback_data=f"adm:camp:toggle:{name}")],
        [InlineKeyboardButton(text=f"⏱ Кулдаун: {cooldown}ч (изменить)", callback_data=f"adm:camp:cooldown:{name}")],
        [InlineKeyboardButton(text="📑 Шаги", callback_data=f"adm:steps:{name}"),
         InlineKeyboardButton(text="▶ Тест (мне)", callback_data=f"adm:camp:test:{name}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="adm:camps"),
         InlineKeyboardButton(text="🏠 В меню", callback_data="adm:menu")]
    ])

def kb_steps_list(name, steps_len):
    rows = []
    for i in range(steps_len):
        rows.append([InlineKeyboardButton(text=f"Шаг {i+1}", callback_data=f"adm:step:{name}:{i}")])
    rows.append([InlineKeyboardButton(text="➕ Добавить шаг", callback_data=f"adm:step:add:{name}")])
    rows.append([InlineKeyboardButton(text="⬅️ Кампания", callback_data=f"adm:camp:{name}"),
                 InlineKeyboardButton(text="🏠 Меню", callback_data="adm:menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_step_actions(name, idx):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Текст", callback_data=f"adm:step:edit:text:{name}:{idx}"),
         InlineKeyboardButton(text="🖼 Картинка", callback_data=f"adm:step:edit:image:{name}:{idx}")],
        [InlineKeyboardButton(text="📝 Caption (photo)", callback_data=f"adm:step:edit:caption:{name}:{idx}"),
         InlineKeyboardButton(text="🔘 Кнопки (JSON)", callback_data=f"adm:step:edit:buttons:{name}:{idx}")],
        [InlineKeyboardButton(text="🔀 Тип (text/photo)", callback_data=f"adm:step:edit:kind:{name}:{idx}"),
         InlineKeyboardButton(text="⏱ Задержка (сек)", callback_data=f"adm:step:edit:delay:{name}:{idx}")],
        [InlineKeyboardButton(text="⬆️ Вверх", callback_data=f"adm:step:moveup:{name}:{idx}"),
         InlineKeyboardButton(text="⬇️ Вниз", callback_data=f"adm:step:movedown:{name}:{idx}")],
        [InlineKeyboardButton(text="🗑 Удалить", callback_data=f"adm:step:del:{name}:{idx}")],
        [InlineKeyboardButton(text="⬅️ Шаги", callback_data=f"adm:steps:{name}"),
         InlineKeyboardButton(text="🏠 Меню", callback_data="adm:menu")]
    ])

def kb_users_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📄 Список (50)", callback_data="adm:users:list")],
        [InlineKeyboardButton(text="📤 Экспорт CSV", callback_data="adm:users:export")],
        [InlineKeyboardButton(text="🏠 Меню", callback_data="adm:menu")]
    ])

def kb_bcast_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Сегмент: все", callback_data="adm:bcast:seg:all")],
        [InlineKeyboardButton(text="Сегмент: активные 7д", callback_data="adm:bcast:seg:active7")],
        [InlineKeyboardButton(text="Сегмент: активные 30д", callback_data="adm:bcast:seg:active30")],
        [InlineKeyboardButton(text="✏️ Ввести текст/caption", callback_data="adm:bcast:text")],
        [InlineKeyboardButton(text="🖼 Добавить фото (URL)", callback_data="adm:bcast:photo")],
        [InlineKeyboardButton(text="🔘 Ввести кнопки (JSON)", callback_data="adm:bcast:buttons")],
        [InlineKeyboardButton(text="▶ Тест (мне)", callback_data="adm:bcast:test")],
        [InlineKeyboardButton(text="📣 Отправить", callback_data="adm:bcast:send")],
        [InlineKeyboardButton(text="🏠 Меню", callback_data="adm:menu")]
    ])

def kb_settings_menu(current_coupon20: str, current_trial: str, cd_hours: str, featured_ids_str: str):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"COUPON_20: {current_coupon20}", callback_data="adm:set:COUPON_20")],
        [InlineKeyboardButton(text=f"TRIAL_PRICE: {current_trial}", callback_data="adm:set:TRIAL_PRICE")],
        [InlineKeyboardButton(text=f"COOLDOWN (def): {cd_hours}ч", callback_data="adm:set:CAMPAIGN_COOLDOWN_HOURS")],
        [InlineKeyboardButton(text=f"FEATURED IDS: {featured_ids_str or '—'}", callback_data="adm:set:BESTSELLER_IDS")],
        [InlineKeyboardButton(text="🏠 Меню", callback_data="adm:menu")]
    ])

# ─── ADMIN COMMANDS & CALLBACKS ──────────────────────────────────────────────
@rt.message(Command("admin"))
async def admin_entry(msg: Message):
    if not is_admin(msg.from_user.id): return
    ADMIN_STATE[msg.from_user.id] = {"mode": "menu"}
    await msg.answer("Админ-панель 👑", reply_markup=kb_admin_menu())

@rt.callback_query(F.data.startswith("adm:"))
async def admin_cb(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        await ack(cb)
        return
    await _touch_user(cb.from_user.id)
    await ack(cb)  # ранний ответ
    parts = cb.data.split(":")
    # adm:menu
    if cb.data == "adm:menu":
        ADMIN_STATE[cb.from_user.id] = {"mode": "menu"}
        await cb.message.edit_text("Админ-панель 👑", reply_markup=kb_admin_menu())
        return

    # adm:camps
    if cb.data == "adm:camps":
        camps = await db_campaigns_list()
        if not camps:
            await cb.message.edit_text("Кампаний нет.", reply_markup=kb_admin_menu()); return
        rows = [kb_campaign_line(n,t,e) for (n,t,e,cd) in camps]
        rows.append([InlineKeyboardButton(text="🏠 Меню", callback_data="adm:menu")])
        await cb.message.edit_text("🧩 Кампании:", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
        return

    # adm:camp:<name>
    if len(parts) >= 3 and parts[1]=="camp" and parts[2] not in ("toggle","cooldown","test"):
        name = parts[2]
        camps = await db_campaigns_list()
        enabled, cooldown = 1, CAMPAIGN_COOLDOWN_HOURS
        for n,t,e,cd in camps:
            if n==name:
                enabled, cooldown = e, cd
                break
        await cb.message.edit_text(f"Кампания <b>{html.escape(name)}</b>", reply_markup=kb_campaign_actions(name, enabled, cooldown))
        return

    # toggle
    if len(parts)>=4 and parts[1]=="camp" and parts[2]=="toggle":
        name = parts[3]
        await db_campaign_toggle(name)
        camps = await db_campaigns_list()
        enabled, cooldown = 1, CAMPAIGN_COOLDOWN_HOURS
        for n,t,e,cd in camps:
            if n==name:
                enabled, cooldown = e, cd
                break
        with suppress(Exception):
            await cb.message.edit_reply_markup(reply_markup=kb_campaign_actions(name, enabled, cooldown))
        await cb.message.answer("Готово ✅")
        return

    # cooldown edit
    if len(parts)>=4 and parts[1]=="camp" and parts[2]=="cooldown":
        name = parts[3]
        ADMIN_STATE[cb.from_user.id] = {"mode": "edit_cooldown", "campaign": name}
        await cb.message.answer("Введи кулдаун в часах (целое число):")
        return

    # test campaign (send all steps immediately to admin)
    if len(parts)>=4 and parts[1]=="camp" and parts[2]=="test":
        name = parts[3]
        coupon20 = await settings_get("COUPON_20", COUPON_20)
        trial = await settings_get("TRIAL_PRICE", TRIAL_PRICE)
        mf = await get_manifest(force=True)
        bs = await find_bestseller(mf)
        base_ctx = {
            "coupon": COUPON_CODE,
            "coupon20": coupon20,
            "apply_coupon": apply_link(COUPON_CODE),
            "apply_coupon20": apply_link(coupon20),
            "shop_url": SHOP_URL,
            "trial_price": trial,
            "girl_name": (bs or {}).get("name", ""),
            "girl_url":  (bs or {}).get("url") or SHOP_URL,
            "bestseller_name":  (bs or {}).get("name", ""),
            "bestseller_url":   (bs or {}).get("url") or SHOP_URL,
            "bestseller_image": girl_image(bs or {}),
        }
        steps = await load_campaign_steps_from_db(name)
        if steps is None:
            steps = CAMPAIGNS.get(name, [])
        if not steps:
            await cb.message.answer("Пусто"); return
        for i, step in enumerate(steps):
            try:
                await _send_step(cb.from_user.id, step, base_ctx, i, name, None, None, "test")
            except Exception as e:
                log.warning("test send failed: %s", e)
        await cb.message.answer("Отправил тест тебе в личку ✅"); return

    # steps list
    if len(parts)>=3 and parts[1]=="steps":
        name = parts[2]
        steps = await db_campaign_steps(name)
        await cb.message.edit_text(f"Шаги кампании <b>{html.escape(name)}</b>:", reply_markup=kb_steps_list(name, len(steps)))
        return

    # add step
    if len(parts)>=4 and parts[1]=="step" and parts[2]=="add":
        name = parts[3]
        await db_campaign_step_add(name)
        steps = await db_campaign_steps(name)
        with suppress(Exception):
            await cb.message.edit_reply_markup(reply_markup=kb_steps_list(name, len(steps)))
        await cb.message.answer("Шаг добавлен ✅")
        return

    # step detail
    if len(parts)>=4 and parts[1]=="step" and parts[2] not in ("add","edit","moveup","movedown","del"):
        name = parts[2]
        idx = int(parts[3])
        steps = await db_campaign_steps(name)
        if idx<0 or idx>=len(steps):
            await cb.message.answer("Нет такого шага"); return
        st = steps[idx]
        text = (st.get("text") or "")[:500]
        caption = (st.get("caption") or "")[:300]
        kb = kb_step_actions(name, idx)
        await cb.message.edit_text(
            f"Шаг <b>{idx+1}</b>\nТип: <code>{st['kind']}</code>\nЗадержка: <code>{st['delay']}s</code>\n"
            f"Текст: <code>{html.escape(text)}</code>\nCaption: <code>{html.escape(caption)}</code>\n"
            f"Image: <code>{html.escape(st.get('image') or '')}</code>\nКнопки: <code>{html.escape(json.dumps(st.get('buttons') or [], ensure_ascii=False))}</code>",
            reply_markup=kb
        )
        return

    # edit fields
    if len(parts)>=6 and parts[1]=="step" and parts[2]=="edit":
        field = parts[3]  # text/caption/image/buttons/kind/delay
        name = parts[4]
        idx = int(parts[5])
        ADMIN_STATE[cb.from_user.id] = {"mode": "edit_step", "campaign": name, "step_idx": idx, "field": field}
        prompt = {
            "text": "Введи новый <b>текст</b> (HTML можно):",
            "caption": "Введи новый <b>caption</b> (HTML можно):",
            "image": "Вставь URL изображения:",
            "buttons": "Вставь JSON кнопок, пример: [[{\"text\":\"Открыть\",\"url\":\"https://...\"}]]",
            "kind": "Введи тип: <code>text</code> или <code>photo</code>",
            "delay": "Введи задержку в секундах (целое число):"
        }.get(field, "Введи значение:")
        await cb.message.answer(prompt)
        return

    # move up/down
    if len(parts)>=5 and parts[1]=="step" and parts[2] in ("moveup","movedown"):
        name = parts[3]; idx = int(parts[4])
        await db_campaign_step_move(name, idx, -1 if parts[2]=="moveup" else +1)
        steps = await db_campaign_steps(name)
        with suppress(Exception):
            await cb.message.edit_reply_markup(reply_markup=kb_steps_list(name, len(steps)))
        await cb.message.answer("Ок ✅")
        return

    # delete
    if len(parts)>=5 and parts[1]=="step" and parts[2]=="del":
        name = parts[3]; idx = int(parts[4])
        await db_campaign_step_delete(name, idx)
        steps = await db_campaign_steps(name)
        with suppress(Exception):
            await cb.message.edit_reply_markup(reply_markup=kb_steps_list(name, len(steps)))
        await cb.message.answer("Удалил 🗑")
        return

    # USERS
    if cb.data == "adm:users":
        stats = await db_users_stats()
        text = (f"👥 Юзеры\nВсего: <b>{stats['total']}</b>\n"
                f"Активные 7д: <b>{stats['active7']}</b>\n"
                f"Активные 30д: <b>{stats['active30']}</b>\n"
                f"Новые 24ч: <b>{stats['new24']}</b>")
        await cb.message.edit_text(text, reply_markup=kb_users_menu())
        return

    if cb.data == "adm:users:list":
        users = await db_users_list(50)
        if not users:
            await cb.message.answer("Пусто"); return
        lines = []
        for u in users:
            uname = f"@{u['username']}" if u['username'] else "—"
            lines.append(f"• <code>{u['chat_id']}</code> {html.escape(uname)} last_seen={u['last_seen']}")
        await cb.message.answer("\n".join(lines))
        return

    if cb.data == "adm:users:export":
        users = await db_users_list(1000000)
        with tempfile.NamedTemporaryFile("w", delete=False, suffix=".csv", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["chat_id","username","first_name","last_name","added_at","last_seen","last_reason","last_coupon"])
            for u in users:
                writer.writerow([u["chat_id"], u["username"], u["first_name"], u["last_name"], u["added_at"], u["last_seen"], u["last_reason"], u["last_coupon"]])
            path = f.name
        await bot.send_document(cb.from_user.id, FSInputFile(path, filename="users_export.csv"))
        await cb.message.answer("Экспорт отправил в ЛС ✅")
        return

    # BCAST
   # BCAST
    if cb.data == "adm:bcast":
        BCAST_STATE[cb.from_user.id] = {"segment":"all","text":None,"buttons":None,"photo":None}
        await cb.message.edit_text("📣 Рассылка", reply_markup=kb_bcast_menu())
        return

    if cb.data.startswith("adm:bcast:seg:"):
        seg = cb.data.split(":")[3]
        st = BCAST_STATE.get(cb.from_user.id, {})
        st["segment"] = seg
        BCAST_STATE[cb.from_user.id] = st
        await cb.message.answer(f"Сегмент: {seg}")
        return

    if cb.data == "adm:bcast:text":
        BCAST_STATE[cb.from_user.id] = BCAST_STATE.get(cb.from_user.id, {"segment":"all"})
        ADMIN_STATE[cb.from_user.id] = {"mode":"bcast_text"}
        await cb.message.answer("Введи текст/caption рассылки (HTML можно):")
        return

    if cb.data == "adm:bcast:photo":
        BCAST_STATE[cb.from_user.id] = BCAST_STATE.get(cb.from_user.id, {"segment":"all"})
        ADMIN_STATE[cb.from_user.id] = {"mode":"bcast_photo"}
        await cb.message.answer('Пришли URL фото или "нет" чтобы убрать фото:')
        return

    if cb.data == "adm:bcast:buttons":
        BCAST_STATE[cb.from_user.id] = BCAST_STATE.get(cb.from_user.id, {"segment":"all"})
        ADMIN_STATE[cb.from_user.id] = {"mode":"bcast_buttons"}
        await cb.message.answer('Пришли JSON кнопок, пример: [[{"text":"Открыть","url":"https://..."}]] или "нет"')
        return

    if cb.data == "adm:bcast:test":
        st = BCAST_STATE.get(cb.from_user.id)
        if not st:
            await cb.message.answer("Сначала настрой рассылку")
            return

        photo = st.get("photo")
        text  = st.get("text")
        kb    = kb_from(st.get("buttons") or [])

        if photo:
            if not text:
                await cb.message.answer("Нужен текст/caption для фото")
                return
            await bot.send_photo(cb.from_user.id, photo=photo, caption=text, reply_markup=kb)
        else:
            if not text:
                await cb.message.answer("Нужен текст")
                return
            await bot.send_message(cb.from_user.id, text, reply_markup=kb)

        await cb.message.answer("Тест отправлен себе ✅")
        return

    if cb.data == "adm:bcast:send":
        st = BCAST_STATE.get(cb.from_user.id)
        if not st:
            await cb.message.answer("Сначала настрой рассылку"); return
        
        photo = st.get("photo")
        text = st.get("text")
        
        if not text:
            await cb.message.answer("Нет текста/caption"); return
        
        seg = st.get("segment","all")
        uids = await db_user_ids(seg)
        sent, fail = 0, 0
        kb = kb_from(st.get("buttons") or [])
        await cb.message.answer(f"Начал рассылку по сегменту {seg}. Пользователей: {len(uids)}")
        
        for uid in uids:
            try:
                if photo:
                    await bot.send_photo(uid, photo=photo, caption=text, reply_markup=kb)
                else:
                    await bot.send_message(uid, text, reply_markup=kb)
                sent += 1
            except Exception:
                fail += 1
            await asyncio.sleep(0.05)
        await cb.message.answer(f"Готово. Ушло: {sent}, ошибок: {fail}")
        return

    # SETTINGS
    if cb.data == "adm:settings":
        c20 = await settings_get("COUPON_20", COUPON_20)
        tp = await settings_get("TRIAL_PRICE", TRIAL_PRICE)
        cd = await settings_get("CAMPAIGN_COOLDOWN_HOURS", str(CAMPAIGN_COOLDOWN_HOURS))
        featured = await settings_get("BESTSELLER_IDS", os.getenv("BESTSELLER_IDS") or os.getenv("BESTSELLER_ID") or "")
        await cb.message.edit_text("⚙️ Настройки", reply_markup=kb_settings_menu(c20, tp, cd, featured))
        return

    if cb.data.startswith("adm:set:"):
        key = cb.data.split(":")[2]
        ADMIN_STATE[cb.from_user.id] = {"mode":"edit_setting", "key": key}
        await cb.message.answer(f"Введи новое значение для {key}:")
        return

# ─── ADMIN TEXT INPUT HANDLERS ───────────────────────────────────────────────
@rt.message(F.chat.type == "private", F.from_user.id == ADMIN_CHAT_ID, ~F.text.regexp(r"^/"))
async def admin_text_inputs(msg: Message):
    await _touch_user(msg.from_user.id)
    if not is_admin(msg.from_user.id):
        return

    st = ADMIN_STATE.get(msg.from_user.id)

    # edit cooldown
    if st and st.get("mode") == "edit_cooldown":
        name = st["campaign"]
        try:
            hours = int((msg.text or "").strip())
        except Exception:
            await msg.reply("Нужно целое число часов"); return
        await db_campaign_set_cooldown(name, hours)
        ADMIN_STATE.pop(msg.from_user.id, None)
        await msg.reply("Готово ✅")
        return

    # edit step field
    if st and st.get("mode") == "edit_step":
        name = st["campaign"]; idx = st["step_idx"]; field = st["field"]
        val = (msg.text or "").strip()
        fields = {}
        try:
            if field == "delay":
                fields["delay"] = int(val)
            elif field == "kind":
                if val not in ("text","photo"):
                    await msg.reply("Только text/photo"); return
                fields["kind"] = val
            elif field == "buttons":
                if val.lower() == "нет":
                    fields["buttons"] = []
                else:
                    fields["buttons"] = json.loads(val)
            elif field == "image":
                fields["image"] = val
            elif field == "caption":
                fields["caption"] = val
            elif field == "text":
                fields["text"] = val
            else:
                await msg.reply("Неверное поле"); return
        except Exception as e:
            await msg.reply(f"Ошибка парсинга: {e}"); return
        await db_campaign_step_update(name, idx, fields)
        ADMIN_STATE.pop(msg.from_user.id, None)
        await msg.reply("Обновил ✅")
        return

    # edit setting
    if st and st.get("mode") == "edit_setting":
        key = st["key"]
        val = (msg.text or "").strip()
        await settings_set(key, val)
        global COUPON_20, TRIAL_PRICE, CAMPAIGN_COOLDOWN_HOURS
        if key == "COUPON_20": COUPON_20 = val
        if key == "TRIAL_PRICE": TRIAL_PRICE = val
        if key == "CAMPAIGN_COOLDOWN_HOURS":
            with suppress(Exception):
                CAMPAIGN_COOLDOWN_HOURS = int(val)
        ADMIN_STATE.pop(msg.from_user.id, None)
        await msg.reply("Настройка сохранена ✅")
        return

    # broadcast text
    if st and st.get("mode") == "bcast_text":
        bs = BCAST_STATE.get(msg.from_user.id, {"segment":"all"})
        bs["text"] = msg.text or ""
        BCAST_STATE[msg.from_user.id] = bs
        ADMIN_STATE.pop(msg.from_user.id, None)
        await msg.reply("Текст сохранён ✅")
        return
    # broadcast photo
    if st and st.get("mode") == "bcast_photo":
        val = (msg.text or "").strip()
        bs = BCAST_STATE.get(msg.from_user.id, {"segment":"all"})
        if val.lower() == "нет":
            bs["photo"] = None
        else:
            bs["photo"] = val
        BCAST_STATE[msg.from_user.id] = bs
        ADMIN_STATE.pop(msg.from_user.id, None)
        await msg.reply("Фото сохранено ✅" if val.lower() != "нет" else "Фото убрано ✅")
        return
    # broadcast buttons
    if st and st.get("mode") == "bcast_buttons":
        val = (msg.text or "").strip()
        bs = BCAST_STATE.get(msg.from_user.id, {"segment":"all"})
        if val.lower() == "нет":
            bs["buttons"] = []
        else:
            try:
                bs["buttons"] = json.loads(val)
            except Exception as e:
                await msg.reply(f"Ошибка JSON: {e}"); return
        BCAST_STATE[msg.from_user.id] = bs
        ADMIN_STATE.pop(msg.from_user.id, None)
        await msg.reply("Кнопки сохранены ✅")
        return

# ─── MISC CMDS ───────────────────────────────────────────────────────────────
@rt.message(Command("ping"))
async def ping(msg: Message):
    await msg.answer("pong")

@rt.message(Command("refresh"))
async def refresh(msg: Message):
    await get_manifest(force=True)
    await msg.answer("Кэш обновил. Погнали 🔁")

@rt.message(Command("cancel"))
async def cancel(msg: Message):
    PENDING_SUPPORT.pop(msg.from_user.id, None)
    PENDING_SUGGEST.pop(msg.from_user.id, None)
    FIND_STATE.pop(msg.from_user.id, None)
    ADMIN_STATE.pop(msg.from_user.id, None)
    BCAST_STATE.pop(msg.from_user.id, None)
    await msg.reply("Окей, отменил. Чем ещё помочь?")

@rt.message(Command("reply"))
async def admin_reply(msg: Message):
    if msg.chat.id != ADMIN_CHAT_ID:
        return
    parts = (msg.text or "").split(maxsplit=2)
    if len(parts) < 3 or not parts[1].isdigit():
        await msg.reply("Использование: /reply <user_id> <текст>")
        return
    user_id = int(parts[1])
    text = parts[2].strip()
    if not text:
        await msg.reply("Пустой текст ответа.")
        return
    try:
        await bot.send_message(user_id, f"👩‍💻 <b>Поддержка:</b> {html.escape(text)}")
        await msg.reply("✅ Отправлено.")
    except Exception as e:
        log.warning("reply failed: %s", e)
        await msg.reply("❌ Не смог отправить (возможно, юзер не писал боту).")

@rt.message(Command("diag"))
async def cmd_diag(msg: Message):
    """Админская диагностика: /diag <user_id> (или без аргументов — про себя)"""
    if not is_admin(msg.from_user.id):
        return
    parts = (msg.text or "").split(maxsplit=1)
    uid = msg.from_user.id
    if len(parts) == 2 and parts[1].strip().isdigit():
        uid = int(parts[1].strip())

    def _op():
        con = sqlite3.connect(DB_PATH)
        cur = con.execute("""
            SELECT campaign, step_idx, reason, girl_id, payload_hash, sent_at
            FROM campaign_log WHERE chat_id=? ORDER BY id DESC LIMIT 25
        """, (uid,))
        rows = cur.fetchall()
        con.close()
        return rows

    rows = await asyncio.to_thread(_op)
    if not rows:
        await msg.reply(f"Логов нет для user_id={uid}")
        return
    lines = [f"Логи послед. шагов для <code>{uid}</code>:"]
    for camp, idx, rs, gid, ph, ts in rows:
        t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
        lines.append(f"• {t} | {camp} step={idx} reason={rs or '—'} girl={gid or '—'} payload={ph or '—'}")
    await msg.reply("\n".join(lines))

# ─── START (DEEP LINK WITH COUPON / GIRL) ────────────────────────────────────
@rt.message(CommandStart(deep_link=True))
async def start_with_payload(msg: Message, command: CommandObject):
    await _touch_user(msg.from_user.id)
    payload = (command.args or "").strip()
    log.info("START payload: %r from %s (@%s)", payload, msg.from_user.id, msg.from_user.username)

    reason = "unknown"
    coupon = COUPON_CODE
    girl_id = None
    note = None

    if payload:
        try:
            data = json.loads(b64url_decode(payload).decode())
            if "reason" in data:
                reason = str(data["reason"])
            if "coupon" in data:
                coupon = str(data["coupon"])
            _g = data.get("g") or data.get("girl_id") or data.get("id")
            if _g is not None:
                girl_id = int(_g)
            if "note" in data:
                note = str(data["note"]).strip()[:500]
            log.info("PAYLOAD parsed: reason=%r coupon=%r girl_id=%r note=%r", reason, coupon, girl_id, bool(note))
        except Exception:
            m = re.match(r'^(?:g|girl)[:_\-]?(\d+)$', payload)
            if m:
                girl_id = int(m.group(1))
                log.info("PAYLOAD fallback matched girl_id=%s", girl_id)
            elif payload.startswith("exit_"):
                parts = payload.split("_", 2)
                if len(parts) >= 2 and parts[1]:
                    reason = parts[1]
                if len(parts) >= 3 and parts[2]:
                    coupon = parts[2]
                log.info("PAYLOAD exit_* parsed: reason=%r coupon=%r", reason, coupon)

    await db_upsert_user(
        chat_id=msg.chat.id,
        username=msg.from_user.username,
        first=msg.from_user.first_name,
        last=msg.from_user.last_name,
        reason=reason,
        coupon=coupon
    )

    # заметка от юзера (reason=other use-case)
    if note and ADMIN_CHAT_ID:
        with suppress(Exception):
            await bot.send_message(
                ADMIN_CHAT_ID,
                "📝 <b>Reason=other, заметка от пользователя</b>\n"
                f"User: <a href=\"{tg_user_link(msg.from_user)}\">{html.escape(msg.from_user.full_name)}</a> "
                f"(ID: <code>{msg.from_user.id}</code>)\n"
                f"Note: {html.escape(note)}"
            )

    # контекст
    coupon20 = await settings_get("COUPON_20", COUPON_20)
    trial = await settings_get("TRIAL_PRICE", TRIAL_PRICE)
    base_ctx = {
        "coupon": coupon,
        "coupon20": coupon20,
        "apply_coupon": apply_link(coupon),
        "apply_coupon20": apply_link(coupon20),
        "shop_url": SHOP_URL,
        "trial_price": trial,
    }

    # если пришёл girl_id — карточка и прогрев
    if girl_id is not None:
        mf = await get_manifest(force=True)
        g = girl_by_id(mf, girl_id)
        if not g:
            log.warning("START girl_id=%s not found in manifest", girl_id)
            await msg.answer("Не нашёл такую анкету 😭", reply_markup=kb_home())
            return

        # троттлинг админ-нотификации
        already = False
        try:
            already = await db_interest_seen_once(msg.chat.id, girl_id)
        except Exception as e:
            log.warning("interest_once check failed: %s", e)

        if ADMIN_CHAT_ID and not already:
            try:
                gname = str(g.get("name", f"#{girl_id}"))
                gurl = g.get("url") or SHOP_URL
                text_admin = (
                    "👀 <b>Интерес к анкете</b>\n"
                    f"Юзер: <a href=\"tg://user?id={msg.from_user.id}\">{html.escape(msg.from_user.full_name)}</a> "
                    f"(@{msg.from_user.username or '—'}, ID: <code>{msg.from_user.id}</code>)\n"
                    f"Девушка: <b>{html.escape(gname)}</b> (ID: <code>{girl_id}</code>)\n"
                    f"Ссылка: {gurl}\n"
                    f"Источник: deeplink"
                )
                await bot.send_message(
                    ADMIN_CHAT_ID,
                    text_admin,
                    disable_web_page_preview=True,
                    disable_notification=True
                )
                log.info("ADMIN notify interest sent (source=deeplink, already=%s)", already)
            except Exception as e:
                log.warning("admin interest notify failed: %s", e)
        else:
            log.info("ADMIN notify skipped (source=deeplink). already=%s, ADMIN_CHAT_ID=%s", already, ADMIN_CHAT_ID)

        with suppress(Exception):
            await db_add_interest(chat_id=msg.chat.id, girl_id=girl_id, source="deeplink")
        with suppress(Exception):
            asyncio.create_task(maybe_send_personal_reco(msg.chat.id))

        slots = {}
        try:
            if g.get("slot_json"):
                slots = await get_slots(g["slot_json"])
        except Exception as e:
            log.warning("slots fetch failed: %s", e)

        caption = profile_text(g, slots)
        is_fav  = await db_favorite_exists(msg.chat.id, girl_id)
        is_sub  = await db_slot_sub_exists(msg.chat.id, girl_id)
        kb      = kb_profile(g, slots, is_favorite=is_fav, is_slot_subscribed=is_sub)
        img     = g.get("image") or g.get("url")

        try:
            await msg.answer_photo(photo=img, caption=caption, reply_markup=kb)
        except Exception as e:
            log.warning("answer_photo failed, send text instead: %s", e)
            await msg.answer(caption, reply_markup=kb)

        girl_ctx = base_ctx | {
            "girl_name": g.get("name",""),
            "girl_url": g.get("url") or SHOP_URL,
        }
        payload_hash = str(girl_id)
        await run_campaign(
            chat_id=msg.chat.id,
            campaign="girl_interest",
            ctx=girl_ctx,
            reason=reason,
            girl_id=girl_id,
            payload_hash=payload_hash
        )
        return

    # иначе купон + reason-цепочка
    if ADMIN_CHAT_ID:
        admin_text = (
            "🎫 <b>/start с купоном</b>\n"
            f"User: @{msg.from_user.username or '—'} (ID: <code>{msg.from_user.id}</code>)\n"
            f"Name: {html.escape(msg.from_user.full_name)}\n"
            f"Reason: <code>{html.escape(reason)}</code>\n"
            f"Coupon: <code>{html.escape(coupon)}</code>"
        )
        with suppress(Exception):
            await bot.send_message(ADMIN_CHAT_ID, admin_text)
    log.info("START coupon flow: reason=%r coupon=%r", reason, coupon)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [link_button("✅ Применить −10% сейчас", apply_link(coupon))],
        [link_button("🛍 Перейти в магазин", SHOP_URL)],
        [InlineKeyboardButton(text="👩 Посмотреть всех девочек", callback_data="girls:0")],
    ])
    text = (
        f"🔥 Вот твой купон на −10%: <code>{html.escape(coupon)}</code>\n\n"
        f"Действует 7 дней. Жми кнопку — применится автоматически."
    )
    await msg.answer(text, reply_markup=kb)

    mf = await get_manifest()
    bs = await find_bestseller(mf)  # ← обязательно await
    bs_ctx = base_ctx | {
        "bestseller_name": (bs or {}).get("name",""),
        "bestseller_url":  (bs or {}).get("url") or SHOP_URL,
        "bestseller_image": girl_image(bs or {}),
    }

    # Маппинг reason → кампания (coupon идёт как price)
    reason_map = {
        "price": "price",
        "coupon": "price",
        "just-browsing": "just-browsing",
        "no-match": "no-match",
        "schedule": "schedule",
        "other": "other",
        "unknown": "other",
    }
    campaign = reason_map.get(reason, "other")
    ctx = bs_ctx if campaign == "just-browsing" else base_ctx

    await run_campaign(
        chat_id=msg.chat.id,
        campaign=campaign,
        ctx=ctx,
        reason=reason,
        girl_id=None,
        payload_hash=reason  # per-reason кулдаун
    )

@rt.message(Command("check_bs"))
async def check_bs(msg: Message):
    if not is_admin(msg.from_user.id):
        return

    def slugs_local(item: Dict[str, Any]) -> List[str]:
        cats = item.get("category_slugs") or item.get("categories") or item.get("category") or []
        if isinstance(cats, str):
            cats = re.split(r"[,\s;|]+", cats)
        return [str(c).strip().lower() for c in (cats or []) if str(c).strip()]

    def img_local(g: Dict[str, Any]) -> str:
        imgs = g.get("images")
        if isinstance(imgs, list) and imgs:
            return imgs[0]
        return g.get("image") or g.get("url") or ""

    def parse_ids(s: Optional[str]) -> List[int]:
        if not s: return []
        out = []
        for p in re.split(r"[,\s;|]+", s.strip()):
            try: out.append(int(p))
            except: pass
        return out

    featured_env = os.getenv("BESTSELLER_IDS") or os.getenv("BESTSELLER_ID") or ""
    featured_str = await settings_get("BESTSELLER_IDS", featured_env)
    forced_ids   = parse_ids(featured_str)

    mf = await get_manifest(force=True)
    arr = girls_list(mf) or []
    if not arr:
        await msg.reply("🤷‍♂️ В манифесте пусто.")
        return

    source = ""
    chosen = None

    if forced_ids:
        order = {gid: i for i, gid in enumerate(forced_ids)}
        cand = [x for x in arr if str(x.get("id")).isdigit() and int(x["id"]) in order]
        if cand:
            cand.sort(key=lambda x: order.get(int(x.get("id")), 10**9))
            chosen = cand[0]
            source = f"forced_ids ({featured_str})"

    if not chosen:
        for slug in ("bestseller", "main"):
            cand = [x for x in arr if slug in set(slugs_local(x))]
            if cand:
                def order_key(x):
                    for k in ("position", "menu_order", "order", "_index"):
                        if k in x and x[k] is not None:
                            try: return int(x[k])
                            except: pass
                    return 10**9
                cand.sort(key=order_key)
                chosen = cand[0]
                source = f"category:{slug}"
                break

    if not chosen:
        for key in ("bestseller", "top", "featured"):
            for x in arr:
                v = x.get(key)
                if (isinstance(v, bool) and v) or (isinstance(v, (int, str)) and str(v).lower() in ("1","true","yes","y")):
                    chosen = x
                    source = f"legacy_flag:{key}"
                    break
            if chosen:
                break

    if not chosen:
        chosen = arr[0]
        source = "fallback:first"

    gid = chosen.get("id")
    slugs = ", ".join(slugs_local(chosen))
    await msg.reply(
        f"🧪 Выбран бестселлер: <b>{html.escape(str(chosen.get('name')))}</b> (ID: <code>{gid}</code>)\n"
        f"Источник выбора: <code>{html.escape(source)}</code>\n"
        f"category_slugs: <code>{html.escape(slugs)}</code>\n"
        f"FEATURED_IDS: <code>{html.escape(featured_str or '—')}</code>"
    )
    img = img_local(chosen)
    if img:
        with suppress(Exception):
            await bot.send_photo(msg.chat.id, img, caption=f"{html.escape(str(chosen.get('name')))} (ID {gid})")

# ─── START (PLAIN) ───────────────────────────────────────────────────────────
@rt.message(CommandStart())
async def start_plain(msg: Message):
    await db_upsert_user(
        chat_id=msg.chat.id,
        username=msg.from_user.username,
        first=msg.from_user.first_name,
        last=msg.from_user.last_name,
        reason=None,
        coupon=None
    )
    await msg.answer("Привет 😏\nКто сегодня свободен для тебя?", reply_markup=kb_home())

# ─── HOME BTN ────────────────────────────────────────────────────────────────
@rt.callback_query(F.data == "home")
async def back_home(cb: CallbackQuery):
    await _touch_user(cb.from_user.id)
    await ack(cb)
    await cb.message.answer("Привет 😏\nКто сегодня свободен для тебя?", reply_markup=kb_home())
    with suppress(Exception):
        await cb.message.delete()

@rt.callback_query(F.data.startswith("pay:start:"))
async def start_checkout(cb: CallbackQuery):
    await _touch_user(cb.from_user.id)
    await ack(cb, "Создаю заказ…")

    try:
        gid = int(cb.data.split(":")[2])
    except Exception:
        await cb.message.answer("Не удалось создать заказ 😕")
        return

    mf = await get_manifest()
    g = girl_by_id(mf, gid)
    if not g:
        await cb.message.answer("Не нашёл такую анкету 😭")
        return

    if not wc_enabled():
        await cb.message.answer(
            "Оплата через бот временно недоступна.\nИспользуй бронирование на сайте 👇",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [link_button("⚡ Открыть анкету", g.get("url") or SHOP_URL)]
            ])
        )
        return

    order_id, pay_url = await wc_create_order_for_girl(cb.from_user, g)
    if not pay_url:
        await cb.message.answer(
            "Не получилось создать оплату автоматически 😕\n"
            "Можно оформить прямо на сайте:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [link_button("⚡ Открыть анкету", g.get("url") or SHOP_URL)],
                [InlineKeyboardButton(text="📲 Поддержка", callback_data="support")]
            ])
        )
        return

    txt = (
        f"✅ Заказ создан{f' (#{order_id})' if order_id else ''}\n"
        f"Девушка: <b>{html.escape(str(g.get('name', '')))}</b>\n"
        "Нажми кнопку ниже, чтобы перейти к оплате."
    )
    await cb.message.answer(
        txt,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [link_button("💳 Оплатить заказ", pay_url)],
            [InlineKeyboardButton(text="🏠 В меню", callback_data="home")]
        ])
    )

@rt.callback_query(F.data == "noop")
async def noop_cb(cb: CallbackQuery):
    await _touch_user(cb.from_user.id)
    await ack(cb)

@rt.callback_query(F.data == "new20")
async def new20_offer(cb: CallbackQuery):
    await _touch_user(cb.from_user.id)
    await ack(cb)
    coupon20 = await settings_get("COUPON_20", COUPON_20)
    text = (
        "🎁 <b>-20% для новых клиентов</b>\n\n"
        f"Купон: <code>{html.escape(coupon20)}</code>\n"
        "Действует ограниченное время."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [link_button("⚡ Применить -20%", apply_link(coupon20))],
        [InlineKeyboardButton(text="👩 Смотреть девушек", callback_data="girls:0")]
    ])
    await cb.message.answer(text, reply_markup=kb)

@rt.callback_query(F.data.startswith("find:"))
async def find_girl_flow(cb: CallbackQuery):
    await _touch_user(cb.from_user.id)
    await ack(cb)

    uid = cb.from_user.id
    parts = cb.data.split(":")
    action = parts[1] if len(parts) > 1 else ""

    def _kb_budget():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💰 До 500 ₽", callback_data="find:budget:low")],
            [InlineKeyboardButton(text="💰 500-1000 ₽", callback_data="find:budget:mid")],
            [InlineKeyboardButton(text="💰 1000+ ₽", callback_data="find:budget:high")],
            [InlineKeyboardButton(text="✨ Любой бюджет", callback_data="find:budget:any")],
            [InlineKeyboardButton(text="🏠 В меню", callback_data="home")],
        ])

    def _kb_style():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔥 Популярные", callback_data="find:style:popular")],
            [InlineKeyboardButton(text="🎮 Геймерские", callback_data="find:style:gamer")],
            [InlineKeyboardButton(text="🆕 Новенькие", callback_data="find:style:new")],
            [InlineKeyboardButton(text="✨ Любой типаж", callback_data="find:style:any")],
        ])

    def _kb_date():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📅 Свободны сегодня", callback_data="find:date:today")],
            [InlineKeyboardButton(text="⏳ Есть ближайшие окна", callback_data="find:date:soon")],
            [InlineKeyboardButton(text="✨ Любая дата", callback_data="find:date:any")],
        ])

    def _kb_rating():
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⭐ Топ/бестселлеры", callback_data="find:rating:top")],
            [InlineKeyboardButton(text="✅ Надёжный выбор", callback_data="find:rating:safe")],
            [InlineKeyboardButton(text="✨ Без фильтра", callback_data="find:rating:any")],
        ])

    if action == "start":
        FIND_STATE[uid] = {"budget": "any", "style": "any", "date": "any", "rating": "any"}
        await cb.message.answer("🔎 <b>Подобрать девушку</b>\n\nШаг 1/4: выбери бюджет", reply_markup=_kb_budget())
        return

    st = FIND_STATE.get(uid)
    if not st:
        FIND_STATE[uid] = {"budget": "any", "style": "any", "date": "any", "rating": "any"}
        st = FIND_STATE[uid]

    if action == "budget" and len(parts) >= 3:
        st["budget"] = parts[2]
        FIND_STATE[uid] = st
        await cb.message.answer("Шаг 2/4: выбери типаж", reply_markup=_kb_style())
        return

    if action == "style" and len(parts) >= 3:
        st["style"] = parts[2]
        FIND_STATE[uid] = st
        await cb.message.answer("Шаг 3/4: выбери дату", reply_markup=_kb_date())
        return

    if action == "date" and len(parts) >= 3:
        st["date"] = parts[2]
        FIND_STATE[uid] = st
        await cb.message.answer("Шаг 4/4: выбери уровень рейтинга", reply_markup=_kb_rating())
        return

    if action == "rating" and len(parts) >= 3:
        st["rating"] = parts[2]
        FIND_STATE[uid] = st
        mf = await get_manifest()
        picks = _filter_pick_girls(
            mf,
            budget=st.get("budget", "any"),
            style=st.get("style", "any"),
            date_pref=st.get("date", "any"),
            rating_pref=st.get("rating", "any"),
            limit=5
        )
        if not picks:
            await cb.message.answer(
                "Под этот набор фильтров пока ничего не нашлось 😕",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔁 Подобрать заново", callback_data="find:start")],
                    [InlineKeyboardButton(text="👩 Смотреть всех", callback_data="girls:0")],
                ])
            )
            return

        lines = ["<b>🎯 Подобрал для тебя:</b>"]
        rows = []
        for i, g in enumerate(picks, start=1):
            try:
                gid = int(g.get("id"))
            except Exception:
                continue
            full = girl_by_id(mf, gid)
            if not full:
                continue
            idx = int(full.get("_index", 0))
            name = html.escape(str(g.get("name", f"#{gid}")))
            price = _girl_price_num(g)
            price_text = f"{int(price)} ₽" if price is not None else "цена на сайте"
            today_slots = _today_slots_from_ops_calendar(g)
            future_slots = collect_available_slots(g, {})
            slot_text = today_slots[0][1] if today_slots else (future_slots[0] if future_slots else "время уточняется")
            lines.append(f"{i}. <b>{name}</b> — {html.escape(slot_text)} • {price_text}")
            rows.append([InlineKeyboardButton(text=f"⚡ {g.get('name', f'#{gid}')}", callback_data=f"girls:{idx}")])

        rows += [
            [InlineKeyboardButton(text="🔁 Подобрать заново", callback_data="find:start")],
            [InlineKeyboardButton(text="🏠 В меню", callback_data="home")]
        ]
        await cb.message.answer("\n".join(lines), reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
        return

@rt.callback_query(F.data == "free:today")
async def free_today(cb: CallbackQuery):
    await _touch_user(cb.from_user.id)
    await ack(cb)

    mf = await get_manifest()
    picks = pick_free_today(mf, limit=5)
    if not picks:
        await cb.message.answer(
            "На сегодня свободных окон пока нет 😕\n"
            "Можешь посмотреть весь каталог и выбрать удобное время.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="👩 Открыть анкеты", callback_data="girls:0")],
                [InlineKeyboardButton(text="🏠 В меню", callback_data="home")]
            ])
        )
        return

    lines = ["<b>🔥 Свободны сегодня</b>"]
    rows = []
    for i, g in enumerate(picks, start=1):
        try:
            gid = int(g.get("id"))
        except Exception:
            continue
        full = girl_by_id(mf, gid)
        if not full:
            continue
        idx = int(full.get("_index", 0))
        name = html.escape(str(g.get("name", f"#{gid}")))
        slot = html.escape(str(g.get("_first_today_slot", "—")))
        price = _girl_price_num(g)
        price_text = f"{int(price)} ₽" if price is not None else "цена на сайте"
        lines.append(f"{i}. <b>{name}</b> — {slot} • {price_text}")
        rows.append([InlineKeyboardButton(text=f"⚡ {g.get('name', f'#{gid}')}", callback_data=f"girls:{idx}")])

    rows.append([InlineKeyboardButton(text="🏠 В меню", callback_data="home")])
    await cb.message.answer("\n".join(lines), reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))

# ─── GIRLS FLOW ──────────────────────────────────────────────────────────────
@rt.callback_query(F.data.startswith("girls:"))
async def show_girl(cb: CallbackQuery):
    await _touch_user(cb.from_user.id)
    await ack(cb)

    idx = int(cb.data.split(":")[1])
    mf = await get_manifest()
    g = girl_by_index(mf, idx)
    if not g:
        await cb.message.answer("Пока список пуст 😭")
        return

    gid = int(g.get("id"))

    # 1) Только логируем интерес (browse), БЕЗ админ-пинга
    with suppress(Exception):
        await db_add_interest(chat_id=cb.from_user.id, girl_id=gid, source="browse")
    with suppress(Exception):
        asyncio.create_task(maybe_send_personal_reco(cb.from_user.id))

    # 2) Рендер карточки
    slots = {}
    try:
        if g.get("slot_json"):
            slots = await get_slots(g["slot_json"])
    except Exception as e:
        log.warning("slots fetch failed: %s", e)

    caption = profile_text(g, slots)
    is_fav  = await db_favorite_exists(cb.from_user.id, gid)
    is_sub  = await db_slot_sub_exists(cb.from_user.id, gid)
    kb      = kb_profile(g, slots, is_favorite=is_fav, is_slot_subscribed=is_sub)
    img     = g.get("image") or g.get("url")

    try:
        await cb.message.edit_media(
            InputMediaPhoto(media=img, caption=caption, parse_mode=ParseMode.HTML),
            reply_markup=kb
        )
    except Exception as e:
        log.info("edit_media failed, sending new photo: %s", e)
        await cb.message.answer_photo(photo=img, caption=caption, reply_markup=kb)
        with suppress(Exception):
            await cb.message.delete()

    # 3) Прогрев
    try:
        if (await settings_get("GIRL_INTEREST_ON_BROWSE", "0")) == "1":
            coupon20 = await settings_get("COUPON_20", COUPON_20)
            trial    = await settings_get("TRIAL_PRICE", TRIAL_PRICE)
            girl_ctx = {
                "coupon20": coupon20,
                "trial_price": trial,
                "shop_url": SHOP_URL,
                "girl_name": g.get("name", ""),
                "girl_url":  g.get("url") or SHOP_URL,
            }
            await run_campaign(
                chat_id=cb.from_user.id,
                campaign="girl_interest",
                ctx=girl_ctx,
                reason="browse",
                girl_id=gid,
                payload_hash=str(gid)
            )
    except Exception as e:
        log.warning("run_campaign(girl_interest) failed: %s", e)

@rt.callback_query(F.data.startswith("timesall:"))
async def show_all_times(cb: CallbackQuery):
    await _touch_user(cb.from_user.id)
    await ack(cb)
    viewed_at = int(time.time())

    try:
        gid = int(cb.data.split(":")[1])
    except Exception:
        await cb.message.answer("Не удалось открыть время 😕")
        return

    mf = await get_manifest()
    g = girl_by_id(mf, gid)
    if not g:
        await cb.message.answer("Не нашёл такую анкету 😭")
        return

    slots = {}
    try:
        if g.get("slot_json"):
            slots = await get_slots(g["slot_json"])
    except Exception as e:
        log.warning("slots fetch failed: %s", e)

    text = all_slots_text(g, slots)
    if len(text) <= 4096:
        await cb.message.answer(text)
    else:
        # Telegram limit for text message is 4096 chars; split by lines.
        chunk = ""
        for line in text.split("\n"):
            part = (line + "\n")
            if len(chunk) + len(part) > 3800:
                await cb.message.answer(chunk.rstrip("\n"))
                chunk = part
            else:
                chunk += part
        if chunk:
            await cb.message.answer(chunk.rstrip("\n"))

    # Start "still thinking?" follow-up after 15-30 minutes if user went inactive.
    asyncio.create_task(schedule_timesall_followup(cb.from_user.id, gid, viewed_at))

@rt.callback_query(F.data.startswith("slotsub:"))
async def slot_sub_cb(cb: CallbackQuery):
    await _touch_user(cb.from_user.id)
    await ack(cb)

    parts = cb.data.split(":")
    if len(parts) < 3:
        await cb.message.answer("Не удалось обработать подписку 😕")
        return
    action = parts[1]
    gid = int(parts[2])

    mf = await get_manifest()
    g = girl_by_id(mf, gid)
    if not g:
        await cb.message.answer("Не нашёл такую анкету 😭")
        return

    slots = {}
    try:
        if g.get("slot_json"):
            slots = await get_slots(g["slot_json"])
    except Exception as e:
        log.warning("slots fetch failed: %s", e)

    if action == "add":
        known = collect_available_slot_keys(g, slots)
        await db_slot_subscribe(cb.from_user.id, gid, known)
        await cb.message.answer("Готово! Буду уведомлять о новых слотах 🔔")
        return

    if action == "del":
        await db_slot_unsubscribe(cb.from_user.id, gid)
        await cb.message.answer("Окей, отключил уведомления по этой анкете 🔕")
        return

@rt.callback_query(F.data == "vip")
async def vip_info(cb: CallbackQuery):
    await _touch_user(cb.from_user.id)
    await ack(cb)
    text = (
        "🔥 <b>Как оформить красиво</b>\n\n"
        "👑 <b>EGIRLZ PRIVATE CLUB</b>\n\n"
        "Для постоянных клиентов.\n\n"
        "Что даёт VIP:\n\n"
        "• 🔥 -10% на каждое бронирование\n"
        "• ⚡ Ранний доступ к новым слотам\n"
        "• 🎁 1 бесплатная замена времени\n"
        "• 📲 Приоритетная поддержка\n\n"
        "VIP стоит 990 ₽ / месяц.\n"
        "Окупается за 3–4 встречи.\n\n"
        "Подключение занимает 30 секунд."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👑 Подключить VIP", callback_data="vip:buy")],
        [InlineKeyboardButton(text="📲 Поддержка", callback_data="support")],
        [InlineKeyboardButton(text="🏠 В меню", callback_data="home")]
    ])
    await cb.message.answer(text, reply_markup=kb)

@rt.callback_query(F.data == "vip:buy")
async def vip_buy(cb: CallbackQuery):
    await _touch_user(cb.from_user.id)
    await ack(cb, "Готовлю ссылку на оплату…")

    if not platega_enabled():
        await cb.message.answer(
            "Оплата VIP временно недоступна 😕\n"
            "Напиши в поддержку, подключим вручную.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📲 Поддержка", callback_data="support")]
            ])
        )
        return

    redirect_url, tx_id, status = await platega_create_vip_payment(cb.from_user)
    await db_add_vip_payment(
        chat_id=cb.from_user.id,
        transaction_id=tx_id,
        amount=VIP_PRICE,
        currency=VIP_CURRENCY,
        status=status,
        redirect_url=redirect_url
    )

    if not redirect_url:
        await cb.message.answer(
            "Не удалось создать ссылку на оплату 😕\n"
            "Попробуй ещё раз через минуту или напиши в поддержку.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📲 Поддержка", callback_data="support")]
            ])
        )
        return

    text = (
        "✅ <b>VIP заявка создана</b>\n"
        f"Сумма: <b>{int(VIP_PRICE)} {html.escape(VIP_CURRENCY)}</b>\n"
        "Оплати по ссылке ниже (СБП QR)."
    )
    await cb.message.answer(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [link_button("💳 Оплатить VIP", redirect_url)],
            [InlineKeyboardButton(text="🏠 В меню", callback_data="home")]
        ])
    )

@rt.callback_query(F.data.startswith("sub:"))
async def sub_reward_flow(cb: CallbackQuery):
    await _touch_user(cb.from_user.id)
    await ack(cb)

    if not SUBSCRIBE_CHANNEL_URL or not SUBSCRIBE_CHANNEL_ID:
        await cb.message.answer("Акция временно недоступна 😕")
        return

    if cb.data == "sub:start":
        text = (
            "🎁 Подпишись на наш канал и получи <b>20%</b> на первую встречу.\n\n"
            "1) Подпишись на канал\n"
            "2) Нажми «Я подписался»"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [link_button("📢 Открыть канал", SUBSCRIBE_CHANNEL_URL)],
            [InlineKeyboardButton(text="✅ Я подписался", callback_data="sub:check")],
            [InlineKeyboardButton(text="🏠 В меню", callback_data="home")]
        ])
        await cb.message.answer(text, reply_markup=kb)
        return

    if cb.data == "sub:check":
        if await db_sub_reward_exists(cb.from_user.id):
            coupon = SUBSCRIBE_COUPON
            await cb.message.answer(
                f"Ты уже получал купон 🎟\nТвой купон: <code>{html.escape(coupon)}</code>",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [link_button("⚡ Применить купон", apply_link(coupon))]
                ])
            )
            return

        ok = await is_user_subscribed(cb.from_user.id)
        if not ok:
            await cb.message.answer(
                "Пока не вижу подписку 😕\nПодпишись и нажми кнопку ещё раз.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [link_button("📢 Открыть канал", SUBSCRIBE_CHANNEL_URL)],
                    [InlineKeyboardButton(text="✅ Проверить снова", callback_data="sub:check")]
                ])
            )
            return

        coupon = SUBSCRIBE_COUPON
        await db_mark_sub_reward(cb.from_user.id, coupon)
        await cb.message.answer(
            "Готово! Спасибо за подписку 💜\n"
            f"Твой купон: <code>{html.escape(coupon)}</code>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [link_button("⚡ Применить купон", apply_link(coupon))]
            ])
        )
        return

async def slot_subscriptions_watcher():
    while True:
        try:
            subs = await db_slot_subscriptions()
            if subs:
                mf = await get_manifest(force=True)
                for sub in subs:
                    chat_id = sub["chat_id"]
                    gid = sub["girl_id"]
                    g = girl_by_id(mf, gid)
                    if not g:
                        continue

                    slots = {}
                    try:
                        if g.get("slot_json"):
                            slots = await get_slots(g["slot_json"])
                    except Exception as e:
                        log.warning("slots fetch failed in watcher for girl_id=%s: %s", gid, e)
                        continue

                    current_keys = collect_available_slot_keys(g, slots)
                    try:
                        known_keys = json.loads(sub["known_slots"] or "[]")
                        if not isinstance(known_keys, list):
                            known_keys = []
                    except Exception:
                        known_keys = []

                    known_set = {str(x) for x in known_keys}
                    new_keys = [k for k in current_keys if k not in known_set]
                    if not new_keys:
                        # Keep known state fresh to prevent stale snapshots.
                        await db_slot_sub_update_known(chat_id, gid, current_keys, touched_notify=False)
                        continue

                    name = html.escape(str(g.get("name", f"#{gid}")))
                    pretty_lines: List[str] = []
                    for k in new_keys[:3]:
                        try:
                            date_s, start, end = k.split("|", 2)
                            day = datetime.strptime(date_s, "%Y-%m-%d").strftime("%d.%m")
                            pretty_lines.append(f"• {day} {start} - {end}")
                        except Exception:
                            pass

                    text = (
                        f"🔥 У <b>{name}</b> появились новые слоты: <b>{len(new_keys)}</b>\n"
                        + ("\n".join(pretty_lines) if pretty_lines else "")
                    )
                    deeplink = g.get("bot_deeplink") or g.get("url") or SHOP_URL
                    kb = InlineKeyboardMarkup(inline_keyboard=[
                        [link_button("⚡ Открыть анкету", deeplink)]
                    ])
                    with suppress(Exception):
                        await bot.send_message(chat_id, text, reply_markup=kb)
                    await db_slot_sub_update_known(chat_id, gid, current_keys, touched_notify=True)
        except Exception as e:
            log.warning("slot_subscriptions_watcher loop failed: %s", e)

        await asyncio.sleep(60)

async def slot_channel_news_watcher():
    if not SLOT_NEWS_CHAT_ID:
        return
    while True:
        try:
            mf = await get_manifest(force=True)
            for g in girls_list(mf):
                try:
                    gid = int(g.get("id"))
                except Exception:
                    continue
                current_keys = collect_available_slot_keys(g, {})
                known = await db_channel_state_get(gid)
                # first sync: store baseline, don't spam channel on startup
                if known is None:
                    await db_channel_state_set(gid, current_keys, posted_now=False)
                    continue

                known_set = set(known)
                new_keys = [k for k in current_keys if k not in known_set]
                if not new_keys:
                    # keep state fresh (drops expired slots too)
                    await db_channel_state_set(gid, current_keys, posted_now=False)
                    continue

                name = html.escape(str(g.get("name", f"#{gid}")))
                lines = [f"🔥 У <b>{name}</b> освободились новые слоты: <b>{len(new_keys)}</b>"]
                for k in new_keys[:5]:
                    lines.append(f"• {html.escape(slot_key_to_human(k))}")
                deeplink = g.get("bot_deeplink") or g.get("url") or SHOP_URL
                kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="⚡ Открыть анкету", url=deeplink)]
                ])
                with suppress(Exception):
                    await bot.send_message(SLOT_NEWS_CHAT_ID, "\n".join(lines), reply_markup=kb)
                await db_channel_state_set(gid, current_keys, posted_now=True)
        except Exception as e:
            log.warning("slot_channel_news_watcher loop failed: %s", e)

        await asyncio.sleep(60)

@rt.callback_query(F.data.startswith("fav:"))
async def favorites_cb(cb: CallbackQuery):
    await _touch_user(cb.from_user.id)
    await ack(cb)

    parts = cb.data.split(":")
    action = parts[1] if len(parts) > 1 else ""

    if action == "list":
        fav_ids = await db_favorites_list(cb.from_user.id)
        if not fav_ids:
            await cb.message.answer("В избранном пока пусто 💔")
            return

        mf = await get_manifest()
        rows = []
        for gid in fav_ids[:20]:
            g = girl_by_id(mf, gid)
            if not g:
                continue
            rows.append([InlineKeyboardButton(text=f"❤️ {g.get('name', '#'+str(gid))}", callback_data=f"fav:open:{gid}")])
        if not rows:
            await cb.message.answer("В избранном пока пусто 💔")
            return
        rows.append([InlineKeyboardButton(text="🏠 В меню", callback_data="home")])
        await cb.message.answer("❤️ <b>Моё избранное</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
        return

    if action in ("add", "del", "open") and len(parts) < 3:
        await cb.message.answer("Не удалось обработать избранное 😕")
        return

    if action == "add":
        gid = int(parts[2])
        await db_favorite_add(cb.from_user.id, gid)
        await cb.message.answer("Добавил в избранное ❤️")
        return

    if action == "del":
        gid = int(parts[2])
        await db_favorite_remove(cb.from_user.id, gid)
        await cb.message.answer("Убрал из избранного")
        return

    if action == "open":
        gid = int(parts[2])
        mf = await get_manifest()
        g = girl_by_id(mf, gid)
        if not g:
            await cb.message.answer("Не нашёл такую анкету 😭")
            return

        slots = {}
        try:
            if g.get("slot_json"):
                slots = await get_slots(g["slot_json"])
        except Exception as e:
            log.warning("slots fetch failed: %s", e)

        caption = profile_text(g, slots)
        is_sub = await db_slot_sub_exists(cb.from_user.id, gid)
        kb = kb_profile(g, slots, is_favorite=True, is_slot_subscribed=is_sub)
        img = g.get("image") or g.get("url")

        try:
            await cb.message.answer_photo(photo=img, caption=caption, reply_markup=kb)
        except Exception as e:
            log.warning("favorites open answer_photo failed: %s", e)
            await cb.message.answer(caption, reply_markup=kb)
        return

# ─── SUGGEST TIME ────────────────────────────────────────────────────────────
@rt.callback_query(F.data.startswith("suggest:"))
async def suggest_start(cb: CallbackQuery):
    await _touch_user(cb.from_user.id)
    await ack(cb, "Жду время в чате 👇")
    gid = int(cb.data.split(":")[1])
    PENDING_SUGGEST[cb.from_user.id] = gid
    await cb.message.answer(
        "Окей, бро. Кинь время в формате <code>15.08 20:30</code> или <code>2025-08-15 20:30</code>."
        " Если не МСК — укажи пояс (напр. UTC+2)."
    )

@rt.callback_query(F.data == "support")
async def support_start(cb: CallbackQuery):
    await _touch_user(cb.from_user.id)
    if not ADMIN_CHAT_ID:
        await ack(cb, "Поддержка временно недоступна", alert=True)
        return
    await ack(cb, "Жду твоё сообщение 👇")
    PENDING_SUPPORT[cb.from_user.id] = True
    await cb.message.answer(
        "Опиши проблему одним сообщением (можно текст/фото/видео/войс/док). "
        "Чтобы отменить — напиши /cancel."
    )

# ─── PRIVATE INBOX (support + suggest + админ-вводы уже обработаны) ─────────
def _is_time_str(s: str) -> bool:
    if not s:
        return False
    s = s.strip()
    p1 = r"\b\d{1,2}[.\-/]\d{1,2}(?:[.\-/]\d{2,4})?\s+[Tt]?\s*\d{1,2}:\d{2}\b"
    p2 = r"\b\d{4}[.\-/]\d{2}[.\-/]\d{2}\s+[Tt]?\s*\d{1,2}:\d{2}\b"
    return bool(re.search(p1, s) or re.search(p2, s))

@rt.message(F.chat.type == "private", ~F.text.regexp(r"^/"))
async def inbox_private(msg: Message):
    await _touch_user(msg.from_user.id)

    # если админ — тут мы уже обработали его вводы в admin_text_inputs
    if is_admin(msg.from_user.id):
        return

    uid = msg.from_user.id

    if PENDING_SUPPORT.pop(uid, None):
        if not ADMIN_CHAT_ID:
            await msg.reply("Поддержка временно недоступна 😕")
            return
        header = (
            "🆘 <b>Запрос в поддержку</b>\n"
            f"От: <a href=\"{tg_user_link(msg.from_user)}\">{html.escape(msg.from_user.full_name)}</a>\n"
            f"ID: <code>{uid}</code>\n"
            f"Username: <code>{'@'+msg.from_user.username if msg.from_user.username else '—'}</code>\n"
            f"Профиль: {tg_user_link(msg.from_user)}\n"
        )
        with suppress(Exception):
            await bot.send_message(ADMIN_CHAT_ID, header, disable_web_page_preview=True)
        forwarded = True
        try:
            await bot.forward_message(ADMIN_CHAT_ID, msg.chat.id, msg.message_id)
        except Exception as e:
            log.warning("support forward failed: %s", e)
            forwarded = False
        if not forwarded and msg.text:
            with suppress(Exception):
                await bot.send_message(ADMIN_CHAT_ID, "Текст обращения:\n\n" + html.escape(msg.text))
        await msg.reply("Готово! Мы получили твоё обращение — служба поддержки свяжется с тобой. 🙌")
        return

    gid = PENDING_SUGGEST.get(uid)
    if gid is not None:
        PENDING_SUGGEST.pop(uid, None)
        when = (msg.text or "").strip()
        if not _is_time_str(when):
            await msg.reply(
                "Формат не похож на дату/время 😅 Пример: <code>15.08 20:30</code> или <code>2025-08-15 20:30</code>."
            )
            PENDING_SUGGEST[uid] = gid
            return
        mf = await get_manifest()
        girl = next((x for x in girls_list(mf) if int(x.get("id")) == int(gid)), None)
        gname = (girl or {}).get("name", f"#{gid}")
        gurl  = (girl or {}).get("url", SHOP_URL)
        link  = tg_user_link(msg.from_user)
        uname = f"@{msg.from_user.username}" if msg.from_user.username else "—"
        text = (
            "📝 <b>Заявка времени</b>\n"
            f"Юзер: <a href=\"{link}\">{html.escape(msg.from_user.full_name)}</a>\n"
            f"ID: <code>{uid}</code>\n"
            f"Username: <code>{html.escape(uname)}</code>\n"
            f"Профиль: {link}\n"
            f"Девушка: <b>{html.escape(gname)}</b> (ID: <code>{gid}</code>)\n"
            f"Время: <code>{html.escape(when)}</code>\n"
            f"Ссылка на анкету: {gurl}"
        )
        if ADMIN_CHAT_ID:
            with suppress(Exception):
                await bot.send_message(ADMIN_CHAT_ID, text, disable_web_page_preview=True)
        await msg.answer("Принято! Менеджер свяжется для подтверждения 👌")
        return

    # остальное игнорим (или добавляй свою общую логику)
    return

# ─── MAIN ────────────────────────────────────────────────────────────────────
async def main():
    db_init()
    await seed_campaigns_if_empty()
    asyncio.create_task(slot_subscriptions_watcher())
    if SLOT_NEWS_CHAT_ID:
        asyncio.create_task(slot_channel_news_watcher())
    # If webhook was set before, polling will fail with TelegramConflictError.
    with suppress(Exception):
        await bot.delete_webhook(drop_pending_updates=True)
    me = await bot.get_me()
    log.info("Bot online: @%s (%s)", me.username, me.id)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
