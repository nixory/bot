# -*- coding: utf-8 -*-
# E-GIRLZ Telegram Bot — full version with robust logging
# Aiogram v3

import os, json, base64, logging, time, html, re, aiohttp, sqlite3, asyncio, tempfile, csv, traceback
from urllib.parse import quote_plus
from typing import Any, Dict, List, Optional, Tuple
from contextlib import suppress

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    InputMediaPhoto, FSInputFile
)
from aiogram.filters import CommandStart, CommandObject, Command
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest
from dotenv import load_dotenv

import random

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

# ─── HELPERS ─────────────────────────────────────────────────────────────────
def apply_link(coupon: str | None = None) -> str:
    base = SHOP_URL.rstrip("/")
    code = (coupon or COUPON_CODE).strip()
    return f"{base}/?coupon={quote_plus(code)}"

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
    async with aiohttp.ClientSession() as s:
        async with s.get(url, timeout=20) as r:
            r.raise_for_status()
            return await r.json()

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

    s_list = slots.get("slots") or []
    if s_list:
        lines.append("\n<b>Ближайшие окна:</b>")
        for s in s_list[:6]:
            lines.append(f"• {html.escape(s.get('label',''))}")

    if desc:
        lines += ["", desc]
    return "\n".join(lines)

# ─── BUTTON/KB HELPERS ───────────────────────────────────────────────────────
def btn_url(text: str, url: str) -> Dict[str, str]:
    return {"text": text, "url": url}
def btn_cb(text: str, cb: str) -> Dict[str, str]:
    return {"text": text, "cb": cb}
def kb_from(rows: List[List[Dict[str, str]]]) -> InlineKeyboardMarkup:
    _rows = []
    for row in rows or []:
        btns = []
        for b in row:
            if "url" in b:
                btns.append(InlineKeyboardButton(text=b["text"], url=b["url"]))
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
}

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
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛍 Перейти на сайт", url=SHOP_URL)],
        [InlineKeyboardButton(text="👩 Посмотреть всех девочек", callback_data="girls:0")],
        [InlineKeyboardButton(text="🆘 Поддержка", callback_data="support")]
    ])

def kb_profile(g: dict, slots: dict) -> InlineKeyboardMarkup:
    idx = g.get("_index", 0)
    total = max(1, g.get("_total", 1))
    prev_idx = (idx - 1) % total
    next_idx = (idx + 1) % total
    booking_url = slots.get("scheduling_url") or g.get("url")

    rows = [
        [InlineKeyboardButton(text="⚡ Забронировать E-Girl", url=booking_url)],
        [InlineKeyboardButton(text="🕒 Предложить своё время", callback_data=f"suggest:{g['id']}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"girls:{prev_idx}"),
         InlineKeyboardButton(text="Вперёд ➡️", callback_data=f"girls:{next_idx}")],
        [InlineKeyboardButton(text="🏠 В меню", callback_data="home")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

# ─── SIMPLE FSMs ─────────────────────────────────────────────────────────────
PENDING_SUGGEST: Dict[int, int] = {}   # user_id -> girl_id
PENDING_SUPPORT: Dict[int, bool] = {}  # user_id -> waiting for support message

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
                await bot.send_message(ADMIN_CHAT_ID, text_admin, disable_web_page_preview=True)
                log.info("ADMIN notify interest sent (source=deeplink, already=%s)", already)
            except Exception as e:
                log.warning("admin interest notify failed: %s", e)
        else:
            log.info("ADMIN notify skipped (source=deeplink). already=%s, ADMIN_CHAT_ID=%s", already, ADMIN_CHAT_ID)

        with suppress(Exception):
            await db_add_interest(chat_id=msg.chat.id, girl_id=girl_id, source="deeplink")

        slots = {}
        try:
            if g.get("slot_json"):
                slots = await get_slots(g["slot_json"])
        except Exception as e:
            log.warning("slots fetch failed: %s", e)

        caption = profile_text(g, slots)
        kb      = kb_profile(g, slots)
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
        [InlineKeyboardButton(text="✅ Применить −10% сейчас", url=apply_link(coupon))],
        [InlineKeyboardButton(text="🛍 Перейти в магазин", url=SHOP_URL)],
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
    await msg.answer("Приветик!", reply_markup=kb_home())

# ─── HOME BTN ────────────────────────────────────────────────────────────────
@rt.callback_query(F.data == "home")
async def back_home(cb: CallbackQuery):
    await _touch_user(cb.from_user.id)
    await ack(cb)
    await cb.message.answer("Приветик!", reply_markup=kb_home())
    with suppress(Exception):
        await cb.message.delete()

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

    # 2) Рендер карточки
    slots = {}
    try:
        if g.get("slot_json"):
            slots = await get_slots(g["slot_json"])
    except Exception as e:
        log.warning("slots fetch failed: %s", e)

    caption = profile_text(g, slots)
    kb      = kb_profile(g, slots)
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
    me = await bot.get_me()
    log.info("Bot online: @%s (%s)", me.username, me.id)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
