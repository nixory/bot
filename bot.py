# bot.py
import os, json, base64, logging, time, html, re, aiohttp, sqlite3, asyncio
from urllib.parse import quote_plus
from typing import Any, Dict, List, Optional
from contextlib import suppress

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, InputMediaPhoto
)
from aiogram.filters import CommandStart, CommandObject, Command
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest
from dotenv import load_dotenv

# ─── ENV ──────────────────────────────────────────────────────────────────────
load_dotenv(override=True)
logging.basicConfig(level=logging.INFO)

BOT_TOKEN   = os.getenv("BOT_TOKEN")
SHOP_URL    = os.getenv("SHOP_URL", "https://egirlz.chat")
COUPON_CODE = os.getenv("COUPON_CODE", "LEAVE10")
GIRLS_MANIFEST_URL = os.getenv("GIRLS_MANIFEST_URL")
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0"))
DB_PATH = os.getenv("DB_PATH", "egirlz_bot.db")  # SQLite файл

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing. Set env var or use .env")
if not GIRLS_MANIFEST_URL:
    raise RuntimeError("GIRLS_MANIFEST_URL is missing")

# ─── AIOGRAM CORE ────────────────────────────────────────────────────────────
bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp  = Dispatcher()
rt  = Router()
dp.include_router(rt)

# ─── DATABASE (SQLite) ───────────────────────────────────────────────────────
def db_init():
    con = sqlite3.connect(DB_PATH)
    # юзеры как было
    con.execute("""
        CREATE TABLE IF NOT EXISTS users (
            chat_id     INTEGER PRIMARY KEY,
            username    TEXT,
            first_name  TEXT,
            last_name   TEXT,
            added_at    INTEGER,
            last_reason TEXT,
            last_coupon TEXT
        )
    """)
    # новая табличка интересов
    con.execute("""
        CREATE TABLE IF NOT EXISTS interests (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id    INTEGER NOT NULL,
            girl_id    INTEGER NOT NULL,
            source     TEXT,
            created_at INTEGER NOT NULL
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_interests_pair_time ON interests(chat_id, girl_id, created_at)")
    con.commit()
    con.close()


async def db_upsert_user(chat_id: int, username: str | None, first: str | None,
                         last: str | None, reason: str | None, coupon: str | None):
    def _op():
        con = sqlite3.connect(DB_PATH)
        con.execute("""
            INSERT INTO users (chat_id, username, first_name, last_name, added_at, last_reason, last_coupon)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                username=excluded.username,
                first_name=excluded.first_name,
                last_name=excluded.last_name,
                last_reason=excluded.last_reason,
                last_coupon=excluded.last_coupon
        """, (chat_id, username, first, last, int(time.time()), reason, coupon))
        con.commit()
        con.close()
    await asyncio.to_thread(_op)
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

async def db_recent_interest_exists(chat_id: int, girl_id: int, within_sec: int = 86400) -> bool:
    def _op():
        con = sqlite3.connect(DB_PATH)
        cur = con.execute(
            "SELECT 1 FROM interests WHERE chat_id=? AND girl_id=? AND created_at>=?",
            (chat_id, girl_id, int(time.time()) - within_sec)
        )
        row = cur.fetchone()
        con.close()
        return row is not None
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

# ─── CACHE & HTTP ────────────────────────────────────────────────────────────
_manifest_cache: Dict[str, Any] = {}
_manifest_ts = 0.0
_slots_cache: Dict[str, Any] = {}
_slots_ts: Dict[str, float] = {}
TTL = 60  # сек
def girl_by_id(mf: Dict[str, Any], gid: int) -> Optional[Dict[str, Any]]:
    """
    Находит девочку по product ID из манифеста и проставляет _index / _total,
    чтобы работали кнопки навигации и kb_profile.
    """
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
    data = await http_get_json(GIRLS_MANIFEST_URL)
    _manifest_cache = data or {}
    _manifest_ts = now
    return _manifest_cache

async def get_slots(slot_json_url: str) -> Dict[str, Any]:
    now = time.time()
    if slot_json_url in _slots_cache and now - _slots_ts.get(slot_json_url, 0) < TTL:
        return _slots_cache[slot_json_url]
    data = await http_get_json(slot_json_url)
    _slots_cache[slot_json_url] = data or {}
    _slots_ts[slot_json_url] = now
    return _slots_cache[slot_json_url]

def girls_list(mf: Dict[str, Any]) -> List[Dict[str, Any]]:
    return list(mf.get("girls", []))

def girl_by_index(mf: Dict[str, Any], idx: int) -> Optional[Dict[str, Any]]:
    arr = girls_list(mf)
    if not arr: return None
    idx = idx % len(arr)
    g = arr[idx].copy()
    g["_index"] = idx
    g["_total"] = len(arr)
    return g

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

# ─── KEYBOARDS ───────────────────────────────────────────────────────────────
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
    await msg.reply("Окей, отменил. Чем ещё помочь?")

# ─── START (DEEP LINK WITH COUPON) ───────────────────────────────────────────

@rt.message(CommandStart(deep_link=True))
async def start_with_payload(msg: Message, command: CommandObject):
    payload = (command.args or "").strip()
    logging.info("Got /start payload: %r from %s", payload, msg.from_user.id)

    reason = "unknown"
    coupon = COUPON_CODE
    girl_id = None

    # ── парсим payload ───────────────────────────────────────────────────────
    if payload:
        # 1) base64url JSON: {"g": 62, "coupon": "...", "reason": "..."}
        try:
            data = json.loads(b64url_decode(payload).decode())
            if "reason" in data:
                reason = str(data["reason"])
            if "coupon" in data:
                coupon = str(data["coupon"])
            _g = data.get("g") or data.get("girl_id") or data.get("id")
            if _g is not None:
                girl_id = int(_g)
        except Exception:
            # 2) g_62 / girl-62 / girl:62
            m = re.match(r'^(?:g|girl)[:_\-]?(\d+)$', payload)
            if m:
                girl_id = int(m.group(1))
            # 3) старый формат — ТОЛЬКО если начинается с "exit_"
            elif payload.startswith("exit_"):
                parts = payload.split("_", 2)
                if len(parts) >= 2 and parts[1]:
                    reason = parts[1]
                if len(parts) >= 3 and parts[2]:
                    coupon = parts[2]

    # ── сохраняем юзера ──────────────────────────────────────────────────────
    await db_upsert_user(
        chat_id=msg.chat.id,
        username=msg.from_user.username,
        first=msg.from_user.first_name,
        last=msg.from_user.last_name,
        reason=reason,
        coupon=coupon
    )

    # ── если пришёл girl_id — сразу показываем анкету ────────────────────────
    if girl_id is not None:
        # свежий манифест, ищем девочку
        mf = await get_manifest(force=True)
        g  = girl_by_id(mf, girl_id)
        if not g:
            await msg.answer("Не нашёл такую анкету 😭", reply_markup=kb_home())
            return

        # 1) проверка троттлинга: был ли интерес за 24ч (user+girl)
        should_throttle = False
        try:
            should_throttle = await db_recent_interest_exists(msg.chat.id, girl_id, within_sec=24*3600)
            logging.info("interest check: user=%s girl=%s throttle=%s", msg.chat.id, girl_id, should_throttle)
        except Exception as e:
            logging.warning("interest check failed: %s", e)

        # 2) если не душим — уведомляем админа
        if ADMIN_CHAT_ID and not should_throttle:
            try:
                gname = str(g.get("name", f"#{girl_id}"))
                gurl  = g.get("url") or SHOP_URL
                text_admin = (
                    "👀 <b>Интерес к анкете</b>\n"
                    f"Юзер: <a href=\"tg://user?id={msg.from_user.id}\">{html.escape(msg.from_user.full_name)}</a> "
                    f"(@{msg.from_user.username or '—'}, ID: <code>{msg.from_user.id}</code>)\n"
                    f"Девушка: <b>{html.escape(gname)}</b> (ID: <code>{girl_id}</code>)\n"
                    f"Ссылка: {gurl}\n"
                    f"Источник: deeplink"
                )
                await bot.send_message(ADMIN_CHAT_ID, text_admin, disable_web_page_preview=True)
                logging.info("admin notified about interest")
            except Exception as e:
                logging.warning("admin interest notify failed: %s", e)

        # 3) после уведомления — пишем интерес в базу
        with suppress(Exception):
            await db_add_interest(chat_id=msg.chat.id, girl_id=girl_id, source="deeplink")

        # 4) тянем слоты и показываем анкету
        slots = {}
        try:
            if g.get("slot_json"):
                slots = await get_slots(g["slot_json"])
        except Exception as e:
            logging.warning("slots fetch failed: %s", e)

        caption = profile_text(g, slots)
        kb      = kb_profile(g, slots)
        img     = g.get("image") or g.get("url")

        try:
            await msg.answer_photo(photo=img, caption=caption, reply_markup=kb)
        except Exception as e:
            logging.warning("answer_photo failed, send text instead: %s", e)
            await msg.answer(caption, reply_markup=kb)

        return  # ← важно: чтобы не улетел купон ниже

    # ── иначе: классический купон + админ-уведомление ────────────────────────
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
    await cb.answer()
    await cb.message.answer("Приветик!", reply_markup=kb_home())
    with suppress(Exception):
        await cb.message.delete()

# ─── GIRLS FLOW ──────────────────────────────────────────────────────────────
@rt.callback_query(F.data.startswith("girls:"))
async def show_girl(cb: CallbackQuery):
    idx = int(cb.data.split(":")[1])
    mf = await get_manifest()
    g = girl_by_index(mf, idx)
    if not g:
        await cb.answer("Пока список пуст 😭", show_alert=True)
        return

    slots = {}
    try:
        if g.get("slot_json"):
            slots = await get_slots(g["slot_json"])
    except Exception as e:
        logging.warning("slots fetch failed: %s", e)

    caption = profile_text(g, slots)
    kb      = kb_profile(g, slots)
    img     = g.get("image") or g.get("url")

    try:
        await cb.message.edit_media(
            InputMediaPhoto(media=img, caption=caption, parse_mode=ParseMode.HTML),
            reply_markup=kb
        )
    except Exception as e:
        logging.info("edit_media failed, sending new photo: %s", e)
        await cb.message.answer_photo(photo=img, caption=caption, reply_markup=kb)
        with suppress(Exception):
            await cb.message.delete()
    await cb.answer()

# ─── SUGGEST TIME ────────────────────────────────────────────────────────────
@rt.callback_query(F.data.startswith("suggest:"))
async def suggest_start(cb: CallbackQuery):
    gid = int(cb.data.split(":")[1])
    PENDING_SUGGEST[cb.from_user.id] = gid
    await cb.message.answer(
        "Окей, бро. Кинь время в формате <code>15.08 20:30</code> или <code>2025-08-15 20:30</code>."
        " Если не МСК — укажи пояс (напр. UTC+2)."
    )
    await cb.answer("Жду время в чате 👇")

# важный порядок: этот хэндлер для поддержки стоит ВЫШЕ handle_text
@rt.callback_query(F.data == "support")
async def support_start(cb: CallbackQuery):
    if not ADMIN_CHAT_ID:
        await cb.answer("Поддержка временно недоступна", show_alert=True)
        return
    PENDING_SUPPORT[cb.from_user.id] = True
    await cb.message.answer(
        "Опиши проблему одним сообщением (можно текст/фото/видео/войс/док). "
        "Чтобы отменить — напиши /cancel."
    )
    await cb.answer("Жду твоё сообщение 👇")


# ─── ADMIN REPLY ─────────────────────────────────────────────────────────────
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
        logging.warning("reply failed: %s", e)
        await msg.reply("❌ Не смог отправить (возможно, юзер не писал боту).")

# ─── HANDLE TEXT (suggest flow) ──────────────────────────────────────────────
# ─── PRIVATE INBOX: support + suggest ────────────────────────────────────────
def _is_time_str(s: str) -> bool:
    if not s:
        return False
    s = s.strip()
    # 15.08 20:30 | 15-08 20:30 | 2025-08-15 20:30 | допускаем пробел/Т/знаки
    p1 = r"\b\d{1,2}[.\-/]\d{1,2}(?:[.\-/]\d{2,4})?\s+[Tt]?\s*\d{1,2}:\d{2}\b"
    p2 = r"\b\d{4}[.\-/]\d{2}[.\-/]\d{2}\s+[Tt]?\s*\d{1,2}:\d{2}\b"
    return bool(re.search(p1, s) or re.search(p2, s))

@rt.message(F.chat.type == "private")
async def inbox_private(msg: Message):
    uid = msg.from_user.id

    # 1) SUPPORT FLOW
    if PENDING_SUPPORT.pop(uid, None):
        if not ADMIN_CHAT_ID:
            await msg.reply("Поддержка временно недоступна 😕")
            return
        header = (
            "🆘 <b>Запрос в поддержку</b>\n"
            f"От: @{msg.from_user.username or '—'} (ID: <code>{uid}</code>)\n"
            f"Имя: {html.escape(msg.from_user.full_name)}\n"
        )
        with suppress(Exception):
            await bot.send_message(ADMIN_CHAT_ID, header, disable_web_page_preview=True)

        forwarded = True
        try:
            await bot.forward_message(ADMIN_CHAT_ID, msg.chat.id, msg.message_id)
        except Exception as e:
            logging.warning("support forward failed: %s", e)
            forwarded = False

        if not forwarded and msg.text:
            with suppress(Exception):
                await bot.send_message(ADMIN_CHAT_ID, "Текст обращения:\n\n" + html.escape(msg.text))

        await msg.reply("Готово! Мы получили твоё обращение — служба поддержки свяжется с тобой. 🙌")
        return  # важный выход

    # 2) SUGGEST-TIME FLOW
    gid = PENDING_SUGGEST.get(uid)
    if gid is not None:
        # снимаем ожидание
        PENDING_SUGGEST.pop(uid, None)

        when = (msg.text or "").strip()
        if not _is_time_str(when):
            await msg.reply(
                "Формат не похож на дату/время 😅 Пример: <code>15.08 20:30</code> или <code>2025-08-15 20:30</code>."
            )
            # ставим обратно ожидание
            PENDING_SUGGEST[uid] = gid
            return

        mf = await get_manifest()
        girl = next((x for x in girls_list(mf) if int(x.get("id")) == int(gid)), None)
        gname = (girl or {}).get("name", f"#{gid}")
        gurl = (girl or {}).get("url", SHOP_URL)

        text = (
            "📝 <b>Заявка времени</b>\n"
            f"От: <a href=\"tg://user?id={uid}\">{html.escape(msg.from_user.full_name)}</a>\n"
            f"Девушка: <b>{html.escape(gname)}</b>\n"
            f"Время: <code>{html.escape(when)}</code>\n"
            f"Ссылка: {gurl}"
        )
        if ADMIN_CHAT_ID:
            with suppress(Exception):
                await bot.send_message(ADMIN_CHAT_ID, text, disable_web_page_preview=True)

        await msg.answer("Принято! Менеджер свяжется для подтверждения 👌")
        return

    # 3) если это не поддержка и не заявка — игнорим (пусть другие хэндлеры ловят команды)
    return


# ─── MAIN ────────────────────────────────────────────────────────────────────
async def main():
    db_init()
    me = await bot.get_me()
    logging.info("Bot online: @%s (%s)", me.username, me.id)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
