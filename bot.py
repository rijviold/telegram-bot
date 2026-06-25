import asyncio
import contextlib
import html
import httpx
import json
import logging
import math
import os
import re
import sqlite3
import time
from collections import deque
from pathlib import Path

import pyotp

from telegram import (
    CopyTextButton,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    Update,
)
from telegram.ext import (
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
BOT_TOKEN_2 = os.environ.get("TELEGRAM_BOT_TOKEN_2")
OTP_FORWARD_CHAT_ID = os.environ.get("OTP_FORWARD_CHAT_ID", "").strip()
GROUP_INVITE_URL = "https://t.me/+YhxYZPfnKnhlMTc1"
DEFAULT_CHANNELS = [
    {"id": "@AjkerIncomeSite", "url": "https://t.me/AjkerIncomeSite", "label": "📢 চ্যানেলে জয়েন"},
    {"id": "@facebookbuy7", "url": "https://t.me/facebookbuy7", "label": "📢 ফেসবুক বাই চ্যানেলে জয়েন"},
    {"external_key": "youtube_next_income", "url": "https://youtube.com/@next_income_source", "label": "📺 YouTube চ্যানেলে সাবস্ক্রাইব"},
]


def _required_chats(context=None):
    chats = []
    forward_id = OTP_FORWARD_CHAT_ID
    invite_url = GROUP_INVITE_URL
    if context is not None:
        forward_id = context.bot_data.get("otp_forward_chat_id", forward_id)
        invite_url = context.bot_data.get("otp_group_invite_url", invite_url)
    if forward_id:
        try:
            chats.append({"id": int(forward_id), "url": invite_url, "label": "📥 OTP গ্রুপে জয়েন"})
        except (ValueError, TypeError):
            pass
    extra_channels = DEFAULT_CHANNELS
    if context is not None:
        extra_channels = context.bot_data.get("required_channels", DEFAULT_CHANNELS)
    chats.extend(extra_channels)
    return chats

ADMIN_IDS = {
    int(x) for x in os.environ.get("ADMIN_IDS", "7430635878").replace(" ", "").split(",") if x
}

# ── Database ──────────────────────────────────────────────────────────────────
DB_PATH = Path(os.environ.get("DATA_DIR", str(Path(__file__).parent))) / "bot_data.db"
REF_BONUS = 5.0          # default ৳ per milestone (kept for reference)
# Each milestone: (otp_threshold, bonus_amount, db_column)
REF_MILESTONES = [
    (20, 5.0, "ref_paid"),
    (50, 5.0, "ref_paid_2"),
    (100, 10.0, "ref_paid_3"),
]
MIN_WITHDRAW = 20.0      # minimum ৳ balance required to withdraw


def _db_conn() -> sqlite3.Connection:
    # busy_timeout makes a busy connection wait instead of failing instantly, and
    # synchronous=NORMAL is safe + fast under WAL. WAL itself is a persistent DB
    # property enabled once in init_db(), so it is not re-set on every connection.
    conn = sqlite3.connect(str(DB_PATH), timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


@contextlib.contextmanager
def _db():
    """Yield a connection inside a transaction, then ALWAYS close it.
    sqlite3's own context manager commits/rolls back but never closes the
    connection — without this wrapper, connections leak under sustained
    concurrent load (hundreds of users)."""
    conn = _db_conn()
    try:
        with conn:
            yield conn
    finally:
        conn.close()


def init_db() -> None:
    with _db() as conn:
        # WAL is a persistent DB-file property; enabling it once here is enough.
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id     INTEGER PRIMARY KEY,
                first_name  TEXT    DEFAULT '',
                username    TEXT    DEFAULT '',
                balance     REAL    DEFAULT 0.0,
                referred_by INTEGER DEFAULT NULL,
                otp_count   INTEGER DEFAULT 0,
                ref_paid    INTEGER DEFAULT 0,
                ref_paid_2  INTEGER DEFAULT 0,
                joined_at   TEXT    DEFAULT (datetime('now'))
            )
        """)
        # Migrate existing DB — add milestone bonus flags if missing
        for col in ("ref_paid_2", "ref_paid_3"):
            try:
                conn.execute(f"ALTER TABLE users ADD COLUMN {col} INTEGER DEFAULT 0")
            except sqlite3.OperationalError:
                pass
        conn.execute("""
            CREATE TABLE IF NOT EXISTS withdrawals (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id    INTEGER,
                amount     REAL,
                method     TEXT,
                account    TEXT,
                status     TEXT DEFAULT 'pending',
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id    INTEGER,
                amount     REAL,
                kind       TEXT,
                note       TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.commit()


def db_get_or_create(user_id: int, first_name: str, username: str, referred_by: int | None = None) -> bool:
    """Insert user if not exists, else refresh name/username. Returns True if newly created.

    Uses an atomic UPSERT so concurrent /start calls can't crash on a UNIQUE collision.
    `referred_by` is only ever set on first insert — it is never overwritten.
    """
    with _db() as conn:
        existed = conn.execute(
            "SELECT 1 FROM users WHERE user_id=?", (user_id,)
        ).fetchone() is not None
        conn.execute(
            "INSERT INTO users (user_id, first_name, username, referred_by) VALUES (?,?,?,?) "
            "ON CONFLICT(user_id) DO UPDATE SET first_name=excluded.first_name, username=excluded.username",
            (user_id, first_name, username or "", referred_by),
        )
        conn.commit()
        return not existed


def db_get_user(user_id: int) -> dict | None:
    with _db() as conn:
        row = conn.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone()
        return dict(row) if row else None


def db_ref_stats(referrer_id: int) -> dict:
    """Return total referrals, how many unlocked the first bonus, and total ৳ earned from referrals."""
    cols = ", ".join(col for _, _, col in REF_MILESTONES)
    with _db() as conn:
        total = conn.execute(
            "SELECT COUNT(*) FROM users WHERE referred_by=?", (referrer_id,)
        ).fetchone()[0]
        rows = conn.execute(
            f"SELECT {cols} FROM users WHERE referred_by=?", (referrer_id,)
        ).fetchall()
    paid = 0
    earnings = 0.0
    for r in rows:
        if r[REF_MILESTONES[0][2]]:
            paid += 1
        for _, bonus, col in REF_MILESTONES:
            if r[col]:
                earnings += bonus
    return {"total": total, "paid": paid, "earnings": earnings}


def db_record_otp_and_pay_bonus(user_id: int) -> list[tuple[int, int, float]]:
    """
    Atomically (one BEGIN IMMEDIATE transaction):
      1. Increment otp_count, capped at the final referral milestone.
      2. For each newly reached milestone, credit the referrer that milestone's bonus.
    Doing both in a single transaction prevents an OTP count from advancing without
    its corresponding bonus being paid.
    Returns list of (referrer_id, milestone_otp_count, bonus) for newly unlocked bonuses.
    """
    cap = REF_MILESTONES[-1][0]
    unlocked = []
    with _db() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(
                "UPDATE users SET otp_count=otp_count+1 WHERE user_id=? AND otp_count<?",
                (user_id, cap),
            )
            cols = ", ".join(col for _, _, col in REF_MILESTONES)
            row = conn.execute(
                f"SELECT referred_by, otp_count, {cols} FROM users WHERE user_id=?",
                (user_id,)
            ).fetchone()
            if not row or not row["referred_by"]:
                # No referrer: keep the OTP increment, just nothing to pay out.
                conn.commit()
                return []
            referred_by = row["referred_by"]
            otp_count   = row["otp_count"]

            for milestone, bonus, col in REF_MILESTONES:
                if otp_count >= milestone:
                    # Atomically claim this milestone; only credit if not already paid.
                    cur = conn.execute(
                        f"UPDATE users SET {col}=1 WHERE user_id=? AND {col}=0", (user_id,)
                    )
                    if cur.rowcount == 1:
                        conn.execute(
                            "UPDATE users SET balance=balance+? WHERE user_id=?", (bonus, referred_by)
                        )
                        _db_log_txn(conn, referred_by, bonus, "ref_bonus", f"রেফার বোনাস ({milestone}টি OTP)")
                        unlocked.append((referred_by, milestone, bonus))

            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return unlocked


def db_create_withdrawal(user_id: int, amount: float, method: str, account: str) -> int | None:
    """Atomically deduct balance and create a pending withdrawal. Returns withdrawal id, or None if insufficient balance."""
    with _db() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            cur = conn.execute(
                "UPDATE users SET balance=balance-? WHERE user_id=? AND balance>=?",
                (amount, user_id, amount),
            )
            if cur.rowcount != 1:
                conn.rollback()
                return None
            cur = conn.execute(
                "INSERT INTO withdrawals (user_id, amount, method, account) VALUES (?,?,?,?)",
                (user_id, amount, method, account),
            )
            wid = cur.lastrowid
            _db_log_txn(conn, user_id, -amount, "withdraw", f"উইথড্র অনুরোধ #{wid}")
            conn.commit()
            return wid
        except Exception:
            conn.rollback()
            raise


def db_get_withdrawal(wid: int) -> dict | None:
    with _db() as conn:
        row = conn.execute("SELECT * FROM withdrawals WHERE id=?", (wid,)).fetchone()
    return dict(row) if row else None


def db_set_withdrawal_status(wid: int, status: str) -> dict | None:
    """Atomically transition a pending withdrawal. If rejected, refund the amount. Returns the row, or None if already processed."""
    with _db() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute("SELECT * FROM withdrawals WHERE id=?", (wid,)).fetchone()
            if not row or row["status"] != "pending":
                conn.rollback()
                return None
            cur = conn.execute(
                "UPDATE withdrawals SET status=? WHERE id=? AND status='pending'",
                (status, wid),
            )
            if cur.rowcount != 1:
                conn.rollback()
                return None
            if status == "rejected":
                conn.execute(
                    "UPDATE users SET balance=balance+? WHERE user_id=?",
                    (row["amount"], row["user_id"]),
                )
                _db_log_txn(conn, row["user_id"], row["amount"], "refund", f"উইথড্র #{wid} বাতিল — ফেরত")
            conn.commit()
            return dict(row)
        except Exception:
            conn.rollback()
            raise


def _db_log_txn(conn, user_id: int, amount: float, kind: str, note: str) -> None:
    """Insert a transaction record using an already-open connection."""
    conn.execute(
        "INSERT INTO transactions (user_id, amount, kind, note) VALUES (?,?,?,?)",
        (user_id, amount, kind, note),
    )


def db_get_transactions(user_id: int, limit: int = 10) -> list[dict]:
    with _db() as conn:
        rows = conn.execute(
            "SELECT amount, kind, note, created_at FROM transactions "
            "WHERE user_id=? ORDER BY id DESC LIMIT ?",
            (user_id, limit),
        ).fetchall()
    return [dict(r) for r in rows]


def db_ref_progress(referrer_id: int) -> list[dict]:
    """Return progress of each referred user toward the next milestone."""
    with _db() as conn:
        rows = conn.execute(
            "SELECT first_name, otp_count FROM users WHERE referred_by=? ORDER BY otp_count DESC",
            (referrer_id,),
        ).fetchall()
    result = []
    final_otp = REF_MILESTONES[-1][0]
    for r in rows:
        otp = r["otp_count"]
        next_ms = None
        for ms, _, _ in REF_MILESTONES:
            if otp < ms:
                next_ms = ms
                break
        result.append({
            "name": r["first_name"] or "ইউজার",
            "otp": otp,
            "next": next_ms,
            "remaining": (next_ms - otp) if next_ms else 0,
            "done": otp >= final_otp,
        })
    return result


# ── End Database ──────────────────────────────────────────────────────────────

# Service catalog is loaded from services.json on every request,
# so you can edit that file anytime without restarting the bot.
SERVICES_FILE = Path(__file__).parent / "services.json"


def _services_file_for(context) -> Path:
    if context is not None:
        custom = context.bot_data.get("services_file")
        if custom:
            return Path(__file__).parent / custom
    return SERVICES_FILE
_FALLBACK_SERVICES = {
    "fb_new": {"label": "FB- NEW (৳0.300)", "range": "996771XXX"},
    "ig": {"label": "Instagram (৳0.200)", "range": "23276345XXX"},
    "wa": {"label": "WhatsApp (৳0.500)", "range": "23276345XXX"},
}


_SERVICES_CACHE: dict = {}


def load_services(context=None) -> dict:
    path = _services_file_for(context)
    try:
        mtime = path.stat().st_mtime
    except Exception:
        mtime = 0
    cached = _SERVICES_CACHE.get(str(path))
    if cached and cached[0] == mtime:
        return cached[1]
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict) and data:
            _SERVICES_CACHE[str(path)] = (mtime, data)
            return data
    except Exception:
        logger.exception("%s load failed; using fallback", path.name)
    return _FALLBACK_SERVICES


def _normalize_ranges(service: dict) -> list:
    """Return ranges as list of {label, range} dicts. Supports legacy string ranges."""
    raw = service.get("ranges") or ([service["range"]] if service.get("range") else [])
    out = []
    for i, item in enumerate(raw):
        if isinstance(item, dict):
            r = str(item.get("range", "")).strip()
            if not r:
                continue
            label = str(item.get("label") or f"Country {i+1}").strip()
            out.append({"label": label, "range": r})
        else:
            r = str(item).strip()
            if r:
                out.append({"label": r, "range": r})
    return out


def save_services(data: dict, context=None) -> None:
    path = _services_file_for(context)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    try:
        _SERVICES_CACHE[str(path)] = (path.stat().st_mtime, data)
    except Exception:
        _SERVICES_CACHE.pop(str(path), None)


def is_admin(user_id: int, context=None) -> bool:
    if user_id in ADMIN_IDS:
        return True
    if context is not None:
        extra = context.bot_data.get("extra_admins") or set()
        if user_id in extra:
            return True
    return False

def _admin_only(handler):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not is_admin(update.effective_user.id, context):
            await update.message.reply_text(
                "⛔ <b>এই কমান্ড শুধু এডমিনের জন্য।</b>",
                parse_mode="HTML",
            )
            return
        return await handler(update, context)
    return wrapper

def _main_markup(context=None):
    menu = [
        ["📱 GET NUMBER", "🔐 2FA CODE"],
        ["👤 PROFILE", "🎁 REFER"],
        ["💰 WITHDRAW"],
    ]
    return ReplyKeyboardMarkup(menu, resize_keyboard=True)


def _join_prompt_markup(missing_chats=None, context=None) -> InlineKeyboardMarkup:
    chats = missing_chats if missing_chats is not None else _required_chats(context)
    rows = [[InlineKeyboardButton(c["label"], url=c["url"])] for c in chats]
    rows.append([InlineKeyboardButton("✅ জয়েন করেছি", callback_data="check_join")])
    return InlineKeyboardMarkup(rows)


_MEMBERSHIP_CACHE: dict = {}
_MEMBERSHIP_TTL = 900.0


def _membership_cache_key(bot, chat_id, user_id):
    return (id(bot), str(chat_id), int(user_id))


def invalidate_membership_cache(user_id: int):
    keys = [k for k in _MEMBERSHIP_CACHE if k[2] == user_id]
    for k in keys:
        _MEMBERSHIP_CACHE.pop(k, None)


async def _check_chat_membership(bot, chat_id, user_id: int) -> bool:
    import time as _t
    key = _membership_cache_key(bot, chat_id, user_id)
    cached = _MEMBERSHIP_CACHE.get(key)
    now = _t.time()
    if cached and cached[1] > now:
        return cached[0]
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        ok = member.status in ("member", "administrator", "creator", "owner")
    except Exception:
        logger.exception("member check failed: chat=%s user=%s", chat_id, user_id)
        ok = False
    _MEMBERSHIP_CACHE[key] = (ok, now + _MEMBERSHIP_TTL)
    return ok


async def _check_one_chat(bot, chat: dict, user_id: int, user_data) -> bool:
    ext_key = chat.get("external_key")
    if ext_key:
        if user_data is None:
            return False
        return bool(user_data.get(f"ack_{ext_key}", False))
    return await _check_chat_membership(bot, chat["id"], user_id)


async def get_missing_chats(bot, user_id: int, context=None) -> list:
    chats = _required_chats(context)
    if not chats:
        return []
    user_data = getattr(context, "user_data", None) if context is not None else None
    results = await asyncio.gather(
        *[_check_one_chat(bot, c, user_id, user_data) for c in chats]
    )
    return [c for c, ok in zip(chats, results) if not ok]


async def is_group_member(bot, user_id: int, context=None) -> bool:
    return len(await get_missing_chats(bot, user_id, context)) == 0


async def require_membership(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user_id = update.effective_user.id
    missing = await get_missing_chats(context.bot, user_id, context)
    if not missing:
        return True
    text = (
        "🔒 বট ব্যবহার করতে হলে প্রথমে নিচের সবগুলোতে জয়েন করুন।\n\n"
        "জয়েন করার পর নিচের ✅ \"জয়েন করেছি\" বাটনে ক্লিক করুন।"
    )
    target = update.callback_query.message if update.callback_query else update.message
    if target:
        try:
            await target.reply_text(text, reply_markup=_join_prompt_markup(missing, context))
        except Exception:
            logger.exception("failed to send join prompt")
    return False


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    # Parse referral code from /start <referrer_id>
    referred_by: int | None = None
    if context.args:
        try:
            ref_id = int(context.args[0])
            if ref_id != user.id:
                referred_by = ref_id
        except (ValueError, TypeError):
            pass
    # Register user in DB (only sets referred_by on first join)
    await asyncio.to_thread(
        db_get_or_create, user.id, user.first_name, user.username or "", referred_by
    )
    if not await require_membership(update, context):
        return
    welcome = (
        "━━━━━━━\n"
        "   <b>⚡️ NUMBER PANEL</b>\n"
        "━━━━━━━\n\n"
        f"👋 <b>স্বাগতম, {html.escape(user.first_name or '')}!</b>\n\n"
        "<i>আপনার সেবা নির্বাহ প্ল্যাটফর্ম।</i>\n\n"
        "নিচের মেনু থেকে বেছে নিন একটি সার্ভিস।"
    )
    await update.message.reply_text(welcome, parse_mode="HTML", reply_markup=_main_markup(context))


async def show_services(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_membership(update, context):
        return
    services = load_services(context)
    if not services:
        target = update.message or (update.callback_query.message if update.callback_query else None)
        if target:
            await target.reply_text("⚠️ <b>কোনো সার্ভিস নেই।</b>", parse_mode="HTML")
        return
    keyboard = [
        [InlineKeyboardButton(f"🔹 {s['label']}", callback_data=f"buy:{key}")]
        for key, s in services.items()
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    header = "<b>SELECT SERVICE 🌐</b>"
    if update.message:
        await update.message.reply_text(header, parse_mode="HTML", reply_markup=reply_markup)
    else:
        await update.callback_query.message.edit_text(header, parse_mode="HTML", reply_markup=reply_markup)




_CR_API_BASE = "https://api.2oo9.cloud/MXS47FLFX0U/tnevs/@public/api"

# Persistent HTTP client for OTP polling (high-frequency, small payload)
_otp_http_client: httpx.AsyncClient | None = None


def _get_otp_client() -> httpx.AsyncClient:
    global _otp_http_client
    if _otp_http_client is None or _otp_http_client.is_closed:
        _otp_http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=5.0, read=10.0, write=5.0, pool=3.0),
            limits=httpx.Limits(max_connections=30, max_keepalive_connections=10),
        )
    return _otp_http_client


# Persistent HTTP client + concurrency cap for number allocation (POST /getnum).
# A shared connection pool avoids a fresh TLS handshake on every click, and the
# semaphore keeps a burst of clicks (e.g. 500 at once) from opening 500 upstream
# connections — the bot stays responsive and the upstream API is not overwhelmed.
_alloc_http_client: httpx.AsyncClient | None = None
_ALLOC_MAX_CONCURRENCY = 120
_alloc_semaphore: asyncio.Semaphore | None = None


def _get_alloc_client() -> httpx.AsyncClient:
    global _alloc_http_client
    if _alloc_http_client is None or _alloc_http_client.is_closed:
        _alloc_http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=10.0, read=20.0, write=10.0, pool=30.0),
            limits=httpx.Limits(max_connections=150, max_keepalive_connections=60),
        )
    return _alloc_http_client


def _get_alloc_semaphore() -> asyncio.Semaphore:
    global _alloc_semaphore
    if _alloc_semaphore is None:
        _alloc_semaphore = asyncio.Semaphore(_ALLOC_MAX_CONCURRENCY)
    return _alloc_semaphore


async def _allocate_number(rid: str, api_key: str) -> dict:
    """POST /getnum — shared pooled client + bounded concurrency so a burst of
    simultaneous clicks stays fast instead of opening one connection per call."""
    url = f"{_CR_API_BASE}/getnum"
    try:
        sem = _get_alloc_semaphore()
        async with sem:
            client = _get_alloc_client()
            resp = await client.post(url, json={"rid": rid}, headers={"mauthapi": api_key})
        try:
            j = resp.json()
        except Exception:
            j = None
        return {"status": resp.status_code, "json": j}
    except Exception as e:
        logger.warning("allocate_number failed: %s", e)
        # Only rebuild on a transport/pool error, and only drop the reference so
        # the NEXT call builds a fresh client — never force-close here, or we'd
        # break other requests still in-flight on the same shared client.
        if isinstance(e, httpx.TransportError):
            global _alloc_http_client
            _alloc_http_client = None
        return {"status": 0, "json": None, "error": str(e)}


_SEEN_OTP_IDS: set = set()
_SEEN_OTP_ORDER: deque = deque()
_SEEN_OTP_MAX = 20000


def _mark_seen(otp_id: str) -> None:
    """Record an OTP id as processed, evicting the OLDEST first so the newest
    ids are always retained. A plain set slice evicts arbitrary ids, which can
    re-allow a recent OTP and double-count referral progress."""
    if otp_id in _SEEN_OTP_IDS:
        return
    _SEEN_OTP_IDS.add(otp_id)
    _SEEN_OTP_ORDER.append(otp_id)
    while len(_SEEN_OTP_ORDER) > _SEEN_OTP_MAX:
        _SEEN_OTP_IDS.discard(_SEEN_OTP_ORDER.popleft())


# Shared OTP watcher registry — avoids one API call per user.
# Structure: { api_key: [ {chat_id, numbers, forward_chat_id,
#                          number_countries, service_label, expires_at}, ... ] }
_OTP_WATCHERS: dict[str, list] = {}


async def _fetch_success_otps(api_key: str) -> list:
    """GET /success-otp — uses shared OTP client."""
    url = f"{_CR_API_BASE}/success-otp"
    try:
        client = _get_otp_client()
        resp = await client.get(url, headers={"mauthapi": api_key})
        j = resp.json()
        return (j.get("data") or {}).get("otps") or []
    except httpx.TransportError:
        # Drop the shared client so the next poll rebuilds it. Never aclose()
        # here — other api-key poll jobs may be mid-request on the same client.
        global _otp_http_client
        _otp_http_client = None
        return []
    except Exception:
        return []


def _build_otp_message(otp: dict, watcher: dict, number: str):
    """Build OTP text + reply_markup for one OTP + one watcher."""
    msg_text = otp.get("message", "")
    service_label = watcher.get("service_label", "")
    country = (watcher.get("number_countries") or {}).get(number, "")

    m = re.search(r"\b(\d{3,4}[\s\-]\d{3,4})\b", msg_text)
    otp_code = re.sub(r"\s+", " ", m.group(1)).strip() if m else ""
    if not otp_code:
        m2 = re.search(r"\b(\d{4,8})\b", msg_text)
        otp_code = m2.group(1) if m2 else msg_text
    clean_code = otp_code.replace(" ", "")

    country_line = f"\n🌍 {html.escape(country)}" if country else ""
    text = (
        f"📱 {html.escape(service_label)}"
        f"{country_line}\n"
        f"📞 {number}\n"
        f"🔑 {html.escape(otp_code)}"
    )
    reply_markup = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"📋 কোড কপি ({html.escape(otp_code)})", copy_text=CopyTextButton(text=clean_code))],
        [InlineKeyboardButton("📞 নম্বর কপি", copy_text=CopyTextButton(text=number))],
    ])
    return text, reply_markup


async def _shared_otp_poll_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """One job per API key — fetches OTPs ONCE and fans out to all watchers."""
    api_key: str = context.job.data["api_key"]

    # Expire stale watchers
    now = time.time()
    watchers = _OTP_WATCHERS.get(api_key, [])
    watchers = [w for w in watchers if w["expires_at"] > now]
    _OTP_WATCHERS[api_key] = watchers

    # No active watchers → stop the job
    if not watchers:
        context.job.schedule_removal()
        return

    # Single API call for all users
    try:
        otps = await _fetch_success_otps(api_key)
    except Exception:
        return

    if not otps:
        return

    for otp in otps:
        raw_number = str(otp.get("number", "") or "")
        number = re.sub(r"[^\d]", "", raw_number)
        if not number:
            continue

        otp_id = str(otp.get("otp_id") or otp.get("id") or "") or f"{number}:{otp.get('message', '')}"
        # Scope dedupe per API key so an id reused across keys/bots isn't suppressed.
        otp_id = f"{api_key}:{otp_id}"
        if otp_id in _SEEN_OTP_IDS:
            continue

        # Find all watchers waiting for this number
        matching = [w for w in watchers if number in w["numbers"]]
        if not matching:
            continue

        # Dedupe by chat_id: a user with overlapping watchers must get ONE
        # message and ONE referral increment per OTP, not one per watcher entry.
        seen_chats = set()
        deduped = []
        for w in matching:
            if w["chat_id"] in seen_chats:
                continue
            seen_chats.add(w["chat_id"])
            deduped.append(w)
        matching = deduped

        _mark_seen(otp_id)

        # Send to each matching watcher (usually just 1)
        send_tasks = []
        for watcher in matching:
            text, reply_markup = _build_otp_message(otp, watcher, number)
            send_tasks.append(
                context.bot.send_message(watcher["chat_id"], text, parse_mode="HTML", reply_markup=reply_markup)
            )
            fwd = watcher.get("forward_chat_id")
            if fwd:
                send_tasks.append(
                    context.bot.send_message(int(fwd), text, parse_mode="HTML", reply_markup=reply_markup)
                )

        results = await asyncio.gather(*send_tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, Exception):
                logger.warning("otp send failed: %s", r)

        # Track OTP count for referral system
        for watcher in matching:
            chat_id = watcher["chat_id"]
            try:
                unlocked = await asyncio.to_thread(db_record_otp_and_pay_bonus, chat_id)
                for referrer_id, milestone, bonus in unlocked:
                    user_row = await asyncio.to_thread(db_get_user, chat_id)
                    user_name = (user_row or {}).get("first_name", "কেউ")
                    try:
                        await context.bot.send_message(
                            referrer_id,
                            f"🎉 <b>রেফার বোনাস পেয়েছ!</b>\n\n"
                            f"👤 <b>{html.escape(user_name)}</b> <b>{milestone}টি OTP</b> সম্পন্ন করেছে!\n"
                            f"✅ তোমার একাউন্টে <b>৳{bonus:.0f}</b> যোগ হয়েছে! 🎊",
                            parse_mode="HTML",
                        )
                    except Exception as e:
                        logger.warning("ref bonus notify failed: %s", e)
            except Exception as e:
                logger.warning("otp tracking failed for %s: %s", chat_id, e)


def schedule_otp_poll(
    context,
    chat_id: int,
    numbers: list,
    api_key: str,
    forward_chat_id=None,
    number_countries: dict = None,
    service_label: str = "",
    duration_minutes: int = 30,
    interval_seconds: int = 5,
) -> None:
    clean = {re.sub(r"[^\d]", "", n) for n in numbers if n}
    if not clean:
        return

    expires_at = time.time() + duration_minutes * 60

    # Register watcher. Keep prior active watchers for this chat so OTPs for
    # earlier-allocated numbers are still delivered (don't drop them). Only an
    # exact-duplicate watcher (same chat + same number set) is replaced to
    # refresh its expiry; expired watchers are pruned here too.
    if api_key not in _OTP_WATCHERS:
        _OTP_WATCHERS[api_key] = []
    now = time.time()
    _OTP_WATCHERS[api_key] = [
        w for w in _OTP_WATCHERS[api_key]
        if w["expires_at"] > now and not (w["chat_id"] == chat_id and w["numbers"] == clean)
    ]
    _OTP_WATCHERS[api_key].append({
        "chat_id": chat_id,
        "numbers": clean,
        "forward_chat_id": forward_chat_id,
        "number_countries": number_countries or {},
        "service_label": service_label,
        "expires_at": expires_at,
    })

    # Start shared job only if not already running for this API key
    job_name = f"shared_otp_{api_key}"
    if not context.job_queue.get_jobs_by_name(job_name):
        context.job_queue.run_repeating(
            _shared_otp_poll_job,
            interval=interval_seconds,
            first=2,
            name=job_name,
            data={"api_key": api_key},
        )


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data or ""

    if data == "close_msg":
        try:
            await query.message.delete()
        except Exception:
            pass
        return

    if data == "txn_history":
        uid = update.effective_user.id
        txns = await asyncio.to_thread(db_get_transactions, uid, 10)
        if not txns:
            await query.message.reply_text(
                "📜 <b>লেনদেন হিস্টরি</b>\n\n<i>এখনো কোনো লেনদেন নেই।</i>",
                parse_mode="HTML",
            )
            return
        icons = {"ref_bonus": "🎁", "withdraw": "💸", "refund": "↩️"}
        lines = ""
        for t in txns:
            icon = icons.get(t["kind"], "•")
            amt = t["amount"]
            sign = "+" if amt >= 0 else "−"
            date = (t["created_at"] or "")[:16]
            lines += f"{icon} <b>{sign}৳{abs(amt):.2f}</b> — {html.escape(t['note'] or '')}\n<i>{date}</i>\n\n"
        text = (
            "━\n"
            "   <b>📜 লেনদেন হিস্টরি</b>\n"
            "━\n\n"
            f"{lines}"
            "<i>সর্বশেষ ১০টি লেনদেন দেখানো হচ্ছে।</i>"
        )
        await query.message.reply_text(text, parse_mode="HTML")
        return

    if data == "ref_progress":
        uid = update.effective_user.id
        progress = await asyncio.to_thread(db_ref_progress, uid)
        if not progress:
            await query.message.reply_text(
                "📊 <b>রেফার অগ্রগতি</b>\n\n<i>তুমি এখনো কাউকে রেফার করোনি।</i>",
                parse_mode="HTML",
            )
            return
        lines = ""
        for p in progress[:15]:
            name = html.escape(p["name"])
            if p["done"]:
                lines += f"✅ <b>{name}</b> — {p['otp']} OTP (সম্পূর্ণ 🎉)\n"
            else:
                lines += (
                    f"⏳ <b>{name}</b> — {p['otp']}/{p['next']} OTP "
                    f"(আর <b>{p['remaining']}</b>টি বাকি)\n"
                )
        text = (
            "━\n"
            "   <b>📊 রেফার অগ্রগতি</b>\n"
            "━\n\n"
            f"{lines}\n"
            "<i>বন্ধু যত OTP নেবে, তুমি তত বোনাস পাবে!</i>"
        )
        await query.message.reply_text(text, parse_mode="HTML")
        return

    if data == "fa_refresh":
        secret = context.user_data.get("fa_secret")
        if not secret:
            await query.message.reply_text(
                "ℹ️ <b>আগে 2FA Code button থেকে secret key দিন।</b>",
                parse_mode="HTML",
            )
            return
        try:
            code, remaining = _generate_2fa(secret)
        except Exception:
            await query.message.reply_text(
                "❌ <b>Code refresh ব্যর্থ।</b>\n\n"
                "<i>আবার secret key দিন।</i>",
                parse_mode="HTML",
            )
            return
        keyboard = [
            [InlineKeyboardButton(f"📋 COPY: {code}", copy_text=CopyTextButton(text=code))],
        ]
        refresh_text = (
            "━\n"
            "   <b>🔐 2FA CODE REFRESHED</b>\n"
            "━\n\n"
            f"🔑 <b>CODE:</b> <code>{code}</code>\n"
            f"⏱ <b>VALID FOR:</b> {remaining}s\n\n"
            "<i>Code নকল করুন এবং ব্যবহার করুন।</i>"
        )
        await query.message.reply_text(
            refresh_text,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    if data.startswith("wd_approve:") or data.startswith("wd_reject:"):
        if not is_admin(update.effective_user.id, context):
            await query.answer("⛔ শুধু এডমিন।", show_alert=True)
            return
        action, _, wid_str = data.partition(":")
        try:
            wid = int(wid_str)
        except ValueError:
            return
        new_status = "approved" if action == "wd_approve" else "rejected"
        wd = await asyncio.to_thread(db_set_withdrawal_status, wid, new_status)
        if not wd:
            await query.answer("ℹ️ এই অনুরোধ আগেই প্রসেস হয়েছে।", show_alert=True)
            return
        amount = wd["amount"]
        target_user = wd["user_id"]
        if new_status == "approved":
            admin_note = f"✅ <b>#{wid} পেমেন্ট সম্পন্ন</b> — ৳{amount:.2f}"
            user_note = (
                "🎉 <b>উইথড্র সফল!</b>\n\n"
                f"🧾 <b>রিকোয়েস্ট:</b> #{wid}\n"
                f"💵 <b>৳{amount:.2f}</b> তোমার <code>{html.escape(wd['account'])}</code> নাম্বারে পাঠানো হয়েছে! ✅"
            )
        else:
            admin_note = f"❌ <b>#{wid} বাতিল</b> — ৳{amount:.2f} ফেরত দেওয়া হয়েছে"
            user_note = (
                "⚠️ <b>উইথড্র বাতিল হয়েছে</b>\n\n"
                f"🧾 <b>রিকোয়েস্ট:</b> #{wid}\n"
                f"💵 <b>৳{amount:.2f}</b> তোমার ব্যালেন্সে ফেরত দেওয়া হয়েছে।\n\n"
                "<i>সমস্যা হলে সাপোর্টে যোগাযোগ করো।</i>"
            )
        try:
            await query.edit_message_text(query.message.text_html + "\n\n" + admin_note, parse_mode="HTML")
        except Exception:
            pass
        try:
            await context.bot.send_message(target_user, user_note, parse_mode="HTML")
        except Exception as e:
            logger.warning("withdraw user notify failed for %s: %s", target_user, e)
        return

    if data == "fa_clear":
        context.user_data.pop("fa_secret", None)
        context.user_data.pop("awaiting_2fa_secret", None)
        await query.message.reply_text(
            "🗑 <b>Secret মুছে দেওয়া হয়েছে।</b>\n\n"
            "<i>নতুন secret key দিতে 2FA Code button চাপুন।</i>",
            parse_mode="HTML",
        )
        return

    if data == "check_join":
        invalidate_membership_cache(update.effective_user.id)
        for chat in _required_chats(context):
            ext_key = chat.get("external_key")
            if ext_key:
                context.user_data[f"ack_{ext_key}"] = True
        missing = await get_missing_chats(context.bot, update.effective_user.id, context)
        if not missing:
            success_msg = (
                "━\n"
                f"   <b>✅ ধন্যবাদ, {html.escape(update.effective_user.first_name or '')}!</b>\n"
                "━\n\n"
                "এখন আপনি বট ব্যবহার করতে পারবেন।\n\n"
                "<i>নিচের মেনু থেকে বেছে নিন:</i>"
            )
            await query.message.reply_text(success_msg, parse_mode="HTML", reply_markup=_main_markup(context))
        else:
            names = ", ".join(c["label"].split(" ", 1)[1] if " " in c["label"] else c["label"] for c in missing)
            await query.message.reply_text(
                f"❌ <b>এখনো join হয়নি:</b> {names}\n\n"
                "<i>দয়া করে join করে আবার চেষ্টা করুন।</i>",
                parse_mode="HTML",
                reply_markup=_join_prompt_markup(missing, context),
            )
        return

    if data == "back_to_services":
        if not await require_membership(update, context):
            return
        keyboard = [
            [InlineKeyboardButton(s["label"], callback_data=f"buy:{key}")]
            for key, s in load_services(context).items()
        ]
        try:
            await query.edit_message_text(
                "⚙️ সার্ভিস বেছে নিন:",
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
        except Exception:
            await query.message.reply_text(
                "⚙️ সার্ভিস বেছে নিন:",
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
        return

    if data.startswith("buy:") or data.startswith("country:"):
        if not await require_membership(update, context):
            return

        api_key = context.bot_data.get("cr_api_key", "")
        if not api_key:
            await query.message.reply_text(
                "\u26a0\ufe0f \u098f\u0987 \u09ac\u099f\u09c7 \u09a8\u09be\u09ae\u09cd\u09ac\u09b0 \u09b8\u09be\u09b0\u09cd\u09ad\u09bf\u09b8 \u098f\u0996\u09a8\u09cb \u099a\u09be\u09b2\u09c1 \u09b9\u09df\u09a8\u09bf\u0964\n"
                "<i>\u09b6\u09c0\u0998\u09cd\u09b0\u0987 \u09a8\u09a4\u09c1\u09a8 API \u09af\u09cb\u0997 \u0995\u09b0\u09be \u09b9\u09ac\u09c7\u0964</i>",
                parse_mode="HTML",
            )
            return

        if data.startswith("country:"):
            parts = data.split(":")
            key = parts[1]
            try:
                forced_idx = int(parts[2])
            except (IndexError, ValueError):
                forced_idx = None
        else:
            key = data.split(":", 1)[1]
            forced_idx = None

        service = load_services(context).get(key)
        if not service:
            await query.message.reply_text(
                "\u274c <b>\u09b8\u09be\u09b0\u09cd\u09ad\u09bf\u09b8 \u09aa\u09be\u0993\u09af\u09bc\u09be \u09af\u09be\u09df\u09a8\u09bf\u0964</b>",
                parse_mode="HTML",
            )
            return

        normalized = _normalize_ranges(service)
        if not normalized:
            await query.message.reply_text(
                "\u274c <b>\u098f\u0987 \u09b8\u09be\u09b0\u09cd\u09ad\u09bf\u09b8\u09c7 \u0995\u09cb\u09a8\u09cb range \u09b8\u09c7\u099f \u0995\u09b0\u09be \u09a8\u09c7\u0987\u0964</b>",
                parse_mode="HTML",
            )
            return

        # Multi-country picker
        if forced_idx is None and len(normalized) > 1:
            country_keyboard = [
                [InlineKeyboardButton(item["label"], callback_data=f"country:{key}:{i}")]
                for i, item in enumerate(normalized)
            ]
            country_keyboard.append([InlineKeyboardButton("\U0001f519 \u09ac\u09cd\u09af\u09be\u0995", callback_data="back_to_services")])
            header = (
                "\u2501\n"
                f"   <b>\U0001f30d {html.escape(service.get('label', key))}</b>\n"
                "\u2501\n\n"
                "<i>\u0985\u09a8\u09c1\u0997\u09cd\u09b0\u09b9\u09c7 \u09a6\u09c7\u09b6 \u09ac\u09c7\u099b\u09c7 \u09a8\u09bf\u09a8:</i>"
            )
            try:
                await query.edit_message_text(header, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(country_keyboard))
            except Exception:
                await query.message.reply_text(header, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(country_keyboard))
            return

        import random

        try:
            if forced_idx is not None and 0 <= forced_idx < len(normalized):
                chosen_items = [normalized[forced_idx]]
                repeat_cb = f"country:{key}:{forced_idx}"
                chosen_label = normalized[forced_idx]["label"]
            else:
                chosen_items = normalized
                repeat_cb = f"buy:{key}"
                chosen_label = normalized[0]["label"] if len(normalized) == 1 else ""

            count = max(1, int(service.get("count", 1)))
            allocated = []
            last_status = None
            last_msg = None

            # Allocate all requested numbers in parallel (bounded by the alloc
            # semaphore) so a multi-number service returns fast instead of
            # waiting for each upstream call one after another.
            rids = [
                re.sub(r"X+$", "", random.choice(chosen_items)["range"], flags=re.IGNORECASE)
                for _ in range(count)
            ]
            alloc_results = await asyncio.gather(
                *(_allocate_number(rid, api_key) for rid in rids)
            )
            for res in alloc_results:
                last_status = res["status"]
                logger.info("allocate response: %s %s", res["status"], str(res.get("json", ""))[:200])
                if res["status"] == 200 and res.get("json"):
                    d = (res["json"].get("data") or {})
                    full_number = d.get("full_number", "")
                    if full_number:
                        allocated.append({
                            "number": full_number,
                            "country": d.get("country", ""),
                            "operator": d.get("operator", ""),
                        })
                        continue
                last_msg = (res.get("json") or {}).get("message") if res.get("json") else res.get("error", "")

            if allocated:
                back_cb = f"buy:{key}" if len(normalized) > 1 else "back_to_services"
                otp_group_url = context.bot_data.get("otp_group_invite_url", GROUP_INVITE_URL)
                keyboard = [
                    [InlineKeyboardButton("\U0001f504 OTP \u099a\u09c7\u0995 \u0995\u09b0\u09c1\u09a8", url=otp_group_url)],
                    [InlineKeyboardButton("\u267b\ufe0f \u09a8\u09ae\u09cd\u09ac\u09b0 \u09aa\u09b0\u09bf\u09ac\u09b0\u09cd\u09a4\u09a8 \u0995\u09b0\u09c1\u09a8", callback_data=repeat_cb)],
                    [InlineKeyboardButton("\U0001f519 \u09ac\u09cd\u09af\u09be\u0995", callback_data=back_cb)],
                ]
                if len(allocated) == 1:
                    a = allocated[0]
                    text = (
                        "\u2705 <b>\u09a8\u09ae\u09cd\u09ac\u09b0 \u09aa\u09be\u0993\u09af\u09bc\u09be \u0997\u09c7\u099b\u09c7!</b>\n\n"
                        f"\U0001f4de <b>\u09a8\u09ae\u09cd\u09ac\u09b0:</b> <code>{html.escape(a['number'])}</code>\n"
                        f"\U0001f30d <b>\u09a6\u09c7\u09b6:</b> {html.escape(a['country'])}\n"
                        f"\U0001f4e1 <b>\u0985\u09aa\u09be\u09b0\u09c7\u099f\u09b0:</b> {html.escape(a['operator'])}\n\n"
                        "<i>\u23f3 OTP \u09b8\u09cd\u09ac\u09af\u09bc\u0982\u0995\u09cd\u09b0\u09bf\u09af\u09bc\u09ad\u09be\u09ac\u09c7 \u0986\u09b8\u09ac\u09c7 (\u09e9\u09e6 \u09ae\u09bf\u09a8\u09bf\u099f \u09aa\u09b0\u09cd\u09af\u09a8\u09cd\u09a4)\u0964</i>"
                    )
                else:
                    parts_list = [f"\u2705 <b>{len(allocated)}\u099f\u09bf \u09a8\u09ae\u09cd\u09ac\u09b0 \u09aa\u09be\u0993\u09af\u09bc\u09be \u0997\u09c7\u099b\u09c7!</b>\n"]
                    for i, a in enumerate(allocated, 1):
                        parts_list.append(
                            f"\n<b>\u09a8\u09ae\u09cd\u09ac\u09b0 {i}:</b>\n"
                            f"\U0001f4de <code>{html.escape(a['number'])}</code>\n"
                            f"\U0001f30d {html.escape(a['country'])} | \U0001f4e1 {html.escape(a['operator'])}\n"
                        )
                    parts_list.append("\n<i>\u23f3 OTP \u09b8\u09cd\u09ac\u09af\u09bc\u0982\u0995\u09cd\u09b0\u09bf\u09af\u09bc\u09ad\u09be\u09ac\u09c7 \u0986\u09b8\u09ac\u09c7 (\u09e9\u09e6 \u09ae\u09bf\u09a8\u09bf\u099f \u09aa\u09b0\u09cd\u09af\u09a8\u09cd\u09a4)\u0964</i>")
                    text = "".join(parts_list)

                try:
                    await query.edit_message_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(keyboard))
                except Exception:
                    await query.message.reply_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(keyboard))
                schedule_otp_poll(
                    context,
                    query.message.chat_id,
                    [a["number"] for a in allocated],
                    api_key,
                    forward_chat_id=context.bot_data.get("otp_forward_chat_id"),
                    service_label=service.get("label", key),
                    number_countries={re.sub(r"[^\d]", "", a["number"]): a.get("country", "") for a in allocated},
                )
                return

            await query.message.reply_text(
                f"\u26a0\ufe0f <b>\u09a8\u09ae\u09cd\u09ac\u09b0 \u09aa\u09be\u0993\u09af\u09bc\u09be \u09af\u09be\u09df\u09a8\u09bf</b>\n\n"
                f"<i>HTTP \u09b8\u09cd\u099f\u09cd\u09af\u09be\u099f\u09be\u09b8: {last_status}</i>\n"
                f"<i>{html.escape(str(last_msg or ''))}</i>",
                parse_mode="HTML",
            )

        except Exception as e:
            logger.exception("allocate error")
            await query.message.reply_text(
                f"\u274c <b>\u098f\u09aa\u09bf\u0986\u0987 \u09b8\u09ae\u09b8\u09cd\u09af\u09be</b>\n\n<i>{html.escape(str(e))}</i>",
                parse_mode="HTML",
            )

def _format_services(data: dict) -> str:
    if not data:
        return "📭 কোনো সার্ভিস নেই।"
    lines = ["📋 সার্ভিস তালিকা:\n"]
    for key, s in data.items():
        label = s.get("label", key)
        lines.append(f"🔸 `{key}` — {label}")
        for i, item in enumerate(_normalize_ranges(s)):
            lines.append(f"   {i}. *{item['label']}* — `{item['range']}`")
        lines.append("")
    return "\n".join(lines)


@_admin_only
async def cmd_services(update: Update, context: ContextTypes.DEFAULT_TYPE):
    services = load_services(context)
    if not services:
        await update.message.reply_text(
            "📬 <b>কোনো সার্ভিস নেই।</b>",
            parse_mode="HTML",
        )
        return
    lines = [
        "━\n"
        "   <b>📋 SERVICE LIST</b>\n"
        "━\n"
    ]
    for key, s in services.items():
        label = s.get("label", key)
        lines.append(f"\n<b>🔸 {key}</b> — {label}")
        for i, item in enumerate(_normalize_ranges(s)):
            lines.append(f"   <i>{i+1}. {item['label']}</i> — <code>{item['range']}</code>")
    lines.append("")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


@_admin_only
async def cmd_addrange(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args or []
    if len(args) < 2:
        await update.message.reply_text(
            "ব্যবহার: `/addrange <key> <range> [label]`\n"
            "উদাহরণ:\n"
            "`/addrange ig 23762176XXXXXX Bangladesh`\n"
            "`/addrange ig 8801XXXXXXXX India`",
            parse_mode="Markdown",
        )
        return
    key, new_range = args[0], args[1]
    new_label = " ".join(args[2:]).strip().strip('"') if len(args) > 2 else None
    data = load_services(context)
    if key not in data:
        await update.message.reply_text(
            f"❌ <b>`{key}` সার্ভিস নেই।</b>", parse_mode="HTML"
        )
        return
    normalized = _normalize_ranges(data[key])
    if any(item["range"] == new_range for item in normalized):
        await update.message.reply_text(
            f"ℹ️ <b>`{new_range}` আগে থেকেই আছে।</b>", parse_mode="HTML"
        )
        return
    label_to_use = new_label or f"Country {len(normalized) + 1}"
    normalized.append({"label": label_to_use, "range": new_range})
    data[key]["ranges"] = normalized
    data[key].pop("range", None)
    save_services(data, context)
    await update.message.reply_text(
        f"✅ <b>যোগ হয়েছে!</b>\n\n"
        f"<b>{label_to_use}</b> → <code>{new_range}</code>\n\n"
        f"<i>এখন `{key}`-঎ {len(normalized)}টি range আছে।</i>",
        parse_mode="HTML",
    )


@_admin_only
async def cmd_removerange(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args or []
    if len(args) < 2:
        await update.message.reply_text("ব্যবহার: `/removerange <key> <range বা label>`", parse_mode="Markdown")
        return
    key = args[0]
    target = " ".join(args[1:]).strip().strip('"')
    data = load_services(context)
    if key not in data:
        await update.message.reply_text(
            f"❌ <b>`{key}` সার্ভিস নেই।</b>", parse_mode="HTML"
        )
        return
    normalized = _normalize_ranges(data[key])
    new_list = [item for item in normalized if item["range"] != target and item["label"] != target]
    if len(new_list) == len(normalized):
        await update.message.reply_text(
            f"❌ <b>`{target}` পাওয়া যায়নি।</b>", parse_mode="HTML"
        )
        return
    if not new_list:
        await update.message.reply_text(
            "⚠️ <b>সব range বাদ দিলে সার্ভিস কাজ করবে না।</b>\n\n"
            "<i>কমপক্ষে একটি রাখুন।</i>",
            parse_mode="HTML",
        )
        return
    data[key]["ranges"] = new_list
    data[key].pop("range", None)
    save_services(data, context)
    await update.message.reply_text(
        f"✅ <b>বাদ দেওয়া হয়েছে।</b>\n\n"
        f"<i>এখন `{key}`-঎ {len(new_list)}টি range আছে।</i>",
        parse_mode="HTML",
    )


@_admin_only
async def cmd_setrange(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args or []
    if len(args) < 2:
        await update.message.reply_text(
            "ব্যবহার: `/setrange <key> <label=range,label=range,...>`\n"
            "উদাহরণ:\n"
            "`/setrange ig Bangladesh=8801XXX,India=91XXX,Pakistan=92XXX`\n"
            "label বাদ দিতে চাইলে: `/setrange ig 8801XXX,91XXX`",
            parse_mode="Markdown",
        )
        return
    key = args[0]
    raw_parts = [r.strip() for r in " ".join(args[1:]).split(",") if r.strip()]
    if not raw_parts:
        await update.message.reply_text("❌ <b>কমপক্ষে একটি range দিন।</b>", parse_mode="HTML")
        return
    new_ranges = []
    for i, part in enumerate(raw_parts):
        if "=" in part:
            label, rng = part.split("=", 1)
            label, rng = label.strip(), rng.strip()
            if not rng:
                continue
            new_ranges.append({"label": label or f"Country {i+1}", "range": rng})
        else:
            new_ranges.append({"label": f"Country {i+1}", "range": part})
    data = load_services(context)
    if key not in data:
        await update.message.reply_text(f"❌ <b>`{key}` সার্ভিস নেই।</b>", parse_mode="HTML")
        return
    data[key]["ranges"] = new_ranges
    data[key].pop("range", None)
    save_services(data, context)
    lines = [
        "━\n"
        f"   <b>✅ `{key}` UPDATED</b>\n"
        "━\n\n"
        f"<b>এখন {len(new_ranges)}টি range আছে:</b>\n"
    ]
    for item in new_ranges:
        lines.append(f"  • <i>{item['label']}</i> — <code>{item['range']}</code>")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


@_admin_only
async def cmd_setcountrylabel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args or []
    if len(args) < 3:
        await update.message.reply_text(
            "ব্যবহার: `/setcountrylabel <key> <range> <new label>`\n"
            "উদাহরণ: `/setcountrylabel ig 23762176XXXXXX Bangladesh`",
            parse_mode="Markdown",
        )
        return
    key, target_range = args[0], args[1]
    new_label = " ".join(args[2:]).strip().strip('"')
    data = load_services(context)
    if key not in data:
        await update.message.reply_text(f"❌ <b>`{key}` সার্ভিস নেই।</b>", parse_mode="HTML")
        return
    normalized = _normalize_ranges(data[key])
    found = False
    for item in normalized:
        if item["range"] == target_range:
            item["label"] = new_label
            found = True
            break
    if not found:
        await update.message.reply_text(f"❌ <b>`{target_range}` পাওয়া যায়নি।</b>", parse_mode="HTML")
        return
    data[key]["ranges"] = normalized
    data[key].pop("range", None)
    save_services(data, context)
    await update.message.reply_text(
        f"✅ <b>Label বদলানো হয়েছে!</b>\n\n"
        f"<code>{target_range}</code> → <b>{new_label}</b>",
        parse_mode="HTML",
    )


@_admin_only
async def cmd_addservice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args or []
    if len(args) < 3:
        await update.message.reply_text(
            "ব্যবহার: `/addservice <key> <label> <range>`\nউদাহরণ: `/addservice tg \"Telegram\" 99887766XXX`",
            parse_mode="Markdown",
        )
        return
    key = args[0]
    raw = " ".join(args[1:])
    if raw.startswith('"') and '"' in raw[1:]:
        end = raw.index('"', 1)
        label = raw[1:end]
        rest = raw[end+1:].strip()
    else:
        parts = raw.split(" ", 1)
        label = parts[0]
        rest = parts[1] if len(parts) > 1 else ""
    if not rest:
        await update.message.reply_text("❌ <b>একটি range দিন।</b>", parse_mode="HTML")
        return
    data = load_services(context)
    data[key] = {"label": label, "ranges": [rest.strip()]}
    save_services(data, context)
    await update.message.reply_text(
        f"✅ <b>নতুন সার্ভিস যোগ হয়েছে!</b>\n\n"
        f"<b>`{key}`</b> — {label}\n"
        f"<i>Range:</i> <code>{rest.strip()}</code>",
        parse_mode="HTML",
    )


@_admin_only
async def cmd_removeservice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args or []
    if not args:
        await update.message.reply_text("ব্যবহার: `/removeservice <key>`", parse_mode="Markdown")
        return
    key = args[0]
    data = load_services(context)
    if key not in data:
        await update.message.reply_text(f"❌ <b>`{key}` সার্ভিস নেই।</b>", parse_mode="HTML")
        return
    data.pop(key)
    save_services(data, context)
    await update.message.reply_text(
        f"✅ <b>`{key}` সার্ভিস বাদ দেওয়া হয়েছে।</b>", parse_mode="HTML"
    )


@_admin_only
async def cmd_setlabel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args or []
    if len(args) < 2:
        await update.message.reply_text("ব্যবহার: `/setlabel <key> <new label>`", parse_mode="Markdown")
        return
    key = args[0]
    new_label = " ".join(args[1:]).strip().strip('"')
    data = load_services(context)
    if key not in data:
        await update.message.reply_text(f"❌ <b>`{key}` সার্ভিস নেই।</b>", parse_mode="HTML")
        return
    data[key]["label"] = new_label
    save_services(data, context)
    await update.message.reply_text(
        f"✅ <b>Label বদলানো হয়েছে!</b>\n\n"
        f"<code>{key}</code> → <b>{new_label}</b>",
        parse_mode="HTML",
    )


@_admin_only
async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    admin_text = (
        "━\n"
        "   <b>🛠 ADMIN PANEL</b>\n"
        "━\n\n"
        "<b>📋 কমান্ড তালিকা:</b>\n\n"
        "<code>/services</code> — সব সার্ভিস দেখাও\n"
        "<code>/addrange &lt;key&gt; &lt;range&gt; [label]</code> — range যোগ\n"
        "<code>/removerange &lt;key&gt; &lt;range|label&gt;</code> — range বাদ\n"
        "<code>/setrange &lt;key&gt; &lt;label=range,...&gt;</code> — সব range বদলাও\n"
        "<code>/setcountrylabel &lt;key&gt; &lt;range&gt; &lt;label&gt;</code> — country name বদলাও\n"
        "<code>/addservice &lt;key&gt; &lt;label&gt; &lt;range&gt;</code> — নতুন সার্ভিস\n"
        "<code>/removeservice &lt;key&gt;</code> — সার্ভিস বাদ\n"
        "<code>/setlabel &lt;key&gt; &lt;new label&gt;</code> — service label বদলাও\n\n"
        "<i>শুধুমাত্র অ্যাডমিনদের জন্য।</i>"
    )
    await update.message.reply_text(admin_text, parse_mode="HTML")


def _normalize_2fa_secret(raw: str) -> str:
    # Strip invisible Unicode (zero-width spaces, RTL/LTR marks, etc.)
    s = re.sub(r"[^\x20-\x7E]", "", raw or "")
    s = s.upper().replace(" ", "").replace("-", "")
    # Common OCR-style substitutions used in TOTP apps
    s = s.replace("0", "O").replace("1", "I").replace("8", "B")
    s = re.sub(r"[^A-Z2-7]", "", s)
    # Ensure valid Base32 padding
    pad = (8 - len(s) % 8) % 8
    return s + "=" * pad


def _generate_2fa(secret: str) -> tuple[str, int]:
    totp = pyotp.TOTP(secret)
    code = totp.now()
    remaining = totp.interval - int(time.time()) % totp.interval
    return code, remaining


async def show_2fa_intro(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["awaiting_2fa_secret"] = True
    await update.message.reply_text(
        "🔐 <b>2FA</b> — Secret key পাঠান:",
        parse_mode="HTML",
    )


async def handle_2fa_secret(update: Update, context: ContextTypes.DEFAULT_TYPE, raw_text: str):
    context.user_data["awaiting_2fa_secret"] = False  # always clear — never leave user stuck
    secret = _normalize_2fa_secret(raw_text)
    if len(secret) < 16:
        await update.message.reply_text(
            "❌ Secret key ভুল!",
            parse_mode="HTML",
        )
        return
    try:
        code, remaining = _generate_2fa(secret)
    except Exception:
        logger.exception("2fa generate failed")
        await update.message.reply_text(
            "❌ কোড তৈরি হয়নি।",
            parse_mode="HTML",
        )
        return
    context.user_data["fa_secret"] = secret
    context.user_data["awaiting_2fa_secret"] = False
    keyboard = [
        [InlineKeyboardButton(f"📋 COPY: {code}", copy_text=CopyTextButton(text=code))],
    ]
    result_text = "🔑 <code>" + code + "</code>  ⏱" + str(remaining) + "s"
    await update.message.reply_text(
        result_text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def show_withdraw_intro(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    row = await asyncio.to_thread(db_get_user, uid)
    balance = row["balance"] if row else 0.0

    if balance < MIN_WITHDRAW:
        await update.message.reply_text(
            "━━━━━━━━━\n"
            "   <b>💰 WITHDRAW</b>\n"
            "━━━━━━━━━\n\n"
            f"👛 <b>তোমার ব্যালেন্স:</b> ৳{balance:.2f}\n\n"
            f"❌ উইথড্র করতে কমপক্ষে <b>৳{MIN_WITHDRAW:.0f}</b> থাকতে হবে।\n\n"
            f"💡 <i>আরো ৳{MIN_WITHDRAW - balance:.2f} জমা করো — বন্ধুদের রেফার করে আয় করো!</i>",
            parse_mode="HTML",
        )
        return

    context.user_data["awaiting_withdraw_amount"] = True
    await update.message.reply_text(
        "━━━━━━━━━\n"
        "   <b>💰 WITHDRAW</b>\n"
        "━━━━━━━━━\n\n"
        f"👛 <b>তোমার ব্যালেন্স:</b> ৳{balance:.2f}\n\n"
        f"💵 কত টাকা উইথড্র করতে চাও?\n"
        f"<i>(সর্বনিম্ন ৳{MIN_WITHDRAW:.0f}, সর্বোচ্চ ৳{balance:.0f})</i>\n\n"
        "👇 নিচে শুধু সংখ্যা লিখে পাঠাও:",
        parse_mode="HTML",
    )


async def handle_withdraw_amount(update: Update, context: ContextTypes.DEFAULT_TYPE, raw_text: str):
    uid = update.effective_user.id
    row = await asyncio.to_thread(db_get_user, uid)
    balance = row["balance"] if row else 0.0

    digits = raw_text.translate(str.maketrans("০১২৩৪৫৬৭৮৯", "0123456789")).strip()
    try:
        amount = float(digits)
    except ValueError:
        await update.message.reply_text(
            "❌ সঠিক সংখ্যা লিখো! যেমন: <code>20</code>",
            parse_mode="HTML",
        )
        return

    # Reject nan/inf and non-positive values (nan slips past < and > comparisons).
    if not math.isfinite(amount) or amount <= 0:
        await update.message.reply_text(
            "❌ সঠিক সংখ্যা লিখো! যেমন: <code>20</code>",
            parse_mode="HTML",
        )
        return

    if amount < MIN_WITHDRAW:
        await update.message.reply_text(
            f"❌ সর্বনিম্ন <b>৳{MIN_WITHDRAW:.0f}</b> উইথড্র করা যাবে।",
            parse_mode="HTML",
        )
        return
    if amount > balance:
        await update.message.reply_text(
            f"❌ তোমার ব্যালেন্স মাত্র <b>৳{balance:.2f}</b>। এত টাকা উইথড্র করা যাবে না।",
            parse_mode="HTML",
        )
        return

    context.user_data["withdraw_amount"] = amount
    context.user_data.pop("awaiting_withdraw_amount", None)
    context.user_data["awaiting_withdraw_account"] = True
    await update.message.reply_text(
        f"✅ <b>৳{amount:.2f}</b> উইথড্র করবে।\n\n"
        "📲 এখন তোমার <b>বিকাশ / নগদ</b> নাম্বার পাঠাও:\n"
        "<i>(যেমন: বিকাশ 01XXXXXXXXX)</i>",
        parse_mode="HTML",
    )


async def handle_withdraw_account(update: Update, context: ContextTypes.DEFAULT_TYPE, raw_text: str):
    uid = update.effective_user.id
    amount = context.user_data.get("withdraw_amount", 0.0)
    account = raw_text.strip()

    context.user_data.pop("awaiting_withdraw_account", None)
    context.user_data.pop("withdraw_amount", None)

    if amount < MIN_WITHDRAW:
        await update.message.reply_text(
            "❌ অনুরোধে সমস্যা হয়েছে। আবার <b>💰 WITHDRAW</b> চাপো।",
            parse_mode="HTML",
        )
        return

    if len(account) < 5:
        await update.message.reply_text(
            "❌ সঠিক পেমেন্ট নাম্বার পাঠাও। আবার <b>💰 WITHDRAW</b> চাপো।",
            parse_mode="HTML",
        )
        return

    wid = await asyncio.to_thread(db_create_withdrawal, uid, amount, "bKash/Nagad", account)
    if not wid:
        await update.message.reply_text(
            "❌ ব্যালেন্স যথেষ্ট নয়। আবার চেষ্টা করো।",
            parse_mode="HTML",
        )
        return

    await update.message.reply_text(
        "━━━━━━━━━\n"
        "   <b>✅ অনুরোধ জমা হয়েছে</b>\n"
        "━━━━━━━━━\n\n"
        f"🧾 <b>রিকোয়েস্ট ID:</b> #{wid}\n"
        f"💵 <b>পরিমাণ:</b> ৳{amount:.2f}\n"
        f"📲 <b>নাম্বার:</b> <code>{html.escape(account)}</code>\n\n"
        "⏳ <i>এডমিন যাচাই করে শীঘ্রই পেমেন্ট পাঠাবে। ধন্যবাদ!</i>",
        parse_mode="HTML",
    )

    # Notify admins
    user = update.effective_user
    uname = f"@{user.username}" if user.username else "নেই"
    admin_msg = (
        "🔔 <b>নতুন উইথড্র অনুরোধ</b>\n\n"
        f"🧾 <b>ID:</b> #{wid}\n"
        f"👤 <b>ইউজার:</b> {html.escape(user.first_name or '')} ({uname})\n"
        f"🆔 <b>User ID:</b> <code>{uid}</code>\n"
        f"💵 <b>পরিমাণ:</b> ৳{amount:.2f}\n"
        f"📲 <b>নাম্বার:</b> <code>{html.escape(account)}</code>"
    )
    admin_kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ পেমেন্ট দিয়েছি", callback_data=f"wd_approve:{wid}"),
        InlineKeyboardButton("❌ বাতিল", callback_data=f"wd_reject:{wid}"),
    ]])
    admin_targets = set(ADMIN_IDS) | set(context.bot_data.get("extra_admins") or set())
    for admin_id in admin_targets:
        try:
            await context.bot.send_message(admin_id, admin_msg, parse_mode="HTML", reply_markup=admin_kb)
        except Exception as e:
            logger.warning("withdraw admin notify failed for %s: %s", admin_id, e)


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    clean = (text or "").strip()
    lowered = clean.lower()

    menu_keywords = ("get number", "profile", "refer", "withdraw", "2fa")
    is_menu_tap = any(k in lowered for k in menu_keywords)

    if context.user_data.get("awaiting_2fa_secret") and not is_menu_tap:
        await handle_2fa_secret(update, context, clean)
        return

    if context.user_data.get("awaiting_withdraw_amount") and not is_menu_tap:
        await handle_withdraw_amount(update, context, clean)
        return

    if context.user_data.get("awaiting_withdraw_account") and not is_menu_tap:
        await handle_withdraw_account(update, context, clean)
        return

    if is_menu_tap:
        context.user_data.pop("awaiting_2fa_secret", None)
        context.user_data.pop("awaiting_withdraw_amount", None)
        context.user_data.pop("awaiting_withdraw_account", None)
        context.user_data.pop("withdraw_amount", None)

    if "get number" in lowered:
        await show_services(update, context)
    elif "profile" in lowered:
        uid = update.effective_user.id
        row = await asyncio.to_thread(db_get_user, uid)
        balance = row["balance"] if row else 0.0
        otp_count = row["otp_count"] if row else 0
        stats = await asyncio.to_thread(db_ref_stats, uid)
        profile = (
            "━\n"
            "   <b>👤 MY PROFILE</b>\n"
            "━\n\n"
            f"<b>নাম:</b> {html.escape(update.effective_user.first_name)}\n"
            f"<b>আইডি:</b> <code>{uid}</code>\n"
            f"<b>ব্যালেন্স:</b> ৳{balance:.2f}\n"
            f"<b>মোট OTP:</b> {otp_count}\n"
            f"<b>মোট রেফার:</b> {stats['total']} জন\n"
            f"<b>বোনাস আনলক:</b> {stats['paid']} জন\n"
            "<b>স্ট্যাটাস:</b> সক্রিয় ✅\n\n"
            "<i>আপনার একাউন্ট তথ্য।</i>"
        )
        profile_kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📜 লেনদেন হিস্টরি", callback_data="txn_history")],
        ])
        await update.message.reply_text(profile, parse_mode="HTML", reply_markup=profile_kb)
    elif "refer" in lowered:
        uid = update.effective_user.id
        bot_username = (await context.bot.get_me()).username
        link = f"https://t.me/{bot_username}?start={uid}"
        stats = await asyncio.to_thread(db_ref_stats, uid)
        row = await asyncio.to_thread(db_get_user, uid)
        balance = row["balance"] if row else 0.0
        share_text = (
            f"🔥 আমি এই বটে OTP নম্বর পাচ্ছি! তুমিও ব্যবহার করো:\n"
            f"{link}\n\n"
            f"👉 এই লিংকে ক্লিক করে বটে যোগ দাও এবং OTP নাও!"
        )
        medals = ["🥉", "🥈", "🥇", "🏅", "🎖"]
        milestone_lines = ""
        for i, (otp, bonus, _) in enumerate(REF_MILESTONES):
            medal = medals[i] if i < len(medals) else "⭐"
            tail = "🎉" if i == 0 else "বোনাস! 🎊"
            prefix = "তুমি পাবে" if i == 0 else "আরো"
            milestone_lines += f"{medal} <b>{otp}টি OTP</b> → {prefix} <b>৳{bonus:.0f}</b> {tail}\n"
        total_max = sum(b for _, b, _ in REF_MILESTONES)
        refer = (
            "━━━━━━━━━\n"
            "   <b>🎁 REFER &amp; EARN</b>\n"
            "━━━━━━━━━\n\n"
            "📢 <b>কিভাবে আয় করবে?</b>\n\n"
            "1️⃣ নিচের লিংকটি বন্ধুকে পাঠাও\n"
            "2️⃣ বন্ধু লিংকে ক্লিক করে বটে join করবে\n"
            "3️⃣ সে OTP নিতে থাকলে তুমি বোনাস পাবে!\n\n"
            "━━━━━━━━━\n"
            "🏆 <b>বোনাস মাইলস্টোন:</b>\n\n"
            f"{milestone_lines}\n"
            f"💡 <i>মোট সর্বোচ্চ ৳{total_max:.0f} পাবে প্রতি রেফারে!</i>\n"
            "━━━━━━━━━\n"
            f"🔗 <b>তোমার রেফার লিংক:</b>\n"
            f"<code>{link}</code>\n"
            "━━━━━━━━━\n\n"
            f"👥 <b>মোট রেফার করেছ:</b> {stats['total']} জন\n"
            f"✅ <b>বোনাস আনলক হয়েছে:</b> {stats['paid']} জন\n"
            f"💰 <b>রেফার থেকে আয়:</b> ৳{stats['earnings']:.2f}\n"
            f"👛 <b>মোট ব্যালেন্স:</b> ৳{balance:.2f}\n\n"
            "<i>⚡ যত বেশি রেফার, তত বেশি আয়!</i>"
        )
        keyboard = [
            [InlineKeyboardButton("📋 লিংক কপি করো", copy_text=CopyTextButton(text=link))],
            [InlineKeyboardButton("📤 বন্ধুকে শেয়ার করো", copy_text=CopyTextButton(text=share_text))],
            [InlineKeyboardButton("📊 রেফার অগ্রগতি", callback_data="ref_progress")],
            [InlineKeyboardButton("❌ বন্ধ করো", callback_data="close_msg")],
        ]
        await update.message.reply_text(refer, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(keyboard))
    elif "withdraw" in lowered:
        await show_withdraw_intro(update, context)
    elif "2fa" in lowered:
        await show_2fa_intro(update, context)


def _build_app(token: str, label: str, required_channels=None, extra_admins=None, cr_api_key=None, services_file=None, support_username=None, otp_forward_chat_id=None, otp_group_invite_url=None):
    from telegram.request import HTTPXRequest
    request = HTTPXRequest(
        connection_pool_size=256,
        pool_timeout=30.0,
        connect_timeout=15.0,
        read_timeout=30.0,
        write_timeout=30.0,
    )
    get_updates_request = HTTPXRequest(
        connection_pool_size=64,
        pool_timeout=30.0,
        connect_timeout=15.0,
        read_timeout=60.0,
        write_timeout=30.0,
    )
    app = (
        ApplicationBuilder()
        .token(token)
        .concurrent_updates(256)
        .request(request)
        .get_updates_request(get_updates_request)
        .build()
    )
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("admin", cmd_admin))
    app.add_handler(CommandHandler("services", cmd_services))
    app.add_handler(CommandHandler("addrange", cmd_addrange))
    app.add_handler(CommandHandler("removerange", cmd_removerange))
    app.add_handler(CommandHandler("setrange", cmd_setrange))
    app.add_handler(CommandHandler("setcountrylabel", cmd_setcountrylabel))
    app.add_handler(CommandHandler("addservice", cmd_addservice))
    app.add_handler(CommandHandler("removeservice", cmd_removeservice))
    app.add_handler(CommandHandler("setlabel", cmd_setlabel))
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_message))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.bot_data["bot_label"] = label
    if required_channels is not None:
        app.bot_data["required_channels"] = required_channels
    if extra_admins:
        app.bot_data["extra_admins"] = set(extra_admins)
    if cr_api_key:
        app.bot_data["cr_api_key"] = cr_api_key
    if services_file:
        app.bot_data["services_file"] = services_file
    if support_username:
        app.bot_data["support_username"] = support_username
    if otp_forward_chat_id:
        app.bot_data["otp_forward_chat_id"] = str(otp_forward_chat_id)
    if otp_group_invite_url:
        app.bot_data["otp_group_invite_url"] = otp_group_invite_url
    return app


async def _health_server():
    port = int(os.environ.get("PORT", 5000))
    async def handle(reader, writer):
        try:
            await asyncio.wait_for(reader.read(1024), timeout=5)
        except Exception:
            pass
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 2\r\n\r\nOK")
        await writer.drain()
        writer.close()
    try:
        server = await asyncio.start_server(handle, "0.0.0.0", port)
        logger.info("Health server listening on port %s", port)
        async with server:
            await server.serve_forever()
    except Exception as e:
        logger.warning("Health server could not start on port %s: %s", port, e)


async def _run_apps(apps):
    # Initialize SQLite database
    await asyncio.to_thread(init_db)
    logger.info("Database initialized at %s", DB_PATH)
    try:
        import concurrent.futures
        loop = asyncio.get_running_loop()
        loop.set_default_executor(
            concurrent.futures.ThreadPoolExecutor(max_workers=256, thread_name_prefix="bot-io")
        )
        logger.info("Default executor set to 256 workers")
    except Exception as e:
        logger.warning("Could not enlarge executor: %s", e)
    asyncio.create_task(_health_server())
    for app in apps:
        await app.initialize()
        await app.start()
        await app.updater.start_polling()
        logger.info("বট সফলভাবে চালু হয়েছে: %s", app.bot_data.get("bot_label", "?"))
    try:
        await asyncio.Event().wait()
    finally:
        for app in apps:
            try:
                await app.updater.stop()
                await app.stop()
                await app.shutdown()
            except Exception as e:
                logger.warning("Shutdown error for %s: %s", app.bot_data.get("bot_label"), e)


def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN environment variable is not set")

    apps = [_build_app(BOT_TOKEN, "Number Panel (Bot 1)", cr_api_key=os.environ.get("CRAPI_KEY", "").strip() or None, otp_forward_chat_id=OTP_FORWARD_CHAT_ID or None)]

    if BOT_TOKEN_2:
        bot2_channels = [
            {"id": "@nrnambarchenel1517", "url": "https://t.me/nrnambarchenel1517", "label": "📢 NR Nambar চ্যানেলে জয়েন"},
        ]
        apps.append(_build_app(
            BOT_TOKEN_2,
            "NR NAMBAR BOT (Bot 2)",
            required_channels=bot2_channels,
            extra_admins={8705862954},
            services_file="services2.json",
            support_username="@nrrifat15170",
            otp_forward_chat_id=os.environ.get("OTP_FORWARD_CHAT_ID_2", "").strip() or None,
            otp_group_invite_url=os.environ.get("OTP_GROUP_INVITE_URL_2", "").strip() or None,
        ))
        logger.info("দুটি বট চালু হচ্ছে...")
    else:
        logger.info("একটি বট চালু হচ্ছে (TELEGRAM_BOT_TOKEN_2 set নেই)...")

    asyncio.run(_run_apps(apps))


if __name__ == "__main__":
    main()
