#!/usr/bin/env python3
"""ربات ژورنال معاملاتی حرفه‌ای (تلگرام) - تک‌کاربره - اجرا روی GitHub Actions
دیتابیس داخل خود تلگرام ذخیره می‌شود (فایل پین‌شده)؛ نیازی به سرور نیست.
env: BOT_TOKEN, OWNER_ID, [STORE_CHAT_ID], [RUN_SECONDS], [PROXY], [DB_PATH]
"""
import asyncio
import hashlib
import io
import os
import signal
import sqlite3
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

import httpx

from telegram import BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (Application, ApplicationHandlerStop, CallbackQueryHandler,
                          CommandHandler, ContextTypes, ConversationHandler,
                          MessageHandler, TypeHandler, filters)

TOKEN = os.environ["BOT_TOKEN"]
OWNER = int(os.environ["OWNER_ID"])
PROXY = os.getenv("PROXY")  # مثلا socks5://127.0.0.1:1080 یا http://...
DB = os.getenv("DB_PATH", "journal.db")
STORE = int(os.getenv("STORE_CHAT_ID") or OWNER)  # چت/کانالی که فایل دیتابیس در آن پین می‌شود
RUN_SECONDS = int(os.getenv("RUN_SECONDS", "270"))
TZ = ZoneInfo("Asia/Tehran")
ASK, PICK, CONFIRM = range(3)

RULES = (
    "📜 قوانین ثابت من:\n"
    "1) استاپ‌لاس اجباری روی همه پوزیشن‌ها\n"
    "2) مارتینگل ممنوع (اضافه کردن مارجین به پوزیشن ضررده)\n"
    "3) بعد از ۲ ضرر پشت سر هم، تا پایان روز معامله نه\n"
    "4) خروج پله‌ای TP1/TP2/TP3 و انتقال استاپ به ورود بعد از TP1\n"
    "5) پیرامیدینگ فقط روی پوزیشن سودده، با حجم کمتر، حداکثر ۲ بار، بعد از تأیید CVD/دلتا"
)

# ----------------------------------------------------------------- DB
def db():
    c = sqlite3.connect(DB)
    c.row_factory = sqlite3.Row
    return c


def init():
    with db() as c:
        c.executescript("""
        create table if not exists trades(
          id integer primary key autoincrement,
          opened_at text, closed_at text, status text default 'open',
          symbol text, side text, exchange text, leverage real, margin real,
          entry real, stop real, tp1 real, tp2 real, tp3 real,
          size_usd real, risk_usd real, rr real,
          setup text, confluence text, emotion_pre text, notes text, photo text,
          exit_price real, fees real, pnl real, r_multiple real,
          followed_plan text, mistake text, emotion_post text, lesson text);
        create table if not exists days(
          day text primary key, mood text, discipline integer,
          worked text, improve text, plan text);
        create table if not exists settings(k text primary key, v text);
        """)


def setting(k, default=None):
    r = db().execute("select v from settings where k=?", (k,)).fetchone()
    return r["v"] if r else default


def set_setting(k, v):
    with db() as c:
        c.execute("insert or replace into settings values(?,?)", (k, str(v)))


def equity():
    start = float(setting("equity", 0))
    tot = db().execute("select coalesce(sum(pnl),0) s from trades where status='closed'").fetchone()["s"]
    return start + tot


def today():
    return datetime.now(TZ).date().isoformat()


def now_iso():
    return datetime.now(TZ).isoformat(timespec="seconds")


def consecutive_losses_today():
    rows = db().execute(
        "select pnl from trades where status='closed' and closed_at like ? order by closed_at desc",
        (today() + "%",)).fetchall()
    n = 0
    for r in rows:
        if r["pnl"] < 0:
            n += 1
        else:
            break
    return n


def streak():
    days = {r["day"] for r in db().execute("select day from days")}
    d = datetime.now(TZ).date()
    if d.isoformat() not in days:
        d -= timedelta(days=1)
    s = 0
    while d.isoformat() in days:
        s += 1
        d -= timedelta(days=1)
    return s


# ----------------------------------------------------------------- ذخیره‌سازی در تلگرام
API = f"https://api.telegram.org/bot{TOKEN}"
FILE_API = f"https://api.telegram.org/file/bot{TOKEN}"
_st = {"msg": None, "hash": None}


def _sha(path):
    return hashlib.sha256(open(path, "rb").read()).hexdigest()


def pull_db():
    """دانلود آخرین دیتابیس از پیام پین‌شده. اگر درخواست خطا بدهد اجرا متوقف می‌شود (تا دیتای قبلی بازنویسی نشود)."""
    if os.path.exists(DB):
        _st["hash"] = _sha(DB)
        return
    r = httpx.post(f"{API}/getChat", data={"chat_id": STORE}, timeout=30).json()
    if not r.get("ok"):
        raise SystemExit(f"getChat failed: {r}")
    pm = r["result"].get("pinned_message")
    if pm and pm.get("document") and pm["document"].get("file_name", "").startswith("journal"):
        f = httpx.post(f"{API}/getFile", data={"file_id": pm["document"]["file_id"]}, timeout=30).json()
        if not f.get("ok"):
            raise SystemExit(f"getFile failed: {f}")
        data = httpx.get(f"{FILE_API}/{f['result']['file_path']}", timeout=60).content
        with open(DB, "wb") as fh:
            fh.write(data)
        _st["msg"] = pm["message_id"]
        _st["hash"] = _sha(DB)


def push_db():
    """اگر دیتابیس تغییر کرده، فایل جدید را بفرست، پین کن و نسخه‌ی قبلی را پاک کن."""
    if not os.path.exists(DB) or _sha(DB) == _st["hash"]:
        return
    h = _sha(DB)
    with open(DB, "rb") as fh:
        r = httpx.post(f"{API}/sendDocument", timeout=60,
                       data={"chat_id": STORE, "disable_notification": "true",
                             "caption": f"💾 journal backup {now_iso()}"},
                       files={"document": ("journal.db", fh, "application/octet-stream")}).json()
    if not r.get("ok"):
        raise RuntimeError(f"sendDocument failed: {r}")
    mid = r["result"]["message_id"]
    p = httpx.post(f"{API}/pinChatMessage", timeout=30,
                   data={"chat_id": STORE, "message_id": mid, "disable_notification": "true"}).json()
    if p.get("ok") and _st["msg"]:
        httpx.post(f"{API}/unpinChatMessage", data={"chat_id": STORE, "message_id": _st["msg"]}, timeout=30)
        httpx.post(f"{API}/deleteMessage", data={"chat_id": STORE, "message_id": _st["msg"]}, timeout=30)
    _st.update(msg=mid, hash=h)


async def sync():
    await asyncio.to_thread(push_db)


# ----------------------------------------------------------------- helpers
FA2EN = str.maketrans("۰۱۲۳۴۵۶۷۸۹٫٬،", "0123456789.,,")


def to_num(t):
    return float(t.translate(FA2EN).replace(",", "").strip())


def liq_price(side, entry, lev):
    # تخمین ایزوله؛ ~۰.۵٪ برای مین‌تننس مارجین و کارمزد
    if side == "long":
        return entry * (1 - 1 / lev + 0.005)
    return entry * (1 + 1 / lev - 0.005)


def calc(t):
    size = t["margin"] * t["leverage"]
    qty = size / t["entry"]
    risk = abs(t["entry"] - t["stop"]) * qty
    final_tp = t.get("tp3") or t.get("tp2") or t["tp1"]
    rr1 = abs(t["tp1"] - t["entry"]) * qty / risk if risk else 0
    rrf = abs(final_tp - t["entry"]) * qty / risk if risk else 0
    return size, qty, risk, rr1, rrf


def milestone(s):
    return {3: "🥉 ۳ روز پشت سر هم!", 7: "🥈 یک هفته کامل! عادت داره شکل می‌گیره.",
            14: "🥇 دو هفته بدون وقفه!", 30: "🏆 یک ماه! الان یک تریدر ژورنال‌نویس واقعی‌ای.",
            60: "💎 ۶۰ روز! کمتر کسی تا اینجا میاد.", 100: "👑 ۱۰۰ روز! افسانه‌ای."}.get(s, "")


# ----------------------------------------------------------------- flows
EMO = [("😌 آرام و طبق پلن", "آرام"), ("😬 مضطرب", "مضطرب"), ("🤑 طمع/FOMO", "FOMO"),
       ("😤 انتقام/عجله", "انتقام"), ("😴 خسته/بی‌حوصله", "خسته")]

NEW = [
    ("symbol", "📌 نماد؟ (مثلاً QNTUSDT)", "text", None),
    ("side", "جهت معامله؟", "choice", [("🟢 Long", "long"), ("🔴 Short", "short")]),
    ("exchange", "صرافی؟", "choice", [(x, x) for x in ["Binance", "Bybit", "OKX", "LBank"]]),
    ("leverage", "اهرم؟ (عدد)", "num", None),
    ("margin", "مارجین به دلار؟", "num", None),
    ("entry", "قیمت ورود؟", "num", None),
    ("stop", "قیمت استاپ؟", "num", None),
    ("tp1", "TP1؟", "num", None),
    ("tp2", "TP2؟ (یا رد کن)", "num_opt", None),
    ("tp3", "TP3؟ (یا رد کن)", "num_opt", None),
    ("setup", "نوع ستاپ؟", "choice", [(x, x) for x in [
        "CHoCH+OB", "BOS/ریتست", "Sweep/ریورسال", "FVG/IFVG", "RTM نود قیمتی", "سایر"]]),
    ("confluence", "کانفلوئنس‌های تأییدکننده؟ (چندتایی، بعد ادامه)", "multi", [(x, x) for x in [
        "CVD دیورژانس", "دلتا اوردربوک", "CHoCH ۱۵دقیقه", "هیت‌مپ نقدینگی",
        "روند تایم بالاتر", "Funding/OI"]]),
    ("emotion_pre", "حالت ذهنی قبل ورود؟", "choice", EMO),
    ("notes", "دلیل ورود و پلن (کوتاه)؟ یا رد کن", "text_opt", None),
    ("photo", "اسکرین‌شات چارت؟ یا رد کن", "photo_opt", None),
]

CLOSE = [
    ("exit_price", "قیمت خروج؟", "num", None),
    ("fees", "کارمزد+فاندینگ کل به دلار؟ (یا رد کن = ۰)", "num_opt", None),
    ("followed_plan", "طبق پلن عمل کردی؟", "choice", [("✅ بله", "بله"), ("🟡 تا حدی", "تا حدی"), ("❌ نه", "نه")]),
    ("mistake", "اشتباه اصلی؟", "choice", [(x, x) for x in [
        "بدون اشتباه", "ورود زودهنگام", "ورود دیرهنگام/FOMO", "جابجایی استاپ", "مارتینگل",
        "خروج زودهنگام", "حجم زیاد", "نقض قانون ۲ ضرر", "ورود بدون کانفلوئنس", "سایر"]]),
    ("emotion_post", "حالت ذهنی حین/بعد معامله؟", "choice", EMO),
    ("lesson", "درس این معامله در یک جمله؟ (یا رد کن)", "text_opt", None),
]

DAILY = [
    ("mood", "حال کلی امروز؟", "choice", [("😄 عالی", "عالی"), ("🙂 خوب", "خوب"), ("😐 معمولی", "معمولی"),
                                         ("😞 بد", "بد")]),
    ("discipline", "نمره‌ی انضباط امروز به قوانین؟", "choice", [(str(i), i) for i in range(1, 6)]),
    ("worked", "✅ امروز چی خوب کار کرد؟", "text", None),
    ("improve", "🔧 چی رو باید فردا بهتر کنم؟", "text", None),
    ("plan", "🎯 پلن/سطوح فردا؟ (یا رد کن)", "text_opt", None),
]
FLOWS = {"new": NEW, "close": CLOSE, "daily": DAILY}


def build_kb(d):
    key, _, kind, opts = FLOWS[d["flow"]][d["i"]]
    rows = []
    if kind == "choice":
        b = [InlineKeyboardButton(l, callback_data=f"c:{i}") for i, (l, _) in enumerate(opts)]
        rows = [b[j:j + 2] for j in range(0, len(b), 2)]
    elif kind == "multi":
        sel = d.setdefault("sel", set())
        rows = [[InlineKeyboardButton(("✅ " if i in sel else "") + l, callback_data=f"m:{i}")]
                for i, (l, _) in enumerate(opts)]
        rows.append([InlineKeyboardButton("➡️ ادامه", callback_data="m:done")])
    if kind.endswith("_opt"):
        rows.append([InlineKeyboardButton("⏭ رد کن", callback_data="s")])
    rows.append([InlineKeyboardButton("❌ لغو", callback_data="x")])
    return InlineKeyboardMarkup(rows)


async def send_step(context):
    d = context.user_data
    steps = FLOWS[d["flow"]]
    key, prompt, kind, _ = steps[d["i"]]
    if kind == "multi":
        d["sel"] = set()
    await context.bot.send_message(d["chat"], f"({d['i'] + 1}/{len(steps)}) {prompt}",
                                   reply_markup=build_kb(d))


async def start_flow(update, context, flow, data=None):
    if update.callback_query:
        await update.callback_query.answer()
    d = context.user_data
    d.clear()
    d.update(flow=flow, i=0, data=data or {}, chat=update.effective_chat.id)
    await send_step(context)
    return ASK


async def start_new(update, context):
    msgs = []
    if consecutive_losses_today() >= 2:
        msgs.append("⛔ طبق قانون خودت امروز ۲ ضرر پشت سر هم داشتی. معامله‌ی جدید ممنوعه. "
                    "اگه واقعاً می‌خوای ثبت کنی ادامه بده، ولی بدون ثبت هم می‌تونی امروز رو تموم کنی.")
    n_today = db().execute("select count(*) c from trades where opened_at like ?", (today() + "%",)).fetchone()["c"]
    mx = int(setting("max_trades", 3))
    if n_today >= mx:
        msgs.append(f"⚠️ امروز {n_today} معامله باز کردی (سقف تو: {mx}). اوور‌تریدینگ؟")
    if msgs:
        await context.bot.send_message(update.effective_chat.id, "\n".join(msgs))
    return await start_flow(update, context, "new")


async def start_close(update, context):
    if update.callback_query:
        await update.callback_query.answer()
    chat = update.effective_chat.id
    rows = db().execute("select * from trades where status='open' order by id").fetchall()
    if not rows:
        await context.bot.send_message(chat, "معامله‌ی بازی نداری.")
        return ConversationHandler.END
    kb = [[InlineKeyboardButton(
        f"#{r['id']} {r['symbol']} {'Long' if r['side'] == 'long' else 'Short'} @ {r['entry']:g}",
        callback_data=f"o:{r['id']}")] for r in rows]
    kb.append([InlineKeyboardButton("❌ لغو", callback_data="x")])
    context.user_data.clear()
    context.user_data["chat"] = chat
    await context.bot.send_message(chat, "کدوم معامله بسته شد؟", reply_markup=InlineKeyboardMarkup(kb))
    return PICK


async def pick_trade(update, context):
    q = update.callback_query
    tid = int(q.data[2:])
    await q.answer()
    await q.edit_message_reply_markup(None)
    return await start_flow(update, context, "close", {"id": tid})


async def start_daily(update, context):
    chat = update.effective_chat.id
    await context.bot.send_message(chat, today_summary())
    return await start_flow(update, context, "daily")


async def on_text(update, context):
    d = context.user_data
    if "flow" not in d:
        return ConversationHandler.END
    key, _, kind, _ = FLOWS[d["flow"]][d["i"]]
    t = update.message.text
    if kind in ("num", "num_opt"):
        try:
            v = to_num(t)
            assert v >= 0
        except Exception:
            await update.message.reply_text("⚠️ فقط عدد بفرست (مثلاً 241.5)")
            return ASK
        d["data"][key] = v
    elif kind in ("text", "text_opt"):
        d["data"][key] = t.strip()
    else:
        return ASK
    return await advance(context)


async def on_photo(update, context):
    d = context.user_data
    if "flow" not in d:
        return ConversationHandler.END
    key, _, kind, _ = FLOWS[d["flow"]][d["i"]]
    if kind != "photo_opt":
        return ASK
    d["data"][key] = update.message.photo[-1].file_id
    return await advance(context)


async def on_cb(update, context):
    q = update.callback_query
    await q.answer()
    d = context.user_data
    if "flow" not in d:
        return ConversationHandler.END
    key, _, kind, opts = FLOWS[d["flow"]][d["i"]]
    v, shown = q.data, None
    if v == "s" and kind.endswith("_opt"):
        d["data"][key], shown = None, "—"
    elif v.startswith("c:") and kind == "choice":
        label, val = opts[int(v[2:])]
        d["data"][key], shown = val, label
    elif v.startswith("m:") and kind == "multi":
        if v == "m:done":
            sel = sorted(d["sel"])
            d["data"][key] = ",".join(opts[i][1] for i in sel) or "-"
            shown = d["data"][key]
        else:
            d["sel"] ^= {int(v[2:])}
            await q.edit_message_reply_markup(build_kb(d))
            return ASK
    else:
        return ASK
    await q.edit_message_text(f"{q.message.text}\n↳ {shown}")
    return await advance(context)


async def advance(context):
    d = context.user_data
    d["i"] += 1
    if d["i"] >= len(FLOWS[d["flow"]]):
        return await finish(context)
    await send_step(context)
    return ASK


# ----------------------------------------------------------------- finish
async def finish(context):
    d = context.user_data
    if d["flow"] == "new":
        return await finish_new(context)
    if d["flow"] == "close":
        return await finish_close(context)
    return await finish_daily(context)


async def finish_new(context):
    d, chat = context.user_data, context.user_data["chat"]
    t = d["data"]
    t["symbol"] = t["symbol"].upper().replace("/", "").replace(".P", "")
    side = t["side"]
    # --- اعتبارسنجی ساختاری
    ok = (t["stop"] < t["entry"] < t["tp1"]) if side == "long" else (t["tp1"] < t["entry"] < t["stop"])
    if not ok:
        await context.bot.send_message(chat, "❌ ورودی/استاپ/تارگت با جهت معامله نمی‌خونه. دوباره /new بزن.")
        d.clear()
        return ConversationHandler.END
    size, qty, risk, rr1, rrf = calc(t)
    liq = liq_price(side, t["entry"], t["leverage"])
    eq = equity()
    lines = [f"📋 خلاصه‌ی معامله {t['symbol']} {'Long' if side == 'long' else 'Short'} ({t['exchange']})",
             f"اهرم {t['leverage']:g}x | مارجین {t['margin']:g}$ | حجم ≈ {size:,.0f}$",
             f"ورود {t['entry']:g} | استاپ {t['stop']:g} | TP1 {t['tp1']:g}"
             + (f" | TP2 {t['tp2']:g}" if t.get("tp2") else "") + (f" | TP3 {t['tp3']:g}" if t.get("tp3") else ""),
             f"💸 ریسک: {risk:.2f}$" + (f" ({100 * risk / eq:.1f}٪ از اکانت)" if eq > 0 else ""),
             f"⚖️ R:R تا TP1 = {rr1:.2f} | تا آخرین TP = {rrf:.2f}",
             f"☠️ قیمت لیکوئید (تخمینی): {liq:,.4g}",
             f"ستاپ: {t['setup']} | کانفلوئنس: {t['confluence']}", f"ذهن: {t['emotion_pre']}"]
    warns = []
    if (side == "short" and t["stop"] >= liq) or (side == "long" and t["stop"] <= liq):
        warns.append("⛔ لیکوئید قبل از استاپ می‌خوره! اهرم رو کم کن یا مارجین بده بالا.")
    elif abs(liq - t["stop"]) / t["stop"] < 0.015:
        warns.append("⚠️ فاصله‌ی لیکوئید تا استاپ خیلی کمه (زیر ۱.۵٪). اسلیپیج/ویک می‌تونه لیکوئیدت کنه.")
    if eq > 0 and 100 * risk / eq > float(setting("risk_pct", 2)):
        warns.append(f"⚠️ ریسک بیشتر از سقف {setting('risk_pct', 2)}٪ اکانته.")
    if rrf < 1.5:
        warns.append("⚠️ R:R پایینه (زیر ۱.۵).")
    if t["emotion_pre"] in ("FOMO", "انتقام", "خسته"):
        warns.append(f"🧠 حالت ذهنی «{t['emotion_pre']}» داری. مطمئنی این معامله از پلنه نه از احساس؟")
    if "-" == t["confluence"]:
        warns.append("⚠️ هیچ کانفلوئنسی انتخاب نکردی. طبق قانونت ورود بدون هم‌پوشانی ممنوعه.")
    same = db().execute("select count(*) c from trades where status='open' and symbol=? and side=?",
                        (t["symbol"], side)).fetchone()["c"]
    if same:
        warns.append("⚠️ پوزیشن باز هم‌نماد و هم‌جهت داری. اگه اون ضررده، این مارتینگله؛ اگه سودده باید قانون پیرامیدینگ رو چک کنی.")
    if consecutive_losses_today() >= 2:
        warns.append("⛔ قانون ۲ ضرر پشت سر هم فعاله.")
    if warns:
        lines += ["", "🚨 هشدارها:"] + warns
    d["trade"] = dict(t, size_usd=size, risk_usd=risk, rr=rrf)
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("✅ ذخیره", callback_data="save"),
                                InlineKeyboardButton("❌ لغو", callback_data="x")]])
    await context.bot.send_message(chat, "\n".join(lines), reply_markup=kb)
    return CONFIRM


async def on_confirm(update, context):
    q = update.callback_query
    await q.answer()
    t = context.user_data.get("trade")
    if not t:
        return ConversationHandler.END
    cols = ["symbol", "side", "exchange", "leverage", "margin", "entry", "stop", "tp1", "tp2", "tp3",
            "size_usd", "risk_usd", "rr", "setup", "confluence", "emotion_pre", "notes", "photo"]
    with db() as c:
        cur = c.execute(f"insert into trades(opened_at,{','.join(cols)}) values(?,{','.join('?' * len(cols))})",
                        [now_iso()] + [t.get(k) for k in cols])
    await q.edit_message_reply_markup(None)
    await sync()
    await context.bot.send_message(
        context.user_data["chat"],
        f"✅ معامله #{cur.lastrowid} ثبت شد.\nبعد از خروج حتماً /close رو بزن. معامله‌ای که بسته نشه ثبت نشده حساب میشه 😉")
    context.user_data.clear()
    return ConversationHandler.END


async def finish_close(context):
    d, chat = context.user_data, context.user_data["chat"]
    x = d["data"]
    r = db().execute("select * from trades where id=?", (x["id"],)).fetchone()
    qty = r["size_usd"] / r["entry"]
    gross = (r["entry"] - x["exit_price"]) * qty if r["side"] == "short" else (x["exit_price"] - r["entry"]) * qty
    fees = x.get("fees") or 0
    net = gross - fees
    rm = net / r["risk_usd"] if r["risk_usd"] else None
    with db() as c:
        c.execute("""update trades set status='closed', closed_at=?, exit_price=?, fees=?, pnl=?, r_multiple=?,
                     followed_plan=?, mistake=?, emotion_post=?, lesson=? where id=?""",
                  (now_iso(), x["exit_price"], fees, net, rm, x["followed_plan"], x["mistake"],
                   x["emotion_post"], x.get("lesson"), x["id"]))
    emoji = "🟢" if net > 0 else ("🔴" if net < 0 else "⚪")
    out = [f"{emoji} #{x['id']} {r['symbol']} بسته شد", f"نتیجه: {net:+.2f}$" + (f" = {rm:+.2f}R" if rm is not None else ""),
           f"اکانت فعلی: {equity():,.2f}$" if float(setting("equity", 0)) else ""]
    if net > 0 and x["followed_plan"] == "نه":
        out.append("⚠️ سود گرفتی ولی بدون پلن. نتیجه‌ی خوب ≠ تصمیم خوب؛ این رو تکرار نکن.")
    if net < 0 and x["followed_plan"] == "بله":
        out.append("💪 ضرر طبق پلن بخشی از بازیه. انضباطت درسته.")
    if x["mistake"] != "بدون اشتباه":
        out.append(f"🔧 اشتباه ثبت‌شده: {x['mistake']}")
    if net < 0 and consecutive_losses_today() >= 2:
        out.append("\n⛔ ۲ ضرر پشت سر هم! طبق قانونت امروز تموم شد. لپ‌تاپ رو ببند و ژورنال روزانه رو پر کن (/daily).")
    elif net < 0:
        out.append("\nیه ضرر دیگه امروز = توقف. نفس بکش.")
    await sync()
    await context.bot.send_message(chat, "\n".join(o for o in out if o))
    d.clear()
    return ConversationHandler.END


async def finish_daily(context):
    d, chat = context.user_data, context.user_data["chat"]
    x = d["data"]
    with db() as c:
        c.execute("insert or replace into days values(?,?,?,?,?,?)",
                  (today(), x["mood"], x["discipline"], x["worked"], x["improve"], x.get("plan")))
    s = streak()
    msg = [f"📝 ژورنال امروز ثبت شد. 🔥 استریک: {s} روز"]
    if milestone(s):
        msg.append(milestone(s))
    msg.append("\nهر روزی که می‌نویسی، یه قدم از ‘تریدر احساسی’ دورتر و به ‘تریدر سیستماتیک’ نزدیک‌تری. فردا می‌بینمت 🌙")
    await sync()
    await context.bot.send_message(chat, "\n".join(msg))
    d.clear()
    return ConversationHandler.END


async def cancel(update, context):
    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_reply_markup(None)
    context.user_data.clear()
    await context.bot.send_message(update.effective_chat.id, "لغو شد.")
    return ConversationHandler.END


# ----------------------------------------------------------------- stats
def today_summary():
    rows = db().execute("select * from trades where opened_at like ? or closed_at like ?",
                        (today() + "%", today() + "%")).fetchall()
    closed = [r for r in rows if r["status"] == "closed" and (r["closed_at"] or "").startswith(today())]
    pnl = sum(r["pnl"] for r in closed)
    opn = db().execute("select count(*) c from trades where status='open'").fetchone()["c"]
    return (f"📅 امروز {today()}\nمعاملات بسته‌شده: {len(closed)} | سود/زیان: {pnl:+.2f}$\n"
            f"پوزیشن باز: {opn} | ضرر متوالی: {consecutive_losses_today()}\n🔥 استریک: {streak()} روز")


def group_lines(title, groups):
    out = [title]
    for k, v in sorted(groups.items(), key=lambda kv: sum(kv[1])):
        wr = 100 * sum(1 for p in v if p > 0) / len(v)
        out.append(f"• {k}: {len(v)} | {sum(v):+.1f}$ | WR {wr:.0f}٪")
    return out if len(out) > 1 else []


def stats_text(days):
    q = "select * from trades where status='closed'"
    args = []
    if days:
        since = (datetime.now(TZ) - timedelta(days=days)).isoformat()
        q += " and closed_at>=?"
        args.append(since)
    rows = db().execute(q + " order by closed_at", args).fetchall()
    if not rows:
        return "هنوز معامله‌ی بسته‌شده‌ای در این بازه نداری."
    pn = [r["pnl"] for r in rows]
    w = [p for p in pn if p > 0]
    l = [p for p in pn if p < 0]
    rs = [r["r_multiple"] for r in rows if r["r_multiple"] is not None]
    pf = (sum(w) / -sum(l)) if l else float("inf")
    cum, peak, mdd, cl, mcl = 0, 0, 0, 0, 0
    for p in pn:
        cum += p
        peak = max(peak, cum)
        mdd = max(mdd, peak - cum)
        cl = cl + 1 if p < 0 else 0
        mcl = max(mcl, cl)
    wr = 100 * len(w) / len(rows)
    out = [f"📊 آمار {'آخرین ' + str(days) + ' روز' if days else 'کل دوره'} ({len(rows)} معامله)",
           f"سود خالص: {sum(pn):+.2f}$ | Win Rate: {wr:.0f}٪",
           f"میانگین برد: {(sum(w) / len(w) if w else 0):+.2f}$ | میانگین باخت: {(sum(l) / len(l) if l else 0):+.2f}$",
           f"Profit Factor: {pf:.2f} | امید ریاضی: {(sum(rs) / len(rs) if rs else 0):+.2f}R",
           f"بیشترین دراودان: {mdd:.2f}$ | بیشترین ضرر متوالی: {mcl}"]

    def grp(key):
        g = {}
        for r in rows:
            g.setdefault(r[key] or "-", []).append(r["pnl"])
        return g

    conf = {}
    for r in rows:
        for c in (r["confluence"] or "-").split(","):
            conf.setdefault(c, []).append(r["pnl"])
    for title, g in (("\n🎯 به تفکیک ستاپ:", grp("setup")), ("\n🧩 به تفکیک کانفلوئنس:", conf),
                     ("\n❌ به تفکیک اشتباه:", grp("mistake")), ("\n🧠 ذهن قبل ورود:", grp("emotion_pre")),
                     ("\n💱 نماد:", grp("symbol")), ("\n📋 پایبندی به پلن:", grp("followed_plan"))):
        out += group_lines(title, g)
    leaks = {k: sum(v) for k, v in grp("mistake").items() if k != "بدون اشتباه"}
    if leaks and min(leaks.values()) < 0:
        k = min(leaks, key=leaks.get)
        out.append(f"\n🔍 گران‌ترین نشتی: «{k}» با {leaks[k]:+.1f}$. هدف این هفته: حذفش.")
    return "\n".join(out)


async def cmd_stats(update, context):
    a = context.args[0] if context.args else "30"
    await update.message.reply_text(stats_text(0 if a == "all" else int(a)))


async def cmd_chart(update, context):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    rows = db().execute("select closed_at,pnl from trades where status='closed' order by closed_at").fetchall()
    if not rows:
        await update.message.reply_text("داده‌ای نیست.")
        return
    cum, y = float(setting("equity", 0)), []
    for r in rows:
        cum += r["pnl"]
        y.append(cum)
    plt.figure(figsize=(8, 4))
    plt.plot(range(1, len(y) + 1), y, marker="o")
    plt.title("Equity curve")
    plt.xlabel("trade #")
    plt.grid(alpha=.3)
    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=120, bbox_inches="tight")
    plt.close()
    buf.seek(0)
    await update.message.reply_photo(buf)


async def cmd_export(update, context):
    from openpyxl import Workbook
    wb = Workbook()
    for name, q in (("trades", "select * from trades order by id"), ("days", "select * from days order by day")):
        ws = wb.active if name == "trades" else wb.create_sheet()
        ws.title = name
        cur = db().execute(q)
        ws.append([c[0] for c in cur.description])
        for r in cur:
            ws.append(list(tuple(r)))
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    buf.name = f"journal_{today()}.xlsx"
    await update.message.reply_document(buf)


async def cmd_open(update, context):
    rows = db().execute("select * from trades where status='open'").fetchall()
    if not rows:
        await update.message.reply_text("پوزیشن باز نداری.")
        return
    await update.message.reply_text("\n".join(
        f"#{r['id']} {r['symbol']} {r['side']} @ {r['entry']:g} | SL {r['stop']:g} | TP1 {r['tp1']:g} | ریسک {r['risk_usd']:.1f}$"
        for r in rows))


async def cmd_delete(update, context):
    if not context.args:
        await update.message.reply_text("مثال: /delete 12")
        return
    with db() as c:
        c.execute("delete from trades where id=?", (int(context.args[0]),))
    await update.message.reply_text("حذف شد.")


async def cmd_set(update, context):
    name = update.message.text.split()[0][1:].split("@")[0]
    key = {"equity": "equity", "risk": "risk_pct", "maxtrades": "max_trades"}[name]
    if not context.args:
        await update.message.reply_text(f"مقدار فعلی: {setting(key, '-')}\nمثال: /{name} 500")
        return
    set_setting(key, to_num(context.args[0]))
    await update.message.reply_text("✅ تنظیم شد.")


MENU = InlineKeyboardMarkup([
    [InlineKeyboardButton("➕ معامله جدید", callback_data="menu_new"),
     InlineKeyboardButton("✅ بستن معامله", callback_data="menu_close")],
    [InlineKeyboardButton("📝 ژورنال روزانه", callback_data="menu_daily"),
     InlineKeyboardButton("📊 آمار", callback_data="menu_stats")],
    [InlineKeyboardButton("📂 پوزیشن‌های باز", callback_data="menu_open")]])


async def cmd_menu(update, context):
    await update.message.reply_text(
        "📓 ژورنال معاملاتی\nاول /equity <موجودی اولیه> رو تنظیم کن.\n\n" + RULES, reply_markup=MENU)


async def menu_cb(update, context):
    q = update.callback_query
    await q.answer()
    if q.data == "menu_stats":
        await context.bot.send_message(q.message.chat_id, stats_text(30))
    elif q.data == "menu_open":
        rows = db().execute("select * from trades where status='open'").fetchall()
        txt = "\n".join(f"#{r['id']} {r['symbol']} {r['side']} @ {r['entry']:g}" for r in rows) or "پوزیشن باز نداری."
        await context.bot.send_message(q.message.chat_id, txt)


async def cmd_rules(update, context):
    await update.message.reply_text(RULES)


# ----------------------------------------------------------------- jobs
async def send_morning(bot):
    last = db().execute("select plan from days where plan is not null order by day desc limit 1").fetchone()
    txt = [f"☀️ صبح بخیر. 🔥 استریک: {streak()} روز", "", RULES]
    if last and last["plan"]:
        txt += ["", f"🎯 پلنی که دیشب برای امروز نوشتی:\n{last['plan']}"]
    txt += ["", "قبل از اولین معامله: خواب کافی؟ ذهن آروم؟ پلن و سطوح آماده؟"]
    await bot.send_message(OWNER, "\n".join(txt), reply_markup=MENU)


async def send_evening(bot, n):
    done = db().execute("select 1 from days where day=?", (today(),)).fetchone()
    opn = db().execute("select count(*) c from trades where status='open'").fetchone()["c"]
    if opn:
        await bot.send_message(OWNER, f"⚠️ {opn} پوزیشن باز داری. اگه بسته شده، /close یادت نره.")
    if done:
        return
    late = "⏳ نیم ساعت تا آخر روز! " if n == 2 else ""
    txt = f"{late}📝 وقت ژورنال امروزه (۲ دقیقه). استریک {streak()} روزه‌ات رو نسوزون 🔥"
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("📝 شروع ژورنال", callback_data="menu_daily")]])
    await bot.send_message(OWNER, txt, reply_markup=kb)


async def send_weekly(bot):
    await bot.send_message(OWNER, "🗓 گزارش هفتگی\n\n" + stats_text(7) +
                           "\n\n🎯 یک چیز رو برای هفته‌ی بعد انتخاب کن که اصلاح بشه.")


def due(name, hh, mm, window_min, weekday=None):
    """چون ربات دائماً روشن نیست: اگر زمانش گذشته و هنوز در بازه‌ی مجاز است و امروز نفرستاده‌ایم، بفرست."""
    n = datetime.now(TZ)
    if weekday is not None and n.weekday() != weekday:
        return False
    start = n.replace(hour=hh, minute=mm, second=0, microsecond=0)
    if not (start <= n < start + timedelta(minutes=window_min)):
        return False
    if setting("sent_" + name) == today():
        return False
    set_setting("sent_" + name, today())
    return True


async def job_schedule(context):
    bot = context.bot
    if due("morning", 8, 30, 240):
        await send_morning(bot)
    if due("eve1", 22, 0, 89):
        await send_evening(bot, 1)
    if due("eve2", 23, 30, 29):
        await send_evening(bot, 2)
    if due("weekly", 20, 0, 180, weekday=4):  # جمعه
        await send_weekly(bot)


async def job_stop(context):
    """پایان اجرا؛ اگر وسط ثبت یک معامله هستی تا ۳ دقیقه صبر می‌کند."""
    grace = context.job.data
    if context.application.user_data.get(OWNER, {}).get("flow") and grace < 3:
        context.job_queue.run_once(job_stop, 60, data=grace + 1)
        return
    os.kill(os.getpid(), signal.SIGINT)


async def guard(update, context):
    u = update.effective_user
    if u is None or u.id != OWNER:
        raise ApplicationHandlerStop


async def post_init(app):
    await app.bot.set_my_commands([
        BotCommand("new", "ثبت معامله جدید"), BotCommand("close", "بستن معامله"),
        BotCommand("daily", "ژورنال روزانه"), BotCommand("open", "پوزیشن‌های باز"),
        BotCommand("stats", "آمار (۷ / ۳۰ / all)"), BotCommand("chart", "منحنی اکوییتی"),
        BotCommand("export", "خروجی اکسل"), BotCommand("rules", "قوانین"),
        BotCommand("equity", "موجودی اولیه"), BotCommand("risk", "سقف ریسک ٪"),
        BotCommand("maxtrades", "سقف معامله روزانه"), BotCommand("menu", "منو")])


def main():
    b = Application.builder().token(TOKEN).post_init(post_init)
    if PROXY:
        b = b.proxy(PROXY).get_updates_proxy(PROXY)
    app = b.build()
    app.add_handler(TypeHandler(Update, guard), group=-1)

    conv = ConversationHandler(
        entry_points=[CommandHandler("new", start_new), CallbackQueryHandler(start_new, pattern="^menu_new$"),
                      CommandHandler("close", start_close), CallbackQueryHandler(start_close, pattern="^menu_close$"),
                      CommandHandler("daily", start_daily), CallbackQueryHandler(start_daily, pattern="^menu_daily$")],
        states={
            PICK: [CallbackQueryHandler(pick_trade, pattern="^o:")],
            ASK: [CallbackQueryHandler(on_cb, pattern="^(c:|m:|s$)"),
                  MessageHandler(filters.TEXT & ~filters.COMMAND, on_text),
                  MessageHandler(filters.PHOTO, on_photo)],
            CONFIRM: [CallbackQueryHandler(on_confirm, pattern="^save$")],
        },
        fallbacks=[CommandHandler("cancel", cancel), CallbackQueryHandler(cancel, pattern="^x$")],
        allow_reentry=True)
    app.add_handler(conv)
    for name, fn in (("start", cmd_menu), ("menu", cmd_menu), ("stats", cmd_stats), ("chart", cmd_chart),
                     ("export", cmd_export), ("open", cmd_open), ("delete", cmd_delete), ("rules", cmd_rules),
                     ("equity", cmd_set), ("risk", cmd_set), ("maxtrades", cmd_set)):
        app.add_handler(CommandHandler(name, fn))
    app.add_handler(CallbackQueryHandler(menu_cb, pattern="^menu_(stats|open)$"))

    jq = app.job_queue
    jq.run_repeating(job_schedule, interval=60, first=3)
    jq.run_once(job_stop, RUN_SECONDS, data=0)
    app.run_polling()


if __name__ == "__main__":
    pull_db()
    init()
    try:
        main()
    finally:
        push_db()
