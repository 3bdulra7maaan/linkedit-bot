import os
import logging
import html
import urllib.parse
import asyncio
from threading import Thread

from flask import Flask
import pandas as pd

from jobspy import scrape_jobs

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)
from telegram.constants import ParseMode


# -----------------------
# Flask (Render Web Service needs PORT)
# -----------------------
flask_app = Flask(__name__)

@flask_app.get("/")
def home():
    return "Bot is running!"

def run_flask():
    port = int(os.environ.get("PORT", "10000"))
    flask_app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)


# -----------------------
# Bot config (ENV VARS)
# -----------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
WHATSAPP_LINK = os.environ.get("WHATSAPP_LINK", "")

if not BOT_TOKEN:
    raise RuntimeError("Missing BOT_TOKEN environment variable")
if not WHATSAPP_LINK:
    WHATSAPP_LINK = "https://whatsapp.com/channel/0029Vat1TW960eBmmdCzvA0r"

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("telegram-bot")


# -----------------------
# Data
# -----------------------
COUNTRIES = {
    "qa": {"name": "قطر 🇶🇦", "indeed_country": "Qatar", "location": "Qatar"},
    "ae": {"name": "الإمارات 🇦🇪", "indeed_country": "United Arab Emirates", "location": "United Arab Emirates"},
    "sa": {"name": "السعودية 🇸🇦", "indeed_country": "Saudi Arabia", "location": "Saudi Arabia"},
    "bh": {"name": "البحرين 🇧🇭", "indeed_country": "Bahrain", "location": "Bahrain"},
}

JOB_CATEGORIES = {
    "eng": {"name": "هندسة 🔧", "query": "engineer"},
    "it": {"name": "تقنية المعلومات 💻", "query": "IT software developer"},
    "acc": {"name": "محاسبة 📊", "query": "accountant"},
    "mkt": {"name": "تسويق 📢", "query": "marketing"},
    "hr": {"name": "موارد بشرية 👥", "query": "human resources"},
    "med": {"name": "طب وصحة 🏥", "query": "medical healthcare"},
    "edu": {"name": "تعليم 📚", "query": "teacher education"},
    "sales": {"name": "مبيعات 🛒", "query": "sales"},
    "admin": {"name": "إدارة 🏢", "query": "admin manager"},
    "fin": {"name": "مالية وبنوك 🏦", "query": "finance banking"},
}


# -----------------------
# Helpers
# -----------------------
def esc(t: str) -> str:
    return html.escape(str(t or ""))

def format_job(job: dict, country_name: str) -> tuple[str, str]:
    title = esc(job.get("title", "غير محدد"))
    company = esc(job.get("company", "غير محدد"))
    job_url = str(job.get("job_url", "") or "")
    desc = esc((job.get("description", "") or "")[:350] + "...")

    msg = (
        "━━━━━━━━━━━━━━━━━━━━━\n"
        f"💼 <b>{title} - {country_name}</b>\n"
        f"🏢 {company}\n\n"
        f"{desc}\n\n"
    )
    if job_url:
        msg += f"🔗 <a href='{esc(job_url)}'>رابط التقديم المباشر</a>\n"
    msg += f"\n👉 <a href='{esc(WHATSAPP_LINK)}'>تابعنا على واتساب للمزيد</a>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━"

    share_text = f"💼 {title} - {country_name}\n🔗 التقديم: {job_url}\n\n📱 للمزيد: {WHATSAPP_LINK}"
    whatsapp_url = f"https://api.whatsapp.com/send?text={urllib.parse.quote(share_text)}"
    return msg, whatsapp_url

async def scrape_jobs_async(search_term: str, country_key: str, results: int = 10):
    """
    Run scraping in a thread so it doesn't block the bot.
    """
    c = COUNTRIES[country_key]
    loop = asyncio.get_running_loop()

    def _run():
        # python-jobspy usage
        df = scrape_jobs(
            site_name=["indeed"],
            search_term=search_term,
            location=c["location"],
            results_wanted=results,
            country_indeed=c["indeed_country"],
            hours_old=72,
        )
        if isinstance(df, pd.DataFrame):
            return df.to_dict(orient="records")
        return []

    return await loop.run_in_executor(None, _run)


# -----------------------
# UI flows
# -----------------------
def main_menu():
    keyboard = [
        [InlineKeyboardButton("🔍 بحث بكلمة", callback_data="search")],
        [InlineKeyboardButton("📂 بحث حسب التصنيف", callback_data="categories")],
        [InlineKeyboardButton("🌍 اختيار الدولة", callback_data="country")],
        [InlineKeyboardButton("📱 تابعنا على واتساب", url=WHATSAPP_LINK)],
    ]
    return InlineKeyboardMarkup(keyboard)

def countries_menu(prefix: str):
    rows = []
    for k, v in COUNTRIES.items():
        rows.append([InlineKeyboardButton(v["name"], callback_data=f"{prefix}:{k}")])
    rows.append([InlineKeyboardButton("⬅️ رجوع", callback_data="back_home")])
    return InlineKeyboardMarkup(rows)

def categories_menu(country_key: str):
    rows = []
    for k, v in JOB_CATEGORIES.items():
        rows.append([InlineKeyboardButton(v["name"], callback_data=f"cat:{country_key}:{k}")])
    rows.append([InlineKeyboardButton("⬅️ رجوع", callback_data="back_home")])
    return InlineKeyboardMarkup(rows)


# -----------------------
# Handlers
# -----------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.setdefault("country", "qa")
    await update.message.reply_text(
        "👋 أهلاً بك في بوت <b>LinkedIt By Abdulrahman</b>\n\n"
        "اختر من القائمة أدناه للبدء",
        parse_mode=ParseMode.HTML,
        reply_markup=main_menu(),
    )

async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data
    if data == "back_home":
        await query.edit_message_text("القائمة الرئيسية 👇", reply_markup=main_menu())
        return

    if data == "country":
        await query.edit_message_text("اختر الدولة 🌍", reply_markup=countries_menu("setcountry"))
        return

    if data.startswith("setcountry:"):
        country_key = data.split(":")[1]
        context.user_data["country"] = country_key
        await query.edit_message_text(f"تم اختيار: {COUNTRIES[country_key]['name']}\n\nالقائمة 👇", reply_markup=main_menu())
        return

    if data == "categories":
        country_key = context.user_data.get("country", "qa")
        await query.edit_message_text("اختر التصنيف 📂", reply_markup=categories_menu(country_key))
        return

    if data == "search":
        context.user_data["awaiting_search"] = True
        await query.edit_message_text("اكتب كلمة البحث الآن (مثال: Data Analyst, Nurse, IT Support) ✍️\n\n⬅️ اكتب /start للرجوع")
        return

    if data.startswith("cat:"):
        _, country_key, cat_key = data.split(":")
        search_term = JOB_CATEGORIES[cat_key]["query"]
        await query.edit_message_text(f"⏳ جاري البحث عن وظائف: {JOB_CATEGORIES[cat_key]['name']} في {COUNTRIES[country_key]['name']} ...")

        jobs = await scrape_jobs_async(search_term, country_key, results=8)
        if not jobs:
            await query.edit_message_text("ما لقيت نتائج حالياً، جرّب بعد شوية أو غيّر الكلمة", reply_markup=main_menu())
            return

        # Send results
        for job in jobs[:8]:
            msg, wa = format_job(job, COUNTRIES[country_key]["name"])
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("📤 مشاركة في واتساب", url=wa)]])
            await context.bot.send_message(chat_id=query.message.chat_id, text=msg, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)

        await context.bot.send_message(chat_id=query.message.chat_id, text="✅ انتهينا\n\nالقائمة 👇", reply_markup=main_menu())
        return

async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("awaiting_search"):
        return

    context.user_data["awaiting_search"] = False
    search_term = update.message.text.strip()
    country_key = context.user_data.get("country", "qa")

    await update.message.reply_text(f"⏳ جاري البحث عن: <b>{esc(search_term)}</b> في {COUNTRIES[country_key]['name']} ...", parse_mode=ParseMode.HTML)

    jobs = await scrape_jobs_async(search_term, country_key, results=8)
    if not jobs:
        await update.message.reply_text("ما لقيت نتائج حالياً، جرّب كلمة مختلفة", reply_markup=main_menu())
        return

    for job in jobs[:8]:
        msg, wa = format_job(job, COUNTRIES[country_key]["name"])
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("📤 مشاركة في واتساب", url=wa)]])
        await update.message.reply_text(msg, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)

    await update.message.reply_text("✅ انتهينا\n\nالقائمة 👇", reply_markup=main_menu())


def main():
    # Flask keeps Render web service alive (needs PORT)
    Thread(target=run_flask, daemon=True).start()

    application = Application.builder().token(BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(on_callback))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    logger.info("Bot started...")
    application.run_polling(close_loop=False)

if __name__ == "__main__":
    main()
