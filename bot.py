import logging
import asyncio
import html
import re
import warnings
import urllib.parse
import os
from datetime import datetime
from threading import Thread
from concurrent.futures import ThreadPoolExecutor

from flask import Flask
from jobspy import scrape_jobs
import pandas as pd

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.error import BadRequest, TelegramError
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)

# =========================================================
# LinkedIt By Abdulrahman - Telegram Job Bot (Render-ready)
# Improved version with:
# - Caching for faster repeated searches
# - Concurrent search across all countries
# - Pagination instead of flooding 15 messages
# - Promotion links (Bot, Channel, WhatsApp)
# - Health check endpoint
# - Search timeout protection
# - Safe callback query answering
# - Global error handler
# =========================================================

# --- Caching ---
try:
    from cachetools import TTLCache
except ImportError:
    class TTLCache(dict):
        def __init__(self, maxsize=100, ttl=1800):
            super().__init__()
            self.maxsize = maxsize

# --- Flask Server to keep Render alive / health check ---
app = Flask("")

@app.route("/")
def home():
    return "LinkedIt Bot is running!"

@app.route("/health")
def health():
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}, 200

# --- Bot Settings (ENV only) ---
BOT_TOKEN = os.environ.get("BOT_TOKEN")  # MUST be set in Render env vars
WHATSAPP_LINK = os.environ.get("WHATSAPP_LINK", "")
BOT_LINK = os.environ.get("BOT_LINK", "")          # e.g. https://t.me/YourBotName
CHANNEL_LINK = os.environ.get("CHANNEL_LINK", "")   # e.g. https://t.me/YourChannel

def run_flask():
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)

# Logging setup
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)
warnings.filterwarnings("ignore", category=FutureWarning)

# --- Cache: 30 minutes, max 200 entries ---
job_cache = TTLCache(maxsize=200, ttl=1800)

# --- Thread pool for concurrent scraping ---
executor = ThreadPoolExecutor(max_workers=4)

# --- Constants ---
RESULTS_PER_PAGE = 5
MAX_RESULTS = 15
HOURS_OLD = 168       # 1 week
SEARCH_TIMEOUT = 60   # seconds

# Supported Countries
COUNTRIES = {
    "qa": {"name": "قطر 🇶🇦", "flag": "🇶🇦", "name_en": "Qatar", "indeed_country": "Qatar", "location": "Qatar"},
    "ae": {"name": "الإمارات 🇦🇪", "flag": "🇦🇪", "name_en": "United Arab Emirates", "indeed_country": "United Arab Emirates", "location": "United Arab Emirates"},
    "sa": {"name": "السعودية 🇸🇦", "flag": "🇸🇦", "name_en": "Saudi Arabia", "indeed_country": "Saudi Arabia", "location": "Saudi Arabia"},
    "bh": {"name": "البحرين 🇧🇭", "flag": "🇧🇭", "name_en": "Bahrain", "indeed_country": "Bahrain", "location": "Bahrain"},
}

# Job Categories
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


# ========================
# Helper Functions
# ========================

def escape_html(text: str) -> str:
    if not text:
        return ""
    return html.escape(str(text))

def extract_email_from_text(text: str) -> str:
    if not text:
        return ""
    emails = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", str(text))
    return emails[0] if emails else ""

def _build_promo_keyboard_rows() -> list:
    """Build promotion button rows dynamically based on available links."""
    rows = []
    promo_row = []
    if WHATSAPP_LINK:
        promo_row.append(InlineKeyboardButton("📱 واتساب", url=WHATSAPP_LINK))
    if CHANNEL_LINK:
        promo_row.append(InlineKeyboardButton("📢 قناة الوظائف", url=CHANNEL_LINK))
    if promo_row:
        rows.append(promo_row)
    if BOT_LINK:
        rows.append([InlineKeyboardButton("🤖 شارك البوت مع أصدقائك", url=BOT_LINK)])
    return rows

def format_job_message(job, country_name: str) -> tuple[str, str]:
    title = escape_html(str(job.get("title", "غير محدد")))
    company = escape_html(str(job.get("company", "غير محدد")))
    if company in ("nan", "None", ""):
        company = "غير محدد"

    location_val = str(job.get("location", ""))
    location_display = country_name
    if location_val and location_val not in ("nan", "", "None"):
        city = location_val.split(",")[0].strip()
        location_display = f"{city}، {country_name}"

    description = str(job.get("description", ""))
    if description and description not in ("nan", "", "None"):
        description = re.sub(r"<[^>]+>", "", description)
        description = re.sub(r"\s+", " ", description).strip()
        description = description[:450] + "..." if len(description) > 450 else description
        description = escape_html(description)
    else:
        description = "لا يوجد وصف متاح حالياً"

    job_url = str(job.get("job_url", ""))
    if job_url in ("nan", "", "None"):
        job_url = ""

    emails_val = job.get("emails", "")
    email = ""
    if emails_val and str(emails_val) not in ("nan", "", "None", "[]"):
        if isinstance(emails_val, list):
            email = emails_val[0]
        else:
            found = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", str(emails_val))
            email = found[0] if found else ""
    if not email:
        email = extract_email_from_text(str(job.get("description", "")))

    site = str(job.get("site", ""))
    source_names = {"indeed": "Indeed", "linkedin": "LinkedIn", "google": "Google Jobs"}
    source_name = source_names.get(site, site)

    # --- Build job message ---
    msg = "━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"💼 <b>{title} - {location_display}</b>\n"
    msg += f"🏢 {company}\n"
    if source_name:
        msg += f"🌐 المصدر: {escape_html(source_name)}\n"
    msg += f"\n{description}\n"
    if email:
        msg += f"\n📧 <b>التواصل:</b> {escape_html(email)}\n"
    if job_url:
        msg += f"\n🔗 <a href='{job_url}'>رابط التقديم على الوظيفة</a>\n"
    # Promotion links in each job post
    if CHANNEL_LINK:
        msg += f"\n📢 <a href='{CHANNEL_LINK}'>انضم لقناة الوظائف</a>"
    if WHATSAPP_LINK:
        msg += f"\n👉 <a href='{WHATSAPP_LINK}'>تابعنا على واتساب</a>"
    if BOT_LINK:
        msg += f"\n🤖 <a href='{BOT_LINK}'>شارك البوت مع أصدقائك</a>"
    msg += "\n━━━━━━━━━━━━━━━━━━━━━"

    # --- Build share text ---
    share_text = f"💼 {title} - {location_display}\n"
    if company != "غير محدد":
        share_text += f"🏢 {company}\n"
    if job_url:
        share_text += f"🔗 التقديم: {job_url}\n"
    if email:
        share_text += f"📧 التواصل: {email}\n"
    if CHANNEL_LINK:
        share_text += f"\n📢 قناة الوظائف: {CHANNEL_LINK}"
    if WHATSAPP_LINK:
        share_text += f"\n📱 واتساب: {WHATSAPP_LINK}"
    if BOT_LINK:
        share_text += f"\n🤖 جرب البوت: {BOT_LINK}"
    whatsapp_url = f"https://api.whatsapp.com/send?text={urllib.parse.quote(share_text)}"

    return msg, whatsapp_url


# ========================
# Search Logic (with caching + concurrency)
# ========================

def _search_single_country(search_term: str, cc: str) -> list:
    """Scrape jobs for a single country (runs in thread pool)."""
    try:
        jobs = scrape_jobs(
            site_name=["indeed", "linkedin"],
            search_term=search_term,
            location=COUNTRIES[cc]["location"],
            country_indeed=COUNTRIES[cc]["indeed_country"],
            results_wanted=MAX_RESULTS,
            hours_old=HOURS_OLD,
            verbose=0,
        )
        if jobs is not None and not jobs.empty:
            results = []
            for _, row in jobs.iterrows():
                job_dict = row.to_dict()
                job_dict["_country_name"] = COUNTRIES[cc]["name"]
                results.append(job_dict)
            return results
    except Exception as e:
        logger.error("Error in %s: %s", cc, e)
    return []


async def search_jobs_logic(search_term: str, country_code: str) -> list:
    """Search with caching and concurrent country scraping."""
    cache_key = f"{search_term.lower().strip()}:{country_code}"

    # Check cache first
    if cache_key in job_cache:
        logger.info("Cache hit for: %s", cache_key)
        return job_cache[cache_key]

    logger.info("Cache miss for: %s, starting search...", cache_key)
    loop = asyncio.get_event_loop()

    if country_code == "all":
        # Concurrent search across all countries
        tasks = [
            loop.run_in_executor(executor, _search_single_country, search_term, cc)
            for cc in COUNTRIES.keys()
        ]
        try:
            results_lists = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=SEARCH_TIMEOUT,
            )
        except asyncio.TimeoutError:
            logger.warning("Search timed out for: %s", search_term)
            results_lists = []

        all_jobs = []
        for result in results_lists:
            if isinstance(result, list):
                all_jobs.extend(result)
            elif isinstance(result, Exception):
                logger.error("Search error: %s", result)
    else:
        # Single country search
        try:
            all_jobs = await asyncio.wait_for(
                loop.run_in_executor(executor, _search_single_country, search_term, country_code),
                timeout=SEARCH_TIMEOUT,
            )
        except asyncio.TimeoutError:
            logger.warning("Search timed out for: %s in %s", search_term, country_code)
            all_jobs = []

    # Store in cache
    job_cache[cache_key] = all_jobs
    return all_jobs


# ========================
# Bot Handlers
# ========================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("🔍 بحث عن وظيفة", callback_data="search")],
        [InlineKeyboardButton("📂 بحث حسب التصنيف", callback_data="categories")],
    ]
    keyboard.extend(_build_promo_keyboard_rows())

    await update.message.reply_text(
        "👋 أهلاً بك في بوت <b>LinkedIt By Abdulrahman</b>\n\n"
        "أنا أساعدك في العثور على أحدث الوظائف في دول الخليج (قطر، الإمارات، السعودية، البحرين).\n\n"
        "اختر من القائمة أدناه للبدء:",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(keyboard),
    )

async def search_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [
            InlineKeyboardButton("🇶🇦 قطر", callback_data="country_qa"),
            InlineKeyboardButton("🇦🇪 الإمارات", callback_data="country_ae"),
        ],
        [
            InlineKeyboardButton("🇸🇦 السعودية", callback_data="country_sa"),
            InlineKeyboardButton("🇧🇭 البحرين", callback_data="country_bh"),
        ],
        [InlineKeyboardButton("🌍 جميع الدول", callback_data="country_all")],
    ]
    await update.message.reply_text(
        "🔍 <b>اختر الدولة للبحث عن وظائف:</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(keyboard),
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = (
        "📖 <b>دليل استخدام بوت LinkedIt:</b>\n\n"
        "1️⃣ اضغط على /start للبدء.\n"
        "2️⃣ اختر <b>بحث عن وظيفة</b> ثم اختر الدولة.\n"
        "3️⃣ اكتب المسمى الوظيفي (مثلاً: Accountant أو مهندس).\n"
        "4️⃣ سيقوم البوت بالبحث في Indeed و LinkedIn.\n\n"
        "💡 <i>نصيحة: البحث بالإنجليزية يعطي نتائج أكثر وأدق.</i>\n"
    )
    if CHANNEL_LINK:
        help_text += f"\n📢 <a href='{CHANNEL_LINK}'>انضم لقناة الوظائف</a>"
    if BOT_LINK:
        help_text += f"\n🤖 <a href='{BOT_LINK}'>شارك البوت مع أصدقائك</a>"
    await update.message.reply_text(help_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return

    try:
        await query.answer()
    except BadRequest:
        return

    data = query.data

    if data == "noop":
        return

    if data == "search":
        keyboard = [
            [
                InlineKeyboardButton("🇶🇦 قطر", callback_data="country_qa"),
                InlineKeyboardButton("🇦🇪 الإمارات", callback_data="country_ae"),
            ],
            [
                InlineKeyboardButton("🇸🇦 السعودية", callback_data="country_sa"),
                InlineKeyboardButton("🇧🇭 البحرين", callback_data="country_bh"),
            ],
            [InlineKeyboardButton("🌍 جميع الدول", callback_data="country_all")],
            [InlineKeyboardButton("🏠 القائمة الرئيسية", callback_data="back_main")],
        ]
        await query.edit_message_text(
            "🔍 <b>اختر الدولة للبحث:</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard),
        )

    elif data == "categories":
        keyboard = [[InlineKeyboardButton(c["name"], callback_data=f"cat_{k}")] for k, c in JOB_CATEGORIES.items()]
        keyboard.append([InlineKeyboardButton("🏠 القائمة الرئيسية", callback_data="back_main")])
        await query.edit_message_text(
            "📂 <b>اختر تصنيف الوظائف:</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard),
        )

    elif data.startswith("country_"):
        country_code = data.replace("country_", "")
        context.user_data["country"] = country_code
        await query.edit_message_text(
            "✍️ <b>أرسل الآن المسمى الوظيفي الذي تبحث عنه:</b>\n(مثال: مهندس، محاسبة، Sales، Developer)",
            parse_mode=ParseMode.HTML,
        )

    elif data.startswith("cat_"):
        cat_id = data.replace("cat_", "")
        search_term = JOB_CATEGORIES[cat_id]["query"]
        await perform_search(query, context, search_term, "all", is_callback=True)

    elif data == "back_main":
        keyboard = [
            [InlineKeyboardButton("🔍 بحث عن وظيفة", callback_data="search")],
            [InlineKeyboardButton("📂 بحث حسب التصنيف", callback_data="categories")],
        ]
        keyboard.extend(_build_promo_keyboard_rows())
        await query.edit_message_text(
            "👋 أهلاً بك في بوت <b>LinkedIt By Abdulrahman</b>\n\nاختر من القائمة أدناه للبدء:",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard),
        )

    # --- Pagination ---
    elif data.startswith("page_"):
        parts = data.split("_")
        search_id = parts[1]
        page = int(parts[2])
        results = context.user_data.get(f"results_{search_id}", [])
        if results:
            await send_page(query.message.chat_id, context, results, page, search_id)


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    search_term = update.message.text
    country_code = context.user_data.get("country", "all")
    await perform_search(update, context, search_term, country_code)


# ========================
# Pagination
# ========================

async def send_page(chat_id, context, results, page, search_id):
    """Send one page of results with navigation buttons."""
    start_idx = page * RESULTS_PER_PAGE
    end_idx = min(start_idx + RESULTS_PER_PAGE, len(results))
    total_pages = (len(results) + RESULTS_PER_PAGE - 1) // RESULTS_PER_PAGE

    page_results = results[start_idx:end_idx]

    for job in page_results:
        c_name = job.get("_country_name", "الخليج")
        text, wa_url = format_job_message(job, c_name)
        markup = InlineKeyboardMarkup([[InlineKeyboardButton("📤 مشاركة عبر واتساب", url=wa_url)]])
        try:
            await context.bot.send_message(
                chat_id,
                text,
                parse_mode=ParseMode.HTML,
                reply_markup=markup,
                disable_web_page_preview=True,
            )
            await asyncio.sleep(0.3)
        except Exception as e:
            logger.error("Error sending job message: %s", e)

    # Navigation buttons
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ السابق", callback_data=f"page_{search_id}_{page - 1}"))
    nav_buttons.append(InlineKeyboardButton(f"📄 {page + 1}/{total_pages}", callback_data="noop"))
    if end_idx < len(results):
        nav_buttons.append(InlineKeyboardButton("التالي ➡️", callback_data=f"page_{search_id}_{page + 1}"))

    if total_pages > 1:
        await context.bot.send_message(
            chat_id,
            f"📊 عرض {start_idx + 1}-{end_idx} من {len(results)} وظيفة",
            reply_markup=InlineKeyboardMarkup([nav_buttons]),
        )


async def perform_search(update_or_query, context: ContextTypes.DEFAULT_TYPE, search_term: str, country_code: str, is_callback: bool = False):
    if is_callback:
        await update_or_query.edit_message_text(
            f"🔍 جاري البحث عن <b>{escape_html(search_term)}</b>... يرجى الانتظار.",
            parse_mode=ParseMode.HTML,
        )
        chat_id = update_or_query.message.chat_id
    else:
        await update_or_query.message.reply_text(
            f"🔍 جاري البحث عن <b>{escape_html(search_term)}</b>... يرجى الانتظار.",
            parse_mode=ParseMode.HTML,
        )
        chat_id = update_or_query.message.chat_id

    # Search with caching
    results = await search_jobs_logic(search_term, country_code)

    if not results:
        await context.bot.send_message(
            chat_id,
            f"😔 لم أجد وظائف حالياً لـ <b>{escape_html(search_term)}</b>. حاول مرة أخرى بمسمى مختلف.",
            parse_mode=ParseMode.HTML,
        )
        return

    # Store results for pagination
    search_id = str(abs(hash(f"{search_term}:{country_code}:{datetime.now().timestamp()}")))[-8:]
    context.user_data[f"results_{search_id}"] = results[:MAX_RESULTS]

    await context.bot.send_message(
        chat_id,
        f"✅ تم العثور على <b>{len(results[:MAX_RESULTS])}</b> وظيفة:",
        parse_mode=ParseMode.HTML,
    )

    # Send first page only
    await send_page(chat_id, context, results[:MAX_RESULTS], 0, search_id)


# ========================
# Error Handler
# ========================

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Unhandled exception while handling an update:", exc_info=context.error)


# ========================
# Main
# ========================

def main():
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN is missing. Please set BOT_TOKEN in Render Environment Variables.")
        raise SystemExit(1)

    # Start Flask in a separate thread (health endpoint)
    Thread(target=run_flask, daemon=True).start()

    application = Application.builder().token(BOT_TOKEN).build()

    # Handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("search", search_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CallbackQueryHandler(handle_callback))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    # Error handler
    application.add_error_handler(error_handler)

    logger.info("Bot started (improved version)...")

    # drop_pending_updates to avoid old callbacks after restart
    application.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
