import logging
import html
import urllib.parse
import os
import warnings

from jobspy import scrape_jobs
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.constants import ParseMode

warnings.filterwarnings("ignore", category=FutureWarning)

BOT_TOKEN = os.environ.get("BOT_TOKEN")
WHATSAPP_LINK = os.environ.get("WHATSAPP_LINK", "https://whatsapp.com/channel/0029Vat1TW960eBmmdCzvA0r")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing. Set it in Render Environment Variables.")

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

def escape_html(text: str) -> str:
    if not text:
        return ""
    return html.escape(str(text))

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("🔍 بحث عن وظيفة", callback_data="search")],
        [InlineKeyboardButton("📂 بحث حسب التصنيف", callback_data="categories")],
        [InlineKeyboardButton("📱 تابعنا على واتساب", url=WHATSAPP_LINK)],
    ]
    await update.message.reply_text(
        "👋 أهلاً بك في بوت <b>LinkedIt By Abdulrahman</b>\n\n"
        "أنا أساعدك في العثور على أحدث الوظائف في دول الخليج.\n\n"
        "اختر من القائمة أدناه للبدء:",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(keyboard),
    )

def main():
    application = Application.builder().token(BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", start))

    logger.info("Bot started...")
    application.run_polling()

if __name__ == "__main__":
    main()
