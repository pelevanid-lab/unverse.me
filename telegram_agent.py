import os
import re
import asyncio
import logging
import orjson
import redis.asyncio as redis
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, MenuButtonWebApp, WebAppInfo
from telegram.ext import (
    Application, CallbackQueryHandler, CommandHandler, ContextTypes,
    MessageHandler, filters,
)
from supabase import create_client, Client

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("TelegramAgent")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
VERCEL_URL = os.getenv("VERCEL_URL", "")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

async def dashboard_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.message.chat_id) != TELEGRAM_CHAT_ID:
        return

    keyboard = [
        [InlineKeyboardButton("📊 Dashboard'u Aç", web_app=WebAppInfo(url=VERCEL_URL))]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Sistem hazır! Güvenli giriş yapmak için aşağıdaki butona tıklayın:", reply_markup=reply_markup)

async def scan_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manually wake htf_agent's 4h cycle right now instead of waiting for
    the timer — same routine, just triggered on demand."""
    if str(update.message.chat_id) != TELEGRAM_CHAT_ID:
        return
    await redis_client.publish("htf:manual_scan", orjson.dumps({}))
    await update.message.reply_text("🔄 Manuel tarama tetiklendi. Sonuçlar birazdan gelecek.")

# A short bare word (optionally $-prefixed, optionally with a USDT suffix) is
# treated as "analyze this symbol". Single-user personal bot, so a casual
# short message occasionally false-triggering an analysis reply is an
# acceptable tradeoff — not worth a stricter grammar.
SYMBOL_PATTERN = re.compile(r"^\$?[A-Za-z]{2,10}(USDT|usdt)?$")

async def symbol_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Plain-text "BTC" / "$ETH" style messages -> on-demand htf_agent analysis."""
    if str(update.message.chat_id) != TELEGRAM_CHAT_ID:
        return
    text = (update.message.text or "").strip()
    if not SYMBOL_PATTERN.match(text):
        return
    symbol = text.lstrip("$").upper()
    await redis_client.publish("htf:analyze_request", orjson.dumps({"symbol": symbol}))
    await update.message.reply_text(f"🔍 {symbol} analiz ediliyor...")

supabase = None
if SUPABASE_URL and SUPABASE_KEY:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
redis_client = None

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    # Check authorization
    if str(query.message.chat_id) != TELEGRAM_CHAT_ID:
        await query.edit_message_text(text="Unauthorized.")
        return

    data = query.data
    action, signal_id = data.split(":")
    
    try:
        if action == "approve":
            supabase.table("pending_signals").update({"status": "APPROVED"}).eq("id", signal_id).execute()
            await query.edit_message_text(text=f"{query.message.text}\n\n✅ ONAYLANDI! İşlem Binance'e iletildi.")
        elif action == "reject":
            supabase.table("pending_signals").update({"status": "REJECTED"}).eq("id", signal_id).execute()
            await query.edit_message_text(text=f"{query.message.text}\n\n❌ REDDEDİLDİ.")
    except Exception as e:
        logger.error(f"Error handling callback: {e}")
        await query.edit_message_text(text=f"{query.message.text}\n\n⚠️ Bir hata oluştu: {e}")

async def listen_redis_for_signals(application: Application):
    pubsub = redis_client.pubsub()
    await pubsub.subscribe("telegram:notify")
    logger.info("Listening for new signals on Redis...")
    
    while True:
        try:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if message and message['type'] == 'message':
                data = orjson.loads(message['data'])
                signal_id = data['signal_id']
                symbol = data['symbol']
                action = data['action']
                confidence = data['confidence']
                reasoning = data['reasoning']
                
                text = f"🚨 *YENİ FIRSAT: {action} {symbol}*\n🎯 Güven Skoru: %{int(confidence*100)}"
                entry_price = data.get('entry_price')
                sl_price = data.get('sl_price')
                risk_pct = data.get('risk_pct')
                if entry_price:
                    text += f"\n💵 Giriş: ~{entry_price:.6g}"
                if sl_price:
                    text += f"\n🛑 Stop: {sl_price:.6g}"
                    if risk_pct:
                        text += f" (%{risk_pct:.1f} risk)"
                    text += "\n🏁 Hedef: sabit TP yok — trend bitene kadar iz süren stop"
                text += f"\n🧠 Sebep: {reasoning}"
                keyboard = [
                    [
                        InlineKeyboardButton("✅ Onayla", callback_data=f"approve:{signal_id}"),
                        InlineKeyboardButton("❌ Reddet", callback_data=f"reject:{signal_id}"),
                    ]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                await application.bot.send_message(
                    chat_id=TELEGRAM_CHAT_ID,
                    text=text,
                    reply_markup=reply_markup,
                    parse_mode='Markdown'
                )
        except Exception as e:
            logger.error(f"Error in redis listener: {e}")
        await asyncio.sleep(0.5)

async def listen_redis_for_analysis(application: Application):
    """On-demand symbol analysis reports from htf_agent — plain text, no
    approve/reject buttons (informational; dispatch_signal handles the
    approval flow separately if the result turns out to be actionable)."""
    pubsub = redis_client.pubsub()
    await pubsub.subscribe("telegram:analysis")
    logger.info("Listening for on-demand analysis reports on Redis...")

    while True:
        try:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if message and message['type'] == 'message':
                data = orjson.loads(message['data'])
                await application.bot.send_message(
                    chat_id=TELEGRAM_CHAT_ID,
                    text=data['report'],
                )
        except Exception as e:
            logger.error(f"Error in analysis listener: {e}")
        await asyncio.sleep(0.5)

async def main():
    global redis_client
    redis_client = redis.from_url(REDIS_URL, decode_responses=False)
    
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.error("TELEGRAM_TOKEN or TELEGRAM_CHAT_ID is missing. Telegram agent won't start properly.")
        return

    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("dashboard", dashboard_command))
    app.add_handler(CommandHandler("tara", scan_command))
    app.add_handler(CallbackQueryHandler(button_callback))
    # Plain-text "BTC" / "$ETH" messages -> on-demand analysis. Registered
    # AFTER the command handlers so /commands are never swallowed by it.
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, symbol_text_handler))

    await app.initialize()
    
    if VERCEL_URL:
        try:
            await app.bot.set_chat_menu_button(
                chat_id=TELEGRAM_CHAT_ID,
                menu_button=MenuButtonWebApp(text="📊 Dashboard", web_app=WebAppInfo(url=VERCEL_URL))
            )
            logger.info("Set Telegram Web App Menu Button successfully.")
        except Exception as e:
            logger.error(f"Failed to set Menu Button: {e}")
            
    await app.start()
    await app.updater.start_polling()
    
    logger.info("Telegram Bot started! Polling and listening to Redis...")

    # Run both redis listeners concurrently.
    await asyncio.gather(
        listen_redis_for_signals(app),
        listen_redis_for_analysis(app),
    )
    
    await app.updater.stop()
    await app.stop()
    await app.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
