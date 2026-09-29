import os
import asyncio
import logging
import ssl
import aiohttp
import pandas as pd
import numpy as np
from aiohttp import web
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters
from telegram.request import HTTPXRequest

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Считываем токен из переменных окружения Render
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
BASE_URL = "https://fapi.binance.com"

# --- Health Check Server для Render ---
async def start_health_check_server():
    app = web.Application()
    app.router.add_get('/', lambda r: web.Response(text="OK"))
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", 8080))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logging.info(f"Health check server running on port {port}")

# --- Вспомогательные функции аналитики Binance ---
async def fetch_json(session, url, params=None):
    try:
        async with session.get(url, params=params, timeout=10) as response:
            if response.status == 200:
                return await response.json()
    except Exception as e:
        logging.error(f"Error fetching {url}: {e}")
    return None

async def get_top_futures_symbols(session, limit=50):
    url = f"{BASE_URL}/fapi/v1/ticker/24hr"
    data = await fetch_json(session, url)
    if not data:
        return []
    
    usdt_pairs = [d for d in data if d['symbol'].endswith('USDT')]
    usdt_pairs.sort(key=lambda x: float(x.get('quoteVolume', 0)), reverse=True)
    return [d['symbol'] for d in usdt_pairs[:limit]]

async def get_klines(session, symbol, interval="5m", limit=100):
    url = f"{BASE_URL}/fapi/v1/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    data = await fetch_json(session, url, params=params)
    if not data:
        return None
    
    df = pd.DataFrame(data, columns=[
        'timestamp', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'number_of_trades',
        'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
    ])
    
    for col in ['open', 'high', 'low', 'close', 'volume']:
        df[col] = df[col].astype(float)
        
    return df

def analyze_symbol(df):
    if df is None or len(df) < 30:
        return None
        
    df['rsi'] = calculate_rsi(df['close'], period=14)
    last_row = df.iloc[-1]
    
    rsi_val = last_row['rsi']
    vol = last_row['volume']
    avg_vol = df['volume'].tail(20).mean()
    
    signals = []
    if rsi_val < 30:
        signals.append("Oversold (RSI < 30)")
    elif rsi_val > 70:
        signals.append("Overbought (RSI > 70)")
        
    if vol > avg_vol * 2.5:
        signals.append("Volume Spike (>2.5x avg)")
        
    if signals:
        return {
            "rsi": round(rsi_val, 2),
            "close": last_row['close'],
            "signals": ", ".join(signals)
        }
    return None

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

# --- Telegram Bot Handlers ---
async def start_command(update: Update, context):
    keyboard = [["🔍 Запустить сканер"]]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text(
        "Привет! Я скринер Binance Futures. Нажмите кнопку ниже для анализа рынка.",
        reply_markup=reply_markup
    )

async def handle_message(update: Update, context):
    text = update.message.text
    if text == "🔍 Запустить сканер":
        await update.message.reply_text("⏳ Сканирую ТОП-50 фьючерсов Binance...")
        
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            symbols = await get_top_futures_symbols(session, limit=50)
            results = []
            
            for sym in symbols:
                df = await get_klines(session, sym, interval="5m", limit=100)
                analysis = analyze_symbol(df)
                if analysis:
                    results.append(f"🟢 **{sym}** | Price: {analysis['close']} | RSI: {analysis['rsi']}\nСигналы: {analysis['signals']}")
            
            if results:
                response_text = "\n\n".join(results[:10])
                await update.message.reply_text(response_text, parse_mode="Markdown")
            else:
                await update.message.reply_text("Сигналов не найдено. Рынок спокойный.")

# --- Главная функция запуска ---
async def main():
    # 1. Запуск веб-сервера health check
    await start_health_check_server()
    
    # 2. Инициализация и запуск Telegram бота
    req = HTTPXRequest(connect_timeout=10, read_timeout=10)
    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).request(req).build()
    
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_message))
    
    logging.info("Telegram bot started successfully...")
    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    
    # Удерживаем процесс непрерывно работающим
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
