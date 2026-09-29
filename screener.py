import asyncio
import logging
import ssl
import aiohttp
import pandas as pd
import numpy as np
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes
from telegram.request import HTTPXRequest

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

TELEGRAM_BOT_TOKEN = "8925614554:AAHlbwqZoKK2rKzhjbOJ1iIzG6LuXKkzCIA"
BASE_URL = "https://fapi.binance.com"


async def fetch(session, url, params=None):
    try:
        async with session.get(url, params=params, timeout=15) as response:
            if response.status == 200:
                return await response.json()
    except Exception as e:
        logging.error(f"Ошибка запроса {url}: {e}")
    return None


async def get_top_usdt_pairs(session, min_volume_usdt=20_000_000):
    data = await fetch(session, f"{BASE_URL}/fapi/v1/ticker/24hr")
    if not data:
        return []
    
    pairs = []
    for item in data:
        symbol = item['symbol']
        quote_vol = float(item['quoteVolume'])
        if symbol.endswith("USDT") and quote_vol >= min_volume_usdt:
            pairs.append({
                'symbol': symbol,
                'priceChangePercent': float(item['priceChangePercent']),
                'quoteVolume': quote_vol,
                'lastPrice': float(item['lastPrice'])
            })
    return pairs


def calculate_indicators(df):
    df['ema20'] = df['close'].ewm(span=20, adjust=False).mean()
    df['ema50'] = df['close'].ewm(span=50, adjust=False).mean()
    df['ema200'] = df['close'].ewm(span=200, adjust=False).mean()

    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['rsi'] = 100 - (100 / (1 + rs))

    ema12 = df['close'].ewm(span=12, adjust=False).mean()
    ema26 = df['close'].ewm(span=26, adjust=False).mean()
    df['macd'] = ema12 - ema26
    df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()

    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift())
    low_close = np.abs(df['low'] - df['close'].shift())
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = np.max(ranges, axis=1)
    df['atr'] = true_range.rolling(14).mean()

    return df


async def analyze_symbol(session, symbol_data):
    symbol = symbol_data['symbol']
    
    klines_task = fetch(session, f"{BASE_URL}/fapi/v1/klines", {"symbol": symbol, "interval": "15m", "limit": 100})
    funding_task = fetch(session, f"{BASE_URL}/fapi/v1/premiumIndex", {"symbol": symbol})
    
    klines, funding = await asyncio.gather(klines_task, funding_task)
    
    if not klines or len(klines) < 50:
        return None

    df = pd.DataFrame(klines, columns=['time', 'open', 'high', 'low', 'close', 'volume', '_1', '_2', '_3', '_4', '_5', '_6'])
    df['close'] = df['close'].astype(float)
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)

    df = calculate_indicators(df)

    last = df.iloc[-1]
    prev = df.iloc[-2]
    
    last_price = last['close']
    funding_rate = float(funding['lastFundingRate']) if funding else 0.0
    atr = last['atr'] if not np.isnan(last['atr']) else last_price * 0.01

    long_score = 0
    short_score = 0

    if last_price > last['ema20'] > last['ema50'] > last['ema200']:
        long_score += 25
    elif last_price < last['ema20'] < last['ema50'] < last['ema200']:
        short_score += 25

    rsi_val = last['rsi'] if not np.isnan(last['rsi']) else 50
    if 40 <= rsi_val <= 55 and prev['rsi'] < rsi_val:
        long_score += 20
    elif 45 <= rsi_val <= 60 and prev['rsi'] > rsi_val:
        short_score += 20

    if prev['macd'] < prev['macd_signal'] and last['macd'] > last['macd_signal']:
        long_score += 20
    elif prev['macd'] > prev['macd_signal'] and last['macd'] < last['macd_signal']:
        short_score += 20

    if funding_rate < -0.0005:
        long_score += 20
    elif funding_rate > 0.0008:
        short_score += 20

    if long_score > short_score:
        direction = "LONG"
        score = long_score
        entry = last_price
        sl = entry - (1.5 * atr)
        tp1 = entry + (1.5 * atr)
        tp2 = entry + (3.0 * atr)
    else:
        direction = "SHORT"
        score = short_score
        entry = last_price
        sl = entry + (1.5 * atr)
        tp1 = entry - (1.5 * atr)
        tp2 = entry - (3.0 * atr)

    risk_reward = round(abs(tp2 - entry) / abs(entry - sl), 2) if abs(entry - sl) > 0 else 1.0

    return {
        'symbol': symbol,
        'direction': direction,
        'score': score,
        'entry': entry,
        'sl': sl,
        'tp1': tp1,
        'tp2': tp2,
        'rr': f"1:{risk_reward}",
        'funding': f"{funding_rate * 100:.4f}%",
        'rsi': round(rsi_val, 1)
    }


async def run_scan():
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE

    connector = aiohttp.TCPConnector(ssl=ssl_ctx)

    async with aiohttp.ClientSession(connector=connector) as session:
        pairs = await get_top_usdt_pairs(session)
        tasks = [analyze_symbol(session, p) for p in pairs]
        results = await asyncio.gather(*tasks)
        
        valid_results = [r for r in results if r is not None]

        longs = sorted([r for r in valid_results if r['direction'] == "LONG"], key=lambda x: x['score'], reverse=True)[:5]
        shorts = sorted([r for r in valid_results if r['direction'] == "SHORT"], key=lambda x: x['score'], reverse=True)[:5]

        output = ["📈 **ТОП-5 LONG (15m/1H):**\n"]
        for idx, item in enumerate(longs, 1):
            output.append(
                f"{idx}. **{item['symbol']}**\n"
                f"   - Вход: `${item['entry']:.4f}` | SL: `${item['sl']:.4f}`\n"
                f"   - TP1: `${item['tp1']:.4f}` | TP2: `${item['tp2']:.4f}` | R:R: `{item['rr']}`\n"
                f"   - RSI: `{item['rsi']}` | Funding: `{item['funding']}`\n"
            )

        output.append("\n📉 **ТОП-5 SHORT (15m/1H):**\n")
        for idx, item in enumerate(shorts, 1):
            output.append(
                f"{idx}. **{item['symbol']}**\n"
                f"   - Вход: `${item['entry']:.4f}` | SL: `${item['sl']:.4f}`\n"
                f"   - TP1: `${item['tp1']:.4f}` | TP2: `${item['tp2']:.4f}` | R:R: `{item['rr']}`\n"
                f"   - RSI: `{item['rsi']}` | Funding: `{item['funding']}`\n"
            )

        return "\n".join(output)


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logging.info(f"Команда /start от {update.effective_user.first_name}")
    keyboard = [["🔄 Сканировать рынок"]]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text("👋 Бот запущен! Нажми кнопку ниже для анализа Binance Futures.", reply_markup=reply_markup)


async def scan_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logging.info(f"Сканирование от {update.effective_user.first_name}")
    status_msg = await update.message.reply_text("⏳ Сканирую фьючерсы Binance...")
    try:
        report = await run_scan()
        await status_msg.edit_text(report, parse_mode="Markdown")
        logging.info("Отчет успешно отправлен!")
    except Exception as e:
        logging.error(f"Ошибка анализа: {e}")
        await status_msg.edit_text(f"❌ Ошибка: {e}")


def main():
    print("🚀 Подключение к Telegram API...")
    
    # Настраиваем расширенные таймауты
    request = HTTPXRequest(
        connect_timeout=30.0,
        read_timeout=30.0,
        write_timeout=30.0,
        pool_timeout=30.0
    )
    
    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).request(request).build()

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("scan", scan_command))
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), scan_command))

    print("🤖 Бот запущен! Напиши ему /start в Telegram.")
    app.run_polling(bootstrap_retries=-1)


if __name__ == "__main__":
    main()
