import asyncio
import logging
import os
import time
import aiohttp
from aiohttp import web
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

if not TELEGRAM_BOT_TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN не найден в переменных окружения!")

bot = Bot(token=TELEGRAM_BOT_TOKEN) if TELEGRAM_BOT_TOKEN else None
dp = Dispatcher()

BINANCE_BASE_URL = "https://fapi.binance.com"

SENT_SIGNALS_CACHE = {}
CACHE_TTL = 2 * 3600  # 2 часа задержка перед повторным сигналом по той же монете


# ==========================================
# 1. HEALTH CHECK СЕРВЕР ДЛЯ RENDER
# ==========================================
async def handle_health(request):
    return web.Response(text="OK", status=200)

async def start_health_check_server():
    app = web.Application()
    app.router.add_get("/", handle_health)
    app.router.add_get("/health", handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"Health check HTTP server успешно запущен на порту {port}")


# ==========================================
# 2. РАСЧЕТ ИНДИКАТОРОВ
# ==========================================
def calculate_ema(prices, period):
    if len(prices) < period:
        return prices[-1] if prices else 0
    k = 2 / (period + 1)
    ema = prices[0]
    for price in prices[1:]:
        ema = price * k + ema * (1 - k)
    return ema

def calculate_rsi(prices, period=14):
    if len(prices) < period + 1:
        return 50
    gains, losses = [], []
    for i in range(1, len(prices)):
        change = prices[i] - prices[i - 1]
        if change >= 0:
            gains.append(change)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(abs(change))
    
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    
    if avg_loss == 0:
        return 100
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def calculate_atr(klines, period=14):
    if len(klines) < period + 1:
        return 0
    trs = []
    for i in range(1, len(klines)):
        high = float(klines[i][2])
        low = float(klines[i][3])
        prev_close = float(klines[i - 1][4])
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
    return sum(trs[-period:]) / period if trs else 0


# ==========================================
# 3. ПОЛУЧЕНИЕ ДАННЫХ И АНАЛИЗ
# ==========================================
async def fetch_klines(session, symbol, interval, limit=100):
    url = f"{BINANCE_BASE_URL}/fapi/v1/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    try:
        async with session.get(url, params=params, timeout=10) as resp:
            if resp.status == 200:
                return await resp.json()
    except Exception as e:
        logger.error(f"Ошибка получения klines для {symbol} ({interval}): {e}")
    return None

async def get_futures_symbols(session):
    url = f"{BINANCE_BASE_URL}/fapi/v1/ticker/24hr"
    try:
        async with session.get(url, timeout=10) as resp:
            if resp.status == 200:
                tickers = await resp.json()
                symbols = [
                    t["symbol"] for t in tickers 
                    if t["symbol"].endswith("USDT") and float(t.get("quoteVolume", 0)) >= 15_000_000
                ]
                return symbols
    except Exception as e:
        logger.error(f"Ошибка получения тикеров: {e}")
    return []

async def analyze_symbol(session, symbol, btc_trend):
    klines_1h = await fetch_klines(session, symbol, "1h", 210)
    klines_15m = await fetch_klines(session, symbol, "15m", 60)
    klines_5m = await fetch_klines(session, symbol, "5m", 30)

    if not klines_1h or not klines_15m or not klines_5m:
        return None

    closes_1h = [float(k[4]) for k in klines_1h[:-1]]
    closes_15m = [float(k[4]) for k in klines_15m[:-1]]
    closes_5m = [float(k[4]) for k in klines_5m[:-1]]
    vols_15m = [float(k[5]) for k in klines_15m[:-1]]

    current_price = float(klines_5m[-1][4])

    ema200_1h = calculate_ema(closes_1h, 200)
    ema20_15m = calculate_ema(closes_15m, 20)
    ema50_15m = calculate_ema(closes_15m, 50)
    ema20_5m = calculate_ema(closes_5m, 20)

    rsi_15m = calculate_rsi(closes_15m, 14)
    atr_15m = calculate_atr(klines_15m, 14)

    avg_vol_15m = sum(vols_15m[-20:]) / 20 if len(vols_15m) >= 20 else 1
    current_vol_15m = vols_15m[-1] if vols_15m else 0
    vol_ratio = current_vol_15m / avg_vol_15m if avg_vol_15m > 0 else 1.0

    score = 50
    direction = None

    if btc_trend == "LONG":
        if closes_1h[-1] > ema200_1h:
            score += 15
        else:
            return None

        if ema20_15m > ema50_15m:
            score += 10
        dist_to_ema20 = abs(current_price - ema20_15m) / current_price
        if dist_to_ema20 <= 0.008:
            score += 15

        if closes_5m[-1] > ema20_5m:
            score += 10

        if 40 <= rsi_15m <= 65:
            score += 10

        if vol_ratio >= 1.3:
            score += 10

        if score >= 70:
            direction = "LONG"
            stop_loss = current_price - (1.5 * atr_15m)
            take_profit = current_price + (3.0 * atr_15m)

    elif btc_trend == "SHORT":
        if closes_1h[-1] < ema200_1h:
            score += 15
        else:
            return None

        if ema20_15m < ema50_15m:
            score += 10
        dist_to_ema20 = abs(current_price - ema20_15m) / current_price
        if dist_to_ema20 <= 0.008:
            score += 15

        if closes_5m[-1] < ema20_5m:
            score += 10

        if 35 <= rsi_15m <= 60:
            score += 10

        if vol_ratio >= 1.3:
            score += 10

        if score >= 70:
            direction = "SHORT"
            stop_loss = current_price + (1.5 * atr_15m)
            take_profit = current_price - (3.0 * atr_15m)

    if direction:
        return {
            "symbol": symbol,
            "direction": direction,
            "score": score,
            "price": current_price,
            "rsi": round(rsi_15m, 1),
            "vol_ratio": round(vol_ratio, 2),
            "stop_loss": round(stop_loss, 4),
            "take_profit": round(take_profit, 4)
        }

    return None

async def run_market_scan():
    async with aiohttp.ClientSession() as session:
        btc_klines = await fetch_klines(session, "BTCUSDT", "1h", 210)
        if not btc_klines:
            logger.error("Не удалось получить Klines для BTCUSDT")
            return [], []

        btc_closes = [float(k[4]) for k in btc_klines[:-1]]
        btc_ema200 = calculate_ema(btc_closes, 200)
        btc_trend = "LONG" if btc_closes[-1] > btc_ema200 else "SHORT"

        symbols = await get_futures_symbols(session)
        if not symbols:
            return [], []

        tasks = [analyze_symbol(session, sym, btc_trend) for sym in symbols]
        results = await asyncio.gather(*tasks)

        longs = [r for r in results if r and r["direction"] == "LONG"]
        shorts = [r for r in results if r and r["direction"] == "SHORT"]

        longs.sort(key=lambda x: x["score"], reverse=True)
        shorts.sort(key=lambda x: x["score"], reverse=True)

        return longs, shorts


# ==========================================
# 4. ФОРМАТИРОВАНИЕ СООБЩЕНИЙ И ОБРАБОТЧИКИ
# ==========================================
def format_signal_message(longs, shorts):
    if not longs and not shorts:
        return "❌ Качественных сетапов с Quality Score >= 70 на данный момент не найдено."

    text = "📊 *РЕЗУЛЬТАТЫ СКАНРИРОВАНИЯ (TREND-PULLBACK)*\n\n"

    if longs:
        text += f"🟢 *TOP-{len(longs)} LONG СЕТАПЫ:*\n"
        for s in longs[:5]:
            text += (
                f"• *{s['symbol']}* | Score: *{s['score']}*\n"
                f"  Вход: `{s['price']}` | SL: `{s['stop_loss']}` | TP: `{s['take_profit']}`\n"
                f"  Vol Ratio: `{s['vol_ratio']}x` | RSI: `{s['rsi']}`\n\n"
            )

    if shorts:
        text += f"🔴 *TOP-{len(shorts)} SHORT СЕТАПЫ:*\n"
        for s in shorts[:5]:
            text += (
                f"• *{s['symbol']}* | Score: *{s['score']}*\n"
                f"  Вход: `{s['price']}` | SL: `{s['stop_loss']}` | TP: `{s['take_profit']}`\n"
                f"  Vol Ratio: `{s['vol_ratio']}x` | RSI: `{s['rsi']}`\n\n"
            )

    return text

def get_scan_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔍 Запустить сканер", callback_data="run_scan")]
        ]
    )

async def auto_scan_job():
    """Фоновая задача: авто-сканирование каждые 15 минут."""
    while True:
        try:
            await asyncio.sleep(900)  # 15 минут
            logger.info("⏰ Запуск автоматического 15-минутного сканирования...")
            longs, shorts = await run_market_scan()

            now = time.time()
            for sym, ts in list(SENT_SIGNALS_CACHE.items()):
                if now - ts > CACHE_TTL:
                    del SENT_SIGNALS_CACHE[sym]

            new_longs = [s for s in longs if s["symbol"] not in SENT_SIGNALS_CACHE]
            new_shorts = [s for s in shorts if s["symbol"] not in SENT_SIGNALS_CACHE]

            if (new_longs or new_shorts) and CHAT_ID:
                msg_text = "🚨 *АВТО-СИГНАЛ СКРИНЕРА (15M)* 🚨\n\n" + format_signal_message(new_longs, new_shorts)
                await bot.send_message(chat_id=CHAT_ID, text=msg_text, parse_mode="Markdown", reply_markup=get_scan_keyboard())
                
                for s in new_longs + new_shorts:
                    SENT_SIGNALS_CACHE[s["symbol"]] = now

        except Exception as e:
            logger.error(f"Ошибка в авто-сканировании: {e}")

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    global CHAT_ID
    CHAT_ID = message.chat.id
    await message.answer(
        "👋 Привет! Я торговый скринер Binance Futures.\n\n"
        "Я автоматически сканирую рынок каждые 15 минут и присылаю сигналы при появлении качественных сетапов.\n"
        "Вы также можете запустить сканирование вручную:",
        reply_markup=get_scan_keyboard()
    )

@dp.message(lambda m: m.text and ("сканер" in m.text.lower() or "сканирование" in m.text.lower() or m.text.startswith("/scan")))
async def cmd_scan_text(message: types.Message):
    global CHAT_ID
    CHAT_ID = message.chat.id
    await execute_scan_and_send(message.chat.id)

@dp.callback_query(lambda c: c.data == "run_scan")
async def callback_scan(callback_query: CallbackQuery):
    global CHAT_ID
    CHAT_ID = callback_query.message.chat.id
    await callback_query.answer("Запуск сканирования рынка...")
    await execute_scan_and_send(callback_query.message.chat.id)

async def execute_scan_and_send(chat_id: int):
    status_msg = await bot.send_message(chat_id, "⏳ Сканирую рынки Binance USDT-M Futures (1H / 15M / 5M)...")
    longs, shorts = await run_market_scan()
    report = format_signal_message(longs, shorts)

    await bot.edit_message_text(
        report,
        chat_id=chat_id,
        message_id=status_msg.message_id,
        parse_mode="Markdown",
        reply_markup=get_scan_keyboard()
    )


# ==========================================
# 5. ТОЧКА ВХОДА (MAIN)
# ==========================================
async def main():
    if not bot:
        logger.error("Бот не инициализирован. Проверьте TELEGRAM_BOT_TOKEN.")
        return
    
    await start_health_check_server()
    await bot.delete_webhook(drop_pending_updates=True)
    
    asyncio.create_task(auto_scan_job())
    
    logger.info("🤖 Бот запущен с функцией 15-минутного авто-сканирования!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
