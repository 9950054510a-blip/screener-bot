import asyncio
import logging
import os
import sqlite3
import time
from datetime import datetime
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
DB_PATH = "nexus_screener.db"
STRATEGY_VERSION = "TPB_v1.1"

# Заголовки для обхода фильтрации ботов на Binance API
BINANCE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json"
}

SENT_SIGNALS_CACHE = {}
CACHE_TTL = 2 * 3600  # 2 часа задержки перед повторным сигналом

AUTO_SCAN_ENABLED = True
SCAN_LOCK = asyncio.Lock()
BACKGROUND_TASK = None

# Агрегатор воронки за 1 час (12 сканов)
HOURLY_FUNNEL_STATS = []


# ==========================================
# 1. БАЗА ДАННЫХ (SQLITE DATA ENGINE)
# ==========================================
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS scans (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts DATETIME NOT NULL,
        universe_count INTEGER,
        volume_pass INTEGER,
        btc_pass INTEGER,
        trend_pass INTEGER,
        pullback_pass INTEGER,
        rsi_pass INTEGER,
        breakout_pass INTEGER,
        signal_pass INTEGER,
        duration_ms INTEGER
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS setups (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        scan_id INTEGER,
        ts DATETIME NOT NULL,
        strategy_version TEXT,
        symbol TEXT NOT NULL,
        direction TEXT NOT NULL,
        entry_price REAL,
        stop_loss REAL,
        take_profit REAL,
        score REAL,
        rsi_15m REAL,
        vol_ratio REAL,
        taker_buy_ratio REAL,
        fee_rate REAL DEFAULT 0.0005,
        slippage_bps REAL DEFAULT 5.0,
        status TEXT DEFAULT 'CANDIDATE',
        FOREIGN KEY(scan_id) REFERENCES scans(id)
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS outcomes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        setup_id INTEGER NOT NULL,
        evaluated_at DATETIME,
        exit_price REAL,
        exit_reason TEXT,
        net_r REAL,
        fees REAL,
        slippage_cost REAL,
        FOREIGN KEY(setup_id) REFERENCES setups(id)
    );
    """)

    conn.commit()
    conn.close()
    logger.info("🗄️ База данных SQLite успешно инициализирована.")

def save_scan_and_setups(funnel, candidates, duration_ms):
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        now_iso = datetime.utcnow().isoformat()

        cursor.execute("""
        INSERT INTO scans (ts, universe_count, volume_pass, btc_pass, trend_pass, pullback_pass, rsi_pass, breakout_pass, signal_pass, duration_ms)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            now_iso,
            funnel["universe"],
            funnel["volume"],
            funnel["btc"],
            funnel["trend"],
            funnel["pullback"],
            funnel["rsi"],
            funnel["breakout"],
            funnel["signals"],
            duration_ms
        ))
        scan_id = cursor.lastrowid

        for c in candidates:
            status = "SIGNAL" if c["score"] >= 70 else "CANDIDATE"
            cursor.execute("""
            INSERT INTO setups (scan_id, ts, strategy_version, symbol, direction, entry_price, stop_loss, take_profit, score, rsi_15m, vol_ratio, taker_buy_ratio, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                scan_id,
                now_iso,
                STRATEGY_VERSION,
                c["symbol"],
                c["direction"],
                c["price"],
                c["stop_loss"],
                c["take_profit"],
                c["score"],
                c["rsi"],
                c["vol_ratio"],
                c["taker_ratio"],
                status
            ))

        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Ошибка сохранения скана в БД: {e}")


# ==========================================
# 2. HEALTH CHECK СЕРВЕР ДЛЯ RENDER
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
    logger.info(f"Health check HTTP server запущен на порту {port}")


# ==========================================
# 3. РАСЧЕТ ИНДИКАТОРОВ
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
# 4. ПОЛУЧЕНИЕ ДАННЫХ И АНАЛИЗ (С РЕТАЯМИ)
# ==========================================
async def fetch_klines(session, symbol, interval, limit=100):
    url = f"{BINANCE_BASE_URL}/fapi/v1/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    for attempt in range(3):
        try:
            async with session.get(url, params=params, headers=BINANCE_HEADERS, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    return await resp.json()
                elif resp.status == 418:
                    logger.warning(f"Binance 418 IP block для {symbol}. Пауза...")
                    await asyncio.sleep(5)
                else:
                    logger.warning(f"Binance API status {resp.status} for {symbol}")
        except Exception as e:
            if attempt == 2:
                logger.error(f"Ошибка получения klines для {symbol} ({interval}) после 3 попыток: {e}")
            await asyncio.sleep(2)
    return None

async def get_futures_symbols(session):
    url = f"{BINANCE_BASE_URL}/fapi/v1/ticker/24hr"
    for attempt in range(3):
        try:
            async with session.get(url, headers=BINANCE_HEADERS, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    tickers = await resp.json()
                    symbols = [
                        t["symbol"] for t in tickers 
                        if t["symbol"].endswith("USDT") and float(t.get("quoteVolume", 0)) >= 15_000_000
                    ]
                    return symbols
        except Exception as e:
            if attempt == 2:
                logger.error(f"Ошибка получения тикеров после 3 попыток: {e}")
            await asyncio.sleep(2)
    return []

async def analyze_symbol(session, symbol, btc_trend):
    klines_1h = await fetch_klines(session, symbol, "1h", 210)
    klines_15m = await fetch_klines(session, symbol, "15m", 60)
    klines_5m = await fetch_klines(session, symbol, "5m", 30)

    if not klines_1h or not klines_15m or not klines_5m:
        return None, "fetch_error"

    closed_1h = klines_1h[:-1]
    closed_15m = klines_15m[:-1]
    closed_5m = klines_5m[:-1]

    if not closed_1h or not closed_15m or not closed_5m:
        return None, "fetch_error"

    closes_1h = [float(k[4]) for k in closed_1h]
    closes_15m = [float(k[4]) for k in closed_15m]
    closes_5m = [float(k[4]) for k in closed_5m]
    vols_15m = [float(k[5]) for k in closed_15m]

    last_closed_5m = closed_5m[-1]
    current_price = float(last_closed_5m[4])
    vol_5m = float(last_closed_5m[5])
    taker_buy_vol_5m = float(last_closed_5m[9])
    taker_buy_ratio = round(taker_buy_vol_5m / vol_5m, 2) if vol_5m > 0 else 0.50

    ema200_1h = calculate_ema(closes_1h, 200)
    ema20_15m = calculate_ema(closes_15m, 20)
    ema50_15m = calculate_ema(closes_15m, 50)
    ema20_5m = calculate_ema(closes_5m, 20)

    rsi_15m = calculate_rsi(closes_15m, 14)
    atr_15m = calculate_atr(closed_15m, 14)

    avg_vol_15m = sum(vols_15m[-20:]) / 20 if len(vols_15m) >= 20 else 1
    current_vol_15m = vols_15m[-1] if vols_15m else 0
    vol_ratio = current_vol_15m / avg_vol_15m if avg_vol_15m > 0 else 1.0

    score = 50
    direction = None

    if btc_trend == "LONG":
        if closes_1h[-1] <= ema200_1h:
            return None, "trend_fail"
        score += 15

        if ema20_15m <= ema50_15m:
            return None, "pullback_fail"
        score += 10

        dist_to_ema20 = abs(current_price - ema20_15m) / current_price
        if dist_to_ema20 <= 0.008:
            score += 15

        if closes_5m[-1] <= ema20_5m:
            return None, "breakout_fail"
        score += 10

        if not (40 <= rsi_15m <= 65):
            return None, "rsi_fail"
        score += 10

        if vol_ratio >= 1.3:
            score += 10

        direction = "LONG"
        stop_loss = current_price - (1.5 * atr_15m)
        take_profit = current_price + (3.0 * atr_15m)

    elif btc_trend == "SHORT":
        if closes_1h[-1] >= ema200_1h:
            return None, "trend_fail"
        score += 15

        if ema20_15m >= ema50_15m:
            return None, "pullback_fail"
        score += 10

        dist_to_ema20 = abs(current_price - ema20_15m) / current_price
        if dist_to_ema20 <= 0.008:
            score += 15

        if closes_5m[-1] >= ema20_5m:
            return None, "breakout_fail"
        score += 10

        if not (35 <= rsi_15m <= 60):
            return None, "rsi_fail"
        score += 10

        if vol_ratio >= 1.3:
            score += 10

        direction = "SHORT"
        stop_loss = current_price + (1.5 * atr_15m)
        take_profit = current_price - (3.0 * atr_15m)

    if direction:
        res = {
            "symbol": symbol,
            "direction": direction,
            "score": score,
            "price": current_price,
            "rsi": round(rsi_15m, 1),
            "vol_ratio": round(vol_ratio, 2),
            "taker_ratio": taker_buy_ratio,
            "stop_loss": round(stop_loss, 4),
            "take_profit": round(take_profit, 4)
        }
        return res, "pass"

    return None, "unknown"

async def run_market_scan():
    if SCAN_LOCK.locked():
        logger.warning("Предыдущий скан еще не завершился! Пропуск цикла.")
        return [], []

    start_time = time.time()
    funnel = {"universe": 0, "volume": 0, "btc": 0, "trend": 0, "pullback": 0, "rsi": 0, "breakout": 0, "signals": 0}

    async with SCAN_LOCK:
        async with aiohttp.ClientSession() as session:
            btc_klines = await fetch_klines(session, "BTCUSDT", "1h", 210)
            if not btc_klines or len(btc_klines) < 2:
                logger.error("Не удалось получить Klines для BTCUSDT")
                return [], []

            btc_closes = [float(k[4]) for k in btc_klines[:-1]]
            btc_ema200 = calculate_ema(btc_closes, 200)
            btc_trend = "LONG" if btc_closes[-1] > btc_ema200 else "SHORT"

            symbols = await get_futures_symbols(session)
            if not symbols:
                return [], []

            funnel["universe"] = 300
            funnel["volume"] = len(symbols)
            funnel["btc"] = len(symbols)

            tasks = [analyze_symbol(session, sym, btc_trend) for sym in symbols]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            candidates = []
            trend_p, pb_p, rsi_p, bo_p = 0, 0, 0, 0

            for r in results:
                if isinstance(r, tuple):
                    res, status = r
                    if status != "trend_fail":
                        trend_p += 1
                        if status != "pullback_fail":
                            pb_p += 1
                            if status != "rsi_fail":
                                rsi_p += 1
                                if status != "breakout_fail" and res:
                                    bo_p += 1
                                    candidates.append(res)

            funnel["trend"] = trend_p
            funnel["pullback"] = pb_p
            funnel["rsi"] = rsi_p
            funnel["breakout"] = bo_p

            longs = [c for c in candidates if c["direction"] == "LONG" and c["score"] >= 70]
            shorts = [c for c in candidates if c["direction"] == "SHORT" and c["score"] >= 70]

            longs.sort(key=lambda x: x["score"], reverse=True)
            shorts.sort(key=lambda x: x["score"], reverse=True)

            funnel["signals"] = len(longs) + len(shorts)
            duration_ms = int((time.time() - start_time) * 1000)

            save_scan_and_setups(funnel, candidates, duration_ms)

            logger.info(
                f"[SCAN FUNNEL] 300→{funnel['volume']}→{funnel['trend']}→{funnel['pullback']}→{funnel['rsi']}→{funnel['breakout']} | "
                f"SIGNALS: {funnel['signals']} | {duration_ms}ms"
            )

            HOURLY_FUNNEL_STATS.append(funnel)
            if len(HOURLY_FUNNEL_STATS) >= 12:
                log_hourly_funnel_summary()

            return longs, shorts

def log_hourly_funnel_summary():
    global HOURLY_FUNNEL_STATS
    if not HOURLY_FUNNEL_STATS:
        return
    
    tot_vol = sum(f["volume"] for f in HOURLY_FUNNEL_STATS)
    tot_tr = sum(f["trend"] for f in HOURLY_FUNNEL_STATS)
    tot_pb = sum(f["pullback"] for f in HOURLY_FUNNEL_STATS)
    tot_rsi = sum(f["rsi"] for f in HOURLY_FUNNEL_STATS)
    tot_bo = sum(f["breakout"] for f in HOURLY_FUNNEL_STATS)
    tot_sig = sum(f["signals"] for f in HOURLY_FUNNEL_STATS)

    summary_text = (
        f"\n════════════════ FUNNEL 1H SUMMARY (12 Scans) ════════════════\n"
        f"Volume Filter (≥15M) : {tot_vol}\n"
        f"1H Trend Pass        : {tot_tr} ({round(tot_tr/tot_vol*100, 1) if tot_vol else 0}%)\n"
        f"15M Pullback Pass    : {tot_pb} ({round(tot_pb/tot_tr*100, 1) if tot_tr else 0}%)\n"
        f"15M RSI Pass         : {tot_rsi} ({round(tot_rsi/tot_pb*100, 1) if tot_pb else 0}%)\n"
        f"5M Breakout Pass     : {tot_bo} ({round(tot_bo/tot_rsi*100, 1) if tot_rsi else 0}%)\n"
        f"FINAL SIGNALS (≥70)  : {tot_sig}\n"
        f"═════════════════════════════════════════════════════════════"
    )
    logger.info(summary_text)
    HOURLY_FUNNEL_STATS.clear()


# ==========================================
# 5. КНОПКИ И ОБРАБОТЧИКИ ТЕЛЕГРАМ
# ==========================================
def format_signal_message(longs, shorts):
    if not longs and not shorts:
        return "❌ Качественных сетапов с Quality Score >= 70 на данный момент не найдено."

    text = f"📊 *РЕЗУЛЬТАТЫ СКАНРИРОВАНИЯ (NEXUS {STRATEGY_VERSION})*\n\n"

    if longs:
        text += f"🟢 *TOP-{len(longs)} LONG СЕТАПЫ:*\n"
        for s in longs[:5]:
            text += (
                f"• *{s['symbol']}* | Score: *{s['score']}*\n"
                f"  Вход: `{s['price']}` | SL: `{s['stop_loss']}` | TP: `{s['take_profit']}`\n"
                f"  Vol Ratio: `{s['vol_ratio']}x` | Taker Buy: `{int(s['taker_ratio']*100)}%` | RSI: `{s['rsi']}`\n\n"
            )

    if shorts:
        text += f"🔴 *TOP-{len(shorts)} SHORT СЕТАПЫ:*\n"
        for s in shorts[:5]:
            text += (
                f"• *{s['symbol']}* | Score: *{s['score']}*\n"
                f"  Вход: `{s['price']}` | SL: `{s['stop_loss']}` | TP: `{s['take_profit']}`\n"
                f"  Vol Ratio: `{s['vol_ratio']}x` | Taker Buy: `{int(s['taker_ratio']*100)}%` | RSI: `{s['rsi']}`\n\n"
            )

    return text

def get_scan_keyboard():
    toggle_text = "🔴 Выключить авто-сканер" if AUTO_SCAN_ENABLED else "🟢 Включить авто-сканер"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔍 Запустить сканер", callback_data="run_scan")],
            [InlineKeyboardButton(text=toggle_text, callback_data="toggle_auto_scan")]
        ]
    )

async def auto_scan_job():
    while True:
        try:
            now = time.time()
            next_interval = (int(now) // 300 + 1) * 300 + 5
            sleep_seconds = next_interval - now
            
            await asyncio.sleep(sleep_seconds)

            if not AUTO_SCAN_ENABLED:
                continue

            longs, shorts = await run_market_scan()

            current_ts = time.time()
            for sym, ts in list(SENT_SIGNALS_CACHE.items()):
                if current_ts - ts > CACHE_TTL:
                    del SENT_SIGNALS_CACHE[sym]

            new_longs = [s for s in longs if s["symbol"] not in SENT_SIGNALS_CACHE]
            new_shorts = [s for s in shorts if s["symbol"] not in SENT_SIGNALS_CACHE]

            if (new_longs or new_shorts) and CHAT_ID:
                msg_text = "🚨 *АВТО-СИГНАЛ СКРИНЕРА (5M)* 🚨\n\n" + format_signal_message(new_longs, new_shorts)
                await bot.send_message(chat_id=CHAT_ID, text=msg_text, parse_mode="Markdown", reply_markup=get_scan_keyboard())
                
                for s in new_longs + new_shorts:
                    SENT_SIGNALS_CACHE[s["symbol"]] = current_ts

        except Exception as e:
            logger.error(f"Ошибка в авто-сканировании: {e}")
            await asyncio.sleep(10)

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    global CHAT_ID
    CHAT_ID = message.chat.id
    status = "включено 🟢 (5M Strict + DB Data Engine)" if AUTO_SCAN_ENABLED else "выключено 🔴"
    await message.answer(
        f"👋 Привет! Я торговый скринер Binance Futures (NEXUS Engine).\n\n"
        f"Статус авто-сканирования: **{status}**.\n"
        f"Используйте кнопки ниже для управления:",
        parse_mode="Markdown",
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

@dp.callback_query(lambda c: c.data == "toggle_auto_scan")
async def callback_toggle_auto_scan(callback_query: CallbackQuery):
    global AUTO_SCAN_ENABLED, CHAT_ID
    CHAT_ID = callback_query.message.chat.id
    AUTO_SCAN_ENABLED = not AUTO_SCAN_ENABLED
    
    status_text = "🟢 5M Авто-сканирование ВКЛЮЧЕНО" if AUTO_SCAN_ENABLED else "🔴 Авто-сканирование ВЫКЛЮЧЕНО"
    await callback_query.answer(status_text)
    await callback_query.message.edit_reply_markup(reply_markup=get_scan_keyboard())

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
# 6. ТОЧКА ВХОДА (MAIN)
# ==========================================
async def main():
    global BACKGROUND_TASK
    if not bot:
        logger.error("Бот не инициализирован. Проверьте TELEGRAM_BOT_TOKEN.")
        return
    
    init_db()

    await start_health_check_server()
    await bot.delete_webhook(drop_pending_updates=True)
    
    BACKGROUND_TASK = asyncio.create_task(auto_scan_job())
    
    logger.info("🤖 NEXUS Screener v1.1 запущен с БД и воронкой!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
