import asyncio
import logging
import os
import ssl
import aiohttp
import numpy as np
import pandas as pd
from telegram import ReplyKeyboardMarkup, Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from telegram.request import HTTPXRequest

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
BASE_URL = "https://fapi.binance.com"


async def fetch(session, url, params=None):
    try:
        async with session.get(url, params=params, timeout=15) as response:
            if response.status == 200:
                return await response.json()
    except Exception as e:
        logging.error(f"Ошибка запроса {url}: {e}")
    return None


async def get_top_usdt_pairs(session, min_volume_usdt=15_000_000):
    data = await fetch(session, f"{BASE_URL}/fapi/v1/ticker/24hr")
    if not data:
        return []

    pairs = []
    for item in data:
        symbol = item["symbol"]
        quote_vol = float(item["quoteVolume"])
        if symbol.endswith("USDT") and quote_vol >= min_volume_usdt:
            pairs.append({"symbol": symbol, "quoteVolume": quote_vol})
    return pairs


def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def calculate_atr(df, period=14):
    high_low = df["high"] - df["low"]
    high_close = np.abs(df["high"] - df["close"].shift())
    low_close = np.abs(df["low"] - df["close"].shift())
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = np.max(ranges, axis=1)
    return true_range.rolling(period).mean()


async def analyze_symbol(session, symbol_data):
    symbol = symbol_data["symbol"]

    k_1h_task = fetch(session, f"{BASE_URL}/fapi/v1/klines", {"symbol": symbol, "interval": "1h", "limit": 100})
    k_15m_task = fetch(session, f"{BASE_URL}/fapi/v1/klines", {"symbol": symbol, "interval": "15m", "limit": 100})
    k_5m_task = fetch(session, f"{BASE_URL}/fapi/v1/klines", {"symbol": symbol, "interval": "5m", "limit": 100})

    k_1h, k_15m, k_5m = await asyncio.gather(k_1h_task, k_15m_task, k_5m_task)

    if not k_1h or not k_15m or not k_5m or len(k_1h) < 60 or len(k_15m) < 60 or len(k_5m) < 30:
        return None

    df_1h = pd.DataFrame(k_1h, columns=["t", "o", "h", "l", "c", "v", "_1", "_2", "_3", "_4", "_5", "_6"])
    df_1h["close"] = df_1h["c"].astype(float)
    df_1h["high"] = df_1h["h"].astype(float)
    df_1h["low"] = df_1h["l"].astype(float)
    df_1h["ema20"] = df_1h["close"].ewm(span=20, adjust=False).mean()
    df_1h["ema50"] = df_1h["close"].ewm(span=50, adjust=False).mean()
    df_1h["atr14"] = calculate_atr(df_1h, 14)

    df_15m = pd.DataFrame(k_15m, columns=["t", "o", "h", "l", "c", "v", "_1", "_2", "_3", "_4", "_5", "_6"])
    df_15m["close"] = df_15m["c"].astype(float)
    df_15m["high"] = df_15m["h"].astype(float)
    df_15m["low"] = df_15m["l"].astype(float)
    df_15m["ema20"] = df_15m["close"].ewm(span=20, adjust=False).mean()
    df_15m["ema50"] = df_15m["close"].ewm(span=50, adjust=False).mean()
    df_15m["rsi14"] = calculate_rsi(df_15m["close"], 14)
    df_15m["atr14"] = calculate_atr(df_15m, 14)

    df_5m = pd.DataFrame(k_5m, columns=["t", "o", "h", "l", "c", "v", "_1", "_2", "_3", "_4", "_5", "_6"])
    df_5m["close"] = df_5m["c"].astype(float)
    df_5m["high"] = df_5m["h"].astype(float)
    df_5m["low"] = df_5m["l"].astype(float)
    df_5m["volume"] = df_5m["v"].astype(float)
    df_5m["vol_sma20"] = df_5m["volume"].rolling(20).mean()

    last_1h = df_1h.iloc[-2]
    last_15m = df_15m.iloc[-2]
    last_5m = df_5m.iloc[-2]

    vol_ratio = last_5m["volume"] / max(df_5m.iloc[-3]["vol_sma20"], 0.0001)
    s_vol = min(100.0, (vol_ratio / 2.0) * 100.0)

    rsi_15m = last_15m["rsi14"]
    if np.isnan(rsi_15m):
        rsi_15m = 50.0

    s_rsi_long = max(0.0, 100.0 - (abs(rsi_15m - 41.5) / 6.5) * 100.0) if 35.0 <= rsi_15m <= 48.0 else 0.0
    s_rsi_short = max(0.0, 100.0 - (abs(rsi_15m - 58.5) / 6.5) * 100.0) if 52.0 <= rsi_15m <= 65.0 else 0.0

    atr_1h = last_1h["atr14"] if not np.isnan(last_1h["atr14"]) and last_1h["atr14"] > 0 else last_1h["close"] * 0.01
    sep_1h = abs(last_1h["ema20"] - last_1h["ema50"]) / atr_1h
    s_trend = min(100.0, max(0.0, (sep_1h - 0.2) / 1.3) * 100.0)

    atr_15m = last_15m["atr14"] if not np.isnan(last_15m["atr14"]) and last_15m["atr14"] > 0 else last_15m["close"] * 0.01
    high_3_5m = df_5m.iloc[-5:-2]["high"].max()
    low_3_5m = df_5m.iloc[-5:-2]["low"].min()

    dist_long = (last_5m["close"] - high_3_5m) / atr_15m
    dist_short = (low_3_5m - last_5m["close"]) / atr_15m

    s_breakout_long = min(100.0, max(0.0, dist_long / 0.5) * 100.0) if dist_long > 0 else 0.0
    s_breakout_short = min(100.0, max(0.0, dist_short / 0.5) * 100.0) if dist_short > 0 else 0.0

    long_score = (0.35 * s_vol) + (0.25 * s_rsi_long) + (0.25 * s_trend) + (0.15 * s_breakout_long)
    short_score = (0.35 * s_vol) + (0.25 * s_rsi_short) + (0.25 * s_trend) + (0.15 * s_breakout_short)

    long_valid = (35.0 <= rsi_15m <= 48.0) and (last_5m["close"] > high_3_5m) and (last_1h["ema20"] > last_1h["ema50"])
    short_valid = (52.0 <= rsi_15m <= 65.0) and (last_5m["close"] < low_3_5m) and (last_1h["ema20"] < last_1h["ema50"])

    results = []
    entry = last_5m["close"]

    if long_valid:
        sl = entry - (1.5 * atr_15m)
        tp1 = entry + (3.0 * atr_15m)
        tp2 = entry + (4.5 * atr_15m)
        tp3 = entry + (6.0 * atr_15m)
        results.append({
            "symbol": symbol,
            "direction": "LONG",
            "score": round(long_score, 1),
            "entry": entry,
            "sl": sl,
            "tp1": tp1,
            "tp2": tp2,
            "tp3": tp3,
            "rsi": round(rsi_15m, 1),
            "quoteVolume": symbol_data["quoteVolume"]
        })

    if short_valid:
        sl = entry + (1.5 * atr_15m)
        tp1 = entry - (3.0 * atr_15m)
        tp2 = entry - (4.5 * atr_15m)
        tp3 = entry - (6.0 * atr_15m)
        results.append({
            "symbol": symbol,
            "direction": "SHORT",
            "score": round(short_score, 1),
            "entry": entry,
            "sl": sl,
            "tp1": tp1,
            "tp2": tp2,
            "tp3": tp3,
            "rsi": round(rsi_15m, 1),
            "quoteVolume": symbol_data["quoteVolume"]
        })

    return results


async def run_scan():
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE

    connector = aiohttp.TCPConnector(ssl=ssl_ctx)

    async with aiohttp.ClientSession(connector=connector) as session:
        pairs = await get_top_usdt_pairs(session)
        tasks = [analyze_symbol(session, p) for p in pairs]
        nested_results = await asyncio.gather(*tasks)

        all_results = []
        for r in nested_results:
            if r:
                all_results.extend(r)

        longs = sorted(
            [r for r in all_results if r["direction"] == "LONG"],
            key=lambda x: (x["score"], x["quoteVolume"]),
            reverse=True
        )[:10]

        shorts = sorted(
            [r for r in all_results if r["direction"] == "SHORT"],
            key=lambda x: (x["score"], x["quoteVolume"]),
            reverse=True
        )[:10]

        output = []

        output.append(f"📈 **ТОП LONG СЕТАПЫ (Найдено: {len(longs)} из 10):**\n")
        if not longs:
            output.append("Качественных LONG сетапов по жестким фильтрам не найдено.\n")
        else:
            for idx, item in enumerate(longs, 1):
                output.append(
                    f"{idx}. **{item['symbol']}** | Quality Score: `{item['score']}/100`\n"
                    f"   - Вход: `${item['entry']:.4f}` | SL: `${item['sl']:.4f}` (1.5 ATR)\n"
                    f"   - TP1 (50%): `${item['tp1']:.4f}` (2R) -> Стоп в BE+0.1%\n"
                    f"   - TP2 (25%): `${item['tp2']:.4f}` | TP3 (25%): `${item['tp3']:.4f}`\n"
                    f"   - RSI 15m: `{item['rsi']}`\n"
                )

        output.append(f"\n📉 **ТОП SHORT СЕТАПЫ (Найдено: {len(shorts)} из 10):**\n")
        if not shorts:
            output.append("Качественных SHORT сетапов по жестким фильтрам не найдено.\n")
        else:
            for idx, item in enumerate(shorts, 1):
                output.append(
                    f"{idx}. **{item['symbol']}** | Quality Score: `{item['score']}/100`\n"
                    f"   - Вход: `${item['entry']:.4f}` | SL: `${item['sl']:.4f}` (1.5 ATR)\n"
                    f"   - TP1 (50%): `${item['tp1']:.4f}` (2R) -> Стоп в BE+0.1%\n"
                    f"   - TP2 (25%): `${item['tp2']:.4f}` | TP3 (25%): `${item['tp3']:.4f}`\n"
                    f"   - RSI 15m: `{item['rsi']}`\n"
                )

        return "\n".join(output)


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [["🔄 Сканировать рынок"]]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text("👋 Бот запущен! Нажми кнопку ниже для сканирования Binance Futures.", reply_markup=reply_markup)


async def scan_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    status_msg = await update.message.reply_text("⏳ Сканирую фьючерсы Binance (1H/15M/5M)...")
    try:
        report = await run_scan()
        await status_msg.edit_text(report, parse_mode="Markdown")
    except Exception as e:
        logging.error(f"Ошибка анализа: {e}")
        await status_msg.edit_text(f"❌ Ошибка при сканировании: {e}")


def main():
    if not TELEGRAM_BOT_TOKEN:
        print("❌ ОШИБКА: Переменная окружения TELEGRAM_BOT_TOKEN не задана!")
        return

    print("🚀 Подключение к Telegram API...")
    request = HTTPXRequest(connect_timeout=30.0, read_timeout=30.0)
    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).request(request).build()

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("scan", scan_command))
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), scan_command))

    print("🤖 Бот запущен!")
    app.run_polling(bootstrap_retries=-1)


if __name__ == "__main__":
    main()
