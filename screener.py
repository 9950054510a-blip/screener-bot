import asyncio
import logging
import math
import os
import sys
from aiohttp import ClientSession, web
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

# Настройка логирования
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Токен бота из переменных окружения
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not TELEGRAM_BOT_TOKEN:
  logger.error("TELEGRAM_BOT_TOKEN не найден в переменных окружения!")
  sys.exit(1)

bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher()

BINANCE_BASE_URL = "https://fapi.binance.com"


# ==========================================
# 1. HEALTH CHECK СЕРВЕР ДЛЯ RENDER
# ==========================================
async def handle_health(request):
  """Возвращает 200 OK для проверки жизнеспособности сервиса в Render."""
  return web.Response(text="OK", status=200)


async def start_health_check_server():
  """Запускает фоновый HTTP-сервер на порту, переданном Render (PORT)."""
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
# 2. РАСЧЕТ ИНДИКАТОРОВ И QUALITY SCORE
# ==========================================
def calculate_ema(prices: list, period: int) -> float:
  """Считает EMA для списка цен."""
  if len(prices) < period:
    return prices[-1] if prices else 0.0
  alpha = 2 / (period + 1)
  ema = sum(prices[:period]) / period
  for price in prices[period:]:
    ema = (price * alpha) + (ema * (1 - alpha))
  return ema


def calculate_atr(highs: list, lows: list, closes: list, period: int = 14) -> float:
  """Считает ATR14."""
  if len(closes) <= period:
    return (highs[-1] - lows[-1]) if highs else 0.0
  tr_list = []
  for i in range(1, len(closes)):
    tr = max(
        highs[i] - lows[i],
        abs(highs[i] - closes[i - 1]),
        abs(lows[i] - closes[i - 1]),
    )
    tr_list.append(tr)
  return sum(tr_list[-period:]) / period


def calculate_rsi(closes: list, period: int = 14) -> float:
  """Считает RSI14."""
  if len(closes) <= period:
    return 50.0
  gains, losses = 0.0, 0.0
  for i in range(1, period + 1):
    diff = closes[i] - closes[i - 1]
    if diff >= 0:
      gains += diff
    else:
      losses += abs(diff)

  avg_gain = gains / period
  avg_loss = losses / period

  for i in range(period + 1, len(closes)):
    diff = closes[i] - closes[i - 1]
    gain = diff if diff > 0 else 0.0
    loss = abs(diff) if diff < 0 else 0.0
    avg_gain = (avg_gain * (period - 1) + gain) / period
    avg_loss = (avg_loss * (period - 1) + loss) / period

  if avg_loss == 0:
    return 100.0
  rs = avg_gain / avg_loss
  return 100.0 - (100.0 / (1.0 + rs))


def calculate_quality_score(
    vol_ratio: float, rsi_15m: float, distance_ema: float, direction: str
) -> float:
  """Считает Quality Score (0 - 100) без будущих данных."""
  s_vol = min(100.0, vol_ratio * 50.0)

  if direction == "LONG":
    target_rsi = 41.5
    s_rsi = max(0.0, 100.0 - (abs(rsi_15m - target_rsi) / 6.5) * 100.0)
  else:
    target_rsi = 58.5
    s_rsi = max(0.0, 100.0 - (abs(rsi_15m - target_rsi) / 6.5) * 100.0)

  s_trend = min(100.0, distance_ema * 20.0)

  score = (0.40 * s_vol) + (0.30 * s_rsi) + (0.30 * s_trend)
  return round(score, 1)


# ==========================================
# 3. ПОЛУЧЕНИЕ ДАННЫХ С BINANCE FUTURES
# ==========================================
async def get_trading_pairs(session: ClientSession) -> list:
  """Получает пары USDT-M с quoteVolume >= 15M."""
  url = f"{BINANCE_BASE_URL}/fapi/v1/ticker/24hr"
  async with session.get(url) as resp:
    if resp.status != 200:
      return []
    tickers = await resp.json()

  valid_pairs = []
  for t in tickers:
    symbol = t.get("symbol", "")
    quote_volume = float(t.get("quoteVolume", 0))
    if symbol.endswith("USDT") and quote_volume >= 15_000_000:
      valid_pairs.append(symbol)
  return valid_pairs


async def fetch_klines(
    session: ClientSession, symbol: str, interval: str, limit: int
) -> list:
  """Запрашивает свечи с Binance."""
  url = f"{BINANCE_BASE_URL}/fapi/v1/klines?symbol={symbol}&interval={interval}&limit={limit}"
  async with session.get(url) as resp:
    if resp.status != 200:
      return []
    return await resp.json()


async def analyze_symbol(session: ClientSession, symbol: str, btc_trend: str):
  """Анализирует инструмент по фильтрам Trend-Pullback."""
  try:
    klines_1h = await fetch_klines(session, symbol, "1h", 210)
    klines_15m = await fetch_klines(session, symbol, "15m", 60)
    klines_5m = await fetch_klines(session, symbol, "5m", 30)

    if (
        len(klines_1h) < 205
        or len(klines_15m) < 20
        or len(klines_5m) < 22
    ):
      return None

    # Закрытые свечи 1H
    closes_1h = [float(k[4]) for k in klines_1h[:-1]]
    ema20_1h = calculate_ema(closes_1h, 20)
    ema50_1h = calculate_ema(closes_1h, 50)
    ema200_1h = calculate_ema(closes_1h, 200)
    last_close_1h = closes_1h[-1]

    coin_long = (ema20_1h > ema50_1h) and (last_close_1h > ema200_1h)
    coin_short = (ema20_1h < ema50_1h) and (last_close_1h < ema200_1h)

    # Закрытые свечи 15M (m-1, m-2, m-3)
    closes_15m = [float(k[4]) for k in klines_15m[:-1]]
    highs_15m = [float(k[2]) for k in klines_15m[:-1]]
    lows_15m = [float(k[3]) for k in klines_15m[:-1]]

    ema20_15m = calculate_ema(closes_15m, 20)
    ema50_15m = calculate_ema(closes_15m, 50)
    zone_min_15m = min(ema20_15m, ema50_15m)
    zone_max_15m = max(ema20_15m, ema50_15m)

    atr14_15m = calculate_atr(highs_15m, lows_15m, closes_15m, 14)
    rsi14_15m = calculate_rsi(closes_15m, 14)

    # 15m Touch, Depth, Hold
    lows_3bars_15m = min(lows_15m[-3:])
    highs_3bars_15m = max(highs_15m[-3:])

    long_touch = lows_3bars_15m <= zone_max_15m
    long_wick_depth = lows_3bars_15m >= (zone_min_15m - (0.3 * atr14_15m))
    long_hold = closes_15m[-1] > zone_min_15m
    rsi_long_ok = 35.0 <= rsi14_15m <= 48.0

    short_touch = highs_3bars_15m >= zone_min_15m
    short_wick_depth = highs_3bars_15m <= (zone_max_15m + (0.3 * atr14_15m))
    short_hold = closes_15m[-1] < zone_max_15m
    rsi_short_ok = 52.0 <= rsi14_15m <= 65.0

    # Закрытые свечи 5M
    closes_5m = [float(k[4]) for k in klines_5m[:-1]]
    highs_5m = [float(k[2]) for k in klines_5m[:-1]]
    lows_5m = [float(k[3]) for k in klines_5m[:-1]]
    volumes_5m = [float(k[5]) for k in klines_5m[:-1]]

    # Breakout 5m (t-1 пробивает t-2..t-4)
    breakout_long = closes_5m[-1] > max(highs_5m[-4:-1])
    breakout_short = closes_5m[-1] < min(lows_5m[-4:-1])

    # Volume 5m (t-1 > SMA20 на t-2..t-21)
    vol_ref = sum(volumes_5m[-21:-1]) / 20.0
    vol_ratio = volumes_5m[-1] / vol_ref if vol_ref > 0 else 0.0
    vol_cond = vol_ratio > 1.0

    highs_1h = [float(k[2]) for k in klines_1h[:-1]]
    lows_1h = [float(k[3]) for k in klines_1h[:-1]]
    atr14_1h = calculate_atr(highs_1h, lows_1h, closes_1h, 14)
    dist_ema = (
        abs(last_close_1h - ema200_1h) / atr14_1h if atr14_1h > 0 else 1.0
    )

    entry_price = closes_5m[-1]

    # Валидация LONG
    if (
        btc_trend == "LONG"
        and coin_long
        and long_touch
        and long_wick_depth
        and long_hold
        and rsi_long_ok
        and breakout_long
        and vol_cond
    ):
      score = calculate_quality_score(vol_ratio, rsi14_15m, dist_ema, "LONG")
      sl = entry_price - (1.5 * atr14_15m)
      tp1 = entry_price + (3.0 * atr14_15m)
      tp2 = entry_price + (4.5 * atr14_15m)
      tp3 = entry_price + (6.0 * atr14_15m)
      return {
          "symbol": symbol,
          "direction": "LONG",
          "score": score,
          "entry": entry_price,
          "sl": sl,
          "tp1": tp1,
          "tp2": tp2,
          "tp3": tp3,
          "vol_ratio": round(vol_ratio, 2),
          "rsi": round(rsi14_15m, 1),
      }

    # Валидация SHORT
    if (
        btc_trend == "SHORT"
        and coin_short
        and short_touch
        and short_wick_depth
        and short_hold
        and rsi_short_ok
        and breakout_short
        and vol_cond
    ):
      score = calculate_quality_score(vol_ratio, rsi14_15m, dist_ema, "SHORT")
      sl = entry_price + (1.5 * atr14_15m)
      tp1 = entry_price - (3.0 * atr14_15m)
      tp2 = entry_price - (4.5 * atr14_15m)
      tp3 = entry_price - (6.0 * atr14_15m)
      return {
          "symbol": symbol,
          "direction": "SHORT",
          "score": score,
          "entry": entry_price,
          "sl": sl,
          "tp1": tp1,
          "tp2": tp2,
          "tp3": tp3,
          "vol_ratio": round(vol_ratio, 2),
          "rsi": round(rsi14_15m, 1),
      }

  except Exception as e:
    logger.error(f"Ошибка анализа {symbol}: {e}")
  return None


async def run_market_scan() -> tuple:
  """Сканирует рынок Binance Futures и формирует выборки."""
  async with ClientSession() as session:
    # 1. Тренд BTC 1H
    btc_klines = await fetch_klines(session, "BTCUSDT", "1h", 210)
    if len(btc_klines) < 205:
      return [], []
    btc_closes = [float(k[4]) for k in btc_klines[:-1]]
    btc_ema200 = calculate_ema(btc_closes, 200)
    btc_trend = "LONG" if btc_closes[-1] > btc_ema200 else "SHORT"

    # 2. Получение пар
    pairs = await get_trading_pairs(session)

    # 3. Анализ монет
    tasks = [analyze_symbol(session, pair, btc_trend) for pair in pairs]
    results = await asyncio.gather(*tasks)

    longs = [r for r in results if r and r["direction"] == "LONG"]
    shorts = [r for r in results if r and r["direction"] == "SHORT"]

    # 4. Ранжирование по Quality Score
    longs.sort(key=lambda x: x["score"], reverse=True)
    shorts.sort(key=lambda x: x["score"], reverse=True)

    return longs[:10], shorts[:10]


# ==========================================
# 4. TELEGRAM БОТ ОБРАБОТЧИКИ
# ==========================================
def get_scan_keyboard():
  """Инлайн-кнопка под сообщением."""
  keyboard = InlineKeyboardMarkup(
      inline_keyboard=[[
          InlineKeyboardButton(
              text="🔍 Запустить сканирование", callback_data="run_scan"
          )
      ]]
  )
  return keyboard


@dp.message(Command("start"))
async def cmd_start(message: types.Message):
  await message.answer(
      "👋 Привет! Я quantitative скринер Binance USDT-M Futures.\n\n"
      "Нажмите кнопку ниже или отправьте /scan для поиска сетапов"
      " Trend-Pullback.",
      reply_markup=get_scan_keyboard(),
  )


# Принимает команду /scan И ЛЮБЫЕ варианты текста с кнопок (включая Reply Keyboard)
@dp.message(
    lambda m: m.text
    and (
        m.text.startswith("/scan")
        or "сканер" in m.text.lower()
        or "сканирование" in m.text.lower()
    )
)
async def cmd_scan_text(message: types.Message):
  await execute_scan_and_send(message.chat.id)


@dp.callback_query(lambda c: c.data == "run_scan")
async def callback_scan(callback_query: types.CallbackQuery):
  await callback_query.answer("Запуск сканирования рынка...")
  await execute_scan_and_send(callback_query.message.chat.id)


async def execute_scan_and_send(chat_id: int):
  status_msg = await bot.send_message(
      chat_id, "⏳ Сканирую рынки Binance USDT-M Futures (1H / 15M / 5M)..."
  )

  longs, shorts = await run_market_scan()

  if not longs and not shorts:
    await bot.edit_message_text(
        "❌ Качественных сетапов не найдено. Фильтры строго выдержаны.",
        chat_id=chat_id,
        message_id=status_msg.message_id,
        reply_markup=get_scan_keyboard(),
    )
    return

  report = "📊 **РЕЗУЛЬТАТЫ СКАННРОВАНИЯ (TREND-PULLBACK)**\n\n"

  if longs:
    report += f"🟢 **TOP-{len(longs)} LONG СЕТАПЫ:**\n"
    for item in longs:
      report += (
          f"• **{item['symbol']}** | Score: **{item['score']}**\n"
          f"  Вход: `{item['entry']}` | SL: `{item['sl']:.4f}`\n"
          f"  TP1: `{item['tp1']:.4f}` | TP2: `{item['tp2']:.4f}` | TP3:"
          f" `{item['tp3']:.4f}`\n"
          f"  Vol Ratio: {item['vol_ratio']}x | RSI: {item['rsi']}\n\n"
      )

  if shorts:
    report += f"🔴 **TOP-{len(shorts)} SHORT СЕТАПЫ:**\n"
    for item in shorts:
      report += (
          f"• **{item['symbol']}** | Score: **{item['score']}**\n"
          f"  Вход: `{item['entry']}` | SL: `{item['sl']:.4f}`\n"
          f"  TP1: `{item['tp1']:.4f}` | TP2: `{item['tp2']:.4f}` | TP3:"
          f" `{item['tp3']:.4f}`\n"
          f"  Vol Ratio: {item['vol_ratio']}x | RSI: {item['rsi']}\n\n"
      )

  report += "⚠️ *Исполнение ордеров отключено (Screener-only).* "

  await bot.edit_message_text(
      report,
      chat_id=chat_id,
      message_id=status_msg.message_id,
      parse_mode="Markdown",
      reply_markup=get_scan_keyboard(),
  )


# ==========================================
# 5. ТОЧКА ВХОДА (MAIN)
# ==========================================
async def main():
  await start_health_check_server()
  await bot.delete_webhook(drop_pending_updates=True)
  logger.info("Бот успешно запущен в режиме Long Polling!")
  await dp.start_polling(bot)


if __name__ == "__main__":
  asyncio.run(main())
