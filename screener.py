import asyncio
import os
import sys
import aiohttp
import sqlite3
import logging
from aiogram import Bot

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

# Переменные окружения из GitHub Secrets
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

DB_PATH = "screener.db"

def init_db():
    """Инициализация локальной базы данных SQLite для хранения состояния"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            symbol TEXT PRIMARY KEY,
            last_price REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()
    logging.info("📁 База данных SQLite успешно инициализирована.")

async def fetch_binance_futures():
    """Асинхронный запрос к публичному API Binance Futures для получения цен"""
    url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    return data
                else:
                    logging.error(f"Ошибка Binance API: статус {response.status}")
                    return []
        except Exception as e:
            logging.error(f"Ошибка подключения к Binance: {e}")
            return []

async def analyze_market(tickers):
    """Логика сканирования и отбора топ-монет по волатильности"""
    signals = []
    
    valid_tickers = [t for t in tickers if t.get('symbol', '').endswith('USDT')]
    valid_tickers.sort(key=lambda x: float(x.get('priceChangePercent', 0)), reverse=True)
    
    top_gainers = valid_tickers[:3]
    
    report_lines = ["🚀 *NEXUS Screener: 5-Min Report*\n"]
    report_lines.append("📊 *Топ движения на Binance Futures:*")
    
    for item in top_gainers:
        symbol = item.get('symbol')
        price = float(item.get('lastPrice', 0))
        change = float(item.get('priceChangePercent', 0))
        report_lines.append(f"• `{symbol}`: *{price}* (`{change:+.2f}%`)")
        
    return "\n".join(report_lines)

async def send_telegram_message(text: str):
    """Отправка отчета в Telegram без использования polling"""
    if not BOT_TOKEN or not CHAT_ID:
        logging.error("Не заданы TELEGRAM_BOT_TOKEN или CHAT_ID в переменных окружения!")
        return

    bot = Bot(token=BOT_TOKEN)
    try:
        await bot.send_message(chat_id=CHAT_ID, text=text, parse_mode="Markdown")
        logging.info("✅ Отчет успешно отправлен в Telegram.")
    except Exception as e:
        logging.error(f"Ошибка при отправке сообщения в Telegram: {e}")
    finally:
        await bot.session.close()

async def main():
    logging.info("🤖 NEXUS Screener запущен в режиме разового сканирования (GitHub Actions).")
    
    init_db()
    
    raw_data = await fetch_binance_futures()
    if not raw_data:
        logging.warning("Не удалось получить данные с биржи. Завершаем работу.")
        return

    report_text = await analyze_market(raw_data)
    await send_telegram_message(report_text)
    
    logging.info("✨ Сканирование и отправка завершены. Выход.")

if __name__ == "__main__":
    asyncio.run(main())
