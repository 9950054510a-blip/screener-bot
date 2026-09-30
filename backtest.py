"""Historical event-driven proxy backtest for the fixed scanner baseline.

Example: python backtest.py --months 24 --symbols BTCUSDT,ETHUSDT,SOLUSDT

This intentionally uses public Binance OHLC klines and a conservative intrabar
ordering rule. It is a research tool, not proof of future profitability.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

import aiohttp
import pandas as pd

from strategy import CONFIG, find_signal, levels_for_entry


BASE_URL = "https://fapi.binance.com"
INTERVAL_MS = {"5m": 300_000, "15m": 900_000, "1h": 3_600_000}
KLINE_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume", "close_time",
    "quote_asset_volume", "number_of_trades", "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume", "ignore",
]
DEFAULT_SYMBOLS = (
    "BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT,ADAUSDT,DOGEUSDT,LINKUSDT,"
    "AVAXUSDT,TRXUSDT,DOTUSDT,LTCUSDT,ATOMUSDT,UNIUSDT,ETCUSDT,NEARUSDT"
)
logger = logging.getLogger("backtest")


async def request_json(
    session: aiohttp.ClientSession,
    path: str,
    params: dict[str, Any],
) -> Any:
    for attempt in range(5):
        async with session.get(f"{BASE_URL}{path}", params=params) as response:
            if response.status == 200:
                return await response.json()
            if response.status == 429 and attempt < 4:
                retry_after = response.headers.get("Retry-After", "1")
                await asyncio.sleep(min(10.0, float(retry_after)))
                continue
            raise RuntimeError(f"Binance {path} returned HTTP {response.status}.")
    raise RuntimeError(f"Binance {path} failed after rate-limit retries.")


async def download_klines(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
) -> pd.DataFrame:
    rows: list[list[Any]] = []
    cursor = start_ms
    async with semaphore:
        while cursor <= end_ms:
            batch = await request_json(
                session,
                "/fapi/v1/klines",
                {
                    "symbol": symbol,
                    "interval": interval,
                    "startTime": cursor,
                    "endTime": end_ms,
                    "limit": 1500,
                },
            )
            if not batch:
                break
            rows.extend(batch)
            last_open = int(batch[-1][0])
            next_cursor = last_open + INTERVAL_MS[interval]
            if next_cursor <= cursor:
                raise RuntimeError(f"Pagination did not advance for {symbol} {interval}.")
            cursor = next_cursor
            if len(batch) < 1500:
                break
            # Binance USD-M weight rises for large klines; modest spacing keeps
            # parallel symbol downloads comfortably below shared request limits.
            await asyncio.sleep(0.08)

    frame = pd.DataFrame(rows, columns=KLINE_COLUMNS)
    if frame.empty:
        return frame
    for column in KLINE_COLUMNS:
        if column != "ignore":
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna(subset=["open_time", "close_time", "open", "high", "low", "close", "volume"])
    return frame.sort_values("open_time").drop_duplicates("open_time").reset_index(drop=True)


async def download_symbol(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    symbol: str,
    start_ms: int,
    end_ms: int,
) -> dict[str, pd.DataFrame]:
    tasks = []
    for interval, warmup_bars in (("5m", 60), ("15m", 100), ("1h", 300)):
        warm_start = start_ms - warmup_bars * INTERVAL_MS[interval]
        tasks.append(download_klines(session, semaphore, symbol, interval, warm_start, end_ms))
    frames = await asyncio.gather(*tasks)
    return dict(zip(("5m", "15m", "1h"), frames, strict=True))


def _intrabar_touched(side: str, level: float, high: float, low: float, *, stop: bool) -> bool:
    if side == "LONG":
        return low <= level if stop else high >= level
    return high >= level if stop else low <= level


def simulate_trade(
    frame5: pd.DataFrame,
    entry_index: int,
    side: str,
    entry: float,
    risk_distance: float,
) -> dict[str, Any]:
    sign = 1.0 if side == "LONG" else -1.0
    stop = entry - sign * risk_distance
    targets = [entry + sign * multiple * risk_distance for multiple in CONFIG.target_r_multiples]
    remaining = 1.0
    exited_fraction = 0.0
    gross_quote = 0.0
    exit_cost_quote = 0.0
    exits: list[tuple[float, float, str]] = []
    targets_hit = 0
    max_bars = max(1, CONFIG.max_hold_minutes // 5)
    final_index = min(len(frame5) - 1, entry_index + max_bars - 1)

    def close_fraction(fraction: float, price: float, reason: str) -> None:
        nonlocal remaining, exited_fraction, gross_quote, exit_cost_quote
        fraction = min(fraction, remaining)
        if fraction <= 0:
            return
        gross_quote += sign * (price - entry) * fraction
        exit_cost_quote += price * fraction * CONFIG.cost_per_fill_pct
        exits.append((fraction, price, reason))
        remaining -= fraction
        exited_fraction += fraction

    exit_index = final_index
    exit_reason = "time"
    for index in range(entry_index, final_index + 1):
        row = frame5.iloc[index]
        high, low = float(row.high), float(row.low)
        # When a candle hits the active stop, it is assumed to execute before
        # any target touched in the same candle (worst-case OHLC ordering).
        if _intrabar_touched(side, stop, high, low, stop=True):
            close_fraction(remaining, stop, "stop" if targets_hit == 0 else "managed_stop")
            exit_index, exit_reason = index, exits[-1][2]
            break

        for target_index in range(targets_hit, len(targets)):
            target = targets[target_index]
            if not _intrabar_touched(side, target, high, low, stop=False):
                break
            fraction = CONFIG.target_fractions[target_index]
            close_fraction(fraction, target, f"TP{target_index + 1}")
            targets_hit = target_index + 1
            if targets_hit == 1:
                # After TP1, the remaining stop includes one per-fill cost
                # reserve; it is an estimate, not a guaranteed live fill.
                stop = entry + sign * entry * CONFIG.cost_per_fill_pct
            elif targets_hit == 2:
                stop = targets[0]
            if remaining <= 1e-9:
                exit_index, exit_reason = index, "TP3"
                break
            # If the same OHLC candle also crossed a newly raised stop, count
            # the stop conservatively after the target, before any later TP.
            if _intrabar_touched(side, stop, high, low, stop=True):
                close_fraction(remaining, stop, "managed_stop")
                exit_index, exit_reason = index, "managed_stop"
                break
        if remaining <= 1e-9:
            break
        if exits and exit_reason == "managed_stop":
            break

    if remaining > 1e-9:
        exit_row = frame5.iloc[final_index]
        close_fraction(remaining, float(exit_row.close), "time")
        exit_index, exit_reason = final_index, "time"

    entry_cost_quote = entry * CONFIG.cost_per_fill_pct
    net_r = (gross_quote - entry_cost_quote - exit_cost_quote) / risk_distance
    gross_r = gross_quote / risk_distance
    return {
        "gross_r": gross_r,
        "net_r": net_r,
        "targets_hit": targets_hit,
        "exit_reason": exit_reason,
        "exit_index": exit_index,
        "hold_minutes": (exit_index - entry_index + 1) * 5,
    }


def find_raw_signals(symbol: str, frames: dict[str, pd.DataFrame], start_ms: int) -> list[dict[str, Any]]:
    frame5, frame15, frame1h = frames["5m"], frames["15m"], frames["1h"]
    if len(frame5) < 30:
        return []
    results = []
    volume_baseline = frame5["volume"].shift(1).rolling(CONFIG.volume_lookback_5m).mean()
    prior_high = frame5["high"].shift(1).rolling(CONFIG.breakout_lookback_5m).max()
    prior_low = frame5["low"].shift(1).rolling(CONFIG.breakout_lookback_5m).min()

    for index in range(20, len(frame5) - 1):
        row = frame5.iloc[index]
        if int(row.close_time) < start_ms:
            continue
        vol_avg = volume_baseline.iloc[index]
        if pd.isna(vol_avg) or vol_avg <= 0 or float(row.volume) <= float(vol_avg) * CONFIG.min_volume_ratio:
            continue
        if not (float(row.close) > float(prior_high.iloc[index]) or float(row.close) < float(prior_low.iloc[index])):
            continue

        signal_time = int(row.close_time)
        recent5 = frame5.iloc[max(0, index - 59):index + 1]
        recent15 = frame15.loc[frame15["close_time"] <= signal_time].tail(100)
        recent1h = frame1h.loc[frame1h["close_time"] <= signal_time].tail(300)
        if recent15.empty or recent1h.empty:
            continue
        signal = find_signal(recent5, recent15, recent1h, base_asset=symbol[:-4])
        if signal is None:
            continue
        entry_index = index + 1
        entry = float(frame5.iloc[entry_index].open)
        levels = levels_for_entry(signal, entry)
        if levels is None:
            continue
        results.append({
            "symbol": symbol,
            "side": signal["side"],
            "signal_index": index,
            "entry_index": entry_index,
            "signal_time": signal_time,
            "entry_time": int(frame5.iloc[entry_index].open_time),
            "entry": entry,
            "risk_distance": float(signal["risk_distance"]),
            "score": float(signal["score"]),
            "volume_ratio": float(signal["volume_ratio"]),
        })
    return results


def _trade_metrics(trades: list[dict[str, Any]]) -> dict[str, Any]:
    if not trades:
        return {"trades": 0, "win_rate_pct": None, "avg_net_r": None, "median_net_r": None, "total_net_r": 0.0, "profit_factor": None}
    returns = [float(trade["net_r"]) for trade in trades]
    positive = sum(value for value in returns if value > 0)
    negative = -sum(value for value in returns if value < 0)
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for value in returns:
        equity += value
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
    return {
        "trades": len(trades),
        "win_rate_pct": round(sum(value > 0 for value in returns) / len(returns) * 100, 2),
        "avg_net_r": round(sum(returns) / len(returns), 4),
        "median_net_r": round(float(pd.Series(returns).median()), 4),
        "total_net_r": round(sum(returns), 2),
        "profit_factor": round(positive / negative, 3) if negative else None,
        "max_drawdown_r": round(max_drawdown, 2),
        "tp1_hit_pct": round(sum(int(t["targets_hit"]) >= 1 for t in trades) / len(trades) * 100, 2),
        "tp2_hit_pct": round(sum(int(t["targets_hit"]) >= 2 for t in trades) / len(trades) * 100, 2),
        "tp3_hit_pct": round(sum(int(t["targets_hit"]) >= 3 for t in trades) / len(trades) * 100, 2),
    }


async def run(args: argparse.Namespace) -> dict[str, Any]:
    months = max(3, int(args.months))
    symbols = [value.strip().upper() for value in args.symbols.split(",") if value.strip()]
    timeout = aiohttp.ClientTimeout(total=90, connect=10, sock_read=60)
    semaphore = asyncio.Semaphore(4)
    connector = aiohttp.TCPConnector(limit=8, ssl=True)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        server_time = await request_json(session, "/fapi/v1/time", {})
        end_ms = int(server_time["serverTime"])
        end = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc)
        start = end - timedelta(days=30.4375 * months)
        start_ms = int(start.timestamp() * 1000)
        tasks = [download_symbol(session, semaphore, symbol, start_ms, end_ms) for symbol in symbols]
        downloads = await asyncio.gather(*tasks, return_exceptions=True)

    symbol_frames: dict[str, dict[str, pd.DataFrame]] = {}
    for symbol, frames in zip(symbols, downloads, strict=True):
        if isinstance(frames, BaseException):
            logger.error("Failed to download %s: %s", symbol, frames)
            continue
        for interval, frame in frames.items():
            if interval == "5m" and not frame.empty:
                frame = frame.loc[frame["close_time"] < end_ms]
            frames[interval] = frame.reset_index(drop=True)
        if all(not frames[key].empty for key in ("5m", "15m", "1h")):
            symbol_frames[symbol] = frames

    raw_signals: list[dict[str, Any]] = []
    for symbol, frames in symbol_frames.items():
        raw_signals.extend(find_raw_signals(symbol, frames, start_ms))

    # Backtest the top ten score-ranked candidates on each side at each 5m
    # close. The candidate universe is fixed by --symbols and is survivorship
    # biased; it is not a historical reconstruction of the Binance universe.
    by_scan: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for signal in raw_signals:
        by_scan[signal["signal_time"]].append(signal)
    selected = []
    for scan_time, group in by_scan.items():
        for side in ("LONG", "SHORT"):
            ranked = sorted(
                (item for item in group if item["side"] == side),
                key=lambda item: (item["score"], item["volume_ratio"], item["symbol"]),
                reverse=True,
            )
            selected.extend(ranked[:10])

    # Suppress overlapping positions in the same symbol. Different symbols
    # remain independent; no portfolio leverage or correlated-risk model here.
    by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in selected:
        by_symbol[item["symbol"]].append(item)
    trades: list[dict[str, Any]] = []
    for symbol, candidates in by_symbol.items():
        frames = symbol_frames[symbol]
        locked_until = -1
        for candidate in sorted(candidates, key=lambda item: item["entry_index"]):
            if candidate["entry_index"] <= locked_until:
                continue
            result = simulate_trade(
                frames["5m"],
                candidate["entry_index"],
                candidate["side"],
                candidate["entry"],
                candidate["risk_distance"],
            )
            trade = {**candidate, **result}
            trades.append(trade)
            locked_until = int(result["exit_index"])
    trades.sort(key=lambda item: (item["entry_time"], item["symbol"]))

    # Fixed 14/5/5 month split when --months=24; the OOS period is reported
    # separately and is not used to choose the strategy or score weights.
    train_end_ms = int((start + timedelta(days=30.4375 * months * 14 / 24)).timestamp() * 1000)
    wfa_end_ms = int((start + timedelta(days=30.4375 * months * 19 / 24)).timestamp() * 1000)
    periods = {
        "train": [trade for trade in trades if trade["entry_time"] < train_end_ms],
        "walk_forward_validation": [trade for trade in trades if train_end_ms <= trade["entry_time"] < wfa_end_ms],
        "untouched_oos": [trade for trade in trades if trade["entry_time"] >= wfa_end_ms],
    }
    return {
        "period_utc": {"start": start.isoformat(), "end": end.isoformat()},
        "months": months,
        "symbols_downloaded": sorted(symbol_frames),
        "raw_candidates_before_top10": len(raw_signals),
        "candidate_selection": "top 10 by score per side per 5m close; one open position per symbol",
        "assumptions": {
            "entry": "next 5m candle open as OHLC proxy; reject >0.2% drift from signal close",
            "cost": f"{CONFIG.cost_per_fill_pct:.3%} per fill, including modeled taker fee, half-spread and slippage",
            "intrabar": "active stop first; if a target and newly raised stop are both touched, assume target then stop before later targets",
            "funding": "historical funding not included",
            "universe": "fixed user-supplied symbols; survivorship and universe-selection bias",
            "rank_score": "ranking weights and thresholds are unvalidated; score is not a probability",
        },
        "strategy": {
            "stop_r": CONFIG.stop_atr,
            "targets_r": list(CONFIG.target_r_multiples),
            "target_fractions": list(CONFIG.target_fractions),
            "max_hold_minutes": CONFIG.max_hold_minutes,
            "cost_per_fill_pct": CONFIG.cost_per_fill_pct,
        },
        "period_metrics": {name: _trade_metrics(rows) for name, rows in periods.items()},
        "sample_note": "OOS below 200 trades is descriptive only; 200 trades is not proof of robustness.",
        "trade_count": len(trades),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--months", type=int, default=24)
    parser.add_argument("--symbols", default=DEFAULT_SYMBOLS)
    parser.add_argument("--output", help="Optional JSON file path for a machine-readable report.")
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args()
    report = asyncio.run(run(args))
    rendered = json.dumps(report, ensure_ascii=False, indent=2)
    print(rendered)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as output_file:
            output_file.write(rendered + "\n")
