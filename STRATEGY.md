# Binance screener strategy

## Purpose and limits

The bot scans only when the user sends `/scan` or presses **🔍 Запустить сканер**. It returns up to ten ranked LONG and ten ranked SHORT setups from Binance USD-M USDT perpetual contracts. It does not place orders. A score sorts setups within a side; it is not a win probability, expected return, or guarantee.

The thresholds below are a fixed research baseline, not a claim that the strategy is profitable. `backtest.py` is provided to assess it without selecting parameters on the untouched out-of-sample period. Do not change baseline values after looking at OOS results; any change requires a new holdout period.

## Market universe

- Contract metadata must identify a currently trading USDT-quoted perpetual.
- Exclude stablecoin base assets.
- Minimum 24-hour quote volume: 15,000,000 USDT.
- Maximum quoted bid/ask spread: 0.05% of midpoint.
- Download candles only for the 60 most liquid eligible contracts.
- Historical backtest currently accepts a fixed symbol list. That creates survivorship and universe-selection bias; it does not reproduce Binance's historical top-volume universe.

## Entry setup

All calculations use closed candles. The last closed 5-minute candle is the trigger. The latest closed 15-minute and 1-hour candles with close times no later than the trigger candle are used, so no forming higher-timeframe candle can leak into a signal.

### LONG

1. On 1h, EMA20 > EMA50 and close > EMA200.
2. On 15m, RSI14 is 35–48. During the latest three closed 15m candles, price touches the EMA20–EMA50 band without wicking more than 0.3 ATR14 below the lower edge; the latest 15m close is above that lower edge.
3. The 5m close exceeds the high of the previous three closed 5m candles.
4. Trigger 5m volume is greater than the mean volume of the previous 20 closed 5m candles, excluding the trigger candle.

### SHORT

Mirror the conditions: 1h EMA20 < EMA50 and close < EMA200; 15m RSI14 is 52–65; price touches the EMA20–EMA50 band over the latest three closed 15m candles, with no wick more than 0.3 ATR14 above the upper edge and the latest close below it; 5m close breaks the previous three-candle low; trigger volume exceeds the previous 20-candle mean.

RSI and ATR use Wilder smoothing. EMAs use `adjust=False`. If required history or indicators are missing, the contract is skipped.

## Entry, levels, and management plan

- Candidate entry is the current ask for LONG or current bid for SHORT. If that quote is more than 0.2% away from the trigger 5m close, skip the candidate.
- Initial stop distance is 1.5 × ATR14 on 15m.
- TP1 is 2R and closes 50% of the initial position; TP2 is 3R and closes 25%; TP3 is 4R and closes the remaining 25%.
- After TP1, move the remaining stop to entry plus a 0.10% cost reserve in the trade direction. After TP2, move it to TP1. Maximum holding time is 240 minutes.
- The 0.10% per-fill cost assumption combines an estimated 0.05% taker fee, 0.02% half-spread, and 0.03% slippage. Actual fees, spread, slippage, funding, and fills vary by account and market. The stop buffer is a planning estimate, not a guaranteed execution price.

The bot displays indicative levels and a cost-aware process assumption; it cannot know whether an exchange order will fill at those prices. The strategy is informational and does not manage a live position.

## Ranking (not probability)

Score each valid candidate from 0 to 100, then sort separately within LONG and SHORT. Ties are broken by higher 24-hour quote volume.

- Volume, 35%: `min(100, volume_ratio / 2 × 100)`, where `volume_ratio = trigger_volume / mean(previous 20 closed 5m volumes)`.
- RSI, 25%: `max(0, 100 × (1 - abs(RSI - band_midpoint) / band_half_width))`; midpoint is 41.5 LONG and 58.5 SHORT, half-width 6.5.
- Trend, 25%: `clamp((directional_abs(EMA20_1h - EMA50_1h) / ATR14_1h - 0.2) / 1.3, 0, 1) × 100`. The directional EMA stack is already a hard filter; the score saturates at 1.5 ATR.
- Breakout, 15%: `clamp((close - previous_3_bar_high) / ATR14_15m / 0.5, 0, 1) × 100` for LONG, mirrored against the previous 3-bar low for SHORT.

These weights are transparent but unvalidated. They must not be interpreted as calibrated probabilities. Candidates failing liquidity, timeframe, setup, or quote-drift filters are omitted; the bot never pads the lists with invalid setups.

## Backtest assumptions and gates

`backtest.py` evaluates signals at each closed 5m candle, enters at the next 5m open as an OHLC proxy, and rejects entry gaps over 0.2%. It reports fixed train / walk-forward validation / untouched OOS windows (14/5/5 months when run with 24 months); do not tune using OOS. Within a candle, an active stop is assumed to execute before any target. If a target and a newly raised stop are both touched, the stop is assumed after that target and before later targets. Every entry and exit fill incurs the 0.10% modeled cost.

Known limitations: historical funding is not included; OHLC cannot resolve exact intrabar ordering or actual bid/ask; historical spread and slippage are approximated; fixed-symbol backtests have survivorship bias; correlated positions and account-level risk are not modeled. OOS results with fewer than 200 trades are descriptive only, and even 200 trades do not prove robustness. Run cost and parameter sensitivity on the training / validation data before treating OOS as a one-time final check.

## Telegram and deployment

Set `TELEGRAM_BOT_TOKEN` in the hosting provider's environment. No Binance API credentials are required. The health server responds at `/` and `/health`; the Telegram bot supports `/start`, `/help`, `/scan`, and the existing scan button. The app suppresses HTTP-client INFO logs that would otherwise include token-bearing Telegram API URLs and redacts the configured token from log records.
