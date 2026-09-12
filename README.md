# Reddington Bot

Crypto analytics bot that posts to **Reddington Trade** Telegram channel.
Runs on GitHub Actions schedule — no VPS needed.

## Schedule (YEKT = UTC+5)

| Time | Job | Days |
|------|-----|------|
| 09:00 | Morning brief | Mon–Fri |
| 14:45 | Tiger of the day | Mon–Fri |
| 16:15 | Asian review | Mon–Fri |
| 20:00 (Sun) | Weekly preview | Sun |
| every 10 min | Alert monitor | daily |

## Setup

1. Clone this repo
2. Set GitHub Secrets:
   - `REDDINGTON_BOT_TOKEN` — Telegram bot token from @BotFather
   - `REDDINGTON_CHANNEL_ID` — numeric chat_id of the channel
3. Push to `main` branch
4. Actions runs automatically on cron schedule

## Local development

```bash
export REDDINGTON_BOT_TOKEN=...
export REDDINGTON_CHANNEL_ID=...
python -m pip install -r requirements.txt
python jobs/morning_brief.py
```

## Architecture

- `lib/fetch.py` — multi-source price fetcher (CoinGecko, Binance, Kraken, Coinbase) with median + retry
- `lib/chart.py` — TradingView-style chart generator
- `lib/publish.py` — Telegram publishing
- `lib/post.py` — post templates (caption + body)
- `jobs/*.py` — one script per post type
- `.github/workflows/bot.yml` — schedule + dispatcher

## Verification

Every post goes through a multi-source price check:
- 4 sources fetched in parallel
- Median price computed
- Spread checked: OK (<0.3%), MINOR_DRIFT, MAJOR_DRIFT, CRITICAL
- Retry logic: 3 attempts × 15min delays for regular posts
- Alerts: no retry, published within 5 min or skipped
