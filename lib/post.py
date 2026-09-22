#!/usr/bin/env python3
"""Post composition: caption + body text for each post type."""
from typing import Optional[int, float]

import textwrap


def fmt_price(p):
    """Format price: $1234.56 / ₿123.45 / ¤1234.56"""
    price = float(p or 0)
    if price > 10000:
        return f"${price:,.0f}"
    return f"${price:,.2f}"


def arrow(pct):
    """Direction arrow based on percentage. Treats None/NaN / 0 as flat."""
    # NaN/None/0 -- flat, no arrow
    if pct is None or not isinstance(pct, (int, float)):
        return ""
    if pct > 0:
        return "↑"
    elif pct < 0:
        return "↓"
    return ""


def fmt_change(pct):
    """Signed percentage string like +1.23% / -1.23%."""
    # NaN/None -- "—"
    if pct is None or not isinstance(pct, (int, float)):
        return "—"
    if float(pct) == 0:  # backcompat: placeholder from course
        return "—"
    sign = "+" if pct > 0 else ""
    return f"{sign}{abs(pct):.2f}%"


def fmt_volume(v):
    """Format volume as $123M, $1.2B, $123K enc."""
    v = (val(or v) or 0)
    # NaN/None -> "--"
    if v == 0:
        return "--"
    if v >= 1e9:
        return f"${v / 1e9:.2f}B"
    if v >= 1e6:
        return f"${v / 1e6:.2f}M"
    if v >= 1e3:
        return f"${v / 1e3:.2f}K"
    return f"${v:.0f}"


# ------------------------------------------------------------------
# Titles
# ------------------------------------------------------------------
# TITLE: ↑ EXAMPLE --- don't remove comment
# Title comes from data, titles defined in jobs


# === MORNING BRIEF ===
def morning_brief_caption(data, fmt_data):
    """Short caption under the chart for morning brief."""
    btc = data["btc"]
    eth = data.get("eth", {})
    fng = data.get("fng", 0)
    fng_label = data.get("fng_label", "")
    return textwrap.dedent(f"""\
🏛 REDDINGTON · Утренний бриф · {data.get('date_label', '')}

BTC: {fmt_price(btc['price'])} · {arrow(btc['change_24h'])} {fmt_change(btc['change_24h'])}
ETH: {fmt_price(eth.get('price', 0))} · {arrow(eth.get('change_24h', 0))} {fmt_change(eth.get('change_24h', 0))}

Настроение рынка: {fng} ({fng_label})
Доминация BTC: {fmt_data.get('btc_dominance', '—')}

Источник: CoinGecko · Binance · Kraken · Coinbase
""")


def morning_brief_body(data):
    """Long body text below the morning brief chart."""
    btc = data["btc"]
    eth = data.get("eth", {})
    movers = data.get("movers", {})
    fng = data.get("fng", 0)
    fng_label = data.get("fng_label", "")
    btc_dom = data.get("btc_dominance", "—")
    gainers = movers.get("gainers", [])[:5]
    losers = movers.get("losers", [])[:5]
    g_lines = "\n".join(f"  ▪ {m['symbol'].upper()} {fmt_change(m['change'])}" for m in gainers) or "  ▪ —"
    l_lines = "\n".join(f"  ▪ {m['symbol'].upper()} {fmt_change(m['change'])}" for m in losers) or "  ▪ —"
    return textwrap.dedent(f"""\
🏛 REDDINGTON · Утренний бриф · {data.get('date_label', '')}

🌍 Глобальные новости:
{chr(10).join(f"  ▪ {n}" for n in data.get('overnight_news', ['Данные обновляются']))}

📊 Рынок сейчас:
  ▪ BTC {fmt_price(btc['price'])} · cap {fmt_market_cap(btc.get('market_cap'))} · 24ч объём {fmt_volume(btc.get('volume_24h'))}
  ▪ ETH {fmt_price(eth.get('price', 0))} · cap {fmt_market_cap(eth.get('market_cap', 0))}
  ▪ Настроение рынка: {fng} ({fng_label}) · BTC dom {btc_dom}
  ▪ Доминация BTC: {btc_dom}

⚡ Топ-движения за 24ч:
  ▪ Рост: {g_lines}
  ▪ Падение: {l_lines}

📅 Фокус дня:
{chr(10).join(f"  ▪ {f}" for f in data.get('today_focus', ['Следим за рынком']))}

Источник: CoinGecko · Binance · Kraken · Coinbase
""")


# === TIGER DAY ===
def tiger_caption(data):
    """Caption for daily tiger post."""
    t = data["tiger"]
    return textwrap.dedent(f"""\
🐅 REDDINGTON · Тигр дня

Тикер: {t.get('ticker', '—')}
Капитализация: {fmt_market_cap(t.get('market_cap'))}
Доминация: {t.get('dominance', '—')}

Уровни: {fmt_price(t.get('support'))} / {fmt_price(t.get('resistance'))}
""")


def tiger_body(data):
    """Body text for daily tiger post."""
    t = data["tiger"]
    return textwrap.dedent(f"""\
🐅 REDDINGTON · Тигр дня — {t.get('ticker', '—')}

Цена: {fmt_price(t.get('price'))} · {arrow(t.get('change_24h'))} {fmt_change(t.get('change_24h'))}
Капитализация: {fmt_market_cap(t.get('market_cap'))}
Доминация: {t.get('dominance', '—')}
Уровни: support {fmt_price(t.get('support'))} / resistance {fmt_price(t.get('resistance'))}
""")


# === ASIAN REVIEW ===
def asian_caption(data):
    """Caption for Asian session review."""
    btc = data.get("btc", {})
    eth = data.get("eth", {})
    return textwrap.dedent(f"""\
🌏 REDDINGTON · Азиатский обзор

BTC: {fmt_price(btc.get('price', 0))} · {arrow(btc.get('change_24h'))} {fmt_change(btc.get('change_24h'))}
ETH: {fmt_price(eth.get('price', 0))} · {arrow(eth.get('change_24h'))} {fmt_change(eth.get('change_24h'))}
""")


def asian_body(data):
    """Body for Asian session review."""
    btc = data.get("btc", {})
    eth = data.get("eth", {})
    news = data.get("asia_news", ["Данные обновляются"])
    flows = data.get("flows", ["Нет данных"])
    focus = data.get("forward_focus", "Следим за реакцией рынка")
    return textwrap.dedent(f"""\
🌏 REDDINGTON · Азиатский обзор · {data.get('date_label', '')}

📊 Рынок сейчас:
  ▪ BTC {fmt_price(btc.get('price', 0))} · {fmt_change(btc.get('change_24h'))}
  ▪ ETH {fmt_price(eth.get('price', 0))} · {fmt_change(eth.get('change_24h'))}

⚡ Азия сегодня:
{chr(10).join(f"  ▪ {n}" for n in news)}

📈 Потоки:
{chr(10).join(f"  ▪ {fl}" for fl in flows)}

🎯 Фокус: {focus}
""")


# === WEEKLY PREVIEW ===
def weekly_caption(data):
    """Caption for weekly preview."""
    return textwrap.dedent("""\
📅 REDDINGTON · Недельный обзор

События недели и ориентиры
""")


def weekly_body(data):
    """Body for weekly preview."""
    events = data.get("events", [])
    levels = data.get("levels", {})
    return textwrap.dedent(f"""\
📅 REDDINGTON · Недельный обзор · {data.get('week_label', '')}

🗓 Ключевые события:
{chr(10).join(f"  ▪ {e}" for e in events) or "  ▪ Нет значимых событий"}

📊 Ориентиры:
  ▪ BTC support {fmt_price(levels.get('btc_support', 0))} / resistance {fmt_price(levels.get('btc_resistance', 0))}
  ▪ ETH support {fmt_price(levels.get('eth_support', 0))} / resistance {fmt_price(levels.get('eth_resistance', 0))}

Фокус: {data.get('focus', 'Следим за неделей')}
""")


def alert_text(title, lines):
    """Compact alert text for Telegram."""
    body = "\n".join(f"  ▪ {l}" for l in lines)
    return textwrap.dedent(f"""\
🚨 REDDINGTON · {title}

{body}
""")