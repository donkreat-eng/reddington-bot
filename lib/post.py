"""Post composition: caption + body text for each post type."""
import textwrap


def fmt_price(p):
    """Format price: 4 decimals under 100, 0 decimals above 1000, 2 between."""
    if p is None:
        return "—"
    if p < 1:
        return f"${p:.4f}"
    if p < 100:
        return f"${p:.2f}"
    if p < 1000:
        return f"${p:.2f}"
    return f"${p:,.0f}"


def arrow(pct):
    if pct is None or pct == 0:
        return ""
    return "▲" if pct >= 0 else "▼"


def fmt_change(pct):
    if pct is None or pct == 0:
        return "—"
    sign = "+" if pct >= 0 else ""
    return f"{sign}{pct:.2f}%"


def fmt_volume(v):
    """Format volume in M or B."""
    if v is None or v == 0:
        return "—"
    if v >= 1e9:
        return f"${v/1e9:.1f}B"
    if v >= 1e6:
        return f"${v/1e6:.1f}M"
    return f"${v/1e3:.0f}K"


def fmt_market_cap(v):
    if v is None or v == 0:
        return "—"
    if v >= 1e12:
        return f"${v/1e12:.1f}T"
    if v >= 1e9:
        return f"${v/1e9:.1f}B"
    return f"${v/1e6:.0f}M"


def morning_brief_caption(data, fmt_data):
    btc = data["btc"]
    eth = data.get("eth", {})
    fng = data.get("fng", 0)
    fng_label = data.get("fng_label", "")
    return textwrap.dedent(f"""\
🏛 REDDINGTON · Утренний бриф | {btc['date_label']}

BTC: {fmt_price(btc['price'])} · {arrow(btc['change_24h'])} {fmt_change(btc['change_24h'])}
ETH: {fmt_price(eth.get('price', 0))} · {arrow(eth.get('change_24h', 0))} {fmt_change(eth.get('change_24h', 0))}

⚡ Индекс страха и жадности: {fng} ({fng_label})
⚖ Доминация BTC: {fmt_data.get('btc_dominance', '-')}

📊 Источники: CoinGecko · Binance · Kraken · Coinbase
""")


def morning_brief_body(data):
    btc = data["btc"]
    eth = data.get("eth", {})
    movers = data.get("movers", {})
    fng = data.get("fng", 0)
    fng_label = data.get("fng_label", "")
    btc_dom = data.get("btc_dominance", "-")
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

📈 Лидеры роста:
{g_lines}

📉 Лидеры падения:
{l_lines}

🎯 Что ждать сегодня:
{chr(10).join(f"  ▪ {f}" for f in data.get('today_focus', ['Мониторим макро-календарь и уровни BTC']))}

💬 Если у вас есть вопросы по утреннему брифу - спрашивайте в комментариях.

Источники: CoinGecko · Binance · Kraken · Coinbase
Cross-check: {btc.get('sources_count', '-')} источников в консенсусе
""")


def tiger_caption(data):
    t = data["tiger"]
    direction = "long" if t["bias"] == "long" else "short" if t["bias"] == "short" else "neutral"
    bias_emoji = {"long": "🟢", "short": "🔴", "neutral": "⚪"}.get(direction, "⚪")
    return textwrap.dedent(f"""\
🏛 REDDINGTON · Тигр дня | {t['ticker']}

{fmt_price(t['price'])} · {arrow(t['change_24h'])} {fmt_change(t['change_24h'])}
Cap: {fmt_market_cap(t.get('market_cap'))}

{bias_emoji} Bias: {direction.upper()}
🛡 Поддержка: {fmt_price(t['support'][0])}–{fmt_price(t['support'][1])}
⚔ Сопротивление: {fmt_price(t['resistance'][0])}–{fmt_price(t['resistance'][1])}
""")


def tiger_body(data):
    t = data["tiger"]
    return textwrap.dedent(f"""\
🏛 REDDINGTON · Тигр дня · {t['ticker']}

🎯 {t['ticker']} {fmt_price(t['price'])}
Тигр дня: {t.get('reason', 'Выбран по алгоритму: лидер по объёму + волатильности')}

📊 Ключевые метрики:
{fmt_price(t['price'])} · 24ч {fmt_change(t['change_24h'])} · 7д {fmt_change(t.get('change_7d', 0))}
Капитализация: {fmt_market_cap(t.get('market_cap'))}
Доминация: {t.get('dominance', '—')}

👥 Драйверы:
{chr(10).join(f"  ▪ {d}" for d in t.get('drivers', ['Высокий объём торгов']))}

💰 Фандинг:
{chr(10).join(f"  ▪ {f}" for f in t.get('fundamentals', ['Данные обновляются']))}

⚠ Риски:
{chr(10).join(f"  ▪ {r}" for r in t.get('risks', ['Рынок волатилен, риск высок']))}

🛡 Зоны входа и рисков:
  ▪ Поддержка: Support {fmt_price(t['support'][0])}–{fmt_price(t['support'][1])} / Resistance {fmt_price(t['resistance'][0])}–{fmt_price(t['resistance'][1])}
  ▪ Базовый: {t.get('scenarios', {}).get('base', 'Консервативный сценарий - вход на поддержке > 3%')}
  ▪ Бычий: {t.get('scenarios', {}).get('bull', 'Пробой сопротивления → продолжение движения вверх')}
  ▪ Медвежий: {t.get('scenarios', {}).get('bear', 'Потеря поддержки → продолжение движения вниз')}

📌 P.S. {t.get('ps', 'Тип сделки - спекулятивный, не для долгосрочного портфеля.')}

📊 Что обсуждают в крипто-сообществе по {t['ticker']} - спрашивайте в комментариях.

Источники: CoinGecko OHLC · Binance · Kraken · Coinbase
""")


def asian_caption(data):
    return textwrap.dedent(f"""\
🏛 REDDINGTON · Азиатский обзор | {data.get('date_label', '')}

BTC: {fmt_price(data['btc']['price'])} · {arrow(data['btc'].get('change_24h'))} {fmt_change(data['btc'].get('change_24h'))}
ETH: {fmt_price(data['eth'].get('price', 0))} · {arrow(data['eth'].get('change_24h', 0))} {fmt_change(data['eth'].get('change_24h', 0))}

Азиатская сессия закрывается
""")


def asian_body(data):
    btc = data["btc"]
    return textwrap.dedent(f"""\
🏛 REDDINGTON · Азиатский обзор · {data.get('date_label', '')}

🌏 Азия сегодня:
{chr(10).join(f"  ▪ {n}" for n in data.get('asia_news', ['Азиатская сессия без значимых событий']))}

📊 Движения на закрытии азиатской сессии:
  ▪ BTC: {fmt_price(btc['price'])} ({fmt_change(btc.get('change_24h'))})
  ▪ ETH: {fmt_price(data['eth'].get('price', 0))} ({fmt_change(data['eth'].get('change_24h', 0))})

🏦 Институциональные потоки:
{chr(10).join(f"  ▪ {e}" for e in data.get('flows', ['Данные пока в обработке']))}

🎯 Что ждать от европейской/американской сессии:
{chr(10).join(f"  ▪ {f}" for f in data.get('forward_focus', ['Мониторим макро-календарь']))}

📬 Если у вас есть вопросы по азиатскому рынку - спрашивайте в комментариях.

Источники: Wu Blockchain · CoinGecko · Binance
""")


def weekly_caption(data):
    return textwrap.dedent(f"""\
🏛 REDDINGTON · Неделя {data.get('week_range', '')}

BTC {fmt_price(data['btc']['price'])} · {fmt_change(data['btc'].get('change_7d', 0))} за неделю
ETH {fmt_price(data['eth'].get('price', 0))} · {fmt_change(data['eth'].get('change_7d', 0))} за неделю

Закрытие недели. События и план.
""")


def weekly_body(data):
    btc = data["btc"]
    eth = data.get("eth", {})
    return textwrap.dedent(f"""\
🏛 REDDINGTON · Неделя {data.get('week_range', '')}

📊 Итоги недели:
  ▪ BTC: {fmt_price(btc['price'])} · {fmt_change(btc.get('change_7d', 0))} за неделю
  ▪ ETH: {fmt_price(eth.get('price', 0))} · {fmt_change(eth.get('change_7d', 0))}
  ▪ Настроение рынка: {data.get('fng', '-')} ({data.get('fng_label', '')})
  ▪ Доминация BTC: {data.get('btc_dominance', '-')}
{chr(10).join(f"  ▪ {m}" for m in data.get('week_summary', ['Рынок провёл неделю в боковике']))}

🎯 Что ждать от следующей недели:
{chr(10).join(f"  ▪ {w}" for w in data.get('week_lookahead', ['Следим за макро-календарём']))}

💬 Если у вас есть вопросы по итогам недели - спрашивайте в комментариях.

📌 Корреляции:
{chr(10).join(f"  ▪ {l}" for l in data.get('correlations', ['DXY и крипто показывают отрицательную корреляцию']))}

🛡 Зоны входа и рисков:
  ▪ Поддержка: {data.get('plan_base', 'Консервативный сценарий - набор позиции > 3%')}
  ▪ Агрессивный: {data.get('plan_aggr', 'Агрессивный сценарий при пробое ключевых уровней')}

📌 P.S. {data.get('ps', 'Тип сделки - спекулятивный, не для долгосрочного портфеля.')}

📊 Что обсуждают в крипто-сообществе по итогам недели - спрашивайте в комментариях.

Источники: CoinGecko · Wu Blockchain · Federal Reserve · BLS
""")


def alert_text(title, lines):
    body = "\n".join(f"  ▪ {l}" for l in lines)
    return f"""⚡ REDDINGTON ALERTS
{title}

{body}

Источник: Reuters · Bloomberg · Coinglass · Federal Reserve
"""