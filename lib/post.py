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


# === MORNING BRIEF ===
def morning_brief_caption(data, fmt_data):
    """Short caption under the chart for morning brief."""
    btc = data["btc"]
    eth = data.get("eth", {})
    fng = data.get("fng", 0)
    fng_label = data.get("fng_label", "")
    return textwrap.dedent(f"""\
🏛 REDDINGTON · Утренний бриф

BTC: {fmt_price(btc['price'])} · {arrow(btc['change_24h'])} {fmt_change(btc['change_24h'])}
ETH: {fmt_price(eth.get('price', 0))} · {arrow(eth.get('change_24h', 0))} {fmt_change(eth.get('change_24h', 0))}

Настроение рынка: {fng} ({fng_label})
Доминация BTC: {fmt_data.get('btc_dominance', '—')}

Источник: CoinGecko · Binance · Kraken · Coinbase
""")


def morning_brief_body(data):
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

🔍 Овернайт:
{chr(10).join(f"  ▪ {n}" for n in data.get('overnight_news', ['Рынок без значимых новостей']))}

📊 Рынок:
  ▪ BTC {fmt_price(btc['price'])} · cap {fmt_market_cap(btc.get('market_cap'))} · 24ч объём {fmt_volume(btc.get('volume_24h'))}
  ▪ ETH {fmt_price(eth.get('price', 0))} · cap {fmt_market_cap(eth.get('market_cap', 0))}
  ▪ Настроение: {fng} ({fng_label}) · BTC dom {btc_dom}

🟢 Лидеры роста 24ч:
{g_lines}

🔴 Аутсайдеры 24ч:
{l_lines}

🎯 Что смотреть сегодня:
{chr(10).join(f"  ▪ {f}" for f in data.get('today_focus', ['Макро-фон спокойный', 'Ждём CPI/FOMC если в графике']))}

⚠️ Главные события этой недели:
{chr(10).join(f"  ▪ {e}" for e in data.get('week_events', [])) or '  ▪ Календарь спокойный'}

📊 Источники: CoinGecko · Binance · Kraken · Coinbase
Cross-check: {btc.get('sources_count', '—')} источников согласованы
""")


# === TIGER OF THE DAY ===
def tiger_caption(data):
    t = data["tiger"]
    direction = "long" if t["bias"] == "long" else "short" if t["bias"] == "short" else "neutral"
    bias_emoji = {"long": "🟢", "short": "🔴", "neutral": "⚪"}.get(direction, "⚪")
    return textwrap.dedent(f"""\
🏛 REDDINGTON · Тайгер дня | {t['ticker']}

{fmt_price(t['price'])} · {arrow(t['change_24h'])} {fmt_change(t['change_24h'])}
Cap: {fmt_market_cap(t.get('market_cap'))}

{bias_emoji} Bias: {direction.upper()}
Поддержка: {fmt_price(t['support'][0])}–{fmt_price(t['support'][1])}
Сопротивление: {fmt_price(t['resistance'][0])}–{fmt_price(t['resistance'][1])}
""")


def tiger_body(data):
    t = data["tiger"]
    return textwrap.dedent(f"""\
🏛 REDDINGTON · Тайгер дня · {t['ticker']}
Почему именно {t['ticker']} сегодня: {t.get('reason', 'Самое сильное движение на рынке')}

📊 Состояние:
{fmt_price(t['price'])} · 24ч {fmt_change(t['change_24h'])} · 7д {fmt_change(t.get('change_7d', 0))}
Капитализация: {fmt_market_cap(t.get('market_cap'))}
Доминация: {t.get('dominance', '—')}

🔻 Что движет цену:
{chr(10).join(f"  ▪ {d}" for d in t.get('drivers', ['Нет значимых нарративов']))}

✅ Фундаментал:
{chr(10).join(f"  ▪ {f}" for f in t.get('fundamentals', ['Нет данных']))}

⚠️ Риски:
{chr(10).join(f"  ▪ {r}" for r in t.get('risks', ['Рынок нестабилен']))}

🎯 Торговый план:
  ▪ Зоны: Support {fmt_price(t['support'][0])}–{fmt_price(t['support'][1])} / Resistance {fmt_price(t['resistance'][0])}–{fmt_price(t['resistance'][1])}
  ▪ Базовый: {t.get('scenarios', {}).get('base', 'Боковик до триггера')}
  ▪ Бычий: {t.get('scenarios', {}).get('bull', 'Пробой сопротивления с объёмом')}
  ▪ Медвежий: {t.get('scenarios', {}).get('bear', 'Потеря поддержки → глубокая коррекция')}

🟠 P.S. {t.get('ps', 'Следи за объёмами и реакцией на уровни')}

📬 Вопрос: у кого есть позиция в {t['ticker']} — на каком уровне средняя?

Источники: CoinGecko OHLC · Binance · Kraken · Coinbase
""")


# === ASIAN REVIEW ===
def asian_caption(data):
    return textwrap.dedent(f"""\
🏛 REDDINGTON · Азиатский обзор | {data.get('date_label', '')}

BTC: {fmt_price(data['btc']['price'])} · {arrow(data['btc'].get('change_24h'))} {fmt_change(data['btc'].get('change_24h'))}
ETH: {fmt_price(data['eth'].get('price', 0))} · {fmt_change(data['eth'].get('change_24h', 0))}

Азиатская сессия закрывается
""")


def asian_body(data):
    return textwrap.dedent(f"""\
🏛 REDDINGTON · Азиатский обзор · {data.get('date_label', '')}

{data.get('session_focus', '')}

📊 Рынок:
  ▪ BTC {fmt_price(data['btc']['price'])} · {fmt_change(data['btc'].get('change_24h', 0))}
  ▪ ETH {fmt_price(data['eth'].get('price', 0))} · {fmt_change(data['eth'].get('change_24h', 0))}
  ▪ Капитализация BTC {fmt_market_cap(data['btc'].get('market_cap'))}
  ▪ Доминация BTC {data.get('btc_dominance', '—')}

🟢 Лидеры роста 24ч:
{chr(10).join(f"  ▪ {m['symbol'].upper()} {fmt_change(m['change'])}" for m in data.get('gainers', [])[:5]) or '  ▪ —'}

🔴 Аутсайдеры 24ч:
{chr(10).join(f"  ▪ {m['symbol'].upper()} {fmt_change(m['change'])}" for m in data.get('losers', [])[:5]) or '  ▪ —'}

📰 Новости Азии:
{chr(10).join(f"  ▪ {n}" for n in data.get('asia_news', ['Нет значимых азиатских новостей']))}

💱 Потоки капитала:
{chr(10).join(f"  ▪ {f}" for f in data.get('flows', ['Данных о крупных потоках нет']))}

🔮 Фокус сессии:
{chr(10).join(f"  ▪ {f}" for f in data.get('forward_focus', ['Ждём триггера']))}

Источники: CoinGecko · Binance · Kraken · Coinbase
""")


# === WEEKLY PREVIEW ===
def weekly_caption(data):
    return textwrap.dedent(f"""\
🏛 REDDINGTON · Недельный обзор | {data.get('date_label', '')}

{data.get('theme', 'Рынок в боковике — ждём катализатора')}

🔑 Уровни BTC: {fmt_price(data['btc']['support'][0])}–{fmt_price(data['btc']['resistance'][1])}
""")


def weekly_body(data):
    btc = data["btc"]
    sectors = data.get("sectors", {})
    return textwrap.dedent(f"""\
🏛 REDDINGTON · Недельный обзор · {data.get('date_label', '')}

{data.get('theme', 'Неделя без явного нарратива')}

📅 Ключевые события недели:
{chr(10).join(f"  ▪ {e}" for e in data.get('week_events', [])) or '  ▪ Календарь спокойный'}

📊 Секторы:
{chr(10).join(f"  ▪ {s}: {fmt_change(sectors.get(s, {}).get('change', 0))}" for s in ['L1', 'L2', 'DeFi', 'AI', 'RWA', 'Memes', 'GameFi'])}

🎯 Топ-7 альтов на неделю:
{chr(10).join(f"  ▪ {a['symbol'].upper()} {fmt_change(a['change'])} — {a.get('reason', '')}" for a in data.get('top_alts', [])[:7]) or '  ▪ —'}

⚠️ Риски недели:
{chr(10).join(f"  ▪ {r}" for r in data.get('risks', ['Нет явных рисков']))}

📊 Источники: CoinGecko · Binance · Kraken · Coinbase
""")


# === ALERTS ===
def alert_text(data):
    return textwrap.dedent(f"""\
🚨 REDDINGTON · АЛЕРТ

{emoji}{ticker}: {fmt_price(price)} · {fmt_change(change_24h)}

{reason}

⏰ {timestamp}
""")


def fmt_data_summary(data):
    """Compact one-line summary used by caption."""
    btc = data.get("btc", {})
    return {
        "btc_price": fmt_price(btc.get("price")),
        "btc_change": fmt_change(btc.get("change_24h")),
        "btc_dominance": data.get("btc_dominance", "—"),
    }