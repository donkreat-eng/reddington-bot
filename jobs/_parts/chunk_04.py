 None:
        return "n/a"
    sign = "+" if f >= 0 else ""
    return f"{sign}{f:.3f}%"


def fmt_ls(ls: float | None) -> str:
    if ls is None:
        return "n/a"
    pct = ls / (1 + ls) * 100
    return f"{pct:.0f}% лонг"


def adaptive_zones(price: float) -> tuple[float, float]:
    if price >= 1000:
        sup = round(price * 0.97, 2)
        res = round(price * 1.03, 2)
    elif price >= 10:
        sup = round(price * 0.95, 2)
        res = round(price * 1.05, 2)
    elif price >= 1:
        sup = round(price * 0.93, 2)
        res = round(price * 1.07, 2)
    else:
        sup = round(price * 0.88, 4)
        res = round(price * 1.12, 4)
    return sup, res


def build_top5_chart(ohlc_by_ticker: dict, out_path: Path) -> None:
    """Render BTC chart — main asset for top5 overview."""
    btc_ohlc = ohlc_by_ticker["BTC"]
    sup, res = adaptive_zones(ohlc_by_ticker["_btc_price"])
    chartlib.generate_chart(
        btc_ohlc, "BTC/USDT",
        (sup * 0.998, sup * 1.005),
        (res * 0.995, res * 1.002),
        str(out_path),
    )


def build_caption(prices: dict, changes: dict, ts: datetime) -> str:
    parts = ["🏛 <b>REDDINGTON · Топ-5 + Металлы</b>",
             f"_{ts.strftime('%d %b %Y · %H:%M')} YEKT_",
             ""]
    parts.append("📊 <b>Крипто (24ч)</b>")
    for t in CRYPTO_TICKERS:
        p = prices[t]["price"]
        ch = changes[t]["change_24h"]
        arrow = "🟢" if ch >= 0 else "🔴"
        parts.append(f"  {arrow} <b>{t}</b> · ${fmt_price(p)} · {fmt_change(ch)}")
    parts.append("")
    parts.append("🥇 <b>Металлы</b>")
    parts.append(f"  🟡 <b>Gold</b> · ${fmt_price(prices['Gold']['price'])} · "
                 f"{fmt_change(prices['Gold']['change_pct'])}")
    parts.append(f"  ⚪ <b>Silver</b> · ${fmt_price(prices['Silver']['price'])} · "
                 f"{fmt_change(prices['Silver']['change_pct'])}")
    parts.append("")
    parts.append("🎯 Зоны BTC (адаптивные)")
    sup, res = adaptive_zones(prices["BTC"]["price"])
    parts.append(f"  Поддержка: ${fmt_price(sup)}")
    parts.append(f"  Сопротивление: ${fmt_price(res)}")
    return "\n".join(parts)


def build_body(prices: dict, changes: dict, positional: dict,
               fng_value: int | None, ts: datetime) -> str:
    lines = ["🏛 <b>REDDINGTON · Обзор рынка</b>",
             f"<i>{ts.strftime('%A, %d %B %Y · %H:%M')} YEKT</i>",
             ""]

    lines.append("📈 <b>Сводка по активам</b>")
    lines.append("<pre>")
    lines.append(f"{'Актив':<8} {'Цена':>11} {'24ч':>7} {'OI':>9} {'Funding':>9} {'L/S':>8}")
    lines.append("─" * 60)
    for t in CRYPTO_TICKERS:
        p = prices[t]["price"]
        ch = changes[t]["change_24h"]
        pos = positional.get(t, {})
        arrow = "▲" if ch >= 0 else "▼"
        lines.append(
            f"{t:<8} ${p:>10,.2f} {arrow}{abs(ch):>5.2f}% "
            
