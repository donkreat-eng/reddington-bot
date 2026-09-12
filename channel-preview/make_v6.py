"""Reddington chart v6 - TradingView-style chart with candles + volume + watermark + timeframe pill."""
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle, FancyBboxPatch
from PIL import Image
from datetime import datetime, timezone
import matplotlib.dates as mdates
import numpy as np

BG = '#0d0d0f'
GOLD = '#d4b878'
GOLD_BRIGHT = '#f0d896'
GOLD_FAINT = '#2a2418'
RED = '#e8625c'
RED_FILL = '#e8625c66'
GREEN = '#5cc890'
GREEN_FILL = '#5cc89066'
TEXT_DIM = '#6a5a3a'
TEXT = '#b8a878'
GRID = '#18161a'
WATERMARK = '#2a2418'

try:
    logo = Image.open('reddington_logo.jpg')
    w_l, h_l = logo.size
    r_crop = logo.crop((int(w_l*0.32), int(h_l*0.21), int(w_l*0.68), int(h_l*0.45)))
    r_resized = r_crop.resize((56, 56), Image.LANCZOS)
    r_arr = np.array(r_resized)
except (FileNotFoundError, IOError):
    r_arr = None


def to_datetime(ts_ms):
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)


def draw_candle(ax, t, o, h, l, c, width_days=0.18):
    bullish = c >= o
    color = GREEN if bullish else RED
    ax.plot([t, t], [l, h], color=color, linewidth=1.0, zorder=3, solid_capstyle='round')
    body_low = min(o, c)
    body_high = max(o, c)
    body_height = max(body_high - body_low, (h - l) * 0.005)
    t_num = mdates.date2num(t)
    rect = Rectangle(
        (t_num - width_days/2, body_low),
        width_days, body_height,
        facecolor=color, edgecolor=color, linewidth=0.8, zorder=4
    )
    ax.add_patch(rect)


def draw_tv_chart(ohlc, ticker, support_range, resistance_range, filename):
    times = [to_datetime(t) for t, *_ in ohlc]
    opens = [r[1] for r in ohlc]
    highs = [r[2] for r in ohlc]
    lows = [r[3] for r in ohlc]
    closes = [r[4] for r in ohlc]
    last_close = closes[-1]
    first_close = closes[0]
    last_open = opens[-1]
    change_pct = (last_close - first_close) / first_close * 100
    change_color = GREEN if change_pct >= 0 else RED
    arrow = 'UP' if change_pct >= 0 else 'DOWN'
    is_bullish = last_close >= last_open
    date_start = times[0]
    date_end = times[-1]
    fig = plt.figure(figsize=(14, 7.2), facecolor=BG)
    ax_price = fig.add_axes([0.04, 0.24, 0.88, 0.62])
    ax_vol = fig.add_axes([0.04, 0.10, 0.88, 0.14], sharex=ax_price)
    ax_tag = fig.add_axes([0.93, 0.24, 0.025, 0.62])
    ax_price.set_facecolor(BG)
    ax_vol.set_facecolor(BG)
    ax_tag.set_facecolor(BG)
    watermark_text = ticker.split('/')[0]
    ax_price.text(0.97, 0.55, watermark_text, transform=ax_price.transAxes,
                  fontsize=140, color=WATERMARK, alpha=1.0,
                  fontweight='bold', family='serif', ha='right', va='center', zorder=0)
    sup_lo, sup_hi = support_range
    res_lo, res_hi = resistance_range
    ax_price.axhspan(sup_lo, sup_hi, color=GREEN, alpha=0.10, zorder=1)
    ax_price.axhspan(res_lo, res_hi, color=RED, alpha=0.08, zorder=1)
    ax_price.axhline(sup_lo, color=GREEN, linewidth=0.6, alpha=0.5, linestyle=(0, (5, 4)), zorder=2)
    ax_price.axhline(sup_hi, color=GREEN, linewidth=0.6, alpha=0.5, linestyle=(0, (5, 4)), zorder=2)
    ax_price.axhline(res_lo, color=RED, linewidth=0.6, alpha=0.5, linestyle=(0, (5, 4)), zorder=2)
    ax_price.axhline(res_hi, color=RED, linewidth=0.6, alpha=0.5, linestyle=(0, (5, 4)), zorder=2)
    candle_width = (times[-1] - times[0]).total_seconds() / 86400 * 0.06
    for t, o, h, l, c in zip(times, opens, highs, lows, closes):
        draw_candle(ax_price, t, o, h, l, c, width_days=candle_width)
    np.random.seed(42)
    volumes = []
    for o, c, h, l in zip(opens, closes, highs, lows):
        base = (h - l) * 1000
        jitter = np.random.uniform(0.6, 1.4)
        volumes.append(base * jitter)
    max_vol = max(volumes)
    bar_colors = [GREEN if c >= o else RED for o, c in zip(opens, closes)]
    ax_vol.bar(times, volumes, width=candle_width*0.9, color=bar_colors, edgecolor='none', zorder=3)
    all_vals = lows + highs + list(support_range) + list(resistance_range)
    y_min = min(all_vals) * 0.998
    y_max = max(all_vals) * 1.002
    ax_price.set_ylim(y_min, y_max)
    ax_vol.set_ylim(0, max(volumes) * 1.05)
    ax_price.set_xlim(times[0], times[-1])
    ax_vol.set_xlim(times[0], times[-1])
    ax_price.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
    ax_price.tick_params(axis='x', labelbottom=False, length=0)
    ax_vol.tick_params(axis='x', colors=TEXT_DIM, labelsize=8, length=0, pad=4)
    ax_vol.tick_params(axis='y', colors=TEXT_DIM, labelsize=0, length=0, labelleft=False)
    ax_vol.set_yticklabels([])
    ax_price.grid(True, axis='y', color=GRID, linewidth=0.4, alpha=0.6)
    ax_price.grid(True, axis='x', color=GRID, linewidth=0.3, alpha=0.4)
    ax_price.set_axisbelow(True)
    ax_vol.grid(True, axis='x', color=GRID, linewidth=0.3, alpha=0.4)
    ax_vol.set_axisbelow(True)
    for spine in ['top', 'right', 'bottom', 'left']:
        ax_price.spines[spine].set_visible(False)
    for spine in ['top', 'right', 'bottom', 'left']:
        ax_vol.spines[spine].set_visible(False)
    ax_vol.xaxis.set_major_formatter(mdates.DateFormatter('%d %b %H:%M'))
    ax_vol.xaxis.set_major_locator(mdates.HourLocator(byhour=[0, 12]))
    ax_tag.set_xlim(0, 1)
    ax_tag.set_ylim(y_min, y_max)
    ax_tag.axis('off')
    tag_color = GREEN if is_bullish else RED
    bar_h = (y_max - y_min) * 0.012
    ax_tag.add_patch(Rectangle((0, last_close - bar_h/2), 1, bar_h,
                                facecolor=tag_color, edgecolor='none'))
    price_str = f'{last_close:,.0f}' if last_close >= 1000 else f'{last_close:,.2f}'
    ax_tag.text(0.5, last_close, price_str, transform=ax_tag.get_yaxis_transform(),
                fontsize=10, color='white', fontweight='bold', ha='center', va='center',
                bbox=dict(boxstyle='square,pad=0.4', facecolor=tag_color, edgecolor='none'), zorder=10)
    sup_mid = (sup_lo + sup_hi) / 2
    res_mid = (res_lo + res_hi) / 2
    ax_price.text(times[-1], sup_mid, f'  SUPPORT  ${sup_lo:,.0f}-{sup_hi:,.0f}  ',
                  fontsize=8, color=GREEN, va='center', ha='right', fontweight='bold', alpha=0.85)
    ax_price.text(times[-1], res_mid, f'  RESISTANCE  ${res_lo:,.0f}-{res_hi:,.0f}  ',
                  fontsize=8, color=RED, va='center', ha='right', fontweight='bold', alpha=0.85)
    fig.text(0.04, 0.955, 'REDDINGTON', fontsize=11, color=GOLD,
             fontweight='bold', family='serif')
    fig.text(0.135, 0.955, '·  PRIVATE CLUB', fontsize=9, color=TEXT_DIM, family='serif')
    fig.text(0.04, 0.91, ticker, fontsize=20, color=GOLD_BRIGHT, fontweight='bold')
    price_display = f'${last_close:,.0f}' if last_close >= 1000 else f'${last_close:,.2f}'
    fig.text(0.91, 0.94, price_display, fontsize=26, color=GOLD_BRIGHT,
             fontweight='bold', ha='right', va='center', family='serif')
    fig.text(0.91, 0.89, f'{arrow} {change_pct:+.2f}%  ·  7д',
             fontsize=11, color=change_color, ha='right', va='center')
    tf_y = 0.955
    tf_x_start = 0.62
    tfs = [('1H', False), ('4H', True), ('1D', False), ('1W', False)]
    for label, active in tfs:
        fig.text(tf_x_start, tf_y, label, fontsize=10,
                 color=GOLD_BRIGHT if active else TEXT_DIM,
                 fontweight='bold' if active else 'normal',
                 ha='center', va='center')
        tf_x_start += 0.028
    fig.text(0.04, 0.045, 'Источник: CoinGecko OHLC  ·  Reddington Analytics',
             fontsize=8, color=TEXT_DIM, ha='left', va='center')
    date_range_str = f'{date_start.strftime("%d %b")} – {date_end.strftime("%d %b %Y")}  ·  UTC'
    fig.text(0.96, 0.045, date_range_str, fontsize=8, color=TEXT_DIM, ha='right', va='center')
    fig_w_in, fig_h_in = fig.get_size_inches()
    dpi = fig.dpi
    margin_right = 16
    margin_bottom = 14
    if r_arr is not None:
        fig.figimage(r_arr,
                     xo=fig_w_in * dpi - r_resized.size[0] - margin_right,
                     yo=margin_bottom,
                     alpha=0.50, zorder=10)
    plt.savefig(filename, dpi=140, facecolor=BG)
    plt.close()
    print(f'saved {filename}')
