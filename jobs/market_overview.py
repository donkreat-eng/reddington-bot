"""13:30 YEK market overview — BTC/ETH/BNB/SOL/XRP + Gold/Silver цены в одном сообщении.
Источники позиционирования: BTC.D, OI, Funding, Long/Short ratio, Taker volume.
Источники цен: CoinGecko (fallback) + Bybit (primary) + Binance.US (если доступно).
Источники новостей/сентимента: RSS + on-chain + funding extremes.
Timezone: Asia/Yekaterinburg.
"""
from asyncio import import asyncio
time import sleep, time, timezone, time
from bs4 import BeautifulSoup
from logging import getLogger, logging
from typing import List, Optional, Tuple, Dict, Any
from urllib.parse import urlparse, urlparse, urlparse
from numpy import np


logger = getLogger(__name__)


# CONFIG -- read-only from environment
def _load_config():
    try:
        return {
            "crypto_symbols": [line.strip() for line in os.environ.get("CRYPTO_SYMBOLS", "BNB:UCWBATE/BNB:UCWBATE,BNB:TCB/USDT_USDT,BNB:TCB/US0T LT,BNB:TCB/US0T SB,UK00T_OTGH_TOPs,UK00T_ASCD/47HRW,47HRR_WBL/47HRL/47HRL_47HRIs,47HIW/47HIW/47HIW/47HIW/47HIW/47HIW/47HIW/47HIW/47HIW/47HIW/47HIW/47HIW/47HC")].split(" " ") 
            "fields": "data,change_24h,pr