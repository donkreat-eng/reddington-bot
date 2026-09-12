"""Chart generator — wraps the v8 TradingView-style chart."""
import sys
import os
import importlib.util
from pathlib import Path

CHART_SRC = Path(__file__).parent.parent / "channel-preview" / "make_v6.py"
LOGO_PATH = Path(__file__).parent.parent / "channel-preview" / "reddington_logo.jpg"

os.chdir(CHART_SRC.parent)

_spec = importlib.util.spec_from_file_location("make_v6", CHART_SRC)
make_v6 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(make_v6)


def generate_chart(ohlc, ticker, support_range, resistance_range, output_path):
    """Generate a TradingView-style chart for the given OHLC data."""
    make_v6.draw_tv_chart(ohlc, ticker, support_range, resistance_range, output_path)
    return output_path


if __name__ == "__main__":
    import json
    ohlc = json.loads(sys.argv[1])
    ticker = sys.argv[2]
    sup = tuple(json.loads(sys.argv[3]))
    res = tuple(json.loads(sys.argv[4]))
    out = sys.argv[5]
    print(generate_chart(ohlc, ticker, sup, res, out))
