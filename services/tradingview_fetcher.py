"""Minimal TradingView fetcher for v2 without legacy Binance side effects."""

from __future__ import annotations

import logging

import pandas as pd

_tv_client = None
_tv_client_initialized = False


def _empty_ohlc_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])


def _get_tv_client():
    global _tv_client_initialized, _tv_client

    if _tv_client_initialized:
        return _tv_client

    _tv_client_initialized = True

    try:
        from tvDatafeed import TvDatafeed
    except ImportError as exc:
        raise ImportError("tvdatafeed missing") from exc

    try:
        _tv_client = TvDatafeed()
    except Exception as exc:
        logging.warning("TradingView client failed: %s", exc)
        _tv_client = None

    return _tv_client


def get_tradingview_ohlc(symbol: str, exchange: str = "BINANCE", interval=None, n_bars: int = 5000) -> pd.DataFrame:
    """Fetch OHLCV data from TradingView using a lazy client."""
    tv_client = _get_tv_client()
    if tv_client is None:
        return _empty_ohlc_frame()

    if interval is None:
        try:
            from tvDatafeed import Interval
        except ImportError as exc:
            raise ImportError("tvdatafeed missing") from exc
        interval = Interval.in_daily

    try:
        df = tv_client.get_hist(
            symbol=symbol,
            exchange=exchange,
            interval=interval,
            n_bars=n_bars,
        )
    except Exception as exc:
        logging.error("TradingView error for %s:%s: %s", exchange, symbol, exc)
        return _empty_ohlc_frame()

    if df is None or df.empty:
        return _empty_ohlc_frame()

    df.columns = df.columns.str.lower()
    required_cols = ["open", "high", "low", "close", "volume"]
    df = df[required_cols]

    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)

    return df
