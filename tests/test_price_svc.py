"""Tests for price_svc.py"""

import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from portfolio_tracker_v2 import config
from portfolio_tracker_v2.core import Database
from portfolio_tracker_v2.services.price_svc import (
    ProviderResolution,
    get_crypto_price,
    get_tradingview_stock_intl_price,
    refresh_prices,
)


@pytest.fixture
def db():
    """In-memory DB for testing."""
    db = Database(":memory:")
    conn = db.connect()
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE assets (
            id INTEGER PRIMARY KEY,
            symbol TEXT NOT NULL,
            asset_type TEXT NOT NULL,
            is_active INTEGER DEFAULT 1,
            current_price REAL,
            price_source TEXT,
            price_updated_at TEXT,
            tradingview_symbol TEXT,
            exchange TEXT,
            currency TEXT,
            divisor REAL DEFAULT 1.0,
            valuation_method TEXT DEFAULT 'market_live'
        )
    """
    )
    cursor.execute(
        """
        CREATE TABLE accounts (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE NOT NULL
        )
    """
    )
    cursor.execute(
        """
        CREATE TABLE transactions (
            id INTEGER PRIMARY KEY,
            asset_id INTEGER NOT NULL,
            account_id INTEGER NOT NULL,
            tx_type TEXT NOT NULL,
            quantity REAL NOT NULL,
            unit_price REAL,
            fee_usd REAL,
            total_usd REAL,
            tx_date TEXT
        )
    """
    )
    conn.commit()
    return db


def add_active_holding(db, symbol, asset_type, qty=1.0, is_active=1, valuation_method='market_live'):
    conn = db.connect()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO assets (symbol, asset_type, is_active, valuation_method) VALUES (?, ?, ?, ?)",
        (symbol, asset_type, is_active, valuation_method),
    )
    asset_id = cursor.lastrowid

    row = cursor.execute("SELECT id FROM accounts WHERE name = 'Main'").fetchone()
    if row:
        account_id = row[0]
    else:
        cursor.execute("INSERT INTO accounts (name) VALUES ('Main')")
        account_id = cursor.lastrowid

    cursor.execute(
        """
        INSERT INTO transactions (asset_id, account_id, tx_type, quantity, unit_price, fee_usd, total_usd, tx_date)
        VALUES (?, ?, 'BUY', ?, 1.0, 0.0, ?, '2026-01-01')
        """,
        (asset_id, account_id, qty, qty),
    )
    conn.commit()
    return asset_id




def test_get_crypto_price_known_symbol():
    with patch("portfolio_tracker_v2.services.price_svc.requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"bitcoin": {"usd": 50000.0}}
        mock_get.return_value = mock_response

        price = get_crypto_price("BTC")
        assert price == 50000.0
        mock_get.assert_called_once_with(
            "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd",
            timeout=10,
        )


def test_get_crypto_price_unknown_symbol():
    price = get_crypto_price("UNKNOWN")
    assert price is None


def test_get_crypto_price_api_failure():
    with patch("portfolio_tracker_v2.services.price_svc.requests.get") as mock_get:
        mock_get.side_effect = Exception("Network error")

        price = get_crypto_price("BTC")
        assert price is None


def test_get_tradingview_stock_intl_price_converts_cop_to_usd():
    resolution = ProviderResolution(
        status="ok",
        provider="tradingview",
        provider_symbol="PEI",
        price_source="tradingview_bvc_fx",
        exchange="BVC",
        currency="COP",
        divisor=1.0,
    )

    with patch("portfolio_tracker_v2.services.price_svc.get_tradingview_latest_close") as mock_close:
        mock_close.side_effect = [70800.0, 4000.0]
        lookup = get_tradingview_stock_intl_price(resolution)

    assert lookup.status == "ok"
    assert lookup.price == pytest.approx(17.7)


def test_get_tradingview_latest_close_uses_v2_shim_not_legacy_import():
    sys.modules.pop("data_fetcher", None)

    with patch("portfolio_tracker_v2.services.tradingview_fetcher.get_tradingview_ohlc") as mock_ohlc:
        mock_df = MagicMock()
        mock_df.empty = False
        mock_df.__getitem__.return_value.iloc.__getitem__.return_value = 70800.0
        mock_ohlc.return_value = mock_df

        from portfolio_tracker_v2.services.price_svc import get_tradingview_latest_close

        price = get_tradingview_latest_close("PEI", "BVC")

    assert price == 70800.0
    assert "data_fetcher" not in sys.modules


def test_get_tradingview_latest_close_retries_for_silver_empty_response():
    empty_df = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
    filled_df = pd.DataFrame([{"open": 33.1, "high": 33.4, "low": 32.9, "close": 33.2, "volume": 1.0}])

    with patch("portfolio_tracker_v2.services.tradingview_fetcher.get_tradingview_ohlc") as mock_ohlc:
        with patch("portfolio_tracker_v2.services.price_svc.time.sleep") as mock_sleep:
            mock_ohlc.side_effect = [empty_df, filled_df]

            from portfolio_tracker_v2.services.price_svc import get_tradingview_latest_close

            price = get_tradingview_latest_close("SILVER", "TVC")

    assert price == 33.2
    assert mock_ohlc.call_count == 2
    mock_sleep.assert_called_once()


def test_refresh_prices_crypto_stays_coingecko(db):
    conn = db.connect()
    cursor = conn.cursor()
    add_active_holding(db, 'BTC', 'crypto')
    conn.commit()

    with patch("portfolio_tracker_v2.services.price_svc.requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"bitcoin": {"usd": 60000.0}}
        mock_get.return_value = mock_response

        report = refresh_prices(db)

    assert report.updated == 1
    assert report.skipped_unmapped == 0
    assert report.skipped_unsupported == 0
    assert report.failed_final == 0
    assert report.results[0].provider == "coingecko"

    cursor.execute("SELECT current_price, price_source, price_updated_at FROM assets WHERE symbol = 'BTC'")
    row = cursor.fetchone()
    assert row[0] == 60000.0
    assert row[1] == "coingecko"
    assert row[2] is not None


def test_refresh_prices_stock_us_alpha_vantage_success(db, monkeypatch):
    monkeypatch.setenv("ALPHA_VANTAGE_API_KEY", "demo-key")

    conn = db.connect()
    cursor = conn.cursor()
    add_active_holding(db, 'AAPL', 'stock_us')
    conn.commit()

    with patch("portfolio_tracker_v2.services.price_svc.requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"Global Quote": {"05. price": "213.45"}}
        mock_get.return_value = mock_response

        report = refresh_prices(db)

    assert report.updated == 1
    assert report.results[0].provider == "alpha_vantage"
    assert report.results[0].provider_symbol == "AAPL"

    cursor.execute("SELECT current_price, price_source, price_updated_at FROM assets WHERE symbol = 'AAPL'")
    row = cursor.fetchone()
    assert row[0] == 213.45
    assert row[1] == "alpha_vantage_global_quote"
    assert row[2] is not None


def test_refresh_prices_stock_us_brk_b_mapping(db, monkeypatch):
    monkeypatch.setenv("ALPHA_VANTAGE_API_KEY", "demo-key")

    conn = db.connect()
    cursor = conn.cursor()
    add_active_holding(db, 'BRKB', 'stock_us')
    conn.commit()

    with patch("portfolio_tracker_v2.services.price_svc.requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"Global Quote": {"05. price": "499.99"}}
        mock_get.return_value = mock_response

        report = refresh_prices(db)

    assert report.updated == 1
    assert report.results[0].provider_symbol == "BRK.B"


def test_refresh_prices_stock_intl_ecopetrol_success_persists_metadata(db):
    conn = db.connect()
    cursor = conn.cursor()
    add_active_holding(db, 'ECOPETROL', 'stock_intl')
    conn.commit()

    with patch("portfolio_tracker_v2.services.price_svc.get_tradingview_latest_close") as mock_close:
        mock_close.side_effect = [1731.82, 4000.0]
        report = refresh_prices(db)

    assert report.updated == 1
    assert report.results[0].provider == "tradingview"
    assert report.results[0].provider_symbol == "ECOPETROL"

    cursor.execute(
        "SELECT current_price, price_source, price_updated_at, tradingview_symbol, exchange, currency, divisor FROM assets WHERE symbol = 'ECOPETROL'"
    )
    row = cursor.fetchone()
    assert row[0] == pytest.approx(0.432955)
    assert row[1] == "tradingview_bvc_fx"
    assert row[2] is not None
    assert row[3] == "ECOPETROL"
    assert row[4] == "BVC"
    assert row[5] == "COP"
    assert row[6] == 1.0


def test_refresh_prices_stock_intl_pei_success(db):
    conn = db.connect()
    cursor = conn.cursor()
    add_active_holding(db, 'PEI', 'stock_intl')
    conn.commit()

    with patch("portfolio_tracker_v2.services.price_svc.get_tradingview_latest_close") as mock_close:
        mock_close.side_effect = [70800.0, 4000.0]
        report = refresh_prices(db)

    assert report.updated == 1
    assert report.results[0].provider_symbol == "PEI"

    cursor.execute("SELECT current_price, price_source FROM assets WHERE symbol = 'PEI'")
    row = cursor.fetchone()
    assert row[0] == pytest.approx(17.7)
    assert row[1] == "tradingview_bvc_fx"


def test_refresh_prices_stock_intl_missing_tradingview_is_clear_skip(db):
    conn = db.connect()
    cursor = conn.cursor()
    add_active_holding(db, 'ECOPETROL', 'stock_intl')
    conn.commit()

    with patch("portfolio_tracker_v2.services.price_svc.get_tradingview_latest_close") as mock_close:
        mock_close.side_effect = ImportError("tvdatafeed missing")
        report = refresh_prices(db)

    assert report.updated == 0
    assert report.skipped_unsupported == 1
    assert report.results[0].status == "skipped_unsupported"
    assert report.results[0].reason == "provider_not_configured:tradingview"


def test_refresh_prices_gold_tradingview_success(db):
    conn = db.connect()
    cursor = conn.cursor()
    add_active_holding(db, 'GOLD', 'commodity')
    conn.commit()

    with patch("portfolio_tracker_v2.services.price_svc.get_tradingview_latest_close") as mock_close:
        mock_close.return_value = 3088.10
        report = refresh_prices(db)

    assert report.updated == 1
    assert report.results[0].provider == "tradingview"
    assert report.results[0].provider_symbol == "GOLD"

    cursor.execute("SELECT current_price, price_source, tradingview_symbol, exchange, currency FROM assets WHERE symbol = 'GOLD'")
    row = cursor.fetchone()
    assert row[0] == 3088.10
    assert row[1] == "tradingview_tvc_spot"
    assert row[2] == "GOLD"
    assert row[3] == "TVC"
    assert row[4] == "USD"


def test_refresh_prices_missing_alpha_key_is_clear_skip(db, monkeypatch):
    monkeypatch.delenv("ALPHA_VANTAGE_API_KEY", raising=False)
    monkeypatch.setattr(config, "ALPHA_VANTAGE_API_KEY", None)

    conn = db.connect()
    cursor = conn.cursor()
    add_active_holding(db, 'AAPL', 'stock_us')
    conn.commit()

    with patch("portfolio_tracker_v2.services.price_svc.requests.get") as mock_get:
        report = refresh_prices(db)

    assert report.updated == 0
    assert report.skipped_unsupported == 1
    assert report.results[0].status == "skipped_unsupported"
    assert report.results[0].reason == "provider_not_configured:alpha_vantage"
    mock_get.assert_not_called()


def test_refresh_prices_provider_no_price_is_skipped_unsupported(db, monkeypatch):
    monkeypatch.setenv("ALPHA_VANTAGE_API_KEY", "demo-key")

    conn = db.connect()
    cursor = conn.cursor()
    add_active_holding(db, 'AAPL', 'stock_us')
    conn.commit()

    with patch("portfolio_tracker_v2.services.price_svc.requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"Global Quote": {}}
        mock_get.return_value = mock_response

        report = refresh_prices(db)

    assert report.updated == 0
    assert report.skipped_unsupported == 1
    assert report.results[0].status == "skipped_unsupported"
    assert report.results[0].reason == "provider_no_price"


def test_refresh_prices_rate_limited_is_failed_lookup(db, monkeypatch):
    monkeypatch.setenv("ALPHA_VANTAGE_API_KEY", "demo-key")

    conn = db.connect()
    cursor = conn.cursor()
    add_active_holding(db, 'AAPL', 'stock_us')
    conn.commit()

    with patch("portfolio_tracker_v2.services.price_svc.requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"Note": "limit reached"}
        mock_get.return_value = mock_response

        report = refresh_prices(db)

    assert report.updated == 0
    assert report.failed_final == 1
    assert report.results[0].status == "failed_final"
    assert report.results[0].reason == "rate_limited"


def test_refresh_prices_http_error_is_failed_lookup(db, monkeypatch):
    monkeypatch.setenv("ALPHA_VANTAGE_API_KEY", "demo-key")

    conn = db.connect()
    cursor = conn.cursor()
    add_active_holding(db, 'AAPL', 'stock_us')
    conn.commit()

    with patch("portfolio_tracker_v2.services.price_svc.requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 503
        mock_response.json.return_value = {}
        mock_get.return_value = mock_response

        report = refresh_prices(db)

    assert report.updated == 0
    assert report.failed_final == 1
    assert report.results[0].status == "failed_final"
    assert report.results[0].reason == "http_503"


def test_refresh_prices_unknown_and_unsupported_stock_intl_symbol_are_explicit(db):
    conn = db.connect()
    cursor = conn.cursor()
    add_active_holding(db, 'NEWASSET', 'unknown')
    add_active_holding(db, 'SAN', 'stock_intl')
    conn.commit()

    report = refresh_prices(db)

    assert report.updated == 0
    assert report.skipped_unsupported == 2
    reasons = sorted(r.reason for r in report.results)
    assert reasons == ["unsupported_asset_type:stock_intl", "unsupported_asset_type:unknown"]




def test_refresh_prices_inactive_asset_is_ignored(db):
    conn = db.connect()
    cursor = conn.cursor()
    add_active_holding(db, 'BTC', 'crypto', is_active=0)
    conn.commit()

    with patch("portfolio_tracker_v2.services.price_svc.requests.get") as mock_get:
        report = refresh_prices(db)

    assert report.updated == 0
    assert report.skipped_unsupported == 0
    assert report.skipped_unmapped == 0
    assert report.failed_final == 0
    assert report.results == []
    mock_get.assert_not_called()
