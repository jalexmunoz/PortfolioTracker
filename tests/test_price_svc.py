"""Tests for price_svc.py"""

from unittest.mock import MagicMock, patch

import pytest

from portfolio_tracker_v2 import config
from portfolio_tracker_v2.core import Database
from portfolio_tracker_v2.services.price_svc import get_crypto_price, refresh_prices


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
            price_updated_at TEXT
        )
    """
    )
    conn.commit()
    return db


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


def test_refresh_prices_crypto_stays_coingecko(db):
    conn = db.connect()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO assets (symbol, asset_type) VALUES ('BTC', 'crypto')")
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
    assert report.failed_lookup == 0
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
    cursor.execute("INSERT INTO assets (symbol, asset_type) VALUES ('AAPL', 'stock_us')")
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
    cursor.execute("INSERT INTO assets (symbol, asset_type) VALUES ('BRKB', 'stock_us')")
    conn.commit()

    with patch("portfolio_tracker_v2.services.price_svc.requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"Global Quote": {"05. price": "499.99"}}
        mock_get.return_value = mock_response

        report = refresh_prices(db)

    assert report.updated == 1
    assert report.results[0].provider_symbol == "BRK.B"


def test_refresh_prices_gold_alpha_vantage_success(db, monkeypatch):
    monkeypatch.setenv("ALPHA_VANTAGE_API_KEY", "demo-key")

    conn = db.connect()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO assets (symbol, asset_type) VALUES ('GOLD', 'commodity')")
    conn.commit()

    with patch("portfolio_tracker_v2.services.price_svc.requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "Realtime Currency Exchange Rate": {"5. Exchange Rate": "3088.10"}
        }
        mock_get.return_value = mock_response

        report = refresh_prices(db)

    assert report.updated == 1
    assert report.results[0].provider_symbol == "XAU"

    cursor.execute("SELECT current_price, price_source FROM assets WHERE symbol = 'GOLD'")
    row = cursor.fetchone()
    assert row[0] == 3088.10
    assert row[1] == "alpha_vantage_gold_silver_spot"


def test_refresh_prices_missing_alpha_key_is_clear_skip(db, monkeypatch):
    monkeypatch.delenv("ALPHA_VANTAGE_API_KEY", raising=False)
    monkeypatch.setattr(config, "ALPHA_VANTAGE_API_KEY", None)

    conn = db.connect()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO assets (symbol, asset_type) VALUES ('AAPL', 'stock_us')")
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
    cursor.execute("INSERT INTO assets (symbol, asset_type) VALUES ('AAPL', 'stock_us')")
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
    cursor.execute("INSERT INTO assets (symbol, asset_type) VALUES ('AAPL', 'stock_us')")
    conn.commit()

    with patch("portfolio_tracker_v2.services.price_svc.requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"Note": "limit reached"}
        mock_get.return_value = mock_response

        report = refresh_prices(db)

    assert report.updated == 0
    assert report.failed_lookup == 1
    assert report.results[0].status == "failed_lookup"
    assert report.results[0].reason == "rate_limited"


def test_refresh_prices_http_error_is_failed_lookup(db, monkeypatch):
    monkeypatch.setenv("ALPHA_VANTAGE_API_KEY", "demo-key")

    conn = db.connect()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO assets (symbol, asset_type) VALUES ('AAPL', 'stock_us')")
    conn.commit()

    with patch("portfolio_tracker_v2.services.price_svc.requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 503
        mock_response.json.return_value = {}
        mock_get.return_value = mock_response

        report = refresh_prices(db)

    assert report.updated == 0
    assert report.failed_lookup == 1
    assert report.results[0].status == "failed_lookup"
    assert report.results[0].reason == "http_503"


def test_refresh_prices_unknown_and_stock_intl_are_explicitly_unsupported(db):
    conn = db.connect()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO assets (symbol, asset_type) VALUES ('NEWASSET', 'unknown')")
    cursor.execute("INSERT INTO assets (symbol, asset_type) VALUES ('SAN', 'stock_intl')")
    conn.commit()

    report = refresh_prices(db)

    assert report.updated == 0
    assert report.skipped_unsupported == 2
    reasons = sorted(r.reason for r in report.results)
    assert reasons == ["unsupported_asset_type:stock_intl", "unsupported_asset_type:unknown"]
