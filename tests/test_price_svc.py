"""Tests for price_svc.py"""

from unittest.mock import MagicMock, patch

import pytest

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


def test_refresh_prices_explicit_mapping_success(db):
    conn = db.connect()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO assets (symbol, asset_type) VALUES ('BAS', 'UNKNOWN')")
    conn.commit()

    with patch("portfolio_tracker_v2.services.price_svc.requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"basis-markets": {"usd": 0.42}}
        mock_get.return_value = mock_response

        report = refresh_prices(db)

    assert report.updated == 1
    assert report.skipped_unmapped == 0
    assert report.skipped_unsupported == 0
    assert report.failed_lookup == 0
    assert report.results[0].status == "updated"
    assert report.results[0].provider_symbol == "basis-markets"

    cursor.execute("SELECT current_price, price_source, price_updated_at FROM assets WHERE symbol = 'BAS'")
    row = cursor.fetchone()
    assert row[0] == 0.42
    assert row[1] == "coingecko"
    assert row[2] is not None


def test_refresh_prices_unmapped_symbol_skipped(db):
    conn = db.connect()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO assets (symbol, asset_type) VALUES ('NEWCOIN', 'crypto')")
    conn.commit()

    with patch("portfolio_tracker_v2.services.price_svc.requests.get") as mock_get:
        report = refresh_prices(db)

    assert report.updated == 0
    assert report.skipped_unmapped == 1
    assert report.skipped_unsupported == 0
    assert report.failed_lookup == 0
    assert report.results[0].status == "skipped_unmapped"
    assert report.results[0].reason == "unmapped_symbol"
    mock_get.assert_not_called()


def test_refresh_prices_mapped_but_provider_unsupported(db):
    conn = db.connect()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO assets (symbol, asset_type) VALUES ('BTC', 'crypto')")
    conn.commit()

    with patch("portfolio_tracker_v2.services.price_svc.requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}
        mock_get.return_value = mock_response

        report = refresh_prices(db)

    assert report.updated == 0
    assert report.skipped_unmapped == 0
    assert report.skipped_unsupported == 1
    assert report.failed_lookup == 0
    assert report.results[0].status == "skipped_unsupported"
    assert report.results[0].reason == "provider_no_usd_price"


def test_refresh_prices_failed_lookup(db):
    conn = db.connect()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO assets (symbol, asset_type) VALUES ('BTC', 'crypto')")
    conn.commit()

    with patch("portfolio_tracker_v2.services.price_svc.requests.get") as mock_get:
        mock_get.side_effect = Exception("timeout")
        report = refresh_prices(db)

    assert report.updated == 0
    assert report.skipped_unmapped == 0
    assert report.skipped_unsupported == 0
    assert report.failed_lookup == 1
    assert report.results[0].status == "failed_lookup"
    assert report.results[0].reason == "request_exception"
