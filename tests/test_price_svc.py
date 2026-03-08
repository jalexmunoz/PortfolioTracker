"""
Tests for price_svc.py
"""
import pytest
from unittest.mock import patch, MagicMock
from portfolio_tracker_v2.services.price_svc import refresh_prices, get_crypto_price
from portfolio_tracker_v2.core import Database


@pytest.fixture
def db():
    """In-memory DB for testing."""
    db = Database(":memory:")
    conn = db.connect()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE assets (
            id INTEGER PRIMARY KEY,
            symbol TEXT NOT NULL,
            asset_type TEXT NOT NULL,
            is_active INTEGER DEFAULT 1,
            current_price REAL,
            price_source TEXT,
            price_updated_at TEXT
        )
    """)
    conn.commit()
    return db


def test_get_crypto_price_known_symbol():
    """Test getting price for known symbol."""
    with patch('portfolio_tracker_v2.services.price_svc.requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'bitcoin': {'usd': 50000.0}}
        mock_get.return_value = mock_response
        
        price = get_crypto_price('BTC')
        assert price == 50000.0
        mock_get.assert_called_once_with('https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd', timeout=10)


def test_get_crypto_price_unknown_symbol():
    """Test getting price for unknown symbol returns None."""
    price = get_crypto_price('UNKNOWN')
    assert price is None


def test_get_crypto_price_api_failure():
    """Test API failure returns None."""
    with patch('portfolio_tracker_v2.services.price_svc.requests.get') as mock_get:
        mock_get.side_effect = Exception("Network error")
        
        price = get_crypto_price('BTC')
        assert price is None


def test_refresh_prices_crypto(db):
    """Test refreshing prices for crypto assets."""
    conn = db.connect()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO assets (symbol, asset_type) VALUES ('BTC', 'crypto'), ('AAPL', 'stock')")
    conn.commit()
    
    with patch('portfolio_tracker_v2.services.price_svc.get_crypto_price') as mock_get_price:
        mock_get_price.return_value = 50000.0
        
        updated, skipped = refresh_prices(db)
        assert updated == 1
        assert skipped == 1
        
        cursor.execute("SELECT current_price, price_source, price_updated_at FROM assets WHERE symbol = 'BTC'")
        row = cursor.fetchone()
        assert row[0] == 50000.0
        assert row[1] == 'coingecko'
        assert row[2] is not None  # timestamp set


def test_refresh_prices_no_price(db):
    """Test skipping when price fetch fails."""
    conn = db.connect()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO assets (symbol, asset_type) VALUES ('BTC', 'crypto')")
    conn.commit()
    
    with patch('portfolio_tracker_v2.services.price_svc.get_crypto_price') as mock_get_price:
        mock_get_price.return_value = None
        
        updated, skipped = refresh_prices(db)
        assert updated == 0
        assert skipped == 1