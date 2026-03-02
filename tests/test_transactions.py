"""
Tests for transaction service.
"""
from decimal import Decimal

import pytest

from portfolio_tracker_v2.core import Database
from portfolio_tracker_v2.core.asset_resolver import AssetResolver
from portfolio_tracker_v2.core.exceptions import InvalidTransaction
from portfolio_tracker_v2.services.transaction_svc import TransactionService


@pytest.fixture
def setup_test_db():
    """Create and initialize in-memory test database."""
    db = Database(':memory:')
    db.init_schema()
    return db


@pytest.fixture
def transaction_svc(setup_test_db):
    """Create transaction service."""
    resolver = AssetResolver(setup_test_db)
    return TransactionService(setup_test_db, resolver)


@pytest.fixture
def db_connection(setup_test_db):
    """Get database connection."""
    return setup_test_db


def test_record_buy_simple(transaction_svc, db_connection):
    """Test recording a simple BUY transaction."""
    tx_id = transaction_svc.record_buy(
        symbol='BTC',
        account='Main',
        qty=Decimal('1'),
        unit_price=Decimal('50000'),
        fee_usd=Decimal('10'),
        tx_date='2020-01-01',
        notes='Test buy',
    )
    
    assert tx_id > 0
    
    conn = db_connection.connect()
    cursor = conn.cursor()
    cursor.execute("SELECT quantity, unit_price, fee_usd, total_usd FROM transactions WHERE id=?", (tx_id,))
    row = cursor.fetchone()
    
    qty, unit_price, fee_usd, total_usd = row
    assert Decimal(str(qty)) == Decimal('1')
    assert Decimal(str(unit_price)) == Decimal('50000')
    assert Decimal(str(fee_usd)) == Decimal('10')
    assert Decimal(str(total_usd)) == Decimal('50010')  # qty*price + fee


def test_record_sell_simple(transaction_svc, db_connection):
    """Test recording a SELL transaction (sufficient holdings)."""
    
    # First, add a BUY
    buy_tx_id = transaction_svc.record_buy(
        symbol='ETH',
        account='Main',
        qty=Decimal('10'),
        unit_price=Decimal('2000'),
        fee_usd=Decimal('5'),
        tx_date='2020-01-01',
    )
    
    # Now SELL 5
    sell_tx_id = transaction_svc.record_sell(
        symbol='ETH',
        account='Main',
        qty=Decimal('5'),
        unit_price=Decimal('3000'),
        fee_usd=Decimal('3'),
        tx_date='2020-01-02',
    )
    
    assert sell_tx_id > 0
    
    conn = db_connection.connect()
    cursor = conn.cursor()
    cursor.execute("SELECT quantity, unit_price, fee_usd, total_usd FROM transactions WHERE id=?", (sell_tx_id,))
    row = cursor.fetchone()
    
    qty, unit_price, fee_usd, total_usd = row
    assert Decimal(str(qty)) == Decimal('5')
    assert Decimal(str(unit_price)) == Decimal('3000')
    assert Decimal(str(fee_usd)) == Decimal('3')
    assert Decimal(str(total_usd)) == Decimal('14997')  # qty*price - fee


def test_fifo_matching_migration_and_buy(transaction_svc, db_connection):
    """
    Test FIFO matching with MIGRATION_BUY and BUY.
    
    - Insert MIGRATION_BUY 10 BTC @ $40k
    - Insert BUY 5 BTC @ $50k
    - Sell 12 BTC @ $60k
    
    Matches:
    - MIGRATION_BUY: 10 matched
    - BUY: 2 matched
    Result: 2 lot_matches entries
    """
    conn = db_connection.connect()
    cursor = conn.cursor()
    resolver = AssetResolver(db_connection)
    asset = resolver.resolve('BTC')
    account_id = transaction_svc._get_or_create_account('Main', cursor)
    conn.commit()
    
    # Manually insert MIGRATION_BUY (simulating import)
    cursor.execute('BEGIN')
    cursor.execute(
        """
        INSERT INTO transactions
        (asset_id, account_id, tx_type, quantity, unit_price, fee_usd, total_usd, tx_date, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (asset['id'], account_id, 'MIGRATION_BUY', 10.0, 40000.0, 0.0, 400000.0, '2020-01-01', 'migration'),
    )
    migration_tx_id = cursor.lastrowid
    conn.commit()
    
    # Record BUY
    buy_tx_id = transaction_svc.record_buy(
        symbol='BTC',
        account='Main',
        qty=Decimal('5'),
        unit_price=Decimal('50000'),
        fee_usd=Decimal('10'),
        tx_date='2020-01-02',
    )
    
    # Record SELL of 12
    sell_tx_id = transaction_svc.record_sell(
        symbol='BTC',
        account='Main',
        qty=Decimal('12'),
        unit_price=Decimal('60000'),
        fee_usd=Decimal('20'),
        tx_date='2020-01-03',
    )
    
    # Verify lot_matches
    conn = db_connection.connect()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT buy_tx_id, quantity, buy_fee_alloc, sell_fee_alloc
        FROM lot_matches
        ORDER BY id ASC
    """)
    matches = cursor.fetchall()
    
    assert len(matches) == 2
    
    # First match: MIGRATION_BUY (10 qty, 0 fee from migration)
    match1 = matches[0]
    assert match1[0] == migration_tx_id
    assert Decimal(str(match1[1])) == Decimal('10')
    assert Decimal(str(match1[2])) == Decimal('0')  # migration had 0 fee
    # allow small floating-rounding differences
    expected1 = Decimal('20') * (Decimal('10') / Decimal('12'))
    assert abs(Decimal(str(match1[3])) - expected1) < Decimal('0.0001')  # sell fee allocation tolerance
    
    # Second match: BUY (2 qty, partial)
    match2 = matches[1]
    assert match2[0] == buy_tx_id
    assert Decimal(str(match2[1])) == Decimal('2')
    assert Decimal(str(match2[2])) == Decimal('10') * (Decimal('2') / Decimal('5'))  # buy fee allocation
    expected2 = Decimal('20') * (Decimal('2') / Decimal('12'))
    assert abs(Decimal(str(match2[3])) - expected2) < Decimal('0.0001')  # sell fee allocation tolerance


def test_oversell_raises_error(transaction_svc):
    """Test that overselling raises InvalidTransaction."""
    
    # BUY 5 ETH
    transaction_svc.record_buy(
        symbol='ETH',
        account='Main',
        qty=Decimal('5'),
        unit_price=Decimal('2000'),
        fee_usd=Decimal('5'),
        tx_date='2020-01-01',
    )
    
    # Try to SELL 10 ETH (insufficient holdings)
    with pytest.raises(InvalidTransaction) as exc_info:
        transaction_svc.record_sell(
            symbol='ETH',
            account='Main',
            qty=Decimal('10'),
            unit_price=Decimal('3000'),
            fee_usd=Decimal('3'),
            tx_date='2020-01-02',
        )
    
    assert 'Insufficient holdings' in str(exc_info.value)


def test_invalid_qty_raises_error(transaction_svc):
    """Test that invalid quantity raises InvalidTransaction."""
    
    with pytest.raises(InvalidTransaction):
        transaction_svc.record_buy(
            symbol='BTC',
            account='Main',
            qty=Decimal('0'),  # Invalid
            unit_price=Decimal('50000'),
            fee_usd=Decimal('10'),
            tx_date='2020-01-01',
        )
    
    with pytest.raises(InvalidTransaction):
        transaction_svc.record_buy(
            symbol='BTC',
            account='Main',
            qty=Decimal('-5'),  # Invalid
            unit_price=Decimal('50000'),
            fee_usd=Decimal('10'),
            tx_date='2020-01-01',
        )


def test_negative_fee_raises_error(transaction_svc):
    """Test that negative fee raises InvalidTransaction."""
    
    with pytest.raises(InvalidTransaction):
        transaction_svc.record_buy(
            symbol='BTC',
            account='Main',
            qty=Decimal('1'),
            unit_price=Decimal('50000'),
            fee_usd=Decimal('-5'),  # Invalid
            tx_date='2020-01-01',
        )
