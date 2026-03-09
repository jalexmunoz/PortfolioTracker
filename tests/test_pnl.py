"""
Tests for PnL service.
"""
from decimal import Decimal

import pytest

from portfolio_tracker_v2.core import Database
from portfolio_tracker_v2.core.asset_resolver import AssetResolver
from portfolio_tracker_v2.services.transaction_svc import TransactionService
from portfolio_tracker_v2.services.pnl_svc import PnLService


@pytest.fixture
def setup_test_db():
    """Create and initialize in-memory test database."""
    db = Database(':memory:')
    db.init_schema()
    return db


@pytest.fixture
def services(setup_test_db):
    """Create both services."""
    resolver = AssetResolver(setup_test_db)
    tx_svc = TransactionService(setup_test_db, resolver)
    pnl_svc = PnLService(setup_test_db, resolver)
    return tx_svc, pnl_svc, setup_test_db


def test_open_position_qty_buy_only(services):
    """Test open position quantity with only buys (no sells)."""
    tx_svc, pnl_svc, _ = services
    
    tx_svc.record_buy(
        symbol='BTC',
        account='Main',
        qty=Decimal('5'),
        unit_price=Decimal('40000'),
        fee_usd=Decimal('10'),
        tx_date='2020-01-01',
    )
    
    tx_svc.record_buy(
        symbol='BTC',
        account='Main',
        qty=Decimal('3'),
        unit_price=Decimal('50000'),
        fee_usd=Decimal('5'),
        tx_date='2020-01-02',
    )
    
    qty = pnl_svc.open_position_qty('BTC')
    assert qty == Decimal('8')


def test_open_position_qty_with_sells(services):
    """Test open position after buy and partial sell."""
    tx_svc, pnl_svc, _ = services
    
    tx_svc.record_buy(
        symbol='ETH',
        account='Main',
        qty=Decimal('10'),
        unit_price=Decimal('2000'),
        fee_usd=Decimal('5'),
        tx_date='2020-01-01',
    )
    
    tx_svc.record_sell(
        symbol='ETH',
        account='Main',
        qty=Decimal('3'),
        unit_price=Decimal('3000'),
        fee_usd=Decimal('3'),
        tx_date='2020-01-02',
    )
    
    qty = pnl_svc.open_position_qty('ETH')
    assert qty == Decimal('7')  # 10 - 3


def test_open_position_qty_account_filter(services):
    """Test open position with account filtering."""
    tx_svc, pnl_svc, _ = services
    
    # Account 1: 5 BTC
    tx_svc.record_buy(
        symbol='BTC',
        account='Account1',
        qty=Decimal('5'),
        unit_price=Decimal('40000'),
        fee_usd=Decimal('10'),
        tx_date='2020-01-01',
    )
    
    # Account 2: 3 BTC
    tx_svc.record_buy(
        symbol='BTC',
        account='Account2',
        qty=Decimal('3'),
        unit_price=Decimal('50000'),
        fee_usd=Decimal('5'),
        tx_date='2020-01-02',
    )
    
    qty1 = pnl_svc.open_position_qty('BTC', account='Account1')
    qty2 = pnl_svc.open_position_qty('BTC', account='Account2')
    
    assert qty1 == Decimal('5')
    assert qty2 == Decimal('3')


def test_realized_pnl_simple(services):
    """
    Test realized PnL calculation.
    
    BUY: 1 BTC @ $40k + $10 fee = $40,010 total cost
    SELL: 1 BTC @ $60k - $3 fee = $59,997 revenue
    P&L = $59,997 - $40,010 = $19,987
    """
    tx_svc, pnl_svc, db = services
    
    tx_svc.record_buy(
        symbol='BTC',
        account='Main',
        qty=Decimal('1'),
        unit_price=Decimal('40000'),
        fee_usd=Decimal('10'),
        tx_date='2020-01-01',
    )
    
    tx_svc.record_sell(
        symbol='BTC',
        account='Main',
        qty=Decimal('1'),
        unit_price=Decimal('60000'),
        fee_usd=Decimal('3'),
        tx_date='2020-01-02',
    )
    
    pnl = pnl_svc.realized_pnl(symbol='BTC')
    
    # (60000 * 1 - 3) - (40000 * 1 + 10) = 59997 - 40010 = 19987
    expected = Decimal('19987')
    assert pnl == expected


def test_realized_pnl_partial_sell(services):
    """
    Test realized PnL with partial sell.
    
    BUY 10 @ $1k + $5 fee
    SELL 4 @ $2k - $2 fee
    
    Matched qty: 4
    Buy cost for matched: 10 * $1k + (5 * 4/10) = $10k + $2
    Sell revenue for matched: 4 * $2k - (2 * 4/4) = $8k - $2
    P&L = ($8k - $2) - ($10k + $2) = $8k - $2 - $10k - $2 = -$2,004
    """
    tx_svc, pnl_svc, _ = services
    
    tx_svc.record_buy(
        symbol='ETH',
        account='Main',
        qty=Decimal('10'),
        unit_price=Decimal('1000'),
        fee_usd=Decimal('5'),
        tx_date='2020-01-01',
    )
    
    tx_svc.record_sell(
        symbol='ETH',
        account='Main',
        qty=Decimal('4'),
        unit_price=Decimal('2000'),
        fee_usd=Decimal('2'),
        tx_date='2020-01-02',
    )
    
    pnl = pnl_svc.realized_pnl(symbol='ETH')
    
    # Revenue: 4 * 2000 - 2 = 7998
    # Cost for matched lot: 4 * 1000 + 5*(4/10) = 4002
    # P&L = 7998 - 4002 = 3996
    expected = Decimal('3996')
    assert pnl == expected


def test_realized_pnl_multiple_matches(services):
    """
    Test realized PnL with multiple BUY->SELL matches (FIFO).
    
    BUY1: 5 @ $1k (migration)
    BUY2: 5 @ $1.2k
    SELL: 8 @ $1.5k
    
    Matches:
    - BUY1: 5 matched @ 5/5 allocation
    - BUY2: 3 matched @ 3/5 allocation
    
    P&L = (8*1.5) - (5*1k + 5*1.2k + 3/5*fee_alloc)
    """
    tx_svc, pnl_svc, db = services
    conn = db.connect()
    cursor = conn.cursor()
    resolver = AssetResolver(db)
    asset = resolver.resolve('ADA')
    account_id = tx_svc._get_or_create_account('Main', cursor)
    conn.commit()
    
    # Manual insert of MIGRATION_BUY (5 @ $1k, 0 fee)
    cursor.execute('BEGIN')
    cursor.execute(
        """
        INSERT INTO transactions
        (asset_id, account_id, tx_type, quantity, unit_price, fee_usd, total_usd, tx_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (asset['id'], account_id, 'MIGRATION_BUY', 5.0, 1000.0, 0.0, 5000.0, '2020-01-01'),
    )
    conn.commit()
    
    # BUY2: 5 @ $1.2k + $10 fee
    tx_svc.record_buy(
        symbol='ADA',
        account='Main',
        qty=Decimal('5'),
        unit_price=Decimal('1200'),
        fee_usd=Decimal('10'),
        tx_date='2020-01-02',
    )
    
    # SELL: 8 @ $1.5k - $5 fee
    tx_svc.record_sell(
        symbol='ADA',
        account='Main',
        qty=Decimal('8'),
        unit_price=Decimal('1500'),
        fee_usd=Decimal('5'),
        tx_date='2020-01-03',
    )
    
    pnl = pnl_svc.realized_pnl(symbol='ADA')
    
    # Revenue: 8 * 1500 - 5
    revenue = Decimal('8') * Decimal('1500') - Decimal('5')
    # Cost: first match 5*1000 + 0
    cost1 = Decimal('5') * Decimal('1000')
    # second match 3*1200 + fee allocation 10*(3/5)
    cost2 = Decimal('3') * Decimal('1200') + Decimal('10') * (Decimal('3') / Decimal('5'))
    expected = revenue - (cost1 + cost2)
    assert pnl == expected


def test_realized_pnl_no_sales(services):
    """Test realized PnL with no sales (should be 0)."""
    tx_svc, pnl_svc, _ = services
    
    tx_svc.record_buy(
        symbol='BTC',
        account='Main',
        qty=Decimal('1'),
        unit_price=Decimal('40000'),
        fee_usd=Decimal('10'),
        tx_date='2020-01-01',
    )
    
    pnl = pnl_svc.realized_pnl(symbol='BTC')
    assert pnl == Decimal('0')


def test_realized_pnl_by_account(services):
    """Test realized PnL filtered by account."""
    tx_svc, pnl_svc, _ = services
    
    # Account 1: BUY 1 @ 40k, SELL 1 @ 60k
    tx_svc.record_buy(
        symbol='BTC',
        account='Account1',
        qty=Decimal('1'),
        unit_price=Decimal('40000'),
        fee_usd=Decimal('10'),
        tx_date='2020-01-01',
    )
    
    tx_svc.record_sell(
        symbol='BTC',
        account='Account1',
        qty=Decimal('1'),
        unit_price=Decimal('60000'),
        fee_usd=Decimal('3'),
        tx_date='2020-01-02',
    )
    
    # Account 2: BUY 1 @ 50k, SELL 1 @ 55k
    tx_svc.record_buy(
        symbol='BTC',
        account='Account2',
        qty=Decimal('1'),
        unit_price=Decimal('50000'),
        fee_usd=Decimal('5'),
        tx_date='2020-01-01',
    )
    
    tx_svc.record_sell(
        symbol='BTC',
        account='Account2',
        qty=Decimal('1'),
        unit_price=Decimal('55000'),
        fee_usd=Decimal('2'),
        tx_date='2020-01-02',
    )
    
    pnl1 = pnl_svc.realized_pnl(symbol='BTC', account='Account1')
    pnl2 = pnl_svc.realized_pnl(symbol='BTC', account='Account2')
    
    # Account1: (60000 - 3) - (40000 + 10) = 19987
    # Account2: (55000 - 2) - (50000 + 5) = 4993
    assert pnl1 == Decimal('19987')
    assert pnl2 == Decimal('4993')


def test_positions_cost_basis_and_avg(services):
    """Test positions() returns qty_open, cost_basis and avg_cost correctly."""
    tx_svc, pnl_svc, db = services
    conn = db.connect()
    cursor = conn.cursor()
    resolver = AssetResolver(db)
    asset = resolver.resolve('XRP')
    acct_id = tx_svc._get_or_create_account('Main', cursor)
    conn.commit()

    # MIGRATION_BUY 10 @ $1
    cursor.execute('BEGIN')
    cursor.execute(
        """
        INSERT INTO transactions (asset_id, account_id, tx_type, quantity, unit_price, fee_usd, total_usd, tx_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (asset['id'], acct_id, 'MIGRATION_BUY', 10.0, 1.0, 0.0, 10.0, '2020-01-01')
    )
    conn.commit()

    # BUY 5 @ $2 + $1 fee
    tx_svc.record_buy(symbol='XRP', account='Main', qty=Decimal('5'), unit_price=Decimal('2'), fee_usd=Decimal('1'), tx_date='2020-01-02')

    # SELL 11 (FIFO: matches 10 from MIGRATION, 1 from BUY -> remaining M:0, B:4)
    tx_svc.record_sell(symbol='XRP', account='Main', qty=Decimal('11'), unit_price=Decimal('3'), fee_usd=Decimal('2'), tx_date='2020-01-03')

    positions = pnl_svc.positions(account='Main')
    # find XRP position
    xrp = [p for p in positions if p['symbol'] == 'XRP'][0]
    # qty_open = (10 + 5) - 11 = 4
    assert xrp['qty_open'] == Decimal('4')
    # cost basis should be remaining from buys:
    # MIGRATION: matched 10, remaining=0, cost = 0
    # BUY: matched 1, remaining=4, cost = 4*2 + 1*(4/5) = 8 + 0.8 = 8.8
    # total cost_basis = 8.8
    assert abs(xrp['cost_basis'] - Decimal('8.8')) < Decimal('0.0001')
    assert xrp['avg_cost'] == xrp['cost_basis'] / xrp['qty_open']


def test_cash_balance_and_summary(services):
    """Test cash balance using __USD_CASH__ and overall summary."""
    tx_svc, pnl_svc, db = services
    conn = db.connect()
    cursor = conn.cursor()
    resolver = AssetResolver(db)
    usd = resolver.get_or_create_usd_cash()
    acct_id = tx_svc._get_or_create_account('Main', cursor)
    conn.commit()

    # DEPOSIT -> increase cash (simulate by inserting a transaction on USD_CASH)
    cursor.execute('BEGIN')
    cursor.execute(
        "INSERT INTO transactions (asset_id, account_id, tx_type, quantity, unit_price, fee_usd, total_usd, tx_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (usd['id'], acct_id, 'DEPOSIT', 1.0, 1.0, 0.0, 1000.0, '2020-01-01')
    )
    # FEE -> reduce cash
    cursor.execute(
        "INSERT INTO transactions (asset_id, account_id, tx_type, quantity, unit_price, fee_usd, total_usd, tx_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (usd['id'], acct_id, 'FEE', 1.0, 1.0, 0.0, -10.0, '2020-01-02')
    )
    conn.commit()

    cash = pnl_svc.cash_balance(account='Main')
    assert cash == Decimal('990')

    summary = pnl_svc.summary(account='Main')
    assert summary['cash_balance'] == Decimal('990')


def test_positions_unrealized_gain_alerts(services):
    """Test unrealized gain alerts for positions."""
    tx_svc, pnl_svc, db = services
    conn = db.connect()
    cursor = conn.cursor()
    resolver = AssetResolver(db)

    # Buy 1 BTC @ $100
    tx_svc.record_buy(symbol='BTC', account='Main', qty=Decimal('1'), unit_price=Decimal('100'), fee_usd=Decimal('0'), tx_date='2020-01-01')
    # Set current_price to $100
    btc_asset = resolver.resolve('BTC')
    cursor.execute("UPDATE assets SET current_price = ?, price_updated_at = ? WHERE id = ?", (100.0, '2020-01-01', btc_asset['id']))
    conn.commit()

    positions = pnl_svc.positions(account='Main')
    btc = [p for p in positions if p['symbol'] == 'BTC'][0]
    assert btc['current_price'] == Decimal('100')
    assert btc['unrealized_pct'] == Decimal('0')
    assert btc['alert'] == ""  # 0% < 30%

    # Update current_price to $200
    cursor.execute("UPDATE assets SET current_price = ?, price_updated_at = ? WHERE id = ?", (200.0, '2020-01-02', btc_asset['id']))
    conn.commit()

    positions = pnl_svc.positions(account='Main')
    btc = [p for p in positions if p['symbol'] == 'BTC'][0]
    # cost_basis = 100
    # market_value = 1 * 200 = 200
    # unrealized = 100
    # pct = 100 / 100 * 100 = 100%
    assert btc['current_price'] == Decimal('200')
    assert btc['unrealized_pct'] == Decimal('100')
    assert btc['alert'] == "YES"  # >30%

    # Test >30% for ETH
    tx_svc.record_buy(symbol='ETH', account='Main', qty=Decimal('1'), unit_price=Decimal('100'), fee_usd=Decimal('0'), tx_date='2020-01-01')
    eth_asset = resolver.resolve('ETH')
    cursor.execute("UPDATE assets SET current_price = ?, price_updated_at = ? WHERE id = ?", (160.0, '2020-01-01', eth_asset['id']))
    conn.commit()

    positions = pnl_svc.positions(account='Main')
    eth = [p for p in positions if p['symbol'] == 'ETH'][0]
    # cost_basis = 100
    # mv = 160
    # pnl = 60
    # pct = 60%
    assert eth['unrealized_pct'] == Decimal('60')
    assert eth['alert'] == "YES"

    # Test <30% for ADA
    tx_svc.record_buy(symbol='ADA', account='Main', qty=Decimal('1'), unit_price=Decimal('100'), fee_usd=Decimal('0'), tx_date='2020-01-01')
    ada_asset = resolver.resolve('ADA')
    cursor.execute("UPDATE assets SET current_price = ?, price_updated_at = ? WHERE id = ?", (125.0, '2020-01-01', ada_asset['id']))
    conn.commit()

    positions = pnl_svc.positions(account='Main')
    ada = [p for p in positions if p['symbol'] == 'ADA'][0]
    # pct = 25%
    assert ada['unrealized_pct'] == Decimal('25')
    assert ada['alert'] == ""


def test_summary_with_valuation(services):
    """Test summary with valuation using usable prices."""
    tx_svc, pnl_svc, db = services
    conn = db.connect()
    cursor = conn.cursor()
    resolver = AssetResolver(db)

    # Buy BTC @ $100, cost_basis = 100
    tx_svc.record_buy(symbol='BTC', account='Main', qty=Decimal('1'), unit_price=Decimal('100'), fee_usd=Decimal('0'), tx_date='2020-01-01')
    btc_asset = resolver.resolve('BTC')
    # Set usable price: $200, updated recently
    from datetime import datetime
    recent_date = datetime.now().isoformat()
    cursor.execute("UPDATE assets SET current_price = ?, price_source = ?, price_updated_at = ? WHERE id = ?", (200.0, 'coingecko', recent_date, btc_asset['id']))
    conn.commit()

    # Buy ETH @ $100, cost_basis = 100
    tx_svc.record_buy(symbol='ETH', account='Main', qty=Decimal('1'), unit_price=Decimal('100'), fee_usd=Decimal('0'), tx_date='2020-01-01')
    eth_asset = resolver.resolve('ETH')
    # Set stale price: from csv_bootstrap
    cursor.execute("UPDATE assets SET current_price = ?, price_source = ?, price_updated_at = ? WHERE id = ?", (150.0, 'csv_bootstrap', '2020-01-01', eth_asset['id']))
    conn.commit()

    # Buy ADA @ $100, cost_basis = 100
    tx_svc.record_buy(symbol='ADA', account='Main', qty=Decimal('1'), unit_price=Decimal('100'), fee_usd=Decimal('0'), tx_date='2020-01-01')
    ada_asset = resolver.resolve('ADA')
    # Set unavailable: no current_price
    cursor.execute("UPDATE assets SET current_price = NULL, price_updated_at = NULL WHERE id = ?", (ada_asset['id'],))
    conn.commit()

    s = pnl_svc.summary(account='Main')
    assert s['total_cost_basis'] == Decimal('300')
    assert s['total_realized_pnl'] == Decimal('0')
    assert s['cash_balance'] == Decimal('0')
    # Only BTC is usable: market_value = 1 * 200 = 200, unrealized = 200 - 100 = 100
    assert s['total_market_value'] == Decimal('200')
    assert s['total_unrealized_pnl'] == Decimal('100')
    assert s['unrealized_return_pct'] == Decimal('33.33')  # 100 / 300 * 100
    assert s['price_quality_counts'] == {'usable': 1, 'stale': 1, 'unavailable': 1}


def test_price_quality_classification(services):
    """Test price quality classification."""
    _, pnl_svc, db = services
    conn = db.connect()
    cursor = conn.cursor()
    resolver = AssetResolver(db)

    # Create assets
    btc_asset = resolver.resolve('BTC')
    eth_asset = resolver.resolve('ETH')
    ada_asset = resolver.resolve('ADA')

    # Usable: price set, source not csv_bootstrap, recent
    from datetime import datetime
    recent_date = datetime.now().isoformat()
    cursor.execute("UPDATE assets SET current_price = ?, price_source = ?, price_updated_at = ? WHERE id = ?", (100.0, 'coingecko', recent_date, btc_asset['id']))
    # Stale: csv_bootstrap
    cursor.execute("UPDATE assets SET current_price = ?, price_source = ?, price_updated_at = ? WHERE id = ?", (100.0, 'csv_bootstrap', recent_date, eth_asset['id']))
    # Unavailable: no price
    cursor.execute("UPDATE assets SET current_price = NULL WHERE id = ?", (ada_asset['id'],))
    conn.commit()

    assert pnl_svc._classify_price_quality(btc_asset['id']) == 'usable'
    assert pnl_svc._classify_price_quality(eth_asset['id']) == 'stale'
    assert pnl_svc._classify_price_quality(ada_asset['id']) == 'unavailable'
