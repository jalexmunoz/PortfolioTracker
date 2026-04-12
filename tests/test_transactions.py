"""
Tests for transaction service.
"""
from decimal import Decimal

import pytest

from portfolio_tracker_v2.core import Database
from portfolio_tracker_v2.core.asset_resolver import AssetResolver
from portfolio_tracker_v2.core.exceptions import InvalidTransaction
from portfolio_tracker_v2.services.pnl_svc import PnLService
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


def test_list_transactions_includes_realized_fields_for_sell_single_lot(transaction_svc):
    transaction_svc.record_buy(
        symbol='BTC',
        account='Main',
        qty=Decimal('1'),
        unit_price=Decimal('100'),
        fee_usd=Decimal('0'),
        tx_date='2026-03-01',
    )
    transaction_svc.record_sell(
        symbol='BTC',
        account='Main',
        qty=Decimal('1'),
        unit_price=Decimal('120'),
        fee_usd=Decimal('0'),
        tx_date='2026-03-02',
    )

    rows = transaction_svc.list_transactions(account='Main', symbol='BTC', limit=10)
    sell_row = next(r for r in rows if r['side'] == 'SELL')

    assert Decimal(str(sell_row['gross_proceeds'])) == Decimal('120')
    assert Decimal(str(sell_row['matched_cost_basis'])) == Decimal('100')
    assert Decimal(str(sell_row['realized_pnl'])) == Decimal('20')


def test_list_transactions_includes_realized_fields_for_sell_multi_lot_fifo(transaction_svc):
    transaction_svc.record_buy(
        symbol='BTC',
        account='Main',
        qty=Decimal('1'),
        unit_price=Decimal('100'),
        fee_usd=Decimal('0'),
        tx_date='2026-03-01',
    )
    transaction_svc.record_buy(
        symbol='BTC',
        account='Main',
        qty=Decimal('2'),
        unit_price=Decimal('110'),
        fee_usd=Decimal('0'),
        tx_date='2026-03-02',
    )
    transaction_svc.record_sell(
        symbol='BTC',
        account='Main',
        qty=Decimal('2.5'),
        unit_price=Decimal('120'),
        fee_usd=Decimal('0'),
        tx_date='2026-03-03',
    )

    rows = transaction_svc.list_transactions(account='Main', symbol='BTC', limit=10)
    sell_row = next(r for r in rows if r['side'] == 'SELL')

    assert Decimal(str(sell_row['gross_proceeds'])) == Decimal('300')
    assert Decimal(str(sell_row['matched_cost_basis'])) == Decimal('265')
    assert Decimal(str(sell_row['realized_pnl'])) == Decimal('35')

def test_list_open_lots_shows_unconsumed_buy(transaction_svc):
    buy_id = transaction_svc.record_buy(
        symbol='BTC',
        account='Main',
        qty=Decimal('1'),
        unit_price=Decimal('100'),
        fee_usd=Decimal('0'),
        tx_date='2026-03-01',
    )

    rows = transaction_svc.list_open_lots(account='Main', symbol='BTC')

    assert len(rows) == 1
    lot = rows[0]
    assert lot['buy_tx_id'] == buy_id
    assert Decimal(str(lot['original_qty'])) == Decimal('1')
    assert Decimal(str(lot['remaining_qty'])) == Decimal('1')
    assert Decimal(str(lot['remaining_cost_basis'])) == Decimal('100')


def test_list_open_lots_partial_and_full_consumed_behavior(transaction_svc):
    buy1 = transaction_svc.record_buy(
        symbol='BTC',
        account='Main',
        qty=Decimal('1'),
        unit_price=Decimal('100'),
        fee_usd=Decimal('0'),
        tx_date='2026-03-01',
    )
    buy2 = transaction_svc.record_buy(
        symbol='BTC',
        account='Main',
        qty=Decimal('2'),
        unit_price=Decimal('110'),
        fee_usd=Decimal('0'),
        tx_date='2026-03-02',
    )
    transaction_svc.record_sell(
        symbol='BTC',
        account='Main',
        qty=Decimal('2.5'),
        unit_price=Decimal('120'),
        fee_usd=Decimal('0'),
        tx_date='2026-03-03',
    )

    rows = transaction_svc.list_open_lots(account='Main', symbol='BTC')

    assert len(rows) == 1
    lot = rows[0]
    assert lot['buy_tx_id'] == buy2
    assert lot['buy_tx_id'] != buy1
    assert Decimal(str(lot['remaining_qty'])) == Decimal('0.5')
    assert Decimal(str(lot['remaining_cost_basis'])) == Decimal('55')


def test_list_open_lots_keeps_lots_separate_and_supports_filters(transaction_svc):
    buy_main_btc_1 = transaction_svc.record_buy(
        symbol='BTC',
        account='Main',
        qty=Decimal('1'),
        unit_price=Decimal('100'),
        fee_usd=Decimal('0'),
        tx_date='2026-03-01',
    )
    buy_main_btc_2 = transaction_svc.record_buy(
        symbol='BTC',
        account='Main',
        qty=Decimal('2'),
        unit_price=Decimal('110'),
        fee_usd=Decimal('0'),
        tx_date='2026-03-02',
    )
    transaction_svc.record_buy(
        symbol='BTC',
        account='Vault',
        qty=Decimal('3'),
        unit_price=Decimal('120'),
        fee_usd=Decimal('0'),
        tx_date='2026-03-03',
    )
    transaction_svc.record_buy(
        symbol='ETH',
        account='Main',
        qty=Decimal('4'),
        unit_price=Decimal('50'),
        fee_usd=Decimal('0'),
        tx_date='2026-03-04',
    )

    rows_main_btc = transaction_svc.list_open_lots(account='Main', symbol='BTC')
    assert [r['buy_tx_id'] for r in rows_main_btc] == [buy_main_btc_1, buy_main_btc_2]

    rows_main = transaction_svc.list_open_lots(account='Main')
    assert all(r['account'] == 'Main' for r in rows_main)

    rows_eth = transaction_svc.list_open_lots(symbol='ETH')
    assert len(rows_eth) == 1
    assert rows_eth[0]['symbol'] == 'ETH'


def test_list_open_lots_restores_after_delete_sell(transaction_svc):
    buy_id = transaction_svc.record_buy(
        symbol='BTC',
        account='Main',
        qty=Decimal('2'),
        unit_price=Decimal('100'),
        fee_usd=Decimal('0'),
        tx_date='2026-03-01',
    )
    sell_id = transaction_svc.record_sell(
        symbol='BTC',
        account='Main',
        qty=Decimal('1.5'),
        unit_price=Decimal('120'),
        fee_usd=Decimal('0'),
        tx_date='2026-03-02',
    )

    before = transaction_svc.list_open_lots(account='Main', symbol='BTC')
    assert len(before) == 1
    assert Decimal(str(before[0]['remaining_qty'])) == Decimal('0.5')

    deleted = transaction_svc.delete_transaction(sell_id)
    assert deleted['id'] == sell_id

    after = transaction_svc.list_open_lots(account='Main', symbol='BTC')
    assert len(after) == 1
    assert after[0]['buy_tx_id'] == buy_id
    assert Decimal(str(after[0]['remaining_qty'])) == Decimal('2')

def test_list_transactions_filters_by_side(transaction_svc):
    transaction_svc.record_buy(
        symbol='BTC',
        account='Main',
        qty=Decimal('1'),
        unit_price=Decimal('100'),
        fee_usd=Decimal('0'),
        tx_date='2026-03-01',
    )
    transaction_svc.record_sell(
        symbol='BTC',
        account='Main',
        qty=Decimal('0.5'),
        unit_price=Decimal('120'),
        fee_usd=Decimal('0'),
        tx_date='2026-03-02',
    )

    rows = transaction_svc.list_transactions(account='Main', symbol='BTC', side='SELL', limit=10)

    assert len(rows) == 1
    assert rows[0]['side'] == 'SELL'


def test_list_transactions_filters_by_tx_id_and_combined_filters(transaction_svc):
    buy_main_btc = transaction_svc.record_buy(
        symbol='BTC',
        account='Main',
        qty=Decimal('1'),
        unit_price=Decimal('100'),
        fee_usd=Decimal('0'),
        tx_date='2026-03-01',
    )
    transaction_svc.record_buy(
        symbol='ETH',
        account='Main',
        qty=Decimal('1'),
        unit_price=Decimal('80'),
        fee_usd=Decimal('0'),
        tx_date='2026-03-01',
    )
    transaction_svc.record_buy(
        symbol='BTC',
        account='Alt',
        qty=Decimal('1'),
        unit_price=Decimal('90'),
        fee_usd=Decimal('0'),
        tx_date='2026-03-01',
    )

    by_id = transaction_svc.list_transactions(tx_id=buy_main_btc, limit=10)
    assert len(by_id) == 1
    assert by_id[0]['id'] == buy_main_btc

    combined = transaction_svc.list_transactions(account='Main', symbol='BTC', side='BUY', limit=10)
    assert len(combined) == 1
    assert combined[0]['id'] == buy_main_btc


def test_list_transactions_filters_by_date_range(transaction_svc):
    transaction_svc.record_buy(
        symbol='BTC',
        account='Main',
        qty=Decimal('1'),
        unit_price=Decimal('100'),
        fee_usd=Decimal('0'),
        tx_date='2026-03-20',
    )
    keep_id = transaction_svc.record_buy(
        symbol='BTC',
        account='Main',
        qty=Decimal('1'),
        unit_price=Decimal('110'),
        fee_usd=Decimal('0'),
        tx_date='2026-03-22',
    )

    rows = transaction_svc.list_transactions(
        account='Main',
        symbol='BTC',
        from_date='2026-03-21',
        to_date='2026-03-22',
        limit=10,
    )

    assert len(rows) == 1
    assert rows[0]['id'] == keep_id

def test_inspect_lot_matches_by_sell_tx_id_single_lot(transaction_svc):
    transaction_svc.record_buy(
        symbol='BTC',
        account='Main',
        qty=Decimal('1'),
        unit_price=Decimal('100'),
        fee_usd=Decimal('0'),
        tx_date='2026-03-01',
    )
    sell_id = transaction_svc.record_sell(
        symbol='BTC',
        account='Main',
        qty=Decimal('1'),
        unit_price=Decimal('120'),
        fee_usd=Decimal('0'),
        tx_date='2026-03-02',
    )

    rows = transaction_svc.inspect_lot_matches(sell_tx_id=sell_id)

    assert len(rows) == 1
    row = rows[0]
    assert row['sell_tx_id'] == sell_id
    assert Decimal(str(row['matched_qty'])) == Decimal('1')
    assert Decimal(str(row['matched_cost_basis'])) == Decimal('100')
    assert Decimal(str(row['matched_proceeds'])) == Decimal('120')
    assert Decimal(str(row['matched_realized_pnl'])) == Decimal('20')


def test_inspect_lot_matches_by_sell_tx_id_multi_lot_fifo(transaction_svc):
    buy1 = transaction_svc.record_buy(
        symbol='BTC',
        account='Main',
        qty=Decimal('1'),
        unit_price=Decimal('100'),
        fee_usd=Decimal('0'),
        tx_date='2026-03-01',
    )
    buy2 = transaction_svc.record_buy(
        symbol='BTC',
        account='Main',
        qty=Decimal('2'),
        unit_price=Decimal('110'),
        fee_usd=Decimal('0'),
        tx_date='2026-03-02',
    )
    sell_id = transaction_svc.record_sell(
        symbol='BTC',
        account='Main',
        qty=Decimal('2.5'),
        unit_price=Decimal('120'),
        fee_usd=Decimal('0'),
        tx_date='2026-03-03',
    )

    rows = transaction_svc.inspect_lot_matches(sell_tx_id=sell_id)

    assert len(rows) == 2
    assert rows[0]['buy_tx_id'] == buy1
    assert Decimal(str(rows[0]['matched_qty'])) == Decimal('1')
    assert rows[1]['buy_tx_id'] == buy2
    assert Decimal(str(rows[1]['matched_qty'])) == Decimal('1.5')

    total_realized = sum(Decimal(str(r['matched_realized_pnl'])) for r in rows)
    assert total_realized == Decimal('35')


def test_inspect_lot_matches_by_buy_tx_id(transaction_svc):
    buy_id = transaction_svc.record_buy(
        symbol='BTC',
        account='Main',
        qty=Decimal('2'),
        unit_price=Decimal('100'),
        fee_usd=Decimal('0'),
        tx_date='2026-03-01',
    )
    sell_id = transaction_svc.record_sell(
        symbol='BTC',
        account='Main',
        qty=Decimal('1'),
        unit_price=Decimal('120'),
        fee_usd=Decimal('0'),
        tx_date='2026-03-02',
    )

    rows = transaction_svc.inspect_lot_matches(buy_tx_id=buy_id)

    assert len(rows) == 1
    assert rows[0]['buy_tx_id'] == buy_id
    assert rows[0]['sell_tx_id'] == sell_id


def test_record_cdt_manual_persists_contractual_metadata_and_asset_method(transaction_svc, db_connection):
    tx_id = transaction_svc.record_cdt(
        account='BBVA',
        symbol='COLTEF CDT',
        open_date='2026-01-26',
        maturity_date='2026-06-23',
        principal=Decimal('4661.13'),
        term_years=Decimal('0.6'),
        annual_rate=Decimal('0.102'),
    )

    conn = db_connection.connect()
    cursor = conn.cursor()
    cursor.execute('SELECT tx_type, quantity, unit_price, notes FROM transactions WHERE id = ?', (tx_id,))
    tx_row = cursor.fetchone()

    assert tx_row[0] == 'BUY'
    assert Decimal(str(tx_row[1])) == Decimal('1')
    assert Decimal(str(tx_row[2])) == Decimal('4661.13')
    assert str(tx_row[3]).startswith('CDT_CONTRACT_V1:')
    assert '"term_source":"derived_from_dates"' in str(tx_row[3])

    cursor.execute(
        """
        SELECT a.valuation_method
        FROM assets a
        JOIN transactions t ON t.asset_id = a.id
        WHERE t.id = ?
        """,
        (tx_id,),
    )
    assert cursor.fetchone()[0] == 'contractual_value'


def test_delete_transaction_allows_unmatched_manual_cdt(transaction_svc):
    tx_id = transaction_svc.record_cdt(
        account='BBVA',
        symbol='COLTEF CDT',
        open_date='2026-01-26',
        maturity_date='2026-06-23',
        principal=Decimal('4661.13'),
        term_years=None,
        annual_rate=Decimal('0.102'),
    )

    deleted = transaction_svc.delete_transaction(tx_id)
    assert deleted['id'] == tx_id
    assert deleted['tx_type'] == 'BUY'


def test_record_fund_movement_uses_monetary_balance_and_no_artificial_realized_pnl(transaction_svc, db_connection):
    conn = db_connection.connect()
    cursor = conn.cursor()
    resolver = AssetResolver(db_connection)
    fund_asset = resolver.resolve('FONDO DINAMICO')
    account_id = transaction_svc._get_or_create_account('Trii', cursor)

    cursor.execute(
        """
        INSERT INTO transactions
        (asset_id, account_id, tx_type, quantity, unit_price, fee_usd, total_usd, tx_date, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (fund_asset['id'], account_id, 'MIGRATION_BUY', 1.0, 263.25, 0.0, 263.25, '2026-04-01', 'legacy seed'),
    )
    conn.commit()

    transaction_svc.record_fund_movement(
        account='Trii',
        symbol='FONDO DINAMICO',
        movement_date='2026-04-11',
        amount=Decimal('300'),
        movement_type='CONTRIBUTION',
    )
    transaction_svc.record_fund_movement(
        account='Trii',
        symbol='FONDO DINAMICO',
        movement_date='2026-04-20',
        amount=Decimal('50'),
        movement_type='WITHDRAWAL',
    )

    rows = transaction_svc.list_transactions(account='Trii', symbol='FONDO DINAMICO', limit=10)
    sell_row = next(r for r in rows if r['side'] == 'SELL')
    assert sell_row['matched_cost_basis'] is None
    assert sell_row['realized_pnl'] is None

    pnl_svc = PnLService(db_connection, resolver)
    pos = next(p for p in pnl_svc.positions('Trii') if p['symbol'] == 'FONDO DINAMICO')
    assert pos['qty_open'] == Decimal('513.25')
    assert pos['cost_basis'] == Decimal('513.25')
    assert pos['avg_cost'] == Decimal('1')
    assert pos['realized_pnl'] == Decimal('0')
    assert pos['approved_value'] == Decimal('513.25')
    assert pos['valuation_method'] == 'snapshot_imported'
    assert pos['valuation_status'] == 'usable_non_market'


def test_record_fund_movement_blocks_negative_monetary_balance(transaction_svc, db_connection):
    conn = db_connection.connect()
    cursor = conn.cursor()
    resolver = AssetResolver(db_connection)
    fund_asset = resolver.resolve('FONDO DINAMICO')
    account_id = transaction_svc._get_or_create_account('Trii', cursor)

    cursor.execute(
        """
        INSERT INTO transactions
        (asset_id, account_id, tx_type, quantity, unit_price, fee_usd, total_usd, tx_date, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (fund_asset['id'], account_id, 'MIGRATION_BUY', 1.0, 263.25, 0.0, 263.25, '2026-04-01', 'legacy seed'),
    )
    conn.commit()

    with pytest.raises(InvalidTransaction) as exc:
        transaction_svc.record_fund_movement(
            account='Trii',
            symbol='FONDO DINAMICO',
            movement_date='2026-04-20',
            amount=Decimal('300'),
            movement_type='WITHDRAWAL',
        )

    assert 'Insufficient fund balance' in str(exc.value)


def test_delete_transaction_fund_movement_keeps_balance_consistent(transaction_svc, db_connection):
    conn = db_connection.connect()
    cursor = conn.cursor()
    resolver = AssetResolver(db_connection)
    fund_asset = resolver.resolve('FONDO DINAMICO')
    account_id = transaction_svc._get_or_create_account('Trii', cursor)

    cursor.execute(
        """
        INSERT INTO transactions
        (asset_id, account_id, tx_type, quantity, unit_price, fee_usd, total_usd, tx_date, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (fund_asset['id'], account_id, 'MIGRATION_BUY', 1.0, 263.25, 0.0, 263.25, '2026-04-01', 'legacy seed'),
    )
    conn.commit()

    transaction_svc.record_fund_movement(
        account='Trii',
        symbol='FONDO DINAMICO',
        movement_date='2026-04-11',
        amount=Decimal('300'),
        movement_type='CONTRIBUTION',
    )
    withdrawal_tx_id = transaction_svc.record_fund_movement(
        account='Trii',
        symbol='FONDO DINAMICO',
        movement_date='2026-04-20',
        amount=Decimal('50'),
        movement_type='WITHDRAWAL',
    )

    pnl_svc = PnLService(db_connection, resolver)
    before = next(p for p in pnl_svc.positions('Trii') if p['symbol'] == 'FONDO DINAMICO')
    assert before['qty_open'] == Decimal('513.25')

    deleted = transaction_svc.delete_transaction(withdrawal_tx_id)
    assert deleted['tx_type'] == 'SELL'

    after = next(p for p in pnl_svc.positions('Trii') if p['symbol'] == 'FONDO DINAMICO')
    assert after['qty_open'] == Decimal('563.25')
    assert after['cost_basis'] == Decimal('563.25')



def test_cash_ledger_tracks_buy_sell_and_account_scopes(transaction_svc, db_connection):
    transaction_svc.record_buy(
        symbol='BTC',
        account='Trezor',
        qty=Decimal('1'),
        unit_price=Decimal('1000'),
        fee_usd=Decimal('5'),
        tx_date='2026-04-12',
    )
    transaction_svc.record_sell(
        symbol='BTC',
        account='Trezor',
        qty=Decimal('0.5'),
        unit_price=Decimal('1200'),
        fee_usd=Decimal('2'),
        tx_date='2026-04-13',
    )
    transaction_svc.record_buy(
        symbol='ETH',
        account='Main',
        qty=Decimal('1'),
        unit_price=Decimal('100'),
        fee_usd=Decimal('0'),
        tx_date='2026-04-12',
    )

    resolver = AssetResolver(db_connection)
    pnl_svc = PnLService(db_connection, resolver)
    assert pnl_svc.cash_balance('Trezor') == Decimal('-407')
    assert pnl_svc.cash_balance('Main') == Decimal('-100')
    assert pnl_svc.cash_balance() == Decimal('-507')


def test_delete_transaction_reverts_cash_for_buy_and_sell(transaction_svc, db_connection):
    buy_id = transaction_svc.record_buy(
        symbol='BTC',
        account='Main',
        qty=Decimal('1'),
        unit_price=Decimal('100'),
        fee_usd=Decimal('0'),
        tx_date='2026-04-12',
    )
    sell_id = transaction_svc.record_sell(
        symbol='BTC',
        account='Main',
        qty=Decimal('1'),
        unit_price=Decimal('150'),
        fee_usd=Decimal('0'),
        tx_date='2026-04-13',
    )

    resolver = AssetResolver(db_connection)
    pnl_svc = PnLService(db_connection, resolver)
    assert pnl_svc.cash_balance('Main') == Decimal('50')

    transaction_svc.delete_transaction(sell_id)
    assert pnl_svc.cash_balance('Main') == Decimal('-100')

    transaction_svc.delete_transaction(buy_id)
    assert pnl_svc.cash_balance('Main') == Decimal('0')


def test_cash_behavior_for_cdt_and_fund_movements(transaction_svc, db_connection):
    conn = db_connection.connect()
    cursor = conn.cursor()
    resolver = AssetResolver(db_connection)
    fund_asset = resolver.resolve('FONDO DINAMICO')
    account_id = transaction_svc._get_or_create_account('Trii', cursor)
    cursor.execute(
        """
        INSERT INTO transactions
        (asset_id, account_id, tx_type, quantity, unit_price, fee_usd, total_usd, tx_date, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (fund_asset['id'], account_id, 'MIGRATION_BUY', 1.0, 263.25, 0.0, 263.25, '2026-04-01', 'legacy seed'),
    )
    conn.commit()

    transaction_svc.record_cdt(
        account='BBVA',
        symbol='COLTEF CDT',
        open_date='2026-01-26',
        maturity_date='2026-06-23',
        principal=Decimal('4661.13'),
        term_years=None,
        annual_rate=Decimal('0.102'),
    )
    transaction_svc.record_fund_movement(
        account='Trii',
        symbol='FONDO DINAMICO',
        movement_date='2026-04-11',
        amount=Decimal('300'),
        movement_type='CONTRIBUTION',
    )
    transaction_svc.record_fund_movement(
        account='Trii',
        symbol='FONDO DINAMICO',
        movement_date='2026-04-20',
        amount=Decimal('50'),
        movement_type='WITHDRAWAL',
    )

    pnl_svc = PnLService(db_connection, resolver)
    assert pnl_svc.cash_balance('BBVA') == Decimal('-4661.13')
    assert pnl_svc.cash_balance('Trii') == Decimal('-250')
