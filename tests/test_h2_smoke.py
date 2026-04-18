"""
Smoke test for H2 GUI flows.
Tests that the core transaction flows work correctly.
"""
import os
from decimal import Decimal
import pytest

from portfolio_tracker_v2.core import Database
from portfolio_tracker_v2.core.asset_resolver import AssetResolver
from portfolio_tracker_v2.services.transaction_svc import TransactionService
from portfolio_tracker_v2.services.pnl_svc import PnLService


@pytest.fixture
def test_db():
    """Create a test database with schema."""
    db = Database(':memory:')
    db.init_schema()
    yield db
    db.close()


def test_add_transaction_buy(test_db):
    """Test BUY transaction flow (matching /add-transaction logic)."""
    resolver = AssetResolver(test_db)
    resolver.get_or_create_usd_cash()
    tx_svc = TransactionService(test_db, resolver)

    # This is what the GUI does: pass fee_usd, not fee
    tx_id = tx_svc.record_buy(
        symbol="BTC",
        account="TestAccount",
        qty=Decimal("0.5"),
        unit_price=Decimal("45000"),
        fee_usd=Decimal("10"),  # IMPORTANT: fee_usd, not fee
        tx_date="2024-01-15",
        notes="Test BUY",
    )
    assert tx_id > 0, "BUY should return a valid tx_id"


def test_add_transaction_sell(test_db):
    """Test SELL transaction flow (matching /add-transaction logic)."""
    resolver = AssetResolver(test_db)
    resolver.get_or_create_usd_cash()
    tx_svc = TransactionService(test_db, resolver)

    # First, BUY
    tx_svc.record_buy(
        symbol="ETH",
        account="TestAccount",
        qty=Decimal("2"),
        unit_price=Decimal("2500"),
        fee_usd=Decimal("5"),
        tx_date="2024-01-10",
    )

    # Then SELL
    tx_id = tx_svc.record_sell(
        symbol="ETH",
        account="TestAccount",
        qty=Decimal("1"),
        unit_price=Decimal("2600"),
        fee_usd=Decimal("5"),  # IMPORTANT: fee_usd, not fee
        tx_date="2024-01-20",
        notes="Test SELL",
    )
    assert tx_id > 0, "SELL should return a valid tx_id"


def test_add_cdt(test_db):
    """Test CDT creation flow."""
    resolver = AssetResolver(test_db)
    resolver.get_or_create_usd_cash()
    tx_svc = TransactionService(test_db, resolver)

    tx_id = tx_svc.record_cdt(
        account="TestAccount",
        symbol="TEST_CDT",
        open_date="2024-01-10",
        maturity_date="2024-07-10",
        principal=Decimal("50000"),
        term_years=Decimal("0.5"),
        annual_rate=Decimal("4.5"),
        notes="Test CDT",
    )
    assert tx_id > 0, "CDT should return a valid tx_id"


def test_add_fund_movement(test_db):
    """Test fund movement creation flow."""
    resolver = AssetResolver(test_db)
    resolver.get_or_create_usd_cash()
    tx_svc = TransactionService(test_db, resolver)

    tx_id = tx_svc.record_fund_movement(
        account="TestAccount",
        symbol="USD",
        movement_date="2024-01-05",
        amount=Decimal("10000"),
        movement_type="CONTRIBUTION",
        notes="Test contribution",
    )
    assert tx_id > 0, "Fund movement should return a valid tx_id"


def test_complete_h2_flow(test_db):
    """Test complete H2 flow: init -> add BUY -> add SELL -> add CDT -> fund movement."""
    resolver = AssetResolver(test_db)
    resolver.get_or_create_usd_cash()
    tx_svc = TransactionService(test_db, resolver)
    pnl_svc = PnLService(test_db, resolver)

    # Step 1: BUY
    buy_id = tx_svc.record_buy(
        symbol="BTC",
        account="Portfolio",
        qty=Decimal("1"),
        unit_price=Decimal("50000"),
        fee_usd=Decimal("20"),
        tx_date="2024-01-01",
    )
    assert buy_id > 0

    # Step 2: SELL (partial)
    sell_id = tx_svc.record_sell(
        symbol="BTC",
        account="Portfolio",
        qty=Decimal("0.3"),
        unit_price=Decimal("52000"),
        fee_usd=Decimal("15"),
        tx_date="2024-01-15",
    )
    assert sell_id > 0

    # Step 3: CDT
    cdt_id = tx_svc.record_cdt(
        account="Portfolio",
        symbol="BBVA_CDT",
        open_date="2024-01-05",
        maturity_date="2024-06-05",
        principal=Decimal("100000"),
        term_years=Decimal("0.5"),
        annual_rate=Decimal("5.0"),
    )
    assert cdt_id > 0

    # Step 4: Fund movement
    fund_id = tx_svc.record_fund_movement(
        account="Portfolio",
        symbol="USD",
        movement_date="2024-01-02",
        amount=Decimal("50000"),
        movement_type="CONTRIBUTION",
    )
    assert fund_id > 0

    # Step 5: Query transactions
    txns = tx_svc.list_transactions(limit=100)
    assert len(txns) > 0, "Should have transactions"

    # Step 6: Positions (read-only)
    positions = pnl_svc.positions()
    assert isinstance(positions, list), "positions should return a list"

    # Step 7: Summary (read-only)
    summary = pnl_svc.summary()
    assert isinstance(summary, dict), "summary should return a dict"
    assert "total_equity" in summary, "summary should have total_equity"
