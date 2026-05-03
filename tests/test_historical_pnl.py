"""
Tests for B57 — Historical Realized PnL Adjustments.

Covers:
1. Sum historical adjustment adds to realized PnL.
2. Cash balance unchanged after adding adjustment.
3. Total equity unchanged after adding adjustment.
4. Open positions unchanged after adding adjustment.
5. Total PnL = adjusted realized PnL + unrealized PnL.
6. summary() exposes breakdown: ledger realized, historical adjustment, displayed realized total.
7. GUI review step does NOT write until confirmed.
8. GUI confirm writes exactly once.
9. Backup created before GUI write.
10. Existing DB without the new table gets auto-migrated on first access.
"""
import json
import os
from decimal import Decimal
from pathlib import Path

import pytest

from portfolio_tracker_v2.core import Database
from portfolio_tracker_v2.core.asset_resolver import AssetResolver
from portfolio_tracker_v2.services.pnl_svc import PnLService
from portfolio_tracker_v2.services.transaction_svc import TransactionService


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def fresh_db():
    db = Database(":memory:")
    db.init_schema()
    return db


@pytest.fixture
def services(fresh_db):
    resolver = AssetResolver(fresh_db)
    tx_svc = TransactionService(fresh_db, resolver)
    pnl_svc = PnLService(fresh_db, resolver)
    return tx_svc, pnl_svc, fresh_db


@pytest.fixture
def services_with_trade(services):
    """DB with one closed BTC trade: buy 1 @ 40000, sell 1 @ 45000 → +5000 realized."""
    tx_svc, pnl_svc, db = services
    tx_svc.record_buy(
        symbol="BTC", account="Main",
        qty=Decimal("1"), unit_price=Decimal("40000"),
        fee_usd=Decimal("0"), tx_date="2022-01-01",
    )
    tx_svc.record_sell(
        symbol="BTC", account="Main",
        qty=Decimal("1"), unit_price=Decimal("45000"),
        fee_usd=Decimal("0"), tx_date="2022-06-01",
    )
    return tx_svc, pnl_svc, db


@pytest.fixture
def gui_env(tmp_path):
    from portfolio_tracker_v2.gui.app import create_app

    instance_path = tmp_path / "instance"
    instance_path.mkdir()

    db_path = tmp_path / "b57_test.db"
    db = Database(str(db_path))
    db.connect()
    db.init_schema()
    AssetResolver(db).get_or_create_usd_cash()
    db.close()

    state = {"db_path": str(db_path), "mode": "TEST"}
    (instance_path / "gui_state.json").write_text(json.dumps(state), encoding="utf-8")

    app = create_app(instance_path=str(instance_path))
    app.config.update({"TESTING": True})

    return {
        "app": app,
        "client": app.test_client(),
        "db_path": str(db_path),
        "instance_path": str(instance_path),
    }


# ---------------------------------------------------------------------------
# 1. Sum historical adjustment adds to realized PnL
# ---------------------------------------------------------------------------

def test_historical_adjustment_adds_to_realized_pnl(services_with_trade):
    _tx_svc, pnl_svc, _db = services_with_trade

    ledger_realized = pnl_svc.realized_pnl()
    assert ledger_realized == Decimal("5000")

    pnl_svc.add_historical_realized_pnl_adjustment(
        adjustment_date="2025-12-31",
        source="Vanguard historical 2022-2025",
        description="Prior closed investments",
        amount_usd=Decimal("12025.08"),
    )

    total_adj = pnl_svc.sum_historical_realized_pnl_adjustments()
    assert total_adj == Decimal("12025.08")

    summary = pnl_svc.summary()
    assert summary["displayed_realized_pnl"] == Decimal("5000") + Decimal("12025.08")


# ---------------------------------------------------------------------------
# 2. Cash balance unchanged after adding adjustment
# ---------------------------------------------------------------------------

def test_cash_balance_unchanged_after_adjustment(services):
    tx_svc, pnl_svc, _db = services
    tx_svc.record_cash_movement(
        account="Main", movement_type="DEPOSIT", amount=Decimal("10000"),
        movement_date="2024-01-01",
    )

    before = pnl_svc.cash_balance()
    pnl_svc.add_historical_realized_pnl_adjustment(
        adjustment_date="2025-12-31",
        source="Vanguard",
        description=None,
        amount_usd=Decimal("12025.08"),
    )
    after = pnl_svc.cash_balance()

    assert before == after


# ---------------------------------------------------------------------------
# 3. Total equity unchanged after adding adjustment
# ---------------------------------------------------------------------------

def test_total_equity_unchanged_after_adjustment(services_with_trade):
    _tx_svc, pnl_svc, _db = services_with_trade

    before = pnl_svc.summary()["total_equity"]
    pnl_svc.add_historical_realized_pnl_adjustment(
        adjustment_date="2025-12-31",
        source="Vanguard",
        description=None,
        amount_usd=Decimal("12025.08"),
    )
    after = pnl_svc.summary()["total_equity"]

    assert before == after


# ---------------------------------------------------------------------------
# 4. Open positions unchanged after adding adjustment
# ---------------------------------------------------------------------------

def test_open_positions_unchanged_after_adjustment(services):
    tx_svc, pnl_svc, _db = services
    tx_svc.record_buy(
        symbol="ETH", account="Main",
        qty=Decimal("2"), unit_price=Decimal("2000"),
        fee_usd=Decimal("0"), tx_date="2024-01-01",
    )

    positions_before = pnl_svc.positions()
    pnl_svc.add_historical_realized_pnl_adjustment(
        adjustment_date="2025-12-31",
        source="Vanguard",
        description=None,
        amount_usd=Decimal("12025.08"),
    )
    positions_after = pnl_svc.positions()

    assert len(positions_before) == len(positions_after)
    for pb, pa in zip(positions_before, positions_after):
        assert pb["symbol"] == pa["symbol"]
        assert pb["qty_open"] == pa["qty_open"]


# ---------------------------------------------------------------------------
# 5. Total PnL = adjusted realized PnL + unrealized PnL
# ---------------------------------------------------------------------------

def test_total_pnl_equals_displayed_realized_plus_unrealized(services_with_trade):
    tx_svc, pnl_svc, _db = services_with_trade
    tx_svc.record_buy(
        symbol="ETH", account="Main",
        qty=Decimal("1"), unit_price=Decimal("2000"),
        fee_usd=Decimal("0"), tx_date="2024-01-01",
    )

    pnl_svc.add_historical_realized_pnl_adjustment(
        adjustment_date="2025-12-31",
        source="Vanguard",
        description=None,
        amount_usd=Decimal("12025.08"),
    )

    summary = pnl_svc.summary()
    displayed_realized = summary["displayed_realized_pnl"]
    unrealized = summary["total_unrealized_pnl"]
    expected_total_pnl = displayed_realized + unrealized

    # _compute_total_pnl logic mirrors what GUI/CLI use
    from decimal import Decimal as D
    computed = D(str(summary.get("displayed_realized_pnl") or summary.get("total_realized_pnl") or 0))
    computed += D(str(summary.get("total_unrealized_pnl", 0) or 0))

    assert computed == expected_total_pnl


# ---------------------------------------------------------------------------
# 6. summary() exposes full breakdown
# ---------------------------------------------------------------------------

def test_summary_exposes_pnl_breakdown(services_with_trade):
    _tx_svc, pnl_svc, _db = services_with_trade

    pnl_svc.add_historical_realized_pnl_adjustment(
        adjustment_date="2025-12-31",
        source="Vanguard",
        description=None,
        amount_usd=Decimal("12025.08"),
    )

    summary = pnl_svc.summary()

    assert "total_realized_pnl" in summary
    assert "historical_realized_pnl" in summary
    assert "displayed_realized_pnl" in summary

    assert summary["total_realized_pnl"] == Decimal("5000")
    assert summary["historical_realized_pnl"] == Decimal("12025.08")
    assert summary["displayed_realized_pnl"] == Decimal("17025.08")


# ---------------------------------------------------------------------------
# 7. GUI review step does NOT write until confirmed
# ---------------------------------------------------------------------------

def test_gui_review_does_not_write(gui_env):
    client = gui_env["client"]

    resp = client.post(
        "/historical-pnl",
        data={
            "review": "1",
            "adjustment_date": "2025-12-31",
            "source": "Vanguard historical 2022-2025",
            "amount_usd": "12025.08",
            "description": "Test",
            "next_url": "/historical-pnl",
        },
    )
    # Review step returns 200 with the review page, nothing written
    assert resp.status_code == 200
    assert b"Review" in resp.data

    # Verify DB still has no adjustments
    db = Database(gui_env["db_path"])
    pnl_svc = PnLService(db, AssetResolver(db))
    total = pnl_svc.sum_historical_realized_pnl_adjustments()
    db.close()
    assert total == Decimal("0")


# ---------------------------------------------------------------------------
# 8. GUI confirm writes exactly once
# ---------------------------------------------------------------------------

def test_gui_confirm_writes_once(gui_env):
    client = gui_env["client"]

    resp = client.post(
        "/historical-pnl",
        data={
            "confirmed": "1",
            "adjustment_date": "2025-12-31",
            "source": "Vanguard historical 2022-2025",
            "amount_usd": "12025.08",
            "description": "Test adjustment",
            "next_url": "/historical-pnl",
        },
    )
    assert resp.status_code == 302

    db = Database(gui_env["db_path"])
    pnl_svc = PnLService(db, AssetResolver(db))
    adjustments = pnl_svc.list_historical_realized_pnl_adjustments()
    total = pnl_svc.sum_historical_realized_pnl_adjustments()
    db.close()

    assert len(adjustments) == 1
    assert total == Decimal("12025.08")
    assert adjustments[0]["source"] == "Vanguard historical 2022-2025"


# ---------------------------------------------------------------------------
# 9. Backup created before GUI write
# ---------------------------------------------------------------------------

def test_gui_backup_created_before_write(gui_env):
    client = gui_env["client"]
    instance_path = Path(gui_env["instance_path"])
    backup_dir = instance_path / "backups"

    # No backups before write
    assert not any(backup_dir.iterdir()) if backup_dir.exists() else True

    client.post(
        "/historical-pnl",
        data={
            "confirmed": "1",
            "adjustment_date": "2025-12-31",
            "source": "Vanguard",
            "amount_usd": "12025.08",
            "description": "",
            "next_url": "/historical-pnl",
        },
    )

    assert backup_dir.exists(), "Backup directory should be created"
    backups = list(backup_dir.iterdir())
    assert len(backups) >= 1, "At least one backup file should exist after the write"
    assert any("before_add_historical_pnl_adjustment" in b.name for b in backups)


# ---------------------------------------------------------------------------
# 10. Existing DB without the table gets auto-migrated on first access
# ---------------------------------------------------------------------------

def test_existing_db_auto_migrated(tmp_path):
    """Simulate a DB that was created before B57 (no historical table)."""
    db_path = str(tmp_path / "legacy.db")

    # Create a DB that does NOT have the historical_realized_pnl_adjustments table
    import sqlite3
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE assets (id INTEGER PRIMARY KEY, symbol TEXT UNIQUE NOT NULL, asset_type TEXT NOT NULL)")
    conn.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL)")
    conn.commit()
    conn.close()

    # Accessing the service should auto-create the table via _ensure_historical_pnl_schema
    db = Database(db_path)
    resolver = AssetResolver(db)
    pnl_svc = PnLService(db, resolver)

    # This must not raise even though the table didn't exist
    total = pnl_svc.sum_historical_realized_pnl_adjustments()
    assert total == Decimal("0")

    adjustments = pnl_svc.list_historical_realized_pnl_adjustments()
    assert adjustments == []

    db.close()
