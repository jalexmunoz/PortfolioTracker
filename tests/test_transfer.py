"""
B60: Asset Transfer between accounts — service + GUI tests.
"""
import json
import os
from decimal import Decimal
from pathlib import Path

import pytest

from portfolio_tracker_v2.core import Database
from portfolio_tracker_v2.core.asset_resolver import AssetResolver
from portfolio_tracker_v2.core.exceptions import InvalidTransaction
from portfolio_tracker_v2.services.pnl_svc import PnLService
from portfolio_tracker_v2.services.transaction_svc import TransactionService


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db():
    d = Database(":memory:")
    d.init_schema()
    return d


@pytest.fixture
def svc(db):
    resolver = AssetResolver(db)
    tx_svc = TransactionService(db, resolver)
    pnl_svc = PnLService(db, resolver)
    return tx_svc, pnl_svc, db


def _buy(tx_svc, symbol, account, qty, price, fee="0", date="2026-01-01"):
    tx_svc.record_buy(
        symbol=symbol,
        account=account,
        qty=Decimal(str(qty)),
        unit_price=Decimal(str(price)),
        fee_usd=Decimal(str(fee)),
        tx_date=date,
    )


def _transfer(tx_svc, symbol, src, dst, qty_sent, fee="0", date="2026-02-01", notes=None):
    return tx_svc.record_asset_transfer(
        symbol=symbol,
        source_account=src,
        dest_account=dst,
        quantity_sent=Decimal(str(qty_sent)),
        fee_quantity=Decimal(str(fee)),
        tx_date=date,
        notes=notes,
    )


# ---------------------------------------------------------------------------
# Service tests — criteria 1–6
# ---------------------------------------------------------------------------

def test_transfer_source_qty_decreases(svc):
    """Source account qty drops by quantity_sent after transfer."""
    tx_svc, pnl_svc, _ = svc
    _buy(tx_svc, "BTC", "Phemex", "0.10", "80000")
    _transfer(tx_svc, "BTC", "Phemex", "Trezor", "0.02", fee="0.0001")
    qty = pnl_svc.open_position_qty("BTC", account="Phemex")
    assert qty == Decimal("0.08"), f"Expected 0.08 got {qty}"


def test_transfer_dest_qty_increases(svc):
    """Destination account qty is quantity_sent - fee_quantity."""
    tx_svc, pnl_svc, _ = svc
    _buy(tx_svc, "BTC", "Phemex", "0.10", "80000")
    _transfer(tx_svc, "BTC", "Phemex", "Trezor", "0.02", fee="0.0001")
    qty = pnl_svc.open_position_qty("BTC", account="Trezor")
    assert qty == Decimal("0.0199"), f"Expected 0.0199 got {qty}"


def test_total_portfolio_qty_decreases_by_fee(svc):
    """Total portfolio qty drops by fee_quantity only."""
    tx_svc, pnl_svc, _ = svc
    _buy(tx_svc, "BTC", "Phemex", "0.10", "80000")
    total_before = pnl_svc.open_position_qty("BTC")
    _transfer(tx_svc, "BTC", "Phemex", "Trezor", "0.02", fee="0.0001")
    total_after = pnl_svc.open_position_qty("BTC")
    assert total_before - total_after == Decimal("0.0001"), (
        f"Expected fee reduction 0.0001, got {total_before - total_after}"
    )


def test_transfer_cost_basis_proportional(svc):
    """Cost basis moves proportionally; TRANSFER_IN gets weighted avg cost per unit."""
    tx_svc, pnl_svc, _ = svc
    # BTC at 80000 with 100 USD fee on 0.10 → cost_per_unit = (80000 + 100/0.10)/... wait
    # cost_per_unit = price + fee/qty = 80000 + 100/0.1 = 80000 + 1000 = 81000 per BTC
    _buy(tx_svc, "BTC", "Phemex", "0.10", "80000", fee="10")
    _transfer(tx_svc, "BTC", "Phemex", "Trezor", "0.02", fee="0")
    positions = {p["account"]: p for p in pnl_svc.positions()}
    # avg_cost_per_unit = 80000 + 10/0.10 = 80100
    expected_avg = Decimal("80100")
    trezor = positions.get("Trezor")
    assert trezor is not None, "Trezor position not found"
    assert abs(trezor["avg_cost"] - expected_avg) < Decimal("0.01"), (
        f"Trezor avg_cost expected ~{expected_avg}, got {trezor['avg_cost']}"
    )


def test_transfer_fee_reduces_total_cost_basis(svc):
    """Total cost_basis drops by fee_qty * avg_cost_per_unit."""
    tx_svc, pnl_svc, _ = svc
    _buy(tx_svc, "BTC", "Phemex", "0.10", "80000", fee="0")
    cost_before = sum(p["cost_basis"] for p in pnl_svc.positions())
    result = _transfer(tx_svc, "BTC", "Phemex", "Trezor", "0.02", fee="0.001")
    cost_after = sum(p["cost_basis"] for p in pnl_svc.positions())
    expected_reduction = result["cost_basis_fee"]
    actual_reduction = cost_before - cost_after
    assert abs(actual_reduction - expected_reduction) < Decimal("0.01"), (
        f"Cost basis should drop by {expected_reduction}, dropped {actual_reduction}"
    )


def test_transfer_no_realized_pnl(svc):
    """Realized PnL is unchanged after transfer."""
    tx_svc, pnl_svc, _ = svc
    _buy(tx_svc, "BTC", "Phemex", "0.10", "80000")
    pnl_before = pnl_svc.realized_pnl()
    _transfer(tx_svc, "BTC", "Phemex", "Trezor", "0.02", fee="0.0001")
    pnl_after = pnl_svc.realized_pnl()
    assert pnl_before == pnl_after, f"Realized PnL changed: {pnl_before} -> {pnl_after}"


def test_transfer_no_cash_impact(svc):
    """Cash balance is unchanged after transfer."""
    tx_svc, pnl_svc, _ = svc
    _buy(tx_svc, "BTC", "Phemex", "0.10", "80000")
    cash_before = pnl_svc.cash_balance()
    _transfer(tx_svc, "BTC", "Phemex", "Trezor", "0.02", fee="0.0001")
    cash_after = pnl_svc.cash_balance()
    assert cash_before == cash_after, f"Cash changed: {cash_before} -> {cash_after}"


# ---------------------------------------------------------------------------
# Service tests — criteria 5 & 6: future SELL FIFO integrity
# ---------------------------------------------------------------------------

def test_future_sell_in_source_respects_transferred_qty(svc):
    """After transfer, source can only sell what remained, not what was transferred."""
    tx_svc, pnl_svc, _ = svc
    _buy(tx_svc, "BTC", "Phemex", "1.0", "80000")
    _transfer(tx_svc, "BTC", "Phemex", "Trezor", "0.5", fee="0")
    # Should be able to sell 0.5 (remaining)
    tx_svc.record_sell("BTC", "Phemex", Decimal("0.5"), Decimal("90000"), Decimal("0"), "2026-03-01")
    # Selling any more should fail
    with pytest.raises(InvalidTransaction, match="Insufficient"):
        tx_svc.record_sell("BTC", "Phemex", Decimal("0.01"), Decimal("90000"), Decimal("0"), "2026-03-02")


def test_future_sell_in_dest_works(svc):
    """After transfer, destination can sell the received quantity."""
    tx_svc, pnl_svc, _ = svc
    _buy(tx_svc, "BTC", "Phemex", "1.0", "80000")
    _transfer(tx_svc, "BTC", "Phemex", "Trezor", "0.5", fee="0")
    tx_svc.record_sell("BTC", "Trezor", Decimal("0.5"), Decimal("90000"), Decimal("0"), "2026-03-01")
    qty = pnl_svc.open_position_qty("BTC", account="Trezor")
    assert qty == Decimal("0"), f"Expected 0 after selling all, got {qty}"


# ---------------------------------------------------------------------------
# Service tests — criteria 7–9: validation rejections
# ---------------------------------------------------------------------------

def test_insufficient_qty_rejected(svc):
    tx_svc, pnl_svc, _ = svc
    _buy(tx_svc, "BTC", "Phemex", "0.05", "80000")
    with pytest.raises(InvalidTransaction, match="Insufficient"):
        _transfer(tx_svc, "BTC", "Phemex", "Trezor", "0.10")


def test_fee_gte_qty_rejected(svc):
    tx_svc, _, _ = svc
    _buy(tx_svc, "BTC", "Phemex", "1.0", "80000")
    with pytest.raises(InvalidTransaction, match="fee_quantity must be less than"):
        _transfer(tx_svc, "BTC", "Phemex", "Trezor", "0.10", fee="0.10")


def test_fee_gt_qty_also_rejected(svc):
    tx_svc, _, _ = svc
    _buy(tx_svc, "BTC", "Phemex", "1.0", "80000")
    with pytest.raises(InvalidTransaction, match="fee_quantity must be less than"):
        _transfer(tx_svc, "BTC", "Phemex", "Trezor", "0.10", fee="0.20")


def test_same_account_rejected(svc):
    tx_svc, _, _ = svc
    _buy(tx_svc, "BTC", "Phemex", "1.0", "80000")
    with pytest.raises(InvalidTransaction, match="must be different"):
        _transfer(tx_svc, "BTC", "Phemex", "Phemex", "0.10")


# ---------------------------------------------------------------------------
# Service test — multi-lot FIFO integrity
# ---------------------------------------------------------------------------

def test_transfer_fifo_multi_lot(svc):
    """Transfer consuming two lots produces correct avg_cost and correct positions."""
    tx_svc, pnl_svc, _ = svc
    # lot1: 0.5 BTC @ 80000 (no fee)  → cost = 40000
    # lot2: 0.5 BTC @ 84000 (no fee)  → cost = 42000
    _buy(tx_svc, "BTC", "Phemex", "0.5", "80000", date="2026-01-01")
    _buy(tx_svc, "BTC", "Phemex", "0.5", "84000", date="2026-01-02")
    # Transfer 0.6 → consumes all of lot1 (0.5) + 0.1 from lot2
    # avg_cost = (0.5*80000 + 0.1*84000) / 0.6 = (40000+8400)/0.6 = 48400/0.6 ≈ 80666.67
    result = _transfer(tx_svc, "BTC", "Phemex", "Trezor", "0.6", fee="0")
    expected_avg = (Decimal("0.5") * Decimal("80000") + Decimal("0.1") * Decimal("84000")) / Decimal("0.6")
    assert abs(result["avg_cost_per_unit"] - expected_avg) < Decimal("0.01"), (
        f"avg_cost_per_unit expected ~{expected_avg}, got {result['avg_cost_per_unit']}"
    )
    phemex_qty = pnl_svc.open_position_qty("BTC", account="Phemex")
    assert phemex_qty == Decimal("0.4"), f"Phemex expected 0.4, got {phemex_qty}"
    trezor_qty = pnl_svc.open_position_qty("BTC", account="Trezor")
    assert trezor_qty == Decimal("0.6"), f"Trezor expected 0.6, got {trezor_qty}"


# ---------------------------------------------------------------------------
# Existing FIFO/summary regression — criterion 13 & 14
# ---------------------------------------------------------------------------

def test_existing_buy_sell_fifo_unaffected(svc):
    """Standard BUY → SELL FIFO still works correctly after B60 changes."""
    tx_svc, pnl_svc, _ = svc
    _buy(tx_svc, "ETH", "Main", "10", "2000", fee="5")
    tx_svc.record_sell("ETH", "Main", Decimal("3"), Decimal("3000"), Decimal("3"), "2020-01-02")
    realized = pnl_svc.realized_pnl("ETH", "Main")
    # matched_cost: 3*2000 + 5*(3/10) = 6000 + 1.5 = 6001.5
    # matched_proceeds: 3*3000 - 3*(3/3) = 9000 - 3 = 8997
    # realized = 8997 - 6001.5 = 2995.5
    assert abs(realized - Decimal("2995.5")) < Decimal("0.01"), f"Unexpected realized PnL: {realized}"


def test_existing_summary_unaffected_by_transfer_schema(svc):
    """Summary still sums positions correctly when no transfers exist."""
    tx_svc, pnl_svc, _ = svc
    _buy(tx_svc, "BTC", "Main", "1.0", "50000")
    summary = pnl_svc.summary()
    assert summary["total_cost_basis"] == Decimal("50000")
    assert summary["total_realized_pnl"] == Decimal("0")
    assert summary["cash_balance"] == Decimal("-50000")


# ---------------------------------------------------------------------------
# GUI tests — criteria 10, 11, 12
# ---------------------------------------------------------------------------

@pytest.fixture
def gui_env(tmp_path):
    from portfolio_tracker_v2.gui.app import create_app

    instance_path = tmp_path / "instance"
    instance_path.mkdir()
    db_path = tmp_path / "transfer_test.db"
    d = Database(str(db_path))
    d.connect()
    d.init_schema()
    AssetResolver(d).get_or_create_usd_cash()
    d.close()

    state = {"db_path": str(db_path), "mode": "TEST"}
    (instance_path / "gui_state.json").write_text(json.dumps(state), encoding="utf-8")

    app = create_app(instance_path=str(instance_path))
    app.config["TESTING"] = True
    return {
        "app": app,
        "client": app.test_client(),
        "db_path": str(db_path),
        "instance_path": str(instance_path),
    }


def _seed_btc(db_path, account, qty, price):
    import sqlite3
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO assets (symbol, asset_type, is_active, valuation_method) "
        "VALUES ('BTC', 'crypto', 1, 'market_live')"
    )
    cur.execute("SELECT id FROM assets WHERE symbol='BTC'")
    asset_id = cur.fetchone()[0]
    cur.execute("INSERT OR IGNORE INTO accounts (name) VALUES (?)", (account,))
    cur.execute("SELECT id FROM accounts WHERE name=?", (account,))
    account_id = cur.fetchone()[0]
    cur.execute(
        "INSERT INTO transactions (asset_id, account_id, tx_type, quantity, unit_price, "
        "fee_usd, total_usd, tx_date) VALUES (?, ?, 'BUY', ?, ?, 0, ?, '2026-01-01')",
        (asset_id, account_id, qty, price, qty * price),
    )
    conn.commit()
    conn.close()


def test_b60_transfer_get_renders(gui_env):
    resp = gui_env["client"].get("/transfer-asset")
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="ignore")
    assert "Transfer Asset" in body
    assert "source_account" in body


def test_b60_transfer_review_does_not_write(gui_env):
    """POSTing review=1 without confirmed shows review page but writes no transactions."""
    _seed_btc(gui_env["db_path"], "Phemex", 0.10, 80000)
    import sqlite3
    before = sqlite3.connect(gui_env["db_path"]).execute(
        "SELECT COUNT(*) FROM transactions"
    ).fetchone()[0]

    resp = gui_env["client"].post("/transfer-asset", data={
        "review": "1",
        "source_account": "Phemex",
        "dest_account": "Trezor",
        "symbol": "BTC",
        "tx_date": "2026-02-01",
        "quantity_sent": "0.02",
        "fee_quantity": "0.0001",
        "notes": "",
    })
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="ignore")
    assert "Review" in body or "Confirm" in body

    after = sqlite3.connect(gui_env["db_path"]).execute(
        "SELECT COUNT(*) FROM transactions"
    ).fetchone()[0]
    assert before == after, f"Review should not write: before={before}, after={after}"


def test_b60_transfer_confirm_writes_two_transactions(gui_env):
    """Confirmed POST writes exactly 2 new transactions (TRANSFER_OUT + TRANSFER_IN)."""
    _seed_btc(gui_env["db_path"], "Phemex", 0.10, 80000)
    import sqlite3
    before = sqlite3.connect(gui_env["db_path"]).execute(
        "SELECT COUNT(*) FROM transactions"
    ).fetchone()[0]

    resp = gui_env["client"].post("/transfer-asset", data={
        "confirmed": "1",
        "source_account": "Phemex",
        "dest_account": "Trezor",
        "symbol": "BTC",
        "tx_date": "2026-02-01",
        "quantity_sent": "0.02",
        "fee_quantity": "0.0001",
        "notes": "",
    }, follow_redirects=True)
    assert resp.status_code == 200

    after = sqlite3.connect(gui_env["db_path"]).execute(
        "SELECT COUNT(*) FROM transactions"
    ).fetchone()[0]
    assert after - before == 2, f"Expected 2 new transactions, got {after - before}"

    rows = sqlite3.connect(gui_env["db_path"]).execute(
        "SELECT tx_type FROM transactions ORDER BY id DESC LIMIT 2"
    ).fetchall()
    tx_types = {r[0] for r in rows}
    assert "TRANSFER_OUT" in tx_types
    assert "TRANSFER_IN" in tx_types


def test_b60_transfer_backup_created_before_write(gui_env):
    """Auto-backup is created in instance_path/backups before the GUI write."""
    _seed_btc(gui_env["db_path"], "Phemex", 0.10, 80000)

    backup_dir = os.path.join(gui_env["instance_path"], "backups")

    before = []
    if os.path.isdir(backup_dir):
        before = [f for f in os.listdir(backup_dir) if f.endswith(".db")]

    resp = gui_env["client"].post("/transfer-asset", data={
        "confirmed": "1",
        "source_account": "Phemex",
        "dest_account": "Trezor",
        "symbol": "BTC",
        "tx_date": "2026-02-01",
        "quantity_sent": "0.02",
        "fee_quantity": "0.0001",
        "notes": "",
    }, follow_redirects=True)
    assert resp.status_code == 200

    after = []
    if os.path.isdir(backup_dir):
        after = [f for f in os.listdir(backup_dir) if "before_asset_transfer" in f]

    assert len(after) >= 1, (
        f"Expected backup 'before_asset_transfer' in {backup_dir}, found: {os.listdir(backup_dir) if os.path.isdir(backup_dir) else 'dir missing'}"
    )
