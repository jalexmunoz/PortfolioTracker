"""
B61A: Correct Transfer Destination Account — service + CLI + GUI tests.
"""
import json
import os
import sqlite3
from decimal import Decimal

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


def _correct(tx_svc, transfer_in_tx_id, new_destination, notes=None):
    return tx_svc.correct_transfer_destination(
        transfer_in_tx_id=transfer_in_tx_id,
        new_destination_account=new_destination,
        notes=notes,
    )


def _get_account_id(db, name):
    row = db.connect().cursor().execute(
        "SELECT id FROM accounts WHERE name = ?", (name,)
    ).fetchone()
    return row[0] if row else None


def _ensure_account(db, name):
    conn = db.connect()
    conn.execute("INSERT OR IGNORE INTO accounts (name) VALUES (?)", (name,))
    conn.commit()


# ---------------------------------------------------------------------------
# Test 1 — correct destination updates TRANSFER_IN account_id
# ---------------------------------------------------------------------------

def test_correct_destination_updates_account_id(svc):
    tx_svc, pnl_svc, db = svc
    _buy(tx_svc, "BTC", "Phemex", "0.10", "80000")
    result = _transfer(tx_svc, "BTC", "Phemex", "Tezor", "0.05", fee="0")
    in_tx_id = result["transfer_in_tx_id"]

    _ensure_account(db, "Trezor")
    correction = _correct(tx_svc, in_tx_id, "Trezor")

    assert not correction.get("no_op")
    assert correction["previous_destination"] == "Tezor"
    assert correction["new_destination"] == "Trezor"

    row = db.connect().cursor().execute(
        "SELECT acc.name FROM transactions t JOIN accounts acc ON acc.id = t.account_id WHERE t.id = ?",
        (in_tx_id,),
    ).fetchone()
    assert row[0] == "Trezor"


# ---------------------------------------------------------------------------
# Test 2 — source account quantity unchanged after correction
# ---------------------------------------------------------------------------

def test_source_qty_unchanged_after_correction(svc):
    tx_svc, pnl_svc, db = svc
    _buy(tx_svc, "BTC", "Phemex", "0.10", "80000")
    result = _transfer(tx_svc, "BTC", "Phemex", "Tezor", "0.05", fee="0")
    in_tx_id = result["transfer_in_tx_id"]

    _ensure_account(db, "Trezor")
    _correct(tx_svc, in_tx_id, "Trezor")

    phemex_qty = pnl_svc.open_position_qty("BTC", account="Phemex")
    assert phemex_qty == Decimal("0.05"), f"Phemex expected 0.05, got {phemex_qty}"


# ---------------------------------------------------------------------------
# Test 3 — old destination loses the position
# ---------------------------------------------------------------------------

def test_old_destination_loses_position(svc):
    tx_svc, pnl_svc, db = svc
    _buy(tx_svc, "BTC", "Phemex", "0.10", "80000")
    result = _transfer(tx_svc, "BTC", "Phemex", "Tezor", "0.05", fee="0")
    in_tx_id = result["transfer_in_tx_id"]

    _ensure_account(db, "Trezor")
    _correct(tx_svc, in_tx_id, "Trezor")

    tezor_qty = pnl_svc.open_position_qty("BTC", account="Tezor")
    assert tezor_qty == Decimal("0"), f"Tezor expected 0, got {tezor_qty}"


# ---------------------------------------------------------------------------
# Test 4 — new destination receives the quantity
# ---------------------------------------------------------------------------

def test_new_destination_receives_quantity(svc):
    tx_svc, pnl_svc, db = svc
    _buy(tx_svc, "BTC", "Phemex", "0.10", "80000")
    result = _transfer(tx_svc, "BTC", "Phemex", "Tezor", "0.05", fee="0.001")
    in_tx_id = result["transfer_in_tx_id"]

    _ensure_account(db, "Trezor")
    _correct(tx_svc, in_tx_id, "Trezor")

    trezor_qty = pnl_svc.open_position_qty("BTC", account="Trezor")
    assert trezor_qty == Decimal("0.049"), f"Trezor expected 0.049, got {trezor_qty}"


# ---------------------------------------------------------------------------
# Test 5 — unit_price / cost basis unchanged
# ---------------------------------------------------------------------------

def test_cost_basis_unchanged_after_correction(svc):
    tx_svc, pnl_svc, db = svc
    _buy(tx_svc, "BTC", "Phemex", "0.10", "80000")
    result = _transfer(tx_svc, "BTC", "Phemex", "Tezor", "0.05", fee="0")
    in_tx_id = result["transfer_in_tx_id"]
    expected_unit_price = result["avg_cost_per_unit"]

    _ensure_account(db, "Trezor")
    correction = _correct(tx_svc, in_tx_id, "Trezor")

    row = db.connect().cursor().execute(
        "SELECT unit_price FROM transactions WHERE id = ?", (in_tx_id,)
    ).fetchone()
    actual_unit_price = Decimal(str(row[0]))
    assert abs(actual_unit_price - expected_unit_price) < Decimal("0.01"), (
        f"unit_price changed: expected {expected_unit_price}, got {actual_unit_price}"
    )
    assert abs(correction["unit_price"] - expected_unit_price) < Decimal("0.01")


# ---------------------------------------------------------------------------
# Test 6 — realized PnL unchanged
# ---------------------------------------------------------------------------

def test_realized_pnl_unchanged_after_correction(svc):
    tx_svc, pnl_svc, db = svc
    _buy(tx_svc, "BTC", "Phemex", "0.10", "80000")
    result = _transfer(tx_svc, "BTC", "Phemex", "Tezor", "0.05", fee="0")
    in_tx_id = result["transfer_in_tx_id"]
    pnl_before = pnl_svc.realized_pnl()

    _ensure_account(db, "Trezor")
    _correct(tx_svc, in_tx_id, "Trezor")

    pnl_after = pnl_svc.realized_pnl()
    assert pnl_before == pnl_after, f"PnL changed: {pnl_before} -> {pnl_after}"


# ---------------------------------------------------------------------------
# Test 7 — cash unchanged
# ---------------------------------------------------------------------------

def test_cash_unchanged_after_correction(svc):
    tx_svc, pnl_svc, db = svc
    _buy(tx_svc, "BTC", "Phemex", "0.10", "80000")
    result = _transfer(tx_svc, "BTC", "Phemex", "Tezor", "0.05", fee="0")
    in_tx_id = result["transfer_in_tx_id"]
    cash_before = pnl_svc.cash_balance()

    _ensure_account(db, "Trezor")
    _correct(tx_svc, in_tx_id, "Trezor")

    cash_after = pnl_svc.cash_balance()
    assert cash_before == cash_after, f"Cash changed: {cash_before} -> {cash_after}"


# ---------------------------------------------------------------------------
# Test 8 — transfer_lot_matches unchanged
# ---------------------------------------------------------------------------

def test_transfer_lot_matches_unchanged(svc):
    tx_svc, pnl_svc, db = svc
    _buy(tx_svc, "BTC", "Phemex", "0.10", "80000")
    result = _transfer(tx_svc, "BTC", "Phemex", "Tezor", "0.05", fee="0")
    in_tx_id = result["transfer_in_tx_id"]
    out_tx_id = result["transfer_out_tx_id"]

    cursor = db.connect().cursor()
    before = cursor.execute(
        "SELECT transfer_out_tx_id, buy_tx_id, quantity FROM transfer_lot_matches "
        "WHERE transfer_out_tx_id = ?",
        (out_tx_id,),
    ).fetchall()

    _ensure_account(db, "Trezor")
    _correct(tx_svc, in_tx_id, "Trezor")

    after = cursor.execute(
        "SELECT transfer_out_tx_id, buy_tx_id, quantity FROM transfer_lot_matches "
        "WHERE transfer_out_tx_id = ?",
        (out_tx_id,),
    ).fetchall()

    assert before == after, f"transfer_lot_matches changed: {before} -> {after}"


# ---------------------------------------------------------------------------
# Test 9 — audit trail appended to notes
# ---------------------------------------------------------------------------

def test_audit_trail_appended_to_notes(svc):
    tx_svc, pnl_svc, db = svc
    _buy(tx_svc, "BTC", "Phemex", "0.10", "80000")
    result = _transfer(tx_svc, "BTC", "Phemex", "Tezor", "0.05", fee="0",
                       notes="original note")
    in_tx_id = result["transfer_in_tx_id"]

    _ensure_account(db, "Trezor")
    _correct(tx_svc, in_tx_id, "Trezor", notes="typo fix")

    row = db.connect().cursor().execute(
        "SELECT notes FROM transactions WHERE id = ?", (in_tx_id,)
    ).fetchone()
    notes_text = row[0]
    assert "CORRECTED_DEST:Tezor->Trezor" in notes_text
    assert "typo fix" in notes_text
    assert "original note" in notes_text


# ---------------------------------------------------------------------------
# Test 10 — reject tx_id that is not TRANSFER_IN
# ---------------------------------------------------------------------------

def test_reject_if_not_transfer_in(svc):
    tx_svc, pnl_svc, db = svc
    _buy(tx_svc, "BTC", "Phemex", "0.10", "80000")
    result = _transfer(tx_svc, "BTC", "Phemex", "Tezor", "0.05", fee="0")
    out_tx_id = result["transfer_out_tx_id"]

    _ensure_account(db, "Trezor")
    with pytest.raises(InvalidTransaction, match="not TRANSFER_IN"):
        _correct(tx_svc, out_tx_id, "Trezor")


# ---------------------------------------------------------------------------
# Test 11 — reject if new_destination == source_account
# ---------------------------------------------------------------------------

def test_reject_if_new_destination_is_source(svc):
    tx_svc, pnl_svc, db = svc
    _buy(tx_svc, "BTC", "Phemex", "0.10", "80000")
    result = _transfer(tx_svc, "BTC", "Phemex", "Tezor", "0.05", fee="0")
    in_tx_id = result["transfer_in_tx_id"]

    with pytest.raises(InvalidTransaction, match="same as source account"):
        _correct(tx_svc, in_tx_id, "Phemex")


# ---------------------------------------------------------------------------
# Test 12 — no-op (no error) if new_destination == current_destination
# ---------------------------------------------------------------------------

def test_noop_if_same_destination(svc):
    tx_svc, pnl_svc, db = svc
    _buy(tx_svc, "BTC", "Phemex", "0.10", "80000")
    result = _transfer(tx_svc, "BTC", "Phemex", "Tezor", "0.05", fee="0")
    in_tx_id = result["transfer_in_tx_id"]

    correction = _correct(tx_svc, in_tx_id, "Tezor")
    assert correction.get("no_op") is True
    assert "already" in correction["message"]

    row = db.connect().cursor().execute(
        "SELECT acc.name FROM transactions t JOIN accounts acc ON acc.id = t.account_id WHERE t.id = ?",
        (in_tx_id,),
    ).fetchone()
    assert row[0] == "Tezor"


# ---------------------------------------------------------------------------
# Test 13 — reject if new_destination account does not exist
# ---------------------------------------------------------------------------

def test_reject_if_account_does_not_exist(svc):
    tx_svc, pnl_svc, db = svc
    _buy(tx_svc, "BTC", "Phemex", "0.10", "80000")
    result = _transfer(tx_svc, "BTC", "Phemex", "Tezor", "0.05", fee="0")
    in_tx_id = result["transfer_in_tx_id"]

    with pytest.raises(InvalidTransaction, match="does not exist"):
        _correct(tx_svc, in_tx_id, "NonExistentWallet")


# ---------------------------------------------------------------------------
# Test 14 — reject if TRANSFER_IN was consumed by a subsequent SELL
# ---------------------------------------------------------------------------

def test_reject_if_transfer_in_consumed_by_sell(svc):
    tx_svc, pnl_svc, db = svc
    _buy(tx_svc, "BTC", "Phemex", "0.10", "80000")
    result = _transfer(tx_svc, "BTC", "Phemex", "Tezor", "0.05", fee="0")
    in_tx_id = result["transfer_in_tx_id"]

    tx_svc.record_sell("BTC", "Tezor", Decimal("0.02"), Decimal("90000"), Decimal("0"), "2026-03-01")

    _ensure_account(db, "Trezor")
    with pytest.raises(InvalidTransaction, match="SELL transaction"):
        _correct(tx_svc, in_tx_id, "Trezor")


# ---------------------------------------------------------------------------
# Test 15 — reject if TRANSFER_IN was consumed by a subsequent TRANSFER_OUT
# ---------------------------------------------------------------------------

def test_reject_if_transfer_in_consumed_by_transfer_out(svc):
    tx_svc, pnl_svc, db = svc
    _buy(tx_svc, "BTC", "Phemex", "0.10", "80000")
    result = _transfer(tx_svc, "BTC", "Phemex", "Tezor", "0.05", fee="0")
    in_tx_id = result["transfer_in_tx_id"]

    _ensure_account(db, "ColdCard")
    _transfer(tx_svc, "BTC", "Tezor", "ColdCard", "0.03", fee="0")

    _ensure_account(db, "Trezor")
    with pytest.raises(InvalidTransaction, match="TRANSFER_OUT"):
        _correct(tx_svc, in_tx_id, "Trezor")


# ---------------------------------------------------------------------------
# GUI fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def gui_env(tmp_path):
    from portfolio_tracker_v2.gui.app import create_app

    instance_path = tmp_path / "instance"
    instance_path.mkdir()
    db_path = tmp_path / "correct_transfer_test.db"
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


def _seed_transfer(db_path):
    """Seed Phemex→Tezor transfer and return transfer_in_tx_id."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO assets (symbol, asset_type, is_active, valuation_method) "
        "VALUES ('BTC', 'crypto', 1, 'market_live')"
    )
    cur.execute("SELECT id FROM assets WHERE symbol='BTC'")
    asset_id = cur.fetchone()[0]

    cur.execute("INSERT OR IGNORE INTO accounts (name) VALUES ('Phemex')")
    cur.execute("SELECT id FROM accounts WHERE name='Phemex'")
    phemex_id = cur.fetchone()[0]

    cur.execute("INSERT OR IGNORE INTO accounts (name) VALUES ('Tezor')")
    cur.execute("SELECT id FROM accounts WHERE name='Tezor'")
    tezor_id = cur.fetchone()[0]

    cur.execute("INSERT OR IGNORE INTO accounts (name) VALUES ('Trezor')")
    cur.execute("SELECT id FROM accounts WHERE name='Trezor'")
    trezor_id = cur.fetchone()[0]

    cur.execute(
        "INSERT INTO transactions (asset_id, account_id, tx_type, quantity, unit_price, "
        "fee_usd, total_usd, tx_date) VALUES (?, ?, 'BUY', 0.1, 80000, 0, 8000, '2026-01-01')",
        (asset_id, phemex_id),
    )
    buy_tx_id = cur.lastrowid

    cur.execute(
        "INSERT INTO transactions (asset_id, account_id, tx_type, quantity, unit_price, "
        "fee_usd, total_usd, tx_date, notes) VALUES (?, ?, 'TRANSFER_OUT', 0.05, 80000, 0, 0, '2026-02-01', ?)",
        (asset_id, phemex_id, "TRANSFER_OUT:to=Tezor,fee=0"),
    )
    out_tx_id = cur.lastrowid

    cur.execute(
        "INSERT INTO transfer_lot_matches (transfer_out_tx_id, buy_tx_id, quantity) VALUES (?, ?, 0.05)",
        (out_tx_id, buy_tx_id),
    )

    in_notes = f"TRANSFER_IN:from=Phemex,ref_out={out_tx_id}"
    cur.execute(
        "INSERT INTO transactions (asset_id, account_id, tx_type, quantity, unit_price, "
        "fee_usd, total_usd, tx_date, notes) VALUES (?, ?, 'TRANSFER_IN', 0.05, 80000, 0, 0, '2026-02-01', ?)",
        (asset_id, tezor_id, in_notes),
    )
    in_tx_id = cur.lastrowid

    conn.commit()
    conn.close()
    return in_tx_id


# ---------------------------------------------------------------------------
# Test 16 — GUI GET renders the form
# ---------------------------------------------------------------------------

def test_gui_get_renders_form(gui_env):
    resp = gui_env["client"].get("/correct-transfer-destination")
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="ignore")
    assert "Correct Transfer Destination" in body
    assert "transfer_in_tx_id" in body
    assert "new_destination" in body


# ---------------------------------------------------------------------------
# Test 17 — GUI review=1 does not write to DB
# ---------------------------------------------------------------------------

def test_gui_review_does_not_write(gui_env):
    in_tx_id = _seed_transfer(gui_env["db_path"])
    before = sqlite3.connect(gui_env["db_path"]).execute(
        "SELECT account_id FROM transactions WHERE id = ?", (in_tx_id,)
    ).fetchone()[0]

    resp = gui_env["client"].post("/correct-transfer-destination", data={
        "review": "1",
        "transfer_in_tx_id": str(in_tx_id),
        "new_destination": "Trezor",
        "notes": "",
    })
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="ignore")
    assert "Review" in body or "Confirm" in body

    after = sqlite3.connect(gui_env["db_path"]).execute(
        "SELECT account_id FROM transactions WHERE id = ?", (in_tx_id,)
    ).fetchone()[0]
    assert before == after, "Review must not write account_id change"


# ---------------------------------------------------------------------------
# Test 18 — GUI confirmed=1 writes exactly once (account_id updated)
# ---------------------------------------------------------------------------

def test_gui_confirm_writes_once(gui_env):
    in_tx_id = _seed_transfer(gui_env["db_path"])

    resp = gui_env["client"].post("/correct-transfer-destination", data={
        "confirmed": "1",
        "transfer_in_tx_id": str(in_tx_id),
        "new_destination": "Trezor",
        "notes": "typo fix",
    }, follow_redirects=True)
    assert resp.status_code == 200

    conn = sqlite3.connect(gui_env["db_path"])
    row = conn.execute(
        "SELECT acc.name FROM transactions t JOIN accounts acc ON acc.id = t.account_id "
        "WHERE t.id = ?", (in_tx_id,)
    ).fetchone()
    assert row[0] == "Trezor", f"Expected Trezor, got {row[0]}"

    notes_row = conn.execute(
        "SELECT notes FROM transactions WHERE id = ?", (in_tx_id,)
    ).fetchone()
    assert "CORRECTED_DEST" in notes_row[0]


# ---------------------------------------------------------------------------
# Test 19 — GUI backup created before write
# ---------------------------------------------------------------------------

def test_gui_backup_created_before_write(gui_env):
    in_tx_id = _seed_transfer(gui_env["db_path"])
    backup_dir = os.path.join(gui_env["instance_path"], "backups")

    resp = gui_env["client"].post("/correct-transfer-destination", data={
        "confirmed": "1",
        "transfer_in_tx_id": str(in_tx_id),
        "new_destination": "Trezor",
        "notes": "",
    }, follow_redirects=True)
    assert resp.status_code == 200

    backups = []
    if os.path.isdir(backup_dir):
        backups = [f for f in os.listdir(backup_dir) if "before_correct_transfer_destination" in f]
    assert len(backups) >= 1, (
        f"Expected backup 'before_correct_transfer_destination' in {backup_dir}. "
        f"Found: {os.listdir(backup_dir) if os.path.isdir(backup_dir) else 'dir missing'}"
    )


# ---------------------------------------------------------------------------
# Test 20 — delete-transaction rejects TRANSFER_IN and TRANSFER_OUT
#           with the improved B61A message
# ---------------------------------------------------------------------------

def test_delete_transaction_rejects_transfer_in_with_clear_message(svc):
    tx_svc, pnl_svc, db = svc
    _buy(tx_svc, "BTC", "Phemex", "0.10", "80000")
    result = _transfer(tx_svc, "BTC", "Phemex", "Tezor", "0.05", fee="0")

    with pytest.raises(InvalidTransaction) as exc_info:
        tx_svc.delete_transaction(result["transfer_in_tx_id"])
    assert "correct-transfer-destination" in str(exc_info.value)

    with pytest.raises(InvalidTransaction) as exc_info:
        tx_svc.delete_transaction(result["transfer_out_tx_id"])
    assert "correct-transfer-destination" in str(exc_info.value)
