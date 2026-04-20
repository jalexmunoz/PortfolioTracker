"""
Tests for B51: COP FX valuation for CDT and fund balances.

All values stored and reported in USD; original COP amount + fx_rate kept
in transaction notes for audit. Cash ledger remains 100% USD.
"""
import json
from decimal import Decimal

import pytest

from portfolio_tracker_v2.core import Database
from portfolio_tracker_v2.core.asset_resolver import AssetResolver
from portfolio_tracker_v2.core.exceptions import InvalidTransaction
from portfolio_tracker_v2.services.pnl_svc import PnLService
from portfolio_tracker_v2.services.transaction_svc import TransactionService


@pytest.fixture
def db():
    db = Database(":memory:")
    db.init_schema()
    return db


@pytest.fixture
def tx_svc(db):
    return TransactionService(db, AssetResolver(db))


@pytest.fixture
def pnl_svc(db):
    return PnLService(db, AssetResolver(db))


def _cdt_payload(db, tx_id):
    cur = db.connect().cursor()
    cur.execute("SELECT notes FROM transactions WHERE id = ?", (tx_id,))
    notes = cur.fetchone()[0]
    payload_text = notes[len(TransactionService.CDT_CONTRACT_NOTE_PREFIX):]
    if " | " in payload_text:
        payload_text = payload_text.split(" | ", 1)[0]
    return json.loads(payload_text)


def _fund_payload(db, tx_id):
    cur = db.connect().cursor()
    cur.execute("SELECT notes FROM transactions WHERE id = ?", (tx_id,))
    notes = cur.fetchone()[0]
    head = notes.split(" | ", 1)[0]
    if ":" not in head:
        return None
    return json.loads(head.split(":", 1)[1])


def _cash_balance(db, account):
    cur = db.connect().cursor()
    cur.execute(
        """
        SELECT COALESCE(SUM(cl.amount_usd), 0)
        FROM cash_ledger cl
        JOIN accounts acc ON acc.id = cl.account_id
        WHERE acc.name = ?
        """,
        (account,),
    )
    return Decimal(str(cur.fetchone()[0] or 0))


# --- 1. CDT COP with explicit fx_rate_at_open ---

def test_cdt_cop_with_explicit_fx_persists_audit_fields_and_usd_principal(tx_svc, db):
    tx_id = tx_svc.record_cdt(
        account="BBVA",
        symbol="BBVA CDT",
        open_date="2026-04-01",
        maturity_date="2026-10-01",
        principal=Decimal("17000000"),
        term_years=None,
        annual_rate=Decimal("0.10"),
        currency="COP",
        fx_rate_at_open=Decimal("4000"),
    )

    cur = db.connect().cursor()
    cur.execute("SELECT unit_price, total_usd FROM transactions WHERE id = ?", (tx_id,))
    unit_price, total_usd = cur.fetchone()
    assert Decimal(str(unit_price)) == Decimal("4250")
    assert Decimal(str(total_usd)) == Decimal("4250")

    payload = _cdt_payload(db, tx_id)
    assert payload["currency"] == "COP"
    assert payload["principal_original"] == "17000000"
    assert payload["fx_rate_at_open"] == "4000"
    assert payload["fx_date"] == "2026-04-01"
    assert payload["principal_usd"] == "4250.000000"
    assert payload["principal"] == "4250.000000"


# --- 2. CDT COP without fx_rate_at_open → reject (no fallback) ---

def test_cdt_cop_without_fx_rate_rejected(tx_svc):
    with pytest.raises(InvalidTransaction, match="fx_rate_at_open is required"):
        tx_svc.record_cdt(
            account="BBVA",
            symbol="BBVA CDT",
            open_date="2026-04-01",
            maturity_date="2026-10-01",
            principal=Decimal("17000000"),
            term_years=None,
            annual_rate=Decimal("0.10"),
            currency="COP",
            fx_rate_at_open=None,
        )


# --- 3. CDT USD continues to work (no regression) ---

def test_cdt_usd_unchanged(tx_svc, db):
    tx_id = tx_svc.record_cdt(
        account="BBVA",
        symbol="BBVA CDT",
        open_date="2026-04-01",
        maturity_date="2026-10-01",
        principal=Decimal("4250"),
        term_years=None,
        annual_rate=Decimal("0.10"),
    )
    payload = _cdt_payload(db, tx_id)
    assert "currency" not in payload
    assert "fx_rate_at_open" not in payload
    assert payload["principal"] == "4250"
    assert _cash_balance(db, "BBVA") == Decimal("-4250")


# --- 4. Fund COP CONTRIBUTION uses fx_rate per movement ---

def test_fund_cop_contribution_lowers_cash_in_usd(tx_svc, db):
    tx_id = tx_svc.record_fund_movement(
        account="Trii",
        symbol="FONDO DINAMICO",
        movement_date="2026-04-19",
        amount=Decimal("5000000"),
        movement_type="CONTRIBUTION",
        currency="COP",
        fx_rate=Decimal("4000"),
    )
    cur = db.connect().cursor()
    cur.execute("SELECT total_usd FROM transactions WHERE id = ?", (tx_id,))
    assert Decimal(str(cur.fetchone()[0])) == Decimal("1250")

    payload = _fund_payload(db, tx_id)
    assert payload == {
        "currency": "COP",
        "amount_original": "5000000",
        "fx_rate": "4000",
        "fx_date": "2026-04-19",
        "amount_usd": "1250.000000",
    }
    assert _cash_balance(db, "Trii") == Decimal("-1250")


# --- 5. Fund COP WITHDRAWAL raises cash in USD ---

def test_fund_cop_withdrawal_raises_cash_in_usd(tx_svc, db):
    tx_svc.record_fund_movement(
        account="Trii",
        symbol="FONDO DINAMICO",
        movement_date="2026-04-19",
        amount=Decimal("5000000"),
        movement_type="CONTRIBUTION",
        currency="COP",
        fx_rate=Decimal("4000"),
    )
    tx_svc.record_fund_movement(
        account="Trii",
        symbol="FONDO DINAMICO",
        movement_date="2026-04-25",
        amount=Decimal("2000000"),
        movement_type="WITHDRAWAL",
        currency="COP",
        fx_rate=Decimal("4100"),
    )
    # 5_000_000 / 4000 = 1250 ; 2_000_000 / 4100 ≈ 487.804878
    expected = Decimal("-1250") + Decimal("487.804878")
    assert abs(_cash_balance(db, "Trii") - expected) < Decimal("0.000001")


# --- 6. Fund COP missing fx_rate → reject ---

def test_fund_cop_without_fx_rate_rejected(tx_svc):
    with pytest.raises(InvalidTransaction, match="fx_rate is required"):
        tx_svc.record_fund_movement(
            account="Trii",
            symbol="FONDO DINAMICO",
            movement_date="2026-04-19",
            amount=Decimal("5000000"),
            movement_type="CONTRIBUTION",
            currency="COP",
            fx_rate=None,
        )


# --- 7. Fund COP over-balance (in USD) blocked ---

def test_fund_cop_overbalance_blocked(tx_svc):
    tx_svc.record_fund_movement(
        account="Trii",
        symbol="FONDO DINAMICO",
        movement_date="2026-04-19",
        amount=Decimal("4000000"),
        movement_type="CONTRIBUTION",
        currency="COP",
        fx_rate=Decimal("4000"),
    )  # = 1000 USD
    with pytest.raises(InvalidTransaction, match="Insufficient fund balance"):
        tx_svc.record_fund_movement(
            account="Trii",
            symbol="FONDO DINAMICO",
            movement_date="2026-04-25",
            amount=Decimal("8000000"),  # = 2000 USD > 1000
            movement_type="WITHDRAWAL",
            currency="COP",
            fx_rate=Decimal("4000"),
        )


# --- 8. CDT COP settlement: realized PnL = interest in USD, no FX PnL ---

def test_cdt_cop_settlement_realized_pnl_is_interest_only(tx_svc, pnl_svc, db):
    buy_tx_id = tx_svc.record_cdt(
        account="BBVA",
        symbol="BBVA CDT",
        open_date="2026-01-01",
        maturity_date="2027-01-01",
        principal=Decimal("4000000"),
        term_years=Decimal("1.0"),
        annual_rate=Decimal("0.10"),
        currency="COP",
        fx_rate_at_open=Decimal("4000"),
    )
    # principal_usd = 4_000_000 / 4000 = 1000
    # term derived from dates (~365/365 = 1.0); interest_usd = 1000 * 1 * 0.10 = 100
    result = tx_svc.settle_cdt(buy_tx_id=buy_tx_id, settlement_date="2027-01-01")
    assert result["principal"] == Decimal("1000")
    assert result["interest"] == Decimal("100")
    assert result["maturity_value"] == Decimal("1100")

    realized = pnl_svc.realized_pnl(symbol="BBVA CDT", account="BBVA")
    assert realized == Decimal("100")

    # Cash flow: -1000 at open + 1100 at settlement = +100 net
    assert _cash_balance(db, "BBVA") == Decimal("100")


# --- 9. Summary: COP fund balance classified as Non-market Valued ---

def test_summary_classifies_cop_fund_as_non_market_valued(tx_svc, pnl_svc, db):
    tx_svc.record_fund_movement(
        account="Trii",
        symbol="FONDO DINAMICO",
        movement_date="2026-04-19",
        amount=Decimal("5000000"),
        movement_type="CONTRIBUTION",
        currency="COP",
        fx_rate=Decimal("4000"),
    )
    s = pnl_svc.summary()
    # 1250 USD valued as non-market, not unvalued
    assert s["non_market_valued"] == Decimal("1250")
    assert s["unvalued_excluded_cost_basis"] == Decimal("0")
    assert s["asset_class_breakdown"]["Non-market"] == Decimal("1250")


# --- 10. No artificial FX PnL: open + settle CDT COP ---

def test_cdt_cop_no_artificial_fx_pnl(tx_svc, pnl_svc):
    buy_tx_id = tx_svc.record_cdt(
        account="BBVA",
        symbol="BBVA CDT",
        open_date="2026-01-01",
        maturity_date="2027-01-01",
        principal=Decimal("4000000"),
        term_years=Decimal("1.0"),
        annual_rate=Decimal("0.10"),
        currency="COP",
        fx_rate_at_open=Decimal("4000"),
    )
    result = tx_svc.settle_cdt(buy_tx_id=buy_tx_id, settlement_date="2027-01-01")
    # Realized PnL is exactly interest in USD; no extra FX-driven PnL.
    realized = pnl_svc.realized_pnl(symbol="BBVA CDT", account="BBVA")
    assert realized == result["interest"]
    assert realized == Decimal("100")


# --- bonus: invalid currency ---

def test_unsupported_currency_rejected(tx_svc):
    with pytest.raises(InvalidTransaction, match="currency must be one of"):
        tx_svc.record_cdt(
            account="BBVA",
            symbol="BBVA CDT",
            open_date="2026-04-01",
            maturity_date="2026-10-01",
            principal=Decimal("100"),
            term_years=None,
            annual_rate=Decimal("0.10"),
            currency="EUR",
        )
    with pytest.raises(InvalidTransaction, match="currency must be one of"):
        tx_svc.record_fund_movement(
            account="Trii",
            symbol="FONDO DINAMICO",
            movement_date="2026-04-19",
            amount=Decimal("100"),
            movement_type="CONTRIBUTION",
            currency="EUR",
        )


# --- Bug 2 regressions: fund balance detection via note prefix ---

def test_fund_cop_arbitrary_symbol_detected_as_fund_balance(tx_svc, pnl_svc):
    """A fund with symbol outside FUND_BALANCE_SYMBOLS must still be treated as
    a fund balance when its transactions carry the FUND_CONTRIBUTION/WITHDRAWAL
    note prefix. Before the fix this produced qty_open=-1 after two SELLs."""
    tx_svc.record_fund_movement(
        account="Trii",
        symbol="FONDO DINAMICO COP",  # NOT in FUND_BALANCE_SYMBOLS
        movement_date="2026-04-10",
        amount=Decimal("4000000"),
        movement_type="CONTRIBUTION",
        currency="COP",
        fx_rate=Decimal("4000"),
    )  # = 1000 USD
    tx_svc.record_fund_movement(
        account="Trii",
        symbol="FONDO DINAMICO COP",
        movement_date="2026-04-12",
        amount=Decimal("1000000"),
        movement_type="WITHDRAWAL",
        currency="COP",
        fx_rate=Decimal("4000"),
    )  # = 250 USD
    positions = pnl_svc.positions("Trii")
    rows = [p for p in positions if p["symbol"] == "FONDO DINAMICO COP"]
    assert len(rows) == 1
    pos = rows[0]
    # Fund balance logic: qty=balance, cost=balance, avg_cost=1
    assert pos["qty_open"] == Decimal("750")
    assert pos["cost_basis"] == Decimal("750")
    assert pos["avg_cost"] == Decimal("1")
    assert pos["approved_value"] == Decimal("750")


def test_fund_cop_two_withdrawals_preserve_positive_qty(tx_svc, pnl_svc):
    """Two COP withdrawals (each < balance) must not produce negative qty_open."""
    tx_svc.record_fund_movement(
        account="Trii",
        symbol="FONDO DINAMICO",
        movement_date="2026-04-10",
        amount=Decimal("8000000"),
        movement_type="CONTRIBUTION",
        currency="COP",
        fx_rate=Decimal("4000"),
    )  # = 2000 USD
    tx_svc.record_fund_movement(
        account="Trii",
        symbol="FONDO DINAMICO",
        movement_date="2026-04-12",
        amount=Decimal("2000000"),
        movement_type="WITHDRAWAL",
        currency="COP",
        fx_rate=Decimal("4000"),
    )  # = 500 USD
    tx_svc.record_fund_movement(
        account="Trii",
        symbol="FONDO DINAMICO",
        movement_date="2026-04-14",
        amount=Decimal("2000000"),
        movement_type="WITHDRAWAL",
        currency="COP",
        fx_rate=Decimal("4000"),
    )  # = 500 USD. Balance = 2000 - 500 - 500 = 1000 USD
    positions = pnl_svc.positions("Trii")
    rows = [p for p in positions if p["symbol"] == "FONDO DINAMICO"]
    assert len(rows) == 1
    assert rows[0]["qty_open"] == Decimal("1000")
    assert rows[0]["qty_open"] > 0


def test_fund_usd_unchanged(tx_svc, db):
    tx_id = tx_svc.record_fund_movement(
        account="Trii",
        symbol="FONDO DINAMICO",
        movement_date="2026-04-19",
        amount=Decimal("1250"),
        movement_type="CONTRIBUTION",
    )
    cur = db.connect().cursor()
    cur.execute("SELECT notes, total_usd FROM transactions WHERE id = ?", (tx_id,))
    notes, total_usd = cur.fetchone()
    assert notes == "FUND_CONTRIBUTION"
    assert Decimal(str(total_usd)) == Decimal("1250")
    assert _cash_balance(db, "Trii") == Decimal("-1250")
