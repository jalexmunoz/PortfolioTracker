"""
B62A — Tests for signal_alerts schema, SignalService, and financial isolation.

Tests:
  1.  signal_table_created_fresh_db          — table exists in new DB
  2.  signal_service_on_existing_db          — existing DB migrates safely
  3.  add_signal_persists                    — row stored after add_signal()
  4.  list_signals_ordered_desc              — ordered by event_time DESC, id DESC
  5.  list_signals_filter_status             — status filter works
  6.  list_signals_filter_severity           — severity filter works
  7.  list_signals_filter_source             — source filter works
  8.  update_status_open_to_resolved         — OPEN → RESOLVED
  9.  update_status_open_to_ignored          — OPEN → IGNORED
  10. count_open_signals                     — count correct
  11. recent_open_signals_limit              — respects limit
  12. summary_unchanged_after_add_signal     — financial isolation: summary
  13. positions_unchanged_after_add_signal   — financial isolation: positions
  14. cash_unchanged_after_add_signal        — financial isolation: cash
"""
import pytest

from portfolio_tracker_v2.core.database import Database
from portfolio_tracker_v2.core.asset_resolver import AssetResolver
from portfolio_tracker_v2.services.signal_svc import SignalService, VALID_SEVERITIES, VALID_STATUSES
from portfolio_tracker_v2.services.transaction_svc import TransactionService
from portfolio_tracker_v2.services.pnl_svc import PnLService


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def fresh_db(tmp_path):
    db = Database(str(tmp_path / "sig_test.db"))
    db.connect()
    db.init_schema()
    return db


@pytest.fixture
def sig_svc(fresh_db):
    return SignalService(fresh_db)


@pytest.fixture
def funded_db(tmp_path):
    """DB with one BUY and one cash deposit — for financial isolation tests."""
    db = Database(str(tmp_path / "funded.db"))
    db.connect()
    db.init_schema()
    resolver = AssetResolver(db)
    resolver.get_or_create_usd_cash()
    tx_svc = TransactionService(db, resolver)
    tx_svc.record_buy(
        symbol="BTC", account="TestWallet",
        qty=1, unit_price=50000, fee_usd=10,
        tx_date="2026-01-01",
    )
    tx_svc.record_cash_movement(
        account="TestWallet", movement_date="2026-01-01",
        amount=1000, movement_type="DEPOSIT",
    )
    return db


# ---------------------------------------------------------------------------
# 1. Table created in new DB
# ---------------------------------------------------------------------------

def test_signal_table_created_fresh_db(fresh_db):
    conn = fresh_db.connect()
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    assert "signal_alerts" in tables


# ---------------------------------------------------------------------------
# 2. DB existing before B62A migrates safely
# ---------------------------------------------------------------------------

def test_signal_service_on_existing_db(tmp_path):
    """Simulate an older DB that didn't have signal_alerts; calling init_schema
    (or _ensure_signal_alerts_schema directly) must not raise and must create
    the table so SignalService works immediately."""
    db = Database(str(tmp_path / "old.db"))
    db.connect()
    # Create a minimal DB without signal_alerts first
    db.connect().executescript(
        """
        CREATE TABLE IF NOT EXISTS assets (id INTEGER PRIMARY KEY, symbol TEXT UNIQUE NOT NULL,
            asset_type TEXT NOT NULL, current_price REAL, price_source TEXT,
            price_updated_at TIMESTAMP, tradingview_symbol TEXT, exchange TEXT,
            currency TEXT DEFAULT 'USD', divisor REAL DEFAULT 1.0,
            valuation_method TEXT DEFAULT 'unvalued', is_active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
        """
    )
    db.commit()
    # Now run the migration
    db._ensure_signal_alerts_schema()
    # Should work without error
    svc = SignalService(db)
    sig_id = svc.add_signal(
        event_time="2026-05-01T10:00:00",
        source="manual",
        event_type="custom",
        severity="INFO",
        message="migration test",
    )
    assert sig_id > 0


# ---------------------------------------------------------------------------
# 3. add_signal persists row
# ---------------------------------------------------------------------------

def test_add_signal_persists(sig_svc, fresh_db):
    sig_id = sig_svc.add_signal(
        event_time="2026-05-01T09:00:00",
        source="tradingview_macro",
        event_type="hard_risk_off_activated",
        severity="HIGH",
        message="Hard risk-off activated",
        notes="Test note",
    )
    assert sig_id > 0
    conn = fresh_db.connect()
    row = conn.execute(
        "SELECT * FROM signal_alerts WHERE id = ?", (sig_id,)
    ).fetchone()
    assert row is not None
    assert row["event_type"] == "hard_risk_off_activated"
    assert row["severity"] == "HIGH"
    assert row["status"] == "OPEN"
    assert row["source"] == "tradingview_macro"
    assert row["notes"] == "Test note"


# ---------------------------------------------------------------------------
# 4. list_signals ordered desc
# ---------------------------------------------------------------------------

def test_list_signals_ordered_desc(sig_svc):
    sig_svc.add_signal("2026-01-01T10:00:00", "manual", "caution", "LOW", "first")
    sig_svc.add_signal("2026-01-03T10:00:00", "manual", "stress", "HIGH", "third")
    sig_svc.add_signal("2026-01-02T10:00:00", "manual", "risk_off", "MEDIUM", "second")

    rows = sig_svc.list_signals()
    assert len(rows) == 3
    # Most recent event_time first
    assert rows[0]["event_time"] == "2026-01-03T10:00:00"
    assert rows[1]["event_time"] == "2026-01-02T10:00:00"
    assert rows[2]["event_time"] == "2026-01-01T10:00:00"


# ---------------------------------------------------------------------------
# 5. list_signals filter by status
# ---------------------------------------------------------------------------

def test_list_signals_filter_status(sig_svc):
    id1 = sig_svc.add_signal("2026-01-01T10:00:00", "manual", "caution", "LOW")
    id2 = sig_svc.add_signal("2026-01-02T10:00:00", "manual", "stress", "HIGH")
    sig_svc.update_signal_status(id1, "RESOLVED")

    open_rows = sig_svc.list_signals(status="OPEN")
    resolved_rows = sig_svc.list_signals(status="RESOLVED")

    assert len(open_rows) == 1
    assert open_rows[0]["id"] == id2
    assert len(resolved_rows) == 1
    assert resolved_rows[0]["id"] == id1


# ---------------------------------------------------------------------------
# 6. list_signals filter by severity
# ---------------------------------------------------------------------------

def test_list_signals_filter_severity(sig_svc):
    sig_svc.add_signal("2026-01-01T10:00:00", "manual", "caution", "LOW")
    sig_svc.add_signal("2026-01-02T10:00:00", "manual", "stress", "CRITICAL")
    sig_svc.add_signal("2026-01-03T10:00:00", "manual", "risk_off", "CRITICAL")

    rows = sig_svc.list_signals(severity="CRITICAL")
    assert len(rows) == 2
    assert all(r["severity"] == "CRITICAL" for r in rows)

    rows_low = sig_svc.list_signals(severity="LOW")
    assert len(rows_low) == 1


# ---------------------------------------------------------------------------
# 7. list_signals filter by source
# ---------------------------------------------------------------------------

def test_list_signals_filter_source(sig_svc):
    sig_svc.add_signal("2026-01-01T10:00:00", "tradingview_macro", "caution", "LOW")
    sig_svc.add_signal("2026-01-02T10:00:00", "manual", "stress", "HIGH")

    tv_rows = sig_svc.list_signals(source="tradingview_macro")
    assert len(tv_rows) == 1
    assert tv_rows[0]["source"] == "tradingview_macro"

    manual_rows = sig_svc.list_signals(source="manual")
    assert len(manual_rows) == 1


# ---------------------------------------------------------------------------
# 8. update_status OPEN → RESOLVED
# ---------------------------------------------------------------------------

def test_update_status_open_to_resolved(sig_svc):
    sig_id = sig_svc.add_signal("2026-05-01T10:00:00", "manual", "custom", "INFO")
    found = sig_svc.update_signal_status(sig_id, "RESOLVED")
    assert found is True

    rows = sig_svc.list_signals(status="RESOLVED")
    assert any(r["id"] == sig_id for r in rows)
    row = next(r for r in rows if r["id"] == sig_id)
    assert row["status"] == "RESOLVED"
    assert row["resolved_at"] is not None


# ---------------------------------------------------------------------------
# 9. update_status OPEN → IGNORED
# ---------------------------------------------------------------------------

def test_update_status_open_to_ignored(sig_svc):
    sig_id = sig_svc.add_signal("2026-05-01T10:00:00", "manual", "custom", "INFO")
    found = sig_svc.update_signal_status(sig_id, "IGNORED")
    assert found is True

    rows = sig_svc.list_signals(status="IGNORED")
    assert any(r["id"] == sig_id for r in rows)


# ---------------------------------------------------------------------------
# 10. count_open_signals
# ---------------------------------------------------------------------------

def test_count_open_signals(sig_svc):
    assert sig_svc.count_open_signals() == 0
    id1 = sig_svc.add_signal("2026-01-01T10:00:00", "manual", "caution", "LOW")
    id2 = sig_svc.add_signal("2026-01-02T10:00:00", "manual", "stress", "HIGH")
    assert sig_svc.count_open_signals() == 2
    sig_svc.update_signal_status(id1, "RESOLVED")
    assert sig_svc.count_open_signals() == 1
    sig_svc.update_signal_status(id2, "IGNORED")
    assert sig_svc.count_open_signals() == 0


# ---------------------------------------------------------------------------
# 11. recent_open_signals respects limit
# ---------------------------------------------------------------------------

def test_recent_open_signals_limit(sig_svc):
    for i in range(7):
        sig_svc.add_signal(f"2026-01-{i+1:02d}T10:00:00", "manual", "caution", "LOW")

    recent = sig_svc.recent_open_signals(limit=3)
    assert len(recent) == 3
    # All should be OPEN
    assert all(r["status"] == "OPEN" for r in recent)


# ---------------------------------------------------------------------------
# 12. summary unchanged after adding signals
# ---------------------------------------------------------------------------

def test_summary_unchanged_after_add_signal(funded_db):
    pnl_svc = PnLService(funded_db, AssetResolver(funded_db))
    summary_before = pnl_svc.summary()

    sig_svc = SignalService(funded_db)
    for i in range(5):
        sig_svc.add_signal(
            f"2026-05-0{i+1}T10:00:00", "manual", "risk_off", "HIGH",
            f"signal {i}"
        )

    summary_after = pnl_svc.summary()

    assert summary_before["total_equity"] == summary_after["total_equity"]
    assert summary_before["total_cost_basis"] == summary_after["total_cost_basis"]
    assert summary_before["total_realized_pnl"] == summary_after["total_realized_pnl"]


# ---------------------------------------------------------------------------
# 13. positions unchanged after adding signals
# ---------------------------------------------------------------------------

def test_positions_unchanged_after_add_signal(funded_db):
    pnl_svc = PnLService(funded_db, AssetResolver(funded_db))
    pos_before = {
        (p["symbol"], p["account"]): p["qty_open"]
        for p in pnl_svc.positions()
    }

    sig_svc = SignalService(funded_db)
    sig_svc.add_signal("2026-05-01T10:00:00", "system", "stress", "CRITICAL", "isolation")

    pos_after = {
        (p["symbol"], p["account"]): p["qty_open"]
        for p in pnl_svc.positions()
    }
    assert pos_before == pos_after


# ---------------------------------------------------------------------------
# 14. cash unchanged after adding signals
# ---------------------------------------------------------------------------

def test_cash_unchanged_after_add_signal(funded_db):
    pnl_svc = PnLService(funded_db, AssetResolver(funded_db))
    cash_before = pnl_svc.summary()["cash_balance"]

    sig_svc = SignalService(funded_db)
    sig_svc.add_signal("2026-05-01T10:00:00", "manual", "oversold", "MEDIUM")

    cash_after = pnl_svc.summary()["cash_balance"]
    assert cash_before == cash_after


# ---------------------------------------------------------------------------
# Validation tests
# ---------------------------------------------------------------------------

def test_add_signal_invalid_severity(sig_svc):
    with pytest.raises(ValueError, match="Invalid severity"):
        sig_svc.add_signal("2026-01-01T10:00:00", "manual", "caution", "EXTREME")


def test_add_signal_missing_source(sig_svc):
    with pytest.raises(ValueError, match="source is required"):
        sig_svc.add_signal("2026-01-01T10:00:00", "", "caution", "LOW")


def test_update_signal_invalid_status(sig_svc):
    sig_id = sig_svc.add_signal("2026-01-01T10:00:00", "manual", "caution", "LOW")
    with pytest.raises(ValueError, match="Invalid status"):
        sig_svc.update_signal_status(sig_id, "DELETED")


def test_update_signal_nonexistent_id(sig_svc):
    found = sig_svc.update_signal_status(99999, "RESOLVED")
    assert found is False
