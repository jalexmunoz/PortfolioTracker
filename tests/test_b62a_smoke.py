# -*- coding: utf-8 -*-
"""Smoke manual B62A -- DB desechable. Se elimina despues del smoke."""
from decimal import Decimal


def test_b62a_smoke_manual(tmp_path):
    from portfolio_tracker_v2.core.database import Database
    from portfolio_tracker_v2.core.asset_resolver import AssetResolver
    from portfolio_tracker_v2.services.transaction_svc import TransactionService
    from portfolio_tracker_v2.services.pnl_svc import PnLService
    from portfolio_tracker_v2.services.signal_svc import SignalService, VALID_SEVERITIES

    # DB desechable
    db_path = tmp_path / "b62a_smoke.db"
    db = Database(str(db_path))
    db.connect()
    db.init_schema()
    resolver = AssetResolver(db)
    resolver.get_or_create_usd_cash()
    tx_svc = TransactionService(db, resolver)
    pnl_svc = PnLService(db, resolver)
    sig_svc = SignalService(db)

    # Seed portfolio
    tx_svc.record_buy("BTC", "TestWallet", qty=1, unit_price=50000, fee_usd=10, tx_date="2026-01-01")
    tx_svc.record_cash_movement("TestWallet", "2026-01-01", amount=55010, movement_type="DEPOSIT")

    summary_before = pnl_svc.summary()
    pos_before = {(p["symbol"], p["account"]): p["qty_open"] for p in pnl_svc.positions()}
    cash_before = summary_before["cash_balance"]

    print("\n" + "=" * 60)
    print("SMOKE B62A -- DB desechable")
    print("  DB                  : " + db_path.name)
    print("  Cash before signals : " + str(cash_before))
    print("  Equity before       : " + str(summary_before["total_equity"]))

    # STEP 1 -- Review step: validate, NO write
    def validate_signal(d):
        severity = (d.get("severity") or "INFO").upper()
        assert severity in VALID_SEVERITIES
        assert d.get("source", "").strip()
        assert d.get("event_type", "").strip()
        assert d.get("event_time", "").strip()
        return d

    form = {
        "event_time": "2026-05-08T10:00:00",
        "source": "tradingview_macro",
        "event_type": "hard_risk_off_activated",
        "severity": "HIGH",
        "message": "Macro hard risk-off activated",
        "notes": "B62A smoke test",
    }
    parsed = validate_signal(form)
    count_after_review = db.connect().execute("SELECT COUNT(*) FROM signal_alerts").fetchone()[0]
    assert count_after_review == 0, "FAIL: review step wrote to DB"
    print("\nSTEP 1 [OK]  Review step: NOT written to DB (count=0)")

    # STEP 2 -- Confirm: writes exactly one row
    sig_id = sig_svc.add_signal(**parsed)
    count_after_confirm = db.connect().execute("SELECT COUNT(*) FROM signal_alerts").fetchone()[0]
    assert count_after_confirm == 1, "FAIL: expected 1 row, got " + str(count_after_confirm)
    print("STEP 2 [OK]  Confirm: 1 row written (id=" + str(sig_id) + ")")

    # STEP 3 -- Signal appears OPEN
    rows = sig_svc.list_signals()
    assert len(rows) == 1
    assert rows[0]["status"] == "OPEN"
    assert rows[0]["event_type"] == "hard_risk_off_activated"
    assert rows[0]["severity"] == "HIGH"
    print("STEP 3 [OK]  Signal OPEN -- type=" + rows[0]["event_type"] + "  sev=" + rows[0]["severity"])

    # STEP 4 -- Dashboard data: recent_open_signals
    recent = sig_svc.recent_open_signals(limit=5)
    open_count = sig_svc.count_open_signals()
    assert len(recent) == 1
    assert open_count == 1
    print("STEP 4 [OK]  Dashboard: " + str(open_count) + " open signal visible -- event=" + recent[0]["event_type"])

    # STEP 5 -- Mark RESOLVED
    found = sig_svc.update_signal_status(sig_id, "RESOLVED")
    assert found is True
    resolved_row = db.connect().execute(
        "SELECT status, resolved_at FROM signal_alerts WHERE id=?", (sig_id,)
    ).fetchone()
    assert resolved_row["status"] == "RESOLVED"
    assert resolved_row["resolved_at"] is not None
    assert sig_svc.count_open_signals() == 0
    assert sig_svc.recent_open_signals() == []
    print("STEP 5 [OK]  Mark RESOLVED -- resolved_at=" + str(resolved_row["resolved_at"]))
    print("             Open signals now: 0 (removed from dashboard)")

    # STEP 6 -- Mark IGNORED
    sig_id2 = sig_svc.add_signal(
        event_time="2026-05-08T11:00:00",
        source="manual",
        event_type="caution",
        severity="MEDIUM",
        message="Caution signal",
    )
    found2 = sig_svc.update_signal_status(sig_id2, "IGNORED")
    assert found2 is True
    row2 = db.connect().execute(
        "SELECT status FROM signal_alerts WHERE id=?", (sig_id2,)
    ).fetchone()
    assert row2["status"] == "IGNORED"
    print("STEP 6 [OK]  Mark IGNORED (id=" + str(sig_id2) + ")")

    # STEP 7 -- Cash / Equity / Positions UNCHANGED
    summary_after = pnl_svc.summary()
    pos_after = {(p["symbol"], p["account"]): p["qty_open"] for p in pnl_svc.positions()}
    cash_after = summary_after["cash_balance"]

    assert cash_before == cash_after, "FAIL: cash " + str(cash_before) + " -> " + str(cash_after)
    assert summary_before["total_equity"] == summary_after["total_equity"], "FAIL: equity changed"
    assert summary_before["total_cost_basis"] == summary_after["total_cost_basis"], "FAIL: cost basis"
    assert summary_before["total_realized_pnl"] == summary_after["total_realized_pnl"], "FAIL: realized pnl"
    assert pos_before == pos_after, "FAIL: positions changed"
    print("STEP 7 [OK]  Cash=" + str(cash_after) + "  Equity=" + str(summary_after["total_equity"]))
    print("             Realized PnL=" + str(summary_after["total_realized_pnl"]) + "  Positions unchanged")
    print("             Cash/Equity/Positions: NO CHANGE")

    # Final summary
    total = db.connect().execute("SELECT COUNT(*) FROM signal_alerts").fetchone()[0]
    resolved_n = db.connect().execute(
        "SELECT COUNT(*) FROM signal_alerts WHERE status='RESOLVED'"
    ).fetchone()[0]
    ignored_n = db.connect().execute(
        "SELECT COUNT(*) FROM signal_alerts WHERE status='IGNORED'"
    ).fetchone()[0]
    print("\n" + "=" * 60)
    print("ALL SMOKE STEPS PASSED")
    print("  Signals total : " + str(total))
    print("  OPEN          : " + str(sig_svc.count_open_signals()))
    print("  RESOLVED      : " + str(resolved_n))
    print("  IGNORED       : " + str(ignored_n))
    print("  DB            : desechable (tmp_path auto-cleaned)")
    db.close()
