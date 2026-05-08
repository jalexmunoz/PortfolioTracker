# -*- coding: utf-8 -*-
"""Smoke manual B62B -- TradingView webhook. DB desechable."""
import json


def test_b62b_smoke_webhook(tmp_path, monkeypatch):
    from portfolio_tracker_v2.core.database import Database
    from portfolio_tracker_v2.core.asset_resolver import AssetResolver
    from portfolio_tracker_v2.services.transaction_svc import TransactionService
    from portfolio_tracker_v2.services.pnl_svc import PnLService
    from portfolio_tracker_v2.services.signal_svc import SignalService
    from portfolio_tracker_v2.gui.app import create_app

    _TOKEN = "test-secret"
    instance_path = tmp_path / "instance"
    instance_path.mkdir()

    db_path = tmp_path / "b62b_smoke.db"
    db = Database(str(db_path))
    db.connect()
    db.init_schema()
    resolver = AssetResolver(db)
    resolver.get_or_create_usd_cash()
    tx_svc = TransactionService(db, resolver)
    pnl_svc = PnLService(db, resolver)

    tx_svc.record_buy("BTC", "TestWallet", qty=1, unit_price=50000, fee_usd=0, tx_date="2026-01-01")
    tx_svc.record_cash_movement("TestWallet", "2026-01-01", amount=55000, movement_type="DEPOSIT")

    summary_before = pnl_svc.summary()
    pos_before = {(p["symbol"], p["account"]): p["qty_open"] for p in pnl_svc.positions()}
    db.close()

    state = {"db_path": str(db_path), "mode": "TEST"}
    (instance_path / "gui_state.json").write_text(json.dumps(state), encoding="utf-8")

    monkeypatch.setenv("PORTFOLIO_WEBHOOK_TOKEN", _TOKEN)
    monkeypatch.setenv("PORTFOLIO_DB_PATH", str(db_path))

    app = create_app(instance_path=str(instance_path))
    app.config.update({"TESTING": True})
    client = app.test_client()

    print("\n" + "=" * 60)
    print("SMOKE B62B -- TradingView Webhook Receiver")
    print("  DB: " + db_path.name)

    # STEP 1 -- text/plain webhook, valid token via ?token=
    resp = client.post(
        "/webhooks/tradingview/macro?token=" + _TOKEN,
        data=(
            "event_type=hard_risk_off_activated\n"
            "regime=RISK-OFF\n"
            "score=-3.25\n"
            "message=Macro hard risk-off activated"
        ),
        content_type="text/plain",
    )
    assert resp.status_code == 201, "FAIL step 1: expected 201, got " + str(resp.status_code)
    body = resp.get_json()
    assert body["ok"] is True
    sig_id = body["signal_id"]
    print("\nSTEP 1 [OK]  text/plain webhook accepted -- signal_id=" + str(sig_id))

    # STEP 2 -- signal appears OPEN in signals page
    resp2 = client.get("/signals")
    assert resp2.status_code == 200
    assert b"hard_risk_off_activated" in resp2.data
    print("STEP 2 [OK]  /signals lists signal OPEN -- event_type=hard_risk_off_activated")

    # STEP 3 -- dashboard shows recent signal
    resp3 = client.get("/")
    assert resp3.status_code == 200
    assert b"hard_risk_off_activated" in resp3.data
    print("STEP 3 [OK]  Dashboard shows Recent Signal -- hard_risk_off_activated")

    # STEP 4 -- JSON webhook
    resp4 = client.post(
        "/webhooks/tradingview/macro?token=" + _TOKEN,
        data=json.dumps({
            "event_type": "confirmed_downgrade",
            "regime": "RISK-OFF",
            "score": -2.75,
            "message": "Confirmed regime downgrade",
        }),
        content_type="application/json",
    )
    assert resp4.status_code == 201
    sig_id2 = resp4.get_json()["signal_id"]
    print("STEP 4 [OK]  JSON webhook accepted -- signal_id=" + str(sig_id2))

    # STEP 5 -- invalid token returns 403, no new signal
    count_before = _count_signals(str(db_path))
    resp5 = client.post(
        "/webhooks/tradingview/macro?token=wrong",
        data="event_type=caution",
        content_type="text/plain",
    )
    assert resp5.status_code == 403
    assert resp5.get_json()["ok"] is False
    assert _count_signals(str(db_path)) == count_before
    print("STEP 5 [OK]  Invalid token returns 403 -- no signal written")

    # STEP 6 -- missing env token returns 503
    monkeypatch.delenv("PORTFOLIO_WEBHOOK_TOKEN", raising=False)
    resp6 = client.post(
        "/webhooks/tradingview/macro?token=" + _TOKEN,
        data="event_type=caution",
        content_type="text/plain",
    )
    assert resp6.status_code == 503
    assert resp6.get_json()["ok"] is False
    print("STEP 6 [OK]  No env token returns 503")
    monkeypatch.setenv("PORTFOLIO_WEBHOOK_TOKEN", _TOKEN)

    # STEP 7 -- severity mapping
    resp7 = client.post(
        "/webhooks/tradingview/macro?token=" + _TOKEN,
        data="event_type=stress",
        content_type="text/plain",
    )
    assert resp7.status_code == 201
    sig = _get_signal_by_id(str(db_path), resp7.get_json()["signal_id"])
    assert sig["severity"] == "CRITICAL"
    print("STEP 7 [OK]  stress -> CRITICAL severity confirmed")

    # STEP 8 -- no backup created
    from pathlib import Path
    auto_backup_dir = Path(str(instance_path)) / "backups"
    backup_files = list(auto_backup_dir.glob("*")) if auto_backup_dir.exists() else []
    assert len(backup_files) == 0, "FAIL: unexpected backup files created by webhook"
    print("STEP 8 [OK]  No backup files created by webhook")

    # STEP 9 -- cash/equity/positions unchanged
    db2 = Database(str(db_path))
    db2.connect()
    resolver2 = AssetResolver(db2)
    pnl_svc2 = PnLService(db2, resolver2)
    summary_after = pnl_svc2.summary()
    pos_after = {(p["symbol"], p["account"]): p["qty_open"] for p in pnl_svc2.positions()}
    db2.close()

    assert summary_before["total_equity"] == summary_after["total_equity"]
    assert summary_before["cash_balance"] == summary_after["cash_balance"]
    assert summary_before["total_realized_pnl"] == summary_after["total_realized_pnl"]
    assert pos_before == pos_after
    print("STEP 9 [OK]  Cash/Equity/Positions: NO CHANGE after webhooks")

    total = _count_signals(str(db_path))
    print("\n" + "=" * 60)
    print("ALL SMOKE STEPS PASSED")
    print("  Signals total: " + str(total))
    print("  DB: desechable (tmp_path auto-cleaned)")


def _count_signals(db_path):
    from portfolio_tracker_v2.core.database import Database
    db = Database(db_path)
    db.connect()
    n = db.connect().execute("SELECT COUNT(*) FROM signal_alerts").fetchone()[0]
    db.close()
    return n


def _get_signal_by_id(db_path, sig_id):
    from portfolio_tracker_v2.core.database import Database
    db = Database(db_path)
    db.connect()
    row = db.connect().execute(
        "SELECT * FROM signal_alerts WHERE id=?", (sig_id,)
    ).fetchone()
    db.close()
    return dict(row)
