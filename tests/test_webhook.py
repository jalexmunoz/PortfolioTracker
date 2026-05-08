"""
B62B -- Tests for TradingView webhook receiver.

Tests:
  1.  text_plain_valid_creates_signal
  2.  json_valid_creates_signal
  3.  missing_event_type_returns_400_no_write
  4.  invalid_token_returns_403_no_write
  5.  missing_env_token_returns_503_no_write
  6.  severity_map_hard_risk_off_high
  7.  severity_map_stress_critical
  8.  payload_json_stored_verbatim
  9.  default_source_is_tradingview_macro
  10. default_status_is_open
  11. dashboard_shows_webhook_signal
  12. signals_page_lists_webhook_signal
  13. webhook_does_not_modify_cash_equity_positions
  14. no_backup_created_by_webhook
"""
import json
import os

import pytest

from portfolio_tracker_v2.core.database import Database
from portfolio_tracker_v2.core.asset_resolver import AssetResolver
from portfolio_tracker_v2.services.transaction_svc import TransactionService
from portfolio_tracker_v2.services.pnl_svc import PnLService
from portfolio_tracker_v2.services.signal_svc import SignalService
from portfolio_tracker_v2.gui.app import create_app


_TOKEN = "test-webhook-secret"

_TEXT_PAYLOAD = (
    "event_type=hard_risk_off_activated\n"
    "regime=RISK-OFF\n"
    "score=-3.25\n"
    "message=Macro hard risk-off activated"
)

_JSON_PAYLOAD = {
    "event_type": "confirmed_downgrade",
    "regime": "RISK-OFF",
    "score": -2.75,
    "message": "Confirmed regime downgrade",
    "symbol": "MACRO",
    "source": "tradingview_macro",
}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def wh_env(tmp_path, monkeypatch):
    """
    Flask app with active DB + PORTFOLIO_WEBHOOK_TOKEN set.
    Uses PORTFOLIO_DB_PATH env var so the webhook finds the DB.
    """
    instance_path = tmp_path / "instance"
    instance_path.mkdir()

    db_path = tmp_path / "wh_test.db"
    db = Database(str(db_path))
    db.connect()
    db.init_schema()
    AssetResolver(db).get_or_create_usd_cash()
    db.close()

    # Write gui_state so load_active_db() also works
    state = {"db_path": str(db_path), "mode": "TEST"}
    (instance_path / "gui_state.json").write_text(json.dumps(state), encoding="utf-8")

    monkeypatch.setenv("PORTFOLIO_WEBHOOK_TOKEN", _TOKEN)
    monkeypatch.setenv("PORTFOLIO_DB_PATH", str(db_path))

    app = create_app(instance_path=str(instance_path))
    app.config.update({"TESTING": True})

    return {
        "app": app,
        "client": app.test_client(),
        "db_path": str(db_path),
        "instance_path": str(instance_path),
    }


@pytest.fixture
def wh_no_token(tmp_path, monkeypatch):
    """App with a DB but NO webhook token configured."""
    instance_path = tmp_path / "instance"
    instance_path.mkdir()

    db_path = tmp_path / "wh_notoken.db"
    db = Database(str(db_path))
    db.connect()
    db.init_schema()
    db.close()

    state = {"db_path": str(db_path), "mode": "TEST"}
    (instance_path / "gui_state.json").write_text(json.dumps(state), encoding="utf-8")

    monkeypatch.delenv("PORTFOLIO_WEBHOOK_TOKEN", raising=False)
    monkeypatch.setenv("PORTFOLIO_DB_PATH", str(db_path))

    app = create_app(instance_path=str(instance_path))
    app.config.update({"TESTING": True})

    return {
        "client": app.test_client(),
        "db_path": str(db_path),
    }


def _signal_count(db_path):
    db = Database(db_path)
    db.connect()
    count = db.connect().execute("SELECT COUNT(*) FROM signal_alerts").fetchone()[0]
    db.close()
    return count


def _get_signals(db_path):
    db = Database(db_path)
    db.connect()
    rows = db.connect().execute(
        "SELECT * FROM signal_alerts ORDER BY id"
    ).fetchall()
    db.close()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# 1. text/plain valid payload creates signal
# ---------------------------------------------------------------------------

def test_text_plain_valid_creates_signal(wh_env):
    resp = wh_env["client"].post(
        f"/webhooks/tradingview/macro?token={_TOKEN}",
        data=_TEXT_PAYLOAD,
        content_type="text/plain",
    )
    assert resp.status_code == 201
    body = resp.get_json()
    assert body["ok"] is True
    assert isinstance(body["signal_id"], int)
    assert _signal_count(wh_env["db_path"]) == 1


# ---------------------------------------------------------------------------
# 2. JSON valid payload creates signal
# ---------------------------------------------------------------------------

def test_json_valid_creates_signal(wh_env):
    resp = wh_env["client"].post(
        f"/webhooks/tradingview/macro?token={_TOKEN}",
        data=json.dumps(_JSON_PAYLOAD),
        content_type="application/json",
    )
    assert resp.status_code == 201
    body = resp.get_json()
    assert body["ok"] is True
    assert _signal_count(wh_env["db_path"]) == 1


# ---------------------------------------------------------------------------
# 3. Missing event_type returns 400, no write
# ---------------------------------------------------------------------------

def test_missing_event_type_returns_400_no_write(wh_env):
    resp = wh_env["client"].post(
        f"/webhooks/tradingview/macro?token={_TOKEN}",
        data="regime=RISK-OFF\nscore=-1.0",
        content_type="text/plain",
    )
    assert resp.status_code == 400
    body = resp.get_json()
    assert body["ok"] is False
    assert "event_type" in body["error"]
    assert _signal_count(wh_env["db_path"]) == 0


# ---------------------------------------------------------------------------
# 4. Invalid token returns 403, no write
# ---------------------------------------------------------------------------

def test_invalid_token_returns_403_no_write(wh_env):
    resp = wh_env["client"].post(
        "/webhooks/tradingview/macro?token=wrong-token",
        data=_TEXT_PAYLOAD,
        content_type="text/plain",
    )
    assert resp.status_code == 403
    body = resp.get_json()
    assert body["ok"] is False
    assert _signal_count(wh_env["db_path"]) == 0


# ---------------------------------------------------------------------------
# 4b. Header-based token also works
# ---------------------------------------------------------------------------

def test_header_token_accepted(wh_env):
    resp = wh_env["client"].post(
        "/webhooks/tradingview/macro",
        data=_TEXT_PAYLOAD,
        content_type="text/plain",
        headers={"X-Webhook-Token": _TOKEN},
    )
    assert resp.status_code == 201
    assert resp.get_json()["ok"] is True


# ---------------------------------------------------------------------------
# 5. Missing env token returns 503, no write
# ---------------------------------------------------------------------------

def test_missing_env_token_returns_503_no_write(wh_no_token):
    resp = wh_no_token["client"].post(
        "/webhooks/tradingview/macro?token=anything",
        data=_TEXT_PAYLOAD,
        content_type="text/plain",
    )
    assert resp.status_code == 503
    body = resp.get_json()
    assert body["ok"] is False
    assert _signal_count(wh_no_token["db_path"]) == 0


# ---------------------------------------------------------------------------
# 6. Severity mapping: hard_risk_off_activated -> HIGH
# ---------------------------------------------------------------------------

def test_severity_map_hard_risk_off_high(wh_env):
    wh_env["client"].post(
        f"/webhooks/tradingview/macro?token={_TOKEN}",
        data="event_type=hard_risk_off_activated",
        content_type="text/plain",
    )
    sigs = _get_signals(wh_env["db_path"])
    assert len(sigs) == 1
    assert sigs[0]["severity"] == "HIGH"


# ---------------------------------------------------------------------------
# 7. Severity mapping: stress -> CRITICAL
# ---------------------------------------------------------------------------

def test_severity_map_stress_critical(wh_env):
    wh_env["client"].post(
        f"/webhooks/tradingview/macro?token={_TOKEN}",
        data="event_type=stress",
        content_type="text/plain",
    )
    sigs = _get_signals(wh_env["db_path"])
    assert sigs[0]["severity"] == "CRITICAL"


# ---------------------------------------------------------------------------
# 8. Full payload_json stored verbatim
# ---------------------------------------------------------------------------

def test_payload_json_stored_verbatim(wh_env):
    import json as _json
    wh_env["client"].post(
        f"/webhooks/tradingview/macro?token={_TOKEN}",
        data=_json.dumps(_JSON_PAYLOAD),
        content_type="application/json",
    )
    sigs = _get_signals(wh_env["db_path"])
    assert len(sigs) == 1
    stored = _json.loads(sigs[0]["payload_json"])
    assert stored["event_type"] == _JSON_PAYLOAD["event_type"]
    assert stored["score"] == _JSON_PAYLOAD["score"]
    assert stored["regime"] == _JSON_PAYLOAD["regime"]


# ---------------------------------------------------------------------------
# 9. Default source = tradingview_macro (text/plain without source field)
# ---------------------------------------------------------------------------

def test_default_source_is_tradingview_macro(wh_env):
    wh_env["client"].post(
        f"/webhooks/tradingview/macro?token={_TOKEN}",
        data="event_type=caution",
        content_type="text/plain",
    )
    sigs = _get_signals(wh_env["db_path"])
    assert sigs[0]["source"] == "tradingview_macro"


# ---------------------------------------------------------------------------
# 10. Default status = OPEN
# ---------------------------------------------------------------------------

def test_default_status_is_open(wh_env):
    wh_env["client"].post(
        f"/webhooks/tradingview/macro?token={_TOKEN}",
        data=_TEXT_PAYLOAD,
        content_type="text/plain",
    )
    sigs = _get_signals(wh_env["db_path"])
    assert sigs[0]["status"] == "OPEN"


# ---------------------------------------------------------------------------
# 11. Dashboard shows webhook signal in recent_signals
# ---------------------------------------------------------------------------

def test_dashboard_shows_webhook_signal(wh_env):
    # First seed a position and price so dashboard loads fully
    db = Database(wh_env["db_path"])
    db.connect()
    tx_svc = TransactionService(db, AssetResolver(db))
    tx_svc.record_buy("BTC", "TestWallet", qty=1, unit_price=50000, fee_usd=0, tx_date="2026-01-01")
    db.close()

    # Send webhook
    wh_env["client"].post(
        f"/webhooks/tradingview/macro?token={_TOKEN}",
        data="event_type=hard_risk_off_activated\nmessage=Dashboard test",
        content_type="text/plain",
    )

    resp = wh_env["client"].get("/")
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="ignore")
    assert "hard_risk_off_activated" in body


# ---------------------------------------------------------------------------
# 12. Signals page lists webhook signal
# ---------------------------------------------------------------------------

def test_signals_page_lists_webhook_signal(wh_env):
    wh_env["client"].post(
        f"/webhooks/tradingview/macro?token={_TOKEN}",
        data="event_type=confirmed_downgrade\nmessage=Webhook signal test",
        content_type="text/plain",
    )
    resp = wh_env["client"].get("/signals")
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="ignore")
    assert "confirmed_downgrade" in body


# ---------------------------------------------------------------------------
# 13. Webhook does not modify cash, equity, positions
# ---------------------------------------------------------------------------

def test_webhook_does_not_modify_financial_state(wh_env):
    db = Database(wh_env["db_path"])
    db.connect()
    resolver = AssetResolver(db)
    tx_svc = TransactionService(db, resolver)
    pnl_svc = PnLService(db, resolver)

    tx_svc.record_buy("BTC", "TestWallet", qty=1, unit_price=50000, fee_usd=0, tx_date="2026-01-01")
    tx_svc.record_cash_movement("TestWallet", "2026-01-01", amount=5000, movement_type="DEPOSIT")

    summary_before = pnl_svc.summary()
    pos_before = {(p["symbol"], p["account"]): p["qty_open"] for p in pnl_svc.positions()}
    db.close()

    # Fire 3 webhooks
    for et in ["hard_risk_off_activated", "stress", "caution"]:
        wh_env["client"].post(
            f"/webhooks/tradingview/macro?token={_TOKEN}",
            data=f"event_type={et}",
            content_type="text/plain",
        )

    db2 = Database(wh_env["db_path"])
    db2.connect()
    resolver2 = AssetResolver(db2)
    pnl_svc2 = PnLService(db2, resolver2)

    summary_after = pnl_svc2.summary()
    pos_after = {(p["symbol"], p["account"]): p["qty_open"] for p in pnl_svc2.positions()}
    db2.close()

    assert summary_before["total_equity"] == summary_after["total_equity"]
    assert summary_before["total_cost_basis"] == summary_after["total_cost_basis"]
    assert summary_before["total_realized_pnl"] == summary_after["total_realized_pnl"]
    assert summary_before["cash_balance"] == summary_after["cash_balance"]
    assert pos_before == pos_after
    assert _signal_count(wh_env["db_path"]) == 3


# ---------------------------------------------------------------------------
# 14. No backup file created by webhook
# ---------------------------------------------------------------------------

def test_no_backup_created_by_webhook(wh_env):
    from pathlib import Path
    auto_backup_dir = Path(wh_env["instance_path"]) / "backups"

    wh_env["client"].post(
        f"/webhooks/tradingview/macro?token={_TOKEN}",
        data=_TEXT_PAYLOAD,
        content_type="text/plain",
    )

    backup_files = list(auto_backup_dir.glob("*")) if auto_backup_dir.exists() else []
    assert len(backup_files) == 0, (
        f"Expected no backup files, found: {[f.name for f in backup_files]}"
    )


# ---------------------------------------------------------------------------
# Extra: comma-separated text/plain
# ---------------------------------------------------------------------------

def test_comma_separated_text_plain_parsed(wh_env):
    resp = wh_env["client"].post(
        f"/webhooks/tradingview/macro?token={_TOKEN}",
        data="event_type=risk_off, regime=RISK-OFF, score=-2.0",
        content_type="text/plain",
    )
    assert resp.status_code == 201
    sigs = _get_signals(wh_env["db_path"])
    assert sigs[0]["event_type"] == "risk_off"


# ---------------------------------------------------------------------------
# Extra: unknown event_type maps to INFO severity
# ---------------------------------------------------------------------------

def test_unknown_event_type_maps_to_info_severity(wh_env):
    wh_env["client"].post(
        f"/webhooks/tradingview/macro?token={_TOKEN}",
        data="event_type=some_new_signal",
        content_type="text/plain",
    )
    sigs = _get_signals(wh_env["db_path"])
    assert sigs[0]["severity"] == "INFO"
