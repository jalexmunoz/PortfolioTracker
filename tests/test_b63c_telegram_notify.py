"""
B63C — Tests for Telegram outbound notifications.

Tests:
  1.  telegram_disabled_no_send            — PORTFOLIO_TELEGRAM_ENABLED != 1 → no send
  2.  missing_token_webhook_still_ok       — missing token/chat_id → webhook ok=True
  3.  hard_risk_off_sends_telegram         — hard_risk_off + pa_created > 0 → sends
  4.  webhook_test_no_telegram             — webhook_test event → no Telegram
  5.  unknown_event_no_telegram            — unknown event_type → no Telegram
  6.  duplicate_webhook_no_duplicate_tg    — second webhook pa_created=0 → no second send
  7.  telegram_failure_webhook_still_ok    — send_message fails → webhook ok=True
  8.  message_contains_event_type         — sent text contains event_type
  9.  message_contains_alert_messages     — sent text contains portfolio alert messages
  10. message_no_token                    — sent text does not contain TELEGRAM_BOT_TOKEN
  11. test_telegram_route                 — POST /signals/test-telegram works with mock
  12. no_cash_change                      — Telegram send does not modify cash balance
  13. no_positions_change                 — Telegram send does not modify positions
  14. no_pnl_change                       — Telegram send does not modify PnL
  15. full_suite_passes                   — sanity: basic service unit tests pass
"""
import json
from datetime import datetime
from decimal import Decimal

import pytest

from portfolio_tracker_v2.core.database import Database
from portfolio_tracker_v2.core.asset_resolver import AssetResolver
from portfolio_tracker_v2.services.transaction_svc import TransactionService
from portfolio_tracker_v2.services.pnl_svc import PnLService
from portfolio_tracker_v2.services.signal_svc import SignalService
from portfolio_tracker_v2.services.telegram_notify_svc import (
    TelegramNotifyService,
    format_summary,
    is_enabled,
)
from portfolio_tracker_v2.gui.app import create_app


_TOKEN = "b63c-test-secret"
_TG_TOKEN = "FAKE_TG_BOT_TOKEN_DONOTLEAK"
_TG_CHAT = "999888777"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _set_live_price(db, symbol, price):
    now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    db.connect().execute(
        """UPDATE assets
              SET current_price = ?,
                  price_source = 'api',
                  price_updated_at = ?,
                  valuation_method = 'market_live'
            WHERE symbol = ?""",
        (float(price), now, symbol),
    )
    db.commit()


def _notification_count(db_path):
    db = Database(db_path)
    db.connect()
    db._ensure_signal_notifications_schema()
    count = db.connect().execute(
        "SELECT COUNT(*) FROM signal_notifications"
    ).fetchone()[0]
    db.close()
    return count


def _post_webhook(client, event_type, token=_TOKEN, source=None):
    body = f"event_type={event_type}"
    if source:
        body += f"\nsource={source}"
    return client.post(
        f"/webhooks/tradingview/macro?token={token}",
        data=body,
        content_type="text/plain",
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def wh_env(tmp_path, monkeypatch):
    """Empty portfolio, webhook token set, Telegram NOT configured by default."""
    instance_path = tmp_path / "instance"
    instance_path.mkdir()

    db_path = tmp_path / "wh_b63c.db"
    db = Database(str(db_path))
    db.connect()
    db.init_schema()
    AssetResolver(db).get_or_create_usd_cash()
    db.close()

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
def wh_funded_env(tmp_path, monkeypatch):
    """
    BTC $50K + $1K cash → crypto ~98% equity.
    Rule 5 (>35%) and Rule 1 (>25% under risk-off) both fire on hard_risk_off_activated.
    Telegram NOT enabled by default — tests opt-in via monkeypatch.setenv.
    """
    instance_path = tmp_path / "instance"
    instance_path.mkdir()

    db_path = tmp_path / "wh_b63c_funded.db"
    db = Database(str(db_path))
    db.connect()
    db.init_schema()
    resolver = AssetResolver(db)
    resolver.get_or_create_usd_cash()

    tx_svc = TransactionService(db, resolver)
    tx_svc.record_buy(
        "BTC", "Main",
        qty=Decimal("1"),
        unit_price=Decimal("50000"),
        fee_usd=Decimal("0"),
        tx_date="2026-01-01",
    )
    tx_svc.record_cash_movement(
        "Main", "2026-01-01", amount=Decimal("1000"), movement_type="DEPOSIT",
    )
    _set_live_price(db, "BTC", 50000)
    db.close()

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


def _enable_telegram(monkeypatch):
    monkeypatch.setenv("PORTFOLIO_TELEGRAM_ENABLED", "1")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", _TG_TOKEN)
    monkeypatch.setenv("TELEGRAM_CHAT_ID", _TG_CHAT)


def _mock_send(monkeypatch):
    """Patch send_message to capture calls without hitting Telegram API."""
    sent = []
    monkeypatch.setattr(
        "portfolio_tracker_v2.services.telegram_notify_svc.send_message",
        lambda text: sent.append(text) or {"ok": True, "error": None},
    )
    return sent


# ---------------------------------------------------------------------------
# 1. Telegram disabled → no send
# ---------------------------------------------------------------------------

def test_telegram_disabled_no_send(wh_funded_env, monkeypatch):
    # Do NOT set PORTFOLIO_TELEGRAM_ENABLED → is_enabled() returns False
    sent = _mock_send(monkeypatch)

    resp = _post_webhook(wh_funded_env["client"], "hard_risk_off_activated")
    assert resp.status_code == 201
    assert resp.get_json()["ok"] is True
    assert sent == [], "send_message must not be called when Telegram is disabled"


# ---------------------------------------------------------------------------
# 2. Missing token/chat_id → webhook still ok=True (no exception)
# ---------------------------------------------------------------------------

def test_missing_token_webhook_still_ok(wh_funded_env, monkeypatch):
    # PORTFOLIO_TELEGRAM_ENABLED=1 but no BOT_TOKEN or CHAT_ID
    monkeypatch.setenv("PORTFOLIO_TELEGRAM_ENABLED", "1")
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)

    resp = _post_webhook(wh_funded_env["client"], "hard_risk_off_activated")
    assert resp.status_code == 201
    body = resp.get_json()
    assert body["ok"] is True
    assert body["portfolio_alerts_created"] >= 1


# ---------------------------------------------------------------------------
# 3. hard_risk_off_activated + portfolio alerts created → sends Telegram
# ---------------------------------------------------------------------------

def test_hard_risk_off_sends_telegram(wh_funded_env, monkeypatch):
    _enable_telegram(monkeypatch)
    sent = _mock_send(monkeypatch)

    resp = _post_webhook(wh_funded_env["client"], "hard_risk_off_activated")
    assert resp.status_code == 201
    body = resp.get_json()
    assert body["ok"] is True
    assert body["portfolio_alerts_created"] >= 1
    assert len(sent) == 1, "Telegram must be called exactly once when alerts are created"


# ---------------------------------------------------------------------------
# 4. webhook_test event_type → no Telegram
# ---------------------------------------------------------------------------

def test_webhook_test_no_telegram(wh_funded_env, monkeypatch):
    _enable_telegram(monkeypatch)
    sent = _mock_send(monkeypatch)

    resp = _post_webhook(wh_funded_env["client"], "webhook_test")
    assert resp.status_code == 201
    assert resp.get_json()["ok"] is True
    assert sent == [], "webhook_test must not trigger Telegram"


# ---------------------------------------------------------------------------
# 5. Unknown event_type → no Telegram
# ---------------------------------------------------------------------------

def test_unknown_event_no_telegram(wh_funded_env, monkeypatch):
    _enable_telegram(monkeypatch)
    sent = _mock_send(monkeypatch)

    resp = _post_webhook(wh_funded_env["client"], "totally_unknown_event_xyz")
    assert resp.status_code == 201
    assert resp.get_json()["ok"] is True
    assert sent == [], "Unknown event_type must not trigger Telegram"


# ---------------------------------------------------------------------------
# 6. Duplicate webhook → no duplicate Telegram (pa_created=0 on second call)
# ---------------------------------------------------------------------------

def test_duplicate_webhook_no_duplicate_tg(wh_funded_env, monkeypatch):
    _enable_telegram(monkeypatch)
    sent = _mock_send(monkeypatch)

    # First webhook: creates portfolio_rule alerts → Telegram sends
    resp1 = _post_webhook(wh_funded_env["client"], "hard_risk_off_activated")
    body1 = resp1.get_json()
    assert body1["portfolio_alerts_created"] >= 1
    assert len(sent) == 1

    # Second identical webhook: alerts already OPEN (skipped) → pa_created=0 → no Telegram
    resp2 = _post_webhook(wh_funded_env["client"], "hard_risk_off_activated")
    body2 = resp2.get_json()
    assert body2["portfolio_alerts_created"] == 0
    assert body2["portfolio_alerts_skipped"] >= 1
    assert len(sent) == 1, "Telegram must NOT be called a second time when pa_created=0"


# ---------------------------------------------------------------------------
# 7. Telegram API failure → webhook response still ok=True
# ---------------------------------------------------------------------------

def test_telegram_failure_webhook_still_ok(wh_funded_env, monkeypatch):
    _enable_telegram(monkeypatch)
    monkeypatch.setattr(
        "portfolio_tracker_v2.services.telegram_notify_svc.send_message",
        lambda text: {"ok": False, "error": "Simulated Telegram API failure"},
    )

    resp = _post_webhook(wh_funded_env["client"], "hard_risk_off_activated")
    assert resp.status_code == 201
    body = resp.get_json()
    assert body["ok"] is True
    assert body["portfolio_alerts_created"] >= 1


# ---------------------------------------------------------------------------
# 8. Sent message contains macro event_type
# ---------------------------------------------------------------------------

def test_message_contains_event_type(wh_funded_env, monkeypatch):
    _enable_telegram(monkeypatch)
    sent = _mock_send(monkeypatch)

    _post_webhook(wh_funded_env["client"], "hard_risk_off_activated")

    assert len(sent) == 1
    assert "hard_risk_off_activated" in sent[0]


# ---------------------------------------------------------------------------
# 9. Sent message contains portfolio_rule alert messages
# ---------------------------------------------------------------------------

def test_message_contains_alert_messages(wh_funded_env, monkeypatch):
    _enable_telegram(monkeypatch)
    sent = _mock_send(monkeypatch)

    _post_webhook(wh_funded_env["client"], "hard_risk_off_activated")

    assert len(sent) == 1
    # The message must list at least one alert detail (not just the count)
    assert "- " in sent[0], "Message must list individual alert messages"


# ---------------------------------------------------------------------------
# 10. Sent message does NOT contain TELEGRAM_BOT_TOKEN
# ---------------------------------------------------------------------------

def test_message_no_token(wh_funded_env, monkeypatch):
    _enable_telegram(monkeypatch)
    sent = _mock_send(monkeypatch)

    _post_webhook(wh_funded_env["client"], "hard_risk_off_activated")

    assert len(sent) == 1
    assert _TG_TOKEN not in sent[0], "Bot token must never appear in message text"


# ---------------------------------------------------------------------------
# 11. POST /signals/test-telegram works with mocked send_message
# ---------------------------------------------------------------------------

def test_test_telegram_route(wh_env, monkeypatch):
    _enable_telegram(monkeypatch)
    sent = _mock_send(monkeypatch)

    resp = wh_env["client"].post("/signals/test-telegram")
    # Should redirect after sending
    assert resp.status_code in (200, 302)
    assert len(sent) == 1, "Test route must call send_message exactly once"
    assert "Test" in sent[0] or "test" in sent[0].lower()


# ---------------------------------------------------------------------------
# 12. Telegram notification does NOT change cash balance
# ---------------------------------------------------------------------------

def test_no_cash_change(wh_funded_env, monkeypatch):
    _enable_telegram(monkeypatch)
    _mock_send(monkeypatch)

    db = Database(wh_funded_env["db_path"])
    db.connect()
    cash_before = PnLService(db, AssetResolver(db)).summary()["cash_balance"]
    db.close()

    _post_webhook(wh_funded_env["client"], "hard_risk_off_activated")

    db2 = Database(wh_funded_env["db_path"])
    db2.connect()
    cash_after = PnLService(db2, AssetResolver(db2)).summary()["cash_balance"]
    db2.close()

    assert cash_before == cash_after


# ---------------------------------------------------------------------------
# 13. Telegram notification does NOT change positions
# ---------------------------------------------------------------------------

def test_no_positions_change(wh_funded_env, monkeypatch):
    _enable_telegram(monkeypatch)
    _mock_send(monkeypatch)

    db = Database(wh_funded_env["db_path"])
    db.connect()
    pos_before = {
        (p["symbol"], p["account"]): p["qty_open"]
        for p in PnLService(db, AssetResolver(db)).positions()
    }
    db.close()

    _post_webhook(wh_funded_env["client"], "hard_risk_off_activated")

    db2 = Database(wh_funded_env["db_path"])
    db2.connect()
    pos_after = {
        (p["symbol"], p["account"]): p["qty_open"]
        for p in PnLService(db2, AssetResolver(db2)).positions()
    }
    db2.close()

    assert pos_before == pos_after


# ---------------------------------------------------------------------------
# 14. Telegram notification does NOT change PnL
# ---------------------------------------------------------------------------

def test_no_pnl_change(wh_funded_env, monkeypatch):
    _enable_telegram(monkeypatch)
    _mock_send(monkeypatch)

    db = Database(wh_funded_env["db_path"])
    db.connect()
    summ_before = PnLService(db, AssetResolver(db)).summary()
    db.close()

    _post_webhook(wh_funded_env["client"], "hard_risk_off_activated")

    db2 = Database(wh_funded_env["db_path"])
    db2.connect()
    summ_after = PnLService(db2, AssetResolver(db2)).summary()
    db2.close()

    assert summ_before["total_realized_pnl"] == summ_after["total_realized_pnl"]
    assert summ_before["total_equity"] == summ_after["total_equity"]
    assert summ_before["total_cost_basis"] == summ_after["total_cost_basis"]


# ---------------------------------------------------------------------------
# 15. Service unit tests: format_summary, is_enabled, TelegramNotifyService.already_sent
# ---------------------------------------------------------------------------

def test_full_suite_passes(tmp_path, monkeypatch):
    # is_enabled() checks
    monkeypatch.delenv("PORTFOLIO_TELEGRAM_ENABLED", raising=False)
    assert is_enabled() is False

    monkeypatch.setenv("PORTFOLIO_TELEGRAM_ENABLED", "1")
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    assert is_enabled() is False

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "tok123")
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
    assert is_enabled() is False

    monkeypatch.setenv("TELEGRAM_CHAT_ID", "chat456")
    assert is_enabled() is True

    # format_summary does not include token
    text = format_summary("hard_risk_off_activated", "HIGH", [{"message": "Crypto is 95%"}])
    assert "hard_risk_off_activated" in text
    assert "HIGH" in text
    assert "Crypto is 95%" in text
    assert "tok123" not in text
    assert "No automatic trade" in text

    # TelegramNotifyService.already_sent with empty DB
    db_path = tmp_path / "tg_unit.db"
    db = Database(str(db_path))
    db.connect()
    db.init_schema()
    svc = TelegramNotifyService(db)
    assert svc.already_sent(999) is False

    # Record a sent notification and verify already_sent
    svc._record(999, "sent")
    assert svc.already_sent(999) is True

    # Second _record for same (signal_id, channel) is silently ignored (INSERT OR IGNORE)
    svc._record(999, "failed", "some error")
    assert svc.already_sent(999) is True

    db.close()
