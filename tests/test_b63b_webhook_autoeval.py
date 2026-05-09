"""
B63B -- Tests for auto-evaluation of portfolio alerts after TradingView webhook.

Tests:
  1.  hard_risk_off_creates_portfolio_alerts       — macro signal + crypto-heavy portfolio
  2.  response_includes_evaluated_true             — portfolio_alerts_evaluated=True in JSON
  3.  response_includes_created_skipped_counts     — created/skipped counts in JSON
  4.  webhook_test_not_evaluated                   — event_type not in allowlist
  5.  unknown_event_type_not_evaluated             — unknown type saves signal, no eval
  6.  dedupe_second_webhook_skips_duplicates       — second call skips existing OPEN alerts
  7.  source_portfolio_rule_not_evaluated          — source=portfolio_rule → no auto-eval
  8.  auto_eval_does_not_modify_cash               — cash_balance unchanged after eval
  9.  auto_eval_does_not_modify_positions          — qty_open unchanged after eval
  10. auto_eval_does_not_modify_pnl               — realized/unrealized PnL unchanged
  11. rule_svc_failure_signal_saved_no_500         — if eval fails, signal saved, ok=true
  12. no_backup_created_by_auto_eval              — no backup file created
  13. manual_evaluate_still_works                 — POST /signals/evaluate-portfolio still works
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
from portfolio_tracker_v2.services.portfolio_alert_rule_svc import PortfolioAlertRuleService
from portfolio_tracker_v2.gui.app import create_app


_TOKEN = "b63b-test-secret"


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


def _portfolio_alerts(db_path):
    """Return all OPEN portfolio_rule signals."""
    db = Database(db_path)
    db.connect()
    svc = SignalService(db)
    alerts = svc.list_signals(status="OPEN", source="portfolio_rule")
    db.close()
    return alerts


def _signal_count(db_path):
    db = Database(db_path)
    count = db.connect().execute("SELECT COUNT(*) FROM signal_alerts").fetchone()[0]
    db.close()
    return count


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def wh_env(tmp_path, monkeypatch):
    """Basic env: DB with USD cash, no crypto position, webhook token set."""
    instance_path = tmp_path / "instance"
    instance_path.mkdir()

    db_path = tmp_path / "wh_b63b.db"
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
    Env with a crypto-heavy portfolio so portfolio alert rules fire.
    BTC $50K, cash $1K  →  Crypto ~98% of equity (Rule 5 >35%, Rule 1 >25%).
    """
    instance_path = tmp_path / "instance"
    instance_path.mkdir()

    db_path = tmp_path / "wh_b63b_funded.db"
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
# 1. hard_risk_off_activated creates portfolio_rule alerts (funded portfolio)
# ---------------------------------------------------------------------------

def test_hard_risk_off_creates_portfolio_alerts(wh_funded_env):
    resp = _post_webhook(wh_funded_env["client"], "hard_risk_off_activated")
    assert resp.status_code == 201
    body = resp.get_json()
    assert body["ok"] is True
    # Rule 5 (Crypto >35%) + Rule 1 (Crypto >25% under risk-off) both fire
    assert body["portfolio_alerts_created"] >= 1
    alerts = _portfolio_alerts(wh_funded_env["db_path"])
    assert len(alerts) >= 1
    assert all(a["source"] == "portfolio_rule" for a in alerts)
    assert all(a["status"] == "OPEN" for a in alerts)


# ---------------------------------------------------------------------------
# 2. Response includes portfolio_alerts_evaluated=True for allowlist event
# ---------------------------------------------------------------------------

def test_response_includes_evaluated_true(wh_env):
    resp = _post_webhook(wh_env["client"], "hard_risk_off_activated")
    body = resp.get_json()
    assert body["portfolio_alerts_evaluated"] is True


# ---------------------------------------------------------------------------
# 3. Response includes portfolio_alerts_created and portfolio_alerts_skipped
# ---------------------------------------------------------------------------

def test_response_includes_created_skipped_counts(wh_funded_env):
    resp = _post_webhook(wh_funded_env["client"], "hard_risk_off_activated")
    body = resp.get_json()
    assert "portfolio_alerts_created" in body
    assert "portfolio_alerts_skipped" in body
    assert isinstance(body["portfolio_alerts_created"], int)
    assert isinstance(body["portfolio_alerts_skipped"], int)
    assert body["portfolio_alerts_created"] >= 1
    assert body["portfolio_alerts_skipped"] == 0


# ---------------------------------------------------------------------------
# 4. webhook_test event_type is NOT in allowlist → no auto-eval
# ---------------------------------------------------------------------------

def test_webhook_test_not_evaluated(wh_env):
    resp = _post_webhook(wh_env["client"], "webhook_test")
    assert resp.status_code == 201
    body = resp.get_json()
    assert body["ok"] is True
    assert body["portfolio_alerts_evaluated"] is False
    assert body["portfolio_alerts_created"] == 0
    assert body["portfolio_alerts_skipped"] == 0
    # Signal was still saved
    assert _signal_count(wh_env["db_path"]) == 1


# ---------------------------------------------------------------------------
# 5. Unknown event_type saves signal (INFO severity) but does NOT auto-eval
# ---------------------------------------------------------------------------

def test_unknown_event_type_not_evaluated(wh_env):
    resp = _post_webhook(wh_env["client"], "some_brand_new_signal")
    assert resp.status_code == 201
    body = resp.get_json()
    assert body["ok"] is True
    assert body["portfolio_alerts_evaluated"] is False
    assert body["portfolio_alerts_created"] == 0
    # Signal was saved with INFO severity
    db = Database(wh_env["db_path"])
    rows = db.connect().execute("SELECT severity FROM signal_alerts").fetchall()
    db.close()
    assert rows[0][0] == "INFO"


# ---------------------------------------------------------------------------
# 6. Dedupe: sending same webhook twice does not duplicate OPEN portfolio_rule alerts
# ---------------------------------------------------------------------------

def test_dedupe_second_webhook_skips_duplicates(wh_funded_env):
    # First call: creates alerts
    resp1 = _post_webhook(wh_funded_env["client"], "hard_risk_off_activated")
    body1 = resp1.get_json()
    assert body1["portfolio_alerts_created"] >= 1

    created_first = body1["portfolio_alerts_created"]

    # Second call: same event_type, same portfolio → alerts already OPEN → skipped
    resp2 = _post_webhook(wh_funded_env["client"], "hard_risk_off_activated")
    body2 = resp2.get_json()
    assert body2["portfolio_alerts_created"] == 0
    assert body2["portfolio_alerts_skipped"] == created_first

    # DB should not have duplicate OPEN portfolio_rule alerts
    alerts = _portfolio_alerts(wh_funded_env["db_path"])
    event_types = [a["event_type"] for a in alerts]
    # All event_types are unique among OPEN portfolio_rule alerts
    assert len(event_types) == len(set(event_types))


# ---------------------------------------------------------------------------
# 7. source=portfolio_rule does NOT trigger auto-evaluation (anti-recursion)
# ---------------------------------------------------------------------------

def test_source_portfolio_rule_not_evaluated(wh_funded_env):
    resp = _post_webhook(
        wh_funded_env["client"],
        "hard_risk_off_activated",
        source="portfolio_rule",
    )
    assert resp.status_code == 201
    body = resp.get_json()
    assert body["ok"] is True
    assert body["portfolio_alerts_evaluated"] is False
    assert body["portfolio_alerts_created"] == 0


# ---------------------------------------------------------------------------
# 8. Auto-evaluation does not modify cash balance
# ---------------------------------------------------------------------------

def test_auto_eval_does_not_modify_cash(wh_funded_env):
    db = Database(wh_funded_env["db_path"])
    db.connect()
    pnl_before = PnLService(db, AssetResolver(db)).summary()
    cash_before = pnl_before["cash_balance"]
    db.close()

    _post_webhook(wh_funded_env["client"], "hard_risk_off_activated")

    db2 = Database(wh_funded_env["db_path"])
    db2.connect()
    pnl_after = PnLService(db2, AssetResolver(db2)).summary()
    cash_after = pnl_after["cash_balance"]
    db2.close()

    assert cash_before == cash_after


# ---------------------------------------------------------------------------
# 9. Auto-evaluation does not modify positions (qty_open)
# ---------------------------------------------------------------------------

def test_auto_eval_does_not_modify_positions(wh_funded_env):
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
# 10. Auto-evaluation does not modify realized/unrealized PnL
# ---------------------------------------------------------------------------

def test_auto_eval_does_not_modify_pnl(wh_funded_env):
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
# 11. If PortfolioAlertRuleService fails, macro signal is saved and response is not 500
# ---------------------------------------------------------------------------

def test_rule_svc_failure_signal_saved_no_500(wh_env, monkeypatch):
    def _fail(self):
        raise RuntimeError("simulated portfolio rule evaluation failure")

    monkeypatch.setattr(PortfolioAlertRuleService, "evaluate_portfolio_alerts", _fail)

    resp = _post_webhook(wh_env["client"], "hard_risk_off_activated")
    assert resp.status_code == 201
    body = resp.get_json()
    assert body["ok"] is True
    assert body["portfolio_alerts_evaluated"] is True
    assert body["portfolio_alerts_created"] == 0
    assert body["portfolio_alerts_skipped"] == 0
    assert "portfolio_alerts_error" in body
    assert "simulated" in body["portfolio_alerts_error"]
    # Original macro signal must be persisted
    assert _signal_count(wh_env["db_path"]) == 1


# ---------------------------------------------------------------------------
# 12. No backup file created by auto-evaluation
# ---------------------------------------------------------------------------

def test_no_backup_created_by_auto_eval(wh_funded_env):
    from pathlib import Path
    backup_dir = Path(wh_funded_env["instance_path"]) / "backups"

    _post_webhook(wh_funded_env["client"], "hard_risk_off_activated")

    files = list(backup_dir.glob("*")) if backup_dir.exists() else []
    assert files == [], f"Unexpected backup files: {[f.name for f in files]}"


# ---------------------------------------------------------------------------
# 13. Manual evaluate-portfolio route still works (B63A regression)
# ---------------------------------------------------------------------------

def test_manual_evaluate_still_works(wh_funded_env):
    # Inject a macro signal directly so the manual evaluate has something to act on
    db = Database(wh_funded_env["db_path"])
    db.connect()
    SignalService(db).add_signal(
        event_time="2026-05-01T10:00:00",
        source="tradingview_macro",
        event_type="hard_risk_off_activated",
        severity="HIGH",
    )
    db.close()

    resp = wh_funded_env["client"].post("/signals/evaluate-portfolio")
    # Should redirect (302) or 200, not 500
    assert resp.status_code in (200, 302)

    # Portfolio_rule alerts were created
    alerts = _portfolio_alerts(wh_funded_env["db_path"])
    assert len(alerts) >= 1
