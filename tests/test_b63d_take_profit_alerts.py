"""
B63D — Tests for take-profit review alerts.

Tests:
  1.  position_30pct_creates_alert              — +30% ≥ $100 gain → take_profit_review_gain_threshold
  2.  position_below_threshold_no_alert         — +29.9% → no alert
  3.  position_50pct_high_severity              — +50% → HIGH severity
  4.  small_absolute_gain_no_alert              — +30% but gain_usd < $100 → no alert
  5.  non_market_position_no_alert              — snapshot_imported asset → no alert
  6.  cash_only_no_alert                        — only cash, no positions → no alert
  7.  dedupe_second_run_no_duplicate            — two evaluate() calls → no duplicate OPEN alert
  8.  message_no_sell_now                       — message contains Review, NOT "SELL" or "NOW"
  9.  does_not_modify_cash                      — cash_balance unchanged after evaluate
  10. does_not_modify_positions                 — qty_open/cost_basis unchanged after evaluate
  11. does_not_modify_pnl                       — realized_pnl unchanged after evaluate
  12. gui_manual_evaluate_works                 — POST /signals/evaluate-portfolio creates alert
  13. webhook_auto_eval_creates_take_profit     — webhook triggers evaluate → take_profit created
"""
import json
from datetime import datetime
from decimal import Decimal

import pytest

from portfolio_tracker_v2.core.database import Database
from portfolio_tracker_v2.core.asset_resolver import AssetResolver
from portfolio_tracker_v2.services.signal_svc import SignalService
from portfolio_tracker_v2.services.transaction_svc import TransactionService
from portfolio_tracker_v2.services.pnl_svc import PnLService
from portfolio_tracker_v2.services.portfolio_alert_rule_svc import PortfolioAlertRuleService


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fresh_db(tmp_path, name="b63d.db"):
    db = Database(str(tmp_path / name))
    db.connect()
    db.init_schema()
    AssetResolver(db).get_or_create_usd_cash()
    return db


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


def _set_snapshot_price(db, symbol, price):
    """Set a snapshot_imported (non-market) valuation — simulates CDT/fund-like asset."""
    db.connect().execute(
        """UPDATE assets
              SET current_price = ?,
                  price_source = 'csv_bootstrap',
                  price_updated_at = '2020-01-01T00:00:00',
                  valuation_method = 'snapshot_imported'
            WHERE symbol = ?""",
        (float(price), symbol),
    )
    db.commit()


def _buy(db, symbol, qty, unit_price, account="Main"):
    resolver = AssetResolver(db)
    TransactionService(db, resolver).record_buy(
        symbol=symbol,
        account=account,
        qty=Decimal(str(qty)),
        unit_price=Decimal(str(unit_price)),
        fee_usd=Decimal("0"),
        tx_date="2026-01-01",
    )


def _cash_in(db, amount, account="Main"):
    resolver = AssetResolver(db)
    TransactionService(db, resolver).record_cash_movement(
        account=account,
        movement_date="2026-01-01",
        amount=Decimal(str(amount)),
        movement_type="DEPOSIT",
    )


def _rule_svc(db):
    return PortfolioAlertRuleService(db, AssetResolver(db))


def _tp_alerts(db):
    """Return all OPEN take_profit_review_gain_threshold alerts."""
    return [
        a for a in SignalService(db).list_signals(status="OPEN", source="portfolio_rule")
        if a["event_type"] == "take_profit_review_gain_threshold"
    ]


# ---------------------------------------------------------------------------
# 1. Position +30% with ≥$100 absolute gain creates take_profit alert
# ---------------------------------------------------------------------------

def test_position_30pct_creates_alert(tmp_path):
    db = _fresh_db(tmp_path)
    # cost=1000, price=1300 → unrealized_pct=30%, gain_usd=300 ≥ 100
    _buy(db, "BTC", 1, 1000)
    _set_live_price(db, "BTC", 1300)

    result = _rule_svc(db).evaluate_portfolio_alerts()
    alerts = _tp_alerts(db)

    assert len(alerts) == 1
    a = alerts[0]
    assert a["event_type"] == "take_profit_review_gain_threshold"
    assert a["source"] == "portfolio_rule"
    assert a["status"] == "OPEN"
    assert a["symbol"] == "BTC"
    assert a["severity"] == "MEDIUM"
    assert result["created"] >= 1


# ---------------------------------------------------------------------------
# 2. Position +29.9% does NOT create an alert
# ---------------------------------------------------------------------------

def test_position_below_threshold_no_alert(tmp_path):
    db = _fresh_db(tmp_path)
    # cost=1000, price=1299 → unrealized_pct=29.9% < 30%
    _buy(db, "BTC", 1, 1000)
    _set_live_price(db, "BTC", 1299)

    _rule_svc(db).evaluate_portfolio_alerts()
    assert _tp_alerts(db) == []


# ---------------------------------------------------------------------------
# 3. Position +50% creates severity HIGH
# ---------------------------------------------------------------------------

def test_position_50pct_high_severity(tmp_path):
    db = _fresh_db(tmp_path)
    # cost=1000, price=1500 → unrealized_pct=50%, gain_usd=500 ≥ 100
    _buy(db, "BTC", 1, 1000)
    _set_live_price(db, "BTC", 1500)

    _rule_svc(db).evaluate_portfolio_alerts()
    alerts = _tp_alerts(db)

    assert len(alerts) == 1
    assert alerts[0]["severity"] == "HIGH"


# ---------------------------------------------------------------------------
# 4. +30% gain but absolute gain_usd < $100 → no alert (noise suppression)
# ---------------------------------------------------------------------------

def test_small_absolute_gain_no_alert(tmp_path):
    db = _fresh_db(tmp_path)
    # cost=200, price=260 → unrealized_pct=30%, gain_usd=60 < 100
    _buy(db, "BTC", 1, 200)
    _set_live_price(db, "BTC", 260)

    _rule_svc(db).evaluate_portfolio_alerts()
    assert _tp_alerts(db) == []


# ---------------------------------------------------------------------------
# 5. Non-market position (snapshot_imported) does NOT generate alert
# ---------------------------------------------------------------------------

def test_non_market_position_no_alert(tmp_path):
    db = _fresh_db(tmp_path)
    # Buy at $1000, then set snapshot_imported at $2000 (hypothetical +100%)
    _buy(db, "BTC", 1, 1000)
    _set_snapshot_price(db, "BTC", 2000)

    _rule_svc(db).evaluate_portfolio_alerts()
    assert _tp_alerts(db) == []


# ---------------------------------------------------------------------------
# 6. Cash-only portfolio (no positions) → no take_profit alert
# ---------------------------------------------------------------------------

def test_cash_only_no_alert(tmp_path):
    db = _fresh_db(tmp_path)
    _cash_in(db, 5000)

    _rule_svc(db).evaluate_portfolio_alerts()
    assert _tp_alerts(db) == []


# ---------------------------------------------------------------------------
# 7. Dedupe: two evaluate() calls produce exactly one OPEN alert
# ---------------------------------------------------------------------------

def test_dedupe_second_run_no_duplicate(tmp_path):
    db = _fresh_db(tmp_path)
    _buy(db, "BTC", 1, 1000)
    _set_live_price(db, "BTC", 1300)

    svc = _rule_svc(db)
    r1 = svc.evaluate_portfolio_alerts()
    r2 = svc.evaluate_portfolio_alerts()

    assert r1["created"] >= 1
    assert r2["created"] == 0
    assert r2["skipped"] >= 1
    # Take_profit alerts count must not grow after second run
    tp_after_r1 = len(_tp_alerts(db))
    assert tp_after_r1 == 1  # exactly one take_profit alert, no duplicates


# ---------------------------------------------------------------------------
# 8. Alert message does NOT contain "SELL NOW" or automatic order language
# ---------------------------------------------------------------------------

def test_message_no_sell_now(tmp_path):
    db = _fresh_db(tmp_path)
    _buy(db, "BTC", 1, 1000)
    _set_live_price(db, "BTC", 1300)

    _rule_svc(db).evaluate_portfolio_alerts()
    alerts = _tp_alerts(db)

    assert len(alerts) == 1
    msg = alerts[0]["message"]
    # Must contain review language
    assert "Review" in msg
    # Must NOT contain sell orders
    assert "SELL NOW" not in msg
    assert "sell now" not in msg.lower()
    assert "BTC is up" in msg


# ---------------------------------------------------------------------------
# 9. evaluate_portfolio_alerts does NOT modify cash balance
# ---------------------------------------------------------------------------

def test_does_not_modify_cash(tmp_path):
    db = _fresh_db(tmp_path)
    _cash_in(db, 500)
    _buy(db, "BTC", 1, 1000)
    _set_live_price(db, "BTC", 1300)

    pnl = PnLService(db, AssetResolver(db))
    cash_before = pnl.cash_balance()

    _rule_svc(db).evaluate_portfolio_alerts()

    cash_after = pnl.cash_balance()
    assert cash_before == cash_after


# ---------------------------------------------------------------------------
# 10. evaluate_portfolio_alerts does NOT modify positions
# ---------------------------------------------------------------------------

def test_does_not_modify_positions(tmp_path):
    db = _fresh_db(tmp_path)
    _buy(db, "BTC", 1, 1000)
    _set_live_price(db, "BTC", 1300)

    pnl = PnLService(db, AssetResolver(db))
    pos_before = {
        (p["symbol"], p["account"]): (p["qty_open"], p["cost_basis"])
        for p in pnl.positions()
    }

    _rule_svc(db).evaluate_portfolio_alerts()

    pos_after = {
        (p["symbol"], p["account"]): (p["qty_open"], p["cost_basis"])
        for p in pnl.positions()
    }
    assert pos_before == pos_after


# ---------------------------------------------------------------------------
# 11. evaluate_portfolio_alerts does NOT modify realized PnL
# ---------------------------------------------------------------------------

def test_does_not_modify_pnl(tmp_path):
    db = _fresh_db(tmp_path)
    _buy(db, "BTC", 1, 1000)
    _set_live_price(db, "BTC", 1300)

    pnl = PnLService(db, AssetResolver(db))
    realized_before = pnl.realized_pnl()

    _rule_svc(db).evaluate_portfolio_alerts()

    realized_after = pnl.realized_pnl()
    assert realized_before == realized_after


# ---------------------------------------------------------------------------
# GUI helpers
# ---------------------------------------------------------------------------

from portfolio_tracker_v2.gui.app import create_app  # noqa: E402


def _make_gui_env(tmp_path, name="gui_b63d.db", with_tp_position=False):
    instance_path = tmp_path / "instance"
    instance_path.mkdir()

    db_path = tmp_path / name
    db = Database(str(db_path))
    db.connect()
    db.init_schema()
    resolver = AssetResolver(db)
    resolver.get_or_create_usd_cash()

    if with_tp_position:
        # BTC: cost=1000, price=1300 → +30%, gain_usd=300 ≥ 100
        TransactionService(db, resolver).record_buy(
            symbol="BTC",
            account="Main",
            qty=Decimal("1"),
            unit_price=Decimal("1000"),
            fee_usd=Decimal("0"),
            tx_date="2026-01-01",
        )
        now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        db.connect().execute(
            """UPDATE assets
                  SET current_price = 1300,
                      price_source = 'api',
                      price_updated_at = ?,
                      valuation_method = 'market_live'
                WHERE symbol = 'BTC'""",
            (now,),
        )
        db.commit()

    db.close()

    state = {"db_path": str(db_path), "mode": "TEST"}
    (instance_path / "gui_state.json").write_text(json.dumps(state), encoding="utf-8")

    app = create_app(instance_path=str(instance_path))
    app.config.update({"TESTING": True})

    return {
        "app": app,
        "db_path": str(db_path),
        "instance_path": str(instance_path),
    }


# ---------------------------------------------------------------------------
# 12. GUI: POST /signals/evaluate-portfolio creates take_profit alert
# ---------------------------------------------------------------------------

def test_gui_manual_evaluate_works(tmp_path):
    env = _make_gui_env(tmp_path, name="gui_b63d_manual.db", with_tp_position=True)

    with env["app"].test_client() as client:
        resp = client.post("/signals/evaluate-portfolio", follow_redirects=False)

    assert resp.status_code == 302
    assert "/signals" in resp.headers.get("Location", "")

    db = Database(env["db_path"])
    db.connect()
    alerts = [
        a for a in SignalService(db).list_signals(source="portfolio_rule")
        if a["event_type"] == "take_profit_review_gain_threshold"
    ]
    db.close()
    assert len(alerts) >= 1


# ---------------------------------------------------------------------------
# 13. Webhook auto-evaluate can create take_profit alert (B63B integration)
# ---------------------------------------------------------------------------

_WH_TOKEN = "b63d-webhook-secret"


def _make_webhook_env(tmp_path, monkeypatch, name="wh_b63d.db"):
    instance_path = tmp_path / "instance"
    instance_path.mkdir()

    db_path = tmp_path / name
    db = Database(str(db_path))
    db.connect()
    db.init_schema()
    resolver = AssetResolver(db)
    resolver.get_or_create_usd_cash()

    # BTC: cost=1000, price=1300 → +30%, gain_usd=300 ≥ 100
    TransactionService(db, resolver).record_buy(
        symbol="BTC",
        account="Main",
        qty=Decimal("1"),
        unit_price=Decimal("1000"),
        fee_usd=Decimal("0"),
        tx_date="2026-01-01",
    )
    now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    db.connect().execute(
        """UPDATE assets
              SET current_price = 1300,
                  price_source = 'api',
                  price_updated_at = ?,
                  valuation_method = 'market_live'
            WHERE symbol = 'BTC'""",
        (now,),
    )
    db.commit()
    db.close()

    state = {"db_path": str(db_path), "mode": "TEST"}
    (instance_path / "gui_state.json").write_text(json.dumps(state), encoding="utf-8")

    monkeypatch.setenv("PORTFOLIO_WEBHOOK_TOKEN", _WH_TOKEN)
    monkeypatch.setenv("PORTFOLIO_DB_PATH", str(db_path))

    app = create_app(instance_path=str(instance_path))
    app.config.update({"TESTING": True})

    return {
        "app": app,
        "db_path": str(db_path),
    }


def test_webhook_auto_eval_creates_take_profit_alert(tmp_path, monkeypatch):
    env = _make_webhook_env(tmp_path, monkeypatch)

    with env["app"].test_client() as client:
        resp = client.post(
            f"/webhooks/tradingview/macro?token={_WH_TOKEN}",
            data="event_type=hard_risk_off_activated",
            content_type="text/plain",
        )

    assert resp.status_code == 201
    body = resp.get_json()
    assert body["ok"] is True
    assert body["portfolio_alerts_evaluated"] is True

    db = Database(env["db_path"])
    db.connect()
    tp = [
        a for a in SignalService(db).list_signals(status="OPEN", source="portfolio_rule")
        if a["event_type"] == "take_profit_review_gain_threshold"
    ]
    db.close()
    assert len(tp) >= 1
    assert tp[0]["symbol"] == "BTC"
