"""
B63A — Tests for portfolio-aware alert rules.

Tests:
  1.  no_macro_no_concentration_no_alerts        — empty portfolio, 0 created
  2.  concentration_high_no_macro_required       — Crypto >35%, asset_class_concentration_high
  3.  hard_risk_off_crypto_high_severity         — hard_risk_off + crypto >25% → HIGH
  4.  confirmed_downgrade_crypto_medium_severity — confirmed_downgrade only → MEDIUM
  5.  stress_low_cash_high_severity              — stress + cash <5% → HIGH
  6.  risk_off_low_cash_medium_severity          — risk_off only + cash <5% → MEDIUM
  7.  large_loss_high_severity                   — unrealized_pct ≈ -35% → HIGH
  8.  large_loss_medium_severity                 — unrealized_pct ≈ -25% → MEDIUM
  9.  top_3_losers_cap                           — 4 losers → only 3 alerts
  10. oversold_cash_available_creates_review     — oversold + cash ≥5% + no stress → MEDIUM
  11. oversold_with_stress_no_buy_review         — oversold + stress active → no buy alert
  12. oversold_low_cash_no_buy_review            — oversold + cash <5% → no buy alert
  13. dedupe_second_run_skips_duplicates         — two runs, second creates 0
  14. portfolio_rule_source_not_used_as_input    — anti-recursion
  15. generated_alerts_source_and_status         — source=portfolio_rule, status=OPEN
  16. evaluate_does_not_modify_cash              — cash_balance unchanged
  17. evaluate_does_not_modify_positions         — qty_open unchanged
  18. gui_evaluate_button_renders                — GET /signals contains evaluate-portfolio
  19. gui_evaluate_post_creates_alerts_backup    — POST creates alerts + backup file
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
# Test helpers
# ---------------------------------------------------------------------------

def _fresh_db(tmp_path, name="rules.db"):
    db = Database(str(tmp_path / name))
    db.connect()
    db.init_schema()
    AssetResolver(db).get_or_create_usd_cash()
    return db


def _set_live_price(db, symbol, price):
    """Set a fresh, usable market_live price on an asset."""
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


def _macro_signal(db, event_type, source="tradingview_macro", severity="HIGH"):
    SignalService(db).add_signal(
        event_time="2026-05-01T10:00:00",
        source=source,
        event_type=event_type,
        severity=severity,
    )


def _rule_svc(db):
    return PortfolioAlertRuleService(db, AssetResolver(db))


def _portfolio_alerts(db):
    """Return all OPEN portfolio_rule alerts."""
    return SignalService(db).list_signals(status="OPEN", source="portfolio_rule")


# ---------------------------------------------------------------------------
# 1. No macro signals, no concentration > 35% → 0 alerts
# ---------------------------------------------------------------------------

def test_no_macro_no_concentration_no_alerts(tmp_path):
    db = _fresh_db(tmp_path)
    # No positions, no macro signals
    result = _rule_svc(db).evaluate_portfolio_alerts()
    assert result["created"] == 0
    assert result["skipped"] == 0
    assert _portfolio_alerts(db) == []


# ---------------------------------------------------------------------------
# 2. Concentration > 35% with no macro signal → asset_class_concentration_high
# ---------------------------------------------------------------------------

def test_concentration_high_no_macro_required(tmp_path):
    db = _fresh_db(tmp_path)
    # BTC dominates → Crypto ~99%
    _buy(db, "BTC", 1, 40000)
    _set_live_price(db, "BTC", 40000)
    _cash_in(db, 200)  # tiny cash, Crypto still >> 35%

    result = _rule_svc(db).evaluate_portfolio_alerts()
    alerts = _portfolio_alerts(db)

    concentration_alerts = [
        a for a in alerts if a["event_type"] == "asset_class_concentration_high"
    ]
    assert len(concentration_alerts) >= 1

    crypto_conc = next(
        (a for a in concentration_alerts if a["asset_class"] == "Crypto"), None
    )
    assert crypto_conc is not None
    assert crypto_conc["severity"] == "MEDIUM"
    assert crypto_conc["source"] == "portfolio_rule"
    assert "Crypto" in crypto_conc["message"]
    assert "concentration" in crypto_conc["message"].lower()
    assert result["created"] >= 1


# ---------------------------------------------------------------------------
# 3. hard_risk_off_activated + crypto >25% → severity HIGH
# ---------------------------------------------------------------------------

def test_hard_risk_off_crypto_high_severity(tmp_path):
    db = _fresh_db(tmp_path)
    _buy(db, "BTC", 1, 40000)
    _set_live_price(db, "BTC", 40000)
    _cash_in(db, 200)
    _macro_signal(db, "hard_risk_off_activated")

    _rule_svc(db).evaluate_portfolio_alerts()
    alerts = _portfolio_alerts(db)

    crypto_alert = next(
        (a for a in alerts if a["event_type"] == "crypto_exposure_high_under_risk_off"),
        None,
    )
    assert crypto_alert is not None
    assert crypto_alert["severity"] == "HIGH"
    assert "Crypto" in crypto_alert["message"]
    assert "Review" in crypto_alert["message"]


# ---------------------------------------------------------------------------
# 4. confirmed_downgrade only + crypto >25% → severity MEDIUM
# ---------------------------------------------------------------------------

def test_confirmed_downgrade_crypto_medium_severity(tmp_path):
    db = _fresh_db(tmp_path)
    _buy(db, "BTC", 1, 40000)
    _set_live_price(db, "BTC", 40000)
    _cash_in(db, 200)
    _macro_signal(db, "confirmed_downgrade", severity="HIGH")

    _rule_svc(db).evaluate_portfolio_alerts()
    alerts = _portfolio_alerts(db)

    crypto_alert = next(
        (a for a in alerts if a["event_type"] == "crypto_exposure_high_under_risk_off"),
        None,
    )
    assert crypto_alert is not None
    assert crypto_alert["severity"] == "MEDIUM"


# ---------------------------------------------------------------------------
# 5. stress + cash <5% → low_cash_under_stress, severity HIGH
# ---------------------------------------------------------------------------

def test_stress_low_cash_high_severity(tmp_path):
    db = _fresh_db(tmp_path)
    # BTC $95k + $100 cash → cash_pct ≈ 0.1% << 5%
    _buy(db, "BTC", 1, 90000)
    _set_live_price(db, "BTC", 95000)
    _cash_in(db, 100)
    _macro_signal(db, "stress", severity="CRITICAL")

    _rule_svc(db).evaluate_portfolio_alerts()
    alerts = _portfolio_alerts(db)

    cash_alert = next(
        (a for a in alerts if a["event_type"] == "low_cash_under_stress"), None
    )
    assert cash_alert is not None
    assert cash_alert["severity"] == "HIGH"
    assert "Cash" in cash_alert["message"]
    assert "dry powder" in cash_alert["message"].lower()


# ---------------------------------------------------------------------------
# 6. risk_off alone + cash <5% → low_cash_under_stress, severity MEDIUM
# ---------------------------------------------------------------------------

def test_risk_off_low_cash_medium_severity(tmp_path):
    db = _fresh_db(tmp_path)
    _buy(db, "BTC", 1, 90000)
    _set_live_price(db, "BTC", 95000)
    _cash_in(db, 100)
    _macro_signal(db, "risk_off")  # no hard stress

    _rule_svc(db).evaluate_portfolio_alerts()
    alerts = _portfolio_alerts(db)

    cash_alert = next(
        (a for a in alerts if a["event_type"] == "low_cash_under_stress"), None
    )
    assert cash_alert is not None
    assert cash_alert["severity"] == "MEDIUM"


# ---------------------------------------------------------------------------
# 7. confirmed_downgrade + unrealized_pct ≈ -35% → HIGH severity
# ---------------------------------------------------------------------------

def test_large_loss_high_severity(tmp_path):
    db = _fresh_db(tmp_path)
    # Deposit covers purchase so cash=0 and total_equity = position value > 0.
    # ETH: 5 @ $2000 cost = $10000, current $1300 → value $6500 → loss -35%
    _cash_in(db, 10000)          # deposit = purchase cost → cash=0 after buy
    _buy(db, "ETH", 5, 2000)
    _set_live_price(db, "ETH", 1300)
    _macro_signal(db, "confirmed_downgrade")

    _rule_svc(db).evaluate_portfolio_alerts()
    alerts = _portfolio_alerts(db)

    loss_alert = next(
        (
            a for a in alerts
            if a["event_type"] == "large_unrealized_loss_under_macro_downgrade"
            and a["symbol"] == "ETH"
        ),
        None,
    )
    assert loss_alert is not None
    assert loss_alert["severity"] == "HIGH"   # -35% ≤ -30% → HIGH
    assert "ETH" in loss_alert["message"]
    assert "down" in loss_alert["message"].lower()
    assert "Review" in loss_alert["message"]


# ---------------------------------------------------------------------------
# 8. hard_risk_off + unrealized_pct ≈ -25% → MEDIUM severity
# ---------------------------------------------------------------------------

def test_large_loss_medium_severity(tmp_path):
    db = _fresh_db(tmp_path)
    # BTC: 1 @ $40000 cost, current $30000 → loss -25%
    _cash_in(db, 40000)          # deposit = purchase cost → cash=0 after buy
    _buy(db, "BTC", 1, 40000)
    _set_live_price(db, "BTC", 30000)
    _macro_signal(db, "hard_risk_off_activated")

    _rule_svc(db).evaluate_portfolio_alerts()
    alerts = _portfolio_alerts(db)

    loss_alert = next(
        (
            a for a in alerts
            if a["event_type"] == "large_unrealized_loss_under_macro_downgrade"
            and a["symbol"] == "BTC"
        ),
        None,
    )
    assert loss_alert is not None
    assert loss_alert["severity"] == "MEDIUM"  # -25%: between -20% and -30%


# ---------------------------------------------------------------------------
# 9. 4 positions with loss >20% → only top 3 alerts (cap at 3)
# ---------------------------------------------------------------------------

def test_top_3_losers_cap(tmp_path):
    db = _fresh_db(tmp_path)
    # 4 crypto positions each down >20%, different loss depths.
    # Deposit total purchase cost first so total_equity > 0 after buys.
    positions_spec = [
        ("BTC",  40000, 26000),  # -35%
        ("ETH",  2000,  1200),   # -40%
        ("SOL",  100,   65),     # -35%
        ("LINK", 20,    13),     # -35%
    ]
    total_cost = sum(cost for _, cost, _ in positions_spec)
    _cash_in(db, total_cost)     # deposit = sum of purchases → cash=0 after all buys
    for sym, cost, cur in positions_spec:
        _buy(db, sym, 1, cost)
        _set_live_price(db, sym, cur)
    _macro_signal(db, "confirmed_downgrade")

    result = _rule_svc(db).evaluate_portfolio_alerts()
    alerts = _portfolio_alerts(db)

    loss_alerts = [
        a for a in alerts
        if a["event_type"] == "large_unrealized_loss_under_macro_downgrade"
    ]
    assert len(loss_alerts) == 3  # capped at 3


# ---------------------------------------------------------------------------
# 10. oversold + cash ≥5% + no stress → potential_buy_review MEDIUM
# ---------------------------------------------------------------------------

def test_oversold_cash_available_creates_review(tmp_path):
    db = _fresh_db(tmp_path)
    # Deposit $50,000, buy BTC @ $40,000 → cash=$10,000, total=$50,000, cash_pct=20% ≥ 5%.
    _cash_in(db, 50000)
    _buy(db, "BTC", 1, 40000)
    _set_live_price(db, "BTC", 40000)
    _macro_signal(db, "oversold", severity="MEDIUM")  # no stress

    _rule_svc(db).evaluate_portfolio_alerts()
    alerts = _portfolio_alerts(db)

    buy_alert = next(
        (a for a in alerts if a["event_type"] == "potential_buy_review_oversold_with_cash"),
        None,
    )
    assert buy_alert is not None
    assert buy_alert["severity"] == "MEDIUM"
    assert "oversold" in buy_alert["message"].lower()
    assert "No automatic buy" in buy_alert["message"]
    assert "Review" in buy_alert["message"]


# ---------------------------------------------------------------------------
# 11. oversold + stress active → NO buy review (anti-trigger blocks it)
# ---------------------------------------------------------------------------

def test_oversold_with_stress_no_buy_review(tmp_path):
    db = _fresh_db(tmp_path)
    _buy(db, "BTC", 1, 40000)
    _set_live_price(db, "BTC", 40000)
    _cash_in(db, 5000)
    _macro_signal(db, "oversold", severity="MEDIUM")
    _macro_signal(db, "stress", severity="CRITICAL")  # anti-trigger

    _rule_svc(db).evaluate_portfolio_alerts()
    alerts = _portfolio_alerts(db)

    buy_alert = next(
        (a for a in alerts if a["event_type"] == "potential_buy_review_oversold_with_cash"),
        None,
    )
    assert buy_alert is None


# ---------------------------------------------------------------------------
# 12. oversold + cash <5% → NO buy review (condition not met)
# ---------------------------------------------------------------------------

def test_oversold_low_cash_no_buy_review(tmp_path):
    db = _fresh_db(tmp_path)
    _buy(db, "BTC", 1, 40000)
    _set_live_price(db, "BTC", 40000)
    _cash_in(db, 100)  # cash_pct ≈ 0.25% < 5%
    _macro_signal(db, "oversold", severity="MEDIUM")

    _rule_svc(db).evaluate_portfolio_alerts()
    alerts = _portfolio_alerts(db)

    buy_alert = next(
        (a for a in alerts if a["event_type"] == "potential_buy_review_oversold_with_cash"),
        None,
    )
    assert buy_alert is None


# ---------------------------------------------------------------------------
# 13. Dedupe: second run creates 0, skipped > 0
# ---------------------------------------------------------------------------

def test_dedupe_second_run_skips_duplicates(tmp_path):
    db = _fresh_db(tmp_path)
    _buy(db, "BTC", 1, 40000)
    _set_live_price(db, "BTC", 40000)
    _cash_in(db, 200)
    _macro_signal(db, "hard_risk_off_activated")

    svc = _rule_svc(db)
    r1 = svc.evaluate_portfolio_alerts()
    r2 = svc.evaluate_portfolio_alerts()

    assert r1["created"] > 0, "First run should create at least one alert"
    assert r2["created"] == 0, "Second run must not create duplicates"
    assert r2["skipped"] > 0, "Second run must report skipped count"
    # Exactly as many alerts as first run created — no duplicates added
    assert len(_portfolio_alerts(db)) == r1["created"]


# ---------------------------------------------------------------------------
# 14. portfolio_rule signals are not used as macro input (anti-recursion)
# ---------------------------------------------------------------------------

def test_portfolio_rule_source_not_used_as_input(tmp_path):
    db = _fresh_db(tmp_path)
    _buy(db, "BTC", 1, 40000)
    _set_live_price(db, "BTC", 40000)
    _cash_in(db, 200)

    # Inject a portfolio_rule signal with a risk-off event_type
    # This MUST NOT be used as input to trigger further rules
    SignalService(db).add_signal(
        event_time="2026-05-01T10:00:00",
        source="portfolio_rule",
        event_type="hard_risk_off_activated",
        severity="HIGH",
    )

    result = _rule_svc(db).evaluate_portfolio_alerts()
    alerts = _portfolio_alerts(db)

    # Rule 5 (concentration) may fire since Crypto ~99% — but Rules 1-4
    # must NOT fire because the only OPEN risk-off signal is source=portfolio_rule
    risk_off_alerts = [
        a for a in alerts
        if a["event_type"] in (
            "crypto_exposure_high_under_risk_off",
            "low_cash_under_stress",
            "large_unrealized_loss_under_macro_downgrade",
            "potential_buy_review_oversold_with_cash",
        )
    ]
    assert len(risk_off_alerts) == 0


# ---------------------------------------------------------------------------
# 15. Generated alerts have source=portfolio_rule and status=OPEN
# ---------------------------------------------------------------------------

def test_generated_alerts_source_and_status(tmp_path):
    db = _fresh_db(tmp_path)
    _buy(db, "BTC", 1, 40000)
    _set_live_price(db, "BTC", 40000)
    _cash_in(db, 200)
    _macro_signal(db, "hard_risk_off_activated")

    _rule_svc(db).evaluate_portfolio_alerts()
    alerts = _portfolio_alerts(db)

    assert len(alerts) > 0
    for a in alerts:
        assert a["source"] == "portfolio_rule"
        assert a["status"] == "OPEN"


# ---------------------------------------------------------------------------
# 16. evaluate_portfolio_alerts does NOT modify cash_balance
# ---------------------------------------------------------------------------

def test_evaluate_does_not_modify_cash(tmp_path):
    db = _fresh_db(tmp_path)
    _buy(db, "BTC", 1, 40000)
    _set_live_price(db, "BTC", 40000)
    _cash_in(db, 500)
    _macro_signal(db, "stress")

    pnl_svc = PnLService(db, AssetResolver(db))
    cash_before = pnl_svc.cash_balance()

    _rule_svc(db).evaluate_portfolio_alerts()

    cash_after = pnl_svc.cash_balance()
    assert cash_before == cash_after


# ---------------------------------------------------------------------------
# 17. evaluate_portfolio_alerts does NOT modify positions
# ---------------------------------------------------------------------------

def test_evaluate_does_not_modify_positions(tmp_path):
    db = _fresh_db(tmp_path)
    _buy(db, "BTC", 1, 40000)
    _set_live_price(db, "BTC", 40000)
    _cash_in(db, 200)
    _macro_signal(db, "confirmed_downgrade")

    pnl_svc = PnLService(db, AssetResolver(db))
    pos_before = {
        (p["symbol"], p["account"]): (p["qty_open"], p["cost_basis"])
        for p in pnl_svc.positions()
    }

    _rule_svc(db).evaluate_portfolio_alerts()

    pos_after = {
        (p["symbol"], p["account"]): (p["qty_open"], p["cost_basis"])
        for p in pnl_svc.positions()
    }
    assert pos_before == pos_after


# ---------------------------------------------------------------------------
# GUI tests (Flask test client)
# ---------------------------------------------------------------------------

from portfolio_tracker_v2.gui.app import create_app  # noqa: E402


def _make_gui_env(tmp_path, monkeypatch, name="gui.db", setup_portfolio=False):
    """Create a Flask test environment with an isolated instance path and DB."""
    instance_path = tmp_path / "instance"
    instance_path.mkdir()

    db_path = tmp_path / name
    db = Database(str(db_path))
    db.connect()
    db.init_schema()
    AssetResolver(db).get_or_create_usd_cash()

    if setup_portfolio:
        tx_svc = TransactionService(db, AssetResolver(db))
        tx_svc.record_buy(
            symbol="BTC",
            account="Main",
            qty=Decimal("1"),
            unit_price=Decimal("40000"),
            fee_usd=Decimal("0"),
            tx_date="2026-01-01",
        )
        now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        db.connect().execute(
            """UPDATE assets
                  SET current_price = 40000,
                      price_source = 'api',
                      price_updated_at = ?,
                      valuation_method = 'market_live'
                WHERE symbol = 'BTC'""",
            (now,),
        )
        db.commit()
        tx_svc.record_cash_movement(
            account="Main",
            movement_date="2026-01-01",
            amount=Decimal("200"),
            movement_type="DEPOSIT",
        )
        SignalService(db).add_signal(
            event_time="2026-05-01T10:00:00",
            source="tradingview_macro",
            event_type="hard_risk_off_activated",
            severity="HIGH",
        )

    db.close()

    state = {"db_path": str(db_path), "mode": "TEST"}
    (instance_path / "gui_state.json").write_text(
        json.dumps(state), encoding="utf-8"
    )

    app = create_app(instance_path=str(instance_path))
    app.config.update({"TESTING": True})

    return {
        "app": app,
        "client": app.test_client(),
        "db_path": str(db_path),
        "instance_path": str(instance_path),
    }


# ---------------------------------------------------------------------------
# 18. GUI: GET /signals renders "Evaluate Portfolio Alerts" button
# ---------------------------------------------------------------------------

def test_gui_evaluate_button_renders(tmp_path, monkeypatch):
    env = _make_gui_env(tmp_path, monkeypatch)

    with env["app"].test_client() as client:
        resp = client.get("/signals")

    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="replace")
    assert "evaluate-portfolio" in body or "Evaluate Portfolio" in body


# ---------------------------------------------------------------------------
# 19. GUI: POST /signals/evaluate-portfolio creates alerts + backup
# ---------------------------------------------------------------------------

def test_gui_evaluate_post_creates_alerts_backup(tmp_path, monkeypatch):
    env = _make_gui_env(tmp_path, monkeypatch, name="gui_eval.db", setup_portfolio=True)

    with env["app"].test_client() as client:
        resp = client.post(
            "/signals/evaluate-portfolio", follow_redirects=False
        )

    # Should redirect to /signals
    assert resp.status_code == 302
    assert "/signals" in resp.headers.get("Location", "")

    # At least one portfolio_rule alert created
    db = Database(env["db_path"])
    db.connect()
    alerts = SignalService(db).list_signals(source="portfolio_rule")
    db.close()
    assert len(alerts) > 0

    # Backup file exists
    import os
    backup_dir = os.path.join(env["instance_path"], "backups")
    assert os.path.isdir(backup_dir), "Backup directory must exist"
    backup_files = [
        f for f in os.listdir(backup_dir)
        if "evaluate_portfolio_alerts" in f and f.endswith(".db")
    ]
    assert len(backup_files) >= 1, f"Expected backup file, found: {os.listdir(backup_dir)}"
