"""
B64A — Tests for auto-evaluate portfolio alerts after refresh-prices.

Tests:
  1.  cli_refresh_triggers_evaluation_output    — CLI prints evaluation result
  2.  gui_refresh_triggers_evaluation           — GUI flash includes evaluation result
  3.  take_profit_30pct_created_after_refresh   — BTC +30% creates alert via CLI
  4.  take_profit_29pct_not_created             — BTC +29.9% → no alert
  5.  repeat_refresh_no_duplicate               — second refresh skips existing OPEN alert
  6.  no_telegram_when_created_zero             — Telegram not called if created == 0
  7.  telegram_sent_when_created_positive       — Telegram message sent with correct content
  8.  telegram_failure_does_not_break_refresh   — CLI exit 0 even if Telegram fails
  9.  evaluation_failure_does_not_break_refresh — CLI exit 0 + warning if evaluation raises
  10. cash_unchanged_after_evaluation           — financial isolation: cash
  11. positions_unchanged_after_evaluation      — financial isolation: positions
  12. pnl_unchanged_after_evaluation            — financial isolation: realized PnL
  13. no_extra_backup_from_evaluation           — GUI backup count unchanged by evaluation
"""
import json
from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from portfolio_tracker_v2.cli import main
from portfolio_tracker_v2.core import Database
from portfolio_tracker_v2.core.asset_resolver import AssetResolver
from portfolio_tracker_v2.services.price_svc import RefreshReport
from portfolio_tracker_v2.services.signal_svc import SignalService
from portfolio_tracker_v2.services.transaction_svc import TransactionService
from portfolio_tracker_v2.services.pnl_svc import PnLService
from portfolio_tracker_v2.services import telegram_notify_svc


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _init_db(db_file):
    db = Database(str(db_file))
    db.connect()
    db.init_schema()
    resolver = AssetResolver(db)
    resolver.get_or_create_usd_cash()
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


def _buy(db, symbol, qty, unit_price):
    resolver = AssetResolver(db)
    TransactionService(db, resolver).record_buy(
        symbol=symbol,
        account="Main",
        qty=Decimal(str(qty)),
        unit_price=Decimal(str(unit_price)),
        fee_usd=Decimal("0"),
        tx_date="2026-01-01",
    )


def _tp_alerts(db):
    """Return all take_profit_review_gain_threshold alerts (any status)."""
    return [
        a for a in SignalService(db).list_signals(source="portfolio_rule")
        if a["event_type"] == "take_profit_review_gain_threshold"
    ]


def _tp_open_alerts(db):
    """Return OPEN take_profit_review_gain_threshold alerts."""
    return [
        a for a in SignalService(db).list_signals(status="OPEN", source="portfolio_rule")
        if a["event_type"] == "take_profit_review_gain_threshold"
    ]


_EMPTY_REFRESH = RefreshReport(
    updated=0, skipped_unsupported=0, skipped_unmapped=0, failed_final=0, results=[]
)


def _invoke_refresh(tmp_path, db_file, extra_env=None):
    """Invoke CLI refresh-prices with mocked network; return CliRunner result."""
    runner = CliRunner()
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    if extra_env:
        env.update(extra_env)

    with pytest.MonkeyPatch.context() as mp:
        import portfolio_tracker_v2.cli as _cli
        mp.setattr(_cli, "refresh_prices", lambda db: _EMPTY_REFRESH)
        result = runner.invoke(main, ["refresh-prices"], env=env)
    return result


# ---------------------------------------------------------------------------
# GUI helpers
# ---------------------------------------------------------------------------

def _make_gui_env(tmp_path, name="b64a.db", with_position=False):
    from portfolio_tracker_v2.gui.app import create_app

    instance_path = tmp_path / "instance"
    instance_path.mkdir()

    db_path = tmp_path / name
    db = _init_db(db_path)

    if with_position:
        # BTC: cost=1000, price=1300 → +30%, gain_usd=300 ≥ 100
        TransactionService(db, AssetResolver(db)).record_buy(
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
    return {"app": app, "client": app.test_client(), "db_path": str(db_path), "instance_path": str(instance_path)}


def _post_refresh_prices(client, monkeypatch):
    """POST /refresh-prices with mocked price refresh."""
    from portfolio_tracker_v2.services import price_svc as _price_svc
    monkeypatch.setattr(_price_svc, "refresh_prices", lambda db: _EMPTY_REFRESH)
    return client.post("/refresh-prices")


def _get_dashboard_body(client, resp):
    follow = client.get(resp.headers["Location"])
    return follow.data.decode("utf-8", errors="replace")


# ---------------------------------------------------------------------------
# 1. CLI outputs evaluation result after refresh
# ---------------------------------------------------------------------------

def test_cli_refresh_triggers_evaluation_output(tmp_path):
    db_file = tmp_path / "t1.db"
    _init_db(db_file).close()

    result = _invoke_refresh(tmp_path, db_file)

    assert result.exit_code == 0, result.output
    assert "Prices refreshed:" in result.output
    assert "Portfolio alerts evaluated:" in result.output


# ---------------------------------------------------------------------------
# 2. GUI flash includes portfolio alert evaluation result after refresh
# ---------------------------------------------------------------------------

def test_gui_refresh_triggers_evaluation(tmp_path, monkeypatch):
    env = _make_gui_env(tmp_path, "t2.db")
    resp = _post_refresh_prices(env["client"], monkeypatch)

    assert resp.status_code == 302
    body = _get_dashboard_body(env["client"], resp)
    assert "Prices refreshed" in body
    assert "Portfolio alerts evaluated" in body


# ---------------------------------------------------------------------------
# 3. BTC +30% creates take_profit alert after CLI refresh-prices
# ---------------------------------------------------------------------------

def test_take_profit_30pct_created_after_refresh(tmp_path):
    db_file = tmp_path / "t3.db"
    db = _init_db(db_file)
    _buy(db, "BTC", 1, 1000)
    _set_live_price(db, "BTC", 1300)
    db.close()

    result = _invoke_refresh(tmp_path, db_file)

    assert result.exit_code == 0, result.output
    db = Database(str(db_file))
    db.connect()
    alerts = _tp_open_alerts(db)
    db.close()

    assert len(alerts) == 1
    assert alerts[0]["symbol"] == "BTC"
    assert "2 created" in result.output or "1 created" in result.output


# ---------------------------------------------------------------------------
# 4. BTC +29.9% does NOT create alert
# ---------------------------------------------------------------------------

def test_take_profit_29pct_not_created(tmp_path):
    db_file = tmp_path / "t4.db"
    db = _init_db(db_file)
    _buy(db, "BTC", 1, 1000)
    _set_live_price(db, "BTC", 1299)  # 29.9%
    db.close()

    result = _invoke_refresh(tmp_path, db_file)

    assert result.exit_code == 0, result.output
    db = Database(str(db_file))
    db.connect()
    alerts = _tp_open_alerts(db)
    db.close()
    assert alerts == []


# ---------------------------------------------------------------------------
# 5. Repeat refresh-prices does not duplicate OPEN take_profit alert
# ---------------------------------------------------------------------------

def test_repeat_refresh_no_duplicate(tmp_path):
    db_file = tmp_path / "t5.db"
    db = _init_db(db_file)
    _buy(db, "BTC", 1, 1000)
    _set_live_price(db, "BTC", 1300)
    db.close()

    # First invoke
    r1 = _invoke_refresh(tmp_path, db_file)
    # Second invoke
    r2 = _invoke_refresh(tmp_path, db_file)

    assert r1.exit_code == 0
    assert r2.exit_code == 0

    db = Database(str(db_file))
    db.connect()
    alerts = _tp_open_alerts(db)
    db.close()

    # Only one OPEN alert despite two evaluations
    assert len(alerts) == 1
    # Second run reports skipped
    assert "0 created" in r2.output
    assert "skipped" in r2.output


# ---------------------------------------------------------------------------
# 6. Telegram NOT called when created == 0
# ---------------------------------------------------------------------------

def test_no_telegram_when_created_zero(tmp_path, monkeypatch):
    db_file = tmp_path / "t6.db"
    _init_db(db_file).close()  # No positions → created = 0

    mock_send = MagicMock(return_value={"ok": True, "error": None})
    monkeypatch.setattr(telegram_notify_svc, "send_message", mock_send)
    monkeypatch.setenv("PORTFOLIO_TELEGRAM_ENABLED", "1")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token-b64a")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "99999")

    result = _invoke_refresh(tmp_path, db_file)

    assert result.exit_code == 0
    mock_send.assert_not_called()


# ---------------------------------------------------------------------------
# 7. Telegram IS sent when created > 0 and Telegram is enabled
# ---------------------------------------------------------------------------

def test_telegram_sent_when_created_positive(tmp_path, monkeypatch):
    db_file = tmp_path / "t7.db"
    db = _init_db(db_file)
    _buy(db, "BTC", 1, 1000)
    _set_live_price(db, "BTC", 1300)  # +30%, gain=$300
    db.close()

    mock_send = MagicMock(return_value={"ok": True, "error": None})
    monkeypatch.setattr(telegram_notify_svc, "send_message", mock_send)
    monkeypatch.setenv("PORTFOLIO_TELEGRAM_ENABLED", "1")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token-b64a")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "99999")

    result = _invoke_refresh(tmp_path, db_file)

    assert result.exit_code == 0
    mock_send.assert_called_once()

    msg_text = mock_send.call_args[0][0]
    assert "refresh-prices" in msg_text
    assert "BTC is up" in msg_text
    assert "SELL NOW" not in msg_text
    assert "BUY NOW" not in msg_text
    assert "Action: Review portfolio alerts. No automatic trade." in msg_text
    assert "Telegram notification: sent" in result.output


# ---------------------------------------------------------------------------
# 8. Telegram failure does NOT break CLI refresh-prices
# ---------------------------------------------------------------------------

def test_telegram_failure_does_not_break_refresh(tmp_path, monkeypatch):
    db_file = tmp_path / "t8.db"
    db = _init_db(db_file)
    _buy(db, "BTC", 1, 1000)
    _set_live_price(db, "BTC", 1300)
    db.close()

    monkeypatch.setattr(
        telegram_notify_svc, "send_message",
        lambda text: {"ok": False, "error": "network timeout"},
    )
    monkeypatch.setenv("PORTFOLIO_TELEGRAM_ENABLED", "1")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token-b64a")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "99999")

    result = _invoke_refresh(tmp_path, db_file)

    assert result.exit_code == 0
    assert "Prices refreshed:" in result.output
    assert "Portfolio alerts evaluated:" in result.output


# ---------------------------------------------------------------------------
# 9. Evaluation failure does NOT break CLI refresh; prints warning
# ---------------------------------------------------------------------------

def test_evaluation_failure_does_not_break_refresh(tmp_path, monkeypatch):
    db_file = tmp_path / "t9.db"
    _init_db(db_file).close()

    from portfolio_tracker_v2.services import portfolio_alert_notify_svc as _pa_svc
    monkeypatch.setattr(
        _pa_svc, "evaluate_and_notify_portfolio_alerts",
        lambda db, trigger: (_ for _ in ()).throw(RuntimeError("simulated evaluation crash")),
    )

    result = _invoke_refresh(tmp_path, db_file)

    # CLI must not crash
    assert result.exit_code == 0
    # Price refresh output still visible
    assert "Prices refreshed:" in result.output
    # Warning about failure visible (stderr merged into output by default)
    assert "Warning" in result.output or "portfolio alert evaluation failed" in result.output


# ---------------------------------------------------------------------------
# 10. Evaluation does NOT change cash balance
# ---------------------------------------------------------------------------

def test_cash_unchanged_after_evaluation(tmp_path):
    db_file = tmp_path / "t10.db"
    db = _init_db(db_file)
    TransactionService(db, AssetResolver(db)).record_cash_movement(
        account="Main",
        movement_date="2026-01-01",
        amount=Decimal("500"),
        movement_type="DEPOSIT",
    )
    _buy(db, "BTC", 1, 1000)
    _set_live_price(db, "BTC", 1300)

    pnl = PnLService(db, AssetResolver(db))
    cash_before = pnl.cash_balance()

    from portfolio_tracker_v2.services.portfolio_alert_notify_svc import evaluate_and_notify_portfolio_alerts
    evaluate_and_notify_portfolio_alerts(db, "refresh-prices")

    cash_after = pnl.cash_balance()
    assert cash_before == cash_after


# ---------------------------------------------------------------------------
# 11. Evaluation does NOT change positions
# ---------------------------------------------------------------------------

def test_positions_unchanged_after_evaluation(tmp_path):
    db_file = tmp_path / "t11.db"
    db = _init_db(db_file)
    _buy(db, "BTC", 1, 1000)
    _set_live_price(db, "BTC", 1300)

    pnl = PnLService(db, AssetResolver(db))
    pos_before = {
        (p["symbol"], p["account"]): (p["qty_open"], p["cost_basis"])
        for p in pnl.positions()
    }

    from portfolio_tracker_v2.services.portfolio_alert_notify_svc import evaluate_and_notify_portfolio_alerts
    evaluate_and_notify_portfolio_alerts(db, "refresh-prices")

    pos_after = {
        (p["symbol"], p["account"]): (p["qty_open"], p["cost_basis"])
        for p in pnl.positions()
    }
    assert pos_before == pos_after


# ---------------------------------------------------------------------------
# 12. Evaluation does NOT change realized PnL
# ---------------------------------------------------------------------------

def test_pnl_unchanged_after_evaluation(tmp_path):
    db_file = tmp_path / "t12.db"
    db = _init_db(db_file)
    _buy(db, "BTC", 1, 1000)
    _set_live_price(db, "BTC", 1300)

    pnl = PnLService(db, AssetResolver(db))
    realized_before = pnl.realized_pnl()

    from portfolio_tracker_v2.services.portfolio_alert_notify_svc import evaluate_and_notify_portfolio_alerts
    evaluate_and_notify_portfolio_alerts(db, "refresh-prices")

    realized_after = pnl.realized_pnl()
    assert realized_before == realized_after


# ---------------------------------------------------------------------------
# 13. GUI: no extra backup file created by alert evaluation
# ---------------------------------------------------------------------------

def test_no_extra_backup_from_evaluation(tmp_path, monkeypatch):
    env = _make_gui_env(tmp_path, "t13.db", with_position=True)
    instance_path = env["instance_path"]

    def _count_backups():
        import os
        backup_dir = os.path.join(instance_path, "backups")
        if not os.path.isdir(backup_dir):
            return 0
        return len([f for f in os.listdir(backup_dir) if f.endswith(".db")])

    before = _count_backups()
    _post_refresh_prices(env["client"], monkeypatch)
    after = _count_backups()

    # Exactly one backup created (by refresh-prices itself, not by evaluation)
    assert after == before + 1
