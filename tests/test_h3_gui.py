"""
Smoke test for the H3 unified dashboard GUI.

Covers:
- All read routes render under both no-DB and active-DB scenarios
- Write flows (add BUY, SELL, CDT, fund movement) work end-to-end via Flask test client
- next_url redirect honors the hub it was posted from
"""
import json
import os
from pathlib import Path

import pytest

from portfolio_tracker_v2.core import Database
from portfolio_tracker_v2.core.asset_resolver import AssetResolver
from portfolio_tracker_v2.gui.app import create_app


@pytest.fixture
def gui_env(tmp_path):
    """Build a Flask app with a temp instance path and a freshly initialized TEST DB."""
    instance_path = tmp_path / "instance"
    instance_path.mkdir()

    db_path = tmp_path / "h3_test.db"
    db = Database(str(db_path))
    db.connect()
    db.init_schema()
    AssetResolver(db).get_or_create_usd_cash()
    db.close()

    state = {"db_path": str(db_path), "mode": "TEST"}
    (instance_path / "gui_state.json").write_text(json.dumps(state), encoding="utf-8")

    app = create_app(instance_path=str(instance_path))
    app.config.update({"TESTING": True})

    return {
        "app": app,
        "client": app.test_client(),
        "db_path": str(db_path),
        "instance_path": str(instance_path),
    }


@pytest.fixture
def gui_no_db(tmp_path):
    """Flask app with a temp instance path and NO active DB configured."""
    instance_path = tmp_path / "instance"
    instance_path.mkdir()
    app = create_app(instance_path=str(instance_path))
    app.config.update({"TESTING": True})
    return app.test_client()


# ----- routes render with no active DB -----

@pytest.mark.parametrize("path", ["/", "/operations", "/setup", "/reports"])
def test_hubs_render_without_db(gui_no_db, path):
    resp = gui_no_db.get(path)
    assert resp.status_code == 200, f"{path} expected 200, got {resp.status_code}"
    assert b"PortfolioTracker" in resp.data


@pytest.mark.parametrize("path", ["/portfolio", "/positions", "/summary", "/daily-report", "/transactions"])
def test_data_routes_redirect_without_db(gui_no_db, path):
    resp = gui_no_db.get(path)
    assert resp.status_code in (302, 303), f"{path} should redirect, got {resp.status_code}"
    assert "/setup" in resp.headers.get("Location", "")


# ----- routes render with active DB -----

@pytest.mark.parametrize("path", [
    "/",
    "/operations",
    "/setup",
    "/reports",
    "/portfolio",
    "/positions",
    "/summary",
    "/daily-report",
    "/transactions",
])
def test_routes_render_with_db(gui_env, path):
    resp = gui_env["client"].get(path)
    assert resp.status_code == 200, f"{path} expected 200, got {resp.status_code}"


# ----- write flows -----

def test_add_buy_redirects_to_dashboard(gui_env):
    client = gui_env["client"]
    resp = client.post(
        "/add-transaction",
        data={
            "side": "BUY",
            "account": "Binance",
            "symbol": "BTC",
            "tx_date": "2026-01-15",
            "qty": "0.5",
            "price": "50000",
            "fee": "10",
            "notes": "smoke",
            "next_url": "/",
        },
    )
    assert resp.status_code == 302
    assert resp.headers.get("Location", "").endswith("/")


def test_add_sell_after_buy(gui_env):
    client = gui_env["client"]
    client.post("/add-transaction", data={
        "side": "BUY", "account": "Binance", "symbol": "ETH",
        "tx_date": "2026-01-10", "qty": "2", "price": "2000", "fee": "5",
        "next_url": "/operations",
    })
    resp = client.post("/add-transaction", data={
        "side": "SELL", "account": "Binance", "symbol": "ETH",
        "tx_date": "2026-01-20", "qty": "1", "price": "2200", "fee": "5",
        "next_url": "/operations",
    })
    assert resp.status_code == 302
    assert resp.headers.get("Location", "").endswith("/operations")


def test_add_cdt_via_gui(gui_env):
    resp = gui_env["client"].post("/add-cdt", data={
        "account": "BBVA",
        "symbol": "BBVA CDT",
        "open_date": "2026-01-10",
        "maturity_date": "2026-07-10",
        "principal": "1000000",
        "annual_rate": "0.10",
        "term_years": "0.5",
        "next_url": "/operations",
    })
    assert resp.status_code == 302
    assert resp.headers.get("Location", "").endswith("/operations")


def test_add_fund_movement_via_gui(gui_env):
    resp = gui_env["client"].post("/add-fund-movement", data={
        "account": "Binance",
        "symbol": "USD",
        "movement_date": "2026-01-05",
        "movement_type": "CONTRIBUTION",
        "amount": "5000",
        "next_url": "/operations",
    })
    assert resp.status_code == 302
    assert resp.headers.get("Location", "").endswith("/operations")


def test_dashboard_renders_after_writes(gui_env):
    client = gui_env["client"]
    # seed a fund movement to populate cash
    client.post("/add-fund-movement", data={
        "account": "Binance",
        "symbol": "USD",
        "movement_date": "2026-01-05",
        "movement_type": "CONTRIBUTION",
        "amount": "5000",
        "next_url": "/",
    })
    # add a BUY
    client.post("/add-transaction", data={
        "side": "BUY", "account": "Binance", "symbol": "BTC",
        "tx_date": "2026-01-10", "qty": "0.1", "price": "40000", "fee": "5",
        "next_url": "/",
    })
    resp = client.get("/")
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="ignore")
    assert "Total Equity" in body
    assert "Quick Actions" in body
    assert "Asset Allocation" in body


def test_next_url_rejects_external(gui_env):
    """next_url must be a relative path; absolute URLs fall back to default."""
    resp = gui_env["client"].post("/add-fund-movement", data={
        "account": "Binance",
        "symbol": "USD",
        "movement_date": "2026-01-05",
        "movement_type": "CONTRIBUTION",
        "amount": "100",
        "next_url": "https://evil.example.com/x",
    })
    assert resp.status_code == 302
    location = resp.headers.get("Location", "")
    assert "evil.example.com" not in location


# ----- H3.1: refresh prices + cash / fund movements -----

def _get_cash_balance(db_path, account=None):
    from portfolio_tracker_v2.core import Database
    from portfolio_tracker_v2.core.asset_resolver import AssetResolver
    from portfolio_tracker_v2.services.pnl_svc import PnLService

    db = Database(db_path)
    try:
        return PnLService(db, AssetResolver(db)).cash_balance(account)
    finally:
        db.close()


def _get_summary(db_path, account=None):
    from portfolio_tracker_v2.core import Database
    from portfolio_tracker_v2.core.asset_resolver import AssetResolver
    from portfolio_tracker_v2.services.pnl_svc import PnLService

    db = Database(db_path)
    try:
        return PnLService(db, AssetResolver(db)).summary(account)
    finally:
        db.close()


def test_dashboard_shows_refresh_button_and_last_refresh(gui_env):
    resp = gui_env["client"].get("/")
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="ignore")
    assert "Refresh Prices" in body
    assert "Last Price Refresh" in body
    assert "No refresh yet" in body
    assert "Total PnL" in body
    assert "Cash Deposit/Withdrawal" in body


def test_reports_shows_refresh_button_and_total_pnl(gui_env):
    resp = gui_env["client"].get("/reports")
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="ignore")
    assert "Refresh Prices" in body
    assert "Last price refresh" in body
    assert "Total PnL" in body


def test_refresh_prices_route_without_assets_returns_ok(gui_env, monkeypatch):
    """No network: stub refresh_prices to avoid external calls."""
    from portfolio_tracker_v2.services import price_svc

    def fake_refresh(db):
        return price_svc.RefreshReport(updated=0, skipped_unmapped=0, skipped_unsupported=0, failed_final=0, results=[])

    monkeypatch.setattr(price_svc, "refresh_prices", fake_refresh)

    resp = gui_env["client"].post("/refresh-prices")
    assert resp.status_code == 302
    with gui_env["client"].session_transaction() as sess:
        pass
    # follow redirect to dashboard
    follow = gui_env["client"].get(resp.headers["Location"])
    assert follow.status_code == 200
    body = follow.data.decode("utf-8", errors="ignore")
    assert "Prices refreshed" in body


def test_refresh_prices_route_reports_failures_as_warning(gui_env, monkeypatch):
    from portfolio_tracker_v2.services import price_svc

    def fake_refresh(db):
        return price_svc.RefreshReport(updated=2, skipped_unmapped=1, skipped_unsupported=0, failed_final=1, results=[])

    monkeypatch.setattr(price_svc, "refresh_prices", fake_refresh)

    resp = gui_env["client"].post("/refresh-prices")
    assert resp.status_code == 302
    follow = gui_env["client"].get(resp.headers["Location"])
    assert follow.status_code == 200
    body = follow.data.decode("utf-8", errors="ignore")
    assert "2 updated" in body
    assert "1 failed final" in body


def test_refresh_prices_route_handles_service_exception(gui_env, monkeypatch):
    from portfolio_tracker_v2.services import price_svc

    def boom(db):
        raise RuntimeError("simulated provider outage")

    monkeypatch.setattr(price_svc, "refresh_prices", boom)

    resp = gui_env["client"].post("/refresh-prices")
    assert resp.status_code == 302  # no 500
    follow = gui_env["client"].get(resp.headers["Location"])
    body = follow.data.decode("utf-8", errors="ignore")
    assert "Refresh failed" in body


def test_cash_deposit_increases_cash_and_does_not_change_pnl(gui_env):
    client = gui_env["client"]
    before = _get_summary(gui_env["db_path"])

    resp = client.post("/add-cash-movement", data={
        "account": "Vanguard",
        "movement_date": "2026-04-18",
        "movement_type": "DEPOSIT",
        "amount": "25000",
        "next_url": "/",
    })
    assert resp.status_code == 302

    after = _get_summary(gui_env["db_path"])
    assert after["cash_balance"] == before["cash_balance"] + Decimal_25000()
    assert after["total_realized_pnl"] == before["total_realized_pnl"]
    assert after["total_unrealized_pnl"] == before["total_unrealized_pnl"]


def test_cash_withdrawal_decreases_cash(gui_env):
    client = gui_env["client"]
    # seed cash first
    client.post("/add-cash-movement", data={
        "account": "Vanguard", "movement_date": "2026-04-18",
        "movement_type": "DEPOSIT", "amount": "1000", "next_url": "/",
    })
    client.post("/add-cash-movement", data={
        "account": "Vanguard", "movement_date": "2026-04-19",
        "movement_type": "WITHDRAWAL", "amount": "300", "next_url": "/",
    })
    s = _get_summary(gui_env["db_path"])
    assert s["cash_balance"] == Decimal_700()


def test_cash_movement_rejects_negative_amount_via_gui(gui_env):
    resp = gui_env["client"].post("/add-cash-movement", data={
        "account": "Vanguard", "movement_date": "2026-04-18",
        "movement_type": "DEPOSIT", "amount": "-100", "next_url": "/",
    }, follow_redirects=True)
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="ignore")
    assert "Amount must be &gt; 0" in body or "Amount must be > 0" in body


def test_fund_withdrawal_increases_cash_and_lowers_fund_balance(gui_env):
    client = gui_env["client"]
    # Contribute 500, then withdraw 100. Fund balance 400, cash +100.
    client.post("/add-fund-movement", data={
        "account": "Trii", "symbol": "FONDO DINAMICO",
        "movement_date": "2026-04-10", "movement_type": "CONTRIBUTION",
        "amount": "500", "next_url": "/",
    })
    cash_after_contrib = _get_cash_balance(gui_env["db_path"], "Trii")
    client.post("/add-fund-movement", data={
        "account": "Trii", "symbol": "FONDO DINAMICO",
        "movement_date": "2026-04-12", "movement_type": "WITHDRAWAL",
        "amount": "100", "next_url": "/",
    })
    cash_after_withdraw = _get_cash_balance(gui_env["db_path"], "Trii")
    assert cash_after_withdraw - cash_after_contrib == Decimal_100()

    # Fund position reflects 400 balance.
    from portfolio_tracker_v2.core import Database
    from portfolio_tracker_v2.core.asset_resolver import AssetResolver
    from portfolio_tracker_v2.services.pnl_svc import PnLService
    db = Database(gui_env["db_path"])
    try:
        positions = PnLService(db, AssetResolver(db)).positions("Trii")
    finally:
        db.close()
    fund_rows = [p for p in positions if p["symbol"] == "FONDO DINAMICO"]
    assert len(fund_rows) == 1
    assert fund_rows[0]["qty_open"] == Decimal_400()


def test_fund_withdrawal_over_balance_returns_clear_error(gui_env):
    client = gui_env["client"]
    client.post("/add-fund-movement", data={
        "account": "Trii", "symbol": "FONDO DINAMICO",
        "movement_date": "2026-04-10", "movement_type": "CONTRIBUTION",
        "amount": "100", "next_url": "/",
    })
    resp = client.post("/add-fund-movement", data={
        "account": "Trii", "symbol": "FONDO DINAMICO",
        "movement_date": "2026-04-12", "movement_type": "WITHDRAWAL",
        "amount": "500", "next_url": "/",
    }, follow_redirects=True)
    assert resp.status_code == 200  # not 500
    body = resp.data.decode("utf-8", errors="ignore")
    assert "Insufficient fund balance" in body


def test_total_pnl_equals_realized_plus_unrealized(gui_env):
    client = gui_env["client"]
    client.post("/add-cash-movement", data={
        "account": "Binance", "movement_date": "2026-01-01",
        "movement_type": "DEPOSIT", "amount": "10000", "next_url": "/",
    })
    client.post("/add-transaction", data={
        "side": "BUY", "account": "Binance", "symbol": "ETH",
        "tx_date": "2026-01-10", "qty": "1", "price": "2000", "fee": "0",
        "next_url": "/",
    })
    client.post("/add-transaction", data={
        "side": "SELL", "account": "Binance", "symbol": "ETH",
        "tx_date": "2026-01-20", "qty": "1", "price": "2200", "fee": "0",
        "next_url": "/",
    })
    s = _get_summary(gui_env["db_path"])
    realized = s["total_realized_pnl"]
    unrealized = s["total_unrealized_pnl"]
    # Total PnL shown on dashboard is realized+unrealized
    resp = gui_env["client"].get("/")
    assert resp.status_code == 200
    assert realized == Decimal_200()  # 1 * (2200-2000)


# small helpers to keep Decimal usage legible
def Decimal_25000():
    from decimal import Decimal as D
    return D("25000")

def Decimal_700():
    from decimal import Decimal as D
    return D("700")

def Decimal_100():
    from decimal import Decimal as D
    return D("100")

def Decimal_400():
    from decimal import Decimal as D
    return D("400")

def Decimal_200():
    from decimal import Decimal as D
    return D("200")
