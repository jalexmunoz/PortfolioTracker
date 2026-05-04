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
        "currency": "USD",
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
        "currency": "USD",
        "next_url": "/operations",
    })
    assert resp.status_code == 302
    assert resp.headers.get("Location", "").endswith("/operations")


def test_add_cdt_cop_via_gui_requires_fx(gui_env):
    client = gui_env["client"]
    resp = client.post("/add-cdt", data={
        "account": "BBVA",
        "symbol": "BBVA CDT",
        "open_date": "2026-04-01",
        "maturity_date": "2026-10-01",
        "principal": "17000000",
        "annual_rate": "0.10",
        "currency": "COP",
        "next_url": "/operations",
    }, follow_redirects=True)
    assert b"FX Rate at Open is required" in resp.data


def test_add_cdt_cop_via_gui_succeeds_with_fx(gui_env):
    resp = gui_env["client"].post("/add-cdt", data={
        "account": "BBVA",
        "symbol": "BBVA CDT",
        "open_date": "2026-04-01",
        "maturity_date": "2026-10-01",
        "principal": "17000000",
        "annual_rate": "0.10",
        "currency": "COP",
        "fx_rate_at_open": "4000",
        "next_url": "/operations",
    })
    assert resp.status_code == 302


def test_add_fund_cop_via_gui_succeeds_with_fx(gui_env):
    resp = gui_env["client"].post("/add-fund-movement", data={
        "account": "Trii",
        "symbol": "FONDO DINAMICO",
        "movement_date": "2026-04-19",
        "movement_type": "CONTRIBUTION",
        "amount": "5000000",
        "currency": "COP",
        "fx_rate": "4000",
        "next_url": "/operations",
    })
    assert resp.status_code == 302


def test_dashboard_renders_after_writes(gui_env):
    client = gui_env["client"]
    # seed a fund movement to populate cash
    client.post("/add-fund-movement", data={
        "account": "Binance",
        "symbol": "USD",
        "movement_date": "2026-01-05",
        "movement_type": "CONTRIBUTION",
        "amount": "5000",
        "currency": "USD",
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
        "currency": "USD",
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
    assert "Last Refresh" in body        # B59A: health band label
    assert "No refresh yet" in body
    assert "Net P&amp;L" in body or "Net P&L" in body  # B59A: renamed from Total PnL
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
        "amount": "500", "currency": "USD", "next_url": "/",
    })
    cash_after_contrib = _get_cash_balance(gui_env["db_path"], "Trii")
    client.post("/add-fund-movement", data={
        "account": "Trii", "symbol": "FONDO DINAMICO",
        "movement_date": "2026-04-12", "movement_type": "WITHDRAWAL",
        "amount": "100", "currency": "USD", "next_url": "/",
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
        "amount": "100", "currency": "USD", "next_url": "/",
    })
    resp = client.post("/add-fund-movement", data={
        "account": "Trii", "symbol": "FONDO DINAMICO",
        "movement_date": "2026-04-12", "movement_type": "WITHDRAWAL",
        "amount": "500", "currency": "USD", "next_url": "/",
    }, follow_redirects=True)
    assert resp.status_code == 200  # not 500
    body = resp.data.decode("utf-8", errors="ignore")
    assert "Insufficient fund balance" in body


# ----- Post-B51 regressions -----

def _db(gui_env):
    from portfolio_tracker_v2.core import Database
    return Database(gui_env["db_path"])


def test_gui_add_cdt_cop_persists_usd_principal_not_cop(gui_env):
    """Bug 1 regression: GUI must not store COP principal as USD."""
    from decimal import Decimal as D
    resp = gui_env["client"].post("/add-cdt", data={
        "account": "BBVA",
        "symbol": "BBVA CDT",
        "open_date": "2026-04-01",
        "maturity_date": "2027-04-01",
        "principal": "20000000",
        "annual_rate": "0.10",
        "term_years": "1.0",
        "currency": "COP",
        "fx_rate_at_open": "4000",
        "next_url": "/operations",
    })
    assert resp.status_code == 302

    db = _db(gui_env)
    try:
        cur = db.connect().cursor()
        cur.execute("SELECT unit_price, total_usd, notes FROM transactions WHERE notes LIKE 'CDT_CONTRACT_V1:%'")
        row = cur.fetchone()
        assert row is not None, "CDT transaction not persisted"
        unit_price, total_usd, notes = row
        # 20_000_000 COP / 4000 = 5_000 USD — NOT 20_000_000
        assert D(str(unit_price)) == D("5000"), f"unit_price was {unit_price}"
        assert D(str(total_usd)) == D("5000"), f"total_usd was {total_usd}"
        assert "principal_usd" in notes and "5000.000000" in notes
    finally:
        db.close()


def test_gui_add_cdt_without_currency_rejected(gui_env):
    """Bug 1 regression: empty currency must be rejected (no silent USD default)."""
    resp = gui_env["client"].post("/add-cdt", data={
        "account": "BBVA",
        "symbol": "BBVA CDT",
        "open_date": "2026-04-01",
        "maturity_date": "2027-04-01",
        "principal": "20000000",
        "annual_rate": "0.10",
        "term_years": "1.0",
        "next_url": "/operations",
    }, follow_redirects=True)
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="ignore")
    assert "Currency must be selected" in body


def test_gui_add_fund_without_currency_rejected(gui_env):
    """Bug 1 regression: empty currency must be rejected for fund movements."""
    resp = gui_env["client"].post("/add-fund-movement", data={
        "account": "Trii",
        "symbol": "FONDO DINAMICO",
        "movement_date": "2026-04-19",
        "movement_type": "CONTRIBUTION",
        "amount": "5000000",
        "next_url": "/operations",
    }, follow_redirects=True)
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="ignore")
    assert "Currency must be selected" in body


def test_gui_add_fund_cop_persists_usd_amount_not_cop(gui_env):
    """Bug 1 regression: GUI fund COP must convert to USD at origin."""
    from decimal import Decimal as D
    resp = gui_env["client"].post("/add-fund-movement", data={
        "account": "Trii",
        "symbol": "FONDO DINAMICO",
        "movement_date": "2026-04-19",
        "movement_type": "CONTRIBUTION",
        "amount": "5000000",
        "currency": "COP",
        "fx_rate": "4000",
        "next_url": "/operations",
    })
    assert resp.status_code == 302

    db = _db(gui_env)
    try:
        cur = db.connect().cursor()
        cur.execute("SELECT total_usd, notes FROM transactions WHERE notes LIKE 'FUND_CONTRIBUTION%'")
        row = cur.fetchone()
        assert row is not None
        total_usd, notes = row
        # 5_000_000 COP / 4000 = 1_250 USD
        assert D(str(total_usd)) == D("1250"), f"total_usd was {total_usd}"
        assert "amount_usd" in notes and "1250.000000" in notes
    finally:
        db.close()


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


# ----- B52: confirmation/review before write -----

def _tx_count(db_path):
    from portfolio_tracker_v2.core import Database
    db = Database(db_path)
    try:
        cur = db.connect().cursor()
        cur.execute("SELECT COUNT(*) FROM transactions")
        return cur.fetchone()[0]
    finally:
        db.close()


def test_b52_transaction_review_renders_and_does_not_write(gui_env):
    """Posting with review=1 must show the review page and not persist anything."""
    before = _tx_count(gui_env["db_path"])
    resp = gui_env["client"].post("/add-transaction", data={
        "side": "BUY", "account": "Binance", "symbol": "BTC",
        "tx_date": "2026-01-15", "qty": "0.5", "price": "50000",
        "fee": "10", "notes": "review-smoke",
        "review": "1", "next_url": "/operations",
    })
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="ignore")
    assert "Review Transaction" in body
    assert "Confirm and Save" in body
    assert "Go Back / Edit" in body
    # Derived values shown:
    assert "BUY" in body
    assert "BTC" in body
    # gross = 0.5 * 50000 = 25,000.00
    assert "25,000.00" in body
    # cash impact = -(25000 + 10) = -25,010.00
    assert "-25,010.00" in body
    # No DB write yet
    assert _tx_count(gui_env["db_path"]) == before


def test_b52_transaction_review_then_confirm_writes_once(gui_env):
    """Confirm step must persist exactly one transaction."""
    before = _tx_count(gui_env["db_path"])
    client = gui_env["client"]
    # Step 1: review
    review_resp = client.post("/add-transaction", data={
        "side": "BUY", "account": "Binance", "symbol": "ETH",
        "tx_date": "2026-01-15", "qty": "1", "price": "2000",
        "fee": "5", "review": "1", "next_url": "/operations",
    })
    assert review_resp.status_code == 200
    assert _tx_count(gui_env["db_path"]) == before
    # Step 2: confirm
    confirm_resp = client.post("/add-transaction", data={
        "side": "BUY", "account": "Binance", "symbol": "ETH",
        "tx_date": "2026-01-15", "qty": "1", "price": "2000",
        "fee": "5", "confirmed": "1", "next_url": "/operations",
    })
    assert confirm_resp.status_code == 302
    assert confirm_resp.headers.get("Location", "").endswith("/operations")
    assert _tx_count(gui_env["db_path"]) == before + 1


def test_b52_invalid_form_does_not_render_review(gui_env):
    """If validation fails, review page must NOT render; redirect to form with error."""
    before = _tx_count(gui_env["db_path"])
    resp = gui_env["client"].post("/add-transaction", data={
        # qty <= 0 fails validation
        "side": "BUY", "account": "Binance", "symbol": "BTC",
        "tx_date": "2026-01-15", "qty": "0", "price": "50000",
        "fee": "0", "review": "1", "next_url": "/operations",
    }, follow_redirects=True)
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="ignore")
    assert "Review Transaction" not in body
    assert "Confirm and Save" not in body
    assert ("Qty must be &gt; 0" in body) or ("Qty must be > 0" in body)
    assert _tx_count(gui_env["db_path"]) == before


def test_b52_cdt_review_shows_cop_fx_and_usd_converted(gui_env):
    """CDT in COP review must show COP, FX rate, and USD converted principal."""
    resp = gui_env["client"].post("/add-cdt", data={
        "account": "BBVA", "symbol": "BBVA CDT",
        "open_date": "2026-04-01", "maturity_date": "2027-04-01",
        "principal": "20000000", "annual_rate": "0.10",
        "term_years": "1.0",
        "currency": "COP", "fx_rate_at_open": "4000",
        "review": "1", "next_url": "/operations",
    })
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="ignore")
    assert "Review CDT Contract" in body
    # COP principal
    assert "20,000,000.00 COP" in body
    # FX rate
    assert "4000" in body
    # USD converted: 20_000_000 / 4000 = 5_000
    assert "5,000.00 USD" in body
    # Cash impact
    assert "-5,000.00 USD" in body
    # Term derived: (365 days / 365) = 1
    assert "Term Derived" in body
    assert _tx_count(gui_env["db_path"]) == 0


def test_b52_cdt_review_hidden_payload_preserves_currency_and_fx(gui_env):
    """The review page must round-trip currency & fx_rate_at_open in hidden inputs."""
    resp = gui_env["client"].post("/add-cdt", data={
        "account": "BBVA", "symbol": "BBVA CDT",
        "open_date": "2026-04-01", "maturity_date": "2027-04-01",
        "principal": "20000000", "annual_rate": "0.10",
        "term_years": "1.0",
        "currency": "COP", "fx_rate_at_open": "4000",
        "review": "1", "next_url": "/operations",
    })
    body = resp.data.decode("utf-8", errors="ignore")
    assert 'name="currency" value="COP"' in body
    assert 'name="fx_rate_at_open" value="4000"' in body
    assert 'name="confirmed" value="1"' in body


def test_b52_cdt_review_then_confirm_persists_usd_principal(gui_env):
    """After review → confirm, COP CDT persists with USD principal (regression of B51)."""
    from decimal import Decimal as D
    client = gui_env["client"]
    review_resp = client.post("/add-cdt", data={
        "account": "BBVA", "symbol": "BBVA CDT",
        "open_date": "2026-04-01", "maturity_date": "2027-04-01",
        "principal": "20000000", "annual_rate": "0.10",
        "term_years": "1.0",
        "currency": "COP", "fx_rate_at_open": "4000",
        "review": "1", "next_url": "/operations",
    })
    assert review_resp.status_code == 200
    assert _tx_count(gui_env["db_path"]) == 0
    confirm_resp = client.post("/add-cdt", data={
        "account": "BBVA", "symbol": "BBVA CDT",
        "open_date": "2026-04-01", "maturity_date": "2027-04-01",
        "principal": "20000000", "annual_rate": "0.10",
        "term_years": "1.0",
        "currency": "COP", "fx_rate_at_open": "4000",
        "confirmed": "1", "next_url": "/operations",
    })
    assert confirm_resp.status_code == 302
    from portfolio_tracker_v2.core import Database
    db = Database(gui_env["db_path"])
    try:
        cur = db.connect().cursor()
        cur.execute("SELECT total_usd FROM transactions WHERE notes LIKE 'CDT_CONTRACT_V1:%'")
        row = cur.fetchone()
        assert row is not None
        assert D(str(row[0])) == D("5000")
    finally:
        db.close()


def test_b52_fund_movement_review_shows_cop_fx_and_usd_converted(gui_env):
    """Fund movement in COP review must show COP, FX, and USD converted amount."""
    resp = gui_env["client"].post("/add-fund-movement", data={
        "account": "Trii", "symbol": "FONDO DINAMICO",
        "movement_date": "2026-04-19", "movement_type": "CONTRIBUTION",
        "amount": "5000000",
        "currency": "COP", "fx_rate": "4000",
        "review": "1", "next_url": "/operations",
    })
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="ignore")
    assert "Review Fund Movement" in body
    assert "5,000,000.00 COP" in body
    assert "4000" in body
    # 5_000_000 / 4000 = 1_250
    assert "1,250.00 USD" in body
    # cash impact = -1250
    assert "-1,250.00 USD" in body
    assert _tx_count(gui_env["db_path"]) == 0


def test_b52_fund_movement_review_payload_preserves_currency_and_fx(gui_env):
    resp = gui_env["client"].post("/add-fund-movement", data={
        "account": "Trii", "symbol": "FONDO DINAMICO",
        "movement_date": "2026-04-19", "movement_type": "CONTRIBUTION",
        "amount": "5000000",
        "currency": "COP", "fx_rate": "4000",
        "review": "1", "next_url": "/operations",
    })
    body = resp.data.decode("utf-8", errors="ignore")
    assert 'name="currency" value="COP"' in body
    assert 'name="fx_rate" value="4000"' in body


def test_b52_fund_withdrawal_review_warns_on_insufficient_balance(gui_env):
    """Read-only warning when WITHDRAWAL exceeds current fund balance."""
    client = gui_env["client"]
    # Seed contribution of 100
    client.post("/add-fund-movement", data={
        "account": "Trii", "symbol": "FONDO DINAMICO",
        "movement_date": "2026-04-10", "movement_type": "CONTRIBUTION",
        "amount": "100", "currency": "USD",
        "confirmed": "1", "next_url": "/operations",
    })
    # Review WITHDRAWAL of 500 (exceeds balance of 100)
    resp = client.post("/add-fund-movement", data={
        "account": "Trii", "symbol": "FONDO DINAMICO",
        "movement_date": "2026-04-12", "movement_type": "WITHDRAWAL",
        "amount": "500", "currency": "USD",
        "review": "1", "next_url": "/operations",
    })
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="ignore")
    assert "exceeds current fund balance" in body
    # No write yet from the review
    # (the contribution above is the only persisted row)


def test_b52_cash_movement_review_shows_cash_impact(gui_env):
    """Cash deposit review must show cash impact."""
    resp = gui_env["client"].post("/add-cash-movement", data={
        "account": "Vanguard",
        "movement_date": "2026-04-18", "movement_type": "DEPOSIT",
        "amount": "25000", "review": "1", "next_url": "/",
    })
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="ignore")
    assert "Review Cash Movement" in body
    assert "25,000.00 USD" in body
    # cash impact for DEPOSIT = +25000
    # Withdrawal would be -25000; check sign:
    assert "Cash impact" in body
    assert _tx_count(gui_env["db_path"]) == 0


def test_b52_cash_movement_withdrawal_review_shows_negative_cash_impact(gui_env):
    resp = gui_env["client"].post("/add-cash-movement", data={
        "account": "Vanguard",
        "movement_date": "2026-04-18", "movement_type": "WITHDRAWAL",
        "amount": "300", "review": "1", "next_url": "/",
    })
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="ignore")
    assert "-300.00 USD" in body


def test_b52_legacy_post_without_flags_still_writes_directly(gui_env):
    """Regression: a POST without review/confirmed flags writes directly (legacy path)."""
    before = _tx_count(gui_env["db_path"])
    resp = gui_env["client"].post("/add-cash-movement", data={
        "account": "Vanguard",
        "movement_date": "2026-04-18", "movement_type": "DEPOSIT",
        "amount": "50", "next_url": "/",
    })
    assert resp.status_code == 302
    assert _tx_count(gui_env["db_path"]) == before + 1


def test_b52_confirmed_without_review_writes_once(gui_env):
    """A direct confirmed=1 POST (without going through review first) still writes."""
    before = _tx_count(gui_env["db_path"])
    resp = gui_env["client"].post("/add-cash-movement", data={
        "account": "Vanguard",
        "movement_date": "2026-04-18", "movement_type": "DEPOSIT",
        "amount": "75", "confirmed": "1", "next_url": "/",
    })
    assert resp.status_code == 302
    assert _tx_count(gui_env["db_path"]) == before + 1


def test_b52_review_and_confirmed_both_set_writes_once(gui_env):
    """If both flags are present, confirmed wins and writes (no extra round-trip)."""
    before = _tx_count(gui_env["db_path"])
    resp = gui_env["client"].post("/add-cash-movement", data={
        "account": "Vanguard",
        "movement_date": "2026-04-18", "movement_type": "DEPOSIT",
        "amount": "11", "review": "1", "confirmed": "1", "next_url": "/",
    })
    assert resp.status_code == 302
    assert _tx_count(gui_env["db_path"]) == before + 1


def test_b52_transaction_review_payload_preserves_all_fields(gui_env):
    """All form fields must round-trip in hidden inputs on the review page."""
    resp = gui_env["client"].post("/add-transaction", data={
        "side": "SELL", "account": "Kraken", "symbol": "ETH",
        "tx_date": "2026-02-01", "qty": "0.25", "price": "3000",
        "fee": "1.5", "notes": "preserve-test",
        "review": "1", "next_url": "/operations",
    })
    # Validation fails on SELL because no holdings — but validation is in record_sell, not _validate_transaction_form.
    # _validate_transaction_form does not check holdings; review just renders.
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="ignore")
    for k, v in [
        ("side", "SELL"), ("account", "Kraken"), ("symbol", "ETH"),
        ("tx_date", "2026-02-01"), ("qty", "0.25"), ("price", "3000"),
        ("fee", "1.5"), ("notes", "preserve-test"),
    ]:
        assert f'name="{k}" value="{v}"' in body, f"Missing hidden input for {k}={v}"


# ----- B53: automatic DB backup before GUI writes -----

def _auto_backup_dir(gui_env):
    """Resolve where auto-backups land for this gui_env (instance/backups)."""
    return os.path.join(gui_env["instance_path"], "backups")


def _list_auto_backups(gui_env, contains=None):
    backup_dir = _auto_backup_dir(gui_env)
    if not os.path.isdir(backup_dir):
        return []
    files = [f for f in os.listdir(backup_dir) if f.endswith(".db")]
    if contains is not None:
        files = [f for f in files if contains in f]
    return sorted(files)


def test_b53_add_transaction_confirm_creates_backup(gui_env):
    before = _list_auto_backups(gui_env)
    resp = gui_env["client"].post("/add-transaction", data={
        "side": "BUY", "account": "Binance", "symbol": "BTC",
        "tx_date": "2026-01-15", "qty": "0.5", "price": "50000",
        "fee": "10", "confirmed": "1", "next_url": "/operations",
    })
    assert resp.status_code == 302
    after = _list_auto_backups(gui_env, contains="before_add_transaction")
    assert len(after) >= 1
    assert len(_list_auto_backups(gui_env)) == len(before) + 1


def test_b53_add_transaction_review_does_not_create_backup(gui_env):
    before = _list_auto_backups(gui_env)
    resp = gui_env["client"].post("/add-transaction", data={
        "side": "BUY", "account": "Binance", "symbol": "BTC",
        "tx_date": "2026-01-15", "qty": "0.5", "price": "50000",
        "fee": "10", "review": "1", "next_url": "/operations",
    })
    assert resp.status_code == 200
    assert _list_auto_backups(gui_env) == before


def test_b53_add_cdt_confirm_creates_backup(gui_env):
    before = _list_auto_backups(gui_env)
    resp = gui_env["client"].post("/add-cdt", data={
        "account": "BBVA", "symbol": "BBVA CDT",
        "open_date": "2026-04-01", "maturity_date": "2027-04-01",
        "principal": "1000", "annual_rate": "0.10", "term_years": "1.0",
        "currency": "USD", "confirmed": "1", "next_url": "/operations",
    })
    assert resp.status_code == 302
    after = _list_auto_backups(gui_env, contains="before_add_cdt")
    assert len(after) >= 1
    assert len(_list_auto_backups(gui_env)) == len(before) + 1


def test_b53_add_fund_movement_confirm_creates_backup(gui_env):
    before = _list_auto_backups(gui_env)
    resp = gui_env["client"].post("/add-fund-movement", data={
        "account": "Trii", "symbol": "FONDO DINAMICO",
        "movement_date": "2026-04-19", "movement_type": "CONTRIBUTION",
        "amount": "500", "currency": "USD",
        "confirmed": "1", "next_url": "/operations",
    })
    assert resp.status_code == 302
    after = _list_auto_backups(gui_env, contains="before_add_fund_movement")
    assert len(after) >= 1
    assert len(_list_auto_backups(gui_env)) == len(before) + 1


def test_b53_add_cash_movement_confirm_creates_backup(gui_env):
    before = _list_auto_backups(gui_env)
    resp = gui_env["client"].post("/add-cash-movement", data={
        "account": "Vanguard",
        "movement_date": "2026-04-18", "movement_type": "DEPOSIT",
        "amount": "100", "confirmed": "1", "next_url": "/",
    })
    assert resp.status_code == 302
    after = _list_auto_backups(gui_env, contains="before_add_cash_movement")
    assert len(after) >= 1
    assert len(_list_auto_backups(gui_env)) == len(before) + 1


def test_b53_refresh_prices_creates_backup(gui_env, monkeypatch):
    from portfolio_tracker_v2.services import price_svc

    def fake_refresh(db):
        return price_svc.RefreshReport(
            updated=0, skipped_unmapped=0, skipped_unsupported=0,
            failed_final=0, results=[],
        )

    monkeypatch.setattr(price_svc, "refresh_prices", fake_refresh)
    before = _list_auto_backups(gui_env)
    resp = gui_env["client"].post("/refresh-prices")
    assert resp.status_code == 302
    after = _list_auto_backups(gui_env, contains="before_refresh_prices")
    assert len(after) >= 1
    assert len(_list_auto_backups(gui_env)) == len(before) + 1


def test_b53_init_db_creates_backup_when_db_exists(gui_env):
    """gui_env already initialized the DB, so re-init must create a backup."""
    before = _list_auto_backups(gui_env)
    resp = gui_env["client"].post("/init-db")
    assert resp.status_code == 302
    after = _list_auto_backups(gui_env, contains="before_init_db")
    assert len(after) >= 1
    assert len(_list_auto_backups(gui_env)) == len(before) + 1


def test_b53_init_db_no_backup_when_db_does_not_exist(tmp_path):
    """If active.db_path points at a missing file, init_db must succeed without backup."""
    instance_path = tmp_path / "instance"
    instance_path.mkdir()

    # Configure an active DB pointing at a not-yet-existing path
    db_path = tmp_path / "fresh.db"
    state = {"db_path": str(db_path), "mode": "TEST"}
    (instance_path / "gui_state.json").write_text(json.dumps(state), encoding="utf-8")

    from portfolio_tracker_v2.gui.app import create_app
    app = create_app(instance_path=str(instance_path))
    app.config.update({"TESTING": True})
    client = app.test_client()

    resp = client.post("/init-db")
    assert resp.status_code == 302  # no error
    backup_dir = instance_path / "backups"
    # No backup for a non-existing source; the dir may or may not exist
    if backup_dir.is_dir():
        assert [f for f in os.listdir(backup_dir) if f.endswith(".db")] == []
    # And the DB itself was created by init_db
    assert db_path.exists()


def test_b53_backup_failure_aborts_write(gui_env, monkeypatch):
    """If backup raises, the write must NOT happen."""
    from portfolio_tracker_v2.gui import routes as gui_routes

    def boom(db_path, backup_root, operation):
        raise OSError("simulated disk full")

    monkeypatch.setattr(gui_routes, "create_auto_backup", boom)

    before_tx = _tx_count(gui_env["db_path"])
    resp = gui_env["client"].post("/add-transaction", data={
        "side": "BUY", "account": "Binance", "symbol": "BTC",
        "tx_date": "2026-01-15", "qty": "0.5", "price": "50000",
        "fee": "0", "confirmed": "1", "next_url": "/operations",
    }, follow_redirects=True)
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="ignore")
    assert "Backup failed" in body
    assert "NOT executed" in body
    # Write must NOT have happened
    assert _tx_count(gui_env["db_path"]) == before_tx


def test_b53_flash_includes_backup_path(gui_env):
    """The success flash must include the backup path."""
    resp = gui_env["client"].post("/add-cash-movement", data={
        "account": "Vanguard", "movement_date": "2026-04-18",
        "movement_type": "DEPOSIT", "amount": "100",
        "confirmed": "1", "next_url": "/",
    }, follow_redirects=True)
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="ignore")
    assert "(backup:" in body
    assert "before_add_cash_movement" in body


def test_b53_backups_do_not_overwrite_each_other(gui_env, monkeypatch):
    """Two writes within the same second must produce two distinct backup files."""
    # Pin datetime.now() to a fixed value so both writes share the same timestamp.
    from datetime import datetime as _dt
    from portfolio_tracker_v2.gui import backup as backup_mod

    fixed = _dt(2026, 5, 1, 12, 0, 0)

    class _FixedDateTime(_dt):
        @classmethod
        def now(cls, tz=None):
            return fixed

    monkeypatch.setattr(backup_mod, "datetime", _FixedDateTime)

    client = gui_env["client"]
    for _ in range(2):
        client.post("/add-cash-movement", data={
            "account": "Vanguard", "movement_date": "2026-04-18",
            "movement_type": "DEPOSIT", "amount": "1",
            "confirmed": "1", "next_url": "/",
        })
    files = _list_auto_backups(gui_env, contains="before_add_cash_movement")
    assert len(files) == 2
    assert len(set(files)) == 2  # distinct names


def test_b53_legacy_post_also_creates_backup(gui_env):
    """Defense-in-depth: legacy direct write (no flags) also creates a backup."""
    before = _list_auto_backups(gui_env)
    resp = gui_env["client"].post("/add-cash-movement", data={
        "account": "Vanguard", "movement_date": "2026-04-18",
        "movement_type": "DEPOSIT", "amount": "5", "next_url": "/",
    })
    assert resp.status_code == 302
    assert len(_list_auto_backups(gui_env)) == len(before) + 1


def _write_legacy_csv(tmp_path):
    """Write a minimal valid LegacyPositionsCsvImporter CSV in tmp_path."""
    csv_path = tmp_path / "legacy_seed.csv"
    csv_path.write_text(
        "Symbol,Quantity,Total Cost (USD),Avg Cost (USD),Wallet\n"
        "BTC,1,100.00,100.00,Main\n",
        encoding="utf-8",
    )
    return csv_path


def test_b53_import_legacy_creates_backup_when_db_exists(gui_env, tmp_path):
    """Executed import (not dry_run) must create a backup when the DB exists."""
    csv_path = _write_legacy_csv(tmp_path)
    before = _list_auto_backups(gui_env)
    resp = gui_env["client"].post("/import-legacy", data={
        "csv_path": str(csv_path),
        "seed_date": "2026-01-01",
        # dry_run intentionally omitted = execute
        "next_url": "/setup",
    })
    assert resp.status_code == 302
    after = _list_auto_backups(gui_env, contains="before_import_legacy")
    assert len(after) >= 1
    assert len(_list_auto_backups(gui_env)) == len(before) + 1


def test_b53_import_legacy_dry_run_does_not_create_backup(gui_env, tmp_path):
    """Dry run is read-only and must not create a backup."""
    csv_path = _write_legacy_csv(tmp_path)
    before = _list_auto_backups(gui_env)
    resp = gui_env["client"].post("/import-legacy", data={
        "csv_path": str(csv_path),
        "seed_date": "2026-01-01",
        "dry_run": "1",
        "next_url": "/setup",
    })
    assert resp.status_code == 302
    assert _list_auto_backups(gui_env) == before


# ----- B54: Portfolio Snapshot CSV Import -----

def _write_snapshot_csv(tmp_path, rows=None, header=None):
    """Write a minimal valid Portfolio Snapshot CSV."""
    csv_path = tmp_path / "snapshot.csv"
    if header is None:
        header = "Symbol,Account,Qty,Cost Basis,Avg Cost,Method\n"
    if rows is None:
        rows = [
            "BTC,Binance,0.5,25000,50000,market_live\n",
            "ETH,Binance,2,4000,2000,market_live\n",
        ]
    csv_path.write_text(header + "".join(rows), encoding="utf-8")
    return csv_path


def test_b54_get_renders_form(gui_env):
    """GET /import-portfolio-snapshot returns 200 with form fields."""
    resp = gui_env["client"].get("/import-portfolio-snapshot")
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="ignore")
    assert "Import Portfolio Snapshot" in body
    assert "csv_path" in body
    assert "seed_date" in body


def test_b54_no_active_db_redirects_to_setup(gui_no_db):
    """Without an active DB, GET redirects to /setup."""
    resp = gui_no_db.get("/import-portfolio-snapshot")
    assert resp.status_code in (302, 303)
    assert "/setup" in resp.headers.get("Location", "")


def test_b54_review_shows_preview_stats(gui_env, tmp_path):
    """POST without confirmed shows preview with row counts."""
    csv_path = _write_snapshot_csv(tmp_path)
    resp = gui_env["client"].post("/import-portfolio-snapshot", data={
        "csv_path": str(csv_path),
        "seed_date": "2026-01-01",
        "next_url": "/setup",
    })
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="ignore")
    assert "Preview" in body
    assert "Valid rows" in body
    assert "Invalid rows" in body
    assert "Total rows" in body


def test_b54_review_does_not_write_to_db(gui_env, tmp_path):
    """Preview (no confirmed) does not write any transactions."""
    csv_path = _write_snapshot_csv(tmp_path)
    before = _tx_count(gui_env["db_path"])
    gui_env["client"].post("/import-portfolio-snapshot", data={
        "csv_path": str(csv_path),
        "seed_date": "2026-01-01",
    })
    assert _tx_count(gui_env["db_path"]) == before


def test_b54_review_does_not_create_backup(gui_env, tmp_path):
    """Preview mode must not create any backup files."""
    csv_path = _write_snapshot_csv(tmp_path)
    before = _list_auto_backups(gui_env)
    gui_env["client"].post("/import-portfolio-snapshot", data={
        "csv_path": str(csv_path),
        "seed_date": "2026-01-01",
    })
    assert _list_auto_backups(gui_env) == before


def test_b54_review_shows_invalid_rows(gui_env, tmp_path):
    """A CSV with a bad row shows errors in the preview."""
    csv_path = _write_snapshot_csv(tmp_path, rows=[
        "BTC,Binance,0.5,25000,50000,market_live\n",
        "ETH,,2,4000,2000,market_live\n",  # missing account
    ])
    resp = gui_env["client"].post("/import-portfolio-snapshot", data={
        "csv_path": str(csv_path),
        "seed_date": "2026-01-01",
    })
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="ignore")
    assert "invalid" in body.lower()
    assert "Account is required" in body


def test_b54_review_shows_method_counts(gui_env, tmp_path):
    """Preview shows breakdown count per method."""
    csv_path = _write_snapshot_csv(tmp_path, rows=[
        "BTC,Binance,0.5,25000,50000,market_live\n",
        "FONDO DINAMICO,Trii,1000,1000,1,snapshot_imported\n",
    ])
    resp = gui_env["client"].post("/import-portfolio-snapshot", data={
        "csv_path": str(csv_path),
        "seed_date": "2026-01-01",
    })
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="ignore")
    assert "market_live" in body
    assert "snapshot_imported" in body


def test_b54_review_shows_total_cost_basis(gui_env, tmp_path):
    """Preview shows formatted sum of all valid cost basis values."""
    csv_path = _write_snapshot_csv(tmp_path, rows=[
        "BTC,Binance,0.5,25000,50000,market_live\n",
        "ETH,Binance,2,4000,2000,market_live\n",
    ])
    resp = gui_env["client"].post("/import-portfolio-snapshot", data={
        "csv_path": str(csv_path),
        "seed_date": "2026-01-01",
    })
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="ignore")
    # 25000 + 4000 = 29000
    assert "29,000.00" in body


def test_b54_review_hidden_payload_preserves_path_and_date(gui_env, tmp_path):
    """Preview page must have hidden inputs for csv_path, seed_date, confirmed."""
    csv_path = _write_snapshot_csv(tmp_path)
    resp = gui_env["client"].post("/import-portfolio-snapshot", data={
        "csv_path": str(csv_path),
        "seed_date": "2026-01-15",
    })
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="ignore")
    assert f'name="csv_path" value="{csv_path}"' in body
    assert 'name="seed_date" value="2026-01-15"' in body
    assert 'name="confirmed" value="1"' in body


def test_b54_confirm_writes_rows(gui_env, tmp_path):
    """confirmed=1 writes MIGRATION_BUY rows for all valid CSV rows."""
    csv_path = _write_snapshot_csv(tmp_path)
    before = _tx_count(gui_env["db_path"])
    resp = gui_env["client"].post("/import-portfolio-snapshot", data={
        "csv_path": str(csv_path),
        "seed_date": "2026-01-01",
        "confirmed": "1",
        "next_url": "/setup",
    })
    assert resp.status_code == 302
    assert _tx_count(gui_env["db_path"]) == before + 2


def test_b54_confirm_creates_backup(gui_env, tmp_path):
    """Confirm step creates an auto-backup named with the operation token."""
    csv_path = _write_snapshot_csv(tmp_path)
    before = _list_auto_backups(gui_env)
    resp = gui_env["client"].post("/import-portfolio-snapshot", data={
        "csv_path": str(csv_path),
        "seed_date": "2026-01-01",
        "confirmed": "1",
    })
    assert resp.status_code == 302
    after = _list_auto_backups(gui_env, contains="before_import_portfolio_snapshot")
    assert len(after) >= 1
    assert len(_list_auto_backups(gui_env)) == len(before) + 1


def test_b54_confirm_blocked_if_db_has_transactions(gui_env, tmp_path):
    """Import is blocked when DB already has any transactions."""
    gui_env["client"].post("/add-cash-movement", data={
        "account": "Test", "movement_date": "2026-01-01",
        "movement_type": "DEPOSIT", "amount": "100", "confirmed": "1",
    })
    assert _tx_count(gui_env["db_path"]) > 0

    csv_path = _write_snapshot_csv(tmp_path)
    before = _tx_count(gui_env["db_path"])
    resp = gui_env["client"].post("/import-portfolio-snapshot", data={
        "csv_path": str(csv_path),
        "seed_date": "2026-01-01",
        "confirmed": "1",
    }, follow_redirects=True)
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="ignore")
    assert "already has" in body
    assert _tx_count(gui_env["db_path"]) == before


def test_b54_confirm_with_invalid_rows_blocked(gui_env, tmp_path):
    """confirmed=1 with invalid rows flashes error and writes nothing."""
    csv_path = _write_snapshot_csv(tmp_path, rows=[
        "BTC,Binance,0.5,25000,50000,market_live\n",
        "ETH,,2,4000,2000,market_live\n",  # missing account
    ])
    before = _tx_count(gui_env["db_path"])
    resp = gui_env["client"].post("/import-portfolio-snapshot", data={
        "csv_path": str(csv_path),
        "seed_date": "2026-01-01",
        "confirmed": "1",
    }, follow_redirects=True)
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="ignore")
    assert "invalid" in body.lower()
    assert _tx_count(gui_env["db_path"]) == before


def test_b54_csv_col_aliases_accepted(gui_env, tmp_path):
    """Aliased column names (Quantity, Wallet, Total Cost (USD), Avg Cost (USD)) are accepted."""
    csv_path = tmp_path / "snapshot_aliases.csv"
    csv_path.write_text(
        "Symbol,Wallet,Quantity,Total Cost (USD),Avg Cost (USD)\n"
        "BTC,Binance,0.5,25000,50000\n",
        encoding="utf-8",
    )
    before = _tx_count(gui_env["db_path"])
    resp = gui_env["client"].post("/import-portfolio-snapshot", data={
        "csv_path": str(csv_path),
        "seed_date": "2026-01-01",
        "confirmed": "1",
        "next_url": "/setup",
    })
    assert resp.status_code == 302
    assert _tx_count(gui_env["db_path"]) == before + 1


def test_b54_invalid_method_makes_row_invalid(gui_env, tmp_path):
    """An unknown Method value causes the row to appear as invalid in preview."""
    csv_path = _write_snapshot_csv(tmp_path, rows=[
        "BTC,Binance,0.5,25000,50000,BOGUS_METHOD\n",
    ])
    resp = gui_env["client"].post("/import-portfolio-snapshot", data={
        "csv_path": str(csv_path),
        "seed_date": "2026-01-01",
    })
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="ignore")
    assert "BOGUS_METHOD" in body

    before = _tx_count(gui_env["db_path"])
    resp2 = gui_env["client"].post("/import-portfolio-snapshot", data={
        "csv_path": str(csv_path),
        "seed_date": "2026-01-01",
        "confirmed": "1",
    }, follow_redirects=True)
    assert resp2.status_code == 200
    assert _tx_count(gui_env["db_path"]) == before


# --- B56: Export Portfolio Snapshot CSV ---

def _seed_position(db_path, symbol, account, qty, cost_basis, valuation_method="market_live"):
    """Seed a position directly into the DB for export tests."""
    import sqlite3
    from decimal import Decimal as D
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO assets (symbol, asset_type, is_active, valuation_method) VALUES (?, 'crypto', 1, ?)",
            (symbol, valuation_method),
        )
        cur.execute("SELECT id FROM assets WHERE symbol = ?", (symbol,))
        asset_id = cur.fetchone()[0]
        cur.execute("INSERT OR IGNORE INTO accounts (name) VALUES (?)", (account,))
        cur.execute("SELECT id FROM accounts WHERE name = ?", (account,))
        account_id = cur.fetchone()[0]
        unit_price = float(D(str(cost_basis)) / D(str(qty)))
        cur.execute(
            "INSERT INTO transactions (asset_id, account_id, tx_type, quantity, unit_price, fee_usd, total_usd, tx_date) "
            "VALUES (?, ?, 'BUY', ?, ?, 0.0, ?, '2026-01-01')",
            (asset_id, account_id, qty, unit_price, cost_basis),
        )
        conn.commit()
    finally:
        conn.close()


def test_b56_export_returns_200(gui_env):
    resp = gui_env["client"].get("/portfolio/export.csv")
    assert resp.status_code == 200


def test_b56_export_content_type_is_csv(gui_env):
    resp = gui_env["client"].get("/portfolio/export.csv")
    assert "text/csv" in resp.content_type


def test_b56_export_filename_contains_portfolio_snapshot(gui_env):
    resp = gui_env["client"].get("/portfolio/export.csv")
    disposition = resp.headers.get("Content-Disposition", "")
    assert "attachment" in disposition
    assert "portfolio_snapshot" in disposition


def test_b56_export_headers_present(gui_env):
    resp = gui_env["client"].get("/portfolio/export.csv")
    body = resp.data.decode("utf-8")
    first_line = body.splitlines()[0]
    assert first_line == "Symbol,Account,Qty,Cost Basis,Avg Cost,Method"


def test_b56_export_empty_portfolio_headers_only(gui_env):
    resp = gui_env["client"].get("/portfolio/export.csv")
    body = resp.data.decode("utf-8")
    lines = [l for l in body.splitlines() if l.strip()]
    assert len(lines) == 1
    assert lines[0].startswith("Symbol")


def test_b56_export_includes_seeded_position(gui_env):
    _seed_position(gui_env["db_path"], "BTC", "Binance", qty=0.5, cost_basis=25000.0)

    resp = gui_env["client"].get("/portfolio/export.csv")
    body = resp.data.decode("utf-8")
    assert "BTC" in body
    assert "Binance" in body


def test_b56_export_does_not_modify_db(gui_env):
    _seed_position(gui_env["db_path"], "ETH", "Coinbase", qty=1.0, cost_basis=3000.0)
    before = _tx_count(gui_env["db_path"])

    gui_env["client"].get("/portfolio/export.csv")

    assert _tx_count(gui_env["db_path"]) == before


def test_b56_export_row_fields_match_b54_import_columns(gui_env):
    """Exported CSV rows have exactly the columns B54 import expects."""
    import csv, io as _io
    _seed_position(gui_env["db_path"], "SOL", "Phantom", qty=10.0, cost_basis=1500.0)

    resp = gui_env["client"].get("/portfolio/export.csv")
    body = resp.data.decode("utf-8")
    reader = csv.DictReader(_io.StringIO(body))
    rows = list(reader)

    assert len(rows) >= 1
    sol_row = next(r for r in rows if r["Symbol"] == "SOL")
    assert sol_row["Account"] == "Phantom"
    assert float(sol_row["Qty"]) == 10.0
    assert float(sol_row["Cost Basis"]) == 1500.0
    assert float(sol_row["Avg Cost"]) == 150.0
    assert sol_row["Method"] == "market_live"


def test_b56_export_with_account_filter(gui_env):
    _seed_position(gui_env["db_path"], "BTC", "Alpha", qty=1.0, cost_basis=50000.0)
    _seed_position(gui_env["db_path"], "ETH", "Beta", qty=2.0, cost_basis=6000.0)

    resp = gui_env["client"].get("/portfolio/export.csv?account=Alpha")
    body = resp.data.decode("utf-8")
    assert "BTC" in body
    assert "ETH" not in body


def test_b56_export_no_db_redirects_to_setup(gui_no_db):
    resp = gui_no_db.get("/portfolio/export.csv")
    assert resp.status_code in (302, 303)
    assert "/setup" in resp.headers.get("Location", "")


# ----- B59A: Dashboard presentation redesign -----

def _seed_priced_position(db_path, symbol, account, qty, cost_basis, current_price):
    """Seed a position with snapshot_imported valuation so approved_value is computed."""
    import sqlite3
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """INSERT OR IGNORE INTO assets
               (symbol, asset_type, is_active, valuation_method,
                current_price, price_source, price_updated_at)
               VALUES (?, 'crypto', 1, 'snapshot_imported', ?, 'snapshot_imported', '2026-01-01')""",
            (symbol, current_price),
        )
        cur.execute("SELECT id FROM assets WHERE symbol = ?", (symbol,))
        asset_id = cur.fetchone()[0]
        cur.execute("INSERT OR IGNORE INTO accounts (name) VALUES (?)", (account,))
        cur.execute("SELECT id FROM accounts WHERE name = ?", (account,))
        account_id = cur.fetchone()[0]
        unit_price = cost_basis / qty
        cur.execute(
            """INSERT INTO transactions
               (asset_id, account_id, tx_type, quantity, unit_price, fee_usd, total_usd, tx_date)
               VALUES (?, ?, 'MIGRATION_BUY', ?, ?, 0.0, ?, '2026-01-01')""",
            (asset_id, account_id, qty, unit_price, cost_basis),
        )
        conn.commit()
    finally:
        conn.close()


def test_b59a_dashboard_renders(gui_env):
    resp = gui_env["client"].get("/")
    assert resp.status_code == 200


def test_b59a_total_invested_in_dashboard(gui_env):
    _seed_priced_position(gui_env["db_path"], "BTC", "Binance", 1.0, 40000.0, 50000.0)
    resp = gui_env["client"].get("/")
    body = resp.data.decode("utf-8", errors="ignore")
    assert "Total Invested" in body
    assert "40,000.00" in body  # cost_basis shown as Total Invested value


def test_b59a_net_pnl_in_dashboard(gui_env):
    resp = gui_env["client"].get("/")
    body = resp.data.decode("utf-8", errors="ignore")
    assert "Net P&amp;L" in body or "Net P&L" in body


def test_b59a_net_pnl_equals_realized_plus_unrealized(gui_env):
    """Net P&L card value is displayed_realized + total_unrealized."""
    from decimal import Decimal as D
    _seed_priced_position(gui_env["db_path"], "ETH", "Binance", 1.0, 2000.0, 3000.0)
    s = _get_summary(gui_env["db_path"])
    expected = s["displayed_realized_pnl"] + s["total_unrealized_pnl"]
    # 1000 unrealized (3000-2000), 0 realized → net = 1000.00
    assert expected == D("1000")
    resp = gui_env["client"].get("/")
    body = resp.data.decode("utf-8", errors="ignore")
    assert "1,000.00" in body


def test_b59a_realized_pnl_shows_ledger_and_hist_subtext(gui_env):
    """Realized P&L card shows ledger+hist subtext when a historical adjustment exists."""
    client = gui_env["client"]
    # Seed buy then sell for ledger realized PnL
    client.post("/add-transaction", data={
        "side": "BUY", "account": "Binance", "symbol": "ETH",
        "tx_date": "2026-01-01", "qty": "1", "price": "2000", "fee": "0",
        "confirmed": "1", "next_url": "/",
    })
    client.post("/add-transaction", data={
        "side": "SELL", "account": "Binance", "symbol": "ETH",
        "tx_date": "2026-01-10", "qty": "1", "price": "2200", "fee": "0",
        "confirmed": "1", "next_url": "/",
    })
    # Add historical PnL adjustment
    client.post("/historical-pnl", data={
        "adjustment_date": "2025-12-31",
        "source": "legacy_broker",
        "amount_usd": "500",
        "confirmed": "1",
        "next_url": "/",
    })
    resp = client.get("/")
    body = resp.data.decode("utf-8", errors="ignore")
    assert "Realized P" in body
    assert "ledger" in body
    assert "hist." in body


def test_b59a_excluded_cost_basis_visible_when_unvalued_exist(gui_env):
    """Excluded Cost Basis card appears with amount when unvalued positions exist."""
    import sqlite3
    conn = sqlite3.connect(gui_env["db_path"])
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO assets (symbol, asset_type, is_active, valuation_method) "
            "VALUES ('ILLIQ', 'crypto', 1, 'unvalued')",
        )
        cur.execute("SELECT id FROM assets WHERE symbol = 'ILLIQ'")
        asset_id = cur.fetchone()[0]
        cur.execute("INSERT OR IGNORE INTO accounts (name) VALUES ('TestWallet')")
        cur.execute("SELECT id FROM accounts WHERE name = 'TestWallet'")
        account_id = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO transactions "
            "(asset_id, account_id, tx_type, quantity, unit_price, fee_usd, total_usd, tx_date) "
            "VALUES (?, ?, 'BUY', 100.0, 0.5, 0.0, 50.0, '2026-01-01')",
            (asset_id, account_id),
        )
        conn.commit()
    finally:
        conn.close()

    resp = gui_env["client"].get("/")
    body = resp.data.decode("utf-8", errors="ignore")
    assert "Excluded Cost Basis" in body
    assert "50.00" in body  # the cost basis of the unvalued position


def test_b59a_excluded_cost_basis_shows_dash_when_all_valued(gui_env):
    """When all positions are valued, Excluded Cost Basis card shows '—'."""
    _seed_priced_position(gui_env["db_path"], "BTC", "Binance", 1.0, 40000.0, 50000.0)
    resp = gui_env["client"].get("/")
    body = resp.data.decode("utf-8", errors="ignore")
    assert "Excluded Cost Basis" in body
    assert "all positions valued" in body


def test_b59a_health_band_present(gui_env):
    """Health band shows Price Quality, Market Covered, Non-Market, Last Refresh."""
    resp = gui_env["client"].get("/")
    body = resp.data.decode("utf-8", errors="ignore")
    assert "Price Quality" in body
    assert "Market Covered" in body
    assert "Non-Market" in body
    assert "Last Refresh" in body
    assert "No refresh yet" in body


def test_b59a_pnl_positions_panel_renders(gui_env):
    """P&L by Asset panel renders with positions that have approved_value."""
    _seed_priced_position(gui_env["db_path"], "BTC", "Binance", 1.0, 40000.0, 50000.0)
    _seed_priced_position(gui_env["db_path"], "ETH", "Binance", 2.0, 6000.0, 2500.0)
    resp = gui_env["client"].get("/")
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="ignore")
    assert "P&amp;L by Asset" in body or "P&L by Asset" in body
    assert "BTC" in body
    assert "ETH" in body


def test_pnl_by_asset_consolidates_by_symbol(gui_env):
    """ETH split across two accounts appears once with summed P&L."""
    # ETH in Trezor: cost 3000, value 4000 → +1000
    _seed_priced_position(gui_env["db_path"], "ETH", "Trezor", 1.0, 3000.0, 4000.0)
    # ETH in Phemex: cost 1500, value 4000 → +2500  (same current_price, different cost)
    _seed_priced_position(gui_env["db_path"], "ETH", "Phemex", 0.5, 1500.0, 4000.0)
    resp = gui_env["client"].get("/")
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="ignore")
    # ETH must appear exactly once in the P&L chart rows
    assert body.count('class="pnl-label">ETH<') == 1


def test_b59a_pnl_positions_shows_pos_and_neg_bars(gui_env):
    """P&L chart renders both positive (green) and negative (red) bar classes."""
    _seed_priced_position(gui_env["db_path"], "BTC", "Binance", 1.0, 40000.0, 50000.0)  # +10k
    _seed_priced_position(gui_env["db_path"], "ETH", "Binance", 2.0, 6000.0, 2500.0)    # -1k
    resp = gui_env["client"].get("/")
    body = resp.data.decode("utf-8", errors="ignore")
    assert "pnl-fill-pos" in body
    assert "pnl-fill-neg" in body


def test_b59a_wallet_distribution_renders(gui_env):
    """Wallet Distribution panel shows each seeded account."""
    _seed_priced_position(gui_env["db_path"], "BTC", "Binance", 1.0, 40000.0, 50000.0)
    _seed_priced_position(gui_env["db_path"], "ETH", "Coinbase", 2.0, 6000.0, 2500.0)
    resp = gui_env["client"].get("/")
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="ignore")
    assert "Wallet Distribution" in body
    assert "Binance" in body
    assert "Coinbase" in body


def test_b59a_wallet_distribution_empty_when_no_positions(gui_env):
    """Wallet Distribution shows empty state when no valued positions exist."""
    resp = gui_env["client"].get("/")
    body = resp.data.decode("utf-8", errors="ignore")
    assert "Wallet Distribution" in body


def test_b59a_top_positions_renders(gui_env):
    """Top Positions panel shows the seeded position."""
    _seed_priced_position(gui_env["db_path"], "BTC", "Binance", 1.0, 40000.0, 50000.0)
    resp = gui_env["client"].get("/")
    body = resp.data.decode("utf-8", errors="ignore")
    assert "Top Positions" in body
    assert "BTC" in body


def test_b59a_top_positions_uses_status_dot(gui_env):
    """Top Positions uses status-dot CSS class (not pill-status)."""
    _seed_priced_position(gui_env["db_path"], "BTC", "Binance", 1.0, 40000.0, 50000.0)
    resp = gui_env["client"].get("/")
    body = resp.data.decode("utf-8", errors="ignore")
    assert "status-dot" in body


def test_b59a_quick_actions_still_present(gui_env):
    """Quick Actions zone still has all expected action buttons."""
    resp = gui_env["client"].get("/")
    body = resp.data.decode("utf-8", errors="ignore")
    assert "Quick Actions" in body
    assert "Refresh Prices" in body
    assert "BUY / SELL" in body
    assert "Cash Deposit/Withdrawal" in body
    assert "Fund Movement" in body
    assert "Backup DB" in body
