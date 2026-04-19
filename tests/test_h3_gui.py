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
