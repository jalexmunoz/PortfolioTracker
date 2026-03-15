import os
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from portfolio_tracker_v2.cli import main
from portfolio_tracker_v2.config import DB_PATH
from portfolio_tracker_v2.services.price_svc import RefreshReport


def run_cmd(runner, args, env=None):
    result = runner.invoke(main, args, env=env or {})
    if result.exit_code != 0:
        print(result.output)
        print(result.exception)
    assert result.exit_code == 0
    return result


def test_help():
    runner = CliRunner()
    result = runner.invoke(main, ["--help"])
    assert result.exit_code == 0
    assert "Portfolio Tracker" in result.output


def test_init_db_and_transactions(tmp_path, monkeypatch):
    # set DB path to temporary file
    db_file = tmp_path / "test.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    # init-db
    result = runner.invoke(main, ["init-db"], env=env)
    assert result.exit_code == 0
    assert db_file.exists()

    # buy some asset
    run_cmd(runner, ["buy", "--symbol", "BTC", "--account", "Main", "--qty", "1", "--price", "1000"], env)

    # verify positions output
    result = runner.invoke(main, ["positions"], env=env)
    assert "BTC" in result.output
    assert "1" in result.output
    assert "Val Method" in result.output
    assert "Val Status" in result.output

    # sell partial and check pnl
    run_cmd(runner, ["sell", "--symbol", "BTC", "--account", "Main", "--qty", "1", "--price", "2000"], env)
    result = runner.invoke(main, ["pnl"], env=env)
    assert "BTC" in result.output
    assert "1000" in result.output or "999" in result.output


def test_summary_and_positions_empty_db(tmp_path, monkeypatch):
    # set DB path to temporary file
    db_file = tmp_path / "empty.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    # init-db
    result = runner.invoke(main, ["init-db"], env=env)
    assert result.exit_code == 0

    # positions on empty DB
    result = runner.invoke(main, ["positions"], env=env)
    assert result.exit_code == 0
    assert "No open positions found" in result.output
    assert "import-csv" in result.output

    # summary on empty DB
    result = runner.invoke(main, ["summary"], env=env)
    assert result.exit_code == 0
    assert "No portfolio data found" in result.output
    assert "import-csv" in result.output


def test_refresh_prices(tmp_path, monkeypatch):
    # set DB path to temporary file
    db_file = tmp_path / "test.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    # init-db
    result = runner.invoke(main, ["init-db"], env=env)
    assert result.exit_code == 0

    with patch("portfolio_tracker_v2.cli.refresh_prices") as mock_refresh:
        mock_refresh.return_value = RefreshReport(
            updated=1,
            skipped_unsupported=2,
            skipped_unmapped=3,
            failed_final=4,
        )
        result = runner.invoke(main, ["refresh-prices"], env=env)

    assert result.exit_code == 0
    assert "Prices refreshed: 1 updated, 2 skipped unsupported, 3 skipped unmapped, 4 failed final" in result.output


def test_summary_with_valuation(tmp_path, monkeypatch):
    # set DB path to temporary file
    db_file = tmp_path / "test.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    # init-db
    result = runner.invoke(main, ["init-db"], env=env)
    assert result.exit_code == 0

    # buy BTC
    run_cmd(runner, ["buy", "--symbol", "BTC", "--account", "Main", "--qty", "1", "--price", "100"], env)

    # Manually set prices in DB for testing
    from portfolio_tracker_v2.core import Database
    from portfolio_tracker_v2.core.asset_resolver import AssetResolver
    from datetime import datetime

    db = Database(str(db_file))
    resolver = AssetResolver(db)
    conn = db.connect()
    cursor = conn.cursor()
    btc_asset = resolver.resolve("BTC")
    recent_date = datetime.now().isoformat()
    cursor.execute(
        "UPDATE assets SET current_price = ?, price_source = ?, price_updated_at = ? WHERE id = ?",
        (200.0, "coingecko", recent_date, btc_asset["id"]),
    )
    conn.commit()

    # summary
    result = runner.invoke(main, ["summary"], env=env)
    assert result.exit_code == 0
    assert "Total cost basis: 100.00" in result.output
    assert "Total Equity: 200.00" in result.output
    assert "Market-Covered Value: 200.00" in result.output
    assert "Non-Market Valued: 0.00" in result.output
    assert "Unvalued / Excluded (cost basis): 0.00" in result.output
    assert "Total unrealized PnL (approved valuations): 100.00" in result.output
    assert "Unrealized return % (approved valuations): 100.00%" in result.output
    assert "Market price quality: 1 usable" in result.output
