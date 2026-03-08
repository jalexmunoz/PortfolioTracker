import os
from decimal import Decimal
from pathlib import Path

import pytest
from click.testing import CliRunner

from portfolio_tracker_v2.cli import main
from portfolio_tracker_v2.config import DB_PATH


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
