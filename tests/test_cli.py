import json
import csv
import os
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from portfolio_tracker_v2.cli import main
from portfolio_tracker_v2.config import DB_PATH
from portfolio_tracker_v2.core import Database
from portfolio_tracker_v2.core.asset_resolver import AssetResolver
from portfolio_tracker_v2.services.price_svc import AssetRefreshResult, RefreshReport


def run_cmd(runner, args, env=None):
    result = runner.invoke(main, args, env=env or {})
    if result.exit_code != 0:
        print(result.output)
        print(result.exception)
    assert result.exit_code == 0
    return result


def sample_daily_report_payload():
    return {
        "report_type": "daily-report",
        "report_schema_version": 1,
        "run_timestamp": "2026-03-20T12:00:00+00:00",
        "summary_result": {
            "total_equity": 54321.99,
            "market_covered_value": 50000.00,
            "non_market_valued": 4321.99,
            "unvalued_excluded_cost_basis": 0.0,
        },
        "created_snapshot_path": "output/history/summary_2026-03-20T12-00-00Z.json",
        "previous_snapshot_path": "output/history/summary_2026-03-19T12-00-00Z.json",
        "alerts_result": {
            "status": "OK",
            "count": 0,
            "alerts": [],
        },
        "final_exit_code": 0,
    }


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
    assert "import-transactions-csv" in result.output
    assert "import-csv --input" not in result.output

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


def test_add_transaction_buy_persists_and_updates_positions(tmp_path, monkeypatch):
    db_file = tmp_path / "add_tx_buy.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    result = runner.invoke(
        main,
        [
            "add-transaction",
            "--date", "2026-03-20",
            "--account", "Main",
            "--symbol", "BTC",
            "--side", "buy",
            "--qty", "1",
            "--price", "100",
            "--fee", "5",
        ],
        env=env,
    )

    assert result.exit_code == 0
    assert "OK: BUY recorded id=" in result.output

    positions = runner.invoke(main, ["positions"], env=env)
    assert positions.exit_code == 0
    assert "BTC" in positions.output
    assert "Main" in positions.output
    assert "1" in positions.output


def test_add_transaction_sell_persists_and_reduces_open_position(tmp_path, monkeypatch):
    db_file = tmp_path / "add_tx_sell.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    assert runner.invoke(
        main,
        [
            "add-transaction",
            "--date", "2026-03-20",
            "--account", "Main",
            "--symbol", "BTC",
            "--side", "buy",
            "--qty", "2",
            "--price", "100",
        ],
        env=env,
    ).exit_code == 0

    result = runner.invoke(
        main,
        [
            "add-transaction",
            "--date", "2026-03-21",
            "--account", "Main",
            "--symbol", "BTC",
            "--side", "sell",
            "--qty", "1",
            "--price", "150",
        ],
        env=env,
    )

    assert result.exit_code == 0
    assert "OK: SELL recorded id=" in result.output

    positions = runner.invoke(main, ["positions"], env=env)
    assert positions.exit_code == 0
    assert "BTC" in positions.output
    assert "Main" in positions.output
    assert "1" in positions.output

    pnl = runner.invoke(main, ["pnl"], env=env)
    assert pnl.exit_code == 0
    assert "BTC" in pnl.output


def test_add_transaction_rejects_invalid_side(tmp_path, monkeypatch):
    db_file = tmp_path / "add_tx_invalid.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    result = runner.invoke(
        main,
        [
            "add-transaction",
            "--date", "2026-03-20",
            "--account", "Main",
            "--symbol", "BTC",
            "--side", "hold",
            "--qty", "1",
            "--price", "100",
        ],
        env=env,
    )

    assert result.exit_code != 0
    assert "Invalid value for '--side'" in result.output

def test_add_transaction_rejects_non_positive_qty(tmp_path, monkeypatch):
    db_file = tmp_path / "add_tx_qty_invalid.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    result = runner.invoke(
        main,
        [
            "add-transaction",
            "--date", "2026-03-20",
            "--account", "Main",
            "--symbol", "BTC",
            "--side", "buy",
            "--qty", "0",
            "--price", "100",
        ],
        env=env,
    )

    assert result.exit_code == 2
    assert "Quantity must be positive" in result.output


def test_add_transaction_rejects_non_positive_unit_price(tmp_path, monkeypatch):
    db_file = tmp_path / "add_tx_price_invalid.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    result = runner.invoke(
        main,
        [
            "add-transaction",
            "--date", "2026-03-20",
            "--account", "Main",
            "--symbol", "BTC",
            "--side", "buy",
            "--qty", "1",
            "--price", "0",
        ],
        env=env,
    )

    assert result.exit_code == 2
    assert "Unit price must be positive" in result.output


def test_import_transactions_csv_imports_valid_buy(tmp_path, monkeypatch):
    db_file = tmp_path / "import_buy.db"
    csv_path = tmp_path / "transactions.csv"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    csv_path.write_text(
        """trade_date,account,symbol,side,quantity,unit_price,fee,notes
2026-03-20,Main,BTC,BUY,1,100,5,first buy
""",
        encoding="utf-8",
    )

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    result = runner.invoke(main, ["import-transactions-csv", str(csv_path)], env=env)

    assert result.exit_code == 0
    assert f"File read: {csv_path}" in result.output
    assert "Rows processed: 1" in result.output
    assert "Imported OK: 1" in result.output
    assert "Rejected: 0" in result.output

    positions = runner.invoke(main, ["positions"], env=env)
    assert positions.exit_code == 0
    assert "BTC" in positions.output
    assert "Main" in positions.output
    assert "1" in positions.output


def test_import_transactions_csv_imports_buy_and_sell_through_normal_flow(tmp_path, monkeypatch):
    db_file = tmp_path / "import_buy_sell.db"
    csv_path = tmp_path / "transactions.csv"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    csv_path.write_text(
        """trade_date,account,symbol,side,quantity,unit_price
2026-03-20,Main,BTC,BUY,2,100
2026-03-21,Main,BTC,SELL,1,150
""",
        encoding="utf-8",
    )

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    result = runner.invoke(main, ["import-transactions-csv", str(csv_path)], env=env)

    assert result.exit_code == 0
    assert "Imported OK: 2" in result.output
    assert "Rejected: 0" in result.output

    listed = runner.invoke(main, ["list-transactions"], env=env)
    assert listed.exit_code == 0
    assert "BUY" in listed.output
    assert "SELL" in listed.output

    positions = runner.invoke(main, ["positions"], env=env)
    assert positions.exit_code == 0
    assert "BTC" in positions.output
    assert "1" in positions.output

    pnl = runner.invoke(main, ["pnl"], env=env)
    assert pnl.exit_code == 0
    assert "BTC" in pnl.output
    assert "50" in pnl.output


def test_import_transactions_csv_dry_run_valid_csv_shows_summary_and_keeps_db_unchanged(tmp_path, monkeypatch):
    db_file = tmp_path / "import_dry_run_valid.db"
    csv_path = tmp_path / "transactions.csv"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    csv_path.write_text(
        """trade_date,account,symbol,side,quantity,unit_price
2026-03-20,Main,BTC,BUY,2,100
2026-03-21,Main,BTC,SELL,1,150
""",
        encoding="utf-8",
    )

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    result = runner.invoke(main, ["import-transactions-csv", str(csv_path), "--dry-run"], env=env)

    assert result.exit_code == 0
    assert "Dry run: no transactions persisted" in result.output
    assert "Rows processed: 2" in result.output
    assert "Would import OK: 2" in result.output
    assert "Rejected: 0" in result.output

    db = Database(str(db_file))
    cursor = db.connect().cursor()
    cursor.execute("SELECT COUNT(1) FROM transactions")
    assert cursor.fetchone()[0] == 0


def test_import_transactions_csv_dry_run_invalid_csv_fails_and_persists_nothing(tmp_path, monkeypatch):
    db_file = tmp_path / "import_dry_run_invalid.db"
    csv_path = tmp_path / "transactions.csv"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    csv_path.write_text(
        """trade_date,symbol,side,quantity,unit_price
2026-03-20,BTC,BUY,1,100
""",
        encoding="utf-8",
    )

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    result = runner.invoke(main, ["import-transactions-csv", str(csv_path), "--dry-run"], env=env)

    assert result.exit_code == 2
    assert "ERROR: missing required columns: account" in result.output

    db = Database(str(db_file))
    cursor = db.connect().cursor()
    cursor.execute("SELECT COUNT(1) FROM transactions")
    assert cursor.fetchone()[0] == 0


def test_import_transactions_csv_missing_required_column_returns_exit_2(tmp_path, monkeypatch):
    db_file = tmp_path / "import_missing_column.db"
    csv_path = tmp_path / "transactions.csv"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    csv_path.write_text(
        """trade_date,symbol,side,quantity,unit_price
2026-03-20,BTC,BUY,1,100
""",
        encoding="utf-8",
    )

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    result = runner.invoke(main, ["import-transactions-csv", str(csv_path)], env=env)

    assert result.exit_code == 2
    assert "ERROR: missing required columns: account" in result.output


def test_import_transactions_csv_rejects_zero_unit_price_and_continues(tmp_path, monkeypatch):
    db_file = tmp_path / "import_zero_unit_price.db"
    csv_path = tmp_path / "transactions.csv"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    csv_path.write_text(
        """trade_date,account,symbol,side,quantity,unit_price
2026-03-20,Main,BTC,BUY,1,0
2026-03-21,Main,BTC,BUY,2,100
""",
        encoding="utf-8",
    )

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    result = runner.invoke(main, ["import-transactions-csv", str(csv_path)], env=env)

    assert result.exit_code == 0
    assert "Rows processed: 2" in result.output
    assert "Imported OK: 1" in result.output
    assert "Rejected: 1" in result.output
    assert "row 2: unit_price must be > 0" in result.output

    positions = runner.invoke(main, ["positions"], env=env)
    assert positions.exit_code == 0
    assert "BTC" in positions.output
    assert "2" in positions.output

def test_import_transactions_csv_skips_invalid_rows_and_summarizes(tmp_path, monkeypatch):
    db_file = tmp_path / "import_mixed_rows.db"
    csv_path = tmp_path / "transactions.csv"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    csv_path.write_text(
        """trade_date,account,symbol,side,quantity,unit_price
2026-03-20,Main,BTC,BUY,2,100
2026-03-21,Main,BTC,HOLD,1,150
2026-03-22,Main,BTC,SELL,abc,150
2026-03-23,Main,BTC,SELL,1,150
""",
        encoding="utf-8",
    )

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    result = runner.invoke(main, ["import-transactions-csv", str(csv_path)], env=env)

    assert result.exit_code == 0
    assert "Rows processed: 4" in result.output
    assert "Imported OK: 2" in result.output
    assert "Rejected: 2" in result.output
    assert "row 3: side must be BUY or SELL" in result.output
    assert "row 4: quantity must be numeric" in result.output

    positions = runner.invoke(main, ["positions"], env=env)
    assert positions.exit_code == 0
    assert "BTC" in positions.output
    assert "1" in positions.output

    pnl = runner.invoke(main, ["pnl"], env=env)
    assert pnl.exit_code == 0
    assert "BTC" in pnl.output
    assert "50" in pnl.output


def test_list_transactions_without_filters(tmp_path, monkeypatch):
    db_file = tmp_path / "list_tx.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["add-transaction", "--date", "2026-03-20", "--account", "Main", "--symbol", "BTC", "--side", "buy", "--qty", "1", "--price", "100"], env)
    run_cmd(runner, ["add-transaction", "--date", "2026-03-21", "--account", "Main", "--symbol", "ETH", "--side", "buy", "--qty", "2", "--price", "50"], env)

    result = runner.invoke(main, ["list-transactions"], env=env)

    assert result.exit_code == 0
    assert "ID" in result.output
    assert "Symbol" in result.output
    assert "BTC" in result.output
    assert "ETH" in result.output


def test_list_transactions_filters_by_account(tmp_path, monkeypatch):
    db_file = tmp_path / "list_tx_account.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["add-transaction", "--date", "2026-03-20", "--account", "Main", "--symbol", "BTC", "--side", "buy", "--qty", "1", "--price", "100"], env)
    run_cmd(runner, ["add-transaction", "--date", "2026-03-21", "--account", "Alt", "--symbol", "BTC", "--side", "buy", "--qty", "1", "--price", "110"], env)

    result = runner.invoke(main, ["list-transactions", "--account", "Main"], env=env)

    assert result.exit_code == 0
    assert "Main" in result.output
    assert "Alt" not in result.output


def test_list_transactions_filters_by_symbol(tmp_path, monkeypatch):
    db_file = tmp_path / "list_tx_symbol.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["add-transaction", "--date", "2026-03-20", "--account", "Main", "--symbol", "BTC", "--side", "buy", "--qty", "1", "--price", "100"], env)
    run_cmd(runner, ["add-transaction", "--date", "2026-03-21", "--account", "Main", "--symbol", "ETH", "--side", "buy", "--qty", "1", "--price", "80"], env)

    result = runner.invoke(main, ["list-transactions", "--symbol", "ETH"], env=env)

    assert result.exit_code == 0
    assert "ETH" in result.output
    assert "BTC" not in result.output


def test_list_transactions_filters_by_date_range(tmp_path, monkeypatch):
    db_file = tmp_path / "list_tx_dates.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["add-transaction", "--date", "2026-03-20", "--account", "Main", "--symbol", "BTC", "--side", "buy", "--qty", "1", "--price", "100"], env)
    run_cmd(runner, ["add-transaction", "--date", "2026-03-22", "--account", "Main", "--symbol", "ETH", "--side", "buy", "--qty", "1", "--price", "80"], env)

    result = runner.invoke(main, ["list-transactions", "--from-date", "2026-03-21", "--to-date", "2026-03-22"], env=env)

    assert result.exit_code == 0
    assert "ETH" in result.output
    assert "BTC" not in result.output


def test_list_transactions_applies_limit(tmp_path, monkeypatch):
    db_file = tmp_path / "list_tx_limit.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["add-transaction", "--date", "2026-03-20", "--account", "Main", "--symbol", "BTC", "--side", "buy", "--qty", "1", "--price", "100"], env)
    run_cmd(runner, ["add-transaction", "--date", "2026-03-21", "--account", "Main", "--symbol", "ETH", "--side", "buy", "--qty", "1", "--price", "80"], env)

    result = runner.invoke(main, ["list-transactions", "--limit", "1"], env=env)

    assert result.exit_code == 0
    assert "ETH" in result.output
    assert "BTC" not in result.output


def test_list_transactions_handles_no_results(tmp_path, monkeypatch):
    db_file = tmp_path / "list_tx_empty.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    result = runner.invoke(main, ["list-transactions", "--symbol", "BTC"], env=env)

    assert result.exit_code == 0
    assert "No transactions found for the given filters." in result.output


def test_list_transactions_rejects_invalid_inputs(tmp_path, monkeypatch):
    db_file = tmp_path / "list_tx_invalid.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0

    result_limit = runner.invoke(main, ["list-transactions", "--limit", "0"], env=env)
    assert result_limit.exit_code != 0
    assert "Invalid value for '--limit'" in result_limit.output

    result_date = runner.invoke(main, ["list-transactions", "--from-date", "2026/03/20"], env=env)
    assert result_date.exit_code != 0
    assert "Invalid value for '--date-from' / '--from-date'" in result_date.output


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


def test_refresh_prices_verbose_shows_final_outcome_per_symbol(tmp_path, monkeypatch):
    db_file = tmp_path / "test_verbose.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    result = runner.invoke(main, ["init-db"], env=env)
    assert result.exit_code == 0

    run_cmd(runner, ["buy", "--symbol", "BTC", "--account", "Main", "--qty", "1", "--price", "100"], env)

    from portfolio_tracker_v2.core import Database

    db = Database(str(db_file))
    conn = db.connect()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM assets WHERE symbol = 'BTC'")
    asset_id = cursor.fetchone()[0]
    cursor.execute(
        "UPDATE assets SET valuation_method = ?, price_source = ?, current_price = ?, price_updated_at = ? WHERE id = ?",
        ("market_live", "coingecko", 123.45, "2026-03-15 10:00:00", asset_id),
    )
    conn.commit()

    with patch("portfolio_tracker_v2.cli.refresh_prices") as mock_refresh:
        mock_refresh.return_value = RefreshReport(
            updated=1,
            skipped_unsupported=0,
            skipped_unmapped=0,
            failed_final=0,
            results=[
                AssetRefreshResult(
                    asset_id=asset_id,
                    symbol="BTC",
                    asset_type="crypto",
                    status="updated",
                    reason="price_updated",
                    provider="coingecko",
                    provider_symbol="bitcoin",
                )
            ],
        )
        result = runner.invoke(main, ["refresh-prices", "--verbose"], env=env)

    assert result.exit_code == 0
    assert "Symbol" in result.output
    assert "Provider" in result.output
    assert "Val Method" in result.output
    assert "Outcome" in result.output
    assert "Price Source" in result.output
    assert "Current Price" in result.output
    assert "Updated At" in result.output
    assert "BTC" in result.output
    assert "coingecko" in result.output
    assert "market_live" in result.output
    assert "updated" in result.output
    assert "price_updated" in result.output
    assert "123.45" in result.output
    assert "2026-03-15 10:00:00" in result.output
    assert "Prices refreshed: 1 updated, 0 skipped unsupported, 0 skipped unmapped, 0 failed final" in result.output


def test_summary_export_json_writes_snapshot_file(tmp_path, monkeypatch):
    db_file = tmp_path / "summary_export.db"
    export_path = tmp_path / "output" / "summary_snapshot.json"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    result = runner.invoke(main, ["init-db"], env=env)
    assert result.exit_code == 0

    run_cmd(runner, ["buy", "--symbol", "BTC", "--account", "Main", "--qty", "1", "--price", "100"], env)

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

    result = runner.invoke(main, ["summary", "--export-json", str(export_path)], env=env)
    assert result.exit_code == 0
    assert "Total Equity: 200.00" in result.output
    assert f"Summary exported to {export_path}" in result.output

    assert export_path.exists()
    payload = json.loads(export_path.read_text(encoding="utf-8"))

    assert "generated_at" in payload
    assert payload["total_cost_basis"] == 100.0
    assert payload["total_realized_pnl"] == 0.0
    assert payload["cash_balance"] == -100.0
    assert payload["total_equity"] == 200.0
    assert payload["market_covered_value"] == 200.0
    assert payload["non_market_valued"] == 0.0
    assert payload["unvalued_excluded_cost_basis"] == 0.0
    assert payload["total_unrealized_pnl_approved"] == 100.0
    assert payload["unrealized_return_pct_approved"] == 100.0
    assert payload["market_price_quality"]["usable"] == 1
    assert payload["asset_class_breakdown"]["Crypto"] == 200.0


def test_summary_export_json_history_writes_timestamped_snapshot(tmp_path, monkeypatch):
    db_file = tmp_path / "summary_export_history.db"
    history_dir = tmp_path / "history"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    result = runner.invoke(main, ["init-db"], env=env)
    assert result.exit_code == 0

    run_cmd(runner, ["buy", "--symbol", "BTC", "--account", "Main", "--qty", "1", "--price", "100"], env)

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

    result = runner.invoke(main, ["summary", "--export-json-history", str(history_dir)], env=env)
    assert result.exit_code == 0
    assert "Total Equity: 200.00" in result.output
    assert "Summary history snapshot exported to" in result.output

    files = list(history_dir.glob("summary_*.json"))
    assert len(files) == 1
    assert ":" not in files[0].name

    payload = json.loads(files[0].read_text(encoding="utf-8"))
    assert "generated_at" in payload
    assert payload["total_equity"] == 200.0
    assert payload["asset_class_breakdown"]["Crypto"] == 200.0


def test_validate_daily_report_json_valid_file(tmp_path, monkeypatch):
    report_path = tmp_path / "daily_report.json"
    report_path.write_text(json.dumps(sample_daily_report_payload(), indent=2), encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(main, ["validate-daily-report-json", str(report_path)])

    assert result.exit_code == 0
    assert "OK: valid daily-report JSON" in result.output


def test_validate_daily_report_json_missing_file(tmp_path, monkeypatch):
    report_path = tmp_path / "missing.json"

    runner = CliRunner()
    result = runner.invoke(main, ["validate-daily-report-json", str(report_path)])

    assert result.exit_code == 2
    assert "ERROR: file not found" in result.output


def test_validate_daily_report_json_invalid_json(tmp_path, monkeypatch):
    report_path = tmp_path / "invalid.json"
    report_path.write_text("{not-json}", encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(main, ["validate-daily-report-json", str(report_path)])

    assert result.exit_code == 2
    assert "ERROR:" in result.output


def test_validate_daily_report_json_wrong_report_type(tmp_path, monkeypatch):
    report_path = tmp_path / "wrong_type.json"
    payload = sample_daily_report_payload()
    payload["report_type"] = "summary"
    report_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(main, ["validate-daily-report-json", str(report_path)])

    assert result.exit_code == 2
    assert "report_type must be 'daily-report'" in result.output


def test_validate_daily_report_json_wrong_schema_version(tmp_path, monkeypatch):
    report_path = tmp_path / "wrong_version.json"
    payload = sample_daily_report_payload()
    payload["report_schema_version"] = 2
    report_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(main, ["validate-daily-report-json", str(report_path)])

    assert result.exit_code == 2
    assert "report_schema_version must be 1" in result.output


def test_validate_daily_report_json_missing_required_field(tmp_path, monkeypatch):
    report_path = tmp_path / "missing_field.json"
    payload = sample_daily_report_payload()
    del payload["summary_result"]
    report_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(main, ["validate-daily-report-json", str(report_path)])

    assert result.exit_code == 2
    assert "missing required top-level field: summary_result" in result.output


def test_show_latest_daily_report_shows_human_summary_for_valid_report(tmp_path, monkeypatch):
    report_path = tmp_path / "daily_report.json"
    report_path.write_text(json.dumps(sample_daily_report_payload(), indent=2), encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(main, ["show-latest-daily-report", "--path", str(report_path)])

    assert result.exit_code == 0
    assert "Latest Daily Report" in result.output
    assert "Run timestamp: 2026-03-20T12:00:00+00:00" in result.output
    assert "Final exit code: 0" in result.output
    assert "Alerts: OK (0)" in result.output
    assert "Created snapshot: output/history/summary_2026-03-20T12-00-00Z.json" in result.output
    assert "Previous snapshot: output/history/summary_2026-03-19T12-00-00Z.json" in result.output
    assert "Total Equity: 54,321.99" in result.output


def test_show_latest_daily_report_fails_for_missing_file(tmp_path, monkeypatch):
    report_path = tmp_path / "missing.json"

    runner = CliRunner()
    result = runner.invoke(main, ["show-latest-daily-report", "--path", str(report_path)])

    assert result.exit_code == 2
    assert "ERROR: file not found" in result.output


def test_show_latest_daily_report_fails_for_invalid_json(tmp_path, monkeypatch):
    report_path = tmp_path / "invalid.json"
    report_path.write_text("{not-json}", encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(main, ["show-latest-daily-report", "--path", str(report_path)])

    assert result.exit_code == 2
    assert "ERROR:" in result.output


def test_show_latest_daily_report_fails_for_invalid_report_contract(tmp_path, monkeypatch):
    report_path = tmp_path / "invalid_report.json"
    payload = sample_daily_report_payload()
    payload["report_schema_version"] = 2
    report_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(main, ["show-latest-daily-report", "--path", str(report_path)])

    assert result.exit_code == 2
    assert "report_schema_version must be 1" in result.output


def test_prune_summary_history_noop_when_snapshot_count_within_limit(tmp_path, monkeypatch):
    history_dir = tmp_path / "history"
    history_dir.mkdir(parents=True, exist_ok=True)
    (history_dir / "summary_2026-03-15T10-00-00Z.json").write_text("{}", encoding="utf-8")
    (history_dir / "summary_2026-03-16T10-00-00Z.json").write_text("{}", encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(main, ["prune-summary-history", "--history-dir", str(history_dir), "--keep-last", "2"])

    assert result.exit_code == 0
    assert "No summary history snapshots pruned" in result.output
    assert len(list(history_dir.glob("summary_*.json"))) == 2


def test_prune_summary_history_dry_run_does_not_delete_files(tmp_path, monkeypatch):
    history_dir = tmp_path / "history"
    history_dir.mkdir(parents=True, exist_ok=True)
    names = [
        "summary_2026-03-15T10-00-00Z.json",
        "summary_2026-03-16T10-00-00Z.json",
        "summary_2026-03-17T10-00-00Z.json",
    ]
    for name in names:
        (history_dir / name).write_text("{}", encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(main, ["prune-summary-history", "--history-dir", str(history_dir), "--keep-last", "1", "--dry-run"])

    assert result.exit_code == 0
    assert "Dry run: would delete 2 summary history snapshot(s):" in result.output
    for name in names:
        assert (history_dir / name).exists()


def test_prune_summary_history_deletes_oldest_and_keeps_latest(tmp_path, monkeypatch):
    history_dir = tmp_path / "history"
    history_dir.mkdir(parents=True, exist_ok=True)
    old_1 = history_dir / "summary_2026-03-15T10-00-00Z.json"
    old_2 = history_dir / "summary_2026-03-16T10-00-00Z.json"
    keep_1 = history_dir / "summary_2026-03-17T10-00-00Z.json"
    keep_2 = history_dir / "summary_2026-03-18T10-00-00Z.json"
    for path in [old_1, old_2, keep_1, keep_2]:
        path.write_text("{}", encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(main, ["prune-summary-history", "--history-dir", str(history_dir), "--keep-last", "2"])

    assert result.exit_code == 0
    assert not old_1.exists()
    assert not old_2.exists()
    assert keep_1.exists()
    assert keep_2.exists()
    assert "Deleting 2 summary history snapshot(s):" in result.output


def test_prune_summary_history_ignores_non_matching_files(tmp_path, monkeypatch):
    history_dir = tmp_path / "history"
    history_dir.mkdir(parents=True, exist_ok=True)
    valid_old = history_dir / "summary_2026-03-15T10-00-00Z.json"
    valid_new = history_dir / "summary_2026-03-16T10-00-00Z.json"
    invalid_name = history_dir / "summary_latest.json"
    other_json = history_dir / "notes.json"
    for path in [valid_old, valid_new, invalid_name, other_json]:
        path.write_text("{}", encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(main, ["prune-summary-history", "--history-dir", str(history_dir), "--keep-last", "1"])

    assert result.exit_code == 0
    assert not valid_old.exists()
    assert valid_new.exists()
    assert invalid_name.exists()
    assert other_json.exists()


def test_compare_summary_snapshots_shows_expected_deltas(tmp_path, monkeypatch):
    old_path = tmp_path / "old.json"
    new_path = tmp_path / "new.json"

    old_payload = {
        "generated_at": "2026-03-15T10:00:00Z",
        "total_cost_basis": 100.0,
        "total_realized_pnl": 0.0,
        "cash_balance": 0.0,
        "total_equity": 200.0,
        "market_covered_value": 180.0,
        "non_market_valued": 20.0,
        "unvalued_excluded_cost_basis": 0.0,
        "total_unrealized_pnl_approved": 100.0,
        "unrealized_return_pct_approved": 100.0,
        "market_price_quality": {"usable": 10, "stale": 1, "unavailable": 0},
        "asset_class_breakdown": {"Crypto": 120.0, "Equities": 60.0, "Non-market": 20.0},
    }
    new_payload = {
        "generated_at": "2026-03-16T10:00:00Z",
        "total_cost_basis": 100.0,
        "total_realized_pnl": 0.0,
        "cash_balance": 0.0,
        "total_equity": 230.0,
        "market_covered_value": 200.0,
        "non_market_valued": 30.0,
        "unvalued_excluded_cost_basis": 0.0,
        "total_unrealized_pnl_approved": 130.0,
        "unrealized_return_pct_approved": 130.0,
        "market_price_quality": {"usable": 11, "stale": 0, "unavailable": 1},
        "asset_class_breakdown": {"Crypto": 140.0, "Equities": 60.0, "Metals": 0.0, "Non-market": 30.0},
    }

    old_path.write_text(json.dumps(old_payload, indent=2), encoding="utf-8")
    new_path.write_text(json.dumps(new_payload, indent=2), encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(main, ["compare-summary-snapshots", str(old_path), str(new_path)])

    assert result.exit_code == 0
    assert "Summary Snapshot Comparison" in result.output
    assert "Old generated_at: 2026-03-15T10:00:00Z" in result.output
    assert "New generated_at: 2026-03-16T10:00:00Z" in result.output

    assert "Total Equity" in result.output
    assert "200.00" in result.output
    assert "230.00" in result.output
    assert "+30.00" in result.output

    assert "Market price quality" in result.output
    assert "usable" in result.output
    assert "+1" in result.output

    assert "Asset class breakdown" in result.output
    assert "Metals" in result.output
    assert "0.00" in result.output


def test_alert_summary_snapshots_ok_when_no_threshold_breaches(tmp_path, monkeypatch):
    old_path = tmp_path / "old_alert_ok.json"
    new_path = tmp_path / "new_alert_ok.json"

    old_payload = {
        "generated_at": "2026-03-15T10:00:00Z",
        "total_equity": 200.0,
        "unvalued_excluded_cost_basis": 0.0,
        "market_price_quality": {"usable": 10, "stale": 1, "unavailable": 0},
        "asset_class_breakdown": {"Crypto": 100.0, "Equities": 80.0, "Metals": 20.0, "Non-market": 0.0},
    }
    new_payload = {
        "generated_at": "2026-03-16T10:00:00Z",
        "total_equity": 198.5,
        "unvalued_excluded_cost_basis": 0.0,
        "market_price_quality": {"usable": 10, "stale": 1, "unavailable": 0},
        "asset_class_breakdown": {"Crypto": 99.25, "Equities": 79.4, "Metals": 19.85, "Non-market": 0.0},
    }

    old_path.write_text(json.dumps(old_payload, indent=2), encoding="utf-8")
    new_path.write_text(json.dumps(new_payload, indent=2), encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(main, ["alert-summary-snapshots", str(old_path), str(new_path)])

    assert result.exit_code == 0
    assert "Summary Snapshot Alerts" in result.output
    assert "OK: no alerts detected" in result.output


def test_alert_summary_snapshots_reports_alerts_for_deterioration(tmp_path, monkeypatch):
    old_path = tmp_path / "old_alert_bad.json"
    new_path = tmp_path / "new_alert_bad.json"

    old_payload = {
        "generated_at": "2026-03-15T10:00:00Z",
        "total_equity": 200.0,
        "unvalued_excluded_cost_basis": 0.0,
        "market_price_quality": {"usable": 10, "stale": 1, "unavailable": 0},
        "asset_class_breakdown": {"Crypto": 100.0, "Equities": 100.0},
    }
    new_payload = {
        "generated_at": "2026-03-16T10:00:00Z",
        "total_equity": 180.0,
        "unvalued_excluded_cost_basis": 15.0,
        "market_price_quality": {"usable": 8, "stale": 1, "unavailable": 2},
        "asset_class_breakdown": {"Crypto": 70.0, "Equities": 90.0, "Non-market": 20.0},
    }

    old_path.write_text(json.dumps(old_payload, indent=2), encoding="utf-8")
    new_path.write_text(json.dumps(new_payload, indent=2), encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "alert-summary-snapshots",
            str(old_path),
            str(new_path),
            "--equity-drop-pct",
            "3",
            "--asset-class-shift-pct",
            "5",
        ],
    )

    assert result.exit_code == 1
    assert "Summary Snapshot Alerts" in result.output
    assert "ALERT:" in result.output
    assert "total_equity dropped" in result.output
    assert "unvalued_excluded_cost_basis increased" in result.output
    assert "market_price_quality usable decreased" in result.output
    assert "market_price_quality unavailable increased" in result.output
    assert "asset_class_breakdown" in result.output


def test_daily_report_without_previous_snapshot(tmp_path, monkeypatch):
    db_file = tmp_path / "daily_no_prev.db"
    history_dir = tmp_path / "history"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["buy", "--symbol", "BTC", "--account", "Main", "--qty", "1", "--price", "100"], env)

    from portfolio_tracker_v2.core import Database
    from portfolio_tracker_v2.core.asset_resolver import AssetResolver
    from datetime import datetime

    db = Database(str(db_file))
    resolver = AssetResolver(db)
    conn = db.connect()
    cursor = conn.cursor()
    btc_asset = resolver.resolve("BTC")
    cursor.execute(
        "UPDATE assets SET current_price = ?, price_source = ?, price_updated_at = ? WHERE id = ?",
        (200.0, "coingecko", datetime.now().isoformat(), btc_asset["id"]),
    )
    conn.commit()

    with patch("portfolio_tracker_v2.cli.refresh_prices") as mock_refresh:
        mock_refresh.return_value = RefreshReport(updated=0, skipped_unsupported=0, skipped_unmapped=0, failed_final=0, results=[])
        result = runner.invoke(main, ["daily-report", "--history-dir", str(history_dir)], env=env)

    assert result.exit_code == 0
    assert "Daily Report" in result.output
    assert "Refresh" in result.output
    assert "Summary" in result.output
    assert "Timestamp:" in result.output
    assert "Total Equity: 200.00" in result.output
    assert "Cash balance: -100.00" in result.output
    assert "Total market value: 200.00" in result.output
    assert "Realized PnL: 0.00" in result.output
    assert "Unrealized PnL: 100.00" in result.output
    assert "Unrealized return %: 100.00%" in result.output
    assert "Price quality: 1 usable, 0 stale, 0 unavailable" in result.output
    assert "Top positions by market value" in result.output
    assert "Market Value" in result.output
    assert "BTC" in result.output
    assert "Summary history snapshot exported to" in result.output
    assert "No previous history snapshot found; skipping compare and alerts." in result.output
    assert len(list(history_dir.glob("summary_*.json"))) == 1


def test_daily_report_with_previous_snapshot_runs_compare_and_alerts(tmp_path, monkeypatch):
    db_file = tmp_path / "daily_prev.db"
    history_dir = tmp_path / "history"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["buy", "--symbol", "BTC", "--account", "Main", "--qty", "1", "--price", "100"], env)

    from portfolio_tracker_v2.core import Database
    from portfolio_tracker_v2.core.asset_resolver import AssetResolver
    from datetime import datetime

    db = Database(str(db_file))
    resolver = AssetResolver(db)
    conn = db.connect()
    cursor = conn.cursor()
    btc_asset = resolver.resolve("BTC")
    cursor.execute(
        "UPDATE assets SET current_price = ?, price_source = ?, price_updated_at = ? WHERE id = ?",
        (200.0, "coingecko", datetime.now().isoformat(), btc_asset["id"]),
    )
    conn.commit()

    history_dir.mkdir(parents=True, exist_ok=True)
    previous_payload = {
        "generated_at": "2026-03-15T10:00:00Z",
        "total_cost_basis": 100.0,
        "total_realized_pnl": 0.0,
        "cash_balance": 0.0,
        "total_equity": 190.0,
        "market_covered_value": 190.0,
        "non_market_valued": 0.0,
        "unvalued_excluded_cost_basis": 0.0,
        "total_unrealized_pnl_approved": 90.0,
        "unrealized_return_pct_approved": 90.0,
        "market_price_quality": {"usable": 1, "stale": 0, "unavailable": 0},
        "asset_class_breakdown": {"Crypto": 190.0, "Equities": 0.0, "Metals": 0.0, "Non-market": 0.0},
    }
    (history_dir / "summary_2026-03-15T10-00-00Z.json").write_text(json.dumps(previous_payload, indent=2), encoding="utf-8")

    with patch("portfolio_tracker_v2.cli.refresh_prices") as mock_refresh:
        mock_refresh.return_value = RefreshReport(updated=1, skipped_unsupported=0, skipped_unmapped=0, failed_final=0, results=[])
        result = runner.invoke(main, ["daily-report", "--history-dir", str(history_dir)], env=env)

    assert result.exit_code == 0
    assert "Compare vs previous snapshot" in result.output
    assert "Summary Snapshot Comparison" in result.output
    assert "Alerts" in result.output
    assert "Summary Snapshot Alerts" in result.output


def test_daily_report_skip_refresh(tmp_path, monkeypatch):
    db_file = tmp_path / "daily_skip.db"
    history_dir = tmp_path / "history"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["buy", "--symbol", "BTC", "--account", "Main", "--qty", "1", "--price", "100"], env)

    from portfolio_tracker_v2.core import Database
    from portfolio_tracker_v2.core.asset_resolver import AssetResolver
    from datetime import datetime

    db = Database(str(db_file))
    resolver = AssetResolver(db)
    conn = db.connect()
    cursor = conn.cursor()
    btc_asset = resolver.resolve("BTC")
    cursor.execute(
        "UPDATE assets SET current_price = ?, price_source = ?, price_updated_at = ? WHERE id = ?",
        (200.0, "coingecko", datetime.now().isoformat(), btc_asset["id"]),
    )
    conn.commit()

    with patch("portfolio_tracker_v2.cli.refresh_prices") as mock_refresh:
        result = runner.invoke(main, ["daily-report", "--skip-refresh", "--history-dir", str(history_dir)], env=env)

    assert result.exit_code == 0
    assert "Refresh skipped (--skip-refresh)" in result.output
    mock_refresh.assert_not_called()


def test_daily_report_shows_stale_and_unavailable_price_warnings(tmp_path, monkeypatch):
    db_file = tmp_path / "daily_price_quality.db"
    history_dir = tmp_path / "history"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["buy", "--symbol", "BTC", "--account", "Main", "--qty", "1", "--price", "100"], env)
    run_cmd(runner, ["buy", "--symbol", "ETH", "--account", "Main", "--qty", "2", "--price", "50"], env)

    from datetime import datetime, timedelta
    from portfolio_tracker_v2.core import Database
    from portfolio_tracker_v2.core.asset_resolver import AssetResolver

    db = Database(str(db_file))
    resolver = AssetResolver(db)
    conn = db.connect()
    cursor = conn.cursor()
    btc_asset = resolver.resolve("BTC")
    eth_asset = resolver.resolve("ETH")
    stale_date = (datetime.now() - timedelta(days=8)).isoformat()
    cursor.execute(
        "UPDATE assets SET current_price = ?, price_source = ?, price_updated_at = ? WHERE id = ?",
        (200.0, "coingecko", stale_date, btc_asset["id"]),
    )
    cursor.execute(
        "UPDATE assets SET current_price = NULL, price_source = NULL, price_updated_at = NULL WHERE id = ?",
        (eth_asset["id"],),
    )
    conn.commit()

    with patch("portfolio_tracker_v2.cli.refresh_prices") as mock_refresh:
        result = runner.invoke(main, ["daily-report", "--skip-refresh", "--history-dir", str(history_dir)], env=env)

    assert result.exit_code == 0
    assert "Refresh skipped (--skip-refresh)" in result.output
    assert "Price quality: 0 usable, 1 stale, 1 unavailable" in result.output
    assert "Top positions by market value" in result.output
    assert "BTC" in result.output
    assert "ETH" in result.output
    assert "Warnings" in result.output
    assert "1 position(s) have stale prices" in result.output
    assert "1 position(s) have unavailable prices" in result.output
    mock_refresh.assert_not_called()


def test_daily_report_refresh_verbose_shows_symbol_details(tmp_path, monkeypatch):
    db_file = tmp_path / "daily_verbose.db"
    history_dir = tmp_path / "history"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["buy", "--symbol", "BTC", "--account", "Main", "--qty", "1", "--price", "100"], env)

    from portfolio_tracker_v2.core import Database
    from portfolio_tracker_v2.core.asset_resolver import AssetResolver
    from datetime import datetime

    db = Database(str(db_file))
    resolver = AssetResolver(db)
    conn = db.connect()
    cursor = conn.cursor()
    btc_asset = resolver.resolve("BTC")
    cursor.execute(
        "UPDATE assets SET valuation_method = ?, current_price = ?, price_source = ?, price_updated_at = ? WHERE id = ?",
        ("market_live", 210.0, "coingecko", "2026-03-16 09:00:00", btc_asset["id"]),
    )
    conn.commit()

    with patch("portfolio_tracker_v2.cli.refresh_prices") as mock_refresh:
        mock_refresh.return_value = RefreshReport(
            updated=1,
            skipped_unsupported=0,
            skipped_unmapped=0,
            failed_final=0,
            results=[
                AssetRefreshResult(
                    asset_id=btc_asset["id"],
                    symbol="BTC",
                    asset_type="crypto",
                    status="updated",
                    reason="price_updated",
                    provider="coingecko",
                    provider_symbol="bitcoin",
                )
            ],
        )
        result = runner.invoke(main, ["daily-report", "--refresh-verbose", "--history-dir", str(history_dir)], env=env)

    assert result.exit_code == 0
    assert "Symbol" in result.output
    assert "Provider" in result.output
    assert "BTC" in result.output
    assert "price_updated" in result.output


def test_daily_report_shows_alerts_when_thresholds_breach(tmp_path, monkeypatch):
    db_file = tmp_path / "daily_alerts.db"
    history_dir = tmp_path / "history"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["buy", "--symbol", "BTC", "--account", "Main", "--qty", "1", "--price", "100"], env)

    from portfolio_tracker_v2.core import Database
    from portfolio_tracker_v2.core.asset_resolver import AssetResolver
    from datetime import datetime

    db = Database(str(db_file))
    resolver = AssetResolver(db)
    conn = db.connect()
    cursor = conn.cursor()
    btc_asset = resolver.resolve("BTC")
    cursor.execute(
        "UPDATE assets SET current_price = ?, price_source = ?, price_updated_at = ? WHERE id = ?",
        (180.0, "coingecko", datetime.now().isoformat(), btc_asset["id"]),
    )
    conn.commit()

    history_dir.mkdir(parents=True, exist_ok=True)
    previous_payload = {
        "generated_at": "2026-03-15T10:00:00Z",
        "total_cost_basis": 100.0,
        "total_realized_pnl": 0.0,
        "cash_balance": 0.0,
        "total_equity": 220.0,
        "market_covered_value": 220.0,
        "non_market_valued": 0.0,
        "unvalued_excluded_cost_basis": 0.0,
        "total_unrealized_pnl_approved": 120.0,
        "unrealized_return_pct_approved": 120.0,
        "market_price_quality": {"usable": 2, "stale": 0, "unavailable": 0},
        "asset_class_breakdown": {"Crypto": 220.0, "Equities": 0.0, "Metals": 0.0, "Non-market": 0.0},
    }
    (history_dir / "summary_2026-03-15T10-00-00Z.json").write_text(json.dumps(previous_payload, indent=2), encoding="utf-8")

    with patch("portfolio_tracker_v2.cli.refresh_prices") as mock_refresh:
        mock_refresh.return_value = RefreshReport(updated=1, skipped_unsupported=0, skipped_unmapped=0, failed_final=0, results=[])
        result = runner.invoke(main, ["daily-report", "--history-dir", str(history_dir)], env=env)

    assert result.exit_code == 1
    assert "ALERT:" in result.output
    assert "total_equity dropped" in result.output or "market_price_quality usable decreased" in result.output


def test_daily_report_returns_exit_2_on_operational_error(tmp_path, monkeypatch):
    db_file = tmp_path / "daily_error.db"
    history_dir = tmp_path / "history"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["buy", "--symbol", "BTC", "--account", "Main", "--qty", "1", "--price", "100"], env)

    from portfolio_tracker_v2.core import Database
    from portfolio_tracker_v2.core.asset_resolver import AssetResolver
    from datetime import datetime

    db = Database(str(db_file))
    resolver = AssetResolver(db)
    conn = db.connect()
    cursor = conn.cursor()
    btc_asset = resolver.resolve("BTC")
    cursor.execute(
        "UPDATE assets SET current_price = ?, price_source = ?, price_updated_at = ? WHERE id = ?",
        (200.0, "coingecko", datetime.now().isoformat(), btc_asset["id"]),
    )
    conn.commit()

    with patch("portfolio_tracker_v2.cli._write_summary_history_export", side_effect=OSError("disk full")):
        result = runner.invoke(main, ["daily-report", "--history-dir", str(history_dir)], env=env)

    assert result.exit_code == 2
    assert "ERROR: disk full" in result.output


def test_daily_report_output_json_without_previous_snapshot(tmp_path, monkeypatch):
    db_file = tmp_path / "daily_json_no_prev.db"
    history_dir = tmp_path / "history"
    output_json = tmp_path / "reports" / "daily.json"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["buy", "--symbol", "BTC", "--account", "Main", "--qty", "1", "--price", "100"], env)

    from portfolio_tracker_v2.core import Database
    from portfolio_tracker_v2.core.asset_resolver import AssetResolver
    from datetime import datetime

    db = Database(str(db_file))
    resolver = AssetResolver(db)
    conn = db.connect()
    cursor = conn.cursor()
    btc_asset = resolver.resolve("BTC")
    cursor.execute(
        "UPDATE assets SET current_price = ?, price_source = ?, price_updated_at = ? WHERE id = ?",
        (200.0, "coingecko", datetime.now().isoformat(), btc_asset["id"]),
    )
    conn.commit()

    with patch("portfolio_tracker_v2.cli.refresh_prices") as mock_refresh:
        mock_refresh.return_value = RefreshReport(updated=0, skipped_unsupported=0, skipped_unmapped=0, failed_final=0, results=[])
        result = runner.invoke(main, ["daily-report", "--history-dir", str(history_dir), "--output-json", str(output_json)], env=env)

    assert result.exit_code == 0
    assert "Structured daily report exported to" in result.output
    payload = json.loads(output_json.read_text(encoding="utf-8"))
    assert payload["report_type"] == "daily-report"
    assert payload["report_schema_version"] == 1
    assert "run_timestamp" in payload
    assert payload["previous_snapshot_path"] is None
    assert payload["compare_result"] is None
    assert payload["alerts_result"] is None
    assert payload["final_exit_code"] == 0
    assert payload["summary_result"]["total_equity"] == 200.0
    assert list(output_json.parent.glob(".tmp_*.json")) == []


def test_daily_report_output_json_history_dir_creates_timestamped_report(tmp_path, monkeypatch):
    db_file = tmp_path / "daily_json_history.db"
    history_dir = tmp_path / "history"
    output_json_history_dir = tmp_path / "reports" / "history"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["buy", "--symbol", "BTC", "--account", "Main", "--qty", "1", "--price", "100"], env)

    from portfolio_tracker_v2.core import Database
    from portfolio_tracker_v2.core.asset_resolver import AssetResolver
    from datetime import datetime

    db = Database(str(db_file))
    resolver = AssetResolver(db)
    conn = db.connect()
    cursor = conn.cursor()
    btc_asset = resolver.resolve("BTC")
    cursor.execute(
        "UPDATE assets SET current_price = ?, price_source = ?, price_updated_at = ? WHERE id = ?",
        (200.0, "coingecko", datetime.now().isoformat(), btc_asset["id"]),
    )
    conn.commit()

    with patch("portfolio_tracker_v2.cli.refresh_prices") as mock_refresh:
        mock_refresh.return_value = RefreshReport(updated=0, skipped_unsupported=0, skipped_unmapped=0, failed_final=0, results=[])
        result = runner.invoke(main, ["daily-report", "--history-dir", str(history_dir), "--output-json-history-dir", str(output_json_history_dir)], env=env)

    assert result.exit_code == 0
    assert output_json_history_dir.exists()
    files = list(output_json_history_dir.glob("daily_report_*.json"))
    assert len(files) == 1
    assert ":" not in files[0].name
    payload = json.loads(files[0].read_text(encoding="utf-8"))
    assert payload["report_type"] == "daily-report"
    assert payload["summary_result"]["total_equity"] == 200.0
    assert list(output_json_history_dir.glob(".tmp_*.json")) == []


def test_daily_report_output_json_history_dir_coexists_with_output_json(tmp_path, monkeypatch):
    db_file = tmp_path / "daily_json_history_with_output.db"
    history_dir = tmp_path / "history"
    output_json = tmp_path / "reports" / "daily.json"
    output_json_history_dir = tmp_path / "reports" / "history"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["buy", "--symbol", "BTC", "--account", "Main", "--qty", "1", "--price", "100"], env)

    from portfolio_tracker_v2.core import Database
    from portfolio_tracker_v2.core.asset_resolver import AssetResolver
    from datetime import datetime

    db = Database(str(db_file))
    resolver = AssetResolver(db)
    conn = db.connect()
    cursor = conn.cursor()
    btc_asset = resolver.resolve("BTC")
    cursor.execute(
        "UPDATE assets SET current_price = ?, price_source = ?, price_updated_at = ? WHERE id = ?",
        (200.0, "coingecko", datetime.now().isoformat(), btc_asset["id"]),
    )
    conn.commit()

    with patch("portfolio_tracker_v2.cli.refresh_prices") as mock_refresh:
        mock_refresh.return_value = RefreshReport(updated=0, skipped_unsupported=0, skipped_unmapped=0, failed_final=0, results=[])
        result = runner.invoke(main, ["daily-report", "--history-dir", str(history_dir), "--output-json", str(output_json), "--output-json-history-dir", str(output_json_history_dir)], env=env)

    assert result.exit_code == 0
    assert output_json.exists()
    assert len(list(output_json_history_dir.glob("daily_report_*.json"))) == 1


def test_daily_report_output_json_history_dir_coexists_with_stdout_json(tmp_path, monkeypatch):
    db_file = tmp_path / "daily_stdout_json_history.db"
    history_dir = tmp_path / "history"
    output_json_history_dir = tmp_path / "reports" / "history"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["buy", "--symbol", "BTC", "--account", "Main", "--qty", "1", "--price", "100"], env)

    from portfolio_tracker_v2.core import Database
    from portfolio_tracker_v2.core.asset_resolver import AssetResolver
    from datetime import datetime

    db = Database(str(db_file))
    resolver = AssetResolver(db)
    conn = db.connect()
    cursor = conn.cursor()
    btc_asset = resolver.resolve("BTC")
    cursor.execute(
        "UPDATE assets SET current_price = ?, price_source = ?, price_updated_at = ? WHERE id = ?",
        (200.0, "coingecko", datetime.now().isoformat(), btc_asset["id"]),
    )
    conn.commit()

    with patch("portfolio_tracker_v2.cli.refresh_prices") as mock_refresh:
        mock_refresh.return_value = RefreshReport(updated=0, skipped_unsupported=0, skipped_unmapped=0, failed_final=0, results=[])
        result = runner.invoke(main, ["daily-report", "--history-dir", str(history_dir), "--output-json", "-", "--output-json-history-dir", str(output_json_history_dir)], env=env)

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["report_type"] == "daily-report"
    assert len(list(output_json_history_dir.glob("daily_report_*.json"))) == 1
    assert "Structured daily report history snapshot exported to" not in result.output


def test_daily_report_output_json_with_previous_snapshot(tmp_path, monkeypatch):
    db_file = tmp_path / "daily_json_prev.db"
    history_dir = tmp_path / "history"
    output_json = tmp_path / "reports" / "daily_prev.json"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["buy", "--symbol", "BTC", "--account", "Main", "--qty", "1", "--price", "100"], env)

    from portfolio_tracker_v2.core import Database
    from portfolio_tracker_v2.core.asset_resolver import AssetResolver
    from datetime import datetime

    db = Database(str(db_file))
    resolver = AssetResolver(db)
    conn = db.connect()
    cursor = conn.cursor()
    btc_asset = resolver.resolve("BTC")
    cursor.execute(
        "UPDATE assets SET current_price = ?, price_source = ?, price_updated_at = ? WHERE id = ?",
        (200.0, "coingecko", datetime.now().isoformat(), btc_asset["id"]),
    )
    conn.commit()

    history_dir.mkdir(parents=True, exist_ok=True)
    previous_payload = {
        "generated_at": "2026-03-15T10:00:00Z",
        "total_cost_basis": 100.0,
        "total_realized_pnl": 0.0,
        "cash_balance": 0.0,
        "total_equity": 190.0,
        "market_covered_value": 190.0,
        "non_market_valued": 0.0,
        "unvalued_excluded_cost_basis": 0.0,
        "total_unrealized_pnl_approved": 90.0,
        "unrealized_return_pct_approved": 90.0,
        "market_price_quality": {"usable": 1, "stale": 0, "unavailable": 0},
        "asset_class_breakdown": {"Crypto": 190.0, "Equities": 0.0, "Metals": 0.0, "Non-market": 0.0},
    }
    (history_dir / "summary_2026-03-15T10-00-00Z.json").write_text(json.dumps(previous_payload, indent=2), encoding="utf-8")

    with patch("portfolio_tracker_v2.cli.refresh_prices") as mock_refresh:
        mock_refresh.return_value = RefreshReport(updated=1, skipped_unsupported=0, skipped_unmapped=0, failed_final=0, results=[])
        result = runner.invoke(main, ["daily-report", "--history-dir", str(history_dir), "--output-json", str(output_json)], env=env)

    assert result.exit_code == 0
    payload = json.loads(output_json.read_text(encoding="utf-8"))
    assert payload["previous_snapshot_path"] is not None
    assert payload["compare_result"] is not None
    assert payload["compare_result"]["metrics"]["total_equity"]["old"] == 190.0
    assert payload["compare_result"]["metrics"]["total_equity"]["new"] == 200.0
    assert payload["alerts_result"]["status"] == "OK"


def test_daily_report_output_json_reflects_alerts(tmp_path, monkeypatch):
    db_file = tmp_path / "daily_json_alerts.db"
    history_dir = tmp_path / "history"
    output_json = tmp_path / "reports" / "daily_alerts.json"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["buy", "--symbol", "BTC", "--account", "Main", "--qty", "1", "--price", "100"], env)

    from portfolio_tracker_v2.core import Database
    from portfolio_tracker_v2.core.asset_resolver import AssetResolver
    from datetime import datetime

    db = Database(str(db_file))
    resolver = AssetResolver(db)
    conn = db.connect()
    cursor = conn.cursor()
    btc_asset = resolver.resolve("BTC")
    cursor.execute(
        "UPDATE assets SET current_price = ?, price_source = ?, price_updated_at = ? WHERE id = ?",
        (180.0, "coingecko", datetime.now().isoformat(), btc_asset["id"]),
    )
    conn.commit()

    history_dir.mkdir(parents=True, exist_ok=True)
    previous_payload = {
        "generated_at": "2026-03-15T10:00:00Z",
        "total_cost_basis": 100.0,
        "total_realized_pnl": 0.0,
        "cash_balance": 0.0,
        "total_equity": 220.0,
        "market_covered_value": 220.0,
        "non_market_valued": 0.0,
        "unvalued_excluded_cost_basis": 0.0,
        "total_unrealized_pnl_approved": 120.0,
        "unrealized_return_pct_approved": 120.0,
        "market_price_quality": {"usable": 2, "stale": 0, "unavailable": 0},
        "asset_class_breakdown": {"Crypto": 220.0, "Equities": 0.0, "Metals": 0.0, "Non-market": 0.0},
    }
    (history_dir / "summary_2026-03-15T10-00-00Z.json").write_text(json.dumps(previous_payload, indent=2), encoding="utf-8")

    with patch("portfolio_tracker_v2.cli.refresh_prices") as mock_refresh:
        mock_refresh.return_value = RefreshReport(updated=1, skipped_unsupported=0, skipped_unmapped=0, failed_final=0, results=[])
        result = runner.invoke(main, ["daily-report", "--history-dir", str(history_dir), "--output-json", str(output_json)], env=env)

    assert result.exit_code == 1
    payload = json.loads(output_json.read_text(encoding="utf-8"))
    assert payload["alerts_result"]["status"] == "ALERT"
    assert payload["alerts_result"]["count"] > 0
    assert payload["report_type"] == "daily-report"
    assert payload["report_schema_version"] == 1
    assert payload["final_exit_code"] == 1


def test_daily_report_output_json_write_failure_returns_exit_2(tmp_path, monkeypatch):
    db_file = tmp_path / "daily_json_error.db"
    history_dir = tmp_path / "history"
    output_json = tmp_path / "reports" / "daily_error.json"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["buy", "--symbol", "BTC", "--account", "Main", "--qty", "1", "--price", "100"], env)

    from portfolio_tracker_v2.core import Database
    from portfolio_tracker_v2.core.asset_resolver import AssetResolver
    from datetime import datetime

    db = Database(str(db_file))
    resolver = AssetResolver(db)
    conn = db.connect()
    cursor = conn.cursor()
    btc_asset = resolver.resolve("BTC")
    cursor.execute(
        "UPDATE assets SET current_price = ?, price_source = ?, price_updated_at = ? WHERE id = ?",
        (200.0, "coingecko", datetime.now().isoformat(), btc_asset["id"]),
    )
    conn.commit()

    with patch("portfolio_tracker_v2.cli._write_summary_json_export", side_effect=OSError("cannot write report")):
        result = runner.invoke(main, ["daily-report", "--history-dir", str(history_dir), "--output-json", str(output_json)], env=env)

    assert result.exit_code == 2
    assert "ERROR: cannot write report" in result.output


def test_daily_report_output_json_stdout_emits_valid_json_only(tmp_path, monkeypatch):
    db_file = tmp_path / "daily_stdout_json.db"
    history_dir = tmp_path / "history"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["buy", "--symbol", "BTC", "--account", "Main", "--qty", "1", "--price", "100"], env)

    from portfolio_tracker_v2.core import Database
    from portfolio_tracker_v2.core.asset_resolver import AssetResolver
    from datetime import datetime

    db = Database(str(db_file))
    resolver = AssetResolver(db)
    conn = db.connect()
    cursor = conn.cursor()
    btc_asset = resolver.resolve("BTC")
    cursor.execute(
        "UPDATE assets SET current_price = ?, price_source = ?, price_updated_at = ? WHERE id = ?",
        (200.0, "coingecko", datetime.now().isoformat(), btc_asset["id"]),
    )
    conn.commit()

    with patch("portfolio_tracker_v2.cli.refresh_prices") as mock_refresh:
        mock_refresh.return_value = RefreshReport(updated=0, skipped_unsupported=0, skipped_unmapped=0, failed_final=0, results=[])
        result = runner.invoke(main, ["daily-report", "--history-dir", str(history_dir), "--output-json", "-"], env=env)

    assert result.exit_code == 0
    assert "Daily Report" not in result.output
    assert "Summary" not in result.output
    assert "Structured daily report exported to" not in result.output
    payload = json.loads(result.output)
    assert payload["final_exit_code"] == 0
    assert payload["summary_result"]["total_equity"] == 200.0


def test_daily_report_output_json_stdout_exit_code_1_with_alerts(tmp_path, monkeypatch):
    db_file = tmp_path / "daily_stdout_alerts.db"
    history_dir = tmp_path / "history"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["buy", "--symbol", "BTC", "--account", "Main", "--qty", "1", "--price", "100"], env)

    from portfolio_tracker_v2.core import Database
    from portfolio_tracker_v2.core.asset_resolver import AssetResolver
    from datetime import datetime

    db = Database(str(db_file))
    resolver = AssetResolver(db)
    conn = db.connect()
    cursor = conn.cursor()
    btc_asset = resolver.resolve("BTC")
    cursor.execute(
        "UPDATE assets SET current_price = ?, price_source = ?, price_updated_at = ? WHERE id = ?",
        (180.0, "coingecko", datetime.now().isoformat(), btc_asset["id"]),
    )
    conn.commit()

    history_dir.mkdir(parents=True, exist_ok=True)
    previous_payload = {
        "generated_at": "2026-03-15T10:00:00Z",
        "total_cost_basis": 100.0,
        "total_realized_pnl": 0.0,
        "cash_balance": 0.0,
        "total_equity": 220.0,
        "market_covered_value": 220.0,
        "non_market_valued": 0.0,
        "unvalued_excluded_cost_basis": 0.0,
        "total_unrealized_pnl_approved": 120.0,
        "unrealized_return_pct_approved": 120.0,
        "market_price_quality": {"usable": 2, "stale": 0, "unavailable": 0},
        "asset_class_breakdown": {"Crypto": 220.0, "Equities": 0.0, "Metals": 0.0, "Non-market": 0.0},
    }
    (history_dir / "summary_2026-03-15T10-00-00Z.json").write_text(json.dumps(previous_payload, indent=2), encoding="utf-8")

    with patch("portfolio_tracker_v2.cli.refresh_prices") as mock_refresh:
        mock_refresh.return_value = RefreshReport(updated=1, skipped_unsupported=0, skipped_unmapped=0, failed_final=0, results=[])
        result = runner.invoke(main, ["daily-report", "--history-dir", str(history_dir), "--output-json", "-"], env=env)

    assert result.exit_code == 1
    assert "Daily Report" not in result.output
    payload = json.loads(result.output)
    assert payload["alerts_result"]["status"] == "ALERT"
    assert payload["final_exit_code"] == 1


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
    assert "Asset class breakdown (approved equity):" in result.output
    assert "Crypto: 200.00 (100.00%)" in result.output
    assert "Equities: 0.00 (0.00%)" in result.output
    assert "Metals: 0.00 (0.00%)" in result.output
    assert "Non-market: 0.00 (0.00%)" in result.output









def test_delete_transaction_buy_unused_succeeds_and_updates_views(tmp_path, monkeypatch):
    db_file = tmp_path / "delete_buy_unused.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(
        runner,
        [
            "add-transaction",
            "--date", "2026-03-20",
            "--account", "Main",
            "--symbol", "BTC",
            "--side", "buy",
            "--qty", "1",
            "--price", "100",
        ],
        env,
    )

    db = Database(str(db_file))
    cursor = db.connect().cursor()
    cursor.execute("SELECT id FROM transactions WHERE tx_type = 'BUY' ORDER BY id ASC LIMIT 1")
    buy_id = cursor.fetchone()[0]

    result = runner.invoke(main, ["delete-transaction", str(buy_id)], env=env)
    assert result.exit_code == 0
    assert "OK: deleted transaction" in result.output

    listed = runner.invoke(main, ["list-transactions"], env=env)
    assert listed.exit_code == 0
    assert "No transactions found for the given filters." in listed.output

    positions = runner.invoke(main, ["positions"], env=env)
    assert positions.exit_code == 0
    assert "No open positions found" in positions.output

    pnl = runner.invoke(main, ["pnl"], env=env)
    assert pnl.exit_code == 0
    assert "BTC" not in pnl.output


def test_delete_transaction_sell_succeeds_cleans_matches_and_reverts_pnl(tmp_path, monkeypatch):
    db_file = tmp_path / "delete_sell.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(
        runner,
        [
            "add-transaction",
            "--date", "2026-03-20",
            "--account", "Main",
            "--symbol", "BTC",
            "--side", "buy",
            "--qty", "2",
            "--price", "100",
        ],
        env,
    )
    run_cmd(
        runner,
        [
            "add-transaction",
            "--date", "2026-03-21",
            "--account", "Main",
            "--symbol", "BTC",
            "--side", "sell",
            "--qty", "1",
            "--price", "150",
        ],
        env,
    )

    db = Database(str(db_file))
    cursor = db.connect().cursor()
    cursor.execute("SELECT id FROM transactions WHERE tx_type = 'SELL' ORDER BY id ASC LIMIT 1")
    sell_id = cursor.fetchone()[0]

    before_pnl = runner.invoke(main, ["pnl"], env=env)
    assert before_pnl.exit_code == 0
    assert "BTC" in before_pnl.output
    assert "50" in before_pnl.output

    result = runner.invoke(main, ["delete-transaction", str(sell_id)], env=env)
    assert result.exit_code == 0
    assert "OK: deleted transaction" in result.output

    cursor.execute("SELECT COUNT(1) FROM lot_matches WHERE sell_tx_id = ?", (sell_id,))
    assert cursor.fetchone()[0] == 0

    listed = runner.invoke(main, ["list-transactions"], env=env)
    assert listed.exit_code == 0
    assert "SELL" not in listed.output
    assert "BUY" in listed.output

    positions = runner.invoke(main, ["positions"], env=env)
    assert positions.exit_code == 0
    assert "BTC" in positions.output
    assert "2" in positions.output

    after_pnl = runner.invoke(main, ["pnl"], env=env)
    assert after_pnl.exit_code == 0
    assert "BTC" in after_pnl.output
    assert "0" in after_pnl.output


def test_delete_transaction_buy_used_by_sell_rejected_with_exit_2(tmp_path, monkeypatch):
    db_file = tmp_path / "delete_buy_used.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["add-transaction", "--date", "2026-03-20", "--account", "Main", "--symbol", "BTC", "--side", "buy", "--qty", "2", "--price", "100"], env)
    run_cmd(runner, ["add-transaction", "--date", "2026-03-21", "--account", "Main", "--symbol", "BTC", "--side", "sell", "--qty", "1", "--price", "150"], env)

    db = Database(str(db_file))
    cursor = db.connect().cursor()
    cursor.execute("SELECT id FROM transactions WHERE tx_type = 'BUY' ORDER BY id ASC LIMIT 1")
    buy_id = cursor.fetchone()[0]

    result = runner.invoke(main, ["delete-transaction", str(buy_id)], env=env)
    assert result.exit_code == 2
    assert "ERROR:" in result.output
    assert "already matched" in result.output

    cursor.execute("SELECT COUNT(1) FROM transactions WHERE id = ?", (buy_id,))
    assert cursor.fetchone()[0] == 1


def test_delete_transaction_nonexistent_id_returns_exit_2(tmp_path, monkeypatch):
    db_file = tmp_path / "delete_missing.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0

    result = runner.invoke(main, ["delete-transaction", "9999"], env=env)
    assert result.exit_code == 2
    assert "ERROR:" in result.output
    assert "does not exist" in result.output



def test_import_legacy_positions_csv_dry_run_valid_csv_shows_summary_and_keeps_db_unchanged(tmp_path, monkeypatch):
    db_file = tmp_path / "legacy_seed_dry_run.db"
    csv_path = tmp_path / "portfoliototal.csv"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    csv_path.write_text(
        """Symbol,Quantity,Total Cost (USD),Avg Cost (USD),Wallet
BTC,1,100,100,Main
ETH,2,400,200,Main
""",
        encoding="utf-8",
    )

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    result = runner.invoke(
        main,
        ["import-legacy-positions-csv", str(csv_path), "--seed-date", "2026-03-20", "--dry-run"],
        env=env,
    )

    assert result.exit_code == 0
    assert f"File read: {csv_path}" in result.output
    assert "Seed date: 2026-03-20" in result.output
    assert "Dry run: no seed positions persisted" in result.output
    assert "Rows processed: 2" in result.output
    assert "Would seed OK: 2" in result.output
    assert "Rejected: 0" in result.output

    db = Database(str(db_file))
    cursor = db.connect().cursor()
    cursor.execute("SELECT COUNT(1) FROM transactions")
    assert cursor.fetchone()[0] == 0


def test_import_legacy_positions_csv_imports_rows_and_preserves_fifo_order(tmp_path, monkeypatch):
    db_file = tmp_path / "legacy_seed_fifo.db"
    csv_path = tmp_path / "portfoliototal.csv"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    csv_path.write_text(
        """Symbol,Quantity,Total Cost (USD),Avg Cost (USD),Wallet
BTC,1,100,100,Main
BTC,2,300,150,Main
""",
        encoding="utf-8",
    )

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    result = runner.invoke(
        main,
        ["import-legacy-positions-csv", str(csv_path), "--seed-date", "2026-03-20"],
        env=env,
    )

    assert result.exit_code == 0
    assert "Seeded OK: 2" in result.output
    assert "Rejected: 0" in result.output

    sell = runner.invoke(
        main,
        ["sell", "--symbol", "BTC", "--account", "Main", "--qty", "1.5", "--price", "200", "--date", "2026-03-21"],
        env=env,
    )
    assert sell.exit_code == 0

    db = Database(str(db_file))
    cursor = db.connect().cursor()
    cursor.execute(
        """
        SELECT t.unit_price, lm.quantity
        FROM lot_matches lm
        JOIN transactions t ON t.id = lm.buy_tx_id
        ORDER BY lm.id ASC
        """
    )
    matches = cursor.fetchall()

    assert len(matches) == 2
    assert Decimal(str(matches[0][0])) == Decimal("100")
    assert Decimal(str(matches[0][1])) == Decimal("1")
    assert Decimal(str(matches[1][0])) == Decimal("150")
    assert Decimal(str(matches[1][1])) == Decimal("0.5")


def test_import_legacy_positions_csv_rejects_inconsistent_row_and_persists_nothing(tmp_path, monkeypatch):
    db_file = tmp_path / "legacy_seed_invalid.db"
    csv_path = tmp_path / "portfoliototal.csv"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    csv_path.write_text(
        """Symbol,Quantity,Total Cost (USD),Avg Cost (USD),Wallet
BTC,2,100,70,Main
""",
        encoding="utf-8",
    )

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    result = runner.invoke(
        main,
        ["import-legacy-positions-csv", str(csv_path), "--seed-date", "2026-03-20"],
        env=env,
    )

    assert result.exit_code == 2
    assert "Rejected: 1" in result.output
    assert "Quantity * Avg Cost (USD) does not match Total Cost (USD)" in result.output

    db = Database(str(db_file))
    cursor = db.connect().cursor()
    cursor.execute("SELECT COUNT(1) FROM transactions")
    assert cursor.fetchone()[0] == 0


def test_import_legacy_positions_csv_positions_reflect_seeded_rows(tmp_path, monkeypatch):
    db_file = tmp_path / "legacy_seed_positions.db"
    csv_path = tmp_path / "portfoliototal.csv"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    csv_path.write_text(
        """Symbol,Quantity,Total Cost (USD),Avg Cost (USD),Wallet
BTC,1,100,100,Main
ETH,2,400,200,Trezor
""",
        encoding="utf-8",
    )

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    assert runner.invoke(
        main,
        ["import-legacy-positions-csv", str(csv_path), "--seed-date", "2026-03-20"],
        env=env,
    ).exit_code == 0

    positions = runner.invoke(main, ["positions"], env=env)

    assert positions.exit_code == 0
    assert "BTC" in positions.output
    assert "ETH" in positions.output
    assert "Main" in positions.output
    assert "Trezor" in positions.output


def test_import_legacy_positions_csv_summary_supports_special_assets(tmp_path, monkeypatch):
    from datetime import date as real_date

    db_file = tmp_path / "legacy_seed_summary_specials.db"
    csv_path = tmp_path / "portfoliototal.csv"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    csv_path.write_text(
        """Symbol,Quantity,Total Cost (USD),Avg Cost (USD),Wallet
BBVA CDT,1,4644,7897.48,BBVA
Fondo Dinamico,1,263.25,263.25,Trii
GOLD,2,7554,3777,SB
SILVER,4,234,58.5,SB
BTC,1,100,100,Main
""",
        encoding="utf-8",
    )

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    assert runner.invoke(
        main,
        ["import-legacy-positions-csv", str(csv_path), "--seed-date", "2026-03-20"],
        env=env,
    ).exit_code == 0

    db = Database(str(db_file))
    conn = db.connect()
    cursor = conn.cursor()
    resolver = AssetResolver(db)
    gold = resolver.resolve("GOLD")
    silver = resolver.resolve("SILVER")
    btc = resolver.resolve("BTC")
    cursor.execute(
        "UPDATE assets SET current_price = ?, price_source = ?, price_updated_at = ? WHERE id = ?",
        (3900.0, "tradingview", real_date.today().isoformat(), gold["id"]),
    )
    cursor.execute(
        "UPDATE assets SET current_price = ?, price_source = ?, price_updated_at = ? WHERE id = ?",
        (65.0, "tradingview", real_date.today().isoformat(), silver["id"]),
    )
    cursor.execute(
        "UPDATE assets SET current_price = ?, price_source = ?, price_updated_at = ? WHERE id = ?",
        (120.0, "coingecko", real_date.today().isoformat(), btc["id"]),
    )
    conn.commit()
    db.close()

    with patch("portfolio_tracker_v2.services.pnl_svc.date") as mock_date:
        mock_date.today.return_value = real_date(2026, 3, 22)
        summary = runner.invoke(main, ["summary"], env=env)

    assert summary.exit_code == 0
    assert "Non-Market Valued" in summary.output
    assert "Metals" in summary.output
    assert "Total Equity: 13,180.89" in summary.output
    assert "Non-Market Valued: 5,000.89" in summary.output


def test_import_legacy_positions_csv_daily_report_supports_special_assets(tmp_path, monkeypatch):
    from datetime import date as real_date

    db_file = tmp_path / "legacy_seed_daily_specials.db"
    csv_path = tmp_path / "portfoliototal.csv"
    history_dir = tmp_path / "history"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    csv_path.write_text(
        """Symbol,Quantity,Total Cost (USD),Avg Cost (USD),Wallet
BBVA CDT,1,4644,7897.48,BBVA
Fondo Dinamico,1,263.25,263.25,Trii
GOLD,2,7554,3777,SB
SILVER,4,234,58.5,SB
BTC,1,100,100,Main
""",
        encoding="utf-8",
    )

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    assert runner.invoke(
        main,
        ["import-legacy-positions-csv", str(csv_path), "--seed-date", "2026-03-20"],
        env=env,
    ).exit_code == 0

    db = Database(str(db_file))
    conn = db.connect()
    cursor = conn.cursor()
    resolver = AssetResolver(db)
    gold = resolver.resolve("GOLD")
    silver = resolver.resolve("SILVER")
    btc = resolver.resolve("BTC")
    cursor.execute(
        "UPDATE assets SET current_price = ?, price_source = ?, price_updated_at = ? WHERE id = ?",
        (3900.0, "tradingview", real_date.today().isoformat(), gold["id"]),
    )
    cursor.execute(
        "UPDATE assets SET current_price = ?, price_source = ?, price_updated_at = ? WHERE id = ?",
        (65.0, "tradingview", real_date.today().isoformat(), silver["id"]),
    )
    cursor.execute(
        "UPDATE assets SET current_price = ?, price_source = ?, price_updated_at = ? WHERE id = ?",
        (120.0, "coingecko", real_date.today().isoformat(), btc["id"]),
    )
    conn.commit()
    db.close()

    with patch("portfolio_tracker_v2.services.pnl_svc.date") as mock_date:
        mock_date.today.return_value = real_date(2026, 3, 22)
        daily = runner.invoke(main, ["daily-report", "--skip-refresh", "--history-dir", str(history_dir)], env=env)

    assert daily.exit_code == 0
    assert "Refresh skipped (--skip-refresh)" in daily.output
    assert "BBVA CDT" in daily.output
    assert "FONDO DINAMICO" in daily.output
    assert "GOLD" in daily.output
    assert "SILVER" in daily.output


def test_list_transactions_shows_realized_fields_for_sell_and_na_for_buy(tmp_path, monkeypatch):
    db_file = tmp_path / "list_tx_realized.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["add-transaction", "--date", "2026-03-01", "--account", "Test", "--symbol", "BTC", "--side", "buy", "--qty", "1", "--price", "100"], env)
    run_cmd(runner, ["add-transaction", "--date", "2026-03-02", "--account", "Test", "--symbol", "BTC", "--side", "buy", "--qty", "2", "--price", "110"], env)
    run_cmd(runner, ["add-transaction", "--date", "2026-03-03", "--account", "Test", "--symbol", "BTC", "--side", "sell", "--qty", "2.5", "--price", "120"], env)

    listed = runner.invoke(main, ["list-transactions", "--account", "Test", "--symbol", "BTC"], env=env)
    assert listed.exit_code == 0
    assert "Proceeds" in listed.output
    assert "Cost Basis" in listed.output
    assert "Realized PnL" in listed.output
    assert "300.00" in listed.output
    assert "265.00" in listed.output
    assert "35.00" in listed.output
    assert "N/A" in listed.output

    summary = runner.invoke(main, ["summary", "--account", "Test"], env=env)
    assert summary.exit_code == 0
    assert "Total realized PnL: 35.00" in summary.output


def test_list_transactions_sell_realized_disappears_after_delete(tmp_path, monkeypatch):
    db_file = tmp_path / "list_tx_realized_delete.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["add-transaction", "--date", "2026-03-01", "--account", "Test", "--symbol", "BTC", "--side", "buy", "--qty", "1", "--price", "100"], env)
    run_cmd(runner, ["add-transaction", "--date", "2026-03-02", "--account", "Test", "--symbol", "BTC", "--side", "sell", "--qty", "0.5", "--price", "120"], env)

    db = Database(str(db_file))
    cursor = db.connect().cursor()
    cursor.execute("SELECT id FROM transactions WHERE tx_type = 'SELL' ORDER BY id ASC LIMIT 1")
    sell_id = cursor.fetchone()[0]

    before = runner.invoke(main, ["list-transactions", "--account", "Test", "--symbol", "BTC"], env=env)
    assert before.exit_code == 0
    assert "Realized PnL" in before.output
    assert "10.00" in before.output

    deleted = runner.invoke(main, ["delete-transaction", str(sell_id)], env=env)
    assert deleted.exit_code == 0

    after = runner.invoke(main, ["list-transactions", "--account", "Test", "--symbol", "BTC"], env=env)
    assert after.exit_code == 0
    assert "SELL" not in after.output
    assert "10.00" not in after.output

def test_list_open_lots_cli_shows_remaining_fifo_lot(tmp_path, monkeypatch):
    db_file = tmp_path / "open_lots_cli.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["add-transaction", "--date", "2026-03-01", "--account", "Test", "--symbol", "BTC", "--side", "buy", "--qty", "1", "--price", "100"], env)
    run_cmd(runner, ["add-transaction", "--date", "2026-03-02", "--account", "Test", "--symbol", "BTC", "--side", "buy", "--qty", "2", "--price", "110"], env)
    run_cmd(runner, ["add-transaction", "--date", "2026-03-03", "--account", "Test", "--symbol", "BTC", "--side", "sell", "--qty", "2.5", "--price", "120"], env)

    result = runner.invoke(main, ["list-open-lots", "--account", "Test", "--symbol", "BTC"], env=env)

    assert result.exit_code == 0
    assert "BuyTxID" in result.output
    assert "Remaining Qty" in result.output
    assert "0.5" in result.output
    assert "55.00" in result.output


def test_list_open_lots_cli_filters_by_account_and_symbol(tmp_path, monkeypatch):
    db_file = tmp_path / "open_lots_cli_filters.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["buy", "--date", "2026-03-01", "--account", "Main", "--symbol", "BTC", "--qty", "1", "--price", "100"], env)
    run_cmd(runner, ["buy", "--date", "2026-03-02", "--account", "Vault", "--symbol", "BTC", "--qty", "1", "--price", "200"], env)
    run_cmd(runner, ["buy", "--date", "2026-03-03", "--account", "Main", "--symbol", "ETH", "--qty", "1", "--price", "50"], env)

    by_account = runner.invoke(main, ["list-open-lots", "--account", "Main"], env=env)
    assert by_account.exit_code == 0
    assert "Main" in by_account.output
    assert "Vault" not in by_account.output

    by_symbol = runner.invoke(main, ["list-open-lots", "--symbol", "ETH"], env=env)
    assert by_symbol.exit_code == 0
    assert "ETH" in by_symbol.output
    assert "BTC" not in by_symbol.output


def test_list_open_lots_cli_reflects_delete_sell_restoration(tmp_path, monkeypatch):
    db_file = tmp_path / "open_lots_cli_delete_sell.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["buy", "--date", "2026-03-01", "--account", "Test", "--symbol", "BTC", "--qty", "2", "--price", "100"], env)
    run_cmd(runner, ["sell", "--date", "2026-03-02", "--account", "Test", "--symbol", "BTC", "--qty", "1.5", "--price", "120"], env)

    before = runner.invoke(main, ["list-open-lots", "--account", "Test", "--symbol", "BTC"], env=env)
    assert before.exit_code == 0
    assert "0.5" in before.output

    db = Database(str(db_file))
    cursor = db.connect().cursor()
    cursor.execute("SELECT id FROM transactions WHERE tx_type = 'SELL' ORDER BY id ASC LIMIT 1")
    sell_id = cursor.fetchone()[0]

    deleted = runner.invoke(main, ["delete-transaction", str(sell_id)], env=env)
    assert deleted.exit_code == 0

    after = runner.invoke(main, ["list-open-lots", "--account", "Test", "--symbol", "BTC"], env=env)
    assert after.exit_code == 0
    assert "2" in after.output
    assert "0.5" not in after.output

def test_list_transactions_filters_by_side(tmp_path, monkeypatch):
    db_file = tmp_path / "list_tx_side.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["add-transaction", "--date", "2026-03-20", "--account", "Trezor", "--symbol", "BTC", "--side", "buy", "--qty", "1", "--price", "100"], env)
    run_cmd(runner, ["add-transaction", "--date", "2026-03-21", "--account", "Trezor", "--symbol", "BTC", "--side", "sell", "--qty", "0.5", "--price", "120"], env)

    result = runner.invoke(main, ["list-transactions", "--side", "SELL"], env=env)

    assert result.exit_code == 0
    assert "SELL" in result.output
    assert "BUY" not in result.output


def test_list_transactions_filters_combined(tmp_path, monkeypatch):
    db_file = tmp_path / "list_tx_combined.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["add-transaction", "--date", "2026-03-20", "--account", "Trezor", "--symbol", "BTC", "--side", "buy", "--qty", "1", "--price", "100"], env)
    run_cmd(runner, ["add-transaction", "--date", "2026-03-20", "--account", "Phemex", "--symbol", "ETH", "--side", "buy", "--qty", "1", "--price", "80"], env)
    run_cmd(runner, ["add-transaction", "--date", "2026-03-20", "--account", "Bingx", "--symbol", "BTC", "--side", "buy", "--qty", "1", "--price", "90"], env)

    result = runner.invoke(main, ["list-transactions", "--symbol", "BTC", "--account", "Trezor", "--side", "BUY"], env=env)

    assert result.exit_code == 0
    assert "Trezor" in result.output
    assert "BTC" in result.output
    assert "Phemex" not in result.output
    assert "Bingx" not in result.output


def test_list_transactions_filter_by_tx_id(tmp_path, monkeypatch):
    db_file = tmp_path / "list_tx_id.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["add-transaction", "--date", "2026-03-20", "--account", "Main", "--symbol", "BTC", "--side", "buy", "--qty", "1", "--price", "100"], env)
    run_cmd(runner, ["add-transaction", "--date", "2026-03-21", "--account", "Main", "--symbol", "ETH", "--side", "buy", "--qty", "1", "--price", "80"], env)

    db = Database(str(db_file))
    cursor = db.connect().cursor()
    cursor.execute("SELECT id FROM transactions ORDER BY id ASC LIMIT 1")
    tx_id = cursor.fetchone()[0]

    result = runner.invoke(main, ["list-transactions", "--tx-id", str(tx_id)], env=env)

    assert result.exit_code == 0
    assert f"{tx_id}" in result.output
    assert "ETH" not in result.output


def test_list_transactions_accepts_date_aliases(tmp_path, monkeypatch):
    db_file = tmp_path / "list_tx_date_alias.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["add-transaction", "--date", "2026-03-20", "--account", "Main", "--symbol", "BTC", "--side", "buy", "--qty", "1", "--price", "100"], env)
    run_cmd(runner, ["add-transaction", "--date", "2026-03-22", "--account", "Main", "--symbol", "ETH", "--side", "buy", "--qty", "1", "--price", "80"], env)

    result = runner.invoke(main, ["list-transactions", "--date-from", "2026-03-21", "--date-to", "2026-03-22"], env=env)

    assert result.exit_code == 0
    assert "ETH" in result.output
    assert "BTC" not in result.output


def test_list_transactions_rejects_invalid_side(tmp_path, monkeypatch):
    db_file = tmp_path / "list_tx_invalid_side.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0

    result = runner.invoke(main, ["list-transactions", "--side", "HOLD"], env=env)
    assert result.exit_code != 0
    assert "Invalid value for '--side'" in result.output


def test_list_transactions_rejects_empty_symbol_account_and_invalid_date_order(tmp_path, monkeypatch):
    db_file = tmp_path / "list_tx_invalid_values.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0

    result_symbol = runner.invoke(main, ["list-transactions", "--symbol", "   "], env=env)
    assert result_symbol.exit_code != 0
    assert "symbol cannot be empty" in result_symbol.output

    result_account = runner.invoke(main, ["list-transactions", "--account", "   "], env=env)
    assert result_account.exit_code != 0
    assert "account cannot be empty" in result_account.output

    result_order = runner.invoke(main, ["list-transactions", "--date-from", "2026-03-22", "--date-to", "2026-03-21"], env=env)
    assert result_order.exit_code != 0
    assert "date-from cannot be after date-to" in result_order.output


def test_list_transactions_output_csv_writes_expected_file(tmp_path, monkeypatch):
    db_file = tmp_path / "list_tx_export.db"
    out_csv = tmp_path / "transactions.csv"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["add-transaction", "--date", "2026-03-01", "--account", "Test", "--symbol", "BTC", "--side", "buy", "--qty", "1", "--price", "100"], env)
    run_cmd(runner, ["add-transaction", "--date", "2026-03-02", "--account", "Test", "--symbol", "BTC", "--side", "sell", "--qty", "0.5", "--price", "120"], env)

    result = runner.invoke(main, ["list-transactions", "--output-csv", str(out_csv)], env=env)
    assert result.exit_code == 0
    assert "CSV exported to" in result.output
    assert out_csv.exists()

    with out_csv.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert len(rows) == 2
    assert rows[0]["id"]
    assert rows[0]["tx_date"]
    assert rows[0]["symbol"] == "BTC"
    assert rows[0]["account"] == "Test"
    assert rows[0]["side"] in {"BUY", "SELL"}

    buy_row = next(r for r in rows if r["side"] == "BUY")
    sell_row = next(r for r in rows if r["side"] == "SELL")
    assert buy_row["gross_proceeds"] == ""
    assert buy_row["matched_cost_basis"] == ""
    assert buy_row["realized_pnl"] == ""
    assert sell_row["realized_pnl"] == "10.00"


def test_list_transactions_output_csv_respects_filters(tmp_path, monkeypatch):
    db_file = tmp_path / "list_tx_export_filter.db"
    out_csv = tmp_path / "transactions_btc.csv"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["add-transaction", "--date", "2026-03-01", "--account", "Trezor", "--symbol", "BTC", "--side", "buy", "--qty", "1", "--price", "100"], env)
    run_cmd(runner, ["add-transaction", "--date", "2026-03-02", "--account", "Phemex", "--symbol", "ETH", "--side", "buy", "--qty", "1", "--price", "80"], env)

    result = runner.invoke(main, ["list-transactions", "--symbol", "BTC", "--output-csv", str(out_csv)], env=env)
    assert result.exit_code == 0

    with out_csv.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert len(rows) == 1
    assert rows[0]["symbol"] == "BTC"
    assert rows[0]["account"] == "Trezor"


def test_list_open_lots_output_csv_writes_expected_file(tmp_path, monkeypatch):
    db_file = tmp_path / "open_lots_export.db"
    out_csv = tmp_path / "open_lots.csv"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["add-transaction", "--date", "2026-03-01", "--account", "Test", "--symbol", "BTC", "--side", "buy", "--qty", "1", "--price", "100"], env)
    run_cmd(runner, ["add-transaction", "--date", "2026-03-02", "--account", "Test", "--symbol", "BTC", "--side", "buy", "--qty", "2", "--price", "110"], env)
    run_cmd(runner, ["add-transaction", "--date", "2026-03-03", "--account", "Test", "--symbol", "BTC", "--side", "sell", "--qty", "2.5", "--price", "120"], env)

    result = runner.invoke(main, ["list-open-lots", "--output-csv", str(out_csv)], env=env)
    assert result.exit_code == 0
    assert out_csv.exists()

    with out_csv.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert len(rows) == 1
    assert rows[0]["symbol"] == "BTC"
    assert rows[0]["remaining_qty"] == "0.5"
    assert rows[0]["remaining_cost_basis"] == "55.00"


def test_list_open_lots_output_csv_respects_filters(tmp_path, monkeypatch):
    db_file = tmp_path / "open_lots_export_filter.db"
    out_csv = tmp_path / "open_lots_trezor.csv"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["buy", "--date", "2026-03-01", "--account", "Trezor", "--symbol", "BTC", "--qty", "1", "--price", "100"], env)
    run_cmd(runner, ["buy", "--date", "2026-03-01", "--account", "Phemex", "--symbol", "ETH", "--qty", "1", "--price", "80"], env)

    result = runner.invoke(main, ["list-open-lots", "--account", "Trezor", "--output-csv", str(out_csv)], env=env)
    assert result.exit_code == 0

    with out_csv.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert len(rows) == 1
    assert rows[0]["account"] == "Trezor"
    assert rows[0]["symbol"] == "BTC"


def test_list_transactions_output_csv_supports_stdout_dash(tmp_path, monkeypatch):
    db_file = tmp_path / "list_tx_stdout.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["add-transaction", "--date", "2026-03-01", "--account", "Test", "--symbol", "BTC", "--side", "buy", "--qty", "1", "--price", "100"], env)

    result = runner.invoke(main, ["list-transactions", "--output-csv", "-"], env=env)
    assert result.exit_code == 0
    assert "id,tx_date,symbol,side,qty,unit_price,account,gross_proceeds,matched_cost_basis,realized_pnl" in result.output
    assert "BUY" in result.output


def test_list_open_lots_output_csv_rejects_empty_path(tmp_path, monkeypatch):
    db_file = tmp_path / "open_lots_export_empty.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    result = runner.invoke(main, ["list-open-lots", "--output-csv", "   "], env=env)

    assert result.exit_code != 0
    assert "output-csv cannot be empty" in result.output

def test_inspect_lot_matches_cli_sell_tx_id_valid(tmp_path, monkeypatch):
    db_file = tmp_path / "inspect_lot_matches_sell.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["add-transaction", "--date", "2026-03-01", "--account", "Test", "--symbol", "BTC", "--side", "buy", "--qty", "1", "--price", "100"], env)
    run_cmd(runner, ["add-transaction", "--date", "2026-03-02", "--account", "Test", "--symbol", "BTC", "--side", "sell", "--qty", "1", "--price", "120"], env)

    db = Database(str(db_file))
    cursor = db.connect().cursor()
    cursor.execute("SELECT id FROM transactions WHERE tx_type = 'SELL' ORDER BY id ASC LIMIT 1")
    sell_id = cursor.fetchone()[0]

    result = runner.invoke(main, ["inspect-lot-matches", "--sell-tx-id", str(sell_id)], env=env)

    assert result.exit_code == 0
    assert "SellTxID" in result.output
    assert "BuyTxID" in result.output
    assert "Realized PnL" in result.output
    assert "20.00" in result.output


def test_inspect_lot_matches_cli_tx_not_found_returns_exit_2(tmp_path, monkeypatch):
    db_file = tmp_path / "inspect_lot_matches_missing.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    result = runner.invoke(main, ["inspect-lot-matches", "--sell-tx-id", "9999"], env=env)

    assert result.exit_code == 2
    assert "ERROR:" in result.output
    assert "does not exist" in result.output


def test_inspect_lot_matches_cli_requires_exactly_one_argument(tmp_path, monkeypatch):
    db_file = tmp_path / "inspect_lot_matches_args.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0

    none_result = runner.invoke(main, ["inspect-lot-matches"], env=env)
    assert none_result.exit_code == 2
    assert "exactly one" in none_result.output

    both_result = runner.invoke(main, ["inspect-lot-matches", "--sell-tx-id", "1", "--buy-tx-id", "1"], env=env)
    assert both_result.exit_code == 2
    assert "exactly one" in both_result.output


def test_inspect_lot_matches_cli_no_matches_message(tmp_path, monkeypatch):
    db_file = tmp_path / "inspect_lot_matches_none.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["add-transaction", "--date", "2026-03-01", "--account", "Test", "--symbol", "BTC", "--side", "buy", "--qty", "1", "--price", "100"], env)

    db = Database(str(db_file))
    cursor = db.connect().cursor()
    cursor.execute("SELECT id FROM transactions WHERE tx_type = 'BUY' ORDER BY id ASC LIMIT 1")
    buy_id = cursor.fetchone()[0]

    result = runner.invoke(main, ["inspect-lot-matches", "--buy-tx-id", str(buy_id)], env=env)

    assert result.exit_code == 0
    assert f"No lot matches found for buy_tx_id={buy_id}." in result.output


def test_inspect_lot_matches_cli_buy_tx_id_valid(tmp_path, monkeypatch):
    db_file = tmp_path / "inspect_lot_matches_buy.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["add-transaction", "--date", "2026-03-01", "--account", "Test", "--symbol", "BTC", "--side", "buy", "--qty", "2", "--price", "100"], env)
    run_cmd(runner, ["add-transaction", "--date", "2026-03-02", "--account", "Test", "--symbol", "BTC", "--side", "sell", "--qty", "1", "--price", "120"], env)

    db = Database(str(db_file))
    cursor = db.connect().cursor()
    cursor.execute("SELECT id FROM transactions WHERE tx_type = 'BUY' ORDER BY id ASC LIMIT 1")
    buy_id = cursor.fetchone()[0]

    result = runner.invoke(main, ["inspect-lot-matches", "--buy-tx-id", str(buy_id)], env=env)

    assert result.exit_code == 0
    assert "SellTxID" in result.output
    assert "BuyTxID" in result.output
    assert "1" in result.output


def test_inspect_lot_matches_csv_export_by_sell_tx_id(tmp_path, monkeypatch):
    db_file = tmp_path / "inspect_lot_matches_sell_csv.db"
    out_csv = tmp_path / "lot_matches_sell.csv"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["add-transaction", "--date", "2026-03-01", "--account", "Test", "--symbol", "BTC", "--side", "buy", "--qty", "1", "--price", "100"], env)
    run_cmd(runner, ["add-transaction", "--date", "2026-03-02", "--account", "Test", "--symbol", "BTC", "--side", "sell", "--qty", "1", "--price", "120"], env)

    db = Database(str(db_file))
    cursor = db.connect().cursor()
    cursor.execute("SELECT id FROM transactions WHERE tx_type = 'SELL' ORDER BY id ASC LIMIT 1")
    sell_id = cursor.fetchone()[0]

    result = runner.invoke(main, ["inspect-lot-matches", "--sell-tx-id", str(sell_id), "--output-csv", str(out_csv)], env=env)

    assert result.exit_code == 0
    assert out_csv.exists()

    with out_csv.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert len(rows) == 1
    assert rows[0]["sell_tx_id"] == str(sell_id)
    assert rows[0]["symbol"] == "BTC"
    assert rows[0]["matched_realized_pnl"] == "20.00"


def test_inspect_lot_matches_csv_export_by_buy_tx_id(tmp_path, monkeypatch):
    db_file = tmp_path / "inspect_lot_matches_buy_csv.db"
    out_csv = tmp_path / "lot_matches_buy.csv"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["add-transaction", "--date", "2026-03-01", "--account", "Test", "--symbol", "BTC", "--side", "buy", "--qty", "2", "--price", "100"], env)
    run_cmd(runner, ["add-transaction", "--date", "2026-03-02", "--account", "Test", "--symbol", "BTC", "--side", "sell", "--qty", "1", "--price", "120"], env)

    db = Database(str(db_file))
    cursor = db.connect().cursor()
    cursor.execute("SELECT id FROM transactions WHERE tx_type = 'BUY' ORDER BY id ASC LIMIT 1")
    buy_id = cursor.fetchone()[0]

    result = runner.invoke(main, ["inspect-lot-matches", "--buy-tx-id", str(buy_id), "--output-csv", str(out_csv)], env=env)

    assert result.exit_code == 0
    assert out_csv.exists()

    with out_csv.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert len(rows) == 1
    assert rows[0]["buy_tx_id"] == str(buy_id)
    assert rows[0]["matched_qty"] == "1"


def test_inspect_lot_matches_csv_export_supports_stdout_dash(tmp_path, monkeypatch):
    db_file = tmp_path / "inspect_lot_matches_stdout_csv.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["add-transaction", "--date", "2026-03-01", "--account", "Test", "--symbol", "BTC", "--side", "buy", "--qty", "1", "--price", "100"], env)
    run_cmd(runner, ["add-transaction", "--date", "2026-03-02", "--account", "Test", "--symbol", "BTC", "--side", "sell", "--qty", "1", "--price", "120"], env)

    db = Database(str(db_file))
    cursor = db.connect().cursor()
    cursor.execute("SELECT id FROM transactions WHERE tx_type = 'SELL' ORDER BY id ASC LIMIT 1")
    sell_id = cursor.fetchone()[0]

    result = runner.invoke(main, ["inspect-lot-matches", "--sell-tx-id", str(sell_id), "--output-csv", "-"], env=env)

    assert result.exit_code == 0
    assert "sell_tx_id,buy_tx_id,buy_tx_date,symbol,account,matched_qty,buy_unit_price,matched_cost_basis,sell_unit_price,matched_proceeds,matched_realized_pnl" in result.output


def test_inspect_lot_matches_csv_export_rejects_empty_path(tmp_path, monkeypatch):
    db_file = tmp_path / "inspect_lot_matches_empty_csv.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    result = runner.invoke(main, ["inspect-lot-matches", "--sell-tx-id", "1", "--output-csv", "   "], env=env)

    assert result.exit_code != 0
    assert "output-csv cannot be empty" in result.output


def test_inspect_lot_matches_csv_export_no_matches_writes_header_only(tmp_path, monkeypatch):
    db_file = tmp_path / "inspect_lot_matches_no_rows_csv.db"
    out_csv = tmp_path / "lot_matches_empty.csv"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["add-transaction", "--date", "2026-03-01", "--account", "Test", "--symbol", "BTC", "--side", "buy", "--qty", "1", "--price", "100"], env)

    db = Database(str(db_file))
    cursor = db.connect().cursor()
    cursor.execute("SELECT id FROM transactions WHERE tx_type = 'BUY' ORDER BY id ASC LIMIT 1")
    buy_id = cursor.fetchone()[0]

    result = runner.invoke(main, ["inspect-lot-matches", "--buy-tx-id", str(buy_id), "--output-csv", str(out_csv)], env=env)

    assert result.exit_code == 0
    with out_csv.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.reader(handle))

    assert len(rows) == 1
    assert rows[0][0] == "sell_tx_id"


def test_positions_output_csv_writes_expected_file(tmp_path, monkeypatch):
    db_file = tmp_path / "positions_export.db"
    out_csv = tmp_path / "positions.csv"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["buy", "--symbol", "BTC", "--account", "Main", "--qty", "1", "--price", "100"], env)

    result = runner.invoke(main, ["positions", "--output-csv", str(out_csv)], env=env)

    assert result.exit_code == 0
    assert out_csv.exists()

    with out_csv.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert len(rows) == 1
    assert rows[0]["symbol"] == "BTC"
    assert rows[0]["account"] == "Main"
    assert rows[0]["qty"] == "1"


def test_positions_output_csv_supports_stdout_dash(tmp_path, monkeypatch):
    db_file = tmp_path / "positions_export_stdout.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["buy", "--symbol", "BTC", "--account", "Main", "--qty", "1", "--price", "100"], env)

    result = runner.invoke(main, ["positions", "--output-csv", "-"], env=env)

    assert result.exit_code == 0
    assert "symbol,account,qty,avg_cost,cost_basis,valuation_method,valuation_status,alert" in result.output
    assert "BTC,Main,1,100.00,100.00" in result.output


def test_positions_output_csv_rejects_empty_path(tmp_path, monkeypatch):
    db_file = tmp_path / "positions_export_empty.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    result = runner.invoke(main, ["positions", "--output-csv", "   "], env=env)

    assert result.exit_code != 0
    assert "output-csv cannot be empty" in result.output


def test_positions_output_csv_no_rows_writes_header_only(tmp_path, monkeypatch):
    db_file = tmp_path / "positions_export_empty_rows.db"
    out_csv = tmp_path / "positions_empty.csv"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    result = runner.invoke(main, ["positions", "--output-csv", str(out_csv)], env=env)

    assert result.exit_code == 0
    with out_csv.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.reader(handle))

    assert len(rows) == 1
    assert rows[0] == ["symbol", "account", "qty", "avg_cost", "cost_basis", "valuation_method", "valuation_status", "alert"]


def test_summary_output_json_writes_structured_file(tmp_path, monkeypatch):
    from datetime import datetime

    db_file = tmp_path / "summary_output_json.db"
    output_path = tmp_path / "output" / "summary_structured.json"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["buy", "--symbol", "BTC", "--account", "Main", "--qty", "1", "--price", "100"], env)

    db = Database(str(db_file))
    resolver = AssetResolver(db)
    conn = db.connect()
    cursor = conn.cursor()
    btc_asset = resolver.resolve("BTC")
    cursor.execute(
        "UPDATE assets SET current_price = ?, price_source = ?, price_updated_at = ? WHERE id = ?",
        (200.0, "coingecko", datetime.now().isoformat(), btc_asset["id"]),
    )
    conn.commit()

    result = runner.invoke(main, ["summary", "--output-json", str(output_path)], env=env)
    assert result.exit_code == 0
    assert f"Summary JSON exported to {output_path}" in result.output
    assert output_path.exists()

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["total_cost_basis"] == 100.0
    assert payload["total_equity"] == 200.0
    assert payload["market_price_quality"]["usable"] == 1
    assert isinstance(payload["asset_class_breakdown"], list)
    assert any(r["asset_class"] == "Crypto" for r in payload["asset_class_breakdown"])


def test_summary_output_json_stdout_dash(tmp_path, monkeypatch):
    from datetime import datetime

    db_file = tmp_path / "summary_output_json_stdout.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["buy", "--symbol", "BTC", "--account", "Main", "--qty", "1", "--price", "100"], env)

    db = Database(str(db_file))
    resolver = AssetResolver(db)
    conn = db.connect()
    cursor = conn.cursor()
    btc_asset = resolver.resolve("BTC")
    cursor.execute(
        "UPDATE assets SET current_price = ?, price_source = ?, price_updated_at = ? WHERE id = ?",
        (200.0, "coingecko", datetime.now().isoformat(), btc_asset["id"]),
    )
    conn.commit()

    result = runner.invoke(main, ["summary", "--output-json", "-"], env=env)
    assert result.exit_code == 0

    payload = json.loads(result.output)
    assert payload["total_cost_basis"] == 100.0
    assert "market_price_quality" in payload
    assert "asset_class_breakdown" in payload


def test_summary_output_json_rejects_empty_path(tmp_path, monkeypatch):
    db_file = tmp_path / "summary_output_json_empty.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    result = runner.invoke(main, ["summary", "--output-json", "   "], env=env)

    assert result.exit_code != 0
    assert "output-json cannot be empty" in result.output


def test_summary_output_json_respects_account_filter(tmp_path, monkeypatch):
    from datetime import datetime

    db_file = tmp_path / "summary_output_json_account.db"
    output_path = tmp_path / "summary_main.json"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["buy", "--symbol", "BTC", "--account", "Main", "--qty", "1", "--price", "100"], env)
    run_cmd(runner, ["buy", "--symbol", "ETH", "--account", "Alt", "--qty", "1", "--price", "50"], env)

    db = Database(str(db_file))
    resolver = AssetResolver(db)
    conn = db.connect()
    cursor = conn.cursor()
    btc_asset = resolver.resolve("BTC")
    eth_asset = resolver.resolve("ETH")
    now_iso = datetime.now().isoformat()
    cursor.execute(
        "UPDATE assets SET current_price = ?, price_source = ?, price_updated_at = ? WHERE id = ?",
        (200.0, "coingecko", now_iso, btc_asset["id"]),
    )
    cursor.execute(
        "UPDATE assets SET current_price = ?, price_source = ?, price_updated_at = ? WHERE id = ?",
        (60.0, "coingecko", now_iso, eth_asset["id"]),
    )
    conn.commit()

    result = runner.invoke(main, ["summary", "--account", "Main", "--output-json", str(output_path)], env=env)
    assert result.exit_code == 0

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["total_cost_basis"] == 100.0
    assert payload["total_equity"] == 200.0


def test_daily_report_output_json_contains_b43_minimum_structure(tmp_path, monkeypatch):
    db_file = tmp_path / "daily_report_b43_structure.db"
    history_dir = tmp_path / "history"
    output_json = tmp_path / "daily_report_structured.json"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["buy", "--symbol", "BTC", "--account", "Main", "--qty", "1", "--price", "100"], env)

    from portfolio_tracker_v2.core import Database
    from portfolio_tracker_v2.core.asset_resolver import AssetResolver
    from datetime import datetime

    db = Database(str(db_file))
    resolver = AssetResolver(db)
    conn = db.connect()
    cursor = conn.cursor()
    btc_asset = resolver.resolve("BTC")
    cursor.execute(
        "UPDATE assets SET current_price = ?, price_source = ?, price_updated_at = ? WHERE id = ?",
        (200.0, "coingecko", datetime.now().isoformat(), btc_asset["id"]),
    )
    conn.commit()

    result = runner.invoke(main, ["daily-report", "--history-dir", str(history_dir), "--output-json", str(output_json)], env=env)
    assert result.exit_code == 0

    payload = json.loads(output_json.read_text(encoding="utf-8"))
    assert "prices" in payload
    assert "valuation_summary" in payload
    assert "status" in payload
    assert payload["status"] in {"OK", "WARN", "ALERT"}
    assert set(payload["prices"].keys()) == {"updated", "skipped_unsupported", "skipped_unmapped", "failed_final"}
    assert "usable_non_market_assets" in payload["valuation_summary"]
    assert "warnings" in payload["valuation_summary"]
    assert "alerts" in payload["valuation_summary"]


def test_daily_report_output_json_stdout_contains_b43_minimum_structure(tmp_path, monkeypatch):
    db_file = tmp_path / "daily_report_b43_stdout.db"
    history_dir = tmp_path / "history"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    run_cmd(runner, ["buy", "--symbol", "BTC", "--account", "Main", "--qty", "1", "--price", "100"], env)

    from portfolio_tracker_v2.core import Database
    from portfolio_tracker_v2.core.asset_resolver import AssetResolver
    from datetime import datetime

    db = Database(str(db_file))
    resolver = AssetResolver(db)
    conn = db.connect()
    cursor = conn.cursor()
    btc_asset = resolver.resolve("BTC")
    cursor.execute(
        "UPDATE assets SET current_price = ?, price_source = ?, price_updated_at = ? WHERE id = ?",
        (200.0, "coingecko", datetime.now().isoformat(), btc_asset["id"]),
    )
    conn.commit()

    result = runner.invoke(main, ["daily-report", "--history-dir", str(history_dir), "--output-json", "-"], env=env)
    assert result.exit_code == 0

    payload = json.loads(result.output)
    assert "prices" in payload
    assert "valuation_summary" in payload
    assert "status" in payload


def test_daily_report_output_json_rejects_empty_path(tmp_path, monkeypatch):
    db_file = tmp_path / "daily_report_b43_empty.db"
    history_dir = tmp_path / "history"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    result = runner.invoke(main, ["daily-report", "--history-dir", str(history_dir), "--output-json", "   "], env=env)

    assert result.exit_code != 0
    assert "output-json cannot be empty" in result.output


def test_add_cdt_manual_reflects_positions_and_summary(tmp_path, monkeypatch):
    db_file = tmp_path / "add_cdt_manual.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0

    result = runner.invoke(
        main,
        [
            "add-cdt",
            "--account", "BBVA",
            "--symbol", "COLTEF CDT",
            "--open-date", "2026-01-26",
            "--maturity-date", "2026-06-23",
            "--principal", "4661.13",
            "--term", "0.6",
            "--rate", "0.102",
        ],
        env=env,
    )

    assert result.exit_code == 0
    assert "OK: CDT recorded id=" in result.output
    assert "term_years_derived=" in result.output

    positions = runner.invoke(main, ["positions", "--account", "BBVA"], env=env)
    assert positions.exit_code == 0
    assert "COLTEF CDT" in positions.output
    assert "contractual_value" in positions.output
    assert "usable_non_market" in positions.output

    summary = runner.invoke(main, ["summary", "--account", "BBVA"], env=env)
    assert summary.exit_code == 0
    assert "Non-Market Valued:" in summary.output


def test_add_cdt_rejects_invalid_date_order(tmp_path, monkeypatch):
    db_file = tmp_path / "add_cdt_invalid_dates.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0
    result = runner.invoke(
        main,
        [
            "add-cdt",
            "--account", "BBVA",
            "--symbol", "BBVA CDT",
            "--open-date", "2026-10-11",
            "--maturity-date", "2026-04-11",
            "--principal", "5000",
            "--term", "0.5",
            "--rate", "0.10",
        ],
        env=env,
    )

    assert result.exit_code == 2
    assert "maturity-date must be after open-date" in result.output


def test_add_fund_movement_reflects_legacy_plus_manual_balance_without_artificial_pnl(tmp_path, monkeypatch):
    db_file = tmp_path / "add_fund_movement.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0

    db = Database(str(db_file))
    conn = db.connect()
    cursor = conn.cursor()
    resolver = AssetResolver(db)
    asset = resolver.resolve("FONDO DINAMICO")
    cursor.execute("SELECT id FROM accounts WHERE name = ?", ("Trii",))
    row = cursor.fetchone()
    if row:
        account_id = row[0]
    else:
        cursor.execute("INSERT INTO accounts (name) VALUES (?)", ("Trii",))
        account_id = cursor.lastrowid
    cursor.execute(
        """
        INSERT INTO transactions
        (asset_id, account_id, tx_type, quantity, unit_price, fee_usd, total_usd, tx_date, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (asset["id"], account_id, "MIGRATION_BUY", 1.0, 263.25, 0.0, 263.25, "2026-04-01", "legacy seed"),
    )
    conn.commit()
    db.close()

    contribution = runner.invoke(
        main,
        [
            "add-fund-movement",
            "--account", "Trii",
            "--symbol", "FONDO DINAMICO",
            "--date", "2026-04-11",
            "--movement-type", "CONTRIBUTION",
            "--amount", "300",
        ],
        env=env,
    )
    assert contribution.exit_code == 0

    withdrawal = runner.invoke(
        main,
        [
            "add-fund-movement",
            "--account", "Trii",
            "--symbol", "FONDO DINAMICO",
            "--date", "2026-04-20",
            "--movement-type", "WITHDRAWAL",
            "--amount", "50",
        ],
        env=env,
    )
    assert withdrawal.exit_code == 0

    listed = runner.invoke(main, ["list-transactions", "--account", "Trii", "--symbol", "FONDO DINAMICO"], env=env)
    assert listed.exit_code == 0
    assert "-262.25" not in listed.output

    positions = runner.invoke(main, ["positions", "--account", "Trii"], env=env)
    assert positions.exit_code == 0
    assert "FONDO DINAMICO" in positions.output
    assert "513.25" in positions.output
    assert "snapshot_imported" in positions.output
    assert "usable_non_market" in positions.output

    summary = runner.invoke(main, ["summary", "--account", "Trii"], env=env)
    assert summary.exit_code == 0
    assert "Total Equity: 513.25" in summary.output
    assert "Non-Market Valued: 513.25" in summary.output


def test_add_fund_movement_blocks_negative_balance(tmp_path, monkeypatch):
    db_file = tmp_path / "add_fund_negative.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0

    db = Database(str(db_file))
    conn = db.connect()
    cursor = conn.cursor()
    resolver = AssetResolver(db)
    asset = resolver.resolve("FONDO DINAMICO")
    cursor.execute("INSERT INTO accounts (name) VALUES (?)", ("Trii",))
    account_id = cursor.lastrowid
    cursor.execute(
        """
        INSERT INTO transactions
        (asset_id, account_id, tx_type, quantity, unit_price, fee_usd, total_usd, tx_date, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (asset["id"], account_id, "MIGRATION_BUY", 1.0, 263.25, 0.0, 263.25, "2026-04-01", "legacy seed"),
    )
    conn.commit()
    db.close()

    blocked = runner.invoke(
        main,
        [
            "add-fund-movement",
            "--account", "Trii",
            "--symbol", "FONDO DINAMICO",
            "--date", "2026-04-20",
            "--movement-type", "WITHDRAWAL",
            "--amount", "300",
        ],
        env=env,
    )
    assert blocked.exit_code == 2
    assert "Insufficient fund balance" in blocked.output


def test_delete_transaction_manual_cdt_and_fund_movement_integrity(tmp_path, monkeypatch):
    db_file = tmp_path / "delete_cdt_fund.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0

    cdt = runner.invoke(
        main,
        [
            "add-cdt",
            "--account", "BBVA",
            "--symbol", "COLTEF CDT",
            "--open-date", "2026-01-26",
            "--maturity-date", "2026-06-23",
            "--principal", "4661.13",
            "--rate", "0.102",
        ],
        env=env,
    )
    assert cdt.exit_code == 0
    cdt_listed = runner.invoke(main, ["list-transactions", "--symbol", "COLTEF CDT"], env=env)
    cdt_id = int([line.split()[0] for line in cdt_listed.output.splitlines() if line.strip() and line.strip()[0].isdigit()][0])

    deleted_cdt = runner.invoke(main, ["delete-transaction", str(cdt_id)], env=env)
    assert deleted_cdt.exit_code == 0

    db = Database(str(db_file))
    conn = db.connect()
    cursor = conn.cursor()
    resolver = AssetResolver(db)
    fund = resolver.resolve("FONDO DINAMICO")
    cursor.execute("INSERT INTO accounts (name) VALUES (?)", ("Trii",))
    account_id = cursor.lastrowid
    cursor.execute(
        """
        INSERT INTO transactions
        (asset_id, account_id, tx_type, quantity, unit_price, fee_usd, total_usd, tx_date, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (fund["id"], account_id, "MIGRATION_BUY", 1.0, 263.25, 0.0, 263.25, "2026-04-01", "legacy seed"),
    )
    conn.commit()
    db.close()

    run_cmd(
        runner,
        ["add-fund-movement", "--account", "Trii", "--symbol", "FONDO DINAMICO", "--date", "2026-04-11", "--movement-type", "CONTRIBUTION", "--amount", "300"],
        env,
    )
    run_cmd(
        runner,
        ["add-fund-movement", "--account", "Trii", "--symbol", "FONDO DINAMICO", "--date", "2026-04-20", "--movement-type", "WITHDRAWAL", "--amount", "50"],
        env,
    )

    fund_listed = runner.invoke(main, ["list-transactions", "--account", "Trii", "--symbol", "FONDO DINAMICO"], env=env)
    fund_ids = [int(line.split()[0]) for line in fund_listed.output.splitlines() if line.strip() and line.strip()[0].isdigit()]
    delete_result = runner.invoke(main, ["delete-transaction", str(fund_ids[0])], env=env)
    assert delete_result.exit_code == 0

    positions = runner.invoke(main, ["positions", "--account", "Trii"], env=env)
    assert positions.exit_code == 0
    assert "563.25" in positions.output



def test_cash_summary_reflects_buy_sell_net_and_account_filter(tmp_path, monkeypatch):
    db_file = tmp_path / "cash_summary_net.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0

    run_cmd(
        runner,
        [
            "add-transaction",
            "--date", "2026-04-12",
            "--account", "Trezor",
            "--symbol", "BTC",
            "--side", "buy",
            "--qty", "1",
            "--price", "1000",
            "--fee", "5",
        ],
        env,
    )
    run_cmd(
        runner,
        [
            "add-transaction",
            "--date", "2026-04-13",
            "--account", "Trezor",
            "--symbol", "BTC",
            "--side", "sell",
            "--qty", "0.5",
            "--price", "1200",
            "--fee", "2",
        ],
        env,
    )

    total_summary = runner.invoke(main, ["summary"], env=env)
    assert total_summary.exit_code == 0
    assert "Cash balance: -407.00" in total_summary.output

    account_summary = runner.invoke(main, ["summary", "--account", "Trezor"], env=env)
    assert account_summary.exit_code == 0
    assert "Cash balance: -407.00" in account_summary.output



def test_backfill_cash_ledger_historical_transactions_is_idempotent_and_updates_summary(tmp_path, monkeypatch):
    db_file = tmp_path / "cash_backfill_historical.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0

    db = Database(str(db_file))
    conn = db.connect()
    cursor = conn.cursor()
    resolver = AssetResolver(db)

    btc = resolver.resolve("BTC")
    eth = resolver.resolve("ETH")
    cdt = resolver.resolve("COLTEF CDT")
    fund = resolver.resolve("FONDO DINAMICO")

    trii_id = cursor.execute("INSERT INTO accounts (name) VALUES (?)", ("Trii",)).lastrowid
    bbva_id = cursor.execute("INSERT INTO accounts (name) VALUES (?)", ("BBVA",)).lastrowid
    trezor_id = cursor.execute("INSERT INTO accounts (name) VALUES (?)", ("Trezor",)).lastrowid

    cdt_note = (
        "CDT_CONTRACT_V1:{\"open_date\":\"2026-01-26\",\"maturity_date\":\"2026-06-23\","
        "\"principal\":\"4661.13\",\"term_years\":\"0.40821918\",\"annual_rate\":\"0.102\","
        "\"term_source\":\"derived_from_dates\"}"
    )

    cursor.execute(
        """
        INSERT INTO transactions
        (asset_id, account_id, tx_type, quantity, unit_price, fee_usd, total_usd, tx_date, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (fund["id"], trii_id, "MIGRATION_BUY", 1.0, 263.25, 0.0, 263.25, "2026-04-01", "legacy seed"),
    )
    cursor.execute(
        """
        INSERT INTO transactions
        (asset_id, account_id, tx_type, quantity, unit_price, fee_usd, total_usd, tx_date, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (btc["id"], trii_id, "BUY", 1.0, 100.0, 1.0, 101.0, "2026-04-02", "legacy buy"),
    )
    cursor.execute(
        """
        INSERT INTO transactions
        (asset_id, account_id, tx_type, quantity, unit_price, fee_usd, total_usd, tx_date, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (btc["id"], trii_id, "SELL", 0.5, 120.0, 2.0, 58.0, "2026-04-03", "legacy sell"),
    )
    cursor.execute(
        """
        INSERT INTO transactions
        (asset_id, account_id, tx_type, quantity, unit_price, fee_usd, total_usd, tx_date, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (cdt["id"], bbva_id, "BUY", 1.0, 4661.13, 0.0, 4661.13, "2026-01-26", cdt_note),
    )
    cursor.execute(
        """
        INSERT INTO transactions
        (asset_id, account_id, tx_type, quantity, unit_price, fee_usd, total_usd, tx_date, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (fund["id"], trii_id, "BUY", 1.0, 300.0, 0.0, 300.0, "2026-04-11", "FUND_CONTRIBUTION | manual"),
    )
    cursor.execute(
        """
        INSERT INTO transactions
        (asset_id, account_id, tx_type, quantity, unit_price, fee_usd, total_usd, tx_date, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (fund["id"], trii_id, "SELL", 1.0, 50.0, 0.0, 50.0, "2026-04-20", "FUND_WITHDRAWAL"),
    )
    cursor.execute(
        """
        INSERT INTO transactions
        (asset_id, account_id, tx_type, quantity, unit_price, fee_usd, total_usd, tx_date, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (eth["id"], trezor_id, "BUY", 2.0, 50.0, 0.0, 100.0, "2026-04-05", "legacy buy"),
    )
    conn.commit()
    db.close()

    first_run = runner.invoke(main, ["backfill-cash-ledger"], env=env)
    assert first_run.exit_code == 0
    assert "Backfill complete: inserted 6 cash movement(s)." in first_run.output
    assert "Skipped existing: 0" in first_run.output
    assert "Skipped non-cash tx types: 1" in first_run.output

    total_summary = runner.invoke(main, ["summary", "--output-json", "-"], env=env)
    assert total_summary.exit_code == 0
    total_payload = json.loads(total_summary.output)
    assert total_payload["cash_balance"] == -5054.13

    trii_summary = runner.invoke(main, ["summary", "--account", "Trii", "--output-json", "-"], env=env)
    assert trii_summary.exit_code == 0
    trii_payload = json.loads(trii_summary.output)
    assert trii_payload["cash_balance"] == -293.0

    trezor_summary = runner.invoke(main, ["summary", "--account", "Trezor", "--output-json", "-"], env=env)
    assert trezor_summary.exit_code == 0
    trezor_payload = json.loads(trezor_summary.output)
    assert trezor_payload["cash_balance"] == -100.0

    second_run = runner.invoke(main, ["backfill-cash-ledger"], env=env)
    assert second_run.exit_code == 0
    assert "No new cash movements inserted; eligible transactions were already present in cash_ledger." in second_run.output
    assert "Skipped existing: 6" in second_run.output
    assert "Skipped non-cash tx types: 1" in second_run.output

def test_cash_summary_delete_transaction_reverts_cash(tmp_path, monkeypatch):
    db_file = tmp_path / "cash_delete_revert.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0

    run_cmd(
        runner,
        [
            "add-transaction",
            "--date", "2026-04-12",
            "--account", "Main",
            "--symbol", "BTC",
            "--side", "buy",
            "--qty", "1",
            "--price", "100",
        ],
        env,
    )

    listed = runner.invoke(main, ["list-transactions", "--account", "Main", "--symbol", "BTC"], env=env)
    assert listed.exit_code == 0
    tx_id = int([line.split()[0] for line in listed.output.splitlines() if line.strip() and line.strip()[0].isdigit()][0])

    before = runner.invoke(main, ["summary", "--account", "Main", "--output-json", "-"], env=env)
    assert before.exit_code == 0
    before_payload = json.loads(before.output)
    assert before_payload["cash_balance"] == -100.0

    deleted = runner.invoke(main, ["delete-transaction", str(tx_id)], env=env)
    assert deleted.exit_code == 0

    after = runner.invoke(main, ["summary", "--account", "Main", "--output-json", "-"], env=env)
    assert after.exit_code == 0
    after_payload = json.loads(after.output)
    assert after_payload["cash_balance"] == 0.0


def test_cash_behavior_cdt_and_fund_movements(tmp_path, monkeypatch):
    db_file = tmp_path / "cash_special_assets.db"
    env = {"PORTFOLIO_DB_PATH": str(db_file)}
    runner = CliRunner()

    assert runner.invoke(main, ["init-db"], env=env).exit_code == 0

    db = Database(str(db_file))
    conn = db.connect()
    cursor = conn.cursor()
    resolver = AssetResolver(db)
    fund = resolver.resolve("FONDO DINAMICO")
    cursor.execute("INSERT INTO accounts (name) VALUES (?)", ("Trii",))
    trii_id = cursor.lastrowid
    cursor.execute(
        """
        INSERT INTO transactions
        (asset_id, account_id, tx_type, quantity, unit_price, fee_usd, total_usd, tx_date, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (fund["id"], trii_id, "MIGRATION_BUY", 1.0, 263.25, 0.0, 263.25, "2026-04-01", "legacy seed"),
    )
    conn.commit()
    db.close()

    run_cmd(
        runner,
        [
            "add-cdt",
            "--account", "BBVA",
            "--symbol", "COLTEF CDT",
            "--open-date", "2026-01-26",
            "--maturity-date", "2026-06-23",
            "--principal", "4661.13",
            "--rate", "0.102",
        ],
        env,
    )
    run_cmd(
        runner,
        [
            "add-fund-movement",
            "--account", "Trii",
            "--symbol", "FONDO DINAMICO",
            "--date", "2026-04-11",
            "--movement-type", "CONTRIBUTION",
            "--amount", "300",
        ],
        env,
    )
    run_cmd(
        runner,
        [
            "add-fund-movement",
            "--account", "Trii",
            "--symbol", "FONDO DINAMICO",
            "--date", "2026-04-20",
            "--movement-type", "WITHDRAWAL",
            "--amount", "50",
        ],
        env,
    )

    bbva_summary = runner.invoke(main, ["summary", "--account", "BBVA", "--output-json", "-"], env=env)
    assert bbva_summary.exit_code == 0
    bbva_payload = json.loads(bbva_summary.output)
    assert bbva_payload["cash_balance"] == -4661.13

    trii_summary = runner.invoke(main, ["summary", "--account", "Trii", "--output-json", "-"], env=env)
    assert trii_summary.exit_code == 0
    trii_payload = json.loads(trii_summary.output)
    assert trii_payload["cash_balance"] == -250.0

