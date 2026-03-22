import json
import os
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from portfolio_tracker_v2.cli import main
from portfolio_tracker_v2.config import DB_PATH
from portfolio_tracker_v2.core import Database
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
    assert "BUY recorded" in result.output

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
    assert "SELL recorded" in result.output

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
    assert "Invalid value for '--from-date'" in result_date.output


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
    assert payload["cash_balance"] == 0.0
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

