import json
import os
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from portfolio_tracker_v2.cli import main
from portfolio_tracker_v2.config import DB_PATH
from portfolio_tracker_v2.services.price_svc import AssetRefreshResult, RefreshReport


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

    assert result.exit_code == 0
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

    assert result.exit_code == 0
    assert "ALERT:" in result.output
    assert "total_equity dropped" in result.output or "market_price_quality usable decreased" in result.output


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
