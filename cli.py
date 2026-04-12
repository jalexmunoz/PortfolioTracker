import contextlib
import csv
import io
import json
import os
import re
import tempfile
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Optional, List

import click

from portfolio_tracker_v2 import config
from portfolio_tracker_v2.core import Database
from portfolio_tracker_v2.core.asset_resolver import AssetResolver
from portfolio_tracker_v2.core.exceptions import InvalidTransaction
from portfolio_tracker_v2.migration.csv_importer import CSVImporter
from portfolio_tracker_v2.services.transaction_svc import TransactionService
from portfolio_tracker_v2.services.transaction_csv_importer import (
    TransactionCsvImportError,
    TransactionCsvImporter,
)
from portfolio_tracker_v2.services.legacy_seed_importer import (
    LegacyPositionsCsvImporter,
    LegacySeedImportError,
)
from portfolio_tracker_v2.services.pnl_svc import PnLService
from portfolio_tracker_v2.services.price_svc import RefreshReport, refresh_prices
from portfolio_tracker_v2.scripts import init_db as init_db_script

# CLI operational defaults (single source of truth)
DEFAULT_HISTORY_DIR = os.path.join("output", "history")
DEFAULT_EQUITY_DROP_PCT = 3.0
DEFAULT_UNVALUED_INCREASE_THRESHOLD = 0.0
DEFAULT_ASSET_CLASS_SHIFT_PCT = 5.0
DEFAULT_KEEP_LAST_SUMMARY_SNAPSHOTS = 10
DEFAULT_LATEST_DAILY_REPORT_PATH = os.path.join("output", "reports", "daily_report_latest.json")
DEFAULT_DAILY_REPORT_TOP_POSITIONS = 5
SUMMARY_HISTORY_SNAPSHOT_RE = re.compile(r'^summary_\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}Z\.json$')
# helpers

def get_db_path() -> str:
    """Determine database path: env override or config value."""
    return os.environ.get("PORTFOLIO_DB_PATH", config.DB_PATH)


def parse_decimal(ctx, param, value):
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError):
        raise click.BadParameter(f"{param} must be a number, got '{value}'")


def parse_optional_decimal(ctx, param, value):
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return Decimal(raw)
    except (InvalidOperation, TypeError):
        raise click.BadParameter(f"{param} must be a number, got '{value}'")


def format_money(value):
    """Format decimal as currency: 1234.56 -> 1,234.56"""
    return f"{value:,.2f}"


def format_qty(value):
    """Format quantity: trim trailing zeros, reasonable precision."""
    s = f"{value:.8f}"
    return s.rstrip('0').rstrip('.') if '.' in s else s


def _to_json_scalar(value):
    if isinstance(value, Decimal):
        return float(value)
    return value


def _build_summary_export_payload(summary: dict) -> dict:
    breakdown = summary.get('asset_class_breakdown', {})
    return {
        'generated_at': datetime.now(timezone.utc).isoformat(),
        'total_cost_basis': _to_json_scalar(summary['total_cost_basis']),
        'total_realized_pnl': _to_json_scalar(summary['total_realized_pnl']),
        'cash_balance': _to_json_scalar(summary['cash_balance']),
        'total_equity': _to_json_scalar(summary['total_equity']),
        'market_covered_value': _to_json_scalar(summary['market_covered_value']),
        'non_market_valued': _to_json_scalar(summary['non_market_valued']),
        'unvalued_excluded_cost_basis': _to_json_scalar(summary['unvalued_excluded_cost_basis']),
        'total_unrealized_pnl_approved': _to_json_scalar(summary['total_unrealized_pnl']),
        'unrealized_return_pct_approved': _to_json_scalar(summary['unrealized_return_pct']),
        'market_price_quality': summary['price_quality_counts'],
        'asset_class_breakdown': {
            'Crypto': _to_json_scalar(breakdown.get('Crypto', Decimal('0'))),
            'Equities': _to_json_scalar(breakdown.get('Equities', Decimal('0'))),
            'Metals': _to_json_scalar(breakdown.get('Metals', Decimal('0'))),
            'Non-market': _to_json_scalar(breakdown.get('Non-market', Decimal('0'))),
        },
    }


def _build_summary_output_json_payload(summary: dict) -> dict:
    breakdown = summary.get('asset_class_breakdown', {}) or {}
    total_equity = Decimal(str(summary.get('total_equity', 0)))
    breakdown_rows = []
    for asset_class in ["Crypto", "Equities", "Metals", "Non-market"]:
        equity = Decimal(str(breakdown.get(asset_class, Decimal('0'))))
        if total_equity > 0:
            pct = ((equity / total_equity) * Decimal('100')).quantize(Decimal('0.01'))
        else:
            pct = Decimal('0.00')
        breakdown_rows.append(
            {
                'asset_class': asset_class,
                'equity': _to_json_scalar(equity),
                'pct': _to_json_scalar(pct),
            }
        )

    return {
        'total_cost_basis': _to_json_scalar(summary['total_cost_basis']),
        'total_realized_pnl': _to_json_scalar(summary['total_realized_pnl']),
        'cash_balance': _to_json_scalar(summary['cash_balance']),
        'total_equity': _to_json_scalar(summary['total_equity']),
        'market_covered_value': _to_json_scalar(summary['market_covered_value']),
        'non_market_valued': _to_json_scalar(summary['non_market_valued']),
        'unvalued_excluded_cost_basis': _to_json_scalar(summary['unvalued_excluded_cost_basis']),
        'total_unrealized_pnl': _to_json_scalar(summary['total_unrealized_pnl']),
        'unrealized_return_pct': _to_json_scalar(summary['unrealized_return_pct']),
        'market_price_quality': {
            'usable': int(summary['price_quality_counts']['usable']),
            'stale': int(summary['price_quality_counts']['stale']),
            'unavailable': int(summary['price_quality_counts']['unavailable']),
        },
        'asset_class_breakdown': breakdown_rows,
    }



def _build_history_snapshot_path(export_dir: str) -> str:
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H-%M-%SZ')
    return os.path.join(export_dir, f'summary_{timestamp}.json')

def _build_daily_report_history_path(export_dir: str) -> str:
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H-%M-%SZ')
    return os.path.join(export_dir, f'daily_report_{timestamp}.json')



def _to_decimal(value) -> Decimal:
    if value is None:
        return Decimal('0')
    return Decimal(str(value))


def _format_delta(value: Decimal) -> str:
    sign = '+' if value >= 0 else '-'
    return f"{sign}{format_money(abs(value))}"


def _build_snapshot_comparison_payload(old: dict, new: dict) -> dict:
    numeric_metrics = [
        ("total_equity", "total_equity"),
        ("market_covered_value", "market_covered_value"),
        ("non_market_valued", "non_market_valued"),
        ("total_unrealized_pnl_approved", "total_unrealized_pnl_approved"),
        ("unrealized_return_pct_approved", "unrealized_return_pct_approved"),
    ]
    metrics = {}
    for output_key, snapshot_key in numeric_metrics:
        old_val = _to_decimal(old.get(snapshot_key))
        new_val = _to_decimal(new.get(snapshot_key))
        metrics[output_key] = {
            "old": _to_json_scalar(old_val),
            "new": _to_json_scalar(new_val),
            "delta": _to_json_scalar(new_val - old_val),
        }

    old_quality = old.get('market_price_quality', {}) or {}
    new_quality = new.get('market_price_quality', {}) or {}
    quality = {}
    for key in ['usable', 'stale', 'unavailable']:
        old_v = int(old_quality.get(key, 0))
        new_v = int(new_quality.get(key, 0))
        quality[key] = {"old": old_v, "new": new_v, "delta": new_v - old_v}

    old_breakdown = old.get('asset_class_breakdown', {}) or {}
    new_breakdown = new.get('asset_class_breakdown', {}) or {}
    classes = sorted(set(old_breakdown.keys()) | set(new_breakdown.keys()) | {'Crypto', 'Equities', 'Metals', 'Non-market'})
    breakdown = {}
    for cls in classes:
        old_v = _to_decimal(old_breakdown.get(cls, 0))
        new_v = _to_decimal(new_breakdown.get(cls, 0))
        breakdown[cls] = {
            "old": _to_json_scalar(old_v),
            "new": _to_json_scalar(new_v),
            "delta": _to_json_scalar(new_v - old_v),
        }

    return {
        "old_generated_at": old.get("generated_at"),
        "new_generated_at": new.get("generated_at"),
        "metrics": metrics,
        "market_price_quality": quality,
        "asset_class_breakdown": breakdown,
    }


def _load_snapshot(path: str) -> dict:
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def _validate_daily_report_payload(payload: dict) -> None:
    if not isinstance(payload, dict):
        raise ValueError("root JSON value must be an object")
    if payload.get("report_type") != "daily-report":
        raise ValueError("report_type must be 'daily-report'")
    if payload.get("report_schema_version") != 1:
        raise ValueError("report_schema_version must be 1")
    for field in ["run_timestamp", "summary_result", "final_exit_code"]:
        if field not in payload:
            raise ValueError(f"missing required top-level field: {field}")
    if not isinstance(payload["summary_result"], dict):
        raise ValueError("summary_result must be an object")

def _load_and_validate_daily_report(path: str) -> dict:
    if not os.path.exists(path):
        raise FileNotFoundError(f"file not found: {path}")
    payload = _load_snapshot(path)
    _validate_daily_report_payload(payload)
    return payload


def _format_optional_money(value) -> str:
    if value is None:
        return "N/A"
    return format_money(Decimal(str(value)))


def _format_optional_pct(value) -> str:
    if value is None:
        return "N/A"
    return f"{Decimal(str(value)):.2f}%"


def _render_daily_report_summary(payload: dict) -> None:
    summary = payload.get("summary_result", {}) or {}
    alerts = payload.get("alerts_result") or {}
    alerts_status = alerts.get("status", "OK" if not alerts else "UNKNOWN")
    alerts_count = alerts.get("count", 0)
    click.echo("Latest Daily Report")
    click.echo(f"Run timestamp: {payload.get('run_timestamp', '-')}")
    click.echo(f"Final exit code: {payload.get('final_exit_code', '-')}")
    click.echo(f"Alerts: {alerts_status} ({alerts_count})")
    click.echo(f"Created snapshot: {payload.get('created_snapshot_path') or '-'}")
    click.echo(f"Previous snapshot: {payload.get('previous_snapshot_path') or '-'}")
    click.echo(f"Total Equity: {_format_optional_money(summary.get('total_equity'))}")
    click.echo(f"Market-Covered Value: {_format_optional_money(summary.get('market_covered_value'))}")
    click.echo(f"Non-Market Valued: {_format_optional_money(summary.get('non_market_valued'))}")
    click.echo(f"Unvalued / Excluded (cost basis): {_format_optional_money(summary.get('unvalued_excluded_cost_basis'))}")


def _print_daily_report_block(run_timestamp: str, summary: dict, positions: list[dict]) -> None:
    counts = summary['price_quality_counts']
    click.echo(f"Timestamp: {run_timestamp}")
    click.echo(f"Total Equity: {format_money(summary['total_equity'])}")
    click.echo(f"Cash balance: {format_money(summary['cash_balance'])}")
    click.echo(f"Total market value: {format_money(summary['total_market_value'])}")
    click.echo(f"Realized PnL: {format_money(summary['total_realized_pnl'])}")
    click.echo(f"Unrealized PnL: {format_money(summary['total_unrealized_pnl'])}")
    click.echo(f"Unrealized return %: {_format_optional_pct(summary['unrealized_return_pct'])}")
    click.echo(
        "Price quality: "
        f"{counts['usable']} usable, {counts['stale']} stale, {counts['unavailable']} unavailable"
    )

    top_positions = sorted(
        (p for p in positions if p['qty_open'] > 0),
        key=lambda p: (p['approved_value'] is not None, p['approved_value'] or Decimal('0')),
        reverse=True,
    )[:DEFAULT_DAILY_REPORT_TOP_POSITIONS]

    click.echo("Top positions by market value")
    if top_positions:
        rows = []
        for position in top_positions:
            rows.append(
                (
                    position['symbol'],
                    position['account'] or '(all)',
                    format_qty(position['qty_open']),
                    _format_optional_money(position['approved_value']),
                    position['valuation_status'],
                )
            )
        display_table(["Symbol", "Account", "Qty", "Market Value", "Price Quality"], rows)
    else:
        click.echo("No open positions found.")

    warnings = []
    if counts['stale']:
        warnings.append(f"{counts['stale']} position(s) have stale prices")
    if counts['unavailable']:
        warnings.append(f"{counts['unavailable']} position(s) have unavailable prices")

    if warnings:
        click.echo("Warnings")
        for warning in warnings:
            click.echo(f"- {warning}")


def _print_summary_block(summary: dict) -> None:
    click.echo(f"Total cost basis: {format_money(summary['total_cost_basis'])}")
    click.echo(f"Total realized PnL: {format_money(summary['total_realized_pnl'])}")
    click.echo(f"Cash balance: {format_money(summary['cash_balance'])}")
    click.echo(f"Total Equity: {format_money(summary['total_equity'])}")
    click.echo(f"Market-Covered Value: {format_money(summary['market_covered_value'])}")
    click.echo(f"Non-Market Valued: {format_money(summary['non_market_valued'])}")
    click.echo(f"Unvalued / Excluded (cost basis): {format_money(summary['unvalued_excluded_cost_basis'])}")
    click.echo(f"Total unrealized PnL (approved valuations): {format_money(summary['total_unrealized_pnl'])}")
    if summary['unrealized_return_pct'] is not None:
        click.echo(f"Unrealized return % (approved valuations): {summary['unrealized_return_pct']:.2f}%")
    else:
        click.echo("Unrealized return % (approved valuations): N/A")

    counts = summary['price_quality_counts']
    click.echo(f"Market price quality: {counts['usable']} usable, {counts['stale']} stale, {counts['unavailable']} unavailable")

    breakdown = summary.get('asset_class_breakdown', {})
    click.echo("Asset class breakdown (approved equity):")
    for cls in ["Crypto", "Equities", "Metals", "Non-market"]:
        value = breakdown.get(cls, Decimal('0'))
        if summary['total_equity'] > 0:
            pct = ((value / summary['total_equity']) * 100).quantize(Decimal('0.01'))
        else:
            pct = Decimal('0.00')
        click.echo(f"  {cls}: {format_money(value)} ({pct:.2f}%)")


def _write_summary_json_export(export_path: str, payload: dict) -> None:
    _write_json_atomic(export_path, payload)


def _write_json_atomic(export_path: str, payload: dict) -> None:
    export_dir = os.path.dirname(export_path)
    if export_dir:
        os.makedirs(export_dir, exist_ok=True)
    tmp_dir = export_dir or "."
    fd, tmp_path = tempfile.mkstemp(prefix=".tmp_", suffix=".json", dir=tmp_dir)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, export_path)
    except Exception:
        with contextlib.suppress(FileNotFoundError):
            os.remove(tmp_path)
        raise


def _write_summary_history_export(export_dir: str, payload: dict) -> str:
    os.makedirs(export_dir, exist_ok=True)
    history_path = _build_history_snapshot_path(export_dir)
    with open(history_path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2)
    return history_path


def _list_history_snapshot_paths(history_dir: str) -> list[str]:
    if not os.path.isdir(history_dir):
        return []
    snapshots = []
    for file_name in os.listdir(history_dir):
        if SUMMARY_HISTORY_SNAPSHOT_RE.match(file_name):
            snapshots.append(os.path.join(history_dir, file_name))
    return sorted(snapshots)


def _find_previous_history_snapshot(history_dir: str, current_path: str) -> str | None:
    snapshots = _list_history_snapshot_paths(history_dir)
    if current_path not in snapshots:
        return None
    idx = snapshots.index(current_path)
    if idx <= 0:
        return None
    return snapshots[idx - 1]


def _render_snapshot_comparison(old: dict, new: dict) -> None:
    click.echo("Summary Snapshot Comparison")
    click.echo(f"Old generated_at: {old.get('generated_at', '-')}")
    click.echo(f"New generated_at: {new.get('generated_at', '-')}")

    numeric_metrics = [
        ("Total Equity", "total_equity"),
        ("Market-Covered Value", "market_covered_value"),
        ("Non-Market Valued", "non_market_valued"),
        ("Total Unrealized PnL (approved)", "total_unrealized_pnl_approved"),
        ("Unrealized return % (approved)", "unrealized_return_pct_approved"),
    ]

    rows = []
    for label, key in numeric_metrics:
        old_val = _to_decimal(old.get(key))
        new_val = _to_decimal(new.get(key))
        delta = new_val - old_val
        if key == 'unrealized_return_pct_approved':
            old_s = f"{old_val:.2f}%"
            new_s = f"{new_val:.2f}%"
            delta_s = f"{delta:+.2f}%"
        else:
            old_s = format_money(old_val)
            new_s = format_money(new_val)
            delta_s = _format_delta(delta)
        rows.append((label, old_s, new_s, delta_s))

    display_table(["Metric", "Old", "New", "Delta"], rows)

    old_quality = old.get('market_price_quality', {}) or {}
    new_quality = new.get('market_price_quality', {}) or {}
    quality_rows = []
    for key in ['usable', 'stale', 'unavailable']:
        old_v = int(old_quality.get(key, 0))
        new_v = int(new_quality.get(key, 0))
        quality_rows.append((key, str(old_v), str(new_v), f"{new_v - old_v:+d}"))

    click.echo("Market price quality")
    display_table(["Metric", "Old", "New", "Delta"], quality_rows)

    old_breakdown = old.get('asset_class_breakdown', {}) or {}
    new_breakdown = new.get('asset_class_breakdown', {}) or {}
    classes = sorted(set(old_breakdown.keys()) | set(new_breakdown.keys()) | {'Crypto', 'Equities', 'Metals', 'Non-market'})
    breakdown_rows = []
    for cls in classes:
        old_v = _to_decimal(old_breakdown.get(cls, 0))
        new_v = _to_decimal(new_breakdown.get(cls, 0))
        breakdown_rows.append((cls, format_money(old_v), format_money(new_v), _format_delta(new_v - old_v)))

    click.echo("Asset class breakdown")
    display_table(["Class", "Old", "New", "Delta"], breakdown_rows)


def _evaluate_snapshot_alerts(
    old: dict,
    new: dict,
    equity_drop_pct: float = DEFAULT_EQUITY_DROP_PCT,
    unvalued_increase_threshold: float = DEFAULT_UNVALUED_INCREASE_THRESHOLD,
    asset_class_shift_pct: float = DEFAULT_ASSET_CLASS_SHIFT_PCT,
) -> list[str]:
    alerts = []

    old_equity = _to_decimal(old.get('total_equity'))
    new_equity = _to_decimal(new.get('total_equity'))
    equity_drop_threshold = Decimal(str(equity_drop_pct))
    if old_equity > 0 and new_equity < old_equity:
        drop_pct = ((old_equity - new_equity) / old_equity) * Decimal('100')
        if drop_pct > equity_drop_threshold:
            alerts.append(
                f"total_equity dropped {drop_pct.quantize(Decimal('0.01'))}% ({format_money(old_equity)} -> {format_money(new_equity)})"
            )

    old_unvalued = _to_decimal(old.get('unvalued_excluded_cost_basis'))
    new_unvalued = _to_decimal(new.get('unvalued_excluded_cost_basis'))
    unvalued_delta = new_unvalued - old_unvalued
    unvalued_threshold = Decimal(str(unvalued_increase_threshold))
    if new_unvalued > 0 and unvalued_delta > unvalued_threshold:
        alerts.append(
            f"unvalued_excluded_cost_basis increased by {format_money(unvalued_delta)} ({format_money(old_unvalued)} -> {format_money(new_unvalued)})"
        )

    old_quality = old.get('market_price_quality', {}) or {}
    new_quality = new.get('market_price_quality', {}) or {}
    old_usable = int(old_quality.get('usable', 0))
    new_usable = int(new_quality.get('usable', 0))
    if new_usable < old_usable:
        alerts.append(f"market_price_quality usable decreased ({old_usable} -> {new_usable})")

    old_unavailable = int(old_quality.get('unavailable', 0))
    new_unavailable = int(new_quality.get('unavailable', 0))
    if new_unavailable > old_unavailable:
        alerts.append(f"market_price_quality unavailable increased ({old_unavailable} -> {new_unavailable})")

    old_breakdown = old.get('asset_class_breakdown', {}) or {}
    new_breakdown = new.get('asset_class_breakdown', {}) or {}
    classes = sorted(set(old_breakdown.keys()) | set(new_breakdown.keys()) | {'Crypto', 'Equities', 'Metals', 'Non-market'})
    class_shift_threshold = Decimal(str(asset_class_shift_pct))
    for cls in classes:
        old_value = _to_decimal(old_breakdown.get(cls, 0))
        new_value = _to_decimal(new_breakdown.get(cls, 0))
        old_share = ((old_value / old_equity) * Decimal('100')) if old_equity > 0 else Decimal('0')
        new_share = ((new_value / new_equity) * Decimal('100')) if new_equity > 0 else Decimal('0')
        shift = abs(new_share - old_share)
        if shift > class_shift_threshold:
            alerts.append(
                f"asset_class_breakdown {cls} shifted {shift.quantize(Decimal('0.01'))} pp ({old_share.quantize(Decimal('0.01'))}% -> {new_share.quantize(Decimal('0.01'))}%)"
            )

    return alerts


def _render_snapshot_alerts(
    old: dict,
    new: dict,
    equity_drop_pct: float = DEFAULT_EQUITY_DROP_PCT,
    unvalued_increase_threshold: float = DEFAULT_UNVALUED_INCREASE_THRESHOLD,
    asset_class_shift_pct: float = DEFAULT_ASSET_CLASS_SHIFT_PCT,
) -> list[str]:
    alerts = _evaluate_snapshot_alerts(old, new, equity_drop_pct, unvalued_increase_threshold, asset_class_shift_pct)
    click.echo("Summary Snapshot Alerts")
    click.echo(f"Old generated_at: {old.get('generated_at', '-')}")
    click.echo(f"New generated_at: {new.get('generated_at', '-')}")
    if alerts:
        click.echo(f"ALERT: {len(alerts)} alert(s) detected")
        for alert in alerts:
            click.echo(f"- {alert}")
    else:
        click.echo("OK: no alerts detected")
    return alerts


def _render_refresh_report(db: Database, report: RefreshReport, verbose: bool) -> None:
    if verbose and report.results:
        cursor = db.connect().cursor()
        rows = []
        for r in report.results:
            cursor.execute(
                """
                SELECT valuation_method, price_source, current_price, price_updated_at
                FROM assets
                WHERE id = ?
                """,
                (r.asset_id,),
            )
            row = cursor.fetchone()
            valuation_method = row[0] if row and row[0] else '-'
            price_source = row[1] if row and row[1] else '-'
            current_price = format_money(Decimal(str(row[2]))) if row and row[2] is not None else '-'
            price_updated_at = row[3] if row and row[3] else '-'
            rows.append(
                (
                    r.symbol,
                    r.provider or '-',
                    valuation_method,
                    r.status,
                    r.reason,
                    price_source,
                    current_price,
                    price_updated_at,
                )
            )
        display_table(
            ["Symbol", "Provider", "Val Method", "Outcome", "Reason", "Price Source", "Current Price", "Updated At"],
            rows,
        )

    click.echo(
        'Prices refreshed: '
        f'{report.updated} updated, '
        f'{report.skipped_unsupported} skipped unsupported, '
        f'{report.skipped_unmapped} skipped unmapped, '
        f'{report.failed_final} failed final'
    )


def ensure_db():
    path = get_db_path()
    db = Database(path)
    db.connect()
    db.init_schema()
    return db


def load_symbols(db: Database) -> List[str]:
    # return list of symbols that appear in transactions
    cursor = db.connect().cursor()
    cursor.execute(
        """
        SELECT DISTINCT a.symbol
        FROM assets a
        JOIN transactions t ON t.asset_id = a.id
        """
    )
    return [row[0] for row in cursor.fetchall()]


def display_table(headers, rows):
    # simple text table with column widths
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))
    fmt = "  ".join(f"{{:<{w}}}" for w in widths)
    click.echo(fmt.format(*headers))
    click.echo(fmt.format(*["-" * w for w in widths]))
    for row in rows:
        click.echo(fmt.format(*row))



def _write_csv_output(output_csv: str, headers: list[str], rows: list[tuple]) -> None:
    output_path = (output_csv or "").strip()
    if not output_path:
        raise click.BadParameter("output-csv cannot be empty", param_hint="output_csv")

    try:
        if output_path == "-":
            writer = csv.writer(click.get_text_stream("stdout"), lineterminator="\n")
            writer.writerow(headers)
            writer.writerows(rows)
            return

        export_dir = os.path.dirname(output_path)
        if export_dir:
            os.makedirs(export_dir, exist_ok=True)

        with open(output_path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(headers)
            writer.writerows(rows)
    except OSError as exc:
        click.echo(f"ERROR: failed to write CSV: {exc}", err=True)
        raise click.exceptions.Exit(2)



def _normalize_tx_date(tx_date) -> str:
    if tx_date is None:
        return date.today().isoformat()
    if isinstance(tx_date, datetime):
        return tx_date.date().isoformat()
    if isinstance(tx_date, date):
        return tx_date.isoformat()

    tx_date_text = str(tx_date).strip()
    try:
        return date.fromisoformat(tx_date_text).isoformat()
    except ValueError as exc:
        raise click.BadParameter("tx_date must be in YYYY-MM-DD format", param_hint="tx_date") from exc


def _record_transaction_from_cli(side, symbol, account, qty, price, fee, tx_date, notes):
    symbol_normalized = (symbol or "").strip().upper()
    if not symbol_normalized:
        raise click.BadParameter("symbol cannot be empty", param_hint="symbol")

    account_normalized = (account or "").strip()
    if not account_normalized:
        raise click.BadParameter("account cannot be empty", param_hint="account")

    tx_date_normalized = _normalize_tx_date(tx_date)
    db = ensure_db()
    resolver = AssetResolver(db)
    svc = TransactionService(db, resolver)
    side_normalized = side.lower()

    try:
        if side_normalized == "buy":
            tx_id = svc.record_buy(symbol_normalized, account_normalized, qty, price, fee, tx_date_normalized, notes)
            side_label = "BUY"
        elif side_normalized == "sell":
            tx_id = svc.record_sell(symbol_normalized, account_normalized, qty, price, fee, tx_date_normalized, notes)
            side_label = "SELL"
        else:
            raise click.BadParameter(f"Unsupported side: {side}", param_hint="side")
    except InvalidTransaction as exc:
        click.echo(f"ERROR: {exc}", err=True)
        raise click.exceptions.Exit(2)

    click.echo(
        "OK: "
        f"{side_label} recorded id={tx_id} "
        f"date={tx_date_normalized} "
        f"account={account_normalized} "
        f"symbol={symbol_normalized} "
        f"qty={format_qty(qty)} "
        f"unit_price={format_money(price)}"
    )

def _render_transaction_csv_import_result(result):
    rejected_count = len(result.rejected_rows)
    imported_label = "Would import OK" if result.dry_run else "Imported OK"
    click.echo(f"File read: {result.file_path}")
    if result.dry_run:
        click.echo("Dry run: no transactions persisted")
    click.echo(f"Rows processed: {result.total_rows}")
    click.echo(f"{imported_label}: {result.imported_rows}")
    click.echo(f"Rejected: {rejected_count}")
    if not rejected_count:
        return

    click.echo("Rejected rows:")
    max_errors_to_show = 20
    for rejected in result.rejected_rows[:max_errors_to_show]:
        click.echo(f"  row {rejected.row_number}: {rejected.reason}")

    remaining = rejected_count - max_errors_to_show
    if remaining > 0:
        click.echo(f"  ... and {remaining} more")


def _render_legacy_seed_import_result(result):
    rejected_count = len(result.rejected_rows)
    imported_label = "Would seed OK" if result.dry_run else "Seeded OK"
    click.echo(f"File read: {result.file_path}")
    click.echo(f"Seed date: {result.seed_date}")
    if result.dry_run:
        click.echo("Dry run: no seed positions persisted")
    click.echo(f"Rows processed: {result.total_rows}")
    click.echo(f"{imported_label}: {result.imported_rows}")
    click.echo(f"Rejected: {rejected_count}")
    if not rejected_count:
        return

    click.echo("Rejected rows:")
    max_errors_to_show = 20
    for rejected in result.rejected_rows[:max_errors_to_show]:
        click.echo(f"  row {rejected.row_number}: {rejected.reason}")

    remaining = rejected_count - max_errors_to_show
    if remaining > 0:
        click.echo(f"  ... and {remaining} more")

@click.group()
def main():
    """Portfolio Tracker v2 command line interface."""
    pass


@main.command("init-db")
def cli_init_db():
    """Initialize the SQLite database (schema and initial assets)."""
    # ensure config DB_PATH matches override
    new_path = get_db_path()
    config.DB_PATH = new_path
    # also patch script's local DB_PATH (imported earlier)
    init_db_script.DB_PATH = new_path
    try:
        init_db_script.main()
    except SystemExit:
        # propagate as click.Abort to keep exit code nonzero
        raise click.Abort()


@main.command("import-csv")
@click.option("--input", "input_path", required=True, type=click.Path(exists=True))
@click.option("--execute", is_flag=True, help="Perform insert instead of dry-run")
def cli_import_csv(input_path, execute):
    """Import legacy CSV data. Dry run by default."""
    db = ensure_db()
    resolver = AssetResolver(db)
    importer = CSVImporter(db, resolver, input_path)
    report = importer.dry_run()
    click.echo(f"Total rows: {report.total_rows}")
    click.echo(f"Valid rows: {report.valid_row_count}")
    click.echo(f"Symbols: {', '.join(sorted(report.unique_symbols))}")
    click.echo(f"Accounts: {', '.join(sorted(report.unique_accounts))}")
    click.echo(f"Total cost: {report.total_cost_sum}")
    if report.warnings:
        click.echo("Warnings:")
        for w in report.warnings:
            click.echo(f"  {w}")
    if execute:
        click.confirm("Proceed with import?", abort=True)
        summary = importer.execute()
        click.echo(f"Imported {summary.transactions_added} transactions")


@main.command("import-transactions-csv")
@click.argument("csv_path", type=click.Path(dir_okay=False))
@click.option("--dry-run", is_flag=True, help="Validate and summarize without writing to the real database.")
def cli_import_transactions_csv(csv_path, dry_run):
    """Import BUY/SELL ledger transactions from one explicit CSV format."""
    db = ensure_db()
    importer = None
    temp_db = None
    temp_db_path = None

    try:
        if dry_run:
            source_conn = db.connect()
            with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_file:
                temp_db_path = tmp_file.name
            temp_db = Database(temp_db_path)
            source_conn.backup(temp_db.connect())
            resolver = AssetResolver(temp_db)
            svc = TransactionService(temp_db, resolver)
            importer = TransactionCsvImporter(svc)
        else:
            resolver = AssetResolver(db)
            svc = TransactionService(db, resolver)
            importer = TransactionCsvImporter(svc)

        result = importer.import_file(csv_path, dry_run=dry_run)
        _render_transaction_csv_import_result(result)
    except TransactionCsvImportError as exc:
        click.echo(f"ERROR: {exc}", err=True)
        raise click.exceptions.Exit(2)
    except Exception as exc:
        click.echo(f"ERROR: {exc}", err=True)
        raise click.exceptions.Exit(2)
    finally:
        if temp_db is not None:
            temp_db.close()
        if temp_db_path and os.path.exists(temp_db_path):
            os.remove(temp_db_path)


@main.command("import-legacy-positions-csv")
@click.argument("csv_path", type=click.Path(dir_okay=False))
@click.option("--seed-date", required=True, help="Common seed date to assign to imported open-position lots (YYYY-MM-DD).")
@click.option("--dry-run", is_flag=True, help="Validate and summarize without writing seed positions to the real database.")
def cli_import_legacy_positions_csv(csv_path, seed_date, dry_run):
    """Import legacy open positions snapshot as seed lots."""
    db = ensure_db()
    resolver = AssetResolver(db)
    importer = LegacyPositionsCsvImporter(db, resolver)

    try:
        result = importer.import_file(csv_path, seed_date=seed_date, dry_run=dry_run)
        _render_legacy_seed_import_result(result)
        if result.rejected_rows:
            raise click.exceptions.Exit(2)
    except LegacySeedImportError as exc:
        click.echo(f"ERROR: {exc}", err=True)
        raise click.exceptions.Exit(2)
    except click.exceptions.Exit:
        raise
    except Exception as exc:
        click.echo(f"ERROR: {exc}", err=True)
        raise click.exceptions.Exit(2)


@main.command("add-transaction")
@click.option("--tx-date", "--date", "tx_date", default=None, type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("--account", required=True)
@click.option("--symbol", required=True)
@click.option("--side", required=True, type=click.Choice(["buy", "sell"], case_sensitive=False))
@click.option("--qty", required=True, callback=parse_decimal)
@click.option("--unit-price", "--price", "price", required=True, callback=parse_decimal)
@click.option("--fee", default="0", callback=parse_decimal)
@click.option("--notes", default=None)
def cli_add_transaction(tx_date, account, symbol, side, qty, price, fee, notes):
    """Record a portfolio transaction from the transaction ledger source of truth."""
    _record_transaction_from_cli(side, symbol, account, qty, price, fee, tx_date, notes)


@main.command("add-cdt")
@click.option("--account", required=True)
@click.option("--symbol", default="BBVA CDT", show_default=True)
@click.option("--open-date", "open_date", required=True, type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("--maturity-date", "maturity_date", required=True, type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("--principal", required=True, callback=parse_decimal)
@click.option("--term", "term_years", default=None, callback=parse_optional_decimal, help="Optional term in years (decimal). If provided, dates remain the source of truth.")
@click.option("--rate", "annual_rate", required=True, callback=parse_decimal, help="Simple annual rate as decimal. Example: 0.10 = 10%.")
@click.option("--notes", default=None)
def cli_add_cdt(account, symbol, open_date, maturity_date, principal, term_years, annual_rate, notes):
    """Record one manual non-market CDT contract (term is in years)."""
    account_normalized = (account or "").strip()
    if not account_normalized:
        raise click.BadParameter("account cannot be empty", param_hint="account")

    symbol_normalized = (symbol or "").strip().upper()
    if not symbol_normalized:
        raise click.BadParameter("symbol cannot be empty", param_hint="symbol")

    if principal <= 0:
        raise click.BadParameter("principal must be > 0", param_hint="principal")
    if annual_rate < 0:
        raise click.BadParameter("rate must be >= 0", param_hint="rate")
    if term_years is not None and term_years <= 0:
        raise click.BadParameter("term must be > 0 (years) when provided", param_hint="term")

    open_date_iso = open_date.date().isoformat()
    maturity_date_iso = maturity_date.date().isoformat()
    if maturity_date_iso <= open_date_iso:
        raise click.BadParameter("maturity-date must be after open-date", param_hint="maturity_date")

    db = ensure_db()
    resolver = AssetResolver(db)
    svc = TransactionService(db, resolver)

    try:
        tx_id = svc.record_cdt(
            account=account_normalized,
            symbol=symbol_normalized,
            open_date=open_date_iso,
            maturity_date=maturity_date_iso,
            principal=principal,
            term_years=term_years,
            annual_rate=annual_rate,
            notes=notes,
        )
        derived_term_years = (Decimal(str((maturity_date.date() - open_date.date()).days)) / Decimal("365")).quantize(Decimal("0.0001"))
        term_fragment = f"term_years_derived={derived_term_years}"
        if term_years is not None:
            term_fragment += f" term_years_input={term_years}"
        click.echo(
            "OK: "
            f"CDT recorded id={tx_id} "
            f"account={account_normalized} "
            f"symbol={symbol_normalized} "
            f"open_date={open_date_iso} "
            f"maturity_date={maturity_date_iso} "
            f"principal={format_money(principal)} "
            f"{term_fragment} "
            f"rate={annual_rate}"
        )
    except InvalidTransaction as exc:
        click.echo(f"ERROR: {exc}", err=True)
        raise click.exceptions.Exit(2)
    finally:
        db.close()


@main.command("add-fund-movement")
@click.option("--account", required=True)
@click.option("--symbol", required=True)
@click.option("--date", "movement_date", required=True, type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("--movement-type", required=True, type=click.Choice(["CONTRIBUTION", "WITHDRAWAL"], case_sensitive=False))
@click.option("--amount", required=True, callback=parse_decimal)
@click.option("--notes", default=None)
def cli_add_fund_movement(account, symbol, movement_date, movement_type, amount, notes):
    """Record one manual non-market fund contribution or withdrawal."""
    account_normalized = (account or "").strip()
    if not account_normalized:
        raise click.BadParameter("account cannot be empty", param_hint="account")

    symbol_normalized = (symbol or "").strip().upper()
    if not symbol_normalized:
        raise click.BadParameter("symbol cannot be empty", param_hint="symbol")

    if amount <= 0:
        raise click.BadParameter("amount must be > 0", param_hint="amount")

    movement_date_iso = movement_date.date().isoformat()
    movement_type_normalized = movement_type.strip().upper()

    db = ensure_db()
    resolver = AssetResolver(db)
    svc = TransactionService(db, resolver)

    try:
        tx_id = svc.record_fund_movement(
            account=account_normalized,
            symbol=symbol_normalized,
            movement_date=movement_date_iso,
            amount=amount,
            movement_type=movement_type_normalized,
            notes=notes,
        )
        click.echo(
            "OK: "
            f"FUND movement recorded id={tx_id} "
            f"account={account_normalized} "
            f"symbol={symbol_normalized} "
            f"date={movement_date_iso} "
            f"type={movement_type_normalized} "
            f"amount={format_money(amount)}"
        )
    except InvalidTransaction as exc:
        click.echo(f"ERROR: {exc}", err=True)
        raise click.exceptions.Exit(2)
    finally:
        db.close()


@main.command("buy")
@click.option("--symbol", required=True)
@click.option("--account", required=True)
@click.option("--qty", required=True, callback=parse_decimal)
@click.option("--price", required=True, callback=parse_decimal)
@click.option("--fee", default="0", callback=parse_decimal)
@click.option("--date", "tx_date", default=None)
@click.option("--notes", default=None)
def cli_buy(symbol, account, qty, price, fee, tx_date, notes):
    """Record a BUY transaction."""
    _record_transaction_from_cli("buy", symbol, account, qty, price, fee, tx_date, notes)


@main.command("sell")
@click.option("--symbol", required=True)
@click.option("--account", required=True)
@click.option("--qty", required=True, callback=parse_decimal)
@click.option("--price", required=True, callback=parse_decimal)
@click.option("--fee", default="0", callback=parse_decimal)
@click.option("--date", "tx_date", default=None)
@click.option("--notes", default=None)
def cli_sell(symbol, account, qty, price, fee, tx_date, notes):
    """Record a SELL transaction."""
    _record_transaction_from_cli("sell", symbol, account, qty, price, fee, tx_date, notes)


@main.command("delete-transaction")
@click.argument("tx_id", type=click.IntRange(min=1))
def cli_delete_transaction(tx_id):
    """Delete a transaction by id from the ledger source of truth."""
    db = ensure_db()
    resolver = AssetResolver(db)
    svc = TransactionService(db, resolver)
    try:
        deleted = svc.delete_transaction(tx_id)
        click.echo(f"OK: deleted transaction {deleted['id']} ({deleted['tx_type']})")
    except Exception as exc:
        click.echo(f"ERROR: {exc}", err=True)
        raise click.exceptions.Exit(2)
@main.command("list-transactions")
@click.option("--account", default=None)
@click.option("--symbol", default=None)
@click.option("--side", default=None, type=click.Choice(["BUY", "SELL", "MIGRATION_BUY"], case_sensitive=False))
@click.option("--tx-id", "tx_id", default=None, type=click.IntRange(min=1))
@click.option("--limit", default=50, show_default=True, type=click.IntRange(min=1))
@click.option("--date-from", "--from-date", "from_date", default=None, type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("--date-to", "--to-date", "to_date", default=None, type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("--output-csv", "output_csv", default=None)
def cli_list_transactions(account, symbol, side, tx_id, limit, from_date, to_date, output_csv):
    """List recorded ledger transactions ordered from newest to oldest."""
    if account is not None and not account.strip():
        raise click.BadParameter("account cannot be empty", param_hint="account")
    if symbol is not None and not symbol.strip():
        raise click.BadParameter("symbol cannot be empty", param_hint="symbol")
    if from_date and to_date and from_date.date() > to_date.date():
        raise click.BadParameter("date-from cannot be after date-to")

    db = ensure_db()
    resolver = AssetResolver(db)
    svc = TransactionService(db, resolver)

    rows = svc.list_transactions(
        account=account.strip() if account is not None else None,
        symbol=symbol.strip().upper() if symbol is not None else None,
        side=side.upper() if side else None,
        tx_id=tx_id,
        from_date=from_date.date().isoformat() if from_date else None,
        to_date=to_date.date().isoformat() if to_date else None,
        limit=limit,
    )

    csv_headers = [
        "id",
        "tx_date",
        "symbol",
        "side",
        "qty",
        "unit_price",
        "account",
        "gross_proceeds",
        "matched_cost_basis",
        "realized_pnl",
    ]
    csv_rows = [
        (
            str(tx["id"]),
            str(tx["tx_date"]),
            tx["symbol"],
            tx["side"],
            format_qty(Decimal(str(tx["quantity"]))),
            format_money(Decimal(str(tx["unit_price"]))),
            tx["account"],
            "" if tx["gross_proceeds"] is None else format_money(Decimal(str(tx["gross_proceeds"]))),
            "" if tx["matched_cost_basis"] is None else format_money(Decimal(str(tx["matched_cost_basis"]))),
            "" if tx["realized_pnl"] is None else format_money(Decimal(str(tx["realized_pnl"]))),
        )
        for tx in rows
    ]

    if output_csv is not None:
        _write_csv_output(output_csv, csv_headers, csv_rows)
        if output_csv.strip() != "-":
            click.echo(f"CSV exported to {output_csv.strip()} ({len(csv_rows)} row(s))")
        return

    if not rows:
        click.echo("No transactions found for the given filters.")
        return

    table_rows = []
    for tx in rows:
        table_rows.append(
            (
                str(tx["id"]),
                str(tx["tx_date"]),
                tx["account"],
                tx["symbol"],
                tx["side"],
                format_qty(Decimal(str(tx["quantity"]))),
                format_money(Decimal(str(tx["unit_price"]))),
                format_money(Decimal(str(tx["fee_usd"]))),
                format_money(Decimal(str(tx["total_usd"]))),
                _format_optional_money(tx["gross_proceeds"]),
                _format_optional_money(tx["matched_cost_basis"]),
                _format_optional_money(tx["realized_pnl"]),
            )
        )

    display_table(
        ["ID", "Date", "Account", "Symbol", "Side", "Qty", "Price", "Fee", "Total", "Proceeds", "Cost Basis", "Realized PnL"],
        table_rows,
    )
@main.command("list-open-lots")
@click.option("--account", default=None)
@click.option("--symbol", default=None)
@click.option("--output-csv", "output_csv", default=None)
def cli_list_open_lots(account, symbol, output_csv):
    """List open buy lots (remaining quantity > 0) for audit/inspection."""
    db = ensure_db()
    resolver = AssetResolver(db)
    svc = TransactionService(db, resolver)

    rows = svc.list_open_lots(account=account, symbol=symbol)

    csv_headers = [
        "buy_tx_id",
        "tx_date",
        "symbol",
        "account",
        "origin",
        "original_qty",
        "remaining_qty",
        "unit_cost",
        "remaining_cost_basis",
    ]
    csv_rows = [
        (
            str(lot["buy_tx_id"]),
            str(lot["tx_date"]),
            lot["symbol"],
            lot["account"],
            lot["origin"],
            format_qty(Decimal(str(lot["original_qty"]))),
            format_qty(Decimal(str(lot["remaining_qty"]))),
            format_money(Decimal(str(lot["unit_price"]))),
            format_money(Decimal(str(lot["remaining_cost_basis"]))),
        )
        for lot in rows
    ]

    if output_csv is not None:
        _write_csv_output(output_csv, csv_headers, csv_rows)
        if output_csv.strip() != "-":
            click.echo(f"CSV exported to {output_csv.strip()} ({len(csv_rows)} row(s))")
        return

    if not rows:
        click.echo("No open lots found for the given filters.")
        return

    table_rows = []
    for lot in rows:
        table_rows.append(
            (
                str(lot["buy_tx_id"]),
                str(lot["tx_date"]),
                lot["account"],
                lot["symbol"],
                lot["origin"],
                format_qty(Decimal(str(lot["original_qty"]))),
                format_qty(Decimal(str(lot["remaining_qty"]))),
                format_money(Decimal(str(lot["unit_price"]))),
                format_money(Decimal(str(lot["remaining_cost_basis"]))),
            )
        )

    display_table(
        ["BuyTxID", "Date", "Account", "Symbol", "Origin", "Original Qty", "Remaining Qty", "Unit Cost", "Remaining Basis"],
        table_rows,
    )


@main.command("inspect-lot-matches")
@click.option("--sell-tx-id", "sell_tx_id", default=None, type=click.IntRange(min=1))
@click.option("--buy-tx-id", "buy_tx_id", default=None, type=click.IntRange(min=1))
@click.option("--output-csv", "output_csv", default=None)
def cli_inspect_lot_matches(sell_tx_id, buy_tx_id, output_csv):
    """Inspect raw lot_matches consumption for a specific SELL or BUY lot."""
    if (sell_tx_id is None and buy_tx_id is None) or (sell_tx_id is not None and buy_tx_id is not None):
        click.echo("ERROR: provide exactly one of --sell-tx-id or --buy-tx-id", err=True)
        raise click.exceptions.Exit(2)

    if output_csv is not None and not output_csv.strip():
        raise click.BadParameter("output-csv cannot be empty", param_hint="output_csv")

    db = ensure_db()
    resolver = AssetResolver(db)
    svc = TransactionService(db, resolver)

    try:
        rows = svc.inspect_lot_matches(sell_tx_id=sell_tx_id, buy_tx_id=buy_tx_id)
    except Exception as exc:
        click.echo(f"ERROR: {exc}", err=True)
        raise click.exceptions.Exit(2)

    csv_headers = [
        "sell_tx_id",
        "buy_tx_id",
        "buy_tx_date",
        "symbol",
        "account",
        "matched_qty",
        "buy_unit_price",
        "matched_cost_basis",
        "sell_unit_price",
        "matched_proceeds",
        "matched_realized_pnl",
    ]
    csv_rows = [
        (
            str(m["sell_tx_id"]),
            str(m["buy_tx_id"]),
            str(m["buy_tx_date"]),
            m["symbol"],
            m["account"],
            format_qty(Decimal(str(m["matched_qty"]))),
            format_money(Decimal(str(m["buy_unit_price"]))),
            format_money(Decimal(str(m["matched_cost_basis"]))),
            format_money(Decimal(str(m["sell_unit_price"]))),
            format_money(Decimal(str(m["matched_proceeds"]))),
            format_money(Decimal(str(m["matched_realized_pnl"]))),
        )
        for m in rows
    ]

    if output_csv is not None:
        _write_csv_output(output_csv, csv_headers, csv_rows)
        if output_csv.strip() != "-":
            click.echo(f"CSV exported to {output_csv.strip()} ({len(csv_rows)} row(s))")
        return

    if not rows:
        if sell_tx_id is not None:
            click.echo(f"No lot matches found for sell_tx_id={sell_tx_id}.")
        else:
            click.echo(f"No lot matches found for buy_tx_id={buy_tx_id}.")
        return

    table_rows = []
    for m in rows:
        table_rows.append(
            (
                str(m["sell_tx_id"]),
                str(m["buy_tx_id"]),
                str(m["buy_tx_date"]),
                m["symbol"],
                m["account"],
                format_qty(Decimal(str(m["matched_qty"]))),
                format_money(Decimal(str(m["buy_unit_price"]))),
                format_money(Decimal(str(m["matched_cost_basis"]))),
                format_money(Decimal(str(m["sell_unit_price"]))),
                format_money(Decimal(str(m["matched_proceeds"]))),
                format_money(Decimal(str(m["matched_realized_pnl"]))),
            )
        )

    display_table(
        [
            "SellTxID",
            "BuyTxID",
            "Buy Date",
            "Symbol",
            "Account",
            "Matched Qty",
            "Buy Price",
            "Cost Basis",
            "Sell Price",
            "Proceeds",
            "Realized PnL",
        ],
        table_rows,
    )

@main.command("positions")
@click.option("--symbol", default=None)
@click.option("--account", default=None)
@click.option("--output-csv", "output_csv", default=None)
def cli_positions(symbol, account, output_csv):
    """Show open position quantities."""
    db = ensure_db()
    resolver = AssetResolver(db)
    svc = PnLService(db, resolver)
    # use positions() to gather richer data
    rows = []
    for p in svc.positions(account):
        if symbol and p['symbol'] != symbol:
            continue
        rows.append((
            p['symbol'],
            p['account'] or '(all)',
            format_qty(p['qty_open']),
            format_money(p['avg_cost']),
            format_money(p['cost_basis']),
            p['valuation_method'],
            p['valuation_status'],
            p['alert'],
        ))
    csv_headers = ["symbol", "account", "qty", "avg_cost", "cost_basis", "valuation_method", "valuation_status", "alert"]
    csv_rows = list(rows)

    if output_csv is not None:
        _write_csv_output(output_csv, csv_headers, csv_rows)
        if output_csv.strip() != "-":
            click.echo(f"CSV exported to {output_csv.strip()} ({len(csv_rows)} row(s))")
        return

    if rows:
        display_table(["Symbol", "Account", "Qty", "Avg Cost", "Cost Basis", "Val Method", "Val Status", "Alert"], rows)
    else:
        click.echo("No open positions found. Database may be empty. Run 'import-csv --execute' to import data.")


@main.command("summary")
@click.option("--account", default=None)
@click.option("--export-json", "export_json", default=None, type=click.Path(dir_okay=False), help="Write summary snapshot to JSON file.")
@click.option("--export-json-history", "export_json_history", default=None, type=click.Path(file_okay=False), help="Write timestamped summary snapshot JSON into a directory.")
@click.option("--output-json", "output_json", default=None, type=click.Path(dir_okay=False), help="Write structured summary JSON for automation (use - for stdout).")
def cli_summary(account, export_json, export_json_history, output_json):
    """Show portfolio summary (cost basis, realized PnL, cash, valuation with usable prices)."""
    db = ensure_db()
    resolver = AssetResolver(db)
    svc = PnLService(db, resolver)
    s = svc.summary(account)

    if output_json is not None and not output_json.strip():
        raise click.BadParameter("output-json cannot be empty", param_hint="output_json")

    output_json_mode = output_json is not None
    stdout_json_mode = output_json == "-"

    if not output_json_mode:
        if s['total_cost_basis'] == 0 and s['total_realized_pnl'] == 0 and s['cash_balance'] == 0:
            click.echo("No portfolio data found. Database may be empty. Run 'import-csv --execute' to import data.")
        else:
            _print_summary_block(s)

    if output_json_mode:
        structured_payload = _build_summary_output_json_payload(s)
        if stdout_json_mode:
            click.echo(json.dumps(structured_payload, indent=2))
        else:
            _write_summary_json_export(output_json, structured_payload)
            click.echo(f"Summary JSON exported to {output_json}")

    payload = None
    if export_json:
        export_dir = os.path.dirname(export_json)
        if export_dir:
            os.makedirs(export_dir, exist_ok=True)
        payload = _build_summary_export_payload(s)
        with open(export_json, 'w', encoding='utf-8') as f:
            json.dump(payload, f, indent=2)
        click.echo(f"Summary exported to {export_json}")

    if export_json_history:
        os.makedirs(export_json_history, exist_ok=True)
        history_path = _build_history_snapshot_path(export_json_history)
        if payload is None:
            payload = _build_summary_export_payload(s)
        with open(history_path, 'w', encoding='utf-8') as f:
            json.dump(payload, f, indent=2)
        click.echo(f"Summary history snapshot exported to {history_path}")


@main.command("prune-summary-history")
@click.option("--history-dir", default=DEFAULT_HISTORY_DIR, show_default=True, type=click.Path(file_okay=False))
@click.option("--keep-last", default=DEFAULT_KEEP_LAST_SUMMARY_SNAPSHOTS, show_default=True, type=click.IntRange(min=0))
@click.option("--dry-run", is_flag=True, help="Show which snapshots would be deleted without removing files.")
def cli_prune_summary_history(history_dir, keep_last, dry_run):
    """Prune old summary history snapshots, keeping only the most recent N valid files."""
    snapshots = _list_history_snapshot_paths(history_dir)
    to_delete = snapshots[:-keep_last] if keep_last > 0 else snapshots
    if not to_delete:
        click.echo(f"No summary history snapshots pruned. {len(snapshots)} valid snapshot(s), keep-last={keep_last}.")
        return

    if dry_run:
        click.echo(f"Dry run: would delete {len(to_delete)} summary history snapshot(s):")
    else:
        click.echo(f"Deleting {len(to_delete)} summary history snapshot(s):")

    for path in to_delete:
        click.echo(path)
        if not dry_run:
            os.remove(path)

    if dry_run:
        click.echo("Dry run complete. No files deleted.")
    else:
        click.echo(f"Pruned summary history to keep the latest {keep_last} valid snapshot(s).")


@main.command("validate-daily-report-json")
@click.argument("path", type=click.Path(dir_okay=False))
def cli_validate_daily_report_json(path):
    """Validate the minimal JSON contract emitted by daily-report."""
    try:
        _load_and_validate_daily_report(path)
        click.echo(f"OK: valid daily-report JSON ({path})")
    except Exception as exc:
        click.echo(f"ERROR: {exc}", err=True)
        raise click.exceptions.Exit(2)


@main.command("show-latest-daily-report")
@click.option("--path", "path", default=DEFAULT_LATEST_DAILY_REPORT_PATH, show_default=True, type=click.Path(dir_okay=False))
def cli_show_latest_daily_report(path):
    """Show a human summary of the latest daily-report JSON."""
    try:
        payload = _load_and_validate_daily_report(path)
        _render_daily_report_summary(payload)
    except Exception as exc:
        click.echo(f"ERROR: {exc}", err=True)
        raise click.exceptions.Exit(2)


@main.command("compare-summary-snapshots")
@click.argument("old_json", type=click.Path(exists=True, dir_okay=False))
@click.argument("new_json", type=click.Path(exists=True, dir_okay=False))
def cli_compare_summary_snapshots(old_json, new_json):
    """Compare two exported summary snapshots (old vs new)."""
    old = _load_snapshot(old_json)
    new = _load_snapshot(new_json)
    _render_snapshot_comparison(old, new)


@main.command("alert-summary-snapshots")
@click.argument("old_json", type=click.Path(exists=True, dir_okay=False))
@click.argument("new_json", type=click.Path(exists=True, dir_okay=False))
@click.option("--equity-drop-pct", default=DEFAULT_EQUITY_DROP_PCT, show_default=True, type=float, help="Alert when total_equity drop exceeds this percentage.")
@click.option("--unvalued-increase-threshold", default=DEFAULT_UNVALUED_INCREASE_THRESHOLD, show_default=True, type=float, help="Alert when unvalued_excluded_cost_basis increase is above this amount.")
@click.option("--asset-class-shift-pct", default=DEFAULT_ASSET_CLASS_SHIFT_PCT, show_default=True, type=float, help="Alert when asset class share shift exceeds this percentage points threshold.")
def cli_alert_summary_snapshots(old_json, new_json, equity_drop_pct, unvalued_increase_threshold, asset_class_shift_pct):
    """Evaluate simple operational alerts from two summary snapshots (old vs new)."""
    try:
        old = _load_snapshot(old_json)
        new = _load_snapshot(new_json)
        alerts = _render_snapshot_alerts(old, new, equity_drop_pct, unvalued_increase_threshold, asset_class_shift_pct)
        if alerts:
            raise click.exceptions.Exit(1)
    except click.exceptions.Exit:
        raise
    except Exception as exc:
        click.echo(f"ERROR: {exc}", err=True)
        raise click.exceptions.Exit(2)


@main.command("pnl")
@click.option("--symbol", default=None)
@click.option("--account", default=None)
def cli_pnl(symbol, account):
    """Show realized profit/loss."""
    db = ensure_db()
    resolver = AssetResolver(db)
    svc = PnLService(db, resolver)
    rows = []
    if symbol:
        pnl_val = svc.realized_pnl(symbol, account)
        rows.append((symbol, account or "(all)", pnl_val))
    else:
        for sym in load_symbols(db):
            pnl_val = svc.realized_pnl(sym, account)
            rows.append((sym, account or "(all)", pnl_val))
    display_table(["Symbol", "Account", "Realized PnL"], rows)


@main.command("daily-report")
@click.option("--account", default=None)
@click.option("--refresh-verbose", is_flag=True, help="Show verbose refresh output.")
@click.option("--history-dir", default=DEFAULT_HISTORY_DIR, show_default=True, type=click.Path(file_okay=False))
@click.option("--skip-refresh", is_flag=True, help="Skip refresh-prices and use current SQLite state.")
@click.option("--output-json", "output_json", default=None, type=click.Path(dir_okay=False), help="Write structured daily report JSON to file.")
@click.option("--output-json-history-dir", "output_json_history_dir", default=None, type=click.Path(file_okay=False), help="Write timestamped structured daily report JSON into a directory.")
def cli_daily_report(account, refresh_verbose, history_dir, skip_refresh, output_json, output_json_history_dir):
    """Run the normal daily operational flow in one command."""
    if output_json is not None and not output_json.strip():
        raise click.BadParameter("output-json cannot be empty", param_hint="output_json")
    stdout_json_mode = output_json == "-"
    try:
        db = ensure_db()
        resolver = AssetResolver(db)
        svc = PnLService(db, resolver)
        refresh_result = None
        compare_result = None
        alerts_result = None
        final_exit_code = 0
        output_payload = None
        run_timestamp = datetime.now(timezone.utc).isoformat()

        output_context = contextlib.nullcontext()
        if stdout_json_mode:
            output_context = contextlib.redirect_stdout(io.StringIO())

        with output_context:
            click.echo('Daily Report')

            click.echo('Refresh')
            if skip_refresh:
                click.echo('Refresh skipped (--skip-refresh)')
            else:
                report = refresh_prices(db)
                refresh_result = {
                    "updated": report.updated,
                    "skipped_unsupported": report.skipped_unsupported,
                    "skipped_unmapped": report.skipped_unmapped,
                    "failed_final": report.failed_final,
                    "results": [],
                }
                if report.results:
                    cursor = db.connect().cursor()
                    for result_row in report.results:
                        cursor.execute(
                            """
                            SELECT valuation_method, price_source, current_price, price_updated_at
                            FROM assets
                            WHERE id = ?
                            """,
                            (result_row.asset_id,),
                        )
                        row = cursor.fetchone()
                        refresh_result["results"].append(
                            {
                                "symbol": result_row.symbol,
                                "provider": result_row.provider,
                                "valuation_method": row[0] if row else None,
                                "outcome": result_row.status,
                                "reason": result_row.reason,
                                "price_source": row[1] if row else None,
                                "current_price": row[2] if row else None,
                                "price_updated_at": row[3] if row else None,
                            }
                        )
                _render_refresh_report(db, report, refresh_verbose)

            click.echo('Summary')
            positions = svc.positions(account)
            summary = svc.summary(account)
            if summary['total_cost_basis'] == 0 and summary['total_realized_pnl'] == 0 and summary['cash_balance'] == 0:
                click.echo("No portfolio data found. Database may be empty. Run 'import-csv --execute' to import data.")
            else:
                _print_daily_report_block(run_timestamp, summary, positions)

            payload = _build_summary_export_payload(summary)
            history_path = _write_summary_history_export(history_dir, payload)
            click.echo(f'Summary history snapshot exported to {history_path}')

            previous_path = _find_previous_history_snapshot(history_dir, history_path)
            if not previous_path:
                click.echo('Compare vs previous snapshot')
                click.echo('No previous history snapshot found; skipping compare and alerts.')
            else:
                previous = _load_snapshot(previous_path)
                current = _load_snapshot(history_path)

                click.echo('Compare vs previous snapshot')
                _render_snapshot_comparison(previous, current)
                compare_result = _build_snapshot_comparison_payload(previous, current)

                click.echo('Alerts')
                alerts = _render_snapshot_alerts(previous, current)
                alerts_result = {
                    "status": "ALERT" if alerts else "OK",
                    "count": len(alerts),
                    "alerts": alerts,
                }
                if alerts:
                    final_exit_code = 1

            counts = summary.get("price_quality_counts", {}) or {}
            warnings = []
            stale_count = int(counts.get("stale", 0) or 0)
            unavailable_count = int(counts.get("unavailable", 0) or 0)
            if stale_count:
                warnings.append(f"{stale_count} position(s) have stale prices")
            if unavailable_count:
                warnings.append(f"{unavailable_count} position(s) have unavailable prices")

            usable_non_market_assets = []
            for position in positions:
                if position.get("valuation_status") == "usable_non_market":
                    usable_non_market_assets.append(
                        {
                            "symbol": position.get("symbol"),
                            "account": position.get("account"),
                        }
                    )

            alert_items = []
            if alerts_result:
                alert_items = list(alerts_result.get("alerts") or [])

            status = "ALERT" if alert_items else ("WARN" if warnings else "OK")

            output_payload = {
                "report_type": "daily-report",
                "report_schema_version": 1,
                "run_timestamp": run_timestamp,
                "account": account,
                "history_dir": history_dir,
                "skip_refresh": skip_refresh,
                "refresh_verbose": refresh_verbose,
                "refresh_result": refresh_result,
                "summary_result": payload,
                "created_snapshot_path": history_path,
                "previous_snapshot_path": previous_path,
                "compare_result": compare_result,
                "alerts_result": alerts_result,
                "final_exit_code": final_exit_code,
                "prices": {
                    "updated": int((refresh_result or {}).get("updated", 0) or 0),
                    "skipped_unsupported": int((refresh_result or {}).get("skipped_unsupported", 0) or 0),
                    "skipped_unmapped": int((refresh_result or {}).get("skipped_unmapped", 0) or 0),
                    "failed_final": int((refresh_result or {}).get("failed_final", 0) or 0),
                },
                "valuation_summary": {
                    "usable_non_market_assets": usable_non_market_assets,
                    "warnings": warnings,
                    "alerts": alert_items,
                },
                "status": status,
            }

            if output_json and not stdout_json_mode:
                _write_summary_json_export(output_json, output_payload)
                click.echo(f'Structured daily report exported to {output_json}')

            if output_json_history_dir:
                os.makedirs(output_json_history_dir, exist_ok=True)
                history_report_path = _build_daily_report_history_path(output_json_history_dir)
                _write_summary_json_export(history_report_path, output_payload)
                click.echo(f'Structured daily report history snapshot exported to {history_report_path}')

        if stdout_json_mode:
            click.echo(json.dumps(output_payload, indent=2))

        if final_exit_code == 1:
            raise click.exceptions.Exit(1)
    except click.exceptions.Exit:
        raise
    except Exception as exc:
        click.echo(f"ERROR: {exc}", err=True)
        raise click.exceptions.Exit(2)


@main.command("refresh-prices")
@click.option("--verbose", is_flag=True, help="Show final outcome per symbol.")
def cli_refresh_prices(verbose):
    """Refresh current prices for active assets from external sources."""
    db = ensure_db()
    report = refresh_prices(db)
    _render_refresh_report(db, report, verbose)
