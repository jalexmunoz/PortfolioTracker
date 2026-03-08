import os
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Optional, List

import click

from portfolio_tracker_v2 import config
from portfolio_tracker_v2.core import Database
from portfolio_tracker_v2.core.asset_resolver import AssetResolver
from portfolio_tracker_v2.migration.csv_importer import CSVImporter
from portfolio_tracker_v2.services.transaction_svc import TransactionService
from portfolio_tracker_v2.services.pnl_svc import PnLService
from portfolio_tracker_v2.scripts import init_db as init_db_script


# helpers

def get_db_path() -> str:
    """Determine database path: env override or config value."""
    return os.environ.get("PORTFOLIO_DB_PATH", config.DB_PATH)


def parse_decimal(ctx, param, value):
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError):
        raise click.BadParameter(f"{param} must be a number, got '{value}'")


def format_money(value):
    """Format decimal as currency: 1234.56 -> 1,234.56"""
    return f"{value:,.2f}"


def format_qty(value):
    """Format quantity: trim trailing zeros, reasonable precision."""
    s = f"{value:.8f}"
    return s.rstrip('0').rstrip('.') if '.' in s else s


def ensure_db():
    path = get_db_path()
    db = Database(path)
    db.connect()
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
    if tx_date is None:
        tx_date = date.today().isoformat()
    db = ensure_db()
    resolver = AssetResolver(db)
    svc = TransactionService(db, resolver)
    svc.record_buy(symbol, account, qty, price, fee, tx_date, notes)
    click.echo("BUY recorded")


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
    if tx_date is None:
        tx_date = date.today().isoformat()
    db = ensure_db()
    resolver = AssetResolver(db)
    svc = TransactionService(db, resolver)
    svc.record_sell(symbol, account, qty, price, fee, tx_date, notes)
    click.echo("SELL recorded")


@main.command("positions")
@click.option("--symbol", default=None)
@click.option("--account", default=None)
def cli_positions(symbol, account):
    """Show open position quantities."""
    db = ensure_db()
    resolver = AssetResolver(db)
    svc = PnLService(db, resolver)
    # use positions() to gather richer data
    rows = []
    for p in svc.positions(account):
        if symbol and p['symbol'] != symbol:
            continue
        rows.append((p['symbol'], p['account'] or '(all)', format_qty(p['qty_open']), format_money(p['avg_cost']), format_money(p['cost_basis'])))
    if rows:
        display_table(["Symbol", "Account", "Qty", "Avg Cost", "Cost Basis"], rows)
    else:
        click.echo("No open positions found. Database may be empty. Run 'import-csv --execute' to import data.")


@main.command("summary")
@click.option("--account", default=None)
def cli_summary(account):
    """Show portfolio summary (cost basis, realized PnL, cash)."""
    db = ensure_db()
    resolver = AssetResolver(db)
    svc = PnLService(db, resolver)
    s = svc.summary(account)
    if s['total_cost_basis'] == 0 and s['total_realized_pnl'] == 0 and s['cash_balance'] == 0:
        click.echo("No portfolio data found. Database may be empty. Run 'import-csv --execute' to import data.")
    else:
        click.echo(f"Total cost basis: {format_money(s['total_cost_basis'])}")
        click.echo(f"Total realized PnL: {format_money(s['total_realized_pnl'])}")
        click.echo(f"Cash balance: {format_money(s['cash_balance'])}")


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
