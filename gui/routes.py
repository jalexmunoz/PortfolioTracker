import csv as _csv_mod
import io
import os
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation

from flask import Blueprint, current_app, flash, make_response, redirect, render_template, request, url_for

from .backup import create_auto_backup, create_backup
from .db_context import clear_active_db, load_active_db, set_active_db

bp = Blueprint("gui", __name__)


def _backup_root() -> str:
    return os.environ.get(
        "PORTFOLIO_GUI_BACKUP_DIR",
        os.path.join(os.path.dirname(current_app.root_path), "output", "gui_backups"),
    )


def _auto_backup_root() -> str:
    """Directory where automatic pre-write backups land.

    Defaults to <flask_instance>/backups so tests with isolated instance paths
    are auto-isolated. Override with PORTFOLIO_GUI_AUTO_BACKUP_DIR.
    """
    return os.environ.get(
        "PORTFOLIO_GUI_AUTO_BACKUP_DIR",
        os.path.join(current_app.instance_path, "backups"),
    )


def _backup_before_write(active, operation: str, redirect_endpoint: str):
    """Create an auto-backup of the active DB before a write.

    Returns (backup_path_or_none, abort_response_or_none).
    On success, abort_response is None and the caller may proceed; the path
    may be None if the source DB does not exist (nothing to back up).
    On failure, flashes an error and returns a redirect response the caller
    should return immediately to abort the write.
    """
    try:
        path = create_auto_backup(active.db_path, _auto_backup_root(), operation)
    except Exception as exc:
        flash(f"Backup failed; {operation} NOT executed: {exc}", "error")
        return None, _redirect_after_write(redirect_endpoint)
    return path, None


def _backup_suffix(backup_path) -> str:
    """Render a flash suffix like ' (backup: <path>)' or '' if no backup."""
    return f" (backup: {backup_path})" if backup_path else ""


def _require_active_db():
    """Return ActiveDbContext or None (flashes error if missing)."""
    active = load_active_db()
    if active is None:
        flash("No active DB selected. Go to Setup first.", "error")
    return active


def _get_db_and_services(active):
    """Instantiate Database, AssetResolver, TransactionService, PnLService from active context."""
    from portfolio_tracker_v2.core import Database
    from portfolio_tracker_v2.core.asset_resolver import AssetResolver
    from portfolio_tracker_v2.services.transaction_svc import TransactionService
    from portfolio_tracker_v2.services.pnl_svc import PnLService

    db = Database(active.db_path)
    resolver = AssetResolver(db)
    tx_svc = TransactionService(db, resolver)
    pnl_svc = PnLService(db, resolver)
    return db, resolver, tx_svc, pnl_svc


def _parse_decimal(raw, field_name):
    """Parse a string to Decimal, raise ValueError on failure."""
    raw = (raw or "").strip()
    if not raw:
        raise ValueError(f"{field_name} is required")
    try:
        return Decimal(raw)
    except InvalidOperation:
        raise ValueError(f"{field_name} must be a valid number, got '{raw}'")


def _format_money(value):
    if value is None:
        return "N/A"
    return f"{Decimal(str(value)):,.2f}"


def _format_qty(value):
    s = f"{Decimal(str(value)):.8f}"
    return s.rstrip('0').rstrip('.') if '.' in s else s


def _redirect_after_write(default_endpoint: str):
    """Honor a posted 'next_url' (must be relative) or fall back to a default route."""
    next_url = (request.form.get("next_url") or "").strip()
    if next_url and next_url.startswith("/") and not next_url.startswith("//"):
        return redirect(next_url)
    return redirect(url_for(default_endpoint))


def _get_last_price_refresh(db) -> str | None:
    """Return the most recent price_updated_at timestamp across all assets, or None."""
    cursor = db.connect().cursor()
    cursor.execute("SELECT MAX(price_updated_at) FROM assets WHERE price_updated_at IS NOT NULL")
    row = cursor.fetchone()
    return row[0] if row and row[0] else None


def _format_last_refresh(ts) -> str:
    if not ts:
        return "No refresh yet"
    try:
        parsed = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        return parsed.strftime("%Y-%m-%d %H:%M")
    except (ValueError, TypeError):
        return str(ts)


def _compute_total_pnl(summary: dict) -> Decimal:
    realized = Decimal(str(
        summary.get("displayed_realized_pnl")
        or summary.get("total_realized_pnl")
        or 0
    ))
    unrealized = Decimal(str(summary.get("total_unrealized_pnl", 0) or 0))
    return realized + unrealized


def _build_breakdown_rows(s):
    breakdown = s.get("asset_class_breakdown", {}) or {}
    total_equity = Decimal(str(s.get("total_equity", 0)))
    rows = []
    for asset_class in ["Crypto", "Equities", "Metals", "Non-market", "Cash"]:
        equity = Decimal(str(breakdown.get(asset_class, 0)))
        if total_equity > 0:
            pct = ((equity / total_equity) * Decimal("100")).quantize(Decimal("0.01"))
        else:
            pct = Decimal("0.00")
        rows.append({"asset_class": asset_class, "equity": equity, "pct": pct})
    return rows


# ---------------------------------------------------------------------------
# B52 — Confirmation/review helpers for write operations
# ---------------------------------------------------------------------------

_HIST_PNL_FIELDS = ("adjustment_date", "source", "amount_usd", "description")
_TRANSACTION_FIELDS = ("side", "account", "symbol", "tx_date", "qty", "price", "fee", "notes")
_CDT_FIELDS = (
    "account", "symbol", "open_date", "maturity_date", "principal", "currency",
    "fx_rate_at_open", "annual_rate", "term_years", "notes",
)
_FUND_FIELDS = (
    "account", "symbol", "movement_date", "movement_type", "amount",
    "currency", "fx_rate", "notes",
)
_CASH_FIELDS = ("account", "movement_date", "movement_type", "amount", "notes")


def _raw_payload(form, fields):
    return {field: form.get(field, "") for field in fields}


def _validate_hist_pnl_form(form):
    """Parse + validate Add Historical PnL Adjustment form. Returns dict or raises ValueError."""
    adjustment_date = form.get("adjustment_date", "").strip()
    source = form.get("source", "").strip()
    amount_str = form.get("amount_usd", "").strip()
    description = form.get("description", "").strip() or None

    if not adjustment_date:
        raise ValueError("Date is required")
    if not source:
        raise ValueError("Source is required")
    amount_usd = _parse_decimal(amount_str, "Amount USD")
    return {
        "adjustment_date": adjustment_date,
        "source": source,
        "amount_usd": amount_usd,
        "description": description,
    }


def _summary_hist_pnl(parsed):
    sign = "+" if parsed["amount_usd"] >= 0 else ""
    return [
        ("Date", parsed["adjustment_date"]),
        ("Source", parsed["source"]),
        ("Amount USD", f"{sign}{_format_money(parsed['amount_usd'])} USD"),
        ("Description", parsed["description"] or "—"),
        ("Affects cash", "No"),
        ("Affects open positions", "No"),
        ("Affects total equity", "No"),
        ("Affects realized PnL", "Yes"),
    ]


def _validate_transaction_form(form):
    """Parse + validate Add Transaction form. Returns dict or raises ValueError."""
    side = form.get("side", "").strip().upper()
    account = form.get("account", "").strip()
    symbol = form.get("symbol", "").strip().upper()
    tx_date = form.get("tx_date", "").strip()
    qty = _parse_decimal(form.get("qty"), "Qty")
    price = _parse_decimal(form.get("price"), "Unit Price")
    fee_raw = (form.get("fee", "0") or "0").strip()
    try:
        fee_usd = Decimal(fee_raw) if fee_raw else Decimal("0")
    except InvalidOperation:
        raise ValueError(f"Fee must be a valid number, got '{fee_raw}'")
    notes = form.get("notes", "").strip() or None

    if side not in ("BUY", "SELL"):
        raise ValueError("Side must be BUY or SELL")
    if not account:
        raise ValueError("Account is required")
    if not symbol:
        raise ValueError("Symbol is required")
    if qty <= 0:
        raise ValueError("Qty must be > 0")
    if price <= 0:
        raise ValueError("Unit Price must be > 0")
    if fee_usd < 0:
        raise ValueError("Fee cannot be negative")
    if not tx_date:
        raise ValueError("Transaction date is required")

    return {
        "side": side, "account": account, "symbol": symbol,
        "tx_date": tx_date, "qty": qty, "price": price,
        "fee_usd": fee_usd, "notes": notes,
    }


def _summary_transaction(parsed):
    qty = parsed["qty"]
    price = parsed["price"]
    fee = parsed["fee_usd"]
    side = parsed["side"]
    gross = qty * price
    if side == "BUY":
        cash_impact = -(gross + fee)
    else:
        cash_impact = gross - fee
    return [
        ("Date", parsed["tx_date"]),
        ("Account", parsed["account"]),
        ("Symbol", parsed["symbol"]),
        ("Side", side),
        ("Quantity", _format_qty(qty)),
        ("Unit Price", f"{_format_money(price)} USD"),
        ("Fee", f"{_format_money(fee)} USD"),
        ("Gross amount", f"{_format_money(gross)} USD"),
        ("Cash impact", f"{_format_money(cash_impact)} USD"),
        ("Notes", parsed["notes"] or "—"),
    ]


def _validate_cdt_form(form):
    account = form.get("account", "").strip()
    symbol = form.get("symbol", "BBVA CDT").strip().upper()
    open_date = form.get("open_date", "").strip()
    maturity_date = form.get("maturity_date", "").strip()
    principal = _parse_decimal(form.get("principal"), "Principal")
    annual_rate = _parse_decimal(form.get("annual_rate"), "Annual Rate")
    term_raw = form.get("term_years", "").strip()
    try:
        term_years = Decimal(term_raw) if term_raw else None
    except InvalidOperation:
        raise ValueError(f"Term in Years must be a valid number, got '{term_raw}'")
    notes = form.get("notes", "").strip() or None
    currency = (form.get("currency", "") or "").strip().upper()
    fx_raw = form.get("fx_rate_at_open", "").strip()
    try:
        fx_rate_at_open = Decimal(fx_raw) if fx_raw else None
    except InvalidOperation:
        raise ValueError(f"FX Rate at Open must be a valid number, got '{fx_raw}'")

    if not account:
        raise ValueError("Account is required")
    if not open_date:
        raise ValueError("Open date is required")
    if not maturity_date:
        raise ValueError("Maturity date is required")
    if principal <= 0:
        raise ValueError("Principal must be > 0")
    if annual_rate < 0:
        raise ValueError("Annual rate must be >= 0")
    if maturity_date <= open_date:
        raise ValueError("Maturity date must be after open date")
    if currency not in ("USD", "COP"):
        raise ValueError("Currency must be selected (USD or COP)")
    if currency != "USD" and fx_rate_at_open is None:
        raise ValueError(f"FX Rate at Open is required for {currency}")
    if fx_rate_at_open is not None and fx_rate_at_open <= 0:
        raise ValueError("FX Rate at Open must be > 0")

    return {
        "account": account, "symbol": symbol,
        "open_date": open_date, "maturity_date": maturity_date,
        "principal": principal, "annual_rate": annual_rate,
        "term_years": term_years, "notes": notes,
        "currency": currency, "fx_rate_at_open": fx_rate_at_open,
    }


def _summary_cdt(parsed):
    principal = parsed["principal"]
    fx = parsed["fx_rate_at_open"]
    if parsed["currency"] != "USD" and fx is not None:
        principal_usd = (principal / fx).quantize(Decimal("0.000001"))
    else:
        principal_usd = principal
    try:
        d_open = datetime.strptime(parsed["open_date"], "%Y-%m-%d").date()
        d_mat = datetime.strptime(parsed["maturity_date"], "%Y-%m-%d").date()
        derived_term = (
            Decimal(str((d_mat - d_open).days)) / Decimal("365")
        ).quantize(Decimal("0.00000001"))
    except ValueError:
        derived_term = None

    rows = [
        ("Account", parsed["account"]),
        ("Symbol", parsed["symbol"]),
        ("Open Date", parsed["open_date"]),
        ("Maturity Date", parsed["maturity_date"]),
        ("Principal", f"{_format_money(principal)} {parsed['currency']}"),
        ("Currency", parsed["currency"]),
    ]
    if parsed["currency"] != "USD":
        rows.append(("FX Rate at Open", f"{fx} {parsed['currency']} per USD"))
        rows.append(("Principal (USD)", f"{_format_money(principal_usd)} USD"))
    rows.append(("Annual Rate", str(parsed["annual_rate"])))
    if parsed["term_years"] is not None:
        rows.append(("Term Input (years)", str(parsed["term_years"])))
    if derived_term is not None:
        rows.append(("Term Derived (years)", str(derived_term)))
    rows.append(("Cash impact", f"{_format_money(-principal_usd)} USD"))
    rows.append(("Notes", parsed["notes"] or "—"))
    return rows


def _validate_fund_movement_form(form):
    account = form.get("account", "").strip()
    symbol = form.get("symbol", "").strip().upper()
    movement_date = form.get("movement_date", "").strip()
    movement_type = form.get("movement_type", "").strip().upper()
    amount = _parse_decimal(form.get("amount"), "Amount")
    notes = form.get("notes", "").strip() or None
    currency = (form.get("currency", "") or "").strip().upper()
    fx_raw = form.get("fx_rate", "").strip()
    try:
        fx_rate = Decimal(fx_raw) if fx_raw else None
    except InvalidOperation:
        raise ValueError(f"FX Rate must be a valid number, got '{fx_raw}'")

    if not account:
        raise ValueError("Account is required")
    if not symbol:
        raise ValueError("Symbol is required")
    if not movement_date:
        raise ValueError("Date is required")
    if movement_type not in ("CONTRIBUTION", "WITHDRAWAL"):
        raise ValueError("Movement type must be CONTRIBUTION or WITHDRAWAL")
    if amount <= 0:
        raise ValueError("Amount must be > 0")
    if currency not in ("USD", "COP"):
        raise ValueError("Currency must be selected (USD or COP)")
    if currency != "USD" and fx_rate is None:
        raise ValueError(f"FX Rate is required for {currency}")
    if fx_rate is not None and fx_rate <= 0:
        raise ValueError("FX Rate must be > 0")

    return {
        "account": account, "symbol": symbol,
        "movement_date": movement_date, "movement_type": movement_type,
        "amount": amount, "notes": notes,
        "currency": currency, "fx_rate": fx_rate,
    }


def _summary_fund_movement(parsed):
    amount = parsed["amount"]
    fx = parsed["fx_rate"]
    if parsed["currency"] != "USD" and fx is not None:
        amount_usd = (amount / fx).quantize(Decimal("0.000001"))
    else:
        amount_usd = amount
    cash_impact = (
        -amount_usd if parsed["movement_type"] == "CONTRIBUTION" else amount_usd
    )
    rows = [
        ("Account", parsed["account"]),
        ("Symbol", parsed["symbol"]),
        ("Date", parsed["movement_date"]),
        ("Movement Type", parsed["movement_type"]),
        ("Amount", f"{_format_money(amount)} {parsed['currency']}"),
        ("Currency", parsed["currency"]),
    ]
    if parsed["currency"] != "USD":
        rows.append(("FX Rate", f"{fx} {parsed['currency']} per USD"))
        rows.append(("Amount (USD)", f"{_format_money(amount_usd)} USD"))
    rows.append(("Cash impact", f"{_format_money(cash_impact)} USD"))
    rows.append(("Notes", parsed["notes"] or "—"))
    return rows


def _fund_balance_warning(active, parsed):
    """Read-only check: warn if WITHDRAWAL would exceed current fund balance.

    Returns None silently on any DB issue; the service still validates on save.
    """
    if parsed["movement_type"] != "WITHDRAWAL":
        return None
    if parsed["currency"] == "USD" or parsed["fx_rate"] is None:
        amount_usd = parsed["amount"]
    else:
        amount_usd = (parsed["amount"] / parsed["fx_rate"]).quantize(Decimal("0.000001"))
    try:
        from portfolio_tracker_v2.core import Database
        db = Database(active.db_path)
        try:
            cursor = db.connect().cursor()
            cursor.execute("SELECT id FROM assets WHERE symbol = ?", (parsed["symbol"],))
            asset_row = cursor.fetchone()
            if not asset_row:
                return None
            cursor.execute("SELECT id FROM accounts WHERE name = ?", (parsed["account"],))
            acc_row = cursor.fetchone()
            if not acc_row:
                return None
            cursor.execute(
                """
                SELECT
                    COALESCE(SUM(CASE WHEN tx_type IN ('BUY', 'MIGRATION_BUY') THEN total_usd ELSE 0 END), 0)
                  - COALESCE(SUM(CASE WHEN tx_type = 'SELL' THEN total_usd ELSE 0 END), 0)
                FROM transactions
                WHERE asset_id = ? AND account_id = ?
                """,
                (asset_row[0], acc_row[0]),
            )
            balance = Decimal(str(cursor.fetchone()[0] or 0))
        finally:
            db.close()
    except Exception:
        return None

    if amount_usd > balance:
        return (
            f"Withdrawal of {_format_money(amount_usd)} USD exceeds current fund "
            f"balance of {_format_money(balance)} USD. The save will be rejected."
        )
    return None


def _validate_cash_movement_form(form):
    account = form.get("account", "").strip()
    movement_date = form.get("movement_date", "").strip()
    movement_type = form.get("movement_type", "").strip().upper()
    amount = _parse_decimal(form.get("amount"), "Amount")
    notes = form.get("notes", "").strip() or None

    if not account:
        raise ValueError("Account is required")
    if not movement_date:
        raise ValueError("Date is required")
    if movement_type not in ("DEPOSIT", "WITHDRAWAL"):
        raise ValueError("Movement type must be DEPOSIT or WITHDRAWAL")
    if amount <= 0:
        raise ValueError("Amount must be > 0")

    return {
        "account": account, "movement_date": movement_date,
        "movement_type": movement_type, "amount": amount,
        "notes": notes,
    }


def _summary_cash_movement(parsed):
    amount = parsed["amount"]
    cash_impact = amount if parsed["movement_type"] == "DEPOSIT" else -amount
    return [
        ("Account", parsed["account"]),
        ("Date", parsed["movement_date"]),
        ("Movement Type", parsed["movement_type"]),
        ("Amount", f"{_format_money(amount)} USD"),
        ("Currency", "USD"),
        ("Cash impact", f"{_format_money(cash_impact)} USD"),
        ("Notes", parsed["notes"] or "—"),
    ]


# ---------------------------------------------------------------------------
# B54 — Portfolio Snapshot CSV Import helpers
# ---------------------------------------------------------------------------

_SNAPSHOT_SEED_NOTE = "Seeded from portfolio snapshot"
_SNAPSHOT_VALID_METHODS = {"market_live", "snapshot_imported", "contractual_value", "unvalued"}
_SNAPSHOT_REQUIRED_COLS = {"Symbol", "Account", "Qty", "Cost Basis"}
_SNAPSHOT_COL_ALIASES = {
    "Quantity": "Qty",
    "Wallet": "Account",
    "Total Cost": "Cost Basis",
    "Total Cost(USD)": "Cost Basis",
    "Total Cost (USD)": "Cost Basis",
    "Avg Cost(USD)": "Avg Cost",
    "Avg Cost (USD)": "Avg Cost",
}


def _parse_snapshot_csv(csv_path):
    """Parse a Portfolio Snapshot CSV. Returns (valid_rows, invalid_rows)."""
    valid = []
    invalid = []

    with open(csv_path, encoding="utf-8-sig", newline="") as fh:
        reader = _csv_mod.DictReader(fh)
        if reader.fieldnames is None:
            raise ValueError("CSV file is empty or has no header row")

        alias_map = {raw: _SNAPSHOT_COL_ALIASES.get((raw or "").strip(), (raw or "").strip())
                     for raw in reader.fieldnames}
        canonical_cols = set(alias_map.values())
        missing = _SNAPSHOT_REQUIRED_COLS - canonical_cols
        if missing:
            raise ValueError(f"Missing required columns: {', '.join(sorted(missing))}")

        for row_num, raw_row in enumerate(reader, start=2):
            row = {alias_map.get(k, k): v for k, v in raw_row.items()}
            parsed, error = _parse_snapshot_row(row, row_num)
            if error:
                invalid.append({"row": row_num, "error": error})
            else:
                valid.append(parsed)

    return valid, invalid


def _parse_snapshot_row(row, row_num):
    """Validate and parse one CSV row. Returns (dict, None) or (None, error_str)."""
    symbol = (row.get("Symbol") or "").strip().upper()
    if not symbol:
        return None, "Symbol is required"

    account = (row.get("Account") or "").strip()
    if not account:
        return None, "Account is required"

    qty_raw = (row.get("Qty") or "").strip().replace(",", "")
    if not qty_raw:
        return None, "Qty is required"
    try:
        qty = Decimal(qty_raw)
    except InvalidOperation:
        return None, f"Qty must be numeric, got '{qty_raw}'"
    if qty <= 0:
        return None, "Qty must be > 0"

    cost_raw = (row.get("Cost Basis") or "").strip().replace("$", "").replace(",", "")
    if not cost_raw:
        return None, "Cost Basis is required"
    try:
        cost_basis = Decimal(cost_raw)
    except InvalidOperation:
        return None, f"Cost Basis must be numeric, got '{cost_raw}'"
    if cost_basis <= 0:
        return None, "Cost Basis must be > 0"

    avg_raw = (row.get("Avg Cost") or "").strip().replace("$", "").replace(",", "")
    if avg_raw:
        try:
            avg_cost = Decimal(avg_raw)
        except InvalidOperation:
            return None, f"Avg Cost must be numeric, got '{avg_raw}'"
        if avg_cost <= 0:
            return None, "Avg Cost must be > 0"
    else:
        avg_cost = (cost_basis / qty).quantize(Decimal("0.00000001"))

    method_raw = (row.get("Method") or "").strip()
    method = method_raw if method_raw else "market_live"
    if method not in _SNAPSHOT_VALID_METHODS:
        return None, (
            f"Unknown Method '{method}'; must be one of: "
            f"{', '.join(sorted(_SNAPSHOT_VALID_METHODS))}"
        )

    return {
        "row_num": row_num,
        "symbol": symbol,
        "account": account,
        "qty": qty,
        "cost_basis": cost_basis,
        "avg_cost": avg_cost,
        "method": method,
    }, None


def _snapshot_preview(valid_rows, invalid_rows):
    total_cost_basis = sum((r["cost_basis"] for r in valid_rows), Decimal("0"))
    method_counts = {}
    for r in valid_rows:
        method_counts[r["method"]] = method_counts.get(r["method"], 0) + 1
    return {
        "total": len(valid_rows) + len(invalid_rows),
        "valid_count": len(valid_rows),
        "invalid_count": len(invalid_rows),
        "total_cost_basis": total_cost_basis,
        "method_counts": method_counts,
        "errors": invalid_rows,
    }


def _write_snapshot_rows(active, valid_rows, seed_date):
    """Write valid snapshot rows atomically. Raises on any error."""
    from portfolio_tracker_v2.core import Database
    from portfolio_tracker_v2.core.asset_resolver import AssetResolver

    db = Database(active.db_path)
    resolver = AssetResolver(db)
    conn = db.connect()
    cursor = conn.cursor()
    cursor.execute("BEGIN")
    try:
        for i, row in enumerate(valid_rows, start=1):
            asset = resolver.resolve(row["symbol"])
            asset_id = asset["id"]

            cursor.execute("SELECT id FROM accounts WHERE name = ?", (row["account"],))
            acc = cursor.fetchone()
            if acc:
                account_id = acc[0]
            else:
                cursor.execute("INSERT INTO accounts (name) VALUES (?)", (row["account"],))
                account_id = cursor.lastrowid

            notes = f"{_SNAPSHOT_SEED_NOTE} (row {row['row_num']})"
            cursor.execute(
                """
                INSERT INTO transactions
                  (asset_id, account_id, tx_type, quantity, unit_price,
                   fee_usd, total_usd, tx_date, sort_order, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (asset_id, account_id, "MIGRATION_BUY",
                 float(row["qty"]), float(row["avg_cost"]),
                 0.0, float(row["cost_basis"]),
                 seed_date, i, notes),
            )

            method = row["method"]
            if method == "snapshot_imported":
                cursor.execute(
                    """UPDATE assets
                       SET valuation_method = ?, current_price = ?,
                           price_source = ?, price_updated_at = ?
                       WHERE id = ?""",
                    ("snapshot_imported", float(row["avg_cost"]),
                     "snapshot_imported", seed_date, asset_id),
                )
            elif method == "contractual_value":
                cursor.execute(
                    "UPDATE assets SET valuation_method = ? WHERE id = ?",
                    ("contractual_value", asset_id),
                )
            elif method == "unvalued":
                cursor.execute(
                    "UPDATE assets SET valuation_method = ? WHERE id = ?",
                    ("unvalued", asset_id),
                )
            else:  # market_live — bootstrap price if not already set
                cursor.execute(
                    """UPDATE assets
                       SET current_price = COALESCE(current_price, ?),
                           price_source = COALESCE(price_source, ?),
                           price_updated_at = COALESCE(price_updated_at, ?)
                       WHERE id = ?""",
                    (float(row["avg_cost"]), "csv_bootstrap", seed_date, asset_id),
                )

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        db.close()


# ---------------------------------------------------------------------------
# H3 — Dashboard (home)
# ---------------------------------------------------------------------------

@bp.get("/")
def dashboard():
    active = load_active_db()
    if active is None:
        return render_template("dashboard.html", active=None, active_section="dashboard")

    summary = None
    positions_list = []
    breakdown_rows = []
    top_positions = []
    pnl_positions = []
    wallet_rows = []
    counts = {}
    warnings = []
    load_error = None
    last_refresh_display = "No refresh yet"
    total_pnl = Decimal("0")

    try:
        db, _, tx_svc, pnl_svc = _get_db_and_services(active)
        try:
            positions_list = pnl_svc.positions()
            summary = pnl_svc.summary()
            tx_count = len(tx_svc.list_transactions(limit=10000))
            last_refresh_display = _format_last_refresh(_get_last_price_refresh(db))
        finally:
            db.close()

        breakdown_rows = _build_breakdown_rows(summary)
        top_positions = sorted(
            [p for p in positions_list if p.get("approved_value") is not None and p["qty_open"] > 0],
            key=lambda p: Decimal(str(p["approved_value"])),
            reverse=True,
        )[:10]

        # B59A: P&L by Position — top 10 by magnitude of unrealized P&L
        valued = [
            p for p in positions_list
            if p.get("approved_value") is not None and p["qty_open"] > 0
        ]
        raw_pnl = []
        for p in valued:
            upnl = Decimal(str(p["approved_value"])) - Decimal(str(p["cost_basis"]))
            raw_pnl.append({
                "symbol": p["symbol"],
                "account": p["account"],
                "unrealized_pnl": upnl,
            })
        raw_pnl.sort(key=lambda x: abs(x["unrealized_pnl"]), reverse=True)
        pnl_positions = raw_pnl[:10]
        pnl_max = max(
            (abs(x["unrealized_pnl"]) for x in pnl_positions),
            default=Decimal("1"),
        ) or Decimal("1")  # guard: all break-even → avoid 0/0
        for row in pnl_positions:
            row["bar_half_pct"] = round(float(abs(row["unrealized_pnl"]) / pnl_max * 50), 1)

        # B59A: Wallet Distribution — group positions by account
        wallet_totals: dict = {}
        for p in valued:
            acc = p["account"] or "Unknown"
            wallet_totals[acc] = wallet_totals.get(acc, Decimal("0")) + Decimal(str(p["approved_value"]))
        wallet_total = sum(wallet_totals.values(), Decimal("0"))
        wallet_rows = sorted(
            [
                {
                    "account": k,
                    "value": v,
                    "pct": ((v / wallet_total) * 100).quantize(Decimal("0.01"))
                    if wallet_total > 0 else Decimal("0"),
                }
                for k, v in wallet_totals.items()
            ],
            key=lambda x: x["value"],
            reverse=True,
        )

        counts = summary.get("price_quality_counts", {}) or {}
        stale = int(counts.get("stale", 0) or 0)
        unavailable = int(counts.get("unavailable", 0) or 0)
        unvalued = int(summary.get("unvalued_positions", 0) or 0)
        total_pnl = _compute_total_pnl(summary)
        if stale:
            warnings.append(f"{stale} position(s) have stale prices.")
        if unavailable:
            warnings.append(f"{unavailable} position(s) have unavailable prices.")
        if unvalued:
            warnings.append(f"{unvalued} position(s) have no approved valuation and are excluded from total equity.")
    except Exception as exc:
        load_error = str(exc)
        tx_count = 0

    return render_template(
        "dashboard.html",
        active=active,
        active_section="dashboard",
        summary=summary,
        positions_count=len([p for p in positions_list if p.get("qty_open", 0) > 0]),
        tx_count=tx_count,
        top_positions=top_positions,
        breakdown_rows=breakdown_rows,
        pnl_positions=pnl_positions,
        wallet_rows=wallet_rows,
        counts=counts,
        warnings=warnings,
        load_error=load_error,
        last_refresh_display=last_refresh_display,
        total_pnl=total_pnl,
        format_money=_format_money,
        format_qty=_format_qty,
    )


# ---------------------------------------------------------------------------
# H3 — Hubs
# ---------------------------------------------------------------------------

@bp.get("/operations")
def operations():
    active = load_active_db()
    return render_template(
        "operations.html",
        active=active,
        active_section="operations",
        next_url=url_for("gui.operations"),
    )


@bp.get("/setup")
def setup():
    active = load_active_db()
    return render_template(
        "setup.html",
        active=active,
        active_section="setup",
        backup_root=_backup_root(),
        next_url=url_for("gui.setup"),
    )


@bp.get("/reports")
def reports():
    active = load_active_db()
    if active is None:
        return render_template(
            "reports.html",
            active=None,
            active_section="reports",
        )

    try:
        db, _, _, pnl_svc = _get_db_and_services(active)
        try:
            positions_list = pnl_svc.positions()
            s = pnl_svc.summary()
            last_refresh_display = _format_last_refresh(_get_last_price_refresh(db))
        finally:
            db.close()
        breakdown_rows = _build_breakdown_rows(s)
        counts = s.get("price_quality_counts", {}) or {}
        total_pnl = _compute_total_pnl(s)
    except Exception as exc:
        return render_template(
            "reports.html",
            active=active,
            active_section="reports",
            load_error=str(exc),
        )

    return render_template(
        "reports.html",
        active=active,
        active_section="reports",
        s=s,
        positions=positions_list,
        breakdown_rows=breakdown_rows,
        counts=counts,
        last_refresh_display=last_refresh_display,
        total_pnl=total_pnl,
        format_money=_format_money,
        format_qty=_format_qty,
    )


# ---------------------------------------------------------------------------
# DB context / backup
# ---------------------------------------------------------------------------

@bp.route("/db-context", methods=["GET", "POST"])
def db_context():
    if request.method == "POST":
        db_path = request.form.get("db_path", "")
        mode = request.form.get("mode", "")
        try:
            context = set_active_db(db_path=db_path, mode=mode)
            flash(
                f"Active DB set: {context.db_path} [{context.mode}]",
                "success",
            )
            return _redirect_after_write("gui.db_context")
        except ValueError as exc:
            flash(str(exc), "error")

    return render_template(
        "db_context.html",
        active_section="setup",
        next_url=request.args.get("next_url", ""),
    )


@bp.post("/db-context/clear")
def db_context_clear():
    clear_active_db()
    flash("Active DB cleared. No database is currently selected.", "success")
    return _redirect_after_write("gui.db_context")


@bp.route("/backup", methods=["GET", "POST"])
def backup():
    if request.method == "POST":
        active = load_active_db()
        if active is None:
            flash("Cannot create backup: no active DB selected.", "error")
            return _redirect_after_write("gui.backup")

        try:
            backup_path = create_backup(active.db_path, _backup_root())
            flash(f"Backup created: {backup_path}", "success")
        except Exception as exc:
            flash(f"Backup failed: {exc}", "error")

        return _redirect_after_write("gui.backup")

    return render_template(
        "backup.html",
        active_section="setup",
        backup_root=_backup_root(),
        next_url=request.args.get("next_url", ""),
    )


# ---------------------------------------------------------------------------
# Setup write routes
# ---------------------------------------------------------------------------

@bp.route("/init-db", methods=["GET", "POST"])
def init_db():
    active = _require_active_db()
    if active is None:
        return redirect(url_for("gui.setup"))

    if request.method == "POST":
        backup_path, abort = _backup_before_write(active, "init_db", "gui.init_db")
        if abort is not None:
            return abort

        try:
            from portfolio_tracker_v2.core import Database
            from portfolio_tracker_v2.core.asset_resolver import AssetResolver

            db = Database(active.db_path)
            db.connect()
            db.init_schema()
            resolver = AssetResolver(db)
            resolver.get_or_create_usd_cash()
            db.close()
            flash(
                f"Database initialized at {active.db_path}{_backup_suffix(backup_path)}",
                "success",
            )
        except Exception as exc:
            flash(f"Init failed: {exc}", "error")
        return _redirect_after_write("gui.init_db")

    return render_template(
        "init_db.html",
        active=active,
        active_section="setup",
        next_url=request.args.get("next_url", ""),
    )


@bp.route("/import-legacy", methods=["GET", "POST"])
def import_legacy():
    active = _require_active_db()
    if active is None:
        return redirect(url_for("gui.setup"))

    if request.method == "POST":
        csv_path = request.form.get("csv_path", "").strip()
        seed_date = request.form.get("seed_date", "").strip()
        dry_run = request.form.get("dry_run") == "1"

        if not csv_path:
            flash("CSV path is required.", "error")
            return _redirect_after_write("gui.import_legacy")
        if not seed_date:
            flash("Seed date is required.", "error")
            return _redirect_after_write("gui.import_legacy")
        if not os.path.isfile(csv_path):
            flash(f"File not found: {csv_path}", "error")
            return _redirect_after_write("gui.import_legacy")

        backup_path = None
        if not dry_run:
            backup_path, abort = _backup_before_write(
                active, "import_legacy", "gui.import_legacy",
            )
            if abort is not None:
                return abort

        try:
            from portfolio_tracker_v2.core import Database
            from portfolio_tracker_v2.core.asset_resolver import AssetResolver
            from portfolio_tracker_v2.services.legacy_seed_importer import (
                LegacyPositionsCsvImporter,
            )

            db = Database(active.db_path)
            resolver = AssetResolver(db)
            importer = LegacyPositionsCsvImporter(db, resolver)
            result = importer.import_file(csv_path, seed_date=seed_date, dry_run=dry_run)
            db.close()

            mode_label = "DRY RUN" if dry_run else "EXECUTED"
            rejected_count = len(result.rejected_rows)
            flash(
                f"[{mode_label}] {result.total_rows} row(s) processed — "
                f"{result.imported_rows} {'would seed' if dry_run else 'seeded'} OK, "
                f"{rejected_count} rejected."
                f"{_backup_suffix(backup_path)}",
                "success" if not rejected_count else "warning",
            )
        except Exception as exc:
            flash(f"Import failed: {exc}", "error")

        return _redirect_after_write("gui.import_legacy")

    return render_template(
        "import_legacy.html",
        active=active,
        active_section="setup",
        next_url=request.args.get("next_url", ""),
    )


@bp.route("/import-portfolio-snapshot", methods=["GET", "POST"])
def import_portfolio_snapshot():
    active = _require_active_db()
    if active is None:
        return redirect(url_for("gui.setup"))

    if request.method == "POST":
        confirmed = request.form.get("confirmed") == "1"
        csv_path = request.form.get("csv_path", "").strip()
        seed_date = request.form.get("seed_date", "").strip()
        next_url_val = request.form.get("next_url", "")

        if not csv_path:
            flash("CSV path is required.", "error")
            return redirect(url_for("gui.import_portfolio_snapshot"))
        if not seed_date:
            flash("Seed date is required.", "error")
            return redirect(url_for("gui.import_portfolio_snapshot"))
        if not os.path.isfile(csv_path):
            flash(f"File not found: {csv_path}", "error")
            return redirect(url_for("gui.import_portfolio_snapshot"))

        try:
            valid_rows, invalid_rows = _parse_snapshot_csv(csv_path)
        except Exception as exc:
            flash(f"CSV parse error: {exc}", "error")
            return redirect(url_for("gui.import_portfolio_snapshot"))

        if not confirmed:
            preview = _snapshot_preview(valid_rows, invalid_rows)
            return render_template(
                "import_portfolio_snapshot_preview.html",
                active=active,
                active_section="setup",
                preview=preview,
                csv_path=csv_path,
                seed_date=seed_date,
                next_url=next_url_val,
                format_money=_format_money,
            )

        # confirmed=1 path
        if invalid_rows:
            flash(
                f"{len(invalid_rows)} invalid row(s) in CSV. Fix all errors and try again.",
                "error",
            )
            return redirect(url_for("gui.import_portfolio_snapshot"))

        from portfolio_tracker_v2.core import Database as _Db
        _chk = _Db(active.db_path)
        try:
            _cur = _chk.connect().cursor()
            _cur.execute("SELECT COUNT(*) FROM transactions")
            tx_count = _cur.fetchone()[0]
        finally:
            _chk.close()

        if tx_count > 0:
            flash(
                f"DB already has {tx_count} transaction(s). "
                "Snapshot import is only allowed on an empty database.",
                "error",
            )
            return redirect(url_for("gui.import_portfolio_snapshot"))

        backup_path, abort = _backup_before_write(
            active, "import_portfolio_snapshot", "gui.import_portfolio_snapshot",
        )
        if abort is not None:
            return abort

        try:
            _write_snapshot_rows(active, valid_rows, seed_date)
            flash(
                f"OK: {len(valid_rows)} row(s) seeded from portfolio snapshot."
                f"{_backup_suffix(backup_path)}",
                "success",
            )
        except Exception as exc:
            flash(f"Import failed: {exc}", "error")

        return _redirect_after_write("gui.import_portfolio_snapshot")

    return render_template(
        "import_portfolio_snapshot.html",
        active=active,
        active_section="setup",
        next_url=request.args.get("next_url", ""),
    )


# ---------------------------------------------------------------------------
# Operations write routes
# ---------------------------------------------------------------------------

@bp.route("/add-transaction", methods=["GET", "POST"])
def add_transaction():
    active = _require_active_db()
    if active is None:
        return redirect(url_for("gui.setup"))

    if request.method == "POST":
        confirmed = request.form.get("confirmed") == "1"
        review_mode = request.form.get("review") == "1" and not confirmed

        try:
            parsed = _validate_transaction_form(request.form)

            if review_mode:
                return render_template(
                    "review_operation.html",
                    active=active,
                    active_section="operations",
                    title="Review Transaction",
                    summary=_summary_transaction(parsed),
                    raw_payload=_raw_payload(request.form, _TRANSACTION_FIELDS),
                    endpoint_url=url_for("gui.add_transaction"),
                    next_url=request.form.get("next_url", ""),
                    warning=None,
                )

            backup_path, abort = _backup_before_write(
                active, "add_transaction", "gui.add_transaction",
            )
            if abort is not None:
                return abort

            db, _resolver, tx_svc, _pnl = _get_db_and_services(active)
            from portfolio_tracker_v2.core.exceptions import InvalidTransaction

            try:
                if parsed["side"] == "BUY":
                    tx_id = tx_svc.record_buy(
                        symbol=parsed["symbol"], account=parsed["account"],
                        qty=parsed["qty"], unit_price=parsed["price"],
                        fee_usd=parsed["fee_usd"], tx_date=parsed["tx_date"],
                        notes=parsed["notes"],
                    )
                else:
                    tx_id = tx_svc.record_sell(
                        symbol=parsed["symbol"], account=parsed["account"],
                        qty=parsed["qty"], unit_price=parsed["price"],
                        fee_usd=parsed["fee_usd"], tx_date=parsed["tx_date"],
                        notes=parsed["notes"],
                    )
                flash(
                    f"OK: {parsed['side']} recorded, tx_id={tx_id}"
                    f"{_backup_suffix(backup_path)}",
                    "success",
                )
            except InvalidTransaction as exc:
                flash(f"Transaction error: {exc}", "error")
            finally:
                db.close()

        except ValueError as exc:
            flash(str(exc), "error")

        return _redirect_after_write("gui.add_transaction")

    return render_template(
        "add_transaction.html",
        active=active,
        active_section="operations",
        next_url=request.args.get("next_url", ""),
    )


@bp.route("/add-cdt", methods=["GET", "POST"])
def add_cdt():
    active = _require_active_db()
    if active is None:
        return redirect(url_for("gui.setup"))

    if request.method == "POST":
        confirmed = request.form.get("confirmed") == "1"
        review_mode = request.form.get("review") == "1" and not confirmed

        try:
            parsed = _validate_cdt_form(request.form)

            if review_mode:
                return render_template(
                    "review_operation.html",
                    active=active,
                    active_section="operations",
                    title="Review CDT Contract",
                    summary=_summary_cdt(parsed),
                    raw_payload=_raw_payload(request.form, _CDT_FIELDS),
                    endpoint_url=url_for("gui.add_cdt"),
                    next_url=request.form.get("next_url", ""),
                    warning=None,
                )

            backup_path, abort = _backup_before_write(active, "add_cdt", "gui.add_cdt")
            if abort is not None:
                return abort

            db, _resolver, tx_svc, _pnl = _get_db_and_services(active)
            from portfolio_tracker_v2.core.exceptions import InvalidTransaction

            try:
                tx_id = tx_svc.record_cdt(
                    account=parsed["account"],
                    symbol=parsed["symbol"],
                    open_date=parsed["open_date"],
                    maturity_date=parsed["maturity_date"],
                    principal=parsed["principal"],
                    term_years=parsed["term_years"],
                    annual_rate=parsed["annual_rate"],
                    notes=parsed["notes"],
                    currency=parsed["currency"],
                    fx_rate_at_open=parsed["fx_rate_at_open"],
                )
                detail = f"principal={_format_money(parsed['principal'])} {parsed['currency']}"
                if parsed["currency"] != "USD":
                    principal_usd = (
                        parsed["principal"] / parsed["fx_rate_at_open"]
                    ).quantize(Decimal("0.000001"))
                    detail += (
                        f" (fx={parsed['fx_rate_at_open']} → "
                        f"{_format_money(principal_usd)} USD)"
                    )
                flash(
                    f"OK: CDT recorded, tx_id={tx_id}, {detail}, "
                    f"rate={parsed['annual_rate']}{_backup_suffix(backup_path)}",
                    "success",
                )
            except InvalidTransaction as exc:
                flash(f"CDT error: {exc}", "error")
            finally:
                db.close()

        except ValueError as exc:
            flash(str(exc), "error")

        return _redirect_after_write("gui.add_cdt")

    return render_template(
        "add_cdt.html",
        active=active,
        active_section="operations",
        next_url=request.args.get("next_url", ""),
    )


@bp.route("/add-cash-movement", methods=["GET", "POST"])
def add_cash_movement():
    active = _require_active_db()
    if active is None:
        return redirect(url_for("gui.setup"))

    if request.method == "POST":
        confirmed = request.form.get("confirmed") == "1"
        review_mode = request.form.get("review") == "1" and not confirmed

        try:
            parsed = _validate_cash_movement_form(request.form)

            if review_mode:
                return render_template(
                    "review_operation.html",
                    active=active,
                    active_section="operations",
                    title="Review Cash Movement",
                    summary=_summary_cash_movement(parsed),
                    raw_payload=_raw_payload(request.form, _CASH_FIELDS),
                    endpoint_url=url_for("gui.add_cash_movement"),
                    next_url=request.form.get("next_url", ""),
                    warning=None,
                )

            backup_path, abort = _backup_before_write(
                active, "add_cash_movement", "gui.add_cash_movement",
            )
            if abort is not None:
                return abort

            db, _resolver, tx_svc, _pnl = _get_db_and_services(active)
            from portfolio_tracker_v2.core.exceptions import InvalidTransaction

            try:
                tx_id = tx_svc.record_cash_movement(
                    account=parsed["account"],
                    movement_date=parsed["movement_date"],
                    amount=parsed["amount"],
                    movement_type=parsed["movement_type"],
                    notes=parsed["notes"],
                )
                flash(
                    f"OK: {parsed['movement_type']} recorded, tx_id={tx_id}, "
                    f"amount={_format_money(parsed['amount'])}"
                    f"{_backup_suffix(backup_path)}",
                    "success",
                )
            except InvalidTransaction as exc:
                flash(f"Cash movement error: {exc}", "error")
            finally:
                db.close()

        except ValueError as exc:
            flash(str(exc), "error")

        return _redirect_after_write("gui.add_cash_movement")

    return render_template(
        "add_cash_movement.html",
        active=active,
        active_section="operations",
        next_url=request.args.get("next_url", ""),
    )


@bp.post("/refresh-prices")
def refresh_prices_route():
    active = _require_active_db()
    if active is None:
        return redirect(url_for("gui.setup"))

    backup_path, abort = _backup_before_write(
        active, "refresh_prices", "gui.dashboard",
    )
    if abort is not None:
        return abort

    try:
        from portfolio_tracker_v2.services.price_svc import refresh_prices

        db = None
        try:
            from portfolio_tracker_v2.core import Database

            db = Database(active.db_path)
            report = refresh_prices(db)
        finally:
            if db is not None:
                db.close()

        flash(
            "Prices refreshed: "
            f"{report.updated} updated, "
            f"{report.skipped_unsupported} skipped unsupported, "
            f"{report.skipped_unmapped} skipped unmapped, "
            f"{report.failed_final} failed final."
            f"{_backup_suffix(backup_path)}",
            "success" if report.failed_final == 0 else "warning",
        )
    except Exception as exc:
        flash(f"Refresh failed: {exc}", "error")

    return _redirect_after_write("gui.dashboard")


@bp.route("/add-fund-movement", methods=["GET", "POST"])
def add_fund_movement():
    active = _require_active_db()
    if active is None:
        return redirect(url_for("gui.setup"))

    if request.method == "POST":
        confirmed = request.form.get("confirmed") == "1"
        review_mode = request.form.get("review") == "1" and not confirmed

        try:
            parsed = _validate_fund_movement_form(request.form)

            if review_mode:
                return render_template(
                    "review_operation.html",
                    active=active,
                    active_section="operations",
                    title="Review Fund Movement",
                    summary=_summary_fund_movement(parsed),
                    raw_payload=_raw_payload(request.form, _FUND_FIELDS),
                    endpoint_url=url_for("gui.add_fund_movement"),
                    next_url=request.form.get("next_url", ""),
                    warning=_fund_balance_warning(active, parsed),
                )

            backup_path, abort = _backup_before_write(
                active, "add_fund_movement", "gui.add_fund_movement",
            )
            if abort is not None:
                return abort

            db, _resolver, tx_svc, _pnl = _get_db_and_services(active)
            from portfolio_tracker_v2.core.exceptions import InvalidTransaction

            try:
                tx_id = tx_svc.record_fund_movement(
                    account=parsed["account"],
                    symbol=parsed["symbol"],
                    movement_date=parsed["movement_date"],
                    amount=parsed["amount"],
                    movement_type=parsed["movement_type"],
                    notes=parsed["notes"],
                    currency=parsed["currency"],
                    fx_rate=parsed["fx_rate"],
                )
                detail = f"amount={_format_money(parsed['amount'])} {parsed['currency']}"
                if parsed["currency"] != "USD":
                    amount_usd = (
                        parsed["amount"] / parsed["fx_rate"]
                    ).quantize(Decimal("0.000001"))
                    detail += (
                        f" (fx={parsed['fx_rate']} → "
                        f"{_format_money(amount_usd)} USD)"
                    )
                flash(
                    f"OK: {parsed['movement_type']} recorded, tx_id={tx_id}, "
                    f"{detail}{_backup_suffix(backup_path)}",
                    "success",
                )
            except InvalidTransaction as exc:
                flash(f"Fund movement error: {exc}", "error")
            finally:
                db.close()

        except ValueError as exc:
            flash(str(exc), "error")

        return _redirect_after_write("gui.add_fund_movement")

    return render_template(
        "add_fund_movement.html",
        active=active,
        active_section="operations",
        next_url=request.args.get("next_url", ""),
    )


# ---------------------------------------------------------------------------
# B57 — Historical Realized PnL Adjustments
# ---------------------------------------------------------------------------

@bp.route("/historical-pnl", methods=["GET", "POST"])
def historical_pnl():
    active = _require_active_db()
    if active is None:
        return redirect(url_for("gui.setup"))

    if request.method == "POST":
        confirmed = request.form.get("confirmed") == "1"
        review_mode = request.form.get("review") == "1" and not confirmed

        try:
            parsed = _validate_hist_pnl_form(request.form)

            if review_mode:
                return render_template(
                    "review_operation.html",
                    active=active,
                    active_section="reports",
                    title="Review Historical PnL Adjustment",
                    summary=_summary_hist_pnl(parsed),
                    raw_payload=_raw_payload(request.form, _HIST_PNL_FIELDS),
                    endpoint_url=url_for("gui.historical_pnl"),
                    next_url=request.form.get("next_url", ""),
                    warning=None,
                )

            backup_path, abort = _backup_before_write(
                active, "add_historical_pnl_adjustment", "gui.historical_pnl",
            )
            if abort is not None:
                return abort

            db, resolver, _tx_svc, pnl_svc = _get_db_and_services(active)
            try:
                adj_id = pnl_svc.add_historical_realized_pnl_adjustment(
                    adjustment_date=parsed["adjustment_date"],
                    source=parsed["source"],
                    description=parsed["description"],
                    amount_usd=parsed["amount_usd"],
                )
                flash(
                    f"OK: Historical PnL adjustment saved, id={adj_id}"
                    f"{_backup_suffix(backup_path)}",
                    "success",
                )
            finally:
                db.close()

        except ValueError as exc:
            flash(str(exc), "error")

        return _redirect_after_write("gui.historical_pnl")

    db, _resolver, _tx_svc, pnl_svc = _get_db_and_services(active)
    try:
        adjustments = pnl_svc.list_historical_realized_pnl_adjustments()
        total_adjustment = pnl_svc.sum_historical_realized_pnl_adjustments()
    finally:
        db.close()

    return render_template(
        "historical_pnl.html",
        active=active,
        active_section="reports",
        adjustments=adjustments,
        total_adjustment=total_adjustment,
        next_url=url_for("gui.historical_pnl"),
        format_money=_format_money,
    )


# ---------------------------------------------------------------------------
# Read-only views
# ---------------------------------------------------------------------------

@bp.get("/transactions")
def transactions():
    active = _require_active_db()
    if active is None:
        return redirect(url_for("gui.setup"))

    account = request.args.get("account", "").strip() or None
    symbol = request.args.get("symbol", "").strip().upper() or None
    side = request.args.get("side", "").strip().upper() or None
    limit = int(request.args.get("limit", "100"))

    db, resolver, tx_svc, _ = _get_db_and_services(active)
    try:
        rows = tx_svc.list_transactions(
            account=account, symbol=symbol, side=side, limit=limit,
        )
    finally:
        db.close()

    return render_template(
        "transactions.html",
        active=active,
        active_section="ledger",
        rows=rows,
        filters={"account": account or "", "symbol": symbol or "", "side": side or "", "limit": limit},
        format_money=_format_money,
        format_qty=_format_qty,
    )


@bp.get("/portfolio")
def portfolio():
    active = _require_active_db()
    if active is None:
        return redirect(url_for("gui.setup"))

    account = request.args.get("account", "").strip() or None

    db, resolver, _, pnl_svc = _get_db_and_services(active)
    try:
        rows = pnl_svc.positions(account)
    finally:
        db.close()

    total_value = Decimal("0")
    total_cost = Decimal("0")
    for p in rows:
        if p.get("approved_value") is not None and p["qty_open"] > 0:
            total_value += Decimal(str(p["approved_value"]))
        if p.get("cost_basis") is not None:
            total_cost += Decimal(str(p["cost_basis"]))

    return render_template(
        "positions.html",
        active=active,
        active_section="portfolio",
        rows=rows,
        filters={"account": account or ""},
        total_value=total_value,
        total_cost=total_cost,
        format_money=_format_money,
        format_qty=_format_qty,
    )


# Backwards-compatible /positions URL with its own endpoint name.
bp.add_url_rule("/positions", endpoint="positions", view_func=portfolio, methods=["GET"])


@bp.get("/portfolio/export.csv")
def portfolio_export_csv():
    active = _require_active_db()
    if active is None:
        return redirect(url_for("gui.setup"))

    account = request.args.get("account", "").strip() or None

    db, _, _, pnl_svc = _get_db_and_services(active)
    try:
        rows = pnl_svc.positions(account)
    finally:
        db.close()

    filename = "portfolio_snapshot_{}.csv".format(
        datetime.now().strftime("%Y%m%d_%H%M%S")
    )

    buf = io.StringIO()
    writer = _csv_mod.writer(buf)
    writer.writerow(["Symbol", "Account", "Qty", "Cost Basis", "Avg Cost", "Method"])
    for p in rows:
        if p["qty_open"] <= 0:
            continue
        writer.writerow([
            p["symbol"],
            p["account"] or "",
            str(p["qty_open"]),
            str(p["cost_basis"]),
            str(p["avg_cost"]),
            p.get("valuation_method") or "market_live",
        ])

    response = make_response(buf.getvalue())
    response.headers["Content-Disposition"] = f"attachment; filename={filename}"
    response.headers["Content-Type"] = "text/csv; charset=utf-8"
    return response


@bp.get("/summary")
def summary():
    active = _require_active_db()
    if active is None:
        return redirect(url_for("gui.setup"))

    account = request.args.get("account", "").strip() or None

    db, resolver, _, pnl_svc = _get_db_and_services(active)
    try:
        s = pnl_svc.summary(account)
        last_refresh_display = _format_last_refresh(_get_last_price_refresh(db))
    finally:
        db.close()

    breakdown_rows = _build_breakdown_rows(s)
    total_pnl = _compute_total_pnl(s)

    return render_template(
        "summary.html",
        active=active,
        active_section="reports",
        s=s,
        breakdown_rows=breakdown_rows,
        filters={"account": account or ""},
        last_refresh_display=last_refresh_display,
        total_pnl=total_pnl,
        format_money=_format_money,
    )


@bp.get("/daily-report")
def daily_report():
    active = _require_active_db()
    if active is None:
        return redirect(url_for("gui.setup"))

    account = request.args.get("account", "").strip() or None

    db, resolver, _, pnl_svc = _get_db_and_services(active)
    try:
        positions_list = pnl_svc.positions(account)
        s = pnl_svc.summary(account)
    finally:
        db.close()

    breakdown_rows = _build_breakdown_rows(s)

    counts = s.get("price_quality_counts", {}) or {}
    warnings = []
    stale = int(counts.get("stale", 0) or 0)
    unavailable = int(counts.get("unavailable", 0) or 0)
    if stale:
        warnings.append(f"{stale} position(s) have stale prices")
    if unavailable:
        warnings.append(f"{unavailable} position(s) have unavailable prices")

    run_timestamp = datetime.now(timezone.utc).isoformat()

    return render_template(
        "daily_report.html",
        active=active,
        active_section="reports",
        s=s,
        positions=positions_list,
        breakdown_rows=breakdown_rows,
        counts=counts,
        warnings=warnings,
        run_timestamp=run_timestamp,
        filters={"account": account or ""},
        format_money=_format_money,
        format_qty=_format_qty,
    )
