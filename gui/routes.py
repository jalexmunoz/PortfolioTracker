import os
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation

from flask import Blueprint, current_app, flash, redirect, render_template, request, url_for

from .backup import create_backup
from .db_context import clear_active_db, load_active_db, set_active_db

bp = Blueprint("gui", __name__)


def _backup_root() -> str:
    return os.environ.get(
        "PORTFOLIO_GUI_BACKUP_DIR",
        os.path.join(os.path.dirname(current_app.root_path), "output", "gui_backups"),
    )


def _require_active_db():
    """Return ActiveDbContext or None (flashes error if missing)."""
    active = load_active_db()
    if active is None:
        flash("No active DB selected. Go to Change DB first.", "error")
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


# ---------------------------------------------------------------------------
# H1 routes
# ---------------------------------------------------------------------------

@bp.get("/")
def home():
    return render_template("home.html")


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
            return redirect(url_for("gui.db_context"))
        except ValueError as exc:
            flash(str(exc), "error")

    return render_template("db_context.html")


@bp.post("/db-context/clear")
def db_context_clear():
    clear_active_db()
    flash("Active DB cleared. No database is currently selected.", "success")
    return redirect(url_for("gui.db_context"))


@bp.route("/backup", methods=["GET", "POST"])
def backup():
    if request.method == "POST":
        active = load_active_db()
        if active is None:
            flash("Cannot create backup: no active DB selected.", "error")
            return redirect(url_for("gui.backup"))

        try:
            backup_path = create_backup(active.db_path, _backup_root())
            flash(f"Backup created: {backup_path}", "success")
        except Exception as exc:
            flash(f"Backup failed: {exc}", "error")

        return redirect(url_for("gui.backup"))

    return render_template("backup.html", backup_root=_backup_root())


# ---------------------------------------------------------------------------
# H2 routes – Setup
# ---------------------------------------------------------------------------

@bp.route("/init-db", methods=["GET", "POST"])
def init_db():
    active = _require_active_db()
    if active is None:
        return redirect(url_for("gui.db_context"))

    if request.method == "POST":
        try:
            from portfolio_tracker_v2.core import Database
            from portfolio_tracker_v2.core.asset_resolver import AssetResolver

            db = Database(active.db_path)
            db.connect()
            db.init_schema()
            resolver = AssetResolver(db)
            resolver.get_or_create_usd_cash()
            db.close()
            flash(f"Database initialized at {active.db_path}", "success")
        except Exception as exc:
            flash(f"Init failed: {exc}", "error")
        return redirect(url_for("gui.init_db"))

    return render_template("init_db.html", active=active)


@bp.route("/import-legacy", methods=["GET", "POST"])
def import_legacy():
    active = _require_active_db()
    if active is None:
        return redirect(url_for("gui.db_context"))

    if request.method == "POST":
        csv_path = request.form.get("csv_path", "").strip()
        seed_date = request.form.get("seed_date", "").strip()
        dry_run = request.form.get("dry_run") == "1"

        if not csv_path:
            flash("CSV path is required.", "error")
            return redirect(url_for("gui.import_legacy"))
        if not seed_date:
            flash("Seed date is required.", "error")
            return redirect(url_for("gui.import_legacy"))
        if not os.path.isfile(csv_path):
            flash(f"File not found: {csv_path}", "error")
            return redirect(url_for("gui.import_legacy"))

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
                f"{rejected_count} rejected.",
                "success" if not rejected_count else "warning",
            )
        except Exception as exc:
            flash(f"Import failed: {exc}", "error")

        return redirect(url_for("gui.import_legacy"))

    return render_template("import_legacy.html", active=active)


# ---------------------------------------------------------------------------
# H2 routes – Write operations
# ---------------------------------------------------------------------------

@bp.route("/add-transaction", methods=["GET", "POST"])
def add_transaction():
    active = _require_active_db()
    if active is None:
        return redirect(url_for("gui.db_context"))

    if request.method == "POST":
        try:
            side = request.form.get("side", "").strip().upper()
            account = request.form.get("account", "").strip()
            symbol = request.form.get("symbol", "").strip().upper()
            tx_date_str = request.form.get("tx_date", "").strip()
            qty = _parse_decimal(request.form.get("qty"), "Qty")
            price = _parse_decimal(request.form.get("price"), "Unit Price")
            fee_raw = request.form.get("fee", "0").strip()
            fee_usd = Decimal(fee_raw) if fee_raw else Decimal("0")
            notes = request.form.get("notes", "").strip() or None

            # Validations
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
            if not tx_date_str:
                raise ValueError("Transaction date is required")

            db, resolver, tx_svc, _ = _get_db_and_services(active)
            from portfolio_tracker_v2.core.exceptions import InvalidTransaction

            try:
                if side == "BUY":
                    tx_id = tx_svc.record_buy(
                        symbol=symbol, account=account, qty=qty,
                        unit_price=price, fee_usd=fee_usd,
                        tx_date=tx_date_str, notes=notes,
                    )
                else:
                    tx_id = tx_svc.record_sell(
                        symbol=symbol, account=account, qty=qty,
                        unit_price=price, fee_usd=fee_usd,
                        tx_date=tx_date_str, notes=notes,
                    )
                flash(f"OK: {side} recorded, tx_id={tx_id}", "success")
            except InvalidTransaction as exc:
                flash(f"Transaction error: {exc}", "error")
            finally:
                db.close()

        except ValueError as exc:
            flash(str(exc), "error")

        return redirect(url_for("gui.add_transaction"))

    return render_template("add_transaction.html", active=active)


@bp.route("/add-cdt", methods=["GET", "POST"])
def add_cdt():
    active = _require_active_db()
    if active is None:
        return redirect(url_for("gui.db_context"))

    if request.method == "POST":
        try:
            account = request.form.get("account", "").strip()
            symbol = request.form.get("symbol", "BBVA CDT").strip().upper()
            open_date = request.form.get("open_date", "").strip()
            maturity_date = request.form.get("maturity_date", "").strip()
            principal = _parse_decimal(request.form.get("principal"), "Principal")
            annual_rate = _parse_decimal(request.form.get("annual_rate"), "Annual Rate")
            term_raw = request.form.get("term_years", "").strip()
            term_years = Decimal(term_raw) if term_raw else None
            notes = request.form.get("notes", "").strip() or None

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

            db, resolver, tx_svc, _ = _get_db_and_services(active)
            from portfolio_tracker_v2.core.exceptions import InvalidTransaction

            try:
                tx_id = tx_svc.record_cdt(
                    account=account,
                    symbol=symbol,
                    open_date=open_date,
                    maturity_date=maturity_date,
                    principal=principal,
                    term_years=term_years,
                    annual_rate=annual_rate,
                    notes=notes,
                )
                flash(
                    f"OK: CDT recorded, tx_id={tx_id}, "
                    f"principal={_format_money(principal)}, rate={annual_rate}",
                    "success",
                )
            except InvalidTransaction as exc:
                flash(f"CDT error: {exc}", "error")
            finally:
                db.close()

        except ValueError as exc:
            flash(str(exc), "error")

        return redirect(url_for("gui.add_cdt"))

    return render_template("add_cdt.html", active=active)


@bp.route("/add-fund-movement", methods=["GET", "POST"])
def add_fund_movement():
    active = _require_active_db()
    if active is None:
        return redirect(url_for("gui.db_context"))

    if request.method == "POST":
        try:
            account = request.form.get("account", "").strip()
            symbol = request.form.get("symbol", "").strip().upper()
            movement_date = request.form.get("movement_date", "").strip()
            movement_type = request.form.get("movement_type", "").strip().upper()
            amount = _parse_decimal(request.form.get("amount"), "Amount")
            notes = request.form.get("notes", "").strip() or None

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

            db, resolver, tx_svc, _ = _get_db_and_services(active)
            from portfolio_tracker_v2.core.exceptions import InvalidTransaction

            try:
                tx_id = tx_svc.record_fund_movement(
                    account=account,
                    symbol=symbol,
                    movement_date=movement_date,
                    amount=amount,
                    movement_type=movement_type,
                    notes=notes,
                )
                flash(
                    f"OK: {movement_type} recorded, tx_id={tx_id}, "
                    f"amount={_format_money(amount)}",
                    "success",
                )
            except InvalidTransaction as exc:
                flash(f"Fund movement error: {exc}", "error")
            finally:
                db.close()

        except ValueError as exc:
            flash(str(exc), "error")

        return redirect(url_for("gui.add_fund_movement"))

    return render_template("add_fund_movement.html", active=active)


# ---------------------------------------------------------------------------
# H2 routes – Read-only views
# ---------------------------------------------------------------------------

@bp.get("/transactions")
def transactions():
    active = _require_active_db()
    if active is None:
        return redirect(url_for("gui.db_context"))

    # Query params for filters
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
        rows=rows,
        filters={"account": account or "", "symbol": symbol or "", "side": side or "", "limit": limit},
        format_money=_format_money,
        format_qty=_format_qty,
    )


@bp.get("/positions")
def positions():
    active = _require_active_db()
    if active is None:
        return redirect(url_for("gui.db_context"))

    account = request.args.get("account", "").strip() or None

    db, resolver, _, pnl_svc = _get_db_and_services(active)
    try:
        rows = pnl_svc.positions(account)
    finally:
        db.close()

    return render_template(
        "positions.html",
        active=active,
        rows=rows,
        filters={"account": account or ""},
        format_money=_format_money,
        format_qty=_format_qty,
    )


@bp.get("/summary")
def summary():
    active = _require_active_db()
    if active is None:
        return redirect(url_for("gui.db_context"))

    account = request.args.get("account", "").strip() or None

    db, resolver, _, pnl_svc = _get_db_and_services(active)
    try:
        s = pnl_svc.summary(account)
    finally:
        db.close()

    # Build breakdown rows with percentages
    breakdown = s.get("asset_class_breakdown", {}) or {}
    total_equity = Decimal(str(s.get("total_equity", 0)))
    breakdown_rows = []
    for asset_class in ["Crypto", "Equities", "Metals", "Non-market", "Cash"]:
        equity = Decimal(str(breakdown.get(asset_class, 0)))
        if total_equity > 0:
            pct = ((equity / total_equity) * Decimal("100")).quantize(Decimal("0.01"))
        else:
            pct = Decimal("0.00")
        breakdown_rows.append({"asset_class": asset_class, "equity": equity, "pct": pct})

    return render_template(
        "summary.html",
        active=active,
        s=s,
        breakdown_rows=breakdown_rows,
        filters={"account": account or ""},
        format_money=_format_money,
    )


@bp.get("/daily-report")
def daily_report():
    active = _require_active_db()
    if active is None:
        return redirect(url_for("gui.db_context"))

    account = request.args.get("account", "").strip() or None

    db, resolver, _, pnl_svc = _get_db_and_services(active)
    try:
        positions_list = pnl_svc.positions(account)
        s = pnl_svc.summary(account)
    finally:
        db.close()

    # Build breakdown rows
    breakdown = s.get("asset_class_breakdown", {}) or {}
    total_equity = Decimal(str(s.get("total_equity", 0)))
    breakdown_rows = []
    for asset_class in ["Crypto", "Equities", "Metals", "Non-market", "Cash"]:
        equity = Decimal(str(breakdown.get(asset_class, 0)))
        if total_equity > 0:
            pct = ((equity / total_equity) * Decimal("100")).quantize(Decimal("0.01"))
        else:
            pct = Decimal("0.00")
        breakdown_rows.append({"asset_class": asset_class, "equity": equity, "pct": pct})

    # Price quality
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
