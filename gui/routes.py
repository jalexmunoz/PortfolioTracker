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
    realized = Decimal(str(summary.get("total_realized_pnl", 0) or 0))
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
        )[:5]

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

        return _redirect_after_write("gui.import_legacy")

    return render_template(
        "import_legacy.html",
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
            currency = (request.form.get("currency", "") or "").strip().upper()
            fx_raw = request.form.get("fx_rate_at_open", "").strip()
            fx_rate_at_open = Decimal(fx_raw) if fx_raw else None

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
                    currency=currency,
                    fx_rate_at_open=fx_rate_at_open,
                )
                detail = f"principal={_format_money(principal)} {currency}"
                if currency != "USD":
                    principal_usd = (principal / fx_rate_at_open).quantize(Decimal("0.000001"))
                    detail += f" (fx={fx_rate_at_open} → {_format_money(principal_usd)} USD)"
                flash(
                    f"OK: CDT recorded, tx_id={tx_id}, {detail}, rate={annual_rate}",
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
        try:
            account = request.form.get("account", "").strip()
            movement_date = request.form.get("movement_date", "").strip()
            movement_type = request.form.get("movement_type", "").strip().upper()
            amount = _parse_decimal(request.form.get("amount"), "Amount")
            notes = request.form.get("notes", "").strip() or None

            if not account:
                raise ValueError("Account is required")
            if not movement_date:
                raise ValueError("Date is required")
            if movement_type not in ("DEPOSIT", "WITHDRAWAL"):
                raise ValueError("Movement type must be DEPOSIT or WITHDRAWAL")
            if amount <= 0:
                raise ValueError("Amount must be > 0")

            db, resolver, tx_svc, _ = _get_db_and_services(active)
            from portfolio_tracker_v2.core.exceptions import InvalidTransaction

            try:
                tx_id = tx_svc.record_cash_movement(
                    account=account,
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
            f"{report.failed_final} failed final.",
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
        try:
            account = request.form.get("account", "").strip()
            symbol = request.form.get("symbol", "").strip().upper()
            movement_date = request.form.get("movement_date", "").strip()
            movement_type = request.form.get("movement_type", "").strip().upper()
            amount = _parse_decimal(request.form.get("amount"), "Amount")
            notes = request.form.get("notes", "").strip() or None
            currency = (request.form.get("currency", "") or "").strip().upper()
            fx_raw = request.form.get("fx_rate", "").strip()
            fx_rate = Decimal(fx_raw) if fx_raw else None

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
                    currency=currency,
                    fx_rate=fx_rate,
                )
                detail = f"amount={_format_money(amount)} {currency}"
                if currency != "USD":
                    amount_usd = (amount / fx_rate).quantize(Decimal("0.000001"))
                    detail += f" (fx={fx_rate} → {_format_money(amount_usd)} USD)"
                flash(
                    f"OK: {movement_type} recorded, tx_id={tx_id}, {detail}",
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
