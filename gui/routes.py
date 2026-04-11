import os

from flask import Blueprint, current_app, flash, redirect, render_template, request, url_for

from .backup import create_backup
from .db_context import clear_active_db, load_active_db, set_active_db

bp = Blueprint("gui", __name__)


def _backup_root() -> str:
    return os.environ.get(
        "PORTFOLIO_GUI_BACKUP_DIR",
        os.path.join(os.path.dirname(current_app.root_path), "output", "gui_backups"),
    )


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

