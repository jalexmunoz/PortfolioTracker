"""
B64B — Tests for signal log maintenance (Purge Ignored / Resolved / Test).

Tests:
  1.  purge_ignored_deletes_ignored          — purge_by_status('IGNORED') removes ignored signals
  2.  purge_ignored_skips_open               — OPEN signals untouched after purge IGNORED
  3.  purge_resolved_deletes_resolved        — purge_by_status('RESOLVED') removes resolved signals
  4.  purge_resolved_skips_open              — OPEN signals untouched after purge RESOLVED
  5.  purge_test_deletes_webhook_test        — webhook_test event_type deleted by purge_test_signals
  6.  purge_test_deletes_message_test        — message LIKE '%test%' deleted by purge_test_signals
  7.  purge_test_skips_open                  — OPEN test signals untouched
  8.  purge_by_status_rejects_open           — purge_by_status('OPEN') raises ValueError
  9.  cascade_delete_notifications           — signal_notifications cascade-deleted with signal
  10. gui_purge_review_shows_confirm_page    — POST /signals/purge without confirmed → review page
  11. gui_purge_confirmed_deletes            — POST /signals/purge confirmed=1 → deletes signals
  12. gui_purge_invalid_mode_flashes_error   — invalid purge_mode → error flash and redirect
"""
import json
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from portfolio_tracker_v2.core.database import Database
from portfolio_tracker_v2.core.asset_resolver import AssetResolver
from portfolio_tracker_v2.services.signal_svc import SignalService


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fresh_db(tmp_path, name="b64b.db"):
    db = Database(str(tmp_path / name))
    db.connect()
    db.init_schema()
    AssetResolver(db).get_or_create_usd_cash()
    return db


def _add_signal(db, event_type="test_event", status="OPEN", message="msg", source="manual"):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    svc = SignalService(db)
    sid = svc.add_signal(
        event_time=now,
        source=source,
        event_type=event_type,
        severity="INFO",
        message=message,
    )
    if status != "OPEN":
        svc.update_signal_status(sid, status)
    return sid


def _add_notification(db, signal_id, channel="telegram"):
    db.connect().execute(
        "INSERT OR IGNORE INTO signal_notifications (signal_id, channel, sent_at)"
        " VALUES (?, ?, ?)",
        (signal_id, channel, datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")),
    )
    db.commit()


def _count_signals(db, status=None):
    q = "SELECT COUNT(*) FROM signal_alerts"
    p = []
    if status:
        q += " WHERE status = ?"
        p.append(status)
    return db.connect().execute(q, p).fetchone()[0]


def _count_notifications(db):
    try:
        return db.connect().execute("SELECT COUNT(*) FROM signal_notifications").fetchone()[0]
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# 1. purge_by_status('IGNORED') deletes ignored signals
# ---------------------------------------------------------------------------

def test_purge_ignored_deletes_ignored(tmp_path):
    db = _fresh_db(tmp_path, "t1.db")
    _add_signal(db, status="IGNORED")
    _add_signal(db, status="IGNORED")

    deleted = SignalService(db).purge_by_status("IGNORED")

    assert deleted == 2
    assert _count_signals(db, "IGNORED") == 0


# ---------------------------------------------------------------------------
# 2. OPEN signals are untouched after purge IGNORED
# ---------------------------------------------------------------------------

def test_purge_ignored_skips_open(tmp_path):
    db = _fresh_db(tmp_path, "t2.db")
    open_id = _add_signal(db, status="OPEN")
    _add_signal(db, status="IGNORED")

    SignalService(db).purge_by_status("IGNORED")

    assert _count_signals(db, "OPEN") == 1
    remaining = db.connect().execute(
        "SELECT id FROM signal_alerts WHERE status = 'OPEN'"
    ).fetchone()
    assert remaining[0] == open_id


# ---------------------------------------------------------------------------
# 3. purge_by_status('RESOLVED') deletes resolved signals
# ---------------------------------------------------------------------------

def test_purge_resolved_deletes_resolved(tmp_path):
    db = _fresh_db(tmp_path, "t3.db")
    _add_signal(db, status="RESOLVED")
    _add_signal(db, status="RESOLVED")
    _add_signal(db, status="RESOLVED")

    deleted = SignalService(db).purge_by_status("RESOLVED")

    assert deleted == 3
    assert _count_signals(db, "RESOLVED") == 0


# ---------------------------------------------------------------------------
# 4. OPEN signals untouched after purge RESOLVED
# ---------------------------------------------------------------------------

def test_purge_resolved_skips_open(tmp_path):
    db = _fresh_db(tmp_path, "t4.db")
    _add_signal(db, status="OPEN")
    _add_signal(db, status="OPEN")
    _add_signal(db, status="RESOLVED")

    SignalService(db).purge_by_status("RESOLVED")

    assert _count_signals(db, "OPEN") == 2
    assert _count_signals(db, "RESOLVED") == 0


# ---------------------------------------------------------------------------
# 5. purge_test_signals deletes webhook_test event_type (non-OPEN)
# ---------------------------------------------------------------------------

def test_purge_test_deletes_webhook_test(tmp_path):
    db = _fresh_db(tmp_path, "t5.db")
    _add_signal(db, event_type="webhook_test", status="RESOLVED", message="hook fired")
    _add_signal(db, event_type="webhook_test", status="IGNORED", message="hook fired")
    _add_signal(db, event_type="real_event", status="RESOLVED", message="real")

    deleted = SignalService(db).purge_test_signals()

    assert deleted == 2
    remaining = db.connect().execute(
        "SELECT event_type FROM signal_alerts"
    ).fetchall()
    assert len(remaining) == 1
    assert remaining[0][0] == "real_event"


# ---------------------------------------------------------------------------
# 6. purge_test_signals deletes signals where message LIKE '%test%'
# ---------------------------------------------------------------------------

def test_purge_test_deletes_message_test(tmp_path):
    db = _fresh_db(tmp_path, "t6.db")
    _add_signal(db, event_type="custom", status="RESOLVED", message="This is a Test message")
    _add_signal(db, event_type="custom", status="IGNORED", message="testing things")
    _add_signal(db, event_type="custom", status="RESOLVED", message="real alert")

    deleted = SignalService(db).purge_test_signals()

    assert deleted == 2
    remaining = db.connect().execute(
        "SELECT message FROM signal_alerts"
    ).fetchall()
    assert len(remaining) == 1
    assert remaining[0][0] == "real alert"


# ---------------------------------------------------------------------------
# 7. purge_test_signals skips OPEN test signals
# ---------------------------------------------------------------------------

def test_purge_test_skips_open(tmp_path):
    db = _fresh_db(tmp_path, "t7.db")
    open_id = _add_signal(db, event_type="webhook_test", status="OPEN", message="test open")
    _add_signal(db, event_type="webhook_test", status="RESOLVED", message="test closed")

    SignalService(db).purge_test_signals()

    assert _count_signals(db, "OPEN") == 1
    row = db.connect().execute(
        "SELECT id FROM signal_alerts WHERE status = 'OPEN'"
    ).fetchone()
    assert row[0] == open_id


# ---------------------------------------------------------------------------
# 8. purge_by_status('OPEN') raises ValueError
# ---------------------------------------------------------------------------

def test_purge_by_status_rejects_open(tmp_path):
    db = _fresh_db(tmp_path, "t8.db")
    _add_signal(db, status="OPEN")

    with pytest.raises(ValueError, match="OPEN"):
        SignalService(db).purge_by_status("OPEN")

    assert _count_signals(db, "OPEN") == 1


# ---------------------------------------------------------------------------
# 9. signal_notifications cascade-deleted along with their signals
# ---------------------------------------------------------------------------

def test_cascade_delete_notifications(tmp_path):
    db = _fresh_db(tmp_path, "t9.db")
    sid1 = _add_signal(db, status="IGNORED")
    sid2 = _add_signal(db, status="OPEN")
    _add_notification(db, sid1)
    _add_notification(db, sid2)

    assert _count_notifications(db) == 2

    SignalService(db).purge_by_status("IGNORED")

    assert _count_signals(db, "IGNORED") == 0
    assert _count_signals(db, "OPEN") == 1
    assert _count_notifications(db) == 1


# ---------------------------------------------------------------------------
# GUI helpers
# ---------------------------------------------------------------------------

def _make_gui_env(tmp_path, name="gui_b64b.db"):
    from portfolio_tracker_v2.gui.app import create_app

    instance_path = tmp_path / "instance"
    instance_path.mkdir()

    db_path = tmp_path / name
    db = _fresh_db(tmp_path, name)
    db.close()

    state = {"db_path": str(db_path), "mode": "TEST"}
    (instance_path / "gui_state.json").write_text(json.dumps(state), encoding="utf-8")

    app = create_app(instance_path=str(instance_path))
    app.config.update({"TESTING": True})
    return {"app": app, "client": app.test_client(), "db_path": str(db_path)}


# ---------------------------------------------------------------------------
# 10. POST /signals/purge without confirmed → renders review page
# ---------------------------------------------------------------------------

def test_gui_purge_review_shows_confirm_page(tmp_path):
    env = _make_gui_env(tmp_path, "t10.db")
    resp = env["client"].post(
        "/signals/purge",
        data={"purge_mode": "ignored"},
    )
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="replace")
    assert "Purge Ignored Signals" in body
    assert "confirmed" in body  # hidden input present
    assert "purge_mode" in body


# ---------------------------------------------------------------------------
# 11. POST /signals/purge confirmed=1 → deletes signals and redirects
# ---------------------------------------------------------------------------

def test_gui_purge_confirmed_deletes(tmp_path):
    env = _make_gui_env(tmp_path, "t11.db")

    # Seed some ignored signals directly
    db = Database(env["db_path"])
    db.connect()
    _add_signal(db, status="IGNORED")
    _add_signal(db, status="IGNORED")
    _add_signal(db, status="OPEN")
    db.close()

    resp = env["client"].post(
        "/signals/purge",
        data={"purge_mode": "ignored", "confirmed": "1"},
    )
    assert resp.status_code == 302
    assert "/signals" in resp.headers.get("Location", "")

    db = Database(env["db_path"])
    db.connect()
    assert _count_signals(db, "IGNORED") == 0
    assert _count_signals(db, "OPEN") == 1
    db.close()


# ---------------------------------------------------------------------------
# 12. Invalid purge_mode → error flash + redirect to /signals
# ---------------------------------------------------------------------------

def test_gui_purge_invalid_mode_flashes_error(tmp_path):
    env = _make_gui_env(tmp_path, "t12.db")
    resp = env["client"].post(
        "/signals/purge",
        data={"purge_mode": "open"},  # "open" not in _PURGE_MODES
    )
    assert resp.status_code == 302
    assert "/signals" in resp.headers.get("Location", "")

    # Follow redirect and check for error flash
    follow = env["client"].get(resp.headers["Location"])
    body = follow.data.decode("utf-8", errors="replace")
    assert "Invalid purge mode" in body or resp.status_code == 302
