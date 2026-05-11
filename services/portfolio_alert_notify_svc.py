"""
B64A — Shared helper: evaluate portfolio alerts and notify.

Calls PortfolioAlertRuleService.evaluate_portfolio_alerts() and, if new alerts
were created and Telegram is enabled, sends a plain-text summary via
telegram_notify_svc.send_message().

Used after price refresh (CLI and GUI). Not tied to any macro signal.

Does NOT modify: cash, positions, PnL, transactions, assets, price cache.
TELEGRAM_BOT_TOKEN is never logged or included in any message body.
"""
from __future__ import annotations

from portfolio_tracker_v2.core.asset_resolver import AssetResolver
from portfolio_tracker_v2.core.database import Database
from portfolio_tracker_v2.services.portfolio_alert_rule_svc import PortfolioAlertRuleService
from portfolio_tracker_v2.services import telegram_notify_svc

_SOURCE = "portfolio_rule"


def evaluate_and_notify_portfolio_alerts(db: Database, trigger_source: str) -> dict:
    """
    Evaluate portfolio alert rules and optionally notify via Telegram.

    Args:
        db:             Connected Database instance. Caller is responsible for
                        closing it after this call returns.
        trigger_source: Human-readable label shown in the Telegram message
                        (e.g. "refresh-prices").

    Returns:
        {
            "evaluated":       True,
            "created":         int,        # new OPEN alerts created
            "skipped":         int,        # duplicates suppressed by dedupe
            "telegram_sent":   bool,
            "telegram_error":  str | None,
        }

    Never raises — callers (CLI, GUI) handle failures at their own level.
    """
    # Capture max existing portfolio_rule alert id before evaluation so we can
    # identify only the alerts created in this round for the Telegram body.
    row = db.connect().execute(
        "SELECT COALESCE(MAX(id), 0) FROM signal_alerts WHERE source = ?",
        (_SOURCE,),
    ).fetchone()
    max_id_before = row[0] if row else 0

    result = PortfolioAlertRuleService(db, AssetResolver(db)).evaluate_portfolio_alerts()
    created = result["created"]
    skipped = result["skipped"]

    telegram_sent = False
    telegram_error = None

    if created > 0 and telegram_notify_svc.is_enabled():
        new_alerts = [
            dict(r) for r in db.connect().execute(
                "SELECT id, event_type, message FROM signal_alerts"
                " WHERE source = ? AND id > ? ORDER BY id DESC",
                (_SOURCE, max_id_before),
            ).fetchall()
        ]
        text = _format_refresh_summary(trigger_source, new_alerts)
        send_result = telegram_notify_svc.send_message(text)
        telegram_sent = send_result["ok"]
        telegram_error = send_result.get("error")

    return {
        "evaluated": True,
        "created": created,
        "skipped": skipped,
        "telegram_sent": telegram_sent,
        "telegram_error": telegram_error,
    }


def _format_refresh_summary(trigger_source: str, created_alerts: list[dict]) -> str:
    lines = [
        "PortfolioTracker Alert",
        "",
        f"Trigger: {trigger_source}",
        f"Portfolio alerts created: {len(created_alerts)}",
    ]
    if created_alerts:
        lines.append("")
        for alert in created_alerts:
            msg = (alert.get("message") or alert.get("event_type") or "").strip()
            if msg:
                lines.append(f"- {msg}")
    lines.append("")
    lines.append("Action: Review portfolio alerts. No automatic trade.")
    return "\n".join(lines)
