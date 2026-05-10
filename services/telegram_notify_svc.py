"""
B63C — Telegram outbound notifications.

Sends a plain-text summary to a configured Telegram chat when portfolio alerts
are created by a macro webhook. Uses stdlib only (urllib.request); no extra deps.

Does NOT modify: cash, positions, PnL, assets, transactions, lot_matches,
                 price_cache, or any financial tables.
TELEGRAM_BOT_TOKEN is never logged or included in any message body.
"""
from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Optional

from portfolio_tracker_v2.core.database import Database

logger = logging.getLogger(__name__)

_CHANNEL = "telegram"
_API_TIMEOUT = 10  # seconds


# ---------------------------------------------------------------------------
# Configuration helpers
# ---------------------------------------------------------------------------

def is_enabled() -> bool:
    """Return True only if all three Telegram env vars are properly set."""
    return (
        os.environ.get("PORTFOLIO_TELEGRAM_ENABLED", "").strip() == "1"
        and bool(os.environ.get("TELEGRAM_BOT_TOKEN", "").strip())
        and bool(os.environ.get("TELEGRAM_CHAT_ID", "").strip())
    )


# ---------------------------------------------------------------------------
# Low-level send (stateless, no DB)
# ---------------------------------------------------------------------------

def send_message(text: str) -> dict:
    """
    POST a plain-text message to the configured Telegram chat.
    Returns {"ok": bool, "error": str|None}.
    TELEGRAM_BOT_TOKEN is never written to logs or to the returned dict.
    """
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

    if not token or not chat_id:
        return {"ok": False, "error": "Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID"}

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = json.dumps({"chat_id": chat_id, "text": text}).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=_API_TIMEOUT) as resp:
            json.loads(resp.read().decode("utf-8"))
            return {"ok": True, "error": None}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        logger.warning("Telegram HTTP %s: %.200s", exc.code, body)
        return {"ok": False, "error": f"HTTP {exc.code}"}
    except Exception as exc:
        logger.warning("Telegram send failed: %s", type(exc).__name__)
        return {"ok": False, "error": str(exc)}


# ---------------------------------------------------------------------------
# Message formatting
# ---------------------------------------------------------------------------

def format_summary(event_type: str, severity: str, created_alerts: list[dict]) -> str:
    lines = [
        "PortfolioTracker Alert",
        f"Macro: {event_type}",
        f"Severity: {severity}",
        f"Portfolio alerts created: {len(created_alerts)}",
    ]
    if created_alerts:
        lines.append("")
        for alert in created_alerts:
            msg = (alert.get("message") or alert.get("event_type") or "").strip()
            if msg:
                lines.append(f"- {msg}")
    lines.append("")
    lines.append("Action: Review portfolio risk. No automatic trade.")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# DB-backed service with deduplication
# ---------------------------------------------------------------------------

class TelegramNotifyService:
    """
    Wraps send_message with DB-backed deduplication via signal_notifications.
    Reads/writes only signal_notifications. Never touches financial tables.
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    def _ensure_schema(self) -> None:
        self._db._ensure_signal_notifications_schema()

    def already_sent(self, signal_id: int) -> bool:
        """Return True if a notification row already exists for this signal."""
        self._ensure_schema()
        row = self._db.connect().execute(
            "SELECT id FROM signal_notifications WHERE signal_id = ? AND channel = ?",
            (signal_id, _CHANNEL),
        ).fetchone()
        return row is not None

    def _record(self, signal_id: int, status: str, error: Optional[str] = None) -> None:
        self._ensure_schema()
        sent_at = (
            datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
            if status == "sent"
            else None
        )
        try:
            self._db.connect().execute(
                """
                INSERT OR IGNORE INTO signal_notifications
                    (signal_id, channel, status, sent_at, error)
                VALUES (?, ?, ?, ?, ?)
                """,
                (signal_id, _CHANNEL, status, sent_at, error),
            )
            self._db.commit()
        except Exception as exc:
            logger.warning("Failed to record signal notification: %s", exc)

    def send_signal_summary(
        self,
        signal_id: int,
        event_type: str,
        severity: str,
        created_alerts: list[dict],
    ) -> dict:
        """
        Format and send a Telegram summary for a macro signal + its portfolio alerts.
        Skips silently if already sent (idempotency guard).
        Returns {"ok": bool, "skipped": bool, "error": str|None}.
        """
        if not is_enabled():
            return {"ok": False, "skipped": False, "error": "Telegram not enabled"}

        if self.already_sent(signal_id):
            return {"ok": True, "skipped": True, "error": None}

        text = format_summary(event_type, severity, created_alerts)
        result = send_message(text)

        status = "sent" if result["ok"] else "failed"
        self._record(signal_id, status, result.get("error"))

        return {"ok": result["ok"], "skipped": False, "error": result.get("error")}
