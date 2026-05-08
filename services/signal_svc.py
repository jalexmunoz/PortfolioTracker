"""
Signal / Alert log service — B62A.

Informational only. Does not touch assets, transactions, lot_matches,
price_cache, cash_ledger, or any PnL / position calculation.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from portfolio_tracker_v2.core.database import Database

VALID_SEVERITIES = ("INFO", "LOW", "MEDIUM", "HIGH", "CRITICAL")
VALID_STATUSES = ("OPEN", "RESOLVED", "IGNORED")


class SignalService:
    def __init__(self, db: Database) -> None:
        self._db = db

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    def add_signal(
        self,
        event_time: str,
        source: str,
        event_type: str,
        severity: str,
        message: str = "",
        *,
        regime: Optional[str] = None,
        score: Optional[float] = None,
        symbol: Optional[str] = None,
        asset_class: Optional[str] = None,
        payload_json: Optional[str] = None,
        notes: Optional[str] = None,
    ) -> int:
        """Insert a new signal row. Returns the new row id."""
        severity = (severity or "INFO").strip().upper()
        if severity not in VALID_SEVERITIES:
            raise ValueError(
                f"Invalid severity '{severity}'. Must be one of {VALID_SEVERITIES}."
            )
        if not source or not source.strip():
            raise ValueError("source is required")
        if not event_type or not event_type.strip():
            raise ValueError("event_type is required")
        if not event_time or not event_time.strip():
            raise ValueError("event_time is required")

        conn = self._db.connect()
        cur = conn.execute(
            """
            INSERT INTO signal_alerts
                (event_time, source, event_type, severity, regime, score,
                 symbol, asset_class, message, payload_json, status, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', ?)
            """,
            (
                event_time.strip(),
                source.strip(),
                event_type.strip(),
                severity,
                regime,
                score,
                symbol,
                asset_class,
                message or "",
                payload_json,
                notes,
            ),
        )
        self._db.commit()
        return cur.lastrowid

    def update_signal_status(self, signal_id: int, new_status: str) -> bool:
        """Update status of a signal. Returns True if a row was found and updated."""
        new_status = (new_status or "").strip().upper()
        if new_status not in VALID_STATUSES:
            raise ValueError(
                f"Invalid status '{new_status}'. Must be one of {VALID_STATUSES}."
            )
        resolved_at = None
        if new_status in ("RESOLVED", "IGNORED"):
            resolved_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

        conn = self._db.connect()
        cur = conn.execute(
            """
            UPDATE signal_alerts
               SET status = ?, resolved_at = ?
             WHERE id = ?
            """,
            (new_status, resolved_at, signal_id),
        )
        self._db.commit()
        return cur.rowcount > 0

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def list_signals(
        self,
        status: Optional[str] = None,
        severity: Optional[str] = None,
        source: Optional[str] = None,
        limit: int = 200,
    ) -> list[dict]:
        """Return signals ordered by event_time DESC, id DESC."""
        clauses = []
        params: list = []

        if status:
            clauses.append("status = ?")
            params.append(status.strip().upper())
        if severity:
            clauses.append("severity = ?")
            params.append(severity.strip().upper())
        if source:
            clauses.append("source = ?")
            params.append(source.strip())

        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(max(1, int(limit)))

        conn = self._db.connect()
        rows = conn.execute(
            f"""
            SELECT id, created_at, event_time, source, event_type, severity,
                   regime, score, symbol, asset_class, message, payload_json,
                   status, resolved_at, notes
              FROM signal_alerts
             {where}
             ORDER BY event_time DESC, id DESC
             LIMIT ?
            """,
            params,
        ).fetchall()
        return [dict(r) for r in rows]

    def count_open_signals(self) -> int:
        conn = self._db.connect()
        row = conn.execute(
            "SELECT COUNT(*) FROM signal_alerts WHERE status = 'OPEN'"
        ).fetchone()
        return row[0] if row else 0

    def recent_open_signals(self, limit: int = 5) -> list[dict]:
        return self.list_signals(status="OPEN", limit=limit)
