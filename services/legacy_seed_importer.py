"""
Legacy snapshot seed importer for portfoliototal.csv open positions.
"""
from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import List, Optional

from portfolio_tracker_v2.core import Database
from portfolio_tracker_v2.core.asset_resolver import AssetResolver


REQUIRED_COLUMNS = [
    "Symbol",
    "Quantity",
    "Total Cost (USD)",
    "Avg Cost (USD)",
    "Wallet",
]

CONSISTENCY_TOLERANCE_USD = Decimal("0.05")
LEGACY_SEED_NOTE = "Seeded from legacy portfoliototal.csv"
SPECIAL_BBVA_CDT = "BBVA CDT"
SPECIAL_FONDO_DINAMICO = "FONDO DINAMICO"


@dataclass
class RejectedLegacySeedRow:
    row_number: int
    reason: str


@dataclass
class ImportLegacyPositionsResult:
    file_path: str
    seed_date: str
    total_rows: int
    imported_rows: int
    rejected_rows: List[RejectedLegacySeedRow]
    dry_run: bool = False


@dataclass
class ParsedLegacySeedRow:
    row_number: int
    symbol: str
    account: str
    quantity: Decimal
    unit_cost: Decimal
    total_cost: Decimal
    notes: str


class LegacySeedImportError(Exception):
    """Operational CSV import error that should stop the command with exit 2."""


class LegacyPositionsCsvImporter:
    """Import legacy open positions snapshot as MIGRATION_BUY seed lots."""

    def __init__(self, db: Database, resolver: AssetResolver):
        self.db = db
        self.resolver = resolver

    def import_file(self, csv_path: str, seed_date: str, dry_run: bool = False) -> ImportLegacyPositionsResult:
        path = Path(csv_path)
        if not path.exists():
            raise LegacySeedImportError(f"file not found: {csv_path}")
        if not path.is_file():
            raise LegacySeedImportError(f"path is not a file: {csv_path}")

        self._parse_seed_date(seed_date)

        try:
            with path.open("r", encoding="utf-8-sig", newline="") as fh:
                reader = csv.DictReader(fh)
                if reader.fieldnames is None:
                    raise LegacySeedImportError("CSV file is empty or missing header row")
                self._validate_required_columns(reader.fieldnames)

                conn = self.db.connect()
                cursor = conn.cursor()
                cursor.execute("BEGIN")

                total_rows = 0
                imported_rows = 0
                rejected_rows: List[RejectedLegacySeedRow] = []

                try:
                    for row_number, raw_row in enumerate(reader, start=2):
                        total_rows += 1
                        parsed_row, validation_error = self._parse_row(row_number, raw_row)
                        if validation_error:
                            rejected_rows.append(RejectedLegacySeedRow(row_number=row_number, reason=validation_error))
                            continue

                        self._insert_seed_row(cursor, parsed_row, seed_date, imported_rows + 1)
                        imported_rows += 1

                    if rejected_rows or dry_run:
                        conn.rollback()
                    else:
                        conn.commit()
                except Exception:
                    conn.rollback()
                    raise

                return ImportLegacyPositionsResult(
                    file_path=str(path),
                    seed_date=seed_date,
                    total_rows=total_rows,
                    imported_rows=imported_rows,
                    rejected_rows=rejected_rows,
                    dry_run=dry_run,
                )
        except LegacySeedImportError:
            raise
        except OSError as exc:
            raise LegacySeedImportError(f"failed to read CSV file: {exc}") from exc
        except csv.Error as exc:
            raise LegacySeedImportError(f"invalid CSV format: {exc}") from exc

    def _validate_required_columns(self, fieldnames: List[str]) -> None:
        normalized = {name.strip() for name in fieldnames if name is not None}
        missing = [name for name in REQUIRED_COLUMNS if name not in normalized]
        if missing:
            raise LegacySeedImportError("missing required columns: " + ", ".join(missing))

    def _parse_row(self, row_number: int, raw_row: dict) -> tuple[Optional[ParsedLegacySeedRow], Optional[str]]:
        try:
            symbol = self._require_text(raw_row.get("Symbol"), "Symbol").upper()
            account = self._require_text(raw_row.get("Wallet"), "Wallet")
            quantity = self._parse_decimal(raw_row.get("Quantity"), "Quantity")
            total_cost = self._parse_decimal(raw_row.get("Total Cost (USD)"), "Total Cost (USD)")
            avg_cost = self._parse_decimal(raw_row.get("Avg Cost (USD)"), "Avg Cost (USD)")
        except ValueError as exc:
            return None, str(exc)

        if quantity <= 0:
            return None, "Quantity must be > 0"
        if total_cost <= 0:
            return None, "Total Cost (USD) must be > 0"
        if avg_cost <= 0:
            return None, "Avg Cost (USD) must be > 0"

        unit_cost = avg_cost
        if symbol == SPECIAL_BBVA_CDT:
            unit_cost = (total_cost / quantity).quantize(Decimal("0.00000001"))
        else:
            expected_total = (quantity * avg_cost).quantize(Decimal("0.00000001"))
            difference = abs(expected_total - total_cost)
            if difference > CONSISTENCY_TOLERANCE_USD:
                return None, (
                    "Quantity * Avg Cost (USD) does not match Total Cost (USD) "
                    f"(difference {difference})"
                )

        notes = f"{LEGACY_SEED_NOTE} (row {row_number})"
        return ParsedLegacySeedRow(
            row_number=row_number,
            symbol=symbol,
            account=account,
            quantity=quantity,
            unit_cost=unit_cost,
            total_cost=total_cost,
            notes=notes,
        ), None

    def _insert_seed_row(self, cursor, row: ParsedLegacySeedRow, seed_date: str, sort_order: int) -> None:
        asset = self.resolver.resolve(row.symbol)
        account_id = self._get_or_create_account(cursor, row.account)
        cursor.execute(
            """
            INSERT INTO transactions
            (asset_id, account_id, tx_type, quantity, unit_price, fee_usd, total_usd, tx_date, sort_order, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                asset["id"],
                account_id,
                "MIGRATION_BUY",
                float(row.quantity),
                float(row.unit_cost),
                0.0,
                float(row.total_cost),
                seed_date,
                sort_order,
                row.notes,
            ),
        )

        if row.symbol == SPECIAL_FONDO_DINAMICO:
            manual_unit_value = (row.total_cost * Decimal("1.02") / row.quantity).quantize(Decimal("0.00000001"))
            cursor.execute(
                """
                UPDATE assets
                SET current_price = ?, price_source = ?, price_updated_at = ?
                WHERE id = ?
                """,
                (float(manual_unit_value), "snapshot_imported", seed_date, asset["id"]),
            )
            return

        if asset.get("valuation_method") == "market_live":
            cursor.execute(
                """
                UPDATE assets
                SET current_price = COALESCE(current_price, ?),
                    price_source = COALESCE(price_source, ?),
                    price_updated_at = COALESCE(price_updated_at, ?)
                WHERE id = ?
                """,
                (float(row.unit_cost), "csv_bootstrap", seed_date, asset["id"]),
            )

    def _get_or_create_account(self, cursor, name: str) -> int:
        cursor.execute("SELECT id FROM accounts WHERE name = ?", (name,))
        row = cursor.fetchone()
        if row:
            return row[0]
        cursor.execute("INSERT INTO accounts (name) VALUES (?)", (name,))
        return cursor.lastrowid

    def _parse_seed_date(self, value: str) -> str:
        raw = self._require_text(value, "seed_date")
        try:
            return datetime.strptime(raw, "%Y-%m-%d").date().isoformat()
        except ValueError as exc:
            raise LegacySeedImportError("seed_date must use YYYY-MM-DD") from exc

    def _parse_decimal(self, value: Optional[str], field_name: str) -> Decimal:
        raw = self._require_text(value, field_name)
        cleaned = raw.replace("$", "").replace(",", "").strip()
        try:
            return Decimal(cleaned)
        except (InvalidOperation, TypeError) as exc:
            raise ValueError(f"{field_name} must be numeric") from exc

    def _require_text(self, value: Optional[str], field_name: str) -> str:
        text = self._optional_text(value)
        if text is None:
            raise ValueError(f"{field_name} is required")
        return text

    def _optional_text(self, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        return text or None
