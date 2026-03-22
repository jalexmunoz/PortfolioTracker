"""
Minimal CSV transaction importer for BUY/SELL ledger entries.
"""
from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import List, Optional

from portfolio_tracker_v2.core.exceptions import InvalidTransaction
from portfolio_tracker_v2.services.transaction_svc import TransactionService


REQUIRED_COLUMNS = [
    "trade_date",
    "account",
    "symbol",
    "side",
    "quantity",
    "unit_price",
]


@dataclass
class RejectedRow:
    row_number: int
    reason: str


@dataclass
class ImportTransactionsCsvResult:
    file_path: str
    total_rows: int
    imported_rows: int
    rejected_rows: List[RejectedRow]


@dataclass
class ParsedTransactionRow:
    trade_date: str
    account: str
    symbol: str
    side: str
    quantity: Decimal
    unit_price: Decimal
    fee: Decimal
    notes: Optional[str]


class TransactionCsvImportError(Exception):
    """Operational CSV import error that should stop the command with exit 2."""


class TransactionCsvImporter:
    """Read one explicit CSV format and import valid rows through TransactionService."""

    def __init__(self, transaction_service: TransactionService):
        self.transaction_service = transaction_service

    def import_file(self, csv_path: str) -> ImportTransactionsCsvResult:
        path = Path(csv_path)
        if not path.exists():
            raise TransactionCsvImportError(f"file not found: {csv_path}")
        if not path.is_file():
            raise TransactionCsvImportError(f"path is not a file: {csv_path}")

        try:
            with path.open("r", encoding="utf-8-sig", newline="") as fh:
                reader = csv.DictReader(fh)
                if reader.fieldnames is None:
                    raise TransactionCsvImportError("CSV file is empty or missing header row")
                self._validate_required_columns(reader.fieldnames)

                total_rows = 0
                imported_rows = 0
                rejected_rows: List[RejectedRow] = []

                for row_number, raw_row in enumerate(reader, start=2):
                    total_rows += 1
                    parsed_row, validation_error = self._parse_row(raw_row)
                    if validation_error:
                        rejected_rows.append(RejectedRow(row_number=row_number, reason=validation_error))
                        continue

                    try:
                        self._record_row(parsed_row)
                        imported_rows += 1
                    except InvalidTransaction as exc:
                        rejected_rows.append(RejectedRow(row_number=row_number, reason=str(exc)))
                    except Exception as exc:
                        raise TransactionCsvImportError(
                            f"unexpected service error at row {row_number}: {exc}"
                        ) from exc

                return ImportTransactionsCsvResult(
                    file_path=str(path),
                    total_rows=total_rows,
                    imported_rows=imported_rows,
                    rejected_rows=rejected_rows,
                )
        except TransactionCsvImportError:
            raise
        except OSError as exc:
            raise TransactionCsvImportError(f"failed to read CSV file: {exc}") from exc
        except csv.Error as exc:
            raise TransactionCsvImportError(f"invalid CSV format: {exc}") from exc

    def _validate_required_columns(self, fieldnames: List[str]) -> None:
        normalized = {name.strip() for name in fieldnames if name is not None}
        missing = [name for name in REQUIRED_COLUMNS if name not in normalized]
        if missing:
            raise TransactionCsvImportError(
                "missing required columns: " + ", ".join(missing)
            )

    def _parse_row(self, raw_row: dict) -> tuple[Optional[ParsedTransactionRow], Optional[str]]:
        try:
            trade_date = self._parse_trade_date(raw_row.get("trade_date"))
            account = self._require_text(raw_row.get("account"), "account")
            symbol = self._require_text(raw_row.get("symbol"), "symbol").upper()
            side = self._parse_side(raw_row.get("side"))
            quantity = self._parse_decimal(raw_row.get("quantity"), "quantity")
            unit_price = self._parse_decimal(raw_row.get("unit_price"), "unit_price")
            fee = self._parse_optional_decimal(raw_row.get("fee"), "fee", default=Decimal("0"))
            notes = self._optional_text(raw_row.get("notes"))
        except ValueError as exc:
            return None, str(exc)

        if quantity <= 0:
            return None, "quantity must be > 0"
        if unit_price < 0:
            return None, "unit_price must be >= 0"
        if fee < 0:
            return None, "fee must be >= 0"

        return ParsedTransactionRow(
            trade_date=trade_date,
            account=account,
            symbol=symbol,
            side=side,
            quantity=quantity,
            unit_price=unit_price,
            fee=fee,
            notes=notes,
        ), None

    def _record_row(self, row: ParsedTransactionRow) -> None:
        if row.side == "BUY":
            self.transaction_service.record_buy(
                symbol=row.symbol,
                account=row.account,
                qty=row.quantity,
                unit_price=row.unit_price,
                fee_usd=row.fee,
                tx_date=row.trade_date,
                notes=row.notes,
            )
            return

        self.transaction_service.record_sell(
            symbol=row.symbol,
            account=row.account,
            qty=row.quantity,
            unit_price=row.unit_price,
            fee_usd=row.fee,
            tx_date=row.trade_date,
            notes=row.notes,
        )

    def _parse_trade_date(self, value: Optional[str]) -> str:
        raw = self._require_text(value, "trade_date")
        try:
            return datetime.strptime(raw, "%Y-%m-%d").date().isoformat()
        except ValueError as exc:
            raise ValueError("trade_date must use YYYY-MM-DD") from exc

    def _parse_side(self, value: Optional[str]) -> str:
        side = self._require_text(value, "side").upper()
        if side not in {"BUY", "SELL"}:
            raise ValueError("side must be BUY or SELL")
        return side

    def _parse_decimal(self, value: Optional[str], field_name: str) -> Decimal:
        raw = self._require_text(value, field_name)
        try:
            return Decimal(raw)
        except (InvalidOperation, TypeError) as exc:
            raise ValueError(f"{field_name} must be numeric") from exc

    def _parse_optional_decimal(self, value: Optional[str], field_name: str, default: Decimal) -> Decimal:
        text = self._optional_text(value)
        if text is None:
            return default
        try:
            return Decimal(text)
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
