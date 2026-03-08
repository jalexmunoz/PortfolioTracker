"""
CSV validation utilities for migration.
"""
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import List, Set, Optional
import csv


@dataclass
class ValidationReport:
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    total_rows: int = 0
    valid_row_count: int = 0
    unique_symbols: Set[str] = field(default_factory=set)
    unique_accounts: Set[str] = field(default_factory=set)
    total_cost_sum: Decimal = Decimal('0')


REQUIRED_COLUMNS = ['symbol', 'quantity', 'total cost (usd)', 'wallet']

DEFAULT_DATE = '2000-01-01'


def _clean_number(value: str) -> Optional[Decimal]:
    if value is None:
        return None
    try:
        # remove $ commas spaces
        cleaned = value.replace('$', '').replace(',', '').strip()
        if cleaned == '':
            return None
        return Decimal(cleaned)
    except InvalidOperation:
        return None


def validate_csv(csv_path: str) -> ValidationReport:
    report = ValidationReport()

    with open(csv_path, newline='', encoding='utf-8', errors='ignore') as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            report.errors.append('CSV file is empty')
            return report

        # normalize header to lowercase
        header_norm = [h.strip().lower() for h in header]

        missing = [col for col in REQUIRED_COLUMNS if col not in header_norm]
        if missing:
            report.errors.append(f"Missing required columns: {missing}")
            return report

        # identify indices
        idx = {name: header_norm.index(name) for name in REQUIRED_COLUMNS}
        # optional date column if present
        date_idx = header_norm.index('date') if 'date' in header_norm else None

        for row_num, row in enumerate(reader, start=1):
            report.total_rows += 1

            # guard row length
            if len(row) < len(header):
                report.errors.append(f"Row {row_num}: fewer columns than header")
                continue

            symbol = row[idx['symbol']].strip()
            wallet = row[idx['wallet']].strip()
            qty_raw = row[idx['quantity']]
            cost_raw = row[idx['total cost (usd)']]

            if not symbol:
                report.errors.append(f"Row {row_num}: empty Symbol")
                continue
            if not wallet:
                report.errors.append(f"Row {row_num}: empty Wallet")
                continue

            qty = _clean_number(qty_raw)
            cost = _clean_number(cost_raw)

            # Policy: skip rows with qty=0 and cost=0 as placeholders
            if qty == 0 and cost == 0:
                report.warnings.append(f"Row {row_num}: skipped placeholder row (qty=0, cost=0)")
                continue

            if qty is None or qty <= 0:
                report.errors.append(f"Row {row_num}: invalid Quantity '{qty_raw}'")
                continue
            if cost is None or cost <= 0:
                report.errors.append(f"Row {row_num}: invalid Total Cost '{cost_raw}'")
                continue

            # Valid row
            report.valid_row_count += 1
            # accumulate
            report.unique_symbols.add(symbol.upper())
            report.unique_accounts.add(wallet)
            report.total_cost_sum += cost

    return report
