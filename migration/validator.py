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
    row_count: int = 0
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
            report.row_count += 1

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
            if not wallet:
                report.errors.append(f"Row {row_num}: empty Wallet")

            qty = _clean_number(qty_raw)
            cost = _clean_number(cost_raw)

            if qty is None or qty <= 0:
                report.errors.append(f"Row {row_num}: invalid Quantity '{qty_raw}'")
            if cost is None or cost <= 0:
                report.errors.append(f"Row {row_num}: invalid Total Cost '{cost_raw}'")

            # accumulate
            if symbol:
                report.unique_symbols.add(symbol.upper())
            if wallet:
                report.unique_accounts.add(wallet)
            if cost is not None:
                report.total_cost_sum += cost

    return report
