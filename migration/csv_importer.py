"""
CSV importer for migration into SQLite.
"""
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Optional, List, Set
import csv

from portfolio_tracker_v2.core import Database
from portfolio_tracker_v2.core.asset_resolver import AssetResolver
from portfolio_tracker_v2.core.exceptions import CSVImportError, DatabaseError
from portfolio_tracker_v2.migration.validator import validate_csv, ValidationReport


@dataclass
class ImportReport:
    total_rows: int
    valid_row_count: int
    transactions_added: int
    total_cost_sum: Decimal
    warnings: List[str] = field(default_factory=list)
    unique_symbols: Set[str] = field(default_factory=set)
    unique_accounts: Set[str] = field(default_factory=set)


class CSVImporter:
    def __init__(self, db: Database, resolver: AssetResolver, csv_path: str, validator=validate_csv):
        self.db = db
        self.resolver = resolver
        self.csv_path = csv_path
        # validator is a callable taking path and returning ValidationReport
        self.validator = validator

    def _clean_number(self, value: str):
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

    def dry_run(self) -> ImportReport:
        """Validate and summarize without writing to DB."""
        report = self.validator(self.csv_path)
        if report.errors:
            raise CSVImportError('; '.join(report.errors))

        # row_count and total already computed
        return ImportReport(
            total_rows=report.total_rows,
            valid_row_count=report.valid_row_count,
            transactions_added=0,
            total_cost_sum=report.total_cost_sum,
            warnings=report.warnings,
            unique_symbols=report.unique_symbols,
            unique_accounts=report.unique_accounts
        )

    def execute(self) -> ImportReport:
        """Perform atomic import. Return counts and sums."""
        # first validate
        report = self.validator(self.csv_path)
        if report.errors:
            raise CSVImportError('; '.join(report.errors))

        conn = self.db.connect()
        cursor = conn.cursor()
        added = 0
        total_cost = Decimal('0')

        try:
            cursor.execute('BEGIN')
            # replicate validation loop but insert valid rows
            with open(self.csv_path, newline='', encoding='utf-8', errors='ignore') as f:
                reader = csv.reader(f)
                header = next(reader)
                header_norm = [h.strip().lower() for h in header]
                required = ['symbol', 'quantity', 'total cost (usd)', 'wallet']
                missing = [col for col in required if col not in header_norm]
                if missing:
                    raise CSVImportError(f"Missing required columns: {missing}")
                idx = {name: header_norm.index(name) for name in required}
                price_idx = header_norm.index('price (usd)') if 'price (usd)' in header_norm else None
                date_idx = header_norm.index('date') if 'date' in header_norm else None

                for row_num, row in enumerate(reader, start=1):
                    if len(row) < len(header):
                        continue

                    symbol = row[idx['symbol']].strip()
                    wallet = row[idx['wallet']].strip()
                    qty_raw = row[idx['quantity']]
                    cost_raw = row[idx['total cost (usd)']]

                    if not symbol or not wallet:
                        continue

                    qty = self._clean_number(qty_raw)
                    cost = self._clean_number(cost_raw)

                    if qty == 0 and cost == 0:
                        continue

                    if qty is None or qty <= 0 or cost is None or cost <= 0:
                        continue

                    # insert
                    asset = self.resolver.resolve(symbol)
                    account_id = self._get_or_create_account(wallet, cursor)

                    # update current_price if available
                    if price_idx is not None and len(row) > price_idx:
                        price_raw = row[price_idx].strip()
                        price = self._clean_number(price_raw)
                        if price is not None:
                            cursor.execute("UPDATE assets SET current_price = ?, price_source = ?, price_updated_at = ? WHERE id = ?", (float(price), 'csv_bootstrap', '2000-01-01', asset['id']))

                    quantity = Decimal(str(qty))
                    cost_dec = Decimal(str(cost))
                    unit_price = cost_dec / quantity if quantity != 0 else Decimal('0')

                    cursor.execute(
                        """
                        INSERT INTO transactions
                        (asset_id, account_id, tx_type, quantity, unit_price, fee_usd, total_usd, tx_date, sort_order, notes)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            asset['id'],
                            account_id,
                            'MIGRATION_BUY',
                            float(quantity),
                            float(unit_price),
                            0.0,
                            float(cost_dec),
                            row[date_idx] if date_idx is not None and date_idx < len(row) and row[date_idx].strip() else '2000-01-01',
                            None,
                            'imported_from_csv',
                        )
                    )
                    added += 1
                    total_cost += cost_dec

            # reconciliation
            cursor.execute(
                "SELECT COUNT(*), SUM(total_usd) FROM transactions WHERE tx_type = 'MIGRATION_BUY'"
            )
            cnt, sum_usd = cursor.fetchone()
            # allow tiny float discrepancy
            if cnt != report.valid_row_count:
                raise CSVImportError(f"Row count mismatch after insert: {cnt} vs {report.valid_row_count}")
            if abs(Decimal(str(sum_usd)) - report.total_cost_sum) > Decimal('0.01'):
                raise CSVImportError(f"Total cost mismatch after insert: {sum_usd} vs {report.total_cost_sum}")

            conn.commit()
            return ImportReport(total_rows=report.total_rows, valid_row_count=report.valid_row_count, transactions_added=added, total_cost_sum=total_cost)
        except Exception:
            conn.rollback()
            raise

    def _row_generator(self):
        with open(self.csv_path, newline='', encoding='utf-8', errors='ignore') as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield row

    def _get_or_create_account(self, name: str, cursor) -> int:
        name = name.strip()
        cursor.execute("SELECT id FROM accounts WHERE name = ?", (name,))
        row = cursor.fetchone()
        if row:
            return row[0]
        cursor.execute("INSERT INTO accounts (name) VALUES (?)", (name,))
        return cursor.lastrowid
