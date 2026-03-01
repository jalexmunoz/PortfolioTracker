"""
CSV importer for migration into SQLite.
"""
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional
import csv

from portfolio_tracker_v2.core import Database
from portfolio_tracker_v2.core.asset_resolver import AssetResolver
from portfolio_tracker_v2.core.exceptions import CSVImportError, DatabaseError
from portfolio_tracker_v2.migration.validator import validate_csv, ValidationReport


@dataclass
class ImportReport:
    row_count: int
    transactions_added: int
    total_cost_sum: Decimal


class CSVImporter:
    def __init__(self, db: Database, resolver: AssetResolver, csv_path: str, validator=validate_csv):
        self.db = db
        self.resolver = resolver
        self.csv_path = csv_path
        # validator is a callable taking path and returning ValidationReport
        self.validator = validator

    def dry_run(self) -> ImportReport:
        """Validate and summarize without writing to DB."""
        report = self.validator(self.csv_path)
        if report.errors:
            raise CSVImportError('; '.join(report.errors))

        # row_count and total already computed
        return ImportReport(
            row_count=report.row_count,
            transactions_added=0,
            total_cost_sum=report.total_cost_sum
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
            for row in self._row_generator():
                asset = self.resolver.resolve(row['Symbol'])
                account_id = self._get_or_create_account(row['Wallet'], cursor)

                quantity = Decimal(str(row['Quantity']))
                cost = Decimal(str(row['Total Cost (USD)']))
                unit_price = cost / quantity if quantity != 0 else Decimal('0')

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
                        float(cost),
                        row.get('Date') or '2000-01-01',
                        None,
                        'imported_from_csv',
                    )
                )
                added += 1
                total_cost += cost

            # reconciliation
            cursor.execute(
                "SELECT COUNT(*), SUM(total_usd) FROM transactions WHERE tx_type = 'MIGRATION_BUY'"
            )
            cnt, sum_usd = cursor.fetchone()
            # allow tiny float discrepancy
            if cnt != report.row_count:
                raise CSVImportError(f"Row count mismatch after insert: {cnt} vs {report.row_count}")
            if abs(Decimal(str(sum_usd)) - report.total_cost_sum) > Decimal('0.01'):
                raise CSVImportError(f"Total cost mismatch after insert: {sum_usd} vs {report.total_cost_sum}")

            conn.commit()
            return ImportReport(row_count=report.row_count, transactions_added=added, total_cost_sum=total_cost)
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
