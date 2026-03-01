import os
from decimal import Decimal
import sqlite3

import pytest
from portfolio_tracker_v2.migration.csv_importer import CSVImporter, ImportReport
from portfolio_tracker_v2.core import Database
from portfolio_tracker_v2.core.asset_resolver import AssetResolver
from portfolio_tracker_v2.migration.validator import validate_csv, ValidationReport

TEST_CSV = os.path.join(os.path.dirname(__file__), 'fixtures', 'sample.csv')


def setup_test_db(tmp_path):
    db = Database(str(tmp_path / 'test.db'))
    db.init_schema()
    return db


def test_validate_good_csv(tmp_path):
    # re-use sample via fixtures
    report = validate_csv(TEST_CSV)
    assert report.errors == []
    assert report.row_count == 3
    assert report.total_cost_sum == Decimal('350.00')


def test_importer_dry_run(tmp_path):
    db = setup_test_db(tmp_path)
    resolver = AssetResolver(db)
    importer = CSVImporter(db, resolver, TEST_CSV)

    report = importer.dry_run()
    assert isinstance(report, ImportReport)
    assert report.row_count == 3
    assert report.transactions_added == 0
    assert report.total_cost_sum == Decimal('350.00')


def test_importer_execute(tmp_path):
    db = setup_test_db(tmp_path)
    resolver = AssetResolver(db)
    importer = CSVImporter(db, resolver, TEST_CSV)

    report = importer.execute()
    assert report.row_count == 3
    assert report.transactions_added == 3
    assert report.total_cost_sum == Decimal('350.00')

    # verify rows in DB
    conn = db.connect()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM transactions WHERE tx_type='MIGRATION_BUY'")
    assert cursor.fetchone()[0] == 3


def test_importer_execute_reconciliation_failure(tmp_path):
    # simulate mismatch by editing validator to mis-report
    # easiest: monkeypatch validate_csv to return wrong count
    db = setup_test_db(tmp_path)
    resolver = AssetResolver(db)

    # simulate mismatch by providing a validator that returns wrong counts
    def fake_validate(path):
        report = validate_csv(path)
        report.row_count = 2
        return report

    importer = CSVImporter(db, resolver, TEST_CSV, validator=fake_validate)

    with pytest.raises(Exception):
        importer.execute()


def test_validate_missing_column(tmp_path):
    # csv lacking wallet column should fail
    bad_csv = tmp_path / "bad.csv"
    bad_csv.write_text("Symbol,Quantity,Total Cost (USD)\nBTC,1,100\n")
    report = validate_csv(str(bad_csv))
    assert report.errors
