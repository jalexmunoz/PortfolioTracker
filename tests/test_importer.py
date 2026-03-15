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
    assert report.valid_row_count == 3
    assert report.total_rows == 3
    assert report.total_cost_sum == Decimal('350.00')


def test_importer_dry_run(tmp_path):
    db = setup_test_db(tmp_path)
    resolver = AssetResolver(db)
    importer = CSVImporter(db, resolver, TEST_CSV)

    report = importer.dry_run()
    assert isinstance(report, ImportReport)
    assert report.valid_row_count == 3
    assert report.transactions_added == 0
    assert report.total_cost_sum == Decimal('350.00')


def test_importer_execute(tmp_path):
    db = setup_test_db(tmp_path)
    resolver = AssetResolver(db)
    importer = CSVImporter(db, resolver, TEST_CSV)

    report = importer.execute()
    assert report.valid_row_count == 3
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
        report.valid_row_count = 2
        return report

    importer = CSVImporter(db, resolver, TEST_CSV, validator=fake_validate)

    with pytest.raises(Exception):
        importer.execute()


def test_validate_csv_with_placeholder_row(tmp_path):
    # csv with a placeholder row qty=0 cost=0
    csv_content = """Symbol,Quantity,Total Cost (USD),Wallet
BTC,1,100,Main
USDT,0,0,Bingx
ETH,2,200,Main"""
    csv_file = tmp_path / "placeholder.csv"
    csv_file.write_text(csv_content)
    report = validate_csv(str(csv_file))
    assert report.errors == []
    assert report.warnings == ["Row 2: skipped placeholder row (qty=0, cost=0)"]
    assert report.valid_row_count == 2
    assert report.total_rows == 3
    assert report.total_cost_sum == Decimal('300')


def test_importer_with_placeholder_row(tmp_path):
    csv_content = """Symbol,Quantity,Total Cost (USD),Wallet
BTC,1,100,Main
USDT,0,0,Bingx
ETH,2,200,Main"""
    csv_file = tmp_path / "placeholder.csv"
    csv_file.write_text(csv_content)
    
    db = setup_test_db(tmp_path)
    resolver = AssetResolver(db)
    importer = CSVImporter(db, resolver, str(csv_file))

    report = importer.execute()
    assert report.valid_row_count == 2  # valid rows
    assert report.transactions_added == 2
    assert report.total_cost_sum == Decimal('300')

    # verify only valid rows in DB
    conn = db.connect()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM transactions WHERE tx_type='MIGRATION_BUY'")
    assert cursor.fetchone()[0] == 2


def test_import_sets_price_metadata(tmp_path):
    csv_content = """Symbol,Quantity,Total Cost (USD),Price (USD),Wallet
BTC,1,100,150,Main"""
    csv_file = tmp_path / "price.csv"
    csv_file.write_text(csv_content)
    
    db = setup_test_db(tmp_path)
    resolver = AssetResolver(db)
    importer = CSVImporter(db, resolver, str(csv_file))

    importer.execute()
    
    # check price metadata
    conn = db.connect()
    cursor = conn.cursor()
    cursor.execute("SELECT current_price, price_source, price_updated_at FROM assets WHERE symbol = 'BTC'")
    row = cursor.fetchone()
    assert row[0] == 150.0
    assert row[1] == 'csv_bootstrap'
    assert row[2] == '2000-01-01'


def test_import_sets_snapshot_imported_source(tmp_path):
    csv_content = """Symbol,Quantity,Total Cost (USD),Price (USD),Wallet
FONDO DINAMICO,2,200,120,Main"""
    csv_file = tmp_path / "snapshot.csv"
    csv_file.write_text(csv_content)

    db = setup_test_db(tmp_path)
    resolver = AssetResolver(db)
    importer = CSVImporter(db, resolver, str(csv_file))

    importer.execute()

    conn = db.connect()
    cursor = conn.cursor()
    cursor.execute("SELECT current_price, price_source, valuation_method FROM assets WHERE symbol = 'FONDO DINAMICO'")
    row = cursor.fetchone()
    assert row[0] == 120.0
    assert row[1] == 'snapshot_imported'
    assert row[2] == 'snapshot_imported'
