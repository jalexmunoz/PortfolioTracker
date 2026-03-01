# Portfolio Tracker v2 — Phase A.1 Foundation

## Structure

```
portfolio_tracker_v2/
├── core/
│   ├── __init__.py          # Package exports
│   ├── database.py          # SQLite connection + schema
│   ├── exceptions.py        # Custom exceptions
│   └── asset_resolver.py    # Asset registry
├── migration/               # (empty, for Phase A.2)
├── services/                # (empty, for Phase B)
├── engine/                  # (empty, for Phase B)
├── ui/                      # (empty, for Phase C)
├── tests/                   # (empty, for Phase A.3)
├── scripts/
│   └── init_db.py           # One-shot DB init
├── config.py                # Configuration
├── requirements.txt         # Dependencies
├── pytest.ini               # Test config
└── portfolio.db             # SQLite (created by init_db.py)
```

## How to Use (Phase A.1)

### 1. Install Dependencies

```bash
cd c:\imp
pip install -r portfolio_tracker_v2\requirements.txt
```

### 2. Initialize Database

```bash
python -m portfolio_tracker_v2.scripts.init_db
```

This will:
- Create `portfolio.db` with schema
- Insert `__USD_CASH__` asset
- Load builtin assets (BTC, ETH, AAPL, etc.)

### 3. Test Imports

```python
from portfolio_tracker_v2 import Database, AssetResolver, PortfolioError
from portfolio_tracker_v2.core import DatabaseError

db = Database('portfolio_tracker_v2/portfolio.db')
db.connect()
resolver = AssetResolver(db)

# Resolve BTC
asset = resolver.resolve('BTC')
print(asset)  # {'id': 2, 'symbol': 'BTC', 'asset_type': 'crypto', ...}

# Normalize and alias
asset = resolver.resolve('  GOOGLE  ')
print(asset['symbol'])  # 'GOOG' (normalized + aliased)

# USD_CASH
usd_cash = resolver.get_or_create_usd_cash()
print(usd_cash['symbol'])  # '__USD_CASH__'

db.close()
```

## Next: Phase A.2

Phase A.2 focuses on ingesting data from the legacy `portfoliototal.csv` into the
new transactional schema.  Only import logic is included here; reconciliation,
lot matching and GUI will follow in later phases.

Key components created in this phase:

* `migration/validator.py` – ensures the CSV has the right columns and
  reasonable numerical values.  Produces `ValidationReport` containing counts,
  unique symbol/account sets and running total.
* `migration/csv_importer.py` – uses the validator and a database/asset resolver
  to insert rows atomically as `MIGRATION_BUY` transactions.  Supports a
  `dry_run()` for preview and will rollback on any reconciliation mismatch.
* Unit tests under `tests/` covering validator, importer, dry‑run,
  reconciliation failure, and a sample fixture CSV.

Refer to the expanded documentation in this repository for CSV format
requirements and usage examples.

Subsequent phases will take the imported `MIGRATION_BUY` rows and match them
into lots, handle portfolio calculations, and eventually power the GUI.

## Notes

- All imports use absolute paths: `from portfolio_tracker_v2.core import ...`
- Execute from `c:\imp` directory
- Database is local SQLite at `portfolio_tracker_v2/portfolio.db`
- Foreign keys are enabled (PRAGMA foreign_keys = ON)
