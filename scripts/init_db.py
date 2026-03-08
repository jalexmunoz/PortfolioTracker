"""
Initialize portfolio database with schema and default assets.

Usage:
    python -m portfolio_tracker_v2.scripts.init_db
"""

import sys
from pathlib import Path

from portfolio_tracker_v2.core import Database, AssetResolver
from portfolio_tracker_v2.config import DB_PATH


def main():
    """
    Initialize database:
    1. Create connection
    2. Create schema
    3. Insert __USD_CASH__ asset
    """
    print(f"Initializing database at {DB_PATH}...")
    
    try:
        # 1. Connect and create schema
        db = Database(DB_PATH)
        db.connect()
        print("  ✅ Connected to database")
        
        db.init_schema()
        print("  ✅ Schema initialized")
        
        # 2. Create AssetResolver and add __USD_CASH__
        resolver = AssetResolver(db)
        usd_cash = resolver.get_or_create_usd_cash()
        print(f"  ✅ USD_CASH asset created (id={usd_cash['id']})")
        
        # 3. Optionally load some builtin assets
        builtin_symbols = ['BTC', 'ETH', 'AAPL', 'TSLA', 'GOLD', 'SILVER']
        for symbol in builtin_symbols:
            try:
                asset = resolver.resolve(symbol)
                # print(f"     • {symbol}: {asset['asset_type']}")
            except Exception as e:
                print(f"  ⚠️  Failed to load {symbol}: {e}")
        
        print(f"  ✅ Builtin assets loaded")
        
        # 4. Close and report
        db.close()
        print("\n✅ Database initialization complete!")
        print(f"   DB path: {DB_PATH}")
        print("\nNext steps:")
        print("   1. Prepare your legacy CSV file (e.g., portfoliototal.csv)")
        print("   2. Run: python -m portfolio_tracker_v2 import-csv --input <PATH_TO_CSV>")
        print("   3. If dry-run looks good, run: python -m portfolio_tracker_v2 import-csv --input <PATH_TO_CSV> --execute")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
