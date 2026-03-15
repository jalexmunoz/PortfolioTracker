"""
SQLite database connection and schema initialization.
"""

import sqlite3
from typing import Optional, Tuple, List

from .exceptions import DatabaseError


class Database:
    """
    Simple SQLite database wrapper with schema initialization.

    No connection pooling for MVP - simple single connection.
    Enforces PRAGMA foreign_keys = ON.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None

    def connect(self) -> sqlite3.Connection:
        if self.conn:
            return self.conn
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.execute("PRAGMA foreign_keys = ON")
            self.conn.row_factory = sqlite3.Row
            return self.conn
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to connect to {self.db_path}: {e}")

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            self.conn = None

    def execute(self, sql: str, params: Optional[Tuple] = None) -> sqlite3.Cursor:
        if not self.conn:
            self.connect()
        try:
            if params is not None:
                return self.conn.execute(sql, params)
            return self.conn.execute(sql)
        except sqlite3.Error as e:
            raise DatabaseError(f"Query failed: {e}\nSQL: {sql}")

    def executemany(self, sql: str, params: List[Tuple]) -> None:
        if not self.conn:
            self.connect()
        try:
            self.conn.executemany(sql, params)
        except sqlite3.Error as e:
            raise DatabaseError(f"Batch query failed: {e}\nSQL: {sql}")

    def commit(self) -> None:
        if self.conn:
            self.conn.commit()

    def rollback(self) -> None:
        if self.conn:
            self.conn.rollback()

    def init_schema(self) -> None:
        if not self.conn:
            self.connect()

        schema_sql = """
        CREATE TABLE IF NOT EXISTS assets (
            id INTEGER PRIMARY KEY,
            symbol TEXT UNIQUE NOT NULL,
            asset_type TEXT NOT NULL,
            current_price REAL,
            price_source TEXT,
            price_updated_at TIMESTAMP,
            tradingview_symbol TEXT,
            exchange TEXT,
            currency TEXT DEFAULT 'USD',
            divisor REAL DEFAULT 1.0,
            valuation_method TEXT DEFAULT 'unvalued',
            is_active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS accounts (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            account_type TEXT DEFAULT 'wallet',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY,
            asset_id INTEGER NOT NULL,
            account_id INTEGER NOT NULL,
            tx_type TEXT NOT NULL,
            quantity REAL NOT NULL,
            unit_price REAL,
            fee_usd REAL DEFAULT 0,
            total_usd REAL NOT NULL,
            tx_date TIMESTAMP NOT NULL,
            sort_order INTEGER,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (asset_id) REFERENCES assets(id),
            FOREIGN KEY (account_id) REFERENCES accounts(id)
        );
        CREATE INDEX IF NOT EXISTS idx_tx_asset_date ON transactions(asset_id, tx_date, id);
        CREATE INDEX IF NOT EXISTS idx_tx_type ON transactions(tx_type);

        CREATE TABLE IF NOT EXISTS lot_matches (
            id INTEGER PRIMARY KEY,
            buy_tx_id INTEGER NOT NULL,
            sell_tx_id INTEGER NOT NULL,
            quantity REAL NOT NULL,
            buy_fee_alloc REAL DEFAULT 0,
            sell_fee_alloc REAL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (buy_tx_id) REFERENCES transactions(id),
            FOREIGN KEY (sell_tx_id) REFERENCES transactions(id)
        );
        CREATE INDEX IF NOT EXISTS idx_match_buysell ON lot_matches(buy_tx_id, sell_tx_id);

        CREATE TABLE IF NOT EXISTS price_cache (
            id INTEGER PRIMARY KEY,
            asset_id INTEGER NOT NULL,
            price_usd REAL NOT NULL,
            source TEXT DEFAULT 'tradingview',
            fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (asset_id) REFERENCES assets(id)
        );
        CREATE INDEX IF NOT EXISTS idx_price_latest ON price_cache(asset_id, fetched_at DESC);

        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY,
            asset_id INTEGER,
            alert_type TEXT NOT NULL,
            threshold_value REAL NOT NULL,
            is_active INTEGER DEFAULT 1,
            last_triggered_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (asset_id) REFERENCES assets(id)
        );
        """

        try:
            self.conn.executescript(schema_sql)
            self._ensure_assets_schema_extensions()
            self.commit()
        except sqlite3.Error as e:
            raise DatabaseError(f"Schema initialization failed: {e}")

    def _ensure_assets_schema_extensions(self) -> None:
        columns = {row['name'] for row in self.conn.execute("PRAGMA table_info(assets)").fetchall()}
        if 'valuation_method' not in columns:
            self.conn.execute("ALTER TABLE assets ADD COLUMN valuation_method TEXT DEFAULT 'unvalued'")
        if 'is_active' not in columns:
            self.conn.execute("ALTER TABLE assets ADD COLUMN is_active INTEGER DEFAULT 1")

        self.conn.execute("UPDATE assets SET valuation_method = 'contractual_value' WHERE symbol = 'BBVA CDT'")
        self.conn.execute("UPDATE assets SET valuation_method = 'snapshot_imported' WHERE symbol = 'FONDO DINAMICO'")
        self.conn.execute(
            """
            UPDATE assets
            SET valuation_method = 'market_live'
            WHERE valuation_method IN ('', 'unvalued')
              AND asset_type IN ('crypto', 'stablecoin', 'stock_us', 'commodity', 'stock_intl')
            """
        )
        self.conn.execute("UPDATE assets SET valuation_method = 'unvalued' WHERE valuation_method IS NULL OR valuation_method = ''")
        self.conn.execute("UPDATE assets SET is_active = 0 WHERE symbol IN ('BAS', 'SWTCH')")
