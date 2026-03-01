"""
SQLite database connection and schema initialization.
"""

import sqlite3
from pathlib import Path
from typing import Optional, Tuple, List

from .exceptions import DatabaseError


class Database:
    """
    Simple SQLite database wrapper with schema initialization.
    
    No connection pooling for MVP - simple single connection.
    Enforces PRAGMA foreign_keys = ON.
    """
    
    def __init__(self, db_path: str):
        """
        Initialize database connection (not opened yet).
        
        Args:
            db_path: Path to SQLite database file.
        """
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
    
    def connect(self) -> sqlite3.Connection:
        """
        Open connection to database (or return existing one).
        
        Enables PRAGMA foreign_keys and row factory for dict-like access.
        
        For file-based databases this is a no-op after first call; for
        in-memory databases it is *destructive* to reopen, so reuse is
        critical.
        
        Returns:
            sqlite3.Connection object.
        
        Raises:
            DatabaseError if connection fails.
        """
        if self.conn:
            # connection already open, reuse it
            return self.conn
        try:
            self.conn = sqlite3.connect(self.db_path)
            # Enable foreign key constraints
            self.conn.execute("PRAGMA foreign_keys = ON")
            # Enable dict-like row access
            self.conn.row_factory = sqlite3.Row
            return self.conn
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to connect to {self.db_path}: {e}")
    
    def close(self) -> None:
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def execute(self, sql: str, params: Optional[Tuple] = None) -> sqlite3.Cursor:
        """
        Execute a single SQL query.
        
        Args:
            sql: SQL query string.
            params: Tuple of query parameters.
        
        Returns:
            sqlite3.Cursor with results.
        
        Raises:
            DatabaseError if query fails or connection not open.
        """
        if not self.conn:
            self.connect()
        
        try:
            if params is not None:
                return self.conn.execute(sql, params)
            return self.conn.execute(sql)
        except sqlite3.Error as e:
            raise DatabaseError(f"Query failed: {e}\nSQL: {sql}")
    
    def executemany(self, sql: str, params: List[Tuple]) -> None:
        """
        Execute batch insert/update.
        
        Args:
            sql: SQL query string with placeholders.
            params: List of tuples, one per row.
        
        Raises:
            DatabaseError if query fails.
        """
        if not self.conn:
            self.connect()
        
        try:
            self.conn.executemany(sql, params)
        except sqlite3.Error as e:
            raise DatabaseError(f"Batch query failed: {e}\nSQL: {sql}")
    
    def commit(self) -> None:
        """Commit current transaction."""
        if self.conn:
            self.conn.commit()
    
    def rollback(self) -> None:
        """Rollback current transaction."""
        if self.conn:
            self.conn.rollback()
    
    def init_schema(self) -> None:
        """
        Initialize database schema with all 6 tables.
        
        Creates tables:
        - assets
        - accounts
        - transactions
        - lot_matches
        - price_cache
        - alerts
        
        Raises:
            DatabaseError if schema creation fails.
        """
        if not self.conn:
            self.connect()
        
        schema_sql = """
        -- 1. ASSETS
        CREATE TABLE IF NOT EXISTS assets (
            id INTEGER PRIMARY KEY,
            symbol TEXT UNIQUE NOT NULL,
            asset_type TEXT NOT NULL,
            tradingview_symbol TEXT,
            exchange TEXT,
            currency TEXT DEFAULT 'USD',
            divisor REAL DEFAULT 1.0,
            is_active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- 2. ACCOUNTS
        CREATE TABLE IF NOT EXISTS accounts (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            account_type TEXT DEFAULT 'wallet',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- 3. TRANSACTIONS (append-only, source of truth)
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
        
        -- 4. LOT_MATCHES (FIFO matching for realized P&L)
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
        
        -- 5. PRICE_CACHE (snapshots, not source of truth)
        CREATE TABLE IF NOT EXISTS price_cache (
            id INTEGER PRIMARY KEY,
            asset_id INTEGER NOT NULL,
            price_usd REAL NOT NULL,
            source TEXT DEFAULT 'tradingview',
            fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (asset_id) REFERENCES assets(id)
        );
        CREATE INDEX IF NOT EXISTS idx_price_latest ON price_cache(asset_id, fetched_at DESC);
        
        -- 6. ALERTS
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
            self.commit()
        except sqlite3.Error as e:
            raise DatabaseError(f"Schema initialization failed: {e}")
