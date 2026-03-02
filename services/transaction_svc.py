"""
Transaction service: record buys/sells with FIFO matching.
"""
from decimal import Decimal
from typing import Optional

from portfolio_tracker_v2.core import Database
from portfolio_tracker_v2.core.asset_resolver import AssetResolver
from portfolio_tracker_v2.core.exceptions import InvalidTransaction


class TransactionService:
    """
    Service for recording and matching transactions.
    
    Handles FIFO lot matching for sells against open buy positions.
    """
    
    def __init__(self, db: Database, resolver: AssetResolver):
        """
        Initialize TransactionService.
        
        Args:
            db: Database instance.
            resolver: AssetResolver for symbol resolution.
        """
        self.db = db
        self.resolver = resolver
    
    def _get_or_create_account(self, account_name: str, cursor) -> int:
        """
        Get or create account by name.
        
        Does NOT commit; caller controls transaction.
        
        Args:
            account_name: Name of the account.
            cursor: Active database cursor (within transaction).
        
        Returns:
            Account ID.
        """
        name = account_name.strip()
        cursor.execute("SELECT id FROM accounts WHERE name = ?", (name,))
        row = cursor.fetchone()
        if row:
            return row[0]
        
        # Insert within transaction; caller commits
        cursor.execute("INSERT INTO accounts (name) VALUES (?)", (name,))
        return cursor.lastrowid
    
    def record_buy(
        self,
        symbol: str,
        account: str,
        qty: Decimal,
        unit_price: Decimal,
        fee_usd: Decimal,
        tx_date: str,
        notes: Optional[str] = None,
    ) -> int:
        """
        Record a BUY transaction.
        
        Args:
            symbol: Asset symbol.
            account: Account name.
            qty: Quantity (Decimal).
            unit_price: Price per unit (Decimal).
            fee_usd: Fee in USD (Decimal).
            tx_date: Transaction date (YYYY-MM-DD).
            notes: Optional notes.
        
        Returns:
            Transaction ID.
        
        Raises:
            InvalidTransaction if parameters are invalid.
        """
        if qty <= 0:
            raise InvalidTransaction("Quantity must be positive")
        if unit_price <= 0:
            raise InvalidTransaction("Unit price must be positive")
        if fee_usd < 0:
            raise InvalidTransaction("Fee cannot be negative")
        
        asset = self.resolver.resolve(symbol)
        total_usd = qty * unit_price + fee_usd
        
        conn = self.db.connect()
        cursor = conn.cursor()
        try:
            # transaction begins automatically on first write
            account_id = self._get_or_create_account(account, cursor)
            
            cursor.execute(
                """
                INSERT INTO transactions
                (asset_id, account_id, tx_type, quantity, unit_price, fee_usd, total_usd, tx_date, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    asset['id'],
                    account_id,
                    'BUY',
                    float(qty),
                    float(unit_price),
                    float(fee_usd),
                    float(total_usd),
                    tx_date,
                    notes,
                )
            )
            tx_id = cursor.lastrowid
            conn.commit()
            return tx_id
        except Exception:
            conn.rollback()
            raise
    
    def record_sell(
        self,
        symbol: str,
        account: str,
        qty: Decimal,
        unit_price: Decimal,
        fee_usd: Decimal,
        tx_date: str,
        notes: Optional[str] = None,
    ) -> int:
        """
        Record a SELL transaction with FIFO matching.
        
        Matches sell qty against open buy positions (BUY + MIGRATION_BUY).
        Creates lot_matches entries. Validates sufficient holdings.
        
        Args:
            symbol: Asset symbol.
            account: Account name.
            qty: Quantity to sell (Decimal).
            unit_price: Price per unit (Decimal).
            fee_usd: Fee in USD (Decimal).
            tx_date: Transaction date (YYYY-MM-DD).
            notes: Optional notes.
        
        Returns:
            Transaction ID.
        
        Raises:
            InvalidTransaction if parameters invalid or insufficient holdings.
        """
        if qty <= 0:
            raise InvalidTransaction("Quantity must be positive")
        if unit_price <= 0:
            raise InvalidTransaction("Unit price must be positive")
        if fee_usd < 0:
            raise InvalidTransaction("Fee cannot be negative")
        
        asset = self.resolver.resolve(symbol)
        total_usd = qty * unit_price - fee_usd
        
        conn = self.db.connect()
        cursor = conn.cursor()
        try:
            # transaction begins automatically on first write
            account_id = self._get_or_create_account(account, cursor)
            
            # Insert SELL transaction first
            cursor.execute(
                """
                INSERT INTO transactions
                (asset_id, account_id, tx_type, quantity, unit_price, fee_usd, total_usd, tx_date, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    asset['id'],
                    account_id,
                    'SELL',
                    float(qty),
                    float(unit_price),
                    float(fee_usd),
                    float(total_usd),
                    tx_date,
                    notes,
                )
            )
            sell_tx_id = cursor.lastrowid
            
            # FIFO matching: find open buy positions
            cursor.execute(
                """
                SELECT id, quantity, fee_usd FROM transactions
                WHERE asset_id=? AND account_id=? AND tx_type IN ('BUY', 'MIGRATION_BUY')
                ORDER BY tx_date ASC, id ASC
                """,
                (asset['id'], account_id)
            )
            buy_rows = cursor.fetchall()
            
            qty_remaining = qty
            
            for buy_row in buy_rows:
                if qty_remaining <= 0:
                    break
                
                buy_tx_id = buy_row[0]
                buy_qty = Decimal(str(buy_row[1]))
                buy_fee = Decimal(str(buy_row[2]))
                
                # Calculate already matched qty for this buy
                cursor.execute(
                    "SELECT COALESCE(SUM(quantity), 0) FROM lot_matches WHERE buy_tx_id = ?",
                    (buy_tx_id,)
                )
                matched_qty = Decimal(str(cursor.fetchone()[0]))
                
                remaining_qty = buy_qty - matched_qty
                
                if remaining_qty <= 0:
                    continue
                
                # Match qty
                match_qty = min(remaining_qty, qty_remaining)
                
                # Allocate fees proportionally
                buy_fee_alloc = buy_fee * (match_qty / buy_qty)
                sell_fee_alloc = fee_usd * (match_qty / qty)
                
                # Insert lot match
                cursor.execute(
                    """
                    INSERT INTO lot_matches
                    (buy_tx_id, sell_tx_id, quantity, buy_fee_alloc, sell_fee_alloc)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        buy_tx_id,
                        sell_tx_id,
                        float(match_qty),
                        float(buy_fee_alloc),
                        float(sell_fee_alloc),
                    )
                )
                
                qty_remaining -= match_qty
            
            # Check if all qty was matched
            if qty_remaining > 0:
                conn.rollback()
                raise InvalidTransaction(
                    f"Insufficient holdings: tried to sell {qty} but only {qty - qty_remaining} available"
                )
            
            conn.commit()
            return sell_tx_id
        except Exception:
            conn.rollback()
            raise
