"""
PnL (Profit and Loss) service.
"""
from decimal import Decimal
from typing import Optional

from portfolio_tracker_v2.core import Database
from portfolio_tracker_v2.core.asset_resolver import AssetResolver


class PnLService:
    """
    Service for calculating position and profit/loss metrics.
    """
    
    def __init__(self, db: Database, resolver: AssetResolver):
        """
        Initialize PnLService.
        
        Args:
            db: Database instance.
            resolver: AssetResolver for symbol resolution.
        """
        self.db = db
        self.resolver = resolver
    
    def realized_pnl(
        self,
        symbol: Optional[str] = None,
        account: Optional[str] = None,
    ) -> Decimal:
        """
        Calculate realized P&L from closed lots.
        
        P&L = (sell_total - sell_fee_alloc) - (buy_total + buy_fee_alloc)
        
        Args:
            symbol: Filter by symbol (if None, all symbols).
            account: Filter by account (if None, all accounts).
        
        Returns:
            Total realized P&L (Decimal).
        """
        conn = self.db.connect()
        cursor = conn.cursor()
        
        query = """
        SELECT
            SUM(
                (lm.quantity * t_sell.unit_price - lm.sell_fee_alloc) -
                (lm.quantity * t_buy.unit_price + lm.buy_fee_alloc)
            ) as total_pnl
        FROM lot_matches lm
        JOIN transactions t_buy ON lm.buy_tx_id = t_buy.id
        JOIN transactions t_sell ON lm.sell_tx_id = t_sell.id
        WHERE 1=1
        """
        
        params = []
        
        if symbol:
            asset = self.resolver.resolve(symbol)
            query += " AND t_buy.asset_id = ?"
            params.append(asset['id'])
        
        if account:
            query += " AND t_buy.account_id = (SELECT id FROM accounts WHERE name = ?)"
            params.append(account)
        
        cursor.execute(query, params)
        result = cursor.fetchone()[0]
        
        return Decimal(str(result)) if result is not None else Decimal('0')
    
    def open_position_qty(
        self,
        symbol: str,
        account: Optional[str] = None,
    ) -> Decimal:
        """
        Calculate open position quantity (unmatched buys - sells).
        
        Takes all BUY + MIGRATION_BUY minus all SELL for a symbol.
        
        Args:
            symbol: Asset symbol.
            account: Filter by account (if None, all accounts).
        
        Returns:
            Quantity held (Decimal).
        """
        asset = self.resolver.resolve(symbol)
        conn = self.db.connect()
        cursor = conn.cursor()
        
        query = """
        WITH buy_qty AS (
            SELECT COALESCE(SUM(quantity), 0) as total
            FROM transactions
            WHERE asset_id=? AND tx_type IN ('BUY', 'MIGRATION_BUY')
        ),
        sell_qty AS (
            SELECT COALESCE(SUM(quantity), 0) as total
            FROM transactions
            WHERE asset_id=? AND tx_type = 'SELL'
        )
        SELECT buy_qty.total - sell_qty.total FROM buy_qty, sell_qty
        """
        
        params = [asset['id'], asset['id']]
        
        if account:
            # Refactor to include account filter
            query = """
            WITH buy_qty AS (
                SELECT COALESCE(SUM(quantity), 0) as total
                FROM transactions
                WHERE asset_id=? AND tx_type IN ('BUY', 'MIGRATION_BUY')
                  AND account_id = (SELECT id FROM accounts WHERE name = ?)
            ),
            sell_qty AS (
                SELECT COALESCE(SUM(quantity), 0) as total
                FROM transactions
                WHERE asset_id=? AND tx_type = 'SELL'
                  AND account_id = (SELECT id FROM accounts WHERE name = ?)
            )
            SELECT buy_qty.total - sell_qty.total FROM buy_qty, sell_qty
            """
            params = [asset['id'], account, asset['id'], account]
        
        cursor.execute(query, params)
        result = cursor.fetchone()[0]
        
        return Decimal(str(result))
