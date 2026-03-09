"""
PnL (Profit and Loss) service.
"""
from datetime import datetime, timedelta
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
    
    def _classify_price_quality(self, asset_id: int) -> str:
        """
        Classify price quality for an asset.
        
        Returns: 'usable', 'stale', 'unavailable'
        """
        conn = self.db.connect()
        cursor = conn.cursor()
        cursor.execute("SELECT current_price, price_source, price_updated_at FROM assets WHERE id = ?", (asset_id,))
        row = cursor.fetchone()
        if not row or row[0] is None or row[2] is None:
            return 'unavailable'
        current_price, price_source, price_updated_at = row
        # Parse price_updated_at if it's string
        if isinstance(price_updated_at, str):
            price_updated_at = datetime.fromisoformat(price_updated_at.replace('Z', '+00:00'))
        now = datetime.now()
        is_old = price_updated_at < now - timedelta(days=7)
        if price_source == 'csv_bootstrap' or is_old:
            return 'stale'
        return 'usable'
    
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

    def positions(self, account: Optional[str] = None) -> list:
        """
        Return list of position dicts grouped by symbol (and account if account is None).

        Each dict: { 'symbol', 'account', 'qty_open', 'cost_basis', 'avg_cost', 'realized_pnl' }
        """
        conn = self.db.connect()
        cursor = conn.cursor()

        params = []
        # If account is specified, filter by that account id
        account_clause = ""
        if account:
            account_clause = "AND t.account_id = (SELECT id FROM accounts WHERE name = ?)"
            params.append(account)

        # gather distinct asset/account pairs where there are transactions
        if account:
            cursor.execute(
                """
                SELECT DISTINCT a.symbol, (SELECT name FROM accounts WHERE id = t.account_id) as account
                FROM transactions t
                JOIN assets a ON a.id = t.asset_id
                WHERE t.account_id = (SELECT id FROM accounts WHERE name = ?)
                """,
                (account,)
            )
            pairs = cursor.fetchall()
        else:
            cursor.execute(
                """
                SELECT DISTINCT a.symbol, (SELECT name FROM accounts WHERE id = t.account_id) as account
                FROM transactions t
                JOIN assets a ON a.id = t.asset_id
                """
            )
            pairs = cursor.fetchall()

        results = []
        for sym, acct in pairs:
            qty_open = self.open_position_qty(sym, acct)

            # compute cost_basis from remaining (unmatched) portions of buy transactions
            asset = self.resolver.resolve(sym)
            if acct:
                cursor.execute(
                    """
                    SELECT t.id, t.quantity, t.unit_price, t.fee_usd,
                           COALESCE((SELECT SUM(quantity) FROM lot_matches WHERE buy_tx_id = t.id), 0) as matched_qty
                    FROM transactions t
                    WHERE t.asset_id = ? AND t.account_id = (SELECT id FROM accounts WHERE name = ?) AND t.tx_type IN ('BUY', 'MIGRATION_BUY')
                    ORDER BY t.tx_date ASC, t.id ASC
                    """,
                    (asset['id'], acct)
                )
            else:
                cursor.execute(
                    """
                    SELECT t.id, t.quantity, t.unit_price, t.fee_usd,
                           COALESCE((SELECT SUM(quantity) FROM lot_matches WHERE buy_tx_id = t.id), 0) as matched_qty
                    FROM transactions t
                    WHERE t.asset_id = ? AND t.tx_type IN ('BUY', 'MIGRATION_BUY')
                    ORDER BY t.tx_date ASC, t.id ASC
                    """,
                    (asset['id'],)
                )

            cost_basis = Decimal('0')
            total_open_qty = Decimal('0')
            for row in cursor.fetchall():
                buy_qty = Decimal(str(row[1]))
                unit_price = Decimal(str(row[2])) if row[2] is not None else Decimal('0')
                buy_fee = Decimal(str(row[3])) if row[3] is not None else Decimal('0')
                matched_qty = Decimal(str(row[4]))

                remaining_qty = buy_qty - matched_qty
                if remaining_qty <= 0:
                    continue
                remaining_fee = buy_fee * (remaining_qty / buy_qty) if buy_qty > 0 else Decimal('0')
                cost_basis += remaining_qty * unit_price + remaining_fee
                total_open_qty += remaining_qty

            avg_cost = (cost_basis / total_open_qty) if total_open_qty > 0 else Decimal('0')
            realized = self.realized_pnl(sym, acct)

            # get current_price from assets
            cursor.execute("SELECT current_price, price_updated_at FROM assets WHERE id = ?", (asset['id'],))
            row = cursor.fetchone()
            current_price = Decimal(str(row[0])) if row and row[0] is not None else None
            price_updated_at = row[1] if row else None

            # calculate unrealized gain %
            alert = ""
            if current_price and price_updated_at and cost_basis > 0 and qty_open > 0:
                market_value = qty_open * current_price
                unrealized_pnl = market_value - cost_basis
                unrealized_pct = (unrealized_pnl / cost_basis) * 100
                if unrealized_pct >= 30:
                    alert = "YES"

            results.append({
                'symbol': sym,
                'account': acct,
                'qty_open': qty_open,
                'cost_basis': cost_basis,
                'avg_cost': avg_cost,
                'realized_pnl': realized,
                'current_price': current_price,
                'unrealized_pct': unrealized_pct if 'unrealized_pct' in locals() else None,
                'alert': alert,
            })

        return results

    def cash_balance(self, account: Optional[str] = None) -> Decimal:
        """
        Return cash balance using __USD_CASH__ asset total_usd sum.
        """
        conn = self.db.connect()
        cursor = conn.cursor()
        usd = self.resolver.get_or_create_usd_cash()
        if account:
            cursor.execute(
                "SELECT COALESCE(SUM(total_usd),0) FROM transactions WHERE asset_id = ? AND account_id = (SELECT id FROM accounts WHERE name = ?)",
                (usd['id'], account)
            )
        else:
            cursor.execute(
                "SELECT COALESCE(SUM(total_usd),0) FROM transactions WHERE asset_id = ?",
                (usd['id'],)
            )
        res = cursor.fetchone()[0]
        return Decimal(str(res)) if res is not None else Decimal('0')

    def summary(self, account: Optional[str] = None) -> dict:
        """
        Return summary dict: total_cost_basis, total_realized_pnl, cash_balance,
        total_market_value (usable prices only), total_unrealized_pnl, unrealized_return_pct,
        price_quality_counts
        """
        positions = self.positions(account)
        total_cost_basis = Decimal('0')
        total_realized = Decimal('0')
        total_market_value = Decimal('0')
        total_unrealized_pnl = Decimal('0')
        price_quality_counts = {'usable': 0, 'stale': 0, 'unavailable': 0}
        
        for p in positions:
            total_cost_basis += p['cost_basis']
            total_realized += p['realized_pnl']
            
            asset = self.resolver.resolve(p['symbol'])
            quality = self._classify_price_quality(asset['id'])
            price_quality_counts[quality] += 1
            
            if quality == 'usable' and p['current_price'] and p['qty_open'] > 0:
                market_value = p['qty_open'] * p['current_price']
                total_market_value += market_value
                unrealized_pnl = market_value - p['cost_basis']
                total_unrealized_pnl += unrealized_pnl
        
        cash = self.cash_balance(account)
        
        unrealized_return_pct = None
        if total_cost_basis > 0:
            unrealized_return_pct = ((total_unrealized_pnl / total_cost_basis) * 100).quantize(Decimal('0.01'))
        
        return {
            'total_cost_basis': total_cost_basis,
            'total_realized_pnl': total_realized,
            'cash_balance': cash,
            'total_market_value': total_market_value,
            'total_unrealized_pnl': total_unrealized_pnl,
            'unrealized_return_pct': unrealized_return_pct,
            'price_quality_counts': price_quality_counts,
        }
