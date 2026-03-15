"""
PnL (Profit and Loss) service.
"""
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional

from portfolio_tracker_v2.core import Database
from portfolio_tracker_v2.core.asset_resolver import AssetResolver


class PnLService:
    """Service for calculating position and valuation metrics."""

    APPROVED_NON_MARKET_METHODS = {'snapshot_imported', 'contractual_value'}

    def __init__(self, db: Database, resolver: AssetResolver):
        self.db = db
        self.resolver = resolver

    def _parse_price_updated_at(self, price_updated_at):
        if isinstance(price_updated_at, str):
            return datetime.fromisoformat(price_updated_at.replace('Z', '+00:00'))
        return price_updated_at

    def _classify_price_quality(self, asset_id: int) -> str:
        conn = self.db.connect()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT current_price, price_source, price_updated_at FROM assets WHERE id = ?",
            (asset_id,),
        )
        row = cursor.fetchone()
        if not row or row[0] is None or row[2] is None:
            return 'unavailable'
        _, price_source, price_updated_at = row
        price_updated_at = self._parse_price_updated_at(price_updated_at)
        now = datetime.now()
        is_old = price_updated_at < now - timedelta(days=7)
        if price_source == 'csv_bootstrap' or is_old:
            return 'stale'
        return 'usable'

    def _resolve_valuation_status(self, valuation_method: str, asset_id: int, current_price: Decimal | None) -> str:
        valuation_method = valuation_method or 'unvalued'
        if valuation_method == 'market_live':
            return self._classify_price_quality(asset_id)
        if valuation_method == 'snapshot_imported':
            return 'usable_non_market' if current_price is not None else 'unavailable'
        if valuation_method == 'contractual_value':
            return 'usable_non_market' if current_price is not None else 'unavailable'
        return 'unvalued'

    def _classify_asset_class(self, asset_type: str, symbol: str, valuation_method: str) -> str:
        if valuation_method in self.APPROVED_NON_MARKET_METHODS:
            return 'Non-market'

        asset_type_lower = (asset_type or '').lower()
        if asset_type_lower in {'crypto', 'stablecoin'}:
            return 'Crypto'
        if asset_type_lower == 'commodity' or symbol in {'GOLD', 'SILVER'}:
            return 'Metals'
        if asset_type_lower in {'stock_us', 'stock_intl'}:
            return 'Equities'

        return 'Equities'

    def _get_non_market_approved_price(self, asset_id: int, account: Optional[str]) -> Decimal | None:
        """Fallback approved valuation for non-market assets from latest buy-side unit price."""
        conn = self.db.connect()
        cursor = conn.cursor()

        if account:
            cursor.execute(
                """
                SELECT t.unit_price
                FROM transactions t
                WHERE t.asset_id = ?
                  AND t.account_id = (SELECT id FROM accounts WHERE name = ?)
                  AND t.tx_type IN ('BUY', 'MIGRATION_BUY')
                  AND t.unit_price IS NOT NULL
                ORDER BY t.tx_date DESC, t.id DESC
                LIMIT 1
                """,
                (asset_id, account),
            )
        else:
            cursor.execute(
                """
                SELECT t.unit_price
                FROM transactions t
                WHERE t.asset_id = ?
                  AND t.tx_type IN ('BUY', 'MIGRATION_BUY')
                  AND t.unit_price IS NOT NULL
                ORDER BY t.tx_date DESC, t.id DESC
                LIMIT 1
                """,
                (asset_id,),
            )
        row = cursor.fetchone()
        if not row or row[0] is None:
            return None
        return Decimal(str(row[0]))

    def realized_pnl(self, symbol: Optional[str] = None, account: Optional[str] = None) -> Decimal:
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

    def open_position_qty(self, symbol: str, account: Optional[str] = None) -> Decimal:
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
        conn = self.db.connect()
        cursor = conn.cursor()

        if account:
            cursor.execute(
                """
                SELECT DISTINCT a.symbol, (SELECT name FROM accounts WHERE id = t.account_id) as account
                FROM transactions t
                JOIN assets a ON a.id = t.asset_id
                WHERE t.account_id = (SELECT id FROM accounts WHERE name = ?)
                  AND a.is_active = 1
                """,
                (account,),
            )
            pairs = cursor.fetchall()
        else:
            cursor.execute(
                """
                SELECT DISTINCT a.symbol, (SELECT name FROM accounts WHERE id = t.account_id) as account
                FROM transactions t
                JOIN assets a ON a.id = t.asset_id
                WHERE a.is_active = 1
                """
            )
            pairs = cursor.fetchall()

        results = []
        for sym, acct in pairs:
            qty_open = self.open_position_qty(sym, acct)
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
                    (asset['id'], acct),
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
                    (asset['id'],),
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

            cursor.execute(
                "SELECT current_price, valuation_method FROM assets WHERE id = ?",
                (asset['id'],),
            )
            row = cursor.fetchone()
            current_price = Decimal(str(row[0])) if row and row[0] is not None else None
            valuation_method = row[1] if row and row[1] else asset.get('valuation_method', 'unvalued')
            effective_price = current_price
            if effective_price is None and valuation_method in self.APPROVED_NON_MARKET_METHODS:
                effective_price = self._get_non_market_approved_price(asset['id'], acct)
            valuation_status = self._resolve_valuation_status(valuation_method, asset['id'], effective_price)

            approved_value = None
            if qty_open > 0 and effective_price is not None:
                if valuation_method == 'market_live' and valuation_status == 'usable':
                    approved_value = qty_open * effective_price
                elif valuation_method in self.APPROVED_NON_MARKET_METHODS and valuation_status != 'unavailable':
                    approved_value = qty_open * effective_price

            alert = ''
            unrealized_pct = None
            if approved_value is not None and cost_basis > 0 and valuation_method == 'market_live':
                unrealized_pnl = approved_value - cost_basis
                unrealized_pct = (unrealized_pnl / cost_basis) * 100
                if unrealized_pct >= 30:
                    alert = 'YES'

            results.append(
                {
                    'symbol': sym,
                    'asset_type': asset.get('asset_type', ''),
                    'account': acct,
                    'qty_open': qty_open,
                    'cost_basis': cost_basis,
                    'avg_cost': avg_cost,
                    'realized_pnl': realized,
                    'current_price': current_price,
                    'valuation_method': valuation_method,
                    'valuation_status': valuation_status,
                    'approved_value': approved_value,
                    'unrealized_pct': unrealized_pct,
                    'alert': alert,
                }
            )

        return results

    def cash_balance(self, account: Optional[str] = None) -> Decimal:
        conn = self.db.connect()
        cursor = conn.cursor()
        usd = self.resolver.get_or_create_usd_cash()
        if account:
            cursor.execute(
                "SELECT COALESCE(SUM(total_usd),0) FROM transactions WHERE asset_id = ? AND account_id = (SELECT id FROM accounts WHERE name = ?)",
                (usd['id'], account),
            )
        else:
            cursor.execute(
                "SELECT COALESCE(SUM(total_usd),0) FROM transactions WHERE asset_id = ?",
                (usd['id'],),
            )
        res = cursor.fetchone()[0]
        return Decimal(str(res)) if res is not None else Decimal('0')

    def summary(self, account: Optional[str] = None) -> dict:
        positions = self.positions(account)
        total_cost_basis = Decimal('0')
        total_realized = Decimal('0')
        total_equity = Decimal('0')
        market_covered_value = Decimal('0')
        non_market_valued = Decimal('0')
        valued_cost_basis = Decimal('0')
        unvalued_excluded_cost_basis = Decimal('0')
        unvalued_positions = 0
        price_quality_counts = {'usable': 0, 'stale': 0, 'unavailable': 0}
        asset_class_breakdown = {
            'Crypto': Decimal('0'),
            'Equities': Decimal('0'),
            'Metals': Decimal('0'),
            'Non-market': Decimal('0'),
        }

        for p in positions:
            total_cost_basis += p['cost_basis']
            total_realized += p['realized_pnl']

            if p['valuation_method'] == 'market_live':
                price_quality_counts[p['valuation_status']] += 1

            if p['approved_value'] is not None and p['qty_open'] > 0:
                total_equity += p['approved_value']
                valued_cost_basis += p['cost_basis']
                if p['valuation_method'] == 'market_live':
                    market_covered_value += p['approved_value']
                else:
                    non_market_valued += p['approved_value']

                asset_class = self._classify_asset_class(
                    p.get('asset_type', ''),
                    p['symbol'],
                    p['valuation_method'],
                )
                asset_class_breakdown[asset_class] += p['approved_value']
            elif p['qty_open'] > 0:
                unvalued_excluded_cost_basis += p['cost_basis']
                unvalued_positions += 1

        cash = self.cash_balance(account)
        total_unrealized_pnl = total_equity - valued_cost_basis
        unrealized_return_pct = None
        if valued_cost_basis > 0:
            unrealized_return_pct = ((total_unrealized_pnl / valued_cost_basis) * 100).quantize(Decimal('0.01'))

        return {
            'total_cost_basis': total_cost_basis,
            'total_realized_pnl': total_realized,
            'cash_balance': cash,
            'total_equity': total_equity,
            'market_covered_value': market_covered_value,
            'non_market_valued': non_market_valued,
            'unvalued_excluded_cost_basis': unvalued_excluded_cost_basis,
            'unvalued_positions': unvalued_positions,
            'valued_cost_basis': valued_cost_basis,
            'total_market_value': market_covered_value,
            'total_unrealized_pnl': total_unrealized_pnl,
            'unrealized_return_pct': unrealized_return_pct,
            'price_quality_counts': price_quality_counts,
            'asset_class_breakdown': asset_class_breakdown,
        }


