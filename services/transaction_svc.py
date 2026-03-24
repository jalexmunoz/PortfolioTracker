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

    def list_transactions(
        self,
        account: Optional[str] = None,
        symbol: Optional[str] = None,
        side: Optional[str] = None,
        tx_id: Optional[int] = None,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        limit: int = 50,
    ):
        """List ledger transactions ordered from most recent to oldest."""
        conn = self.db.connect()
        cursor = conn.cursor()

        query = """
            SELECT
                t.id,
                t.tx_date,
                acc.name,
                a.symbol,
                t.tx_type,
                t.quantity,
                t.unit_price,
                t.fee_usd,
                t.total_usd,
                CASE WHEN t.tx_type = 'SELL' THEN (t.quantity * t.unit_price) ELSE NULL END as gross_proceeds,
                sell_rollup.matched_cost_basis,
                sell_rollup.realized_pnl
            FROM transactions t
            JOIN assets a ON a.id = t.asset_id
            JOIN accounts acc ON acc.id = t.account_id
            LEFT JOIN (
                SELECT
                    lm.sell_tx_id,
                    SUM(lm.quantity * t_buy.unit_price + lm.buy_fee_alloc) as matched_cost_basis,
                    SUM(
                        (lm.quantity * t_sell.unit_price - lm.sell_fee_alloc) -
                        (lm.quantity * t_buy.unit_price + lm.buy_fee_alloc)
                    ) as realized_pnl
                FROM lot_matches lm
                JOIN transactions t_buy ON t_buy.id = lm.buy_tx_id
                JOIN transactions t_sell ON t_sell.id = lm.sell_tx_id
                GROUP BY lm.sell_tx_id
            ) sell_rollup ON sell_rollup.sell_tx_id = t.id
        """

        where_clauses = []
        params = []

        if account:
            where_clauses.append("acc.name = ?")
            params.append(account.strip())
        if symbol:
            where_clauses.append("a.symbol = ?")
            params.append(symbol.strip().upper())
        if side:
            where_clauses.append("t.tx_type = ?")
            params.append(side.strip().upper())
        if tx_id is not None:
            where_clauses.append("t.id = ?")
            params.append(int(tx_id))
        if from_date:
            where_clauses.append("t.tx_date >= ?")
            params.append(from_date)
        if to_date:
            where_clauses.append("t.tx_date <= ?")
            params.append(to_date)

        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)

        query += " ORDER BY t.tx_date DESC, t.id DESC LIMIT ?"
        params.append(int(limit))

        cursor.execute(query, tuple(params))
        rows = cursor.fetchall()
        return [
            {
                "id": row[0],
                "tx_date": row[1],
                "account": row[2],
                "symbol": row[3],
                "side": row[4],
                "quantity": row[5],
                "unit_price": row[6],
                "fee_usd": row[7],
                "total_usd": row[8],
                "gross_proceeds": row[9],
                "matched_cost_basis": row[10],
                "realized_pnl": row[11],
            }
            for row in rows
        ]

    def list_open_lots(
        self,
        account: Optional[str] = None,
        symbol: Optional[str] = None,
    ):
        """List open BUY/MIGRATION_BUY lots with remaining quantity."""
        conn = self.db.connect()
        cursor = conn.cursor()

        query = """
            SELECT
                t.id,
                t.tx_date,
                a.symbol,
                acc.name,
                t.tx_type,
                t.quantity,
                t.unit_price,
                COALESCE(SUM(lm.quantity), 0) AS matched_qty
            FROM transactions t
            JOIN assets a ON a.id = t.asset_id
            JOIN accounts acc ON acc.id = t.account_id
            LEFT JOIN lot_matches lm ON lm.buy_tx_id = t.id
            WHERE t.tx_type IN ('BUY', 'MIGRATION_BUY')
        """

        params = []
        if account:
            query += " AND acc.name = ?"
            params.append(account.strip())
        if symbol:
            query += " AND a.symbol = ?"
            params.append(symbol.strip().upper())

        query += """
            GROUP BY t.id, t.tx_date, a.symbol, acc.name, t.tx_type, t.quantity, t.unit_price
            HAVING (t.quantity - COALESCE(SUM(lm.quantity), 0)) > 0
            ORDER BY acc.name ASC, a.symbol ASC, t.tx_date ASC, t.id ASC
        """

        cursor.execute(query, tuple(params))
        rows = cursor.fetchall()

        result = []
        for row in rows:
            original_qty = Decimal(str(row[5]))
            unit_price = Decimal(str(row[6]))
            matched_qty = Decimal(str(row[7]))
            remaining_qty = original_qty - matched_qty

            result.append(
                {
                    "buy_tx_id": row[0],
                    "tx_date": row[1],
                    "symbol": row[2],
                    "account": row[3],
                    "origin": row[4],
                    "original_qty": float(original_qty),
                    "remaining_qty": float(remaining_qty),
                    "unit_price": float(unit_price),
                    "remaining_cost_basis": float(remaining_qty * unit_price),
                }
            )
        return result

    def delete_transaction(self, tx_id: int) -> dict:
        """
        Delete a transaction by id with minimal consistency rules.

        Rules:
        - SELL can be deleted; related lot_matches (sell side) are removed first.
        - BUY/MIGRATION_BUY can be deleted only if it has no matches as buy_tx_id.
        - Unknown tx_type is rejected as unsafe.

        Returns:
            A dict with deleted transaction metadata.

        Raises:
            InvalidTransaction on missing id or unsafe deletion.
        """
        if tx_id <= 0:
            raise InvalidTransaction("Transaction id must be positive")

        conn = self.db.connect()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT id, tx_type FROM transactions WHERE id = ?", (tx_id,))
            row = cursor.fetchone()
            if not row:
                raise InvalidTransaction(f"Transaction id {tx_id} does not exist")

            tx_type = row[1]

            if tx_type in ("BUY", "MIGRATION_BUY"):
                cursor.execute("SELECT COUNT(1) FROM lot_matches WHERE buy_tx_id = ?", (tx_id,))
                match_count = int(cursor.fetchone()[0] or 0)
                if match_count > 0:
                    raise InvalidTransaction(
                        f"Cannot delete transaction {tx_id}: BUY is already matched to one or more SELL transactions"
                    )

                cursor.execute(
                    "DELETE FROM lot_matches WHERE buy_tx_id = ? OR sell_tx_id = ?",
                    (tx_id, tx_id),
                )
                cursor.execute("DELETE FROM transactions WHERE id = ?", (tx_id,))
                conn.commit()
                return {"id": tx_id, "tx_type": tx_type}

            if tx_type == "SELL":
                cursor.execute("DELETE FROM lot_matches WHERE sell_tx_id = ?", (tx_id,))
                cursor.execute("DELETE FROM transactions WHERE id = ?", (tx_id,))
                conn.commit()
                return {"id": tx_id, "tx_type": tx_type}

            raise InvalidTransaction(
                f"Cannot delete transaction {tx_id}: unsupported tx_type '{tx_type}'"
            )
        except Exception:
            conn.rollback()
            raise
