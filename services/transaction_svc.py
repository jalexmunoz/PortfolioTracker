"""
Transaction service: record buys/sells with FIFO matching.
"""
import json
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Optional

from portfolio_tracker_v2.core import Database
from portfolio_tracker_v2.core.asset_resolver import AssetResolver
from portfolio_tracker_v2.core.exceptions import InvalidTransaction


class TransactionService:
    """
    Service for recording and matching transactions.

    Handles FIFO lot matching for sells against open buy positions.
    """
    CDT_CONTRACT_NOTE_PREFIX = "CDT_CONTRACT_V1:"
    FUND_MOVEMENT_TYPES = {"CONTRIBUTION", "WITHDRAWAL"}
    CASH_MOVEMENT_TYPES = {"DEPOSIT", "WITHDRAWAL"}
    SUPPORTED_CURRENCIES = {"USD", "COP"}
    FX_USD_QUANTIZE = Decimal("0.000001")


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

    def _parse_iso_date(self, value: str, field_name: str) -> str:
        raw = (value or "").strip()
        if not raw:
            raise InvalidTransaction(f"{field_name} is required")
        try:
            return datetime.strptime(raw, "%Y-%m-%d").date().isoformat()
        except ValueError as exc:
            raise InvalidTransaction(f"{field_name} must use YYYY-MM-DD format") from exc

    def _open_qty_for_asset_account(self, cursor, asset_id: int, account_id: int) -> Decimal:
        cursor.execute(
            """
            WITH buy_qty AS (
                SELECT COALESCE(SUM(quantity), 0) AS total
                FROM transactions
                WHERE asset_id = ? AND account_id = ? AND tx_type IN ('BUY', 'MIGRATION_BUY')
            ),
            sell_qty AS (
                SELECT COALESCE(SUM(quantity), 0) AS total
                FROM transactions
                WHERE asset_id = ? AND account_id = ? AND tx_type = 'SELL'
            )
            SELECT buy_qty.total - sell_qty.total FROM buy_qty, sell_qty
            """,
            (asset_id, account_id, asset_id, account_id),
        )
        row = cursor.fetchone()
        return Decimal(str(row[0] if row else 0))

    def _set_asset_non_market_valuation(
        self,
        cursor,
        asset_id: int,
        valuation_method: str,
        current_price: Decimal | None = None,
        price_source: str | None = None,
        price_updated_at: str | None = None,
    ) -> None:
        cursor.execute(
            """
            UPDATE assets
            SET valuation_method = ?,
                current_price = CASE WHEN ? IS NULL THEN current_price ELSE ? END,
                price_source = CASE WHEN ? IS NULL THEN price_source ELSE ? END,
                price_updated_at = CASE WHEN ? IS NULL THEN price_updated_at ELSE ? END
            WHERE id = ?
            """,
            (
                valuation_method,
                None if current_price is None else float(current_price),
                None if current_price is None else float(current_price),
                price_source,
                price_source,
                price_updated_at,
                price_updated_at,
                asset_id,
            ),
        )


    def _fund_balance_for_asset_account(self, cursor, asset_id: int, account_id: int) -> Decimal:
        cursor.execute(
            """
            SELECT
                COALESCE(SUM(CASE WHEN tx_type IN ('BUY', 'MIGRATION_BUY') THEN total_usd ELSE 0 END), 0) AS inflows,
                COALESCE(SUM(CASE WHEN tx_type = 'SELL' THEN total_usd ELSE 0 END), 0) AS outflows
            FROM transactions
            WHERE asset_id = ? AND account_id = ?
            """,
            (asset_id, account_id),
        )
        row = cursor.fetchone()
        if not row:
            return Decimal('0')
        inflows = Decimal(str(row[0] or 0))
        outflows = Decimal(str(row[1] or 0))
        return inflows - outflows


    def _record_cash_movement(
        self,
        cursor,
        tx_id: int,
        account_id: int,
        movement_type: str,
        amount_usd: Decimal,
        note: Optional[str] = None,
    ) -> None:
        cursor.execute(
            """
            INSERT INTO cash_ledger (tx_id, account_id, movement_type, amount_usd, note)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                int(tx_id),
                int(account_id),
                movement_type,
                float(amount_usd),
                note,
            ),
        )

    def _infer_backfill_cash_movement(self, tx_type: str, notes: Optional[str]) -> Optional[tuple[str, str]]:
        tx_type_normalized = (tx_type or "").strip().upper()
        if tx_type_normalized not in ("BUY", "SELL"):
            return None

        note_text = (notes or "").strip()
        if tx_type_normalized == "BUY" and note_text.startswith(self.CDT_CONTRACT_NOTE_PREFIX):
            return "CDT_BUY", "Backfill cash delta from manual CDT"
        if tx_type_normalized == "BUY" and note_text.startswith("FUND_CONTRIBUTION"):
            return "FUND_CONTRIBUTION", "Backfill cash delta from fund movement"
        if tx_type_normalized == "SELL" and note_text.startswith("FUND_WITHDRAWAL"):
            return "FUND_WITHDRAWAL", "Backfill cash delta from fund movement"
        if tx_type_normalized == "BUY":
            return "BUY", "Backfill cash delta from BUY"
        return "SELL", "Backfill cash delta from SELL"

    def backfill_cash_ledger(self) -> dict:
        """
        Populate missing cash_ledger rows from historical BUY/SELL transactions.

        Idempotent by tx_id: if a transaction already has one or more cash_ledger rows,
        it is skipped.
        """
        conn = self.db.connect()
        cursor = conn.cursor()
        inserted = 0
        skipped_existing = 0
        skipped_non_cash = 0
        try:
            cursor.execute(
                """
                SELECT
                    t.id,
                    t.account_id,
                    t.tx_type,
                    t.quantity,
                    t.unit_price,
                    t.fee_usd,
                    t.notes,
                    CASE
                        WHEN EXISTS(SELECT 1 FROM cash_ledger cl WHERE cl.tx_id = t.id) THEN 1
                        ELSE 0
                    END AS has_cash_entry
                FROM transactions t
                ORDER BY t.id ASC
                """
            )
            rows = cursor.fetchall()

            for row in rows:
                tx_id = int(row[0])
                account_id = row[1]
                tx_type_normalized = (row[2] or "").strip().upper()
                qty = Decimal(str(row[3] or 0))
                unit_price = Decimal(str(row[4] or 0))
                fee = Decimal(str(row[5] or 0))
                notes = row[6]
                has_cash_entry = int(row[7] or 0) == 1

                movement = self._infer_backfill_cash_movement(tx_type_normalized, notes)
                if movement is None or account_id is None:
                    skipped_non_cash += 1
                    continue

                if has_cash_entry:
                    skipped_existing += 1
                    continue

                movement_type, note = movement
                gross = qty * unit_price
                if tx_type_normalized == "BUY":
                    cash_delta = -(gross + fee)
                else:
                    cash_delta = gross - fee

                self._record_cash_movement(
                    cursor,
                    tx_id=tx_id,
                    account_id=int(account_id),
                    movement_type=movement_type,
                    amount_usd=cash_delta,
                    note=note,
                )
                inserted += 1

            conn.commit()
            return {
                "inserted": inserted,
                "skipped_existing": skipped_existing,
                "skipped_non_cash": skipped_non_cash,
            }
        except Exception:
            conn.rollback()
            raise

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
            self._record_cash_movement(
                cursor,
                tx_id=tx_id,
                account_id=account_id,
                movement_type='BUY',
                amount_usd=-total_usd,
                note='Auto cash delta from BUY',
            )
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

            self._record_cash_movement(
                cursor,
                tx_id=sell_tx_id,
                account_id=account_id,
                movement_type='SELL',
                amount_usd=total_usd,
                note='Auto cash delta from SELL',
            )
            conn.commit()
            return sell_tx_id
        except Exception:
            conn.rollback()
            raise

    def record_cdt(
        self,
        account: str,
        symbol: str,
        open_date: str,
        maturity_date: str,
        principal: Decimal,
        term_years: Optional[Decimal],
        annual_rate: Decimal,
        notes: Optional[str] = None,
        currency: str = "USD",
        fx_rate_at_open: Optional[Decimal] = None,
    ) -> int:
        """Record one manual CDT contract as one BUY lot with contractual metadata.

        For non-USD currencies (COP) the principal is interpreted in that currency
        and converted to USD using fx_rate_at_open (units of currency per 1 USD).
        The fx is locked at open: principal and interest at settlement are both
        valued in USD using this same rate, so no FX PnL is introduced.
        """
        account_name = (account or "").strip()
        if not account_name:
            raise InvalidTransaction("account cannot be empty")

        symbol_name = (symbol or "").strip().upper()
        if not symbol_name:
            raise InvalidTransaction("symbol cannot be empty")

        if principal <= 0:
            raise InvalidTransaction("principal must be positive")
        if annual_rate < 0:
            raise InvalidTransaction("rate cannot be negative")
        if term_years is not None and term_years <= 0:
            raise InvalidTransaction("term must be positive when provided")

        currency_normalized = (currency or "USD").strip().upper()
        if currency_normalized not in self.SUPPORTED_CURRENCIES:
            raise InvalidTransaction(
                f"currency must be one of {sorted(self.SUPPORTED_CURRENCIES)}"
            )

        if currency_normalized != "USD":
            if fx_rate_at_open is None:
                raise InvalidTransaction(
                    f"fx_rate_at_open is required for {currency_normalized} CDT"
                )
            try:
                fx_rate_at_open_dec = Decimal(str(fx_rate_at_open))
            except (InvalidOperation, TypeError) as exc:
                raise InvalidTransaction("fx_rate_at_open must be numeric") from exc
            if fx_rate_at_open_dec <= 0:
                raise InvalidTransaction("fx_rate_at_open must be positive")
            principal_usd = (principal / fx_rate_at_open_dec).quantize(self.FX_USD_QUANTIZE)
        else:
            fx_rate_at_open_dec = None
            principal_usd = principal

        open_date_iso = self._parse_iso_date(open_date, "open_date")
        maturity_date_iso = self._parse_iso_date(maturity_date, "maturity_date")
        open_date_obj = datetime.strptime(open_date_iso, "%Y-%m-%d").date()
        maturity_date_obj = datetime.strptime(maturity_date_iso, "%Y-%m-%d").date()
        if maturity_date_obj <= open_date_obj:
            raise InvalidTransaction("maturity_date must be after open_date")

        derived_term_years = (Decimal(str((maturity_date_obj - open_date_obj).days)) / Decimal("365")).quantize(Decimal("0.00000001"))

        contract_payload = {
            "open_date": open_date_iso,
            "maturity_date": maturity_date_iso,
            "principal": str(principal_usd),
            "term_years": str(derived_term_years),
            "annual_rate": str(annual_rate),
            "term_source": "derived_from_dates",
        }
        if term_years is not None:
            contract_payload["term_years_input"] = str(term_years)
        if currency_normalized != "USD":
            contract_payload["currency"] = currency_normalized
            contract_payload["principal_original"] = str(principal)
            contract_payload["fx_rate_at_open"] = str(fx_rate_at_open_dec)
            contract_payload["fx_date"] = open_date_iso
            contract_payload["principal_usd"] = str(principal_usd)

        payload_notes = f"{self.CDT_CONTRACT_NOTE_PREFIX}{json.dumps(contract_payload, separators=(',', ':'))}"
        extra_notes = (notes or "").strip()
        stored_notes = payload_notes if not extra_notes else f"{payload_notes} | {extra_notes}"

        conn = self.db.connect()
        cursor = conn.cursor()
        try:
            asset = self.resolver.resolve(symbol_name)
            account_id = self._get_or_create_account(account_name, cursor)

            cursor.execute(
                """
                INSERT INTO transactions
                (asset_id, account_id, tx_type, quantity, unit_price, fee_usd, total_usd, tx_date, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    asset["id"],
                    account_id,
                    "BUY",
                    1.0,
                    float(principal_usd),
                    0.0,
                    float(principal_usd),
                    open_date_iso,
                    stored_notes,
                ),
            )
            tx_id = cursor.lastrowid
            self._record_cash_movement(
                cursor,
                tx_id=tx_id,
                account_id=account_id,
                movement_type='CDT_BUY',
                amount_usd=-principal_usd,
                note='Auto cash delta from manual CDT',
            )
            self._set_asset_non_market_valuation(
                cursor,
                asset_id=asset["id"],
                valuation_method="contractual_value",
            )
            conn.commit()
            return tx_id
        except Exception:
            conn.rollback()
            raise

    def record_fund_movement(
        self,
        account: str,
        symbol: str,
        movement_date: str,
        amount: Decimal,
        movement_type: str,
        notes: Optional[str] = None,
        currency: str = "USD",
        fx_rate: Optional[Decimal] = None,
    ) -> int:
        """Record one non-market fund movement as monetary balance (no FIFO lot matching).

        For non-USD currencies (COP) the amount is interpreted in that currency
        and converted to USD using fx_rate (units of currency per 1 USD) for the
        specific movement. Each movement carries its own fx (no fx lock).
        Cash impact and the running balance are stored in USD.
        """
        account_name = (account or "").strip()
        if not account_name:
            raise InvalidTransaction("account cannot be empty")

        symbol_name = (symbol or "").strip().upper()
        if not symbol_name:
            raise InvalidTransaction("symbol cannot be empty")

        movement_type_normalized = (movement_type or "").strip().upper()
        if movement_type_normalized not in self.FUND_MOVEMENT_TYPES:
            raise InvalidTransaction("movement_type must be CONTRIBUTION or WITHDRAWAL")

        movement_date_iso = self._parse_iso_date(movement_date, "date")
        try:
            amount_dec = Decimal(str(amount))
        except (InvalidOperation, TypeError) as exc:
            raise InvalidTransaction("amount must be numeric") from exc
        if amount_dec <= 0:
            raise InvalidTransaction("amount must be positive")

        currency_normalized = (currency or "USD").strip().upper()
        if currency_normalized not in self.SUPPORTED_CURRENCIES:
            raise InvalidTransaction(
                f"currency must be one of {sorted(self.SUPPORTED_CURRENCIES)}"
            )

        if currency_normalized != "USD":
            if fx_rate is None:
                raise InvalidTransaction(
                    f"fx_rate is required for {currency_normalized} fund movement"
                )
            try:
                fx_rate_dec = Decimal(str(fx_rate))
            except (InvalidOperation, TypeError) as exc:
                raise InvalidTransaction("fx_rate must be numeric") from exc
            if fx_rate_dec <= 0:
                raise InvalidTransaction("fx_rate must be positive")
            amount_usd = (amount_dec / fx_rate_dec).quantize(self.FX_USD_QUANTIZE)
        else:
            fx_rate_dec = None
            amount_usd = amount_dec

        movement_note = f"FUND_{movement_type_normalized}"
        if currency_normalized != "USD":
            fx_payload = {
                "currency": currency_normalized,
                "amount_original": str(amount_dec),
                "fx_rate": str(fx_rate_dec),
                "fx_date": movement_date_iso,
                "amount_usd": str(amount_usd),
            }
            movement_note = f"{movement_note}:{json.dumps(fx_payload, separators=(',', ':'))}"
        user_note = (notes or "").strip()
        stored_notes = movement_note if not user_note else f"{movement_note} | {user_note}"

        conn = self.db.connect()
        cursor = conn.cursor()
        try:
            asset = self.resolver.resolve(symbol_name)
            account_id = self._get_or_create_account(account_name, cursor)
            current_balance = self._fund_balance_for_asset_account(cursor, asset["id"], account_id)

            if movement_type_normalized == "WITHDRAWAL" and amount_usd > current_balance:
                raise InvalidTransaction(
                    f"Insufficient fund balance: tried to withdraw {amount_usd} but only {current_balance} available"
                )

            tx_type = "BUY" if movement_type_normalized == "CONTRIBUTION" else "SELL"
            cursor.execute(
                """
                INSERT INTO transactions
                (asset_id, account_id, tx_type, quantity, unit_price, fee_usd, total_usd, tx_date, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    asset["id"],
                    account_id,
                    tx_type,
                    1.0,
                    float(amount_usd),
                    0.0,
                    float(amount_usd),
                    movement_date_iso,
                    stored_notes,
                ),
            )
            tx_id = cursor.lastrowid

            cash_delta = -amount_usd if movement_type_normalized == 'CONTRIBUTION' else amount_usd
            self._record_cash_movement(
                cursor,
                tx_id=tx_id,
                account_id=account_id,
                movement_type=f'FUND_{movement_type_normalized}',
                amount_usd=cash_delta,
                note='Auto cash delta from fund movement',
            )

            self._set_asset_non_market_valuation(
                cursor,
                asset_id=asset["id"],
                valuation_method="snapshot_imported",
                current_price=Decimal("1"),
                price_source="manual_non_market",
                price_updated_at=movement_date_iso,
            )
            conn.commit()
            return tx_id
        except Exception:
            conn.rollback()
            raise

    def record_cash_movement(
        self,
        account: str,
        movement_date: str,
        amount: Decimal,
        movement_type: str,
        notes: Optional[str] = None,
    ) -> int:
        """Record one manual cash DEPOSIT or WITHDRAWAL that only moves cash."""
        account_name = (account or "").strip()
        if not account_name:
            raise InvalidTransaction("account cannot be empty")

        movement_type_normalized = (movement_type or "").strip().upper()
        if movement_type_normalized not in self.CASH_MOVEMENT_TYPES:
            raise InvalidTransaction("movement_type must be DEPOSIT or WITHDRAWAL")

        movement_date_iso = self._parse_iso_date(movement_date, "date")
        try:
            amount_dec = Decimal(str(amount))
        except (InvalidOperation, TypeError) as exc:
            raise InvalidTransaction("amount must be numeric") from exc
        if amount_dec <= 0:
            raise InvalidTransaction("amount must be positive")

        movement_note = f"CASH_{movement_type_normalized}"
        user_note = (notes or "").strip()
        stored_notes = movement_note if not user_note else f"{movement_note} | {user_note}"

        conn = self.db.connect()
        cursor = conn.cursor()
        try:
            cash_asset = self.resolver.get_or_create_usd_cash()
            account_id = self._get_or_create_account(account_name, cursor)

            # total_usd=0 on the transaction row: the cash delta lives in cash_ledger
            # so cash_balance() does not double-count via the legacy __USD_CASH__ sum.
            cursor.execute(
                """
                INSERT INTO transactions
                (asset_id, account_id, tx_type, quantity, unit_price, fee_usd, total_usd, tx_date, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    cash_asset["id"],
                    account_id,
                    movement_type_normalized,
                    float(amount_dec),
                    1.0,
                    0.0,
                    0.0,
                    movement_date_iso,
                    stored_notes,
                ),
            )
            tx_id = cursor.lastrowid

            cash_delta = amount_dec if movement_type_normalized == "DEPOSIT" else -amount_dec
            self._record_cash_movement(
                cursor,
                tx_id=tx_id,
                account_id=account_id,
                movement_type=f"CASH_{movement_type_normalized}",
                amount_usd=cash_delta,
                note="Manual cash movement",
            )
            conn.commit()
            return tx_id
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


    def inspect_lot_matches(
        self,
        sell_tx_id: Optional[int] = None,
        buy_tx_id: Optional[int] = None,
    ):
        """Inspect lot match rows for one SELL or one BUY transaction id."""
        if (sell_tx_id is None and buy_tx_id is None) or (sell_tx_id is not None and buy_tx_id is not None):
            raise InvalidTransaction("Provide exactly one of sell_tx_id or buy_tx_id")

        target_id = sell_tx_id if sell_tx_id is not None else buy_tx_id
        if target_id is None or int(target_id) <= 0:
            raise InvalidTransaction("Transaction id must be positive")

        conn = self.db.connect()
        cursor = conn.cursor()

        tx_type_expected = "SELL" if sell_tx_id is not None else None
        if buy_tx_id is not None:
            tx_type_expected = "BUY_OR_MIGRATION"

        cursor.execute(
            "SELECT id, tx_type FROM transactions WHERE id = ?",
            (int(target_id),),
        )
        tx_row = cursor.fetchone()
        if not tx_row:
            raise InvalidTransaction(f"Transaction id {target_id} does not exist")

        tx_type = tx_row[1]
        if tx_type_expected == "SELL" and tx_type != "SELL":
            raise InvalidTransaction(f"Transaction id {target_id} is not a SELL transaction")
        if tx_type_expected == "BUY_OR_MIGRATION" and tx_type not in ("BUY", "MIGRATION_BUY"):
            raise InvalidTransaction(f"Transaction id {target_id} is not a BUY/MIGRATION_BUY transaction")

        query = """
            SELECT
                lm.sell_tx_id,
                lm.buy_tx_id,
                t_buy.tx_date,
                a.symbol,
                acc.name,
                lm.quantity,
                t_buy.unit_price,
                (lm.quantity * t_buy.unit_price + lm.buy_fee_alloc) as matched_cost_basis,
                t_sell.unit_price,
                (lm.quantity * t_sell.unit_price - lm.sell_fee_alloc) as matched_proceeds,
                ((lm.quantity * t_sell.unit_price - lm.sell_fee_alloc) - (lm.quantity * t_buy.unit_price + lm.buy_fee_alloc)) as matched_realized_pnl
            FROM lot_matches lm
            JOIN transactions t_buy ON t_buy.id = lm.buy_tx_id
            JOIN transactions t_sell ON t_sell.id = lm.sell_tx_id
            JOIN assets a ON a.id = t_buy.asset_id
            JOIN accounts acc ON acc.id = t_buy.account_id
        """

        if sell_tx_id is not None:
            query += " WHERE lm.sell_tx_id = ? ORDER BY lm.id ASC"
            cursor.execute(query, (int(sell_tx_id),))
        else:
            query += " WHERE lm.buy_tx_id = ? ORDER BY lm.id ASC"
            cursor.execute(query, (int(buy_tx_id),))

        rows = cursor.fetchall()
        return [
            {
                "sell_tx_id": row[0],
                "buy_tx_id": row[1],
                "buy_tx_date": row[2],
                "symbol": row[3],
                "account": row[4],
                "matched_qty": row[5],
                "buy_unit_price": row[6],
                "matched_cost_basis": row[7],
                "sell_unit_price": row[8],
                "matched_proceeds": row[9],
                "matched_realized_pnl": row[10],
            }
            for row in rows
        ]

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
                cursor.execute("DELETE FROM cash_ledger WHERE tx_id = ?", (tx_id,))
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
                cursor.execute("DELETE FROM cash_ledger WHERE tx_id = ?", (tx_id,))
                cursor.execute("DELETE FROM lot_matches WHERE sell_tx_id = ?", (tx_id,))
                cursor.execute("DELETE FROM transactions WHERE id = ?", (tx_id,))
                conn.commit()
                return {"id": tx_id, "tx_type": tx_type}

            if tx_type in ("DEPOSIT", "WITHDRAWAL"):
                cursor.execute("DELETE FROM cash_ledger WHERE tx_id = ?", (tx_id,))
                cursor.execute("DELETE FROM transactions WHERE id = ?", (tx_id,))
                conn.commit()
                return {"id": tx_id, "tx_type": tx_type}

            raise InvalidTransaction(
                f"Cannot delete transaction {tx_id}: unsupported tx_type '{tx_type}'"
            )
        except Exception:
            conn.rollback()
            raise

    def settle_cdt(self, buy_tx_id: int, settlement_date: str) -> dict:
        """
        Settle (liquidate) a CDT at maturity.

        Records a SELL tx at maturity_value (principal + interest) against
        the original BUY lot, which closes the position via lot_match and
        books the full payout into cash_ledger. The realized PnL (interest
        only) is derived automatically by the lot_match formula:
            realized = maturity_value - principal.

        Args:
            buy_tx_id: ID of the original CDT BUY transaction.
            settlement_date: Date of settlement (YYYY-MM-DD, must be >= maturity_date).

        Returns:
            dict with sell_tx_id, principal, interest, maturity_value.

        Raises:
            InvalidTransaction on invalid state.
        """
        if buy_tx_id <= 0:
            raise InvalidTransaction("buy_tx_id must be positive")

        settlement_date_iso = self._parse_iso_date(settlement_date, "settlement_date")

        conn = self.db.connect()
        cursor = conn.cursor()
        try:
            # Fetch and validate the original BUY transaction
            cursor.execute(
                """
                SELECT t.id, t.tx_type, t.quantity, t.unit_price, t.notes,
                       a.symbol, a.id AS asset_id,
                       acc.id AS account_id, acc.name AS account_name
                FROM transactions t
                JOIN assets a ON a.id = t.asset_id
                JOIN accounts acc ON acc.id = t.account_id
                WHERE t.id = ?
                """,
                (buy_tx_id,),
            )
            row = cursor.fetchone()
            if not row:
                raise InvalidTransaction(f"Transaction {buy_tx_id} does not exist")

            tx_type = row[1]
            if tx_type not in ("BUY", "MIGRATION_BUY"):
                raise InvalidTransaction(
                    f"Transaction {buy_tx_id} is of type '{tx_type}'; settle-cdt requires a BUY transaction"
                )

            notes = row[4] or ""
            if self.CDT_CONTRACT_NOTE_PREFIX not in notes:
                raise InvalidTransaction(
                    f"Transaction {buy_tx_id} does not appear to be a CDT (missing contract note prefix)"
                )

            qty = Decimal(str(row[2]))
            principal = Decimal(str(row[3])) * qty  # unit_price * qty
            symbol = row[5]
            asset_id = row[6]
            account_id = row[7]
            account_name = row[8]

            # Parse contract from notes
            note_text = notes.strip()
            payload_text = note_text[len(self.CDT_CONTRACT_NOTE_PREFIX):]
            if " | " in payload_text:
                payload_text = payload_text.split(" | ", 1)[0].strip()
            try:
                contract = json.loads(payload_text)
            except json.JSONDecodeError as exc:
                raise InvalidTransaction(f"Cannot parse CDT contract notes for tx {buy_tx_id}") from exc

            maturity_date_iso = contract.get("maturity_date")
            if not maturity_date_iso:
                raise InvalidTransaction(f"CDT contract for tx {buy_tx_id} is missing maturity_date")

            term_years = Decimal(str(contract.get("term_years", "0")))
            annual_rate = Decimal(str(contract.get("annual_rate", "0")))

            settlement_date_obj = datetime.strptime(settlement_date_iso, "%Y-%m-%d").date()
            maturity_date_obj = datetime.strptime(maturity_date_iso, "%Y-%m-%d").date()

            if settlement_date_obj < maturity_date_obj:
                raise InvalidTransaction(
                    f"settlement_date {settlement_date_iso} is before maturity_date {maturity_date_iso}; "
                    "early redemption is not supported"
                )

            # Check not already settled (BUY lot fully matched)
            cursor.execute(
                "SELECT COALESCE(SUM(quantity), 0) FROM lot_matches WHERE buy_tx_id = ?",
                (buy_tx_id,),
            )
            matched_qty = Decimal(str(cursor.fetchone()[0] or 0))
            remaining_qty = qty - matched_qty
            if remaining_qty <= 0:
                raise InvalidTransaction(
                    f"CDT transaction {buy_tx_id} is already fully settled"
                )

            # Calculate maturity value (simple interest)
            interest = principal * term_years * annual_rate
            maturity_value = principal + interest

            if maturity_value <= 0:
                raise InvalidTransaction("Computed maturity_value is not positive; check CDT contract parameters")

            # Insert SELL settlement transaction
            settlement_note = f"CDT_SETTLEMENT:buy_tx_id={buy_tx_id} maturity_date={maturity_date_iso}"
            cursor.execute(
                """
                INSERT INTO transactions
                (asset_id, account_id, tx_type, quantity, unit_price, fee_usd, total_usd, tx_date, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    asset_id,
                    account_id,
                    "SELL",
                    float(qty),
                    float(maturity_value / qty),  # unit_price = maturity_value per unit
                    0.0,
                    float(maturity_value),
                    settlement_date_iso,
                    settlement_note,
                ),
            )
            sell_tx_id = cursor.lastrowid

            # Create lot_match: closes the BUY lot, drives realized PnL = interest
            cursor.execute(
                """
                INSERT INTO lot_matches (buy_tx_id, sell_tx_id, quantity, buy_fee_alloc, sell_fee_alloc)
                VALUES (?, ?, ?, ?, ?)
                """,
                (buy_tx_id, sell_tx_id, float(qty), 0.0, 0.0),
            )

            # Record cash inflow: full maturity_value
            self._record_cash_movement(
                cursor,
                tx_id=sell_tx_id,
                account_id=account_id,
                movement_type="CDT_SETTLEMENT",
                amount_usd=maturity_value,
                note=f"CDT settlement: principal={principal} interest={interest}",
            )

            conn.commit()
            return {
                "buy_tx_id": buy_tx_id,
                "sell_tx_id": sell_tx_id,
                "symbol": symbol,
                "account": account_name,
                "principal": principal,
                "interest": interest,
                "maturity_value": maturity_value,
                "settlement_date": settlement_date_iso,
            }
        except Exception:
            conn.rollback()
            raise
