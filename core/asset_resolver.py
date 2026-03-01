"""
Asset registry and symbol resolution.
"""

from typing import Optional, Dict
import sqlite3

from .database import Database
from .exceptions import AssetNotFound, DatabaseError


class AssetResolver:
    """
    Manages asset resolution and registry.
    
    Normalizes symbols (strip, upper) and applies aliases.
    Auto-creates unknown assets with type='UNKNOWN'.
    """
    
    # Symbol aliases mapping (common variations)
    SYMBOL_ALIASES = {
        'GOOGLE': 'GOOG',
        'GOOGL': 'GOOG',
        'APPLE': 'AAPL',
        'TESLA': 'TSLA',
        'PATRIMONIO': 'PEI',
        'ORO': 'GOLD',
        'PLATA': 'SILVER',
        'XAU': 'GOLD',
        'XAG': 'SILVER',
        'BITCOIN': 'BTC',
        'ETHEREUM': 'ETH',
    }
    
    # Hardcoded asset type detection (symbol -> asset_type)
    HARDCODED_TYPES = {
        # Crypto
        'BTC': 'crypto',
        'ETH': 'crypto',
        'SOL': 'crypto',
        'LINK': 'crypto',
        'HBAR': 'crypto',
        'JUP': 'crypto',
        'PEPE': 'crypto',
        'DOGE': 'crypto',
        'UNI': 'crypto',
        'AAVE': 'crypto',
        'MATIC': 'crypto',
        'ATOM': 'crypto',
        'XRP': 'crypto',
        'ADA': 'crypto',
        'DOT': 'crypto',
        'AVAX': 'crypto',
        'LTC': 'crypto',
        'BCH': 'crypto',
        'BNB': 'crypto',
        
        # US Stocks
        'AAPL': 'stock_us',
        'MSFT': 'stock_us',
        'GOOG': 'stock_us',
        'AMZN': 'stock_us',
        'NVDA': 'stock_us',
        'META': 'stock_us',
        'TSLA': 'stock_us',
        'NFLX': 'stock_us',
        'AMD': 'stock_us',
        'INTC': 'stock_us',
        'PYPL': 'stock_us',
        'PLTR': 'stock_us',
        'BRK.B': 'stock_us',
        'BRKB': 'stock_us',
        'JPM': 'stock_us',
        'BAC': 'stock_us',
        'WFC': 'stock_us',
        'V': 'stock_us',
        'MA': 'stock_us',
        'DIS': 'stock_us',
        'NKE': 'stock_us',
        'KO': 'stock_us',
        'PEP': 'stock_us',
        'VZ': 'stock_us',
        'T': 'stock_us',
        'WMT': 'stock_us',
        'HD': 'stock_us',
        'MCD': 'stock_us',
        'COST': 'stock_us',
        'CRM': 'stock_us',
        'ORCL': 'stock_us',
        'CSCO': 'stock_us',
        'IBM': 'stock_us',
        'GE': 'stock_us',
        'F': 'stock_us',
        'GM': 'stock_us',
        'BA': 'stock_us',
        'CAT': 'stock_us',
        'XOM': 'stock_us',
        'CVX': 'stock_us',
        'CEG': 'stock_us',
        
        # International Stocks
        'ECOPETROL': 'stock_intl',
        'EC': 'stock_intl',
        'PEI': 'stock_intl',
        'BBVA': 'stock_intl',
        'SAN': 'stock_intl',
        
        # Commodities / Metals
        'GOLD': 'commodity',
        'SILVER': 'commodity',
        
        # Stablecoins
        'USDT': 'stablecoin',
        'USDC': 'stablecoin',
        'DAI': 'stablecoin',
        'BUSD': 'stablecoin',
        
        # Special: USD Cash
        '__USD_CASH__': 'cash',
    }
    
    # TradingView and exchange mappings (optional)
    TRADINGVIEW_MAP = {
        'BTC': ('BTCUSDT', 'BINANCE'),
        'ETH': ('ETHUSDT', 'BINANCE'),
        'SOL': ('SOLUSDT', 'BINANCE'),
        'AAPL': ('AAPL', 'NASDAQ'),
        'MSFT': ('MSFT', 'NASDAQ'),
        'GOOG': ('GOOG', 'NASDAQ'),
        'GOLD': ('GOLD', 'TVC'),
        'SILVER': ('SILVER', 'TVC'),
    }
    
    def __init__(self, db: Database):
        """
        Initialize AssetResolver with database connection.
        
        Args:
            db: Database instance (must be connected).
        """
        self.db = db
    
    def resolve(self, symbol: str) -> Dict:
        """
        Resolve symbol to asset dict. Auto-creates if not found.
        
        Normalizes symbol (strip, upper) and applies aliases.
        If asset not found, creates it with asset_type='UNKNOWN'.
        
        Args:
            symbol: Raw symbol string (e.g., '  btc  ', 'GOOGLE').
        
        Returns:
            dict with keys: id, symbol, asset_type, tradingview_symbol, 
                           exchange, currency, divisor
        
        Raises:
            DatabaseError if query/insert fails.
        """
        if not symbol or not symbol.strip():
            raise DatabaseError("Symbol cannot be empty or None")
        # Normalize
        symbol_normalized = self._normalize_symbol(symbol)
        
        # Apply aliases
        symbol_resolved = self._apply_aliases(symbol_normalized)
        
        # Try to get existing asset
        asset = self._get_asset_internal(symbol_resolved)
        
        if asset:
            return asset
        
        # Auto-create with UNKNOWN type
        asset_type = self._hardcoded_type(symbol_resolved) or 'UNKNOWN'
        tv_symbol, exchange = self.TRADINGVIEW_MAP.get(symbol_resolved, (None, None))
        
        return self._create_asset(
            symbol=symbol_resolved,
            asset_type=asset_type,
            tradingview_symbol=tv_symbol,
            exchange=exchange,
            currency='USD',
            divisor=1.0
        )
    
    def get_asset(self, symbol: str) -> Dict:
        """
        Query asset by symbol. Raises if not found.
        
        Args:
            symbol: Raw symbol string.
        
        Returns:
            Asset dict.
        
        Raises:
            AssetNotFound if symbol not in database.
            DatabaseError if query fails.
        """
        symbol_normalized = self._normalize_symbol(symbol)
        symbol_resolved = self._apply_aliases(symbol_normalized)
        
        asset = self._get_asset_internal(symbol_resolved)
        
        if not asset:
            raise AssetNotFound(symbol)
        
        return asset
    
    def get_or_create_usd_cash(self) -> Dict:
        """
        Get or create __USD_CASH__ special asset.
        
        Used for DEPOSIT, WITHDRAWAL, FEE transactions.
        
        Returns:
            USD_CASH asset dict.
        """
        try:
            return self.get_asset('__USD_CASH__')
        except AssetNotFound:
            return self._create_asset(
                symbol='__USD_CASH__',
                asset_type='cash',
                tradingview_symbol=None,
                exchange=None,
                currency='USD',
                divisor=1.0
            )
    
    def list_all(self) -> list[Dict]:
        """
        List all assets in database.
        
        Returns:
            List of asset dicts.
        
        Raises:
            DatabaseError if query fails.
        """
        try:
            cursor = self.db.execute("SELECT * FROM assets ORDER BY symbol ASC")
            rows = cursor.fetchall()
            return [self._row_to_dict(row) for row in rows]
        except Exception as e:
            raise DatabaseError(f"Failed to list assets: {e}")
    
    # Private helper methods
    
    def _normalize_symbol(self, symbol: str) -> str:
        """Normalize symbol: strip whitespace and convert to uppercase."""
        return symbol.strip().upper()
    
    def _apply_aliases(self, symbol: str) -> str:
        """Apply symbol alias mapping if exists."""
        return self.SYMBOL_ALIASES.get(symbol, symbol)
    
    def _hardcoded_type(self, symbol: str) -> Optional[str]:
        """Get hardcoded asset type for symbol, if known."""
        return self.HARDCODED_TYPES.get(symbol)
    
    def _get_asset_internal(self, symbol: str) -> Optional[Dict]:
        """
        Internal fetch of asset by symbol.
        
        Returns None if not found. Raises DatabaseError on SQL error.
        """
        try:
            cursor = self.db.execute(
                "SELECT * FROM assets WHERE symbol = ?",
                (symbol,)
            )
            row = cursor.fetchone()
            if not row:
                return None
            return self._row_to_dict(row)
        except sqlite3.Error as e:
            # propagate DB errors explicitly
            raise DatabaseError(f"SQL error fetching asset '{symbol}': {e}")
    
    def _row_to_dict(self, row: sqlite3.Row) -> Dict:
        """Convert sqlite3.Row to dict."""
        if row is None:
            return None
        return {
            'id': row['id'],
            'symbol': row['symbol'],
            'asset_type': row['asset_type'],
            'tradingview_symbol': row['tradingview_symbol'],
            'exchange': row['exchange'],
            'currency': row['currency'],
            'divisor': row['divisor'],
            'is_active': row['is_active'],
        }
    
    def _create_asset(
        self,
        symbol: str,
        asset_type: str,
        tradingview_symbol: Optional[str] = None,
        exchange: Optional[str] = None,
        currency: str = 'USD',
        divisor: float = 1.0
    ) -> Dict:
        """
        Create new asset in database.
        
        Args:
            symbol: Normalized symbol.
            asset_type: Type of asset.
            tradingview_symbol: TradingView symbol for price fetching.
            exchange: Exchange name.
            currency: Currency for prices.
            divisor: Price divisor (for currency conversion or fractional).
        
        Returns:
            Created asset dict.
        
        Raises:
            DatabaseError if insert fails.
        """
        try:
            cursor = self.db.execute(
                """
                INSERT INTO assets 
                (symbol, asset_type, tradingview_symbol, exchange, currency, divisor)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (symbol, asset_type, tradingview_symbol, exchange, currency, divisor)
            )
            # commit should be done by caller, not here
            
            # Fetch and return the created asset
            asset_id = cursor.lastrowid
            cursor = self.db.execute("SELECT * FROM assets WHERE id = ?", (asset_id,))
            row = cursor.fetchone()
            return self._row_to_dict(row)
        except sqlite3.IntegrityError as e:
            raise DatabaseError(f"Asset '{symbol}' already exists: {e}")
        except Exception as e:
            raise DatabaseError(f"Failed to create asset: {e}")
