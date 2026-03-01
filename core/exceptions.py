"""
Custom exceptions for Portfolio Tracker v2.
"""


class PortfolioError(Exception):
    """Base exception for portfolio tracking."""
    pass


class AssetNotFound(PortfolioError):
    """Raised when an asset is not found in the database."""
    
    def __init__(self, symbol: str):
        super().__init__(f"Asset not found: {symbol}")


class AccountNotFound(PortfolioError):
    """Raised when an account is not found in the database."""
    
    def __init__(self, name: str):
        super().__init__(f"Account not found: {name}")


class InvalidTransaction(PortfolioError):
    """Raised when transaction parameters are invalid."""
    
    def __init__(self, message: str):
        super().__init__(f"Invalid transaction: {message}")


class CSVImportError(PortfolioError):
    """Raised when CSV import fails."""
    
    def __init__(self, message: str):
        super().__init__(f"CSV import error: {message}")


class DatabaseError(PortfolioError):
    """Raised when a database operation fails."""
    
    def __init__(self, message: str):
        super().__init__(f"Database error: {message}")
