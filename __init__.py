"""
Portfolio Tracker v2 - Transaction-based portfolio management.
"""

__version__ = "2.0.0-alpha"

# Exports públicas desde core
from .core import (
    Database,
    AssetResolver,
    PortfolioError,
    AssetNotFound,
    DatabaseError,
)

__all__ = [
    'Database',
    'AssetResolver',
    'PortfolioError',
    'AssetNotFound',
    'DatabaseError',
    '__version__',
]
