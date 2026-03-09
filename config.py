"""
Centralized configuration for Portfolio Tracker v2.
"""

import os
from pathlib import Path

# Database path (relative to package root)
PACKAGE_ROOT = Path(__file__).parent
DB_PATH = str(PACKAGE_ROOT / "portfolio.db")

# CSV paths
LEGACY_CSV_PATH = str(PACKAGE_ROOT.parent / "legacy" / "portfoliototal.csv")

# Logging
LOG_LEVEL = "INFO"

# Constants
USD_CASH_SYMBOL = "__USD_CASH__"

# Defaults for transactions
DEFAULT_ACCOUNT_TYPE = "wallet"
DEFAULT_ASSET_CURRENCY = "USD"
DEFAULT_ASSET_DIVISOR = 1.0

# External providers
ALPHA_VANTAGE_API_KEY = os.environ.get("ALPHA_VANTAGE_API_KEY")
