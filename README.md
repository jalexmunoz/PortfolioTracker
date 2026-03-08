# PortfolioTracker

Transaction-based portfolio tracker (SQLite).

Current status: B5 complete (services + FIFO + CLI + alerts + price refresh)

## Quick Start

1. Install dependencies: `pip install -r requirements.txt`
2. Initialize DB: `python -m portfolio_tracker_v2 init-db`
3. Import CSV: `python -m portfolio_tracker_v2 import-csv --input portfoliototal.csv --execute`
4. Refresh prices: `python -m portfolio_tracker_v2 refresh-prices`
5. View positions: `python -m portfolio_tracker_v2 positions`

See: README_A1.md
