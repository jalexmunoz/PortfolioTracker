"""
Price refresh service.
"""
import requests
from portfolio_tracker_v2.core import Database


def refresh_prices(db: Database) -> tuple[int, int]:
    """
    Refresh current prices for active assets.
    
    Returns (updated_count, skipped_count)
    """
    conn = db.connect()
    cursor = conn.cursor()
    
    cursor.execute("SELECT id, symbol, asset_type FROM assets WHERE is_active = 1")
    assets = cursor.fetchall()
    
    updated = 0
    skipped = 0
    
    for asset_id, symbol, asset_type in assets:
        if asset_type.lower() == 'crypto':
            price = get_crypto_price(symbol)
            if price is not None:
                cursor.execute(
                    "UPDATE assets SET current_price = ?, price_source = ?, price_updated_at = datetime('now') WHERE id = ?",
                    (price, 'coingecko', asset_id)
                )
                updated += 1
            else:
                skipped += 1
        else:
            skipped += 1
    
    conn.commit()
    return updated, skipped


def get_crypto_price(symbol: str) -> float | None:
    """
    Get current USD price for a crypto symbol using CoinGecko.
    """
    # Simple mapping for common symbols
    mapping = {
        'BTC': 'bitcoin',
        'ETH': 'ethereum',
        'ADA': 'cardano',
        'SOL': 'solana',
        'XRP': 'ripple',
        'LINK': 'chainlink',
        'HBAR': 'hedera-hashgraph',
        'JUP': 'jupiter',
        'BAS': 'basis-markets',
        'PEPE': 'pepe',
        'USDT': 'tether',
    }
    
    coin_id = mapping.get(symbol.upper())
    if not coin_id:
        return None
    
    try:
        url = f"https://api.coingecko.com/api/v3/simple/price?ids={coin_id}&vs_currencies=usd"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            return data.get(coin_id, {}).get('usd')
    except Exception:
        pass
    
    return None