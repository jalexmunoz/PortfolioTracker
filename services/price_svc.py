"""Price refresh service with explicit provider mappings."""

from dataclasses import dataclass

import requests

from portfolio_tracker_v2.core import Database

COINGECKO_SUPPORTED_TYPES = {"crypto", "stablecoin"}

# Explicit and centralized symbol -> provider id mapping.
COINGECKO_SYMBOL_MAP = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "ADA": "cardano",
    "SOL": "solana",
    "XRP": "ripple",
    "LINK": "chainlink",
    "HBAR": "hedera-hashgraph",
    "JUP": "jupiter-exchange-solana",
    "BAS": "basis-markets",
    "PEPE": "pepe",
    "USDT": "tether",
    "USDC": "usd-coin",
    "DAI": "dai",
    "BUSD": "binance-usd",
}


@dataclass(frozen=True)
class PriceLookupResult:
    status: str
    price: float | None = None
    reason: str | None = None


@dataclass(frozen=True)
class AssetRefreshResult:
    asset_id: int
    symbol: str
    asset_type: str
    status: str
    reason: str
    provider: str | None = None
    provider_symbol: str | None = None


@dataclass
class RefreshReport:
    updated: int = 0
    skipped_unmapped: int = 0
    skipped_unsupported: int = 0
    failed_lookup: int = 0
    results: list[AssetRefreshResult] | None = None

    @property
    def skipped_total(self) -> int:
        return self.skipped_unmapped + self.skipped_unsupported

    def __post_init__(self) -> None:
        if self.results is None:
            self.results = []


def refresh_prices(db: Database) -> RefreshReport:
    """Refresh current prices for active assets and classify skip/failure reasons."""
    conn = db.connect()
    cursor = conn.cursor()

    cursor.execute("SELECT id, symbol, asset_type FROM assets WHERE is_active = 1")
    assets = cursor.fetchall()

    report = RefreshReport()

    for asset_id, symbol, asset_type in assets:
        provider_id, resolution_reason = resolve_coingecko_id(symbol, asset_type)
        symbol_upper = symbol.upper()
        asset_type_lower = asset_type.lower()
        if provider_id is None:
            if resolution_reason == "unmapped_symbol":
                report.skipped_unmapped += 1
                report.results.append(
                    AssetRefreshResult(
                        asset_id=asset_id,
                        symbol=symbol_upper,
                        asset_type=asset_type_lower,
                        status="skipped_unmapped",
                        reason=resolution_reason,
                        provider="coingecko",
                    )
                )
            else:
                report.skipped_unsupported += 1
                report.results.append(
                    AssetRefreshResult(
                        asset_id=asset_id,
                        symbol=symbol_upper,
                        asset_type=asset_type_lower,
                        status="skipped_unsupported",
                        reason=resolution_reason,
                        provider="coingecko",
                    )
                )
            continue

        lookup = get_coingecko_price_by_id(provider_id)
        if lookup.status == "ok":
            cursor.execute(
                "UPDATE assets SET current_price = ?, price_source = ?, price_updated_at = datetime('now') WHERE id = ?",
                (lookup.price, "coingecko", asset_id),
            )
            report.updated += 1
            report.results.append(
                AssetRefreshResult(
                    asset_id=asset_id,
                    symbol=symbol_upper,
                    asset_type=asset_type_lower,
                    status="updated",
                    reason="price_updated",
                    provider="coingecko",
                    provider_symbol=provider_id,
                )
            )
            continue

        if lookup.status == "unsupported":
            report.skipped_unsupported += 1
            report.results.append(
                AssetRefreshResult(
                    asset_id=asset_id,
                    symbol=symbol_upper,
                    asset_type=asset_type_lower,
                    status="skipped_unsupported",
                    reason=lookup.reason or "provider_no_price",
                    provider="coingecko",
                    provider_symbol=provider_id,
                )
            )
            continue

        report.failed_lookup += 1
        report.results.append(
            AssetRefreshResult(
                asset_id=asset_id,
                symbol=symbol_upper,
                asset_type=asset_type_lower,
                status="failed_lookup",
                reason=lookup.reason or "lookup_failed",
                provider="coingecko",
                provider_symbol=provider_id,
            )
        )

    conn.commit()
    return report


def resolve_coingecko_id(symbol: str, asset_type: str) -> tuple[str | None, str]:
    """Resolve symbol to CoinGecko id with explicit reasons."""
    symbol_upper = symbol.upper()
    asset_type_lower = asset_type.lower()
    if asset_type_lower not in COINGECKO_SUPPORTED_TYPES:
        if symbol_upper in COINGECKO_SYMBOL_MAP:
            return COINGECKO_SYMBOL_MAP[symbol_upper], "mapped_override"
        return None, f"unsupported_asset_type:{asset_type_lower}"

    coin_id = COINGECKO_SYMBOL_MAP.get(symbol_upper)
    if not coin_id:
        return None, "unmapped_symbol"
    return coin_id, "mapped"


def get_coingecko_price_by_id(coin_id: str) -> PriceLookupResult:
    """Fetch USD price by CoinGecko coin id."""
    try:
        url = f"https://api.coingecko.com/api/v3/simple/price?ids={coin_id}&vs_currencies=usd"
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            return PriceLookupResult(status="failed", reason=f"http_{response.status_code}")
        data = response.json()
        usd_price = data.get(coin_id, {}).get("usd")
        if usd_price is None:
            return PriceLookupResult(status="unsupported", reason="provider_no_usd_price")
        return PriceLookupResult(status="ok", price=float(usd_price))
    except Exception:
        return PriceLookupResult(status="failed", reason="request_exception")


def get_crypto_price(symbol: str) -> float | None:
    """Backward-compatible helper used by existing tests/callers."""
    coin_id = COINGECKO_SYMBOL_MAP.get(symbol.upper())
    if not coin_id:
        return None
    lookup = get_coingecko_price_by_id(coin_id)
    if lookup.status != "ok":
        return None
    return lookup.price
