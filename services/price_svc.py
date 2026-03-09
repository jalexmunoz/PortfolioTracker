"""Price refresh service with explicit provider mappings."""

import os
import re
from dataclasses import dataclass

import requests

from portfolio_tracker_v2 import config
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

ALPHA_VANTAGE_STOCK_OVERRIDES = {
    "BRK.B": "BRK.B",
    "BRKB": "BRK.B",
}

ALPHA_VANTAGE_COMMODITY_MAP = {
    "GOLD": "XAU",
    "SILVER": "XAG",
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


@dataclass(frozen=True)
class ProviderResolution:
    status: str
    provider: str | None = None
    provider_symbol: str | None = None
    price_source: str | None = None
    reason: str | None = None


def refresh_prices(db: Database) -> RefreshReport:
    """Refresh current prices for active assets and classify skip/failure reasons."""
    conn = db.connect()
    cursor = conn.cursor()

    cursor.execute("SELECT id, symbol, asset_type FROM assets WHERE is_active = 1")
    assets = cursor.fetchall()

    report = RefreshReport()

    for asset_id, symbol, asset_type in assets:
        symbol_upper = symbol.upper()
        asset_type_lower = asset_type.lower()
        resolution = resolve_provider(symbol_upper, asset_type_lower)

        if resolution.status == "unmapped":
            report.skipped_unmapped += 1
            report.results.append(
                AssetRefreshResult(
                    asset_id=asset_id,
                    symbol=symbol_upper,
                    asset_type=asset_type_lower,
                    status="skipped_unmapped",
                    reason=resolution.reason or "unmapped_symbol",
                    provider=resolution.provider,
                    provider_symbol=resolution.provider_symbol,
                )
            )
            continue

        if resolution.status == "unsupported":
            report.skipped_unsupported += 1
            report.results.append(
                AssetRefreshResult(
                    asset_id=asset_id,
                    symbol=symbol_upper,
                    asset_type=asset_type_lower,
                    status="skipped_unsupported",
                    reason=resolution.reason or "unsupported",
                    provider=resolution.provider,
                    provider_symbol=resolution.provider_symbol,
                )
            )
            continue

        lookup = lookup_price(resolution)
        if lookup.status == "ok":
            cursor.execute(
                "UPDATE assets SET current_price = ?, price_source = ?, price_updated_at = datetime('now') WHERE id = ?",
                (lookup.price, resolution.price_source, asset_id),
            )
            report.updated += 1
            report.results.append(
                AssetRefreshResult(
                    asset_id=asset_id,
                    symbol=symbol_upper,
                    asset_type=asset_type_lower,
                    status="updated",
                    reason="price_updated",
                    provider=resolution.provider,
                    provider_symbol=resolution.provider_symbol,
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
                    provider=resolution.provider,
                    provider_symbol=resolution.provider_symbol,
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
                provider=resolution.provider,
                provider_symbol=resolution.provider_symbol,
            )
        )

    conn.commit()
    return report


def resolve_provider(symbol: str, asset_type: str) -> ProviderResolution:
    """Resolve provider and provider symbol by asset type and symbol."""
    if asset_type in COINGECKO_SUPPORTED_TYPES:
        coin_id = COINGECKO_SYMBOL_MAP.get(symbol)
        if not coin_id:
            return ProviderResolution(status="unmapped", provider="coingecko", reason="unmapped_symbol")
        return ProviderResolution(
            status="ok",
            provider="coingecko",
            provider_symbol=coin_id,
            price_source="coingecko",
        )

    if asset_type == "stock_us":
        av_symbol = resolve_alpha_vantage_stock_symbol(symbol)
        if not av_symbol:
            return ProviderResolution(status="unmapped", provider="alpha_vantage", reason="unmapped_symbol")
        return ProviderResolution(
            status="ok",
            provider="alpha_vantage",
            provider_symbol=av_symbol,
            price_source="alpha_vantage_global_quote",
        )

    if asset_type == "commodity":
        commodity_symbol = ALPHA_VANTAGE_COMMODITY_MAP.get(symbol)
        if not commodity_symbol:
            return ProviderResolution(status="unmapped", provider="alpha_vantage", reason="unmapped_symbol")
        return ProviderResolution(
            status="ok",
            provider="alpha_vantage",
            provider_symbol=commodity_symbol,
            price_source="alpha_vantage_gold_silver_spot",
        )

    return ProviderResolution(
        status="unsupported",
        reason=f"unsupported_asset_type:{asset_type}",
    )


def resolve_alpha_vantage_stock_symbol(symbol: str) -> str | None:
    if symbol in ALPHA_VANTAGE_STOCK_OVERRIDES:
        return ALPHA_VANTAGE_STOCK_OVERRIDES[symbol]
    if re.fullmatch(r"[A-Z][A-Z0-9.-]{0,9}", symbol):
        return symbol
    return None


def lookup_price(resolution: ProviderResolution) -> PriceLookupResult:
    if resolution.provider == "coingecko":
        return get_coingecko_price_by_id(resolution.provider_symbol or "")

    if resolution.provider == "alpha_vantage" and resolution.price_source == "alpha_vantage_global_quote":
        return get_alpha_vantage_stock_price(resolution.provider_symbol or "")

    if resolution.provider == "alpha_vantage" and resolution.price_source == "alpha_vantage_gold_silver_spot":
        return get_alpha_vantage_gold_silver_price(resolution.provider_symbol or "")

    return PriceLookupResult(status="failed", reason="unsupported_provider")


def get_alpha_vantage_api_key() -> str | None:
    return os.environ.get("ALPHA_VANTAGE_API_KEY") or config.ALPHA_VANTAGE_API_KEY


def alpha_vantage_query(params: dict[str, str]) -> tuple[int, dict]:
    response = requests.get("https://www.alphavantage.co/query", params=params, timeout=10)
    return response.status_code, response.json()


def get_alpha_vantage_stock_price(symbol: str) -> PriceLookupResult:
    api_key = get_alpha_vantage_api_key()
    if not api_key:
        return PriceLookupResult(status="unsupported", reason="provider_not_configured:alpha_vantage")

    try:
        status_code, data = alpha_vantage_query(
            {
                "function": "GLOBAL_QUOTE",
                "symbol": symbol,
                "apikey": api_key,
            }
        )
    except Exception:
        return PriceLookupResult(status="failed", reason="request_exception")

    if status_code != 200:
        return PriceLookupResult(status="failed", reason=f"http_{status_code}")
    if "Note" in data:
        return PriceLookupResult(status="failed", reason="rate_limited")

    price_value = (data.get("Global Quote") or {}).get("05. price")
    if not price_value:
        return PriceLookupResult(status="unsupported", reason="provider_no_price")

    try:
        return PriceLookupResult(status="ok", price=float(price_value))
    except (TypeError, ValueError):
        return PriceLookupResult(status="unsupported", reason="provider_no_price")


def get_alpha_vantage_gold_silver_price(from_currency: str) -> PriceLookupResult:
    api_key = get_alpha_vantage_api_key()
    if not api_key:
        return PriceLookupResult(status="unsupported", reason="provider_not_configured:alpha_vantage")

    try:
        status_code, data = alpha_vantage_query(
            {
                "function": "CURRENCY_EXCHANGE_RATE",
                "from_currency": from_currency,
                "to_currency": "USD",
                "apikey": api_key,
            }
        )
    except Exception:
        return PriceLookupResult(status="failed", reason="request_exception")

    if status_code != 200:
        return PriceLookupResult(status="failed", reason=f"http_{status_code}")
    if "Note" in data:
        return PriceLookupResult(status="failed", reason="rate_limited")

    exchange_rate = (data.get("Realtime Currency Exchange Rate") or {}).get("5. Exchange Rate")
    if not exchange_rate:
        return PriceLookupResult(status="unsupported", reason="provider_no_price")

    try:
        return PriceLookupResult(status="ok", price=float(exchange_rate))
    except (TypeError, ValueError):
        return PriceLookupResult(status="unsupported", reason="provider_no_price")


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
            return PriceLookupResult(status="unsupported", reason="provider_no_price")
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
