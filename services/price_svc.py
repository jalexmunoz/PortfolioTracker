"""Price refresh service with explicit provider mappings."""

import os
import time
import re
from dataclasses import dataclass

import requests

from portfolio_tracker_v2 import config
from portfolio_tracker_v2.core import Database

COINGECKO_SUPPORTED_TYPES = {"crypto", "stablecoin"}
COINGECKO_RETRY_ATTEMPTS = 3
COINGECKO_RETRY_BASE_DELAY_SECONDS = 0.4
TRADINGVIEW_RETRY_ATTEMPTS = 3
TRADINGVIEW_RETRY_DELAY_SECONDS = 0.5

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

TRADINGVIEW_COMMODITY_MAP = {
    "GOLD": {"tradingview_symbol": "GOLD", "exchange": "TVC", "currency": "USD", "divisor": 1.0},
    "SILVER": {"tradingview_symbol": "SILVER", "exchange": "TVC", "currency": "USD", "divisor": 1.0},
}

TRADINGVIEW_STOCK_INTL_MAP = {
    "ECOPETROL": {"tradingview_symbol": "ECOPETROL", "exchange": "BVC", "currency": "COP", "divisor": 1.0},
    "PEI": {"tradingview_symbol": "PEI", "exchange": "BVC", "currency": "COP", "divisor": 1.0},
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
    failed_final: int = 0
    results: list[AssetRefreshResult] | None = None

    @property
    def skipped_total(self) -> int:
        return self.skipped_unmapped + self.skipped_unsupported

    @property
    def failed_lookup(self) -> int:
        return self.failed_final

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
    exchange: str | None = None
    currency: str | None = None
    divisor: float = 1.0


def refresh_prices(db: Database) -> RefreshReport:
    """Refresh current prices for active assets and classify skip/failure reasons."""
    conn = db.connect()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT a.id, a.symbol, a.asset_type
        FROM assets a
        WHERE a.is_active = 1
          AND COALESCE(a.valuation_method, 'market_live') = 'market_live'
          AND (
                COALESCE(
                    (SELECT SUM(t.quantity) FROM transactions t
                     WHERE t.asset_id = a.id AND t.tx_type IN ('BUY', 'MIGRATION_BUY')),
                    0
                )
                -
                COALESCE(
                    (SELECT SUM(t.quantity) FROM transactions t
                     WHERE t.asset_id = a.id AND t.tx_type = 'SELL'),
                    0
                )
              ) > 0
        """
    )
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

        persist_asset_market_metadata(cursor, asset_id, resolution)
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

        report.failed_final += 1
        report.results.append(
            AssetRefreshResult(
                asset_id=asset_id,
                symbol=symbol_upper,
                asset_type=asset_type_lower,
                status="failed_final",
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
        commodity_metadata = TRADINGVIEW_COMMODITY_MAP.get(symbol)
        if not commodity_metadata:
            return ProviderResolution(status="unmapped", provider="tradingview", reason="unmapped_symbol")
        return ProviderResolution(
            status="ok",
            provider="tradingview",
            provider_symbol=commodity_metadata["tradingview_symbol"],
            price_source="tradingview_tvc_spot",
            exchange=commodity_metadata["exchange"],
            currency=commodity_metadata["currency"],
            divisor=float(commodity_metadata["divisor"]),
        )

    if asset_type == "stock_intl":
        intl_metadata = TRADINGVIEW_STOCK_INTL_MAP.get(symbol)
        if not intl_metadata:
            return ProviderResolution(
                status="unsupported",
                provider="tradingview",
                reason="unsupported_asset_type:stock_intl",
            )
        return ProviderResolution(
            status="ok",
            provider="tradingview",
            provider_symbol=intl_metadata["tradingview_symbol"],
            price_source="tradingview_bvc_fx",
            exchange=intl_metadata["exchange"],
            currency=intl_metadata["currency"],
            divisor=float(intl_metadata["divisor"]),
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

    if resolution.provider == "tradingview" and resolution.price_source == "tradingview_tvc_spot":
        return get_tradingview_commodity_price(resolution)

    if resolution.provider == "tradingview" and resolution.price_source == "tradingview_bvc_fx":
        return get_tradingview_stock_intl_price(resolution)

    return PriceLookupResult(status="failed", reason="unsupported_provider")


def persist_asset_market_metadata(cursor, asset_id: int, resolution: ProviderResolution) -> None:
    """Persist deterministic market metadata separately from price timestamps."""
    if not resolution.provider_symbol and not resolution.exchange and not resolution.currency:
        return

    cursor.execute(
        """
        UPDATE assets
        SET tradingview_symbol = COALESCE(?, tradingview_symbol),
            exchange = COALESCE(?, exchange),
            currency = COALESCE(?, currency),
            divisor = ?
        WHERE id = ?
        """,
        (
            resolution.provider_symbol,
            resolution.exchange,
            resolution.currency,
            resolution.divisor,
            asset_id,
        ),
    )


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


def get_tradingview_stock_intl_price(resolution: ProviderResolution) -> PriceLookupResult:
    """Fetch local TradingView close and convert to USD using live FX."""
    provider_symbol = resolution.provider_symbol or ""
    exchange = resolution.exchange or ""
    currency = resolution.currency or "USD"

    try:
        local_price = get_tradingview_latest_close(provider_symbol, exchange)
        if local_price is None:
            return PriceLookupResult(status="unsupported", reason="provider_no_price")

        divisor = resolution.divisor or 1.0
        if divisor != 1.0:
            local_price = local_price / divisor

        if currency == "USD":
            return PriceLookupResult(status="ok", price=float(local_price))

        fx_rate = get_usd_base_exchange_rate(currency)
        if fx_rate is None or fx_rate <= 0:
            return PriceLookupResult(status="unsupported", reason="provider_no_price")

        return PriceLookupResult(status="ok", price=float(local_price / fx_rate))
    except ImportError:
        return PriceLookupResult(status="unsupported", reason="provider_not_configured:tradingview")
    except Exception:
        return PriceLookupResult(status="failed", reason="request_exception")


def get_tradingview_commodity_price(resolution: ProviderResolution) -> PriceLookupResult:
    provider_symbol = resolution.provider_symbol or ""
    exchange = resolution.exchange or ""

    try:
        local_price = get_tradingview_latest_close(provider_symbol, exchange)
        if local_price is None:
            return PriceLookupResult(status="unsupported", reason="provider_no_price")

        divisor = resolution.divisor or 1.0
        if divisor != 1.0:
            local_price = local_price / divisor

        return PriceLookupResult(status="ok", price=float(local_price))
    except ImportError:
        return PriceLookupResult(status="unsupported", reason="provider_not_configured:tradingview")
    except Exception:
        return PriceLookupResult(status="failed", reason="request_exception")


def get_tradingview_latest_close(symbol: str, exchange: str) -> float | None:
    """Fetch the latest TradingView close from the v2 TradingView shim."""
    from portfolio_tracker_v2.services.tradingview_fetcher import get_tradingview_ohlc

    for attempt in range(1, TRADINGVIEW_RETRY_ATTEMPTS + 1):
        df = get_tradingview_ohlc(symbol=symbol, exchange=exchange, n_bars=1)
        if df is not None and not df.empty:
            close_value = df["close"].iloc[-1]
            return float(close_value) if close_value is not None else None
        if attempt < TRADINGVIEW_RETRY_ATTEMPTS:
            time.sleep(TRADINGVIEW_RETRY_DELAY_SECONDS)
    return None


def get_usd_base_exchange_rate(currency: str) -> float | None:
    """Return TradingView FX quote in legacy format: units of currency per 1 USD."""
    if currency == "USD":
        return 1.0
    return get_tradingview_latest_close(f"USD{currency}", "FX_IDC")


def get_coingecko_price_by_id(coin_id: str) -> PriceLookupResult:
    """Fetch USD price by CoinGecko coin id with minimal retry for transient failures."""
    url = f"https://api.coingecko.com/api/v3/simple/price?ids={coin_id}&vs_currencies=usd"

    for attempt in range(1, COINGECKO_RETRY_ATTEMPTS + 1):
        try:
            response = requests.get(url, timeout=10)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            if attempt < COINGECKO_RETRY_ATTEMPTS:
                time.sleep(COINGECKO_RETRY_BASE_DELAY_SECONDS * attempt)
                continue
            return PriceLookupResult(status="failed", reason="request_exception")
        except requests.exceptions.RequestException:
            return PriceLookupResult(status="failed", reason="request_exception")
        except Exception:
            return PriceLookupResult(status="failed", reason="request_exception")

        if response.status_code == 200:
            try:
                data = response.json()
            except ValueError:
                return PriceLookupResult(status="failed", reason="request_exception")
            usd_price = data.get(coin_id, {}).get("usd")
            if usd_price is None:
                return PriceLookupResult(status="unsupported", reason="provider_no_price")
            return PriceLookupResult(status="ok", price=float(usd_price))

        should_retry = response.status_code == 429 or response.status_code >= 500
        if should_retry and attempt < COINGECKO_RETRY_ATTEMPTS:
            time.sleep(COINGECKO_RETRY_BASE_DELAY_SECONDS * attempt)
            continue

        return PriceLookupResult(status="failed", reason=f"http_{response.status_code}")

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

