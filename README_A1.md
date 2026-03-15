# PortfolioTracker v2 - Cierre de Fase B8

## 1. Estado general

La fase B8 queda cerrada.

El sistema de valuacion de PortfolioTracker v2 quedo funcionalmente recuperado y alineado con el portfolio activo real.

## 2. Resultado consolidado

- SQLite es la source of truth operativa.
- CSV queda solo para bootstrap/migracion.
- `refresh-prices` opera sobre holdings activos reales.
- `positions` refleja activos vigentes del portfolio activo.
- `summary` separa:
  - Total Equity
  - Market-Covered Value
  - Non-Market Valued
  - Unvalued / Excluded

## 3. Cobertura funcional lograda

### 3.1 Market live

- Crypto/stablecoin soportados.
- Stocks US soportados.
- PEI.
- ECOPETROL.
- GOLD.
- SILVER.

### 3.2 Non-market valuation

- BBVA CDT -> `contractual_value`.
- FONDO DINAMICO -> `snapshot_imported`.

### 3.3 Cleanup

- BAS y SWTCH quedan fuera del portfolio activo.

## 4. Reglas de valuacion vigentes

### 4.1 Approved valuation

Una posicion entra al valuation aprobado si cumple una de estas condiciones:

- `market_live` con precio usable.
- `snapshot_imported` con valor aprobado disponible.
- `contractual_value` con valor aprobado disponible.

### 4.2 Ecuacion de summary

`Total Equity = Market-Covered Value + Non-Market Valued`

## 5. Universo operativo de refresh

`refresh-prices` procesa solo activos que cumplan:

- `is_active = 1`
- `valuation_method = market_live`
- holding abierto real (`qty_open > 0`)

## 6. Providers y fuentes activas

- CoinGecko -> crypto/stablecoin.
- Alpha Vantage -> stock_us.
- TradingView -> PEI, ECOPETROL, GOLD, SILVER.

## 7. Matiz operativo importante

`refresh-prices` puede mostrar `failed_final` por intermitencias externas o rate limits aunque `summary` siga en verde.

Eso puede ocurrir cuando el intento de refresh actual falla, pero SQLite conserva un precio previo todavia usable.

Por tanto:

- `failed_final` describe el resultado del intento de refresh de esa corrida.
- `summary` describe la valuacion aprobada vigente del portfolio.

Esto no contradice el cierre funcional de B8.

## 8. Estado esperado al cierre

- `positions` sin BAS ni SWTCH.
- `summary` con `Unvalued / Excluded = 0.00`.
- `Non-Market Valued > 0`.
- Cobertura activa coherente con holdings reales.

## 9. Comandos de validacion

```bash
python -m portfolio_tracker_v2 refresh-prices
python -m portfolio_tracker_v2 positions
python -m portfolio_tracker_v2 summary
```

## 10. Proximos pasos recomendados

1. Observabilidad liviana del refresh (modo verbose/diagnostico por simbolo).
2. Retry/backoff minimo para CoinGecko si los `429` se vuelven molestos.
3. Mejoras de UX/reporting (breakdown por asset class y reportes mas claros).

---

Documento de estado de release para cierre de fase B8.
