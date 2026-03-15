# PortfolioTracker v2 - Estado de Proyecto (B8-B10)

## 1. Estado general actualizado

- B8 quedo cerrado con el core de valuacion recuperado y estable.
- B9 y B10 agregaron observabilidad operativa y analitica sobre snapshots (export, historico, comparacion y alertas).
- El core de pricing/valuacion no se modifico de forma disruptiva en estas fases.

## 2. Cierre consolidado de B8

### 2.1 Base operativa

- SQLite es la source of truth.
- CSV queda solo para bootstrap/migracion.
- `refresh-prices` opera sobre holdings activos reales.
- `positions` refleja activos vigentes.
- `summary` separa:
  - Total Equity
  - Market-Covered Value
  - Non-Market Valued
  - Unvalued / Excluded

### 2.2 Cobertura funcional vigente

Soportado hoy:

- holdings crypto soportados
- stocks US soportados
- PEI
- ECOPETROL
- GOLD
- SILVER
- BBVA CDT
- FONDO DINAMICO

Fuera del portfolio activo:

- BAS
- SWTCH

### 2.3 Regla de valuacion aprobada

Una posicion entra al valuation aprobado si cumple una de estas condiciones:

- `market_live` con precio usable
- `snapshot_imported` con valor aprobado disponible
- `contractual_value` con valor aprobado disponible

Ecuacion vigente:

`Total Equity = Market-Covered Value + Non-Market Valued`

## 3. Capacidades agregadas en B9

### 3.1 B9.1 - `refresh-prices --verbose`

Comando:

- `python -m portfolio_tracker_v2 refresh-prices --verbose`

Capacidad:

- diagnostico final por simbolo
- muestra `provider`, `valuation_method`, `outcome`, `reason`, `price_source`, `current_price`, `price_updated_at`
- no muestra ruido de intentos intermedios

### 3.2 B9.2 - Retry/backoff minimo para CoinGecko

Regla aplicada (solo CoinGecko):

- reintentos en `http_429`
- reintentos en `http_5xx`
- reintentos en timeout/connection error transitorio
- maximo 3 intentos
- backoff corto lineal

### 3.3 B9.3 - Asset class breakdown en summary

`summary` ahora incluye breakdown por clases (sobre approved equity):

- Crypto
- Equities
- Metals
- Non-market

### 3.4 B9.4 - Export JSON de summary

Comando:

- `python -m portfolio_tracker_v2 summary --export-json <path>`

Comportamiento:

- reutiliza la misma fuente de datos de `summary`
- crea directorio destino si no existe

## 4. Capacidades agregadas en B10

### 4.1 B10 - Historico timestamped de snapshots

Comando:

- `python -m portfolio_tracker_v2 summary --export-json-history <dir>`

Comportamiento:

- genera archivos tipo `summary_YYYY-MM-DDTHH-MM-SSZ.json`
- formato de nombre seguro para Windows

### 4.2 B10.1 - Comparacion entre snapshots

Comando:

- `python -m portfolio_tracker_v2 compare-summary-snapshots old.json new.json`

Comportamiento:

- compara metricas principales
- compara `market_price_quality`
- compara `asset_class_breakdown`
- muestra `old / new / delta`

### 4.3 B10.2 - Alertas operativas simples sobre snapshots

Comando:

- `python -m portfolio_tracker_v2 alert-summary-snapshots old.json new.json`

Alertas soportadas:

- caida de `total_equity`
- aumento de `unvalued_excluded_cost_basis`
- deterioro de `market_price_quality`
- cambios grandes en `asset_class_breakdown`

Defaults:

- `equity-drop-pct = 3.0`
- `asset-class-shift-pct = 5.0`
- `unvalued-increase-threshold = 0.0`

### 4.4 B10.3 - Daily report consolidado

Comando:

- `python -m portfolio_tracker_v2 daily-report`

Flags soportados:

- `--account <name>`
- `--refresh-verbose`
- `--history-dir <dir>`
- `--skip-refresh`

Flujo:

1. corre `refresh-prices` salvo que se use `--skip-refresh`
2. muestra `summary`
3. exporta snapshot timestamped al historico
4. compara contra el snapshot previo mas reciente si existe
5. evalua alertas sobre snapshots si existe baseline previo

Ejemplos:

- `python -m portfolio_tracker_v2 daily-report`
- `python -m portfolio_tracker_v2 daily-report --refresh-verbose`
- `python -m portfolio_tracker_v2 daily-report --history-dir output/history`
- `python -m portfolio_tracker_v2 daily-report --skip-refresh`

## 5. Comandos operativos vigentes

- `python -m portfolio_tracker_v2 refresh-prices`
- `python -m portfolio_tracker_v2 refresh-prices --verbose`
- `python -m portfolio_tracker_v2 positions`
- `python -m portfolio_tracker_v2 summary`
- `python -m portfolio_tracker_v2 summary --export-json <path>`
- `python -m portfolio_tracker_v2 summary --export-json-history <dir>`
- `python -m portfolio_tracker_v2 compare-summary-snapshots old.json new.json`
- `python -m portfolio_tracker_v2 alert-summary-snapshots old.json new.json`
- `python -m portfolio_tracker_v2 daily-report`
- `python -m portfolio_tracker_v2 daily-report --refresh-verbose`
- `python -m portfolio_tracker_v2 daily-report --history-dir <dir>`
- `python -m portfolio_tracker_v2 daily-report --skip-refresh`

## 6. Matiz operativo clave

`refresh-prices` puede reportar `failed_final` por intermitencias/rate limits externos.

`summary` puede seguir en verde si SQLite conserva precios previos todavia usables.

Esto no contradice el estado funcional del sistema:

- `failed_final` describe el intento de refresh actual
- `summary` describe la valuacion aprobada vigente

## 7. Estado funcional actual

Hoy el sistema se considera funcionalmente estable para el objetivo del tracker:

- pricing core recuperado
- hybrid valuation operando
- observabilidad basica disponible
- export e historico de snapshots disponibles
- comparacion y alertas simples sobre snapshots disponibles

## 8. Proximos pasos sugeridos

Con la base actual, los siguientes pasos naturales son:

1. Daily report consolidado a partir de snapshots.
2. Snapshot de `positions` para trazabilidad de composicion y cambios.
3. Mejoras visuales/reporting (presentacion y legibilidad de resultados).
4. Automatizacion futura (si se decide) para ejecucion periodica de refresh + snapshot + alertas.

---

Documento canonico de estado de PortfolioTracker v2 actualizado hasta B10.
