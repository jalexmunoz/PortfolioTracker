# PortfolioTracker v2 - Estado de Proyecto (B8-B14)

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

## 4. Capacidades agregadas en B10-B14

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
- `python -m portfolio_tracker_v2 prune-summary-history --history-dir <dir> --keep-last <N>`
- `python -m portfolio_tracker_v2 validate-daily-report-json <path>`
- `python -m portfolio_tracker_v2 show-latest-daily-report --path <file>`
- `python -m portfolio_tracker_v2 validate-daily-report-json <path>`

Alertas soportadas:

- caida de `total_equity`
- aumento de `unvalued_excluded_cost_basis`
- deterioro de `market_price_quality`
- cambios grandes en `asset_class_breakdown`

Defaults:

- `equity-drop-pct = 3.0`
- `asset-class-shift-pct = 5.0`
- `unvalued-increase-threshold = 0.0`

### 4.4 B11 - Daily report consolidado

Comando:

- `python -m portfolio_tracker_v2 daily-report`

Flags soportados:

- `--account <name>`
- `--refresh-verbose`
- `--history-dir <dir>`
- `--skip-refresh`
- `--output-json <path>`
- `--output-json -`
- `--output-json-history-dir <dir>`

Flujo:

1. corre `refresh-prices` salvo que se use `--skip-refresh`
2. muestra `summary`
3. exporta snapshot timestamped al historico
4. compara contra el snapshot previo mas reciente si existe
5. evalua alertas sobre snapshots si existe baseline previo
6. retorna exit code final segun estado operativo/alertas

Ejemplos:

- `python -m portfolio_tracker_v2 daily-report`
- `python -m portfolio_tracker_v2 daily-report --skip-refresh`
- `python -m portfolio_tracker_v2 daily-report --history-dir output/history`
- `python -m portfolio_tracker_v2 daily-report --refresh-verbose`
- `python -m portfolio_tracker_v2 daily-report --output-json output/daily_report.json`
- `python -m portfolio_tracker_v2 daily-report --output-json -`
- `python -m portfolio_tracker_v2 daily-report --output-json-history-dir <dir>`
- `python -m portfolio_tracker_v2 daily-report --output-json - > output/daily_report.json`
- `python -m portfolio_tracker_v2 daily-report --output-json-history-dir output/reports/history`
- `python -m portfolio_tracker_v2 daily-report --output-json output/daily_report.json --output-json-history-dir output/reports/history`

### 4.5 B12 - Contrato de exit codes

Contrato vigente para `daily-report`:

- `0` = OK sin alertas
- `1` = OK con alertas activadas
- `2` = error operativo / input invalido / excepcion controlada

### 4.6 B13-B14 - Contrato de salida de `--output-json`

Modos de salida:

- salida humana interactiva (default): bloques legibles para operacion manual
- `--output-json <path>`: mantiene salida humana y ademas escribe JSON estructurado a archivo
- `--output-json -`: emite JSON estructurado puro por stdout, sin bloques humanos y sin archivo
- `--output-json-history-dir <dir>`: escribe ademas snapshots timestamped del JSON completo sin reemplazar `--output-json <path>`

Encabezado minimo del JSON de `daily-report`:

```json
{
  "report_type": "daily-report",
  "report_schema_version": 1,
  "run_timestamp": "2026-03-15T20:00:00+00:00"
}
```

`report_schema_version` versiona el contrato del payload para scripting futuro. En esta fase queda en `1` y los campos existentes no se renombraron ni se removieron.

### 4.7 B17 - Wrapper operativo minimo en Windows

Wrappers agregados para ejecucion estandar del flujo diario:

- `scripts\run_daily_report.bat`
- `scripts\run_daily_report.ps1`

Comportamiento:

- sin argumentos, ejecutan `python -m portfolio_tracker_v2 daily-report --output-json output/reports/daily_report_latest.json`
- mantienen la salida humana de consola
- escriben JSON estructurado estable en `output/reports/daily_report_latest.json`
- propagan el exit code real del comando (`0`, `1` o `2`)
- si reciben argumentos, los reenvian a `daily-report`
- si el usuario pasa `--output-json` explicitamente, el wrapper no agrega el output JSON por defecto

Uso rapido:

- `scripts\run_daily_report.bat`
- `powershell -ExecutionPolicy Bypass -File .\scripts\run_daily_report.ps1`
- `scripts\run_daily_report.bat --skip-refresh`
- `scripts\run_daily_report.bat --account Main --history-dir output/custom-history`
- `scripts\run_daily_report.bat --output-json -`

### 4.8 B20 - Guia corta para Windows Task Scheduler

Recomendacion practica:

- usar `scripts\run_daily_report.bat` como wrapper principal para scheduler
- `Start in` recomendado: raiz del repo (`c:\imp\portfolio_tracker_v2`)
- JSON estable generado por default: `output\reports\daily_report_latest.json`

Configuracion minima en Task Scheduler:

- Program/script: `c:\imp\portfolio_tracker_v2\scripts\run_daily_report.bat`
- Start in: `c:\imp\portfolio_tracker_v2`

Alternativa PowerShell:

- Program/script: `powershell.exe`
- Add arguments: `-ExecutionPolicy Bypass -File c:\imp\portfolio_tracker_v2\scripts\run_daily_report.ps1`
- Start in: `c:\imp\portfolio_tracker_v2`

Ejemplos con flags:

- `scripts\run_daily_report.bat`
- `scripts\run_daily_report.bat --skip-refresh`
- `scripts\run_daily_report.bat --account Main --history-dir output/custom-history`

Exit codes:

- `0` = corrida OK sin alertas
- `1` = corrida OK con alertas
- `2` = error operativo / controlado

### 4.9 B21 - Retencion manual de summary history

Comando:

- `python -m portfolio_tracker_v2 prune-summary-history`

Flags:

- `--history-dir <dir>`
- `--keep-last <N>`
- `--dry-run`

Comportamiento:

- considera solo snapshots con patron `summary_YYYY-MM-DDTHH-MM-SSZ.json`
- conserva los `N` mas recientes y elimina los mas antiguos
- con `--dry-run` solo reporta que borraria
- ignora archivos que no coinciden con el patron

### 4.10 B22 - Validacion minima de daily-report JSON

Comando:

- `python -m portfolio_tracker_v2 validate-daily-report-json <path>`

Valida el contrato minimo del JSON emitido por `daily-report`:

- `report_type = "daily-report"`
- `report_schema_version = 1`
- presencia de `run_timestamp`, `summary_result` y `final_exit_code`

Ejemplo:

- `python -m portfolio_tracker_v2 validate-daily-report-json output/reports/daily_report_latest.json`

### 4.11 B24 - Resumen humano del latest daily-report

Comando:

- `python -m portfolio_tracker_v2 show-latest-daily-report`

Flags:

- `--path <file>` con default `output/reports/daily_report_latest.json`

Muestra un resumen humano corto del ultimo daily-report valido:

- `run_timestamp`
- `final_exit_code`
- estado/cantidad de alertas
- `created_snapshot_path` y `previous_snapshot_path`
- metricas clave de `summary_result`

Ejemplo:

- `python -m portfolio_tracker_v2 show-latest-daily-report`

### 4.12 B26 - Registro minimo de transacciones reales

Comando recomendado para operacion manual:

- `python -m portfolio_tracker_v2 add-transaction --date 2026-03-20 --account Main --symbol BTC --side buy --qty 1 --price 100 --fee 5`

Campos soportados:

- `--date`
- `--account`
- `--symbol`
- `--side buy|sell`
- `--qty`
- `--price`
- `--fee`
- `--notes`

La fuente de verdad sigue siendo la tabla de `transactions`; `positions`, `summary`, `pnl` y `daily-report` se recalculan desde esa base en vez de editar posiciones manualmente.

### 4.13 B27 - Auditoria rapida del ledger de transacciones

Comando:

- `python -m portfolio_tracker_v2 list-transactions`

Flags:

- `--account <name>`
- `--symbol <ticker>`
- `--limit <N>`
- `--from-date YYYY-MM-DD`
- `--to-date YYYY-MM-DD`

Ejemplos:

- `python -m portfolio_tracker_v2 list-transactions`
- `python -m portfolio_tracker_v2 list-transactions --account Main --limit 50`
- `python -m portfolio_tracker_v2 list-transactions --symbol BTC --from-date 2026-03-01 --to-date 2026-03-31`

## 5. Comandos operativos vigentes

- `python -m portfolio_tracker_v2 refresh-prices`
- `python -m portfolio_tracker_v2 refresh-prices --verbose`
- `python -m portfolio_tracker_v2 add-transaction --date <YYYY-MM-DD> --account <name> --symbol <symbol> --side <buy|sell> --qty <qty> --price <price> [--fee <fee>]`
- `python -m portfolio_tracker_v2 list-transactions [--account <name>] [--symbol <ticker>] [--limit <N>] [--from-date <YYYY-MM-DD>] [--to-date <YYYY-MM-DD>]`
- `python -m portfolio_tracker_v2 import-transactions-csv <csv_path>`
- `python -m portfolio_tracker_v2 positions`
- `python -m portfolio_tracker_v2 summary`
- `python -m portfolio_tracker_v2 summary --export-json <path>`
- `python -m portfolio_tracker_v2 summary --export-json-history <dir>`
- `python -m portfolio_tracker_v2 compare-summary-snapshots old.json new.json`
- `python -m portfolio_tracker_v2 alert-summary-snapshots old.json new.json`
- `python -m portfolio_tracker_v2 daily-report`
- `python -m portfolio_tracker_v2 daily-report --account <name>`
- `python -m portfolio_tracker_v2 daily-report --refresh-verbose`
- `python -m portfolio_tracker_v2 daily-report --history-dir <dir>`
- `python -m portfolio_tracker_v2 daily-report --skip-refresh`
- `python -m portfolio_tracker_v2 daily-report --output-json <path>`
- `python -m portfolio_tracker_v2 daily-report --output-json -`
- `python -m portfolio_tracker_v2 daily-report --output-json - > output/daily_report.json`
- `scripts\run_daily_report.bat`
- `powershell -ExecutionPolicy Bypass -File .\scripts\run_daily_report.ps1`

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
- daily report consolidado con contrato de salida y exit codes

## 8. Proximos pasos sugeridos

Con la base actual, los siguientes pasos naturales son:

1. Snapshot de `positions` para trazabilidad de composicion y cambios.
2. Mejoras visuales/reporting (presentacion y legibilidad de resultados).
3. Automatizacion futura (si se decide) para ejecucion periodica de refresh + snapshot + alertas.

---

Documento canonico de estado de PortfolioTracker v2 actualizado hasta B14.








## Anexo - delete-transaction (B28)

Comando:

- `python -m portfolio_tracker_v2 delete-transaction <id>`

Reglas operativas:

- borra una `SELL` y limpia primero sus filas relacionadas en `lot_matches`
- borra una `BUY` o `MIGRATION_BUY` solo si no fue usada en matching FIFO
- rechaza con `ERROR` si el `id` no existe o si el borrado no es seguro por consistencia

Salida y exit code:

- `OK: ...` y exit code `0` en borrado exitoso
- `ERROR: ...` y exit code `2` en error operativo / id inexistente / borrado no permitido

Ejemplo:

- `python -m portfolio_tracker_v2 delete-transaction 42`

## Anexo - import-transactions-csv (B29)

Comando:

- `python -m portfolio_tracker_v2 import-transactions-csv <csv_path>`

Formato CSV soportado en esta v1:

- una sola variante explicita con columnas obligatorias: `trade_date,account,symbol,side,quantity,unit_price`
- columnas opcionales soportadas: `fee,notes`
- `trade_date` debe usar `YYYY-MM-DD`
- `side` soporta solo `BUY` y `SELL`

Politica operativa:

- fila invalida por datos o reglas de transaccion: se rechaza, se continua y se resume al final
- error operativo serio (`archivo inexistente`, columnas obligatorias faltantes, error de lectura, error inesperado de DB/servicio): `ERROR` + exit code `2`
- las filas validas se registran como transacciones normales via `TransactionService`; `SELL` reutiliza el matching FIFO existente

Ejemplo corto:

```csv
trade_date,account,symbol,side,quantity,unit_price,fee,notes
2026-03-20,Main,BTC,BUY,1,100,5,first buy
2026-03-21,Main,BTC,SELL,0.25,150,0,partial sell
```

Uso:

- `python -m portfolio_tracker_v2 import-transactions-csv .\transactions.csv`

Salida esperada:

- archivo leido
- filas procesadas
- importadas OK
- rechazadas
- detalle resumido por fila rechazada

