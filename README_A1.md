# PortfolioTracker v2 - MVP Quickstart

Documento canonico de uso operativo del MVP.

## Que Es Este MVP
- Tracker de portafolio basado en transacciones (SQLite como source of truth).
- Importa snapshot legacy de posiciones abiertas como lotes seed (`MIGRATION_BUY` por fila).
- Permite registrar ledger manual `BUY` / `SELL`.
- Aplica matching FIFO persistente para `SELL` usando `lot_matches`.
- Expone realized PnL en `list-transactions` y auditoria de matches por transaccion.
- Incluye comandos de inspeccion y exportables CSV/JSON para automatizacion.
- `daily-report` operativo para corrida diaria con salida humana y JSON.

## Que NO Hace Todavia
- GUI.
- Reporteria fiscal avanzada.
- Multi-user / permisos.
- Wash sales.
- Estrategias HIFO/LIFO (solo FIFO).
- Automatizacion enterprise compleja.

## Quickstart Desde Cero
1. Inicializar DB:
```powershell
python -m portfolio_tracker_v2 init-db
```

2. Importar snapshot legacy (seed inicial de inventario abierto):
```powershell
python -m portfolio_tracker_v2 import-legacy-positions-csv .\portfoliototal.csv --seed-date 2026-03-20
```

3. Registrar BUY manual:
```powershell
python -m portfolio_tracker_v2 add-transaction --date 2026-03-21 --account Main --symbol BTC --side buy --qty 1 --price 100 --fee 0
```

4. Registrar SELL manual:
```powershell
python -m portfolio_tracker_v2 add-transaction --date 2026-03-22 --account Main --symbol BTC --side sell --qty 0.25 --price 120 --fee 0
```

5. Revisar ledger:
```powershell
python -m portfolio_tracker_v2 list-transactions --account Main --symbol BTC --side SELL --date-from 2026-03-01 --date-to 2026-03-31
```

6. Revisar lotes abiertos:
```powershell
python -m portfolio_tracker_v2 list-open-lots --account Main --symbol BTC
```

7. Auditar consumo FIFO de una venta:
```powershell
python -m portfolio_tracker_v2 inspect-lot-matches --sell-tx-id 42
```

8. Revisar posiciones:
```powershell
python -m portfolio_tracker_v2 positions --account Main
```

9. Revisar summary:
```powershell
python -m portfolio_tracker_v2 summary --account Main
```

10. Correr daily-report:
```powershell
python -m portfolio_tracker_v2 daily-report --account Main
```

## Exportables Actuales
- `list-transactions` -> CSV (`--output-csv <path|->`).
- `list-open-lots` -> CSV (`--output-csv <path|->`).
- `inspect-lot-matches` -> CSV (`--output-csv <path|->`).
- `positions` -> CSV (`--output-csv <path|->`).
- `summary` -> JSON estructurado (`--output-json <path|->`).
- `daily-report` -> JSON estructurado (`--output-json <path|->`).

## Flujo Operativo Recomendado
1. Seed inicial una sola vez con `import-legacy-positions-csv`.
2. Registrar `BUY`/`SELL` en el ledger (`add-transaction`).
3. Validar consistencia en `list-transactions`, `list-open-lots`, `inspect-lot-matches`.
4. Revisar estado agregado en `positions`, `summary` y `daily-report`.
5. Exportar CSV/JSON cuando se necesite para scripts o snapshots.

## Guardrails Importantes
- No consolidar lotes seed: cada fila legacy debe permanecer como lote independiente.
- FIFO se aplica por lote (`lot_matches`) para cada `SELL`.
- Oversell esta bloqueado.
- `delete-transaction` protege `BUY` consumidas por integridad.
- Usa una DB de prueba para experimentar (`PORTFOLIO_DB_PATH` apuntando a otro archivo).
- No meter archivos de prueba ni artefactos locales en commits.

## Comandos Clave (Flags Utiles)
- `list-transactions`: `--account`, `--symbol`, `--side`, `--tx-id`, `--date-from/--date-to`, `--output-csv`.
- `list-open-lots`: `--account`, `--symbol`, `--output-csv`.
- `inspect-lot-matches`: `--sell-tx-id` o `--buy-tx-id` (exactamente uno), `--output-csv`.
- `positions`: `--account`, `--symbol`, `--output-csv`.
- `summary`: `--account`, `--output-json`, `--export-json`, `--export-json-history`.
- `daily-report`: `--account`, `--skip-refresh`, `--history-dir`, `--output-json`, `--output-json-history-dir`.

## Smoke Test MVP (B45)
Precondiciones minimas:
- Ejecutar desde `C:\imp` para usar `python -m portfolio_tracker_v2 ...`.
- Usar DB temporal de prueba (no productiva) via `PORTFOLIO_DB_PATH`.
- Contar con snapshot legacy valido (`C:\imp\portfoliototal.csv` en esta validacion).

Comandos ejecutados (flujo feliz end-to-end):
```powershell
$env:PORTFOLIO_DB_PATH='C:\imp\portfolio_tracker_v2\tmp_smoke_b45.db'
if (Test-Path $env:PORTFOLIO_DB_PATH) { Remove-Item $env:PORTFOLIO_DB_PATH -Force }

python -m portfolio_tracker_v2 init-db
python -m portfolio_tracker_v2 import-legacy-positions-csv C:\imp\portfoliototal.csv --seed-date 2026-04-01

python -m portfolio_tracker_v2 add-transaction --date 2026-04-02 --account Main --symbol BTC --side buy --qty 1 --price 100 --fee 0
python -m portfolio_tracker_v2 add-transaction --date 2026-04-03 --account Main --symbol BTC --side sell --qty 0.25 --price 120 --fee 0

python -m portfolio_tracker_v2 list-transactions --account Main --symbol BTC --date-from 2026-04-01 --date-to 2026-04-30
python -m portfolio_tracker_v2 list-open-lots --account Main --symbol BTC
python -m portfolio_tracker_v2 inspect-lot-matches --sell-tx-id 55
python -m portfolio_tracker_v2 positions --account Main
python -m portfolio_tracker_v2 summary --account Main
python -m portfolio_tracker_v2 daily-report --account Main --skip-refresh --output-json C:\imp\smoke_b45\daily_report_main.json

python -m portfolio_tracker_v2 inspect-lot-matches --sell-tx-id 55 --output-csv C:\imp\smoke_b45\inspect_lot_matches.csv
python -m portfolio_tracker_v2 summary --account Main --output-json C:\imp\smoke_b45\summary_main.json
```

Que se valida explicitamente y resultado esperado/resumido:
1. `init-db` crea schema y assets base: OK.
2. Import legacy funciona (`Rows processed: 53`, `Seeded OK: 53`, `Rejected: 0`): OK.
3. BUY manual entra (`id=54`): OK.
4. SELL manual entra (`id=55`): OK.
5. FIFO persiste matches en `lot_matches`: OK.
6. Realized PnL aparece en ledger (`Realized PnL = 5.00` para sell `id=55`): OK.
7. Open lots reflejan remanente correcto (`Remaining Qty = 0.75`): OK.
8. `inspect-lot-matches` muestra BUY consumido (`BuyTxID=54` para `SellTxID=55`): OK.
9. `positions` funciona para `Main/BTC`: OK.
10. `summary` funciona y refleja `Total cost basis: 75.00`, `Total realized PnL: 5.00`: OK.
11. `daily-report` funciona (corrida con `--skip-refresh`): OK.
12. Exports CSV/JSON generan salida no vacia:
    - `C:\imp\smoke_b45\inspect_lot_matches.csv` (212 bytes)
    - `C:\imp\smoke_b45\summary_main.json` (759 bytes)
    - `C:\imp\smoke_b45\daily_report_main.json` (3978 bytes)

Nota operativa:
- Para experimentar, usar siempre DB temporal/de prueba (`PORTFOLIO_DB_PATH`) para no contaminar la DB real.

## Release Baseline (B46)
Estado de release: esta version queda marcada como MVP interno usable (`v0.1.0-mvp`).

Alcance cubierto en esta linea base:
- seed import legacy
- ledger manual BUY/SELL
- FIFO persistente con `lot_matches`
- visibilidad de realized PnL
- open lots
- auditoria de lot match
- exports clave CSV/JSON
- `daily-report` usable

Politica de freeze:
- desde `v0.1.0-mvp`, solo fixes de bugs reales y fricciones reales de uso operativo
- no agregar features nuevas antes de uso operativo real
