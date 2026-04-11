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
