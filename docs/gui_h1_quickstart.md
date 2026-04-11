# GUI H1 Quickstart (local)

Alcance actual: **H1 solamente** (esqueleto Flask + contexto DB activo + backup manual).

## Dependencias
Instalar dependencias del proyecto (incluye `Flask`):

```powershell
pip install -r requirements.txt
```

## Arranque GUI
Ejecutar desde `C:\imp`:

```powershell
python -m portfolio_tracker_v2.gui
```

Abrir en navegador:

- `http://127.0.0.1:5000/`

## Flujo minimo H1
1. Entrar a `Change DB`.
2. Definir `DB path` y seleccionar modo `TEST` o `PROD` (explícito).
3. Verificar en header global la ruta completa + badge de modo.
4. Ir a `Manual Backup` y crear copia timestamped cuando se necesite.

## Convencion de backups
- Directorio por defecto: `portfolio_tracker_v2/output/gui_backups/`
- Nombre: `<db_name>_backup_YYYYMMDD_HHMMSS.db`
- Override opcional del directorio: variable `PORTFOLIO_GUI_BACKUP_DIR`

## Guardrails de H1
- Sin DB activa configurada: se muestra `NO DB` y backup no ejecuta.
- Sin fallback silencioso a DB por defecto.
- Sin restore desde GUI en H1.
- Sin operaciones de negocio en H1 (`init-db`, imports, add/list/report todavía no expuestos aquí).
