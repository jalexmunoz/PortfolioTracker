# TradingView Macro Webhook Receiver

## Endpoint

```
POST /webhooks/tradingview/macro
```

## Required environment variables

| Variable | Purpose |
|---|---|
| `PORTFOLIO_WEBHOOK_TOKEN` | Secret token the sender must present. If unset the server returns 503. |
| `PORTFOLIO_DB_PATH` | Absolute path to the SQLite database file. If unset the server falls back to the active GUI database (`gui_state.json`); if neither is available it returns 503. |

## Authentication

Supply the token using **one** of the following — header takes priority:

| Method | Example |
|---|---|
| HTTP header | `X-Webhook-Token: <token>` |
| Query parameter | `?token=<token>` |

## Supported payload formats

### `application/json`

A JSON object. All keys except `event_type` are optional.

```json
{
  "event_type": "hard_risk_off_activated",
  "regime":     "RISK-OFF",
  "score":      -3.25,
  "message":    "Macro hard risk-off activated",
  "symbol":     "MACRO",
  "source":     "tradingview_macro"
}
```

### `text/plain`

Key-value pairs, either **newline-separated** or **comma-separated** (or mixed). Each pair uses `key=value` syntax.

```
event_type=hard_risk_off_activated
regime=RISK-OFF
score=-3.25
message=Macro hard risk-off activated
```

Comma-separated equivalent:

```
event_type=hard_risk_off_activated, regime=RISK-OFF, score=-3.25
```

### Required field

| Field | Description |
|---|---|
| `event_type` | String identifier for the signal (e.g. `hard_risk_off_activated`). Missing or empty → 400. |

### Optional fields

| Field | Default |
|---|---|
| `source` | `tradingview_macro` |
| `severity` | Derived from `event_type` via the severity map (see below); unknown types → `INFO` |
| `message` | `TradingView macro signal: <event_type>` |
| `regime` | `null` |
| `score` | `null` |
| `symbol` | `MACRO` |
| `asset_class` | `macro` |
| `event_time` / `time` | Current UTC timestamp at time of receipt |

### Severity map

| `event_type` | `severity` |
|---|---|
| `stress`, `no_trade_stress` | `CRITICAL` |
| `hard_risk_off_activated`, `confirmed_downgrade`, `risk_off` | `HIGH` |
| `fast_early_warning_downgrade`, `caution`, `oversold`, `stretched` | `MEDIUM` |
| `confirmed_upgrade`, `risk_on` | `LOW` |
| *(any other value)* | `INFO` |

## Expected responses

| Status | Body | Meaning |
|---|---|---|
| `201 Created` | `{"ok": true, "signal_id": N}` | Signal recorded successfully. |
| `400 Bad Request` | `{"ok": false, "error": "event_type is required"}` | Payload missing `event_type`. |
| `403 Forbidden` | `{"ok": false, "error": "Invalid token"}` | Token present but wrong. |
| `503 Service Unavailable` | `{"ok": false, "error": "..."}` | Token not configured on server, or no database available. |

## PowerShell local test examples

Replace `YOUR_TOKEN` and the URL with your actual values. Run while the Flask app is running locally.

### `text/plain` with query-param token

```powershell
Invoke-RestMethod `
  -Uri "http://localhost:5000/webhooks/tradingview/macro?token=YOUR_TOKEN" `
  -Method POST `
  -ContentType "text/plain" `
  -Body "event_type=hard_risk_off_activated`nregime=RISK-OFF`nscore=-3.25`nmessage=Test signal"
```

### `application/json` with header token

```powershell
$payload = @{
    event_type = "confirmed_downgrade"
    regime     = "RISK-OFF"
    score      = -2.75
    message    = "Confirmed regime downgrade"
} | ConvertTo-Json

Invoke-RestMethod `
  -Uri "http://localhost:5000/webhooks/tradingview/macro" `
  -Method POST `
  -ContentType "application/json" `
  -Headers @{ "X-Webhook-Token" = "YOUR_TOKEN" } `
  -Body $payload
```

Expected output for a successful call:

```
ok signal_id
-- ---------
True        1
```

## Notes

- **Token security**: `PORTFOLIO_WEBHOOK_TOKEN` must be kept secret. Do not commit it to source control or expose it in logs. Use a `.env` file or your system's secret manager.
- **Signal log only**: the webhook inserts one row into `signal_alerts`. It does not modify positions, cash balances, or any other financial data, and it does not create database backups.
