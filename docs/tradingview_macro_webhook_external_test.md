# TradingView Macro Webhook — External Smoke Test / Runbook

## Purpose (B62D)

This document is an operator runbook for verifying the TradingView macro webhook
end-to-end as if TradingView itself were firing the alert. It covers local setup,
tunnel exposure, PowerShell test commands, TradingView alert message templates,
verification steps, and troubleshooting. No backend logic is changed by this task.

---

## Required environment variables

Set these before starting the Flask app.

| Variable | Purpose |
|---|---|
| `PORTFOLIO_WEBHOOK_TOKEN` | Secret token TradingView must send. Server returns 503 if unset. |
| `PORTFOLIO_DB_PATH` | Absolute path to the SQLite database file. Falls back to the active GUI DB if unset; returns 503 if neither is available. |

PowerShell example:

```powershell
$env:PORTFOLIO_WEBHOOK_TOKEN = "replace-with-your-secret"
$env:PORTFOLIO_DB_PATH       = "C:\path\to\your\portfolio.db"
```

> **Security warning**: treat `PORTFOLIO_WEBHOOK_TOKEN` as a password. Never paste
> it into screenshots, chat messages, issue trackers, or commit it to source control.

---

## Start the local Flask server

```powershell
cd C:\imp\portfolio_tracker_v2
python -m flask --app portfolio_tracker_v2.gui.app:create_app run --port 5000
```

Confirm it is up:

```powershell
Invoke-RestMethod -Uri "http://localhost:5000/" -Method GET
```

---

## Expose the local endpoint via a temporary public HTTPS tunnel

TradingView requires an HTTPS URL. Use a temporary tunnel tool (e.g. ngrok, Cloudflare
Tunnel, or localtunnel) to forward public HTTPS traffic to `localhost:5000`.

The resulting webhook URL takes the form:

```
https://YOUR_TEMP_PUBLIC_URL/webhooks/tradingview/macro?token=YOUR_TOKEN
```

> **Security warning**: the tunnel makes your local server reachable from the internet
> for the duration of the session. Never share the tunnel URL or the token. Shut the
> tunnel down immediately after testing.

---

## PowerShell test examples (localhost)

Run these while the Flask server is running. Replace `YOUR_TOKEN` with your actual
token value.

### 1. `text/plain` payload — query-param token

```powershell
Invoke-RestMethod `
  -Uri "http://localhost:5000/webhooks/tradingview/macro?token=YOUR_TOKEN" `
  -Method POST `
  -ContentType "text/plain" `
  -Body "event_type=hard_risk_off_activated`nregime=RISK-OFF`nscore=-3.25`nmessage=Manual smoke test"
```

### 2. `application/json` payload — header token

```powershell
$payload = @{
    event_type = "confirmed_downgrade"
    regime     = "RISK-OFF"
    score      = -2.75
    message    = "Manual smoke test — JSON path"
} | ConvertTo-Json

Invoke-RestMethod `
  -Uri "http://localhost:5000/webhooks/tradingview/macro" `
  -Method POST `
  -ContentType "application/json" `
  -Headers @{ "X-Webhook-Token" = "YOUR_TOKEN" } `
  -Body $payload
```

### 3. Invalid token test (expect 403)

```powershell
try {
    Invoke-RestMethod `
      -Uri "http://localhost:5000/webhooks/tradingview/macro?token=wrong-token" `
      -Method POST `
      -ContentType "text/plain" `
      -Body "event_type=caution"
} catch {
    $_.Exception.Response.StatusCode.value__   # should print 403
}
```

Expected console output for a successful (201) call:

```
ok signal_id
-- ---------
True        3
```

---

## TradingView alert message templates

Paste one of these into the **Message** field of a TradingView alert. TradingView
sends the message body as `text/plain` in the POST request.

The webhook URL to set in TradingView:

```
https://YOUR_TEMP_PUBLIC_URL/webhooks/tradingview/macro?token=YOUR_TOKEN
```

### `hard_risk_off_activated` (severity: HIGH)

```
event_type=hard_risk_off_activated
regime=RISK-OFF
score=-3.25
message=TradingView macro: hard risk-off activated
```

### `hard_stress_activated` (severity: INFO — not in severity map, logged as INFO)

```
event_type=hard_stress_activated
regime=RISK-OFF
score=-4.0
message=TradingView macro: hard stress activated
```

### `macro_oversold` (severity: INFO — not in severity map, logged as INFO)

```
event_type=macro_oversold
regime=NEUTRAL
score=-1.5
message=TradingView macro: oversold condition detected
```

### `macro_stretched` (severity: INFO — not in severity map, logged as INFO)

```
event_type=macro_stretched
regime=NEUTRAL
score=1.8
message=TradingView macro: stretched condition detected
```

> Note: only `event_type` is required. All other fields are optional. Unknown
> `event_type` values are stored with severity `INFO`.

---

## Verification steps

After each successful (201) call, confirm the following:

### 1. `/signals` page shows the signal as OPEN

Open `http://localhost:5000/signals` in a browser. The new `event_type` should
appear in the table with status `OPEN`.

PowerShell check:

```powershell
$r = Invoke-WebRequest -Uri "http://localhost:5000/signals"
$r.Content -match "hard_risk_off_activated"   # should return True
```

### 2. Dashboard shows Recent Signal

Open `http://localhost:5000/`. The recent signals widget should list the latest
signal.

### 3. Database has new `signal_alerts` row

```powershell
# Adjust the path to your actual DB file
$db = "C:\path\to\your\portfolio.db"
& sqlite3 $db "SELECT id, event_type, severity, status FROM signal_alerts ORDER BY id DESC LIMIT 5;"
```

Expected output (example):

```
3|hard_risk_off_activated|HIGH|OPEN
```

### 4. Cash, equity, and positions are unchanged

The webhook writes only to `signal_alerts`. No transactions, cash movements, or
position changes should occur. Verify with:

```powershell
& sqlite3 $db "SELECT COUNT(*) FROM transactions;"   # must not increase
& sqlite3 $db "SELECT COUNT(*) FROM cash_movements;" # must not increase
```

Or compare the `/` dashboard summary values before and after firing the webhook.

### 5. No backup file created

```powershell
$backupDir = Join-Path (Split-Path $db) "backups"
if (Test-Path $backupDir) { Get-ChildItem $backupDir } else { "No backup directory — OK" }
```

No new files should appear in the backup directory after a webhook call.

---

## Troubleshooting

| Response | Cause | Fix |
|---|---|---|
| `201 {"ok": true, "signal_id": N}` | Accepted. | Nothing to fix. |
| `400 {"ok": false, "error": "event_type is required"}` | Payload is missing `event_type` or it is empty. | Check the alert message template; ensure `event_type=...` is present. |
| `403 {"ok": false, "error": "Invalid token"}` | Token mismatch. | Verify `PORTFOLIO_WEBHOOK_TOKEN` matches the `?token=` value exactly (case-sensitive, no extra spaces). |
| `503 {"ok": false, "error": "Webhook token not configured on server"}` | `PORTFOLIO_WEBHOOK_TOKEN` env var is unset on the server. | Set the env var and restart Flask. |
| `503 {"ok": false, "error": "No active database..."}` | `PORTFOLIO_DB_PATH` is unset and no GUI DB is active. | Set `PORTFOLIO_DB_PATH` to the absolute path of the `.db` file. |
| Connection refused / no response | Flask is not running, or the tunnel is down. | Restart Flask and/or the tunnel. |

---

## Important: signal logging only

**This webhook does not execute trades.** It inserts one row into `signal_alerts`
and returns. It does not modify positions, cash balances, transaction history, or
any other financial data, and it does not create database backups. All risk
management and trade decisions remain the operator's responsibility.
