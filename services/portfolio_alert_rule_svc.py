"""
B63A — Portfolio-aware alert rules service.

Reads:  signal_alerts (OPEN macro signals), portfolio summary, positions.
Writes: signal_alerts (source='portfolio_rule') via SignalService.

Does NOT modify: transactions, lot_matches, cash_ledger, assets (write),
                 price_cache, positions, PnL calculations, cash balance.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal

from portfolio_tracker_v2.core.asset_resolver import AssetResolver
from portfolio_tracker_v2.core.database import Database
from portfolio_tracker_v2.services.pnl_svc import PnLService
from portfolio_tracker_v2.services.signal_svc import SignalService

# Only signals from these sources are used as macro inputs.
# portfolio_rule is excluded to prevent recursive evaluation.
_MACRO_SOURCES = frozenset({"tradingview_macro", "manual"})

# Event type sets used for rule trigger matching
_HARD_RISK_OFF = frozenset({"hard_risk_off_activated", "stress", "risk_off"})
_RISK_OFF_ALL = frozenset({"hard_risk_off_activated", "confirmed_downgrade", "risk_off", "stress"})
_STRESS_EVENTS = frozenset({"hard_risk_off_activated", "stress", "risk_off"})
_DOWNGRADE_EVENTS = frozenset({"confirmed_downgrade", "hard_risk_off_activated", "stress", "risk_off"})
_ANTI_BUY_EVENTS = frozenset({"stress", "hard_risk_off_activated"})

_SOURCE = "portfolio_rule"
_TOP_LOSERS_CAP = 3

# B63D — Take-profit review thresholds.
# _TP_MIN_GAIN_USD suppresses noise from tiny positions (e.g. +30% on a $10 lot).
_TP_GAIN_PCT = 30.0
_TP_HIGH_PCT = 50.0
_TP_MIN_GAIN_USD = Decimal("100")


class PortfolioAlertRuleService:
    """Evaluates B63A portfolio-aware alert rules against open macro signals."""

    def __init__(self, db: Database, resolver: AssetResolver) -> None:
        self._sig_svc = SignalService(db)
        self._pnl_svc = PnLService(db, resolver)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def evaluate_portfolio_alerts(self) -> dict:
        """
        Run all B63A rules. Returns {'created': int, 'skipped': int}.

        Safe to call repeatedly — dedupe prevents duplicate OPEN alerts.
        Reads portfolio summary + positions + OPEN macro signals.
        Writes only to signal_alerts (source='portfolio_rule').
        """
        created = 0
        skipped = 0

        # ── Read inputs (read-only paths) ─────────────────────────────────
        macro_signals = self._open_macro_signals()
        open_events: frozenset[str] = frozenset(
            s["event_type"].lower() for s in macro_signals
        )

        # Load existing OPEN portfolio_rule alerts once for dedupe.
        existing_open = self._sig_svc.list_signals(status="OPEN", source=_SOURCE)
        existing_keys: set[tuple] = {
            (r["event_type"], r.get("symbol") or None, r.get("asset_class") or None)
            for r in existing_open
        }

        summ = self._pnl_svc.summary()
        total_equity: Decimal = summ["total_equity"]
        breakdown: dict = summ["asset_class_breakdown"]
        cash: Decimal = summ["cash_balance"]
        positions = self._pnl_svc.positions()

        # ── Local helpers ─────────────────────────────────────────────────
        def _pct(value) -> float:
            if total_equity <= 0:
                return 0.0
            return float(Decimal(str(value)) / total_equity * 100)

        def _emit(
            event_type: str,
            severity: str,
            message: str,
            symbol,
            asset_class,
            payload: dict,
        ) -> None:
            nonlocal created, skipped
            key = (event_type, symbol or None, asset_class or None)
            if key in existing_keys:
                skipped += 1
                return
            self._sig_svc.add_signal(
                event_time=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
                source=_SOURCE,
                event_type=event_type,
                severity=severity,
                message=message,
                symbol=symbol,
                asset_class=asset_class,
                payload_json=json.dumps(payload, default=str),
            )
            created += 1

        # ── Rule 5 — Asset class concentration (no macro signal required) ─
        if total_equity > 0:
            for cls, val in breakdown.items():
                cls_pct = _pct(val)
                if cls_pct > 35.0:
                    _emit(
                        event_type="asset_class_concentration_high",
                        severity="MEDIUM",
                        message=(
                            f"{cls} is {cls_pct:.1f}% of portfolio. "
                            "Review concentration risk."
                        ),
                        symbol=None,
                        asset_class=cls,
                        payload={
                            "rule": 5,
                            "asset_class": cls,
                            "class_pct": round(cls_pct, 2),
                            "class_usd": str(val),
                            "total_equity": str(total_equity),
                        },
                    )

        # ── Rule 6 (B63D) — Take-profit review (no macro signal required) ──
        for p in positions:
            if p.get("valuation_method") != "market_live":
                continue
            if p.get("valuation_status") != "usable":
                continue
            upct = p.get("unrealized_pct")
            if upct is None:
                continue
            gain_f = float(upct)
            if gain_f < _TP_GAIN_PCT:
                continue
            approved = p.get("approved_value") or Decimal("0")
            cost = p.get("cost_basis") or Decimal("0")
            if cost <= 0 or (approved - cost) < _TP_MIN_GAIN_USD:
                continue
            sym = p["symbol"]
            sev = "HIGH" if gain_f >= _TP_HIGH_PCT else "MEDIUM"
            asset_cls = self._pnl_svc._classify_asset_class(
                p.get("asset_type", ""), sym, p["valuation_method"]
            )
            _emit(
                event_type="take_profit_review_gain_threshold",
                severity=sev,
                message=(
                    f"{sym} is up {gain_f:.1f}%. "
                    "Review partial profit taking, trailing stop, or hold thesis."
                ),
                symbol=sym,
                asset_class=asset_cls,
                payload={
                    "rule": 6,
                    "symbol": sym,
                    "unrealized_pct": round(gain_f, 2),
                    "unrealized_gain_usd": str(approved - cost),
                    "approved_value": str(approved),
                    "cost_basis": str(cost),
                },
            )

        # ── Guard: Rules 1–4 require an active macro signal ───────────────
        active_risk_off = open_events & _RISK_OFF_ALL
        active_oversold = open_events & frozenset({"oversold"})

        if not active_risk_off and not active_oversold:
            return {"created": created, "skipped": skipped}

        if total_equity <= 0:
            return {"created": created, "skipped": skipped}

        crypto_pct = _pct(breakdown.get("Crypto", Decimal("0")))
        cash_pct = _pct(cash)

        # ── Rule 1 — Crypto exposure high under risk-off ──────────────────
        if active_risk_off and crypto_pct > 25.0:
            sev = "HIGH" if (open_events & _HARD_RISK_OFF) else "MEDIUM"
            _emit(
                event_type="crypto_exposure_high_under_risk_off",
                severity=sev,
                message=(
                    f"Macro risk deteriorated while Crypto exposure is {crypto_pct:.1f}%. "
                    "Review BTC/ETH/SOL exposure."
                ),
                symbol=None,
                asset_class="Crypto",
                payload={
                    "rule": 1,
                    "crypto_pct": round(crypto_pct, 2),
                    "total_equity": str(total_equity),
                    "triggering_events": sorted(active_risk_off),
                },
            )

        # ── Rule 2 — Low cash under stress / risk-off ─────────────────────
        active_stress = open_events & _STRESS_EVENTS
        if active_stress and cash_pct < 5.0:
            hard = open_events & frozenset({"hard_risk_off_activated", "stress"})
            sev = "HIGH" if hard else "MEDIUM"
            _emit(
                event_type="low_cash_under_stress",
                severity=sev,
                message=(
                    f"Cash is only {cash_pct:.1f}% while macro risk is elevated. "
                    "Review dry powder / avoid forced buys."
                ),
                symbol=None,
                asset_class="Cash",
                payload={
                    "rule": 2,
                    "cash_pct": round(cash_pct, 2),
                    "cash_usd": str(cash),
                    "total_equity": str(total_equity),
                    "triggering_events": sorted(active_stress),
                },
            )

        # ── Rule 3 — Large unrealized loss under macro downgrade ──────────
        active_downgrade = open_events & _DOWNGRADE_EVENTS
        if active_downgrade:
            losers = [
                p for p in positions
                if p.get("unrealized_pct") is not None and p["unrealized_pct"] <= -20
            ]
            losers.sort(key=lambda p: p["unrealized_pct"])  # most negative first
            for p in losers[:_TOP_LOSERS_CAP]:
                loss_pct = float(p["unrealized_pct"])
                sev = "HIGH" if loss_pct <= -30 else "MEDIUM"
                sym = p["symbol"]
                _emit(
                    event_type="large_unrealized_loss_under_macro_downgrade",
                    severity=sev,
                    message=(
                        f"{sym} is down {abs(loss_pct):.1f}% while macro risk deteriorated. "
                        "Review thesis / risk plan."
                    ),
                    symbol=sym,
                    asset_class=None,
                    payload={
                        "rule": 3,
                        "symbol": sym,
                        "unrealized_pct": round(loss_pct, 2),
                        "approved_value": str(p.get("approved_value")),
                        "cost_basis": str(p.get("cost_basis")),
                        "triggering_events": sorted(active_downgrade),
                    },
                )

        # ── Rule 4 — Potential buy review on oversold (no hard risk-off) ──
        if active_oversold and not (open_events & _ANTI_BUY_EVENTS) and cash_pct >= 5.0:
            _emit(
                event_type="potential_buy_review_oversold_with_cash",
                severity="MEDIUM",
                message=(
                    f"Market is oversold and cash is available ({cash_pct:.1f}%). "
                    "Review watchlist. No automatic buy."
                ),
                symbol=None,
                asset_class="Portfolio",
                payload={
                    "rule": 4,
                    "cash_pct": round(cash_pct, 2),
                    "cash_usd": str(cash),
                    "triggering_events": ["oversold"],
                },
            )

        return {"created": created, "skipped": skipped}

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _open_macro_signals(self) -> list[dict]:
        """Return OPEN signals from macro sources only.

        Excludes source='portfolio_rule' to prevent recursive evaluation.
        """
        all_open = self._sig_svc.list_signals(status="OPEN")
        return [s for s in all_open if s.get("source") in _MACRO_SOURCES]
