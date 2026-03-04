from __future__ import annotations

from dataclasses import asdict
from typing import Any

from .config import MMConfigV2
from .types import EngineState, PairMarketSnapshot


def _serialize_quote(raw: Any, *, suppressed_reason: str | None = None) -> dict[str, Any] | None:
    if raw is None:
        if not suppressed_reason:
            return None
        return {
            "active": False,
            "suppressed_reason": suppressed_reason,
        }
    payload = asdict(raw)
    payload["active"] = True
    payload["suppressed_reason"] = suppressed_reason
    return payload


def serialize_engine_state(
    state: EngineState,
    *,
    config: MMConfigV2,
    app_version: str = "",
    app_git_hash: str = "",
) -> dict[str, Any]:
    market = asdict(state.market) if state.market else None
    inventory = asdict(state.inventory)
    pair_inventory = {
        "paired_qty": round(state.inventory.paired_qty, 4),
        "excess_up_qty": round(state.inventory.excess_up_qty, 4),
        "excess_dn_qty": round(state.inventory.excess_dn_qty, 4),
        "paired_value_usd": round(state.inventory.paired_value_usd, 4),
        "excess_up_value_usd": round(state.inventory.excess_up_value_usd, 4),
        "excess_dn_value_usd": round(state.inventory.excess_dn_value_usd, 4),
        "excess_value_usd": round(state.inventory.excess_value_usd, 4),
        "signed_excess_value_usd": round(state.inventory.signed_excess_value_usd, 4),
        "inventory_pressure_abs": round(state.inventory.inventory_pressure_abs, 6),
        "inventory_pressure_signed": round(state.inventory.inventory_pressure_signed, 6),
        "sellable_up_shares": round(state.inventory.sellable_up_shares, 4),
        "sellable_dn_shares": round(state.inventory.sellable_dn_shares, 4),
    }
    quotes = {
        "up_bid": _serialize_quote(state.current_quotes.up_bid, suppressed_reason=state.current_quotes.suppressed_reasons.get("up_bid")),
        "up_ask": _serialize_quote(state.current_quotes.up_ask, suppressed_reason=state.current_quotes.suppressed_reasons.get("up_ask")),
        "dn_bid": _serialize_quote(state.current_quotes.dn_bid, suppressed_reason=state.current_quotes.suppressed_reasons.get("dn_bid")),
        "dn_ask": _serialize_quote(state.current_quotes.dn_ask, suppressed_reason=state.current_quotes.suppressed_reasons.get("dn_ask")),
    }
    return {
        "app_version": app_version,
        "app_git_hash": app_git_hash,
        "lifecycle": state.lifecycle,
        "market": market,
        "valuation": {
            "fv_up": round(state.market.fv_up, 6) if state.market else 0.0,
            "fv_dn": round(state.market.fv_dn, 6) if state.market else 0.0,
            "confidence": round(state.market.fv_confidence, 4) if state.market else 0.0,
            "source": state.market.valuation_source if state.market else "",
            "regime": state.market.valuation_regime if state.market else "",
            "divergence_up": round(state.market.divergence_up, 6) if state.market else 0.0,
            "divergence_dn": round(state.market.divergence_dn, 6) if state.market else 0.0,
        },
        "inventory": inventory,
        "pair_inventory": pair_inventory,
        "quotes": quotes,
        "execution": asdict(state.execution),
        "risk": asdict(state.risk),
        "health": asdict(state.health),
        "analytics": asdict(state.analytics),
        "quote_balance_state": state.current_quotes.quote_balance_state,
        "alerts": list(state.alerts),
        "config": config.to_dict(),
    }
