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
        "wallet_total_usdc": round(state.inventory.wallet_total_usdc, 4),
        "wallet_reserved_usdc": round(state.inventory.wallet_reserved_usdc, 4),
        "pending_buy_reserved_usdc": round(state.inventory.pending_buy_reserved_usdc, 4),
        "target_pair_value_usd": round(state.inventory.target_pair_value_usd, 4),
        "pair_value_ratio": round(state.inventory.pair_value_ratio, 6),
        "pair_value_over_target_usd": round(state.inventory.pair_value_over_target_usd, 4),
        "pair_entry_cost": round(state.inventory.pair_entry_cost, 6),
        "pair_entry_pnl_per_share": round(state.inventory.pair_entry_pnl_per_share, 6),
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
            "midpoint_anchor_up": round(state.market.midpoint_anchor_up, 6) if state.market and state.market.midpoint_anchor_up is not None else 0.0,
            "midpoint_anchor_dn": round(state.market.midpoint_anchor_dn, 6) if state.market and state.market.midpoint_anchor_dn is not None else 0.0,
            "model_anchor_up": round(state.market.model_anchor_up, 6) if state.market and state.market.model_anchor_up is not None else 0.0,
            "model_anchor_dn": round(state.market.model_anchor_dn, 6) if state.market and state.market.model_anchor_dn is not None else 0.0,
            "anchor_divergence_up": round(state.market.anchor_divergence_up, 6) if state.market else 0.0,
            "anchor_divergence_dn": round(state.market.anchor_divergence_dn, 6) if state.market else 0.0,
            "quote_anchor_mode": state.market.quote_anchor_mode if state.market else "midpoint_first",
            "realized_vol_per_min": round(state.market.realized_vol_per_min, 6) if state.market else 0.0,
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
