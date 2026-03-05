from __future__ import annotations

from mm.types import Quote

from .types import PairInventoryState


def pending_reservations(
    active_orders: dict[str, Quote],
    *,
    up_token_id: str,
    dn_token_id: str,
) -> dict[str, float]:
    pending_buy_reserved_usdc = 0.0
    pending_buy_up = 0.0
    pending_buy_dn = 0.0
    pending_sell_up = 0.0
    pending_sell_dn = 0.0
    for quote in active_orders.values():
        if quote.side == "BUY":
            pending_buy_reserved_usdc += float(quote.size) * float(quote.price)
            if quote.token_id == up_token_id:
                pending_buy_up += float(quote.size)
            elif quote.token_id == dn_token_id:
                pending_buy_dn += float(quote.size)
        elif quote.side == "SELL":
            if quote.token_id == up_token_id:
                pending_sell_up += float(quote.size)
            elif quote.token_id == dn_token_id:
                pending_sell_dn += float(quote.size)
    return {
        "pending_buy_reserved_usdc": max(0.0, pending_buy_reserved_usdc),
        "pending_buy_up": max(0.0, pending_buy_up),
        "pending_buy_dn": max(0.0, pending_buy_dn),
        "pending_sell_up": max(0.0, pending_sell_up),
        "pending_sell_dn": max(0.0, pending_sell_dn),
    }


def build_pair_inventory(
    *,
    up_shares: float,
    dn_shares: float,
    total_usdc: float,
    available_usdc: float | None,
    active_orders: dict[str, Quote],
    fv_up: float,
    fv_dn: float,
    up_token_id: str,
    dn_token_id: str,
) -> PairInventoryState:
    pending = pending_reservations(
        active_orders,
        up_token_id=up_token_id,
        dn_token_id=dn_token_id,
    )
    paired_qty = min(max(0.0, up_shares), max(0.0, dn_shares))
    excess_up_qty = max(0.0, up_shares - paired_qty)
    excess_dn_qty = max(0.0, dn_shares - paired_qty)
    paired_value_usd = paired_qty * max(0.0, fv_up + fv_dn)
    excess_up_value_usd = excess_up_qty * max(0.0, fv_up)
    excess_dn_value_usd = excess_dn_qty * max(0.0, fv_dn)
    excess_value_usd = excess_up_value_usd + excess_dn_value_usd
    signed_excess_value_usd = excess_up_value_usd - excess_dn_value_usd
    total_inventory_value_usd = max(0.0, up_shares) * max(0.0, fv_up) + max(0.0, dn_shares) * max(0.0, fv_dn)
    wallet_total_usdc = max(0.0, float(total_usdc))
    if available_usdc is None:
        wallet_free_usdc = max(0.0, wallet_total_usdc - pending["pending_buy_reserved_usdc"])
    else:
        wallet_free_usdc = max(0.0, min(wallet_total_usdc, float(available_usdc)))
    wallet_reserved_usdc = max(0.0, wallet_total_usdc - wallet_free_usdc)
    return PairInventoryState(
        up_shares=max(0.0, float(up_shares)),
        dn_shares=max(0.0, float(dn_shares)),
        free_usdc=max(0.0, wallet_free_usdc),
        reserved_usdc=max(0.0, wallet_reserved_usdc),
        pending_buy_up=pending["pending_buy_up"],
        pending_buy_dn=pending["pending_buy_dn"],
        pending_sell_up=pending["pending_sell_up"],
        pending_sell_dn=pending["pending_sell_dn"],
        paired_qty=paired_qty,
        excess_up_qty=excess_up_qty,
        excess_dn_qty=excess_dn_qty,
        paired_value_usd=paired_value_usd,
        excess_up_value_usd=excess_up_value_usd,
        excess_dn_value_usd=excess_dn_value_usd,
        total_inventory_value_usd=total_inventory_value_usd,
        wallet_total_usdc=max(0.0, wallet_total_usdc),
        wallet_reserved_usdc=max(0.0, wallet_reserved_usdc),
        pending_buy_reserved_usdc=pending["pending_buy_reserved_usdc"],
        excess_value_usd=excess_value_usd,
        signed_excess_value_usd=signed_excess_value_usd,
    )
