"""Polymarket fee calculator for crypto markets (fee-curve model).

Crypto markets use a fee curve: fee = notional * (base_fee_bps/10000) * (p*(1-p))^exponent.
BUY taker: fee deducted in SHARES. SELL taker: fee deducted in USDC.
Maker orders: 0 fee.

base_fee_bps should be fetched dynamically via /fee-rate?token_id=... endpoint.
Default for crypto 5m/15m: 1000 bps, exponent=2.
"""
from __future__ import annotations

CRYPTO_FEE_EXPONENT = 2
DEFAULT_BASE_FEE_BPS = 1000  # default for crypto markets


def fee_usdc(price: float, size: float, base_fee_bps: int = DEFAULT_BASE_FEE_BPS,
             exponent: int = CRYPTO_FEE_EXPONENT) -> float:
    """Compute fee in USDC terms using the fee curve.

    Args:
        price: order price (0-1)
        size: order size in shares
        base_fee_bps: from /fee-rate endpoint (default 1000 for crypto)
        exponent: curve exponent (2 for crypto)

    Returns:
        Fee in USDC, rounded to 4 decimal places, minimum 0.0001 if nonzero.
    """
    if price <= 0 or price >= 1.0 or size <= 0 or base_fee_bps <= 0:
        return 0.0
    notional = price * size
    curve = (price * (1.0 - price)) ** exponent
    fee = notional * (base_fee_bps / 10000.0) * curve
    fee = round(fee, 4)
    if 0 < fee < 0.0001:
        fee = 0.0001
    return fee


def taker_fee_usd(price: float, size: float, side: str,
                  base_fee_bps: int = DEFAULT_BASE_FEE_BPS) -> float:
    """Compute taker fee in USDC (same formula regardless of side; settlement differs)."""
    return fee_usdc(price, size, base_fee_bps)


def net_shares_after_buy_fee(gross_size: float, price: float = 0.5,
                             base_fee_bps: int = DEFAULT_BASE_FEE_BPS) -> float:
    """BUY taker: fee is paid in shares. Net shares = gross - fee_shares.

    Args:
        gross_size: shares in the order
        price: fill price (needed for fee curve calculation)
        base_fee_bps: from /fee-rate endpoint

    Returns:
        Net shares received after fee deduction.
    """
    if gross_size <= 0:
        return 0.0
    fee = fee_usdc(price, gross_size, base_fee_bps)
    if price <= 0:
        return gross_size
    fee_shares = fee / price
    return max(0.0, gross_size - fee_shares)


def net_usdc_after_sell_fee(gross_usdc: float, price: float, size: float,
                            base_fee_bps: int = DEFAULT_BASE_FEE_BPS) -> float:
    """SELL taker: fee deducted from USDC proceeds."""
    fee = fee_usdc(price, size, base_fee_bps)
    return max(0.0, gross_usdc - fee)


# Legacy alias kept for backward compatibility during transition
TAKER_FEE_PCT = 2.0  # DEPRECATED - do not use; use fee_usdc() with base_fee_bps
