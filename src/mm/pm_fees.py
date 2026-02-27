"""Polymarket fee calculator for crypto markets (fee-curve model).

Crypto markets use a fee curve: fee = notional * (base_fee_bps/10000) * (p*(1-p))^exponent.
BUY taker: fee deducted in SHARES. SELL taker: fee deducted in USDC.
Maker orders: 0 fee.

base_fee_bps should be fetched dynamically via /fee-rate?token_id=... endpoint.
Default for crypto 5m/15m: 1000 bps, exponent=2.
"""
from __future__ import annotations

import logging
import time

import httpx

CRYPTO_FEE_EXPONENT = 2
DEFAULT_BASE_FEE_BPS = 1000  # default for crypto markets

_FEE_RATE_CACHE_TTL_SECONDS = 20
_fee_rate_cache: dict[str, tuple[float, dict]] = {}
_log = logging.getLogger(__name__)


def fee_curve_weight(price: float, exponent: int = CRYPTO_FEE_EXPONENT) -> float:
    """Return curve weight (p*(1-p))^exponent for fee analysis."""
    if price <= 0 or price >= 1.0 or exponent < 0:
        return 0.0
    return (price * (1.0 - price)) ** exponent


async def fetch_fee_rate(
    token_id: str,
    clob_base_url: str = "https://clob.polymarket.com",
    *,
    force_refresh: bool = False,
) -> dict:
    """Fetch and cache fee-rate payload for token_id from CLOB endpoint."""
    now = time.time()
    cached = _fee_rate_cache.get(token_id)
    if (
        not force_refresh
        and cached
        and now - cached[0] < _FEE_RATE_CACHE_TTL_SECONDS
    ):
        return cached[1]

    url = f"{clob_base_url.rstrip('/')}/fee-rate"
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(url, params={"token_id": token_id})
            response.raise_for_status()
            payload = response.json()
    except Exception as exc:
        _log.warning("Failed to fetch fee rate for token_id=%s: %s", token_id, exc)
        return None

    if not isinstance(payload, dict):
        _log.warning(
            "Unexpected /fee-rate response for token_id=%s: %s", token_id, type(payload)
        )
        return None

    _fee_rate_cache[token_id] = (now, payload)
    return payload


def invalidate_fee_rate_cache(token_id: str | None = None) -> None:
    """Invalidate cached fee-rate payloads."""
    if token_id:
        _fee_rate_cache.pop(token_id, None)
        return
    _fee_rate_cache.clear()


def get_cached_fee_params(token_id: str) -> tuple[int, int]:
    """Return (base_fee_bps, exponent) from cache, else defaults."""
    cached = _fee_rate_cache.get(token_id)
    if not cached:
        return DEFAULT_BASE_FEE_BPS, CRYPTO_FEE_EXPONENT

    ts, payload = cached
    if time.time() - ts >= _FEE_RATE_CACHE_TTL_SECONDS:
        _fee_rate_cache.pop(token_id, None)
        return DEFAULT_BASE_FEE_BPS, CRYPTO_FEE_EXPONENT

    fee_rate = (
        payload.get("feeRate")
        if payload.get("feeRate") is not None
        else payload.get("base_fee")
    )
    exponent = payload.get("exponent", CRYPTO_FEE_EXPONENT)

    try:
        fee_rate_value = float(fee_rate)
        if fee_rate_value < 0:
            raise ValueError("feeRate must be >= 0")
        base_fee_bps = (
            int(round(fee_rate_value * 10000)) if fee_rate_value <= 1.0 else int(round(fee_rate_value))
        )
    except (TypeError, ValueError):
        base_fee_bps = DEFAULT_BASE_FEE_BPS

    try:
        exponent_value = int(exponent)
        if exponent_value < 0:
            raise ValueError("exponent must be >= 0")
    except (TypeError, ValueError):
        exponent_value = CRYPTO_FEE_EXPONENT

    return base_fee_bps, exponent_value


def _resolve_fee_params(
    base_fee_bps: int,
    exponent: int,
    token_id: str | None,
) -> tuple[int, int]:
    """Use cached token-specific fee params when caller is using defaults."""
    if not token_id:
        return base_fee_bps, exponent

    cached_bps, cached_exponent = get_cached_fee_params(token_id)
    if base_fee_bps == DEFAULT_BASE_FEE_BPS:
        base_fee_bps = cached_bps
    if exponent == CRYPTO_FEE_EXPONENT:
        exponent = cached_exponent
    return base_fee_bps, exponent


def optimal_fee_zone(exponent: int = CRYPTO_FEE_EXPONENT) -> tuple[float, float]:
    """Return low-fee zone edges where fee ratio drops below 0.1% of notional.

    Result is `(left_edge, right_edge)` and should be interpreted as:
    [0, left_edge] and [right_edge, 1].
    """
    target_fee_ratio = 0.001  # 0.1% of notional
    base_rate = DEFAULT_BASE_FEE_BPS / 10000.0
    if exponent <= 0 or base_rate <= 0:
        return 0.0, 1.0

    threshold = (target_fee_ratio / base_rate) ** (1.0 / exponent)
    threshold = min(max(threshold, 0.0), 0.25)
    disc = 1.0 - 4.0 * threshold
    if disc <= 0:
        return 0.0, 1.0
    root = disc ** 0.5
    left = (1.0 - root) / 2.0
    right = (1.0 + root) / 2.0
    return round(left, 4), round(right, 4)


def fee_usdc(price: float, size: float, base_fee_bps: int = DEFAULT_BASE_FEE_BPS,
             exponent: int = CRYPTO_FEE_EXPONENT, token_id: str | None = None) -> float:
    """Compute fee in USDC terms using the fee curve.

    Args:
        price: order price (0-1)
        size: order size in shares
        base_fee_bps: from /fee-rate endpoint (default 1000 for crypto)
        exponent: curve exponent (2 for crypto)
        token_id: optional market token id to use cached fee params

    Returns:
        Fee in USDC, rounded to 4 decimal places, minimum 0.0001 if nonzero.
    """
    base_fee_bps, exponent = _resolve_fee_params(base_fee_bps, exponent, token_id)
    if price <= 0 or price >= 1.0 or size <= 0 or base_fee_bps <= 0:
        return 0.0
    notional = price * size
    curve = fee_curve_weight(price, exponent)
    fee = notional * (base_fee_bps / 10000.0) * curve
    fee = round(fee, 4)
    if 0 < fee < 0.0001:
        fee = 0.0001
    return fee


def taker_fee_usd(price: float, size: float, side: str,
                  base_fee_bps: int = DEFAULT_BASE_FEE_BPS,
                  token_id: str | None = None) -> float:
    """Compute taker fee in USDC (same formula regardless of side; settlement differs)."""
    return fee_usdc(price, size, base_fee_bps, token_id=token_id)


def net_shares_after_buy_fee(gross_size: float, price: float = 0.5,
                             base_fee_bps: int = DEFAULT_BASE_FEE_BPS,
                             token_id: str | None = None) -> float:
    """BUY taker: fee is paid in shares. Net shares = gross - fee_shares.

    Args:
        gross_size: shares in the order
        price: fill price (needed for fee curve calculation)
        base_fee_bps: from /fee-rate endpoint
        token_id: optional market token id to use cached fee params

    Returns:
        Net shares received after fee deduction.
    """
    if gross_size <= 0:
        return 0.0
    fee = fee_usdc(price, gross_size, base_fee_bps, token_id=token_id)
    if price <= 0:
        return gross_size
    fee_shares = fee / price
    return max(0.0, gross_size - fee_shares)


def net_usdc_after_sell_fee(gross_usdc: float, price: float, size: float,
                            base_fee_bps: int = DEFAULT_BASE_FEE_BPS,
                            token_id: str | None = None) -> float:
    """SELL taker: fee deducted from USDC proceeds."""
    fee = fee_usdc(price, size, base_fee_bps, token_id=token_id)
    return max(0.0, gross_usdc - fee)


# Legacy alias kept for backward compatibility during transition
TAKER_FEE_PCT = 2.0  # DEPRECATED - do not use; use fee_usdc() with base_fee_bps
