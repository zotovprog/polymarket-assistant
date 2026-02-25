"""Polymarket fee calculator.

PM fees for crypto markets (fee-enabled):
- Maker: always 0
- Taker BUY:  fee_in_shares = size * price * (FEE_PCT / 100) / price = size * FEE_PCT / 100
              net_shares = size * (1 - FEE_PCT / 100)
              fee_in_usd = fee_in_shares * price
- Taker SELL: fee_in_usd = size * (1 - price) * (FEE_PCT / 100)
              net_usdc = gross_usdc - fee_in_usd
"""

TAKER_FEE_PCT = 2.0  # 2% for crypto markets


def taker_fee_usd(price: float, size: float, side: str) -> float:
    """Compute taker fee in USD terms.

    For BUY: fee is technically in shares but we express in USD.
    For SELL: fee is in USDC.
    """
    if side == "BUY":
        fee_shares = size * TAKER_FEE_PCT / 100.0
        return fee_shares * price
    else:  # SELL
        return size * (1.0 - price) * TAKER_FEE_PCT / 100.0


def net_shares_after_buy_fee(gross_size: float) -> float:
    """Net shares received after BUY taker fee deduction."""
    return gross_size * (1.0 - TAKER_FEE_PCT / 100.0)


def net_usdc_after_sell_fee(gross_usdc: float, price: float, size: float) -> float:
    """Net USDC received after SELL taker fee deduction."""
    fee = size * (1.0 - price) * TAKER_FEE_PCT / 100.0
    return gross_usdc - fee
