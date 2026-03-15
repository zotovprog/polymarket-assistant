"""Arb opportunity scanner — polls order books and detects profitable pairs."""
from __future__ import annotations

import asyncio
import logging
import time

from mm_shared.pm_fees import fee_usdc, net_shares_after_buy_fee

from .config import PairArbConfig
from .types import ArbMarket, ArbOpportunity

log = logging.getLogger(__name__)


class ArbScanner:
    """Scans Polymarket order books for pair arb opportunities."""

    def __init__(self, order_mgr, config: PairArbConfig):
        self.order_mgr = order_mgr
        self.config = config

    async def scan_market(self, market: ArbMarket) -> ArbOpportunity | None:
        """Scan a single market for arb opportunity.

        Returns an ArbOpportunity if buying both UP+DN asks and merging
        yields profit above min_profit_bps, else None.
        """
        try:
            up_book, dn_book = await asyncio.gather(
                self.order_mgr.get_full_book(market.up_token_id),
                self.order_mgr.get_full_book(market.dn_token_id),
            )
        except Exception as e:
            log.debug("Book fetch failed for %s: %s", market.scope, e)
            return None

        up_asks = self._extract_asks(up_book)
        dn_asks = self._extract_asks(dn_book)

        if not up_asks or not dn_asks:
            return None

        # Use best (lowest) ask from each side
        best_up_price, best_up_size = up_asks[0]
        best_dn_price, best_dn_size = dn_asks[0]

        # Quick check: if asks sum to >= 1.0, no arb possible
        gross_cost = best_up_price + best_dn_price
        if gross_cost >= 1.0:
            return None

        # Compute executable size
        max_shares = min(
            best_up_size,
            best_dn_size,
            self.config.max_clip_shares,
        )
        if max_shares < self.config.min_clip_shares:
            return None

        # Compute fees
        if self.config.use_maker_orders:
            fee_up = 0.0
            fee_dn = 0.0
            net_up = max_shares
            net_dn = max_shares
        else:
            fee_up = fee_usdc(best_up_price, max_shares, token_id=market.up_token_id)
            fee_dn = fee_usdc(best_dn_price, max_shares, token_id=market.dn_token_id)
            net_up = net_shares_after_buy_fee(
                max_shares, best_up_price, token_id=market.up_token_id
            )
            net_dn = net_shares_after_buy_fee(
                max_shares, best_dn_price, token_id=market.dn_token_id
            )

        # Mergeable shares = min of net received from each side
        mergeable = min(net_up, net_dn)
        if mergeable < self.config.min_clip_shares:
            return None

        # Total cost per pair including fees and gas
        fee_per_share = (fee_up + fee_dn) / max_shares if max_shares > 0 else 0
        gas_per_share = self.config.gas_cost_usd / mergeable if mergeable > 0 else 0
        total_cost_per_pair = gross_cost + fee_per_share + gas_per_share

        profit_per_pair = 1.0 - total_cost_per_pair
        profit_bps = profit_per_pair * 10000

        if profit_bps < self.config.min_profit_bps:
            return None

        profit_usd = profit_per_pair * mergeable

        return ArbOpportunity(
            market=market,
            ask_up=best_up_price,
            ask_dn=best_dn_price,
            size_up=best_up_size,
            size_dn=best_dn_size,
            fee_up_per_share=fee_up / max_shares if max_shares > 0 else 0,
            fee_dn_per_share=fee_dn / max_shares if max_shares > 0 else 0,
            net_shares_up=net_up,
            net_shares_dn=net_dn,
            max_arb_shares=mergeable,
            gross_cost_per_pair=gross_cost,
            total_cost_per_pair=total_cost_per_pair,
            profit_per_pair=profit_per_pair,
            profit_usd=profit_usd,
            detected_at=time.time(),
        )

    async def scan_all(self, markets: list[ArbMarket]) -> list[ArbOpportunity]:
        """Scan all markets in parallel, return opportunities sorted by profit."""
        if not markets:
            return []

        results = await asyncio.gather(
            *(self.scan_market(m) for m in markets),
            return_exceptions=True,
        )

        opps = [r for r in results if isinstance(r, ArbOpportunity)]
        return sorted(opps, key=lambda x: x.profit_usd, reverse=True)

    @staticmethod
    def _extract_asks(book: dict | None) -> list[tuple[float, float]]:
        """Extract and sort asks from order book dict.

        Returns list of (price, size) tuples sorted ascending by price.
        """
        if not book or not isinstance(book, dict):
            return []

        raw_asks = book.get("asks") or []
        asks = []
        for entry in raw_asks:
            try:
                price = float(entry.get("price", 0))
                size = float(entry.get("size", 0))
                if 0 < price < 1.0 and size > 0:
                    asks.append((price, size))
            except (TypeError, ValueError):
                continue

        asks.sort(key=lambda x: x[0])
        return asks
