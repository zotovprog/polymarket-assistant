from __future__ import annotations

import time
from dataclasses import dataclass

from mm.types import Fill, MarketInfo, Quote

from .config import MMConfigV2
from .pair_inventory import build_pair_inventory
from .types import PairInventoryState


@dataclass
class SettlementDelta:
    delta: float = 0.0
    grace_until: float = 0.0


class ReconcileV2:
    STARTUP_RECONCILE_GRACE_SEC = 20.0
    STARTUP_REALIGN_LIMIT = 3

    def __init__(self, config: MMConfigV2):
        self.config = config
        self._expected_up: float | None = None
        self._expected_dn: float | None = None
        self._settlement: dict[str, SettlementDelta] = {}
        self._session_started_ts: float = 0.0
        self._fills_seen: int = 0
        self._startup_realign_count: int = 0
        self.status: str = "bootstrapping"
        self.true_drift: bool = False

    def align(self, up_shares: float, dn_shares: float) -> None:
        self._expected_up = max(0.0, float(up_shares))
        self._expected_dn = max(0.0, float(dn_shares))
        self.status = "ok"
        self.true_drift = False

    def start_session(self, up_shares: float, dn_shares: float) -> None:
        """Initialize expected balances for a fresh runtime session."""
        self.align(up_shares, dn_shares)
        self._session_started_ts = time.time()
        self._fills_seen = 0
        self._startup_realign_count = 0
        self._settlement.clear()

    def _add_settlement_delta(self, token_id: str, delta: float) -> None:
        if abs(float(delta)) <= 1e-9:
            return
        entry = self._settlement.setdefault(token_id, SettlementDelta())
        entry.delta += float(delta)
        entry.grace_until = time.time() + float(self.config.fill_settlement_grace_sec)

    def _startup_realign_allowed(self) -> bool:
        if self._fills_seen > 0:
            return False
        if self._session_started_ts <= 0:
            return False
        if (time.time() - self._session_started_ts) > self.STARTUP_RECONCILE_GRACE_SEC:
            return False
        return self._startup_realign_count < self.STARTUP_REALIGN_LIMIT

    def record_fill(self, fill: Fill, market: MarketInfo) -> None:
        self._fills_seen += 1
        token_id = fill.token_id
        size = float(fill.size)
        explicit_inventory_backed = max(0.0, float(getattr(fill, "inventory_backed_size", 0.0) or 0.0))
        explicit_short_backed = max(0.0, float(getattr(fill, "short_backed_size", 0.0) or 0.0))
        if token_id == market.up_token_id:
            if self._expected_up is None:
                self._expected_up = 0.0
            if fill.side == "BUY":
                self._expected_up = max(0.0, self._expected_up + size)
                self._add_settlement_delta(market.up_token_id, size)
            else:
                if explicit_inventory_backed > 0.0 or explicit_short_backed > 0.0:
                    inventory_backed = min(size, explicit_inventory_backed)
                    short_size = min(size - inventory_backed, explicit_short_backed)
                else:
                    inventory_backed = min(float(self._expected_up or 0.0), size)
                    short_size = max(0.0, size - inventory_backed)
                self._expected_up = max(0.0, float(self._expected_up or 0.0) - inventory_backed)
                if short_size > 0:
                    if self._expected_dn is None:
                        self._expected_dn = 0.0
                    self._expected_dn = max(0.0, float(self._expected_dn or 0.0) + short_size)
                self._add_settlement_delta(market.up_token_id, -inventory_backed)
                self._add_settlement_delta(market.dn_token_id, short_size)
        elif token_id == market.dn_token_id:
            if self._expected_dn is None:
                self._expected_dn = 0.0
            if fill.side == "BUY":
                self._expected_dn = max(0.0, self._expected_dn + size)
                self._add_settlement_delta(market.dn_token_id, size)
            else:
                if explicit_inventory_backed > 0.0 or explicit_short_backed > 0.0:
                    inventory_backed = min(size, explicit_inventory_backed)
                    short_size = min(size - inventory_backed, explicit_short_backed)
                else:
                    inventory_backed = min(float(self._expected_dn or 0.0), size)
                    short_size = max(0.0, size - inventory_backed)
                self._expected_dn = max(0.0, float(self._expected_dn or 0.0) - inventory_backed)
                if short_size > 0:
                    if self._expected_up is None:
                        self._expected_up = 0.0
                    self._expected_up = max(0.0, float(self._expected_up or 0.0) + short_size)
                self._add_settlement_delta(market.dn_token_id, -inventory_backed)
                self._add_settlement_delta(market.up_token_id, short_size)

    def _diff_explained(self, token_id: str, expected: float, real: float) -> bool:
        entry = self._settlement.get(token_id)
        if not entry:
            return False
        if time.time() > entry.grace_until:
            return False
        diff = float(expected) - float(real)
        return abs(diff) <= abs(entry.delta) + 0.25

    def reconcile(
        self,
        *,
        market: MarketInfo,
        real_up: float,
        real_dn: float,
        total_usdc: float,
        available_usdc: float | None,
        active_orders: dict[str, Quote],
        fv_up: float,
        fv_dn: float,
    ) -> PairInventoryState:
        if self._expected_up is None or self._expected_dn is None:
            self.align(real_up, real_dn)
        up_diff = abs(float(self._expected_up or 0.0) - float(real_up))
        dn_diff = abs(float(self._expected_dn or 0.0) - float(real_dn))
        threshold = float(self.config.reconcile_drift_threshold_shares)
        explained = (
            (up_diff <= threshold or self._diff_explained(market.up_token_id, float(self._expected_up or 0.0), real_up))
            and (dn_diff <= threshold or self._diff_explained(market.dn_token_id, float(self._expected_dn or 0.0), real_dn))
        )
        if up_diff <= threshold and dn_diff <= threshold:
            self.status = "ok"
            self.true_drift = False
        elif explained:
            self.status = "settlement_lag"
            self.true_drift = False
        elif self._startup_realign_allowed():
            self._startup_realign_count += 1
            self.status = "startup_realign"
            self.true_drift = False
        else:
            self.status = "broken"
            self.true_drift = True
        if not self.true_drift:
            self._expected_up = max(0.0, float(real_up))
            self._expected_dn = max(0.0, float(real_dn))
        return build_pair_inventory(
            up_shares=float(real_up),
            dn_shares=float(real_dn),
            total_usdc=float(total_usdc or 0.0),
            available_usdc=float(available_usdc) if available_usdc is not None else None,
            active_orders=active_orders,
            fv_up=float(fv_up),
            fv_dn=float(fv_dn),
            up_token_id=market.up_token_id,
            dn_token_id=market.dn_token_id,
        )
