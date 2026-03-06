from __future__ import annotations

from dataclasses import asdict
import time
from dataclasses import dataclass

from mm_shared.types import Fill, MarketInfo, Quote

from .config import MMConfigV2
from .pair_inventory import build_pair_inventory
from .types import PairInventoryState


@dataclass
class SettlementDelta:
    delta: float = 0.0
    grace_until: float = 0.0


@dataclass
class DriftEvidence:
    up_diff: float = 0.0
    dn_diff: float = 0.0
    threshold: float = 0.0
    candidate_count: int = 0
    candidate_started_ts: float = 0.0
    candidate_age_sec: float = 0.0
    sellability_lag_active: bool = False
    wallet_snapshot_stale: bool = False
    classification: str = "bootstrapping"
    reason: str = "bootstrapping"

    def to_dict(self) -> dict[str, float | int | str | bool]:
        return asdict(self)


class ReconcileV2:
    STARTUP_RECONCILE_GRACE_SEC = 20.0
    STARTUP_REALIGN_LIMIT = 3
    DRIFT_CONFIRM_TICKS = 3
    DRIFT_CONFIRM_WINDOW_SEC = 20.0
    DRIFT_CONFIRM_MIN_AGE_SEC = 8.0

    def __init__(self, config: MMConfigV2):
        self.config = config
        self._expected_up: float | None = None
        self._expected_dn: float | None = None
        self._settlement: dict[str, SettlementDelta] = {}
        self._session_started_ts: float = 0.0
        self._fills_seen: int = 0
        self._startup_realign_count: int = 0
        self._drift_candidate_count: int = 0
        self._drift_candidate_started_ts: float = 0.0
        self.status: str = "bootstrapping"
        self.true_drift: bool = False
        self.drift_evidence: DriftEvidence = DriftEvidence()

    def align(self, up_shares: float, dn_shares: float) -> None:
        self._expected_up = max(0.0, float(up_shares))
        self._expected_dn = max(0.0, float(dn_shares))
        self.status = "ok"
        self.true_drift = False
        self._drift_candidate_count = 0
        self._drift_candidate_started_ts = 0.0
        self.drift_evidence = DriftEvidence(
            up_diff=0.0,
            dn_diff=0.0,
            threshold=float(self.config.reconcile_drift_threshold_shares),
            candidate_count=0,
            candidate_started_ts=0.0,
            candidate_age_sec=0.0,
            classification="ok",
            reason="aligned",
        )

    def start_session(self, up_shares: float, dn_shares: float) -> None:
        """Initialize expected balances for a fresh runtime session."""
        self.align(up_shares, dn_shares)
        self._session_started_ts = time.time()
        self._fills_seen = 0
        self._startup_realign_count = 0
        self._settlement.clear()

    def _mark_drift_candidate(self) -> bool:
        """Return True when persistent mismatch is confirmed as true drift."""
        now = time.time()
        if (
            self._drift_candidate_count <= 0
            or self._drift_candidate_started_ts <= 0
            or (now - self._drift_candidate_started_ts) > self.DRIFT_CONFIRM_WINDOW_SEC
        ):
            self._drift_candidate_count = 1
            self._drift_candidate_started_ts = now
        else:
            self._drift_candidate_count += 1
        candidate_age = max(0.0, now - self._drift_candidate_started_ts)
        return (
            self._drift_candidate_count >= self.DRIFT_CONFIRM_TICKS
            and candidate_age >= self.DRIFT_CONFIRM_MIN_AGE_SEC
        )

    def expected_balances(self) -> tuple[float | None, float | None]:
        """Return latest internal expected wallet balances."""
        return self._expected_up, self._expected_dn

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
        sellability_lag_active: bool = False,
        wallet_snapshot_stale: bool = False,
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
        status_reason = "drift within threshold"
        if up_diff <= threshold and dn_diff <= threshold:
            self.status = "ok"
            self.true_drift = False
            self._drift_candidate_count = 0
            self._drift_candidate_started_ts = 0.0
        elif explained:
            self.status = "settlement_lag"
            self.true_drift = False
            self._drift_candidate_count = 0
            self._drift_candidate_started_ts = 0.0
            status_reason = "diff explained by settlement lag"
        elif wallet_snapshot_stale:
            # PM wallet endpoints can be transiently stale/unavailable.
            # Never escalate stale snapshots to true drift.
            self.status = "wallet_stale"
            self.true_drift = False
            self._drift_candidate_count = 0
            self._drift_candidate_started_ts = 0.0
            status_reason = "wallet snapshot stale"
        elif sellability_lag_active:
            # PM may briefly report constrained free token balance after SELL
            # cancel/repost. Treat this window as transient execution lag.
            self.status = "sellability_lag"
            self.true_drift = False
            self._drift_candidate_count = 0
            self._drift_candidate_started_ts = 0.0
            status_reason = "sellability lag active"
        elif self._startup_realign_allowed():
            self._startup_realign_count += 1
            self.status = "startup_realign"
            self.true_drift = False
            self._drift_candidate_count = 0
            self._drift_candidate_started_ts = 0.0
            status_reason = "startup realign"
        else:
            if self._mark_drift_candidate():
                self.status = "broken"
                self.true_drift = True
                status_reason = "persistent unexplained drift confirmed"
            else:
                self.status = "drift_pending"
                self.true_drift = False
                status_reason = "unexplained drift candidate"
        if self.status in {"ok", "settlement_lag", "sellability_lag", "startup_realign", "wallet_stale"}:
            self._expected_up = max(0.0, float(real_up))
            self._expected_dn = max(0.0, float(real_dn))

        now = time.time()
        candidate_age = 0.0
        if self._drift_candidate_count > 0 and self._drift_candidate_started_ts > 0:
            candidate_age = max(0.0, now - self._drift_candidate_started_ts)
        self.drift_evidence = DriftEvidence(
            up_diff=float(up_diff),
            dn_diff=float(dn_diff),
            threshold=float(threshold),
            candidate_count=int(self._drift_candidate_count),
            candidate_started_ts=float(self._drift_candidate_started_ts),
            candidate_age_sec=float(candidate_age),
            sellability_lag_active=bool(sellability_lag_active),
            wallet_snapshot_stale=bool(wallet_snapshot_stale),
            classification=self.status,
            reason=status_reason,
        )
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
            session_budget_usd=float(self.config.session_budget_usd),
            target_pair_value_ratio=float(self.config.target_pair_value_ratio),
        )
