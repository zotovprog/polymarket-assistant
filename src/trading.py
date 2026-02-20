import asyncio
import json
import os
import re
import shutil
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from queue import Empty, SimpleQueue

import config
import indicators as ind


LIVE_CONFIRM_TOKEN = "I_UNDERSTAND_REAL_MONEY_RISK"


class TradeMode(str, Enum):
    OBSERVE = "observe"
    PAPER = "paper"
    LIVE = "live"


@dataclass
class TradingConfig:
    size_usd: float = 5.0
    min_abs_bias: float = 75.0
    min_abs_obi: float = 0.35
    min_price: float = 0.20
    max_price: float = 0.70
    cooldown_sec: int = 900
    max_trades_per_day: int = 20
    eval_interval_sec: int = 2
    control_file: str = ".traderctl"
    approval_beep_enabled: bool = True
    approval_sound_command: str = ""
    executions_log_file: str = "executions.log.jsonl"
    binance_ob_stale_sec: int = 30
    live_manual_approval: bool = True
    pending_approval_ttl_sec: int = 30
    live_entry_require_fill: bool = True
    live_entry_fill_timeout_sec: int = 20
    live_entry_fill_poll_sec: float = 1.0
    live_cancel_unfilled_entry: bool = True
    live_exit_size_buffer_pct: float = 0.0
    exit_retry_backoff_sec: int = 15
    auto_exit_enabled: bool = True
    tp_pct: float = 15.0
    sl_pct: float = 8.0
    max_hold_sec: int = 900
    reverse_exit_enabled: bool = True
    reverse_exit_bias: float = 60.0
    max_daily_loss_usd: float = 50.0
    max_consecutive_losses: int = 5
    trailing_stop_enabled: bool = True
    trailing_stop_activation_pct: float = 5.0
    trailing_stop_distance_pct: float = 3.0
    dynamic_sizing_enabled: bool = False
    min_size_usd: float = 5.0
    max_size_usd: float = 25.0
    sizing_bias_floor: float = 50.0
    sizing_bias_ceiling: float = 90.0


@dataclass
class TradeDecision:
    side: str
    token_id: str
    price: float
    bias: float
    obi: float
    reason: str
    ts: float
    size_usd: float = 0.0


@dataclass
class OpenPosition:
    side: str
    token_id: str
    entry_price: float
    size_usd: float
    shares: float
    entry_ts: float
    entry_bias: float
    entry_obi: float
    high_water_mark_price: float = 0.0
    entry_order_id: str | None = None


@dataclass
class TradeRecord:
    ts_iso: str
    action: str
    mode: str
    coin: str
    timeframe: str
    side: str
    token_id: str
    price: float
    size_usd: float
    status: str
    reason: str
    shares: float | None = None
    order_id: str | None = None
    pnl_usd: float | None = None
    pnl_pct: float | None = None
    fill_elapsed_ms: float | None = None
    slippage_bps: float | None = None


ENTRY_EXECUTION_STATUSES = {
    "paper",
    "posted",
    "filled",
    "partial_filled",
    "partial_filled_cancelled",
    "partial_filled_open",
}

EXIT_EXECUTION_STATUSES = {
    "paper_exit",
    "exit_filled",
    "exit_partial_cancelled",
    "exit_partial_open",
}


def is_execution_status(action: str | None, status: str | None) -> bool:
    action_norm = (action or "").strip().lower()
    status_norm = (status or "").strip().lower()
    if not action_norm or not status_norm:
        return False
    if action_norm == "entry":
        return status_norm in ENTRY_EXECUTION_STATUSES
    if action_norm == "exit":
        return status_norm in EXIT_EXECUTION_STATUSES
    return False


def pnl_with_fees(entry_price: float, exit_price: float, shares: float,
                  entry_fee: float = None, exit_fee: float = None) -> tuple[float, float]:
    """Calculate PnL accounting for entry and exit taker fees.
    Returns (pnl_usd, pnl_pct). Both include fee deductions."""
    import config
    ef = entry_fee if entry_fee is not None else config.PM_TAKER_FEE
    xf = exit_fee if exit_fee is not None else config.PM_TAKER_FEE
    if entry_price <= 0 or shares <= 0:
        return 0.0, 0.0
    entry_cost = entry_price * shares * (1.0 + ef)
    exit_proceeds = exit_price * shares * (1.0 - xf)
    pnl_usd = exit_proceeds - entry_cost
    pnl_pct = (exit_proceeds / entry_cost - 1.0) * 100.0 if entry_cost > 0 else 0.0
    return pnl_usd, pnl_pct


@dataclass
class TraderState:
    trades: list[TradeRecord] = field(default_factory=list)
    open_position: OpenPosition | None = None
    open_side: str | None = None
    open_order_id: str | None = None
    last_trade_ts: float = 0.0
    day_key: str = ""
    trades_today: int = 0
    last_skip_reason: str = ""
    last_skip_log_ts: float = 0.0
    pending_decision: TradeDecision | None = None
    pending_sig: str = ""
    pending_key: str = ""
    approval_armed: bool = False
    force_close_requested: bool = False
    next_exit_attempt_ts: float = 0.0
    session_pnl_usd: float = 0.0
    consecutive_losses: int = 0
    circuit_breaker_active: bool = False
    circuit_breaker_reason: str = ""
    session_stats: "SessionStats" = field(default_factory=lambda: SessionStats())


@dataclass
class SessionStats:
    total_trades: int = 0
    wins: int = 0
    losses: int = 0
    gross_pnl_usd: float = 0.0
    total_fees_usd: float = 0.0
    net_pnl_usd: float = 0.0
    best_trade_pnl_usd: float = 0.0
    worst_trade_pnl_usd: float = 0.0
    pnl_series: list[float] = field(default_factory=list)
    fill_attempts: int = 0      # Total entry attempts (posted orders)
    fill_successes: int = 0     # Successfully filled entries
    fill_partial: int = 0       # Partially filled entries
    total_fill_time_ms: float = 0.0  # Sum of fill times for averaging
    fill_count_timed: int = 0   # Number of fills with timing data
    total_slippage_bps: float = 0.0  # Sum of slippage in basis points
    slippage_count: int = 0     # Number of slippage measurements

    @property
    def win_rate(self) -> float:
        return self.wins / self.total_trades if self.total_trades > 0 else 0.0

    @property
    def avg_win_usd(self) -> float:
        wins = [p for p in self.pnl_series if p > 0]
        return sum(wins) / len(wins) if wins else 0.0

    @property
    def avg_loss_usd(self) -> float:
        losses = [p for p in self.pnl_series if p <= 0]
        return sum(losses) / len(losses) if losses else 0.0

    @property
    def profit_factor(self) -> float:
        total_win = sum(p for p in self.pnl_series if p > 0)
        total_loss = sum(abs(p) for p in self.pnl_series if p < 0)
        return total_win / total_loss if total_loss > 0 else (99.99 if total_win > 0 else 0.0)

    @property
    def fill_ratio(self) -> float:
        return self.fill_successes / self.fill_attempts if self.fill_attempts > 0 else 0.0

    @property
    def avg_fill_time_ms(self) -> float:
        return self.total_fill_time_ms / self.fill_count_timed if self.fill_count_timed > 0 else 0.0

    @property
    def avg_slippage_bps(self) -> float:
        return self.total_slippage_bps / self.slippage_count if self.slippage_count > 0 else 0.0


class PaperExecutor:
    def __init__(self, cfg: TradingConfig):
        self.cfg = cfg

    def execute_entry(self, decision: TradeDecision, coin: str, timeframe: str) -> TradeRecord:
        effective_size = decision.size_usd if decision.size_usd > 0 else self.cfg.size_usd
        shares = effective_size / max(decision.price, 0.01)
        return TradeRecord(
            ts_iso=datetime.now(timezone.utc).isoformat(),
            action="entry",
            mode=TradeMode.PAPER.value,
            coin=coin,
            timeframe=timeframe,
            side=decision.side,
            token_id=decision.token_id,
            price=decision.price,
            size_usd=effective_size,
            shares=shares,
            status="paper",
            reason=decision.reason,
        )

    def close_position(
        self,
        position: OpenPosition,
        exit_price: float,
        reason: str,
        coin: str,
        timeframe: str,
        pnl_usd: float | None,
        pnl_pct: float | None,
    ) -> TradeRecord:
        return TradeRecord(
            ts_iso=datetime.now(timezone.utc).isoformat(),
            action="exit",
            mode=TradeMode.PAPER.value,
            coin=coin,
            timeframe=timeframe,
            side=position.side,
            token_id=position.token_id,
            price=exit_price,
            size_usd=position.size_usd,
            shares=position.shares,
            status="paper_exit",
            reason=reason,
            pnl_usd=pnl_usd,
            pnl_pct=pnl_pct,
        )

    def cancel(self, order_id: str) -> bool:
        return True


class LiveExecutor:
    def __init__(self, cfg: TradingConfig, runtime_env: dict[str, str] | None = None):
        self.cfg = cfg
        self.runtime_env = runtime_env or {}
        self.client = None
        self._min_size_cache: dict[str, float] = {}
        self._conditional_allowance_checked: set[str] = set()
        self._init_client()

    def _env_get(self, key: str, default: str | None = None) -> str | None:
        val = self.runtime_env.get(key)
        if val is None:
            return os.environ.get(key, default)
        return str(val)

    def _init_client(self):
        private_key = self._env_get("PM_PRIVATE_KEY")
        funder = self._env_get("PM_FUNDER")
        sig_type = int(self._env_get("PM_SIGNATURE_TYPE", "0") or "0")

        if self._env_get("PM_ENABLE_LIVE") != "1":
            raise ValueError("PM_ENABLE_LIVE=1 is required for live mode")
        if not private_key:
            raise ValueError("PM_PRIVATE_KEY is required for live mode")
        if not funder:
            raise ValueError("PM_FUNDER is required for live mode")

        try:
            from py_clob_client.client import ClobClient
        except Exception as e:  # pragma: no cover
            raise ValueError(
                "py-clob-client is not installed. Install it with: pip install py-clob-client"
            ) from e

        host = "https://clob.polymarket.com"
        chain_id = 137

        temp_client = ClobClient(host, key=private_key, chain_id=chain_id)
        api_creds = temp_client.create_or_derive_api_creds()

        self.client = ClobClient(
            host,
            key=private_key,
            chain_id=chain_id,
            signature_type=sig_type,
            funder=funder,
            creds=api_creds,
        )

    def preflight(self) -> dict:
        private_key = self._env_get("PM_PRIVATE_KEY") or ""
        funder = self._env_get("PM_FUNDER") or ""
        sig_raw = self._env_get("PM_SIGNATURE_TYPE", "0") or "0"
        checks: list[dict] = []
        ok = True

        def add_check(name: str, status: str, detail: str):
            nonlocal ok
            checks.append({"name": name, "status": status, "detail": detail})
            if status == "error":
                ok = False

        if re.fullmatch(r"0x[a-fA-F0-9]{64}", private_key):
            add_check("private_key_format", "ok", "looks valid")
        else:
            add_check("private_key_format", "error", "expected 0x + 64 hex chars")

        if re.fullmatch(r"0x[a-fA-F0-9]{40}", funder):
            add_check("funder_format", "ok", "looks valid")
        else:
            add_check("funder_format", "error", "expected 0x + 40 hex chars")

        if sig_raw in {"0", "1", "2"}:
            detail = {
                "0": "EOA/private-key signer",
                "1": "Magic/email signer",
                "2": "proxy signer",
            }[sig_raw]
            add_check("signature_type", "ok", f"{sig_raw} ({detail})")
        else:
            add_check("signature_type", "error", f"unsupported value: {sig_raw}")

        try:
            from py_clob_client.clob_types import BalanceAllowanceParams, AssetType

            params = BalanceAllowanceParams(
                asset_type=AssetType.COLLATERAL,
                signature_type=-1,
            )
            data = self.client.get_balance_allowance(params)
            bal_raw = str((data or {}).get("balance", "")).strip()
            bal = self._as_float(bal_raw)
            if bal is None:
                add_check("api_collateral_read", "warn", "response parsed but balance is not numeric")
            else:
                add_check("api_collateral_read", "ok", f"balance={bal}")
        except Exception as e:
            add_check("api_collateral_read", "error", self._format_exception_status("preflight", e))

        return {
            "ok": ok,
            "checks": checks,
            "signature_type": sig_raw,
            "funder": funder,
            "private_key_masked": f"{private_key[:6]}...{private_key[-4:]}" if private_key else "",
        }

    def execute_entry(self, decision: TradeDecision, coin: str, timeframe: str) -> TradeRecord:
        from py_clob_client.clob_types import OrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY

        price = round(decision.price, 3)
        effective_size = decision.size_usd if decision.size_usd > 0 else self.cfg.size_usd
        size = round(effective_size / max(price, 0.01), 2)
        order_id = None
        status = "failed"
        shares: float | None = None
        verify_note = ""
        fill_elapsed_ms: float | None = None
        slippage_bps: float | None = None

        try:
            min_size = self._get_min_order_size(decision.token_id)
            if min_size is not None and size < min_size:
                status = "blocked_min_size"
                verify_note = f"size={size:.2f} < min_order_size={min_size:.2f}"
                raise RuntimeError("min_order_size_guard")

            signed_order = self.client.create_order(
                OrderArgs(
                    token_id=decision.token_id,
                    price=price,
                    size=size,
                    side=BUY,
                )
            )
            _fill_start = time.monotonic()
            resp = self.client.post_order(signed_order, OrderType.GTC)
            order_id = resp.get("orderID") or resp.get("id")
            if not order_id:
                status = "failed"
            elif not self.cfg.live_entry_require_fill:
                status = "posted"
                shares = size
            else:
                outcome, state, matched, total, err, fill_price = self._wait_for_fill(order_id)
                fill_elapsed_ms = (time.monotonic() - _fill_start) * 1000.0
                verify_note = f"fill_check={state}"
                if matched is not None:
                    denom = total if (total is not None and total > 0) else size
                    verify_note = f"{verify_note}, filled={matched:.2f}/{denom:.2f}"
                if fill_price is not None and price > 0 and matched is not None and matched > 0:
                    slippage_bps = (fill_price - price) / price * 10000.0

                if outcome == "filled":
                    status = "filled"
                    shares = matched if matched and matched > 0 else size
                elif outcome == "partial":
                    shares = matched if matched and matched > 0 else None
                    if shares is not None:
                        status = "partial_filled"
                        if self.cfg.live_cancel_unfilled_entry:
                            cancelled = self.cancel(order_id)
                            status = "partial_filled_cancelled" if cancelled else "partial_filled_open"
                    else:
                        status = "unfilled"
                        if self.cfg.live_cancel_unfilled_entry:
                            cancelled = self.cancel(order_id)
                            status = "unfilled_cancelled" if cancelled else "unfilled_open"
                elif outcome == "terminal_unfilled":
                    status = "unfilled_terminal"
                else:
                    status = "unfilled"
                    if self.cfg.live_cancel_unfilled_entry:
                        cancelled = self.cancel(order_id)
                        status = "unfilled_cancelled" if cancelled else "unfilled_open"
                    if err:
                        verify_note = f"{verify_note}, err={err}"
        except Exception as e:  # pragma: no cover
            if status != "blocked_min_size":
                status = self._format_exception_status("error", e)

        reason = decision.reason
        if verify_note:
            reason = f"{reason}, {verify_note}"

        return TradeRecord(
            ts_iso=datetime.now(timezone.utc).isoformat(),
            action="entry",
            mode=TradeMode.LIVE.value,
            coin=coin,
            timeframe=timeframe,
            side=decision.side,
            token_id=decision.token_id,
            price=price,
            size_usd=effective_size,
            shares=shares,
            status=status,
            reason=reason,
            order_id=order_id,
            fill_elapsed_ms=fill_elapsed_ms,
            slippage_bps=slippage_bps,
        )

    def close_position(
        self,
        position: OpenPosition,
        exit_price: float,
        reason: str,
        coin: str,
        timeframe: str,
        pnl_usd: float | None,
        pnl_pct: float | None,
    ) -> TradeRecord:
        from py_clob_client.clob_types import OrderArgs, OrderType
        from py_clob_client.order_builder.constants import SELL

        price = round(exit_price, 3)
        # Default behavior: try to liquidate as much as possible (including residuals),
        # not just the tracked position size.
        size_raw = position.shares * (1.0 - self.cfg.live_exit_size_buffer_pct / 100.0)
        size = round(max(0.01, size_raw), 2)
        order_id = None
        status = "exit_failed"
        reason = f"{reason}, sell_size={size:.2f}"

        self._ensure_conditional_allowance(position.token_id)
        bal_info = self._get_conditional_balance_allowance(position.token_id)
        bal_raw = ""
        est_balance_shares = None
        if bal_info is not None:
            bal_raw = str(bal_info.get("balance", "")).strip()
            est_balance_shares = self._estimate_shares_from_balance_raw(
                bal_raw,
                expected=position.shares,
            )
            if est_balance_shares is not None and est_balance_shares <= 0:
                return TradeRecord(
                    ts_iso=datetime.now(timezone.utc).isoformat(),
                    action="exit",
                    mode=TradeMode.LIVE.value,
                    coin=coin,
                    timeframe=timeframe,
                    side=position.side,
                    token_id=position.token_id,
                    price=price,
                    size_usd=position.size_usd,
                    shares=0.0,
                    status="exit_no_token_balance",
                    reason=f"{reason}, conditional_balance=0",
                    order_id=None,
                    pnl_usd=pnl_usd,
                    pnl_pct=pnl_pct,
                )

        if est_balance_shares is not None and est_balance_shares > 0:
            # Prefer actual token balance to clean residuals automatically.
            target_size = max(0.01, est_balance_shares * (1.0 - self.cfg.live_exit_size_buffer_pct / 100.0))
            size = round(target_size, 2)
            reason = f"{reason}, est_balance={est_balance_shares:.4f}"

        min_size = self._get_min_order_size(position.token_id)
        if (
            min_size is not None
            and est_balance_shares is not None
            and est_balance_shares > 0
            and est_balance_shares < min_size
        ):
            return TradeRecord(
                ts_iso=datetime.now(timezone.utc).isoformat(),
                action="exit",
                mode=TradeMode.LIVE.value,
                coin=coin,
                timeframe=timeframe,
                side=position.side,
                token_id=position.token_id,
                price=price,
                size_usd=position.size_usd,
                shares=est_balance_shares,
                status="exit_dust_cleared",
                reason=f"{reason}, min_order_size={min_size:.2f}, dust_autoclear=1",
                order_id=None,
                pnl_usd=pnl_usd,
                pnl_pct=pnl_pct,
            )
        if min_size is not None and size < min_size:
            return TradeRecord(
                ts_iso=datetime.now(timezone.utc).isoformat(),
                action="exit",
                mode=TradeMode.LIVE.value,
                coin=coin,
                timeframe=timeframe,
                side=position.side,
                token_id=position.token_id,
                price=price,
                size_usd=position.size_usd,
                shares=size,
                status="exit_blocked_min_size",
                reason=f"{reason}, min_order_size={min_size:.2f}",
                order_id=None,
                pnl_usd=pnl_usd,
                pnl_pct=pnl_pct,
            )

        attempt_sizes = self._build_exit_attempt_sizes(size, min_size)
        if not attempt_sizes:
            return TradeRecord(
                ts_iso=datetime.now(timezone.utc).isoformat(),
                action="exit",
                mode=TradeMode.LIVE.value,
                coin=coin,
                timeframe=timeframe,
                side=position.side,
                token_id=position.token_id,
                price=price,
                size_usd=position.size_usd,
                shares=size,
                status="exit_blocked_min_size",
                reason=f"{reason}, min_order_size={min_size:.2f}" if min_size is not None else reason,
                order_id=None,
                pnl_usd=pnl_usd,
                pnl_pct=pnl_pct,
            )

        final_attempt = attempt_sizes[0]
        matched_shares = 0.0
        for idx, sz in enumerate(attempt_sizes, start=1):
            final_attempt = sz
            try:
                signed_order = self.client.create_order(
                    OrderArgs(
                        token_id=position.token_id,
                        price=price,
                        size=sz,
                        side=SELL,
                    )
                )
                resp = self.client.post_order(signed_order, OrderType.GTC)
                order_id = resp.get("orderID") or resp.get("id")
                reason = f"{reason}, attempt={idx}/{len(attempt_sizes)}, placed_sell_size={sz:.2f}"
                if not order_id:
                    status = "exit_failed"
                    continue

                outcome, fill_state, matched, total, err, _fill_price = self._wait_for_fill(order_id)
                if matched is not None and matched > 0:
                    matched_shares = float(matched)
                denom = total if (total is not None and total > 0) else sz
                reason = (
                    f"{reason}, exit_fill={fill_state}, "
                    f"filled={matched_shares:.2f}/{denom:.2f}"
                )

                if outcome == "filled":
                    status = "exit_filled"
                    return TradeRecord(
                        ts_iso=datetime.now(timezone.utc).isoformat(),
                        action="exit",
                        mode=TradeMode.LIVE.value,
                        coin=coin,
                        timeframe=timeframe,
                        side=position.side,
                        token_id=position.token_id,
                        price=price,
                        size_usd=position.size_usd,
                        shares=matched_shares if matched_shares > 0 else sz,
                        status=status,
                        reason=reason,
                        order_id=order_id,
                        pnl_usd=pnl_usd,
                        pnl_pct=pnl_pct,
                    )

                cancelled = False
                if self.cfg.live_cancel_unfilled_entry:
                    cancelled = self.cancel(order_id)

                if outcome == "partial" and matched_shares > 0:
                    status = "exit_partial_cancelled" if cancelled else "exit_partial_open"
                else:
                    status = "exit_unfilled_cancelled" if cancelled else "exit_unfilled_open"
                    if err:
                        reason = f"{reason}, err={err}"

                return TradeRecord(
                    ts_iso=datetime.now(timezone.utc).isoformat(),
                    action="exit",
                    mode=TradeMode.LIVE.value,
                    coin=coin,
                    timeframe=timeframe,
                    side=position.side,
                    token_id=position.token_id,
                    price=price,
                    size_usd=position.size_usd,
                    shares=matched_shares if matched_shares > 0 else 0.0,
                    status=status,
                    reason=reason,
                    order_id=order_id,
                    pnl_usd=pnl_usd,
                    pnl_pct=pnl_pct,
                )
            except Exception as e:  # pragma: no cover
                status = self._format_exception_status("exit_error", e)
                reason = f"{reason}, attempt={idx}/{len(attempt_sizes)}, try_sell_size={sz:.2f}"
                if self._is_not_enough_balance_or_allowance(status):
                    # Force-refresh allowance and try next smaller size.
                    self._ensure_conditional_allowance(position.token_id, force=True)
                    continue
                break

        return TradeRecord(
            ts_iso=datetime.now(timezone.utc).isoformat(),
            action="exit",
            mode=TradeMode.LIVE.value,
            coin=coin,
            timeframe=timeframe,
            side=position.side,
            token_id=position.token_id,
            price=price,
            size_usd=position.size_usd,
            shares=matched_shares if matched_shares > 0 else final_attempt,
            status=status,
            reason=reason,
            order_id=order_id,
            pnl_usd=pnl_usd,
            pnl_pct=pnl_pct,
        )

    def cancel(self, order_id: str) -> bool:
        if not order_id:
            return False
        try:
            self.client.cancel(order_id)
            return True
        except Exception:
            return False

    @staticmethod
    def _format_exception_status(prefix: str, e: Exception) -> str:
        name = type(e).__name__
        status_code = getattr(e, "status_code", None)
        error_msg = getattr(e, "error_msg", None)

        if status_code is not None or error_msg is not None:
            return f"{prefix}:{name}(status_code={status_code}, error={error_msg})"

        text = str(e).strip()
        if text and text != name:
            return f"{prefix}:{name}: {text}"
        return f"{prefix}:{name}"

    @staticmethod
    def _as_float(value) -> float | None:
        try:
            if value is None:
                return None
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _normalize_order_payload(payload) -> dict:
        if not isinstance(payload, dict):
            return {}
        for key in ("order", "data", "result"):
            nested = payload.get(key)
            if isinstance(nested, dict):
                return nested
        return payload

    def _order_metrics(self, order: dict) -> tuple[float | None, float | None, float | None]:
        size = self._as_float(
            order.get("size")
            or order.get("original_size")
            or order.get("initial_size")
            or order.get("amount")
        )
        matched = self._as_float(
            order.get("size_matched")
            or order.get("filled_size")
            or order.get("size_filled")
            or order.get("matched_size")
            or order.get("executed_size")
            or order.get("filled")
        )
        remaining = self._as_float(
            order.get("size_remaining")
            or order.get("remaining_size")
            or order.get("remaining")
            or order.get("unmatched_size")
        )
        return size, matched, remaining

    def _order_fill_price(self, payload) -> float | None:
        order = self._normalize_order_payload(payload)
        for key in (
            "avg_price",
            "average_price",
            "average_fill_price",
            "avg_fill_price",
            "filled_avg_price",
            "execution_price",
            "executed_price",
        ):
            px = self._as_float(order.get(key))
            if px is not None and px > 0:
                return px
        return None

    def _order_state(self, payload) -> tuple[str, str, float | None, float | None]:
        order = self._normalize_order_payload(payload)
        status_raw = str(
            order.get("status")
            or order.get("state")
            or order.get("order_status")
            or ""
        ).strip()
        status = status_raw.lower()
        size, matched, remaining = self._order_metrics(order)

        eps = 1e-9
        is_full_by_size = (
            size is not None
            and matched is not None
            and size > eps
            and matched >= (size - eps)
        ) or (
            remaining is not None
            and remaining <= eps
            and matched is not None
            and matched > eps
        )

        if status and any(x in status for x in ("cancel", "reject", "expire", "fail", "error")):
            if matched is not None and matched > eps:
                return "partial_terminal", status_raw, matched, size
            return "terminal_unfilled", status_raw, matched, size
        if status and any(x in status for x in ("filled", "executed", "complete")):
            return "filled", status_raw, matched, size

        if order.get("is_filled") is True:
            return "filled", status_raw or "is_filled", matched, size

        if is_full_by_size:
            return "filled", status_raw or "size_match", matched, size
        if matched is not None and matched > eps:
            return "partial", status_raw or "partial", matched, size

        return "open", status_raw or "open", matched, size

    def _wait_for_fill(
        self, order_id: str
    ) -> tuple[str, str, float | None, float | None, str, float | None]:
        timeout = max(1.0, float(self.cfg.live_entry_fill_timeout_sec))
        poll = max(0.2, float(self.cfg.live_entry_fill_poll_sec))
        deadline = time.time() + timeout

        last_state = "open"
        last_matched = None
        last_size = None
        last_err = ""
        last_fill_price = None
        while time.time() < deadline:
            try:
                payload = self.client.get_order(order_id)
                state, state_desc, matched, size = self._order_state(payload)
                fill_price = self._order_fill_price(payload)
                if state_desc:
                    last_state = state_desc
                if matched is not None:
                    last_matched = matched
                if size is not None:
                    last_size = size
                if fill_price is not None:
                    last_fill_price = fill_price
                if state == "filled":
                    return "filled", last_state, last_matched, last_size, "", last_fill_price
                if state == "partial_terminal":
                    return "partial", last_state, last_matched, last_size, "", last_fill_price
                if state == "terminal_unfilled":
                    return "terminal_unfilled", last_state, last_matched, last_size, "", last_fill_price
            except Exception as e:
                last_err = str(e)
            time.sleep(poll)

        timeout_state = f"timeout({int(timeout)}s):{last_state}"
        if last_matched is not None and last_matched > 0:
            return "partial", timeout_state, last_matched, last_size, last_err, last_fill_price
        return "unfilled", timeout_state, last_matched, last_size, last_err, last_fill_price

    @staticmethod
    def _allowances_positive(allowances) -> bool:
        if not isinstance(allowances, dict) or not allowances:
            return False
        for v in allowances.values():
            try:
                if int(str(v)) > 0:
                    return True
            except Exception:
                continue
        return False

    def _get_conditional_balance_allowance(self, token_id: str) -> dict | None:
        try:
            from py_clob_client.clob_types import BalanceAllowanceParams, AssetType

            params = BalanceAllowanceParams(
                asset_type=AssetType.CONDITIONAL,
                token_id=token_id,
                signature_type=-1,
            )
            data = self.client.get_balance_allowance(params)
            return data if isinstance(data, dict) else None
        except Exception:
            return None

    def _ensure_conditional_allowance(self, token_id: str, force: bool = False):
        if (not force) and token_id in self._conditional_allowance_checked:
            return
        self._conditional_allowance_checked.add(token_id)

        try:
            from py_clob_client.clob_types import BalanceAllowanceParams, AssetType

            params = BalanceAllowanceParams(
                asset_type=AssetType.CONDITIONAL,
                token_id=token_id,
                signature_type=-1,
            )
            data = self.client.get_balance_allowance(params)
            allowances = data.get("allowances") if isinstance(data, dict) else None
            if force or not self._allowances_positive(allowances):
                self.client.update_balance_allowance(params)
                time.sleep(0.3)
        except Exception:
            pass

    @staticmethod
    def _is_not_enough_balance_or_allowance(status: str) -> bool:
        s = (status or "").lower()
        return (
            "not enough balance" in s
            or "allowance" in s
            or "insufficient" in s
        )

    def _estimate_shares_from_balance_raw(self, balance_raw: str, expected: float) -> float | None:
        bal = self._as_float(balance_raw)
        if bal is None:
            return None
        if bal <= 0:
            return 0.0

        # Different endpoints may return raw token units (scaled) or float shares.
        divisors = [1.0, 10.0, 100.0, 1_000.0, 10_000.0, 100_000.0, 1_000_000.0, 1_000_000_000_000_000_000.0]
        candidates = [bal / d for d in divisors if (bal / d) > 0]
        if not candidates:
            return None

        ref = max(0.01, float(expected))
        near = [c for c in candidates if c <= ref * 3.0]
        pool = near if near else candidates
        best = min(pool, key=lambda c: abs(c - ref))
        return max(0.0, best)

    @staticmethod
    def _build_exit_attempt_sizes(initial_size: float, min_size: float | None) -> list[float]:
        if initial_size <= 0:
            return []
        scales = [1.0, 0.85, 0.7, 0.55, 0.45, 0.35, 0.25, 0.18]
        out: list[float] = []
        for mul in scales:
            s = round(max(0.01, initial_size * mul), 2)
            if min_size is not None and s < min_size:
                continue
            if s not in out:
                out.append(s)
        return out

    def _get_min_order_size(self, token_id: str) -> float | None:
        cached = self._min_size_cache.get(token_id)
        if cached is not None:
            return cached

        try:
            ob = self.client.get_order_book(token_id)
            raw = getattr(ob, "min_order_size", None)
            if raw is None and isinstance(ob, dict):
                raw = ob.get("min_order_size")
            min_size = self._as_float(raw)
            if min_size is not None and min_size > 0:
                self._min_size_cache[token_id] = min_size
                return min_size
        except Exception:
            return None
        return None


class TradingEngine:
    def __init__(self, mode: TradeMode, cfg: TradingConfig, runtime_env: dict[str, str] | None = None,
                 timeframe: str = "15m", on_entry_callback=None, on_exit_callback=None, on_error_callback=None):
        self.mode = mode
        self.cfg = cfg
        self.timeframe = timeframe
        self._on_entry = on_entry_callback
        self._on_exit = on_exit_callback
        self._on_error = on_error_callback
        self.state = TraderState()
        self.executor = LiveExecutor(cfg, runtime_env=runtime_env) if mode == TradeMode.LIVE else PaperExecutor(cfg)
        self.control_file = os.path.abspath(cfg.control_file)
        self._runtime_cmd_queue: SimpleQueue[str] = SimpleQueue()
        self._init_control_file()

    def _init_control_file(self):
        with open(self.control_file, "w", encoding="utf-8") as f:
            f.write("")

    def _approval_beep(self):
        if not self.cfg.approval_beep_enabled:
            return
        did_play = False
        try:
            sys.stdout.write("\a")
            sys.stdout.flush()
            did_play = True
        except Exception:
            pass

        cmd = self.cfg.approval_sound_command.strip()
        if cmd:
            try:
                subprocess.Popen(
                    cmd,
                    shell=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            except Exception:
                pass
            return

        if sys.platform == "darwin" and shutil.which("afplay"):
            try:
                subprocess.Popen(
                    ["afplay", "/System/Library/Sounds/Glass.aiff"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                return
            except Exception:
                pass

        if not did_play and shutil.which("printf"):
            try:
                subprocess.Popen(
                    ["printf", "\a"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            except Exception:
                pass

    def control_help(self) -> str:
        return (
            f"control file: {self.control_file} | commands: "
            "status, approve, reject, close, reset, help "
            "| terminal aliases: y=approve n=reject s=status c=close r=reset h=help"
        )

    @staticmethod
    def _normalize_command(cmd: str) -> str:
        aliases = {
            "y": "approve",
            "yes": "approve",
            "a": "approve",
            "n": "reject",
            "no": "reject",
            "s": "status",
            "c": "close",
            "x": "close",
            "r": "reset",
            "h": "help",
            "?": "help",
        }
        clean = cmd.strip().lower()
        return aliases.get(clean, clean)

    def enqueue_command(self, cmd: str) -> bool:
        normalized = self._normalize_command(cmd)
        if not normalized:
            return False
        self._runtime_cmd_queue.put(normalized)
        return True

    def _drain_runtime_commands(self) -> list[str]:
        out: list[str] = []
        while True:
            try:
                out.append(self._runtime_cmd_queue.get_nowait())
            except Empty:
                return out

    def _read_control_commands(self) -> list[str]:
        try:
            with open(self.control_file, "r+", encoding="utf-8") as f:
                raw = f.read()
                if not raw.strip():
                    return []
                f.seek(0)
                f.truncate(0)
        except FileNotFoundError:
            self._init_control_file()
            return []

        out = []
        for line in raw.splitlines():
            cmd = self._normalize_command(line)
            if not cmd or cmd.startswith("#"):
                continue
            out.append(cmd)
        return out

    def _rollover_day(self):
        day_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if self.state.day_key != day_key:
            self.state.day_key = day_key
            self.state.trades_today = 0

    def _decision_signature(self, decision: TradeDecision) -> str:
        return f"{decision.side}|{decision.token_id}|{decision.price:.3f}"

    def _decision_key(self, decision: TradeDecision) -> str:
        return f"{decision.side}|{decision.token_id}"

    def _build_decision(self, feed_state) -> tuple[TradeDecision | None, str]:
        """Build a trade decision or return (None, skip_reason)."""
        if not feed_state.mid or not feed_state.klines:
            return None, "no feed data (mid/klines)"
        if not feed_state.pm_up_id or not feed_state.pm_dn_id:
            return None, "no PM token IDs"
        if feed_state.pm_up is None or feed_state.pm_dn is None:
            return None, "no PM prices"

        bias = ind.bias_score(
            feed_state.bids, feed_state.asks, feed_state.mid, feed_state.trades, feed_state.klines
        )
        obi = ind.obi(feed_state.bids, feed_state.asks, feed_state.mid) if feed_state.mid else 0.0

        if abs(bias) < self.cfg.min_abs_bias:
            return None, f"bias too low ({bias:+.1f} < ±{self.cfg.min_abs_bias})"
        if abs(obi) < self.cfg.min_abs_obi:
            return None, f"OBI too low ({obi:+.3f} < ±{self.cfg.min_abs_obi})"

        if bias > 0:
            side = "Up"
            token_id = feed_state.pm_up_id
            price = feed_state.pm_up
        else:
            side = "Down"
            token_id = feed_state.pm_dn_id
            price = feed_state.pm_dn

        if price is None or not (self.cfg.min_price <= price <= self.cfg.max_price):
            px_str = f"{price:.3f}" if price is not None else "N/A"
            return None, f"PM price out of range ({px_str} not in [{self.cfg.min_price}, {self.cfg.max_price}])"

        # PM spread gate
        bid = feed_state.pm_up_bid if side == "Up" else feed_state.pm_dn_bid
        if bid is not None and price is not None and bid > 0:
            spread_pct = (price - bid) / price * 100.0
            if spread_pct > config.PM_MAX_SPREAD_PCT:
                return None, f"PM spread too wide ({spread_pct:.1f}% > {config.PM_MAX_SPREAD_PCT}%)"

        # Binance-PM divergence guard
        rsi_val = ind.rsi(feed_state.klines)
        vwap_val = ind.vwap(feed_state.klines)
        fair_up, fair_dn = ind.pm_fair_value(feed_state.mid, feed_state.klines, rsi_val, vwap_val)
        fair = fair_up if side == "Up" else fair_dn
        if fair > 0 and price > fair * (1 + config.PM_DIVERGENCE_MAX_PCT / 100.0):
            return None, f"PM-Binance divergence: PM {side} price={price:.3f} > fair={fair:.3f} +{config.PM_DIVERGENCE_MAX_PCT}%"

        # PM depth quality gate — block entry if insufficient liquidity
        if config.PM_MIN_DEPTH_USD > 0:
            target_token_id = feed_state.pm_up_id if side == "Up" else feed_state.pm_dn_id
            target_price = price
            if target_token_id and target_price > 0:
                try:
                    import httpx

                    url = f"https://clob.polymarket.com/book?token_id={target_token_id}"
                    resp = httpx.get(url, timeout=3.0)
                    if resp.is_success:
                        book = resp.json()
                        asks = book.get("asks", [])
                        depth_usd = sum(
                            float(a.get("price", 0)) * float(a.get("size", 0))
                            for a in asks
                            if float(a.get("price", 0)) <= target_price * 1.02
                        )
                        if depth_usd < config.PM_MIN_DEPTH_USD:
                            return None, (
                                f"PM depth too thin (${depth_usd:.1f} < "
                                f"${config.PM_MIN_DEPTH_USD:.0f})"
                            )
                except Exception:
                    # Fail-open on transient depth endpoint issues.
                    pass

        computed_size = self._compute_position_size(bias)
        reason = f"bias={bias:+.1f}, obi={obi:+.3f}, px={price:.3f}"
        return TradeDecision(
            side=side,
            token_id=token_id,
            price=price,
            bias=bias,
            obi=obi,
            reason=reason,
            ts=time.time(),
            size_usd=computed_size,
        ), ""

    def _allowed(self) -> tuple[bool, str]:
        self._rollover_day()
        now = time.time()

        # Window safety: block entries near window close
        import window as window_mod
        winfo = window_mod.get_window_info(self.timeframe)
        if winfo.entry_blocked:
            buf = window_mod.WINDOW_ENTRY_BUFFER.get(self.timeframe, 0)
            return False, f"window_entry_blocked: {winfo.remaining_sec}s remaining (buffer={buf}s)"

        # Circuit breaker
        if self.state.circuit_breaker_active:
            return False, f"circuit_breaker: {self.state.circuit_breaker_reason}"

        if self.state.open_position is not None:
            return False, f"position already open: {self.state.open_position.side}"
        if self.state.trades_today >= self.cfg.max_trades_per_day:
            return False, "daily trade limit reached"
        if self.state.last_trade_ts and (now - self.state.last_trade_ts) < self.cfg.cooldown_sec:
            return False, "cooldown active"
        return True, ""

    def _maybe_log_skip(self, log, reason: str):
        now = time.time()
        should_log = (
            reason != self.state.last_skip_reason
            or (now - self.state.last_skip_log_ts) >= 60
        )
        # Global throttle: at most one skip log every 30 seconds
        if should_log and (now - self.state.last_skip_log_ts) < 30:
            should_log = False
        if should_log:
            log(f"  [TRADER] skip: {reason}")
            self.state.last_skip_reason = reason
            self.state.last_skip_log_ts = now

    def _mark_price(self, feed_state, side: str) -> float | None:
        if side == "Up":
            return feed_state.pm_up
        return feed_state.pm_dn

    def _compute_position_size(self, bias: float) -> float:
        """Scale position size based on signal strength (bias magnitude)."""
        if not self.cfg.dynamic_sizing_enabled:
            return self.cfg.size_usd
        abs_bias = abs(bias)
        if abs_bias <= self.cfg.sizing_bias_floor:
            return self.cfg.min_size_usd
        if abs_bias >= self.cfg.sizing_bias_ceiling:
            return self.cfg.max_size_usd
        ratio = (abs_bias - self.cfg.sizing_bias_floor) / (
            self.cfg.sizing_bias_ceiling - self.cfg.sizing_bias_floor
        )
        return self.cfg.min_size_usd + ratio * (self.cfg.max_size_usd - self.cfg.min_size_usd)

    def _current_bias(self, feed_state) -> float | None:
        if not feed_state.mid or not feed_state.klines:
            return None
        return ind.bias_score(
            feed_state.bids, feed_state.asks, feed_state.mid, feed_state.trades, feed_state.klines
        )

    def _format_duration(self, secs: float) -> str:
        s = int(max(0, secs))
        mm = s // 60
        ss = s % 60
        return f"{mm:02d}:{ss:02d}"

    def _log_status(self, feed_state, log):
        pending = "yes" if self.state.pending_decision else "no"
        open_pos = self.state.open_position
        open_side = open_pos.side if open_pos else "none"
        log(
            "  [TRADER] status | "
            f"mode={self.mode.value} open={open_side} order={self.state.open_order_id or '-'} "
            f"trades_today={self.state.trades_today}/{self.cfg.max_trades_per_day} "
            f"pending={pending} approval_armed={self.state.approval_armed}"
        )
        if open_pos:
            mark = self._mark_price(feed_state, open_pos.side)
            hold = time.time() - open_pos.entry_ts
            if mark is not None and open_pos.entry_price > 0:
                pnl_usd, pnl_pct = pnl_with_fees(open_pos.entry_price, mark, open_pos.shares)
                log(
                    "  [TRADER] open | "
                    f"token={open_pos.token_id[:10]}.. "
                    f"entry={open_pos.entry_price:.3f} mark={mark:.3f} "
                    f"shares={open_pos.shares:.2f} "
                    f"pnl={pnl_pct:+.1f}% (${pnl_usd:+.2f}) hold={self._format_duration(hold)}"
                )
            else:
                log(
                    "  [TRADER] open | "
                    f"token={open_pos.token_id[:10]}.. "
                    f"entry={open_pos.entry_price:.3f} shares={open_pos.shares:.2f} "
                    f"hold={self._format_duration(hold)}"
                )
        if self.state.pending_decision is not None:
            d = self.state.pending_decision
            log(
                "  [TRADER] pending | "
                f"{d.side} @ {d.price:.3f} | {d.reason}"
            )

    def _clear_position_state(self):
        self.state.open_position = None
        self.state.open_side = None
        self.state.open_order_id = None

    @staticmethod
    def _record_dict(rec: TradeRecord) -> dict:
        payload = asdict(rec)
        payload["is_execution"] = is_execution_status(rec.action, rec.status)
        if rec.shares is not None and rec.price is not None:
            payload["effective_size_usd"] = abs(float(rec.shares) * float(rec.price))
        else:
            payload["effective_size_usd"] = None
        return payload

    def snapshot(self) -> dict:
        return {
            "mode": self.mode.value,
            "trades_today": self.state.trades_today,
            "max_trades_per_day": self.cfg.max_trades_per_day,
            "open_position": asdict(self.state.open_position) if self.state.open_position else None,
            "open_order_id": self.state.open_order_id,
            "pending_decision": asdict(self.state.pending_decision) if self.state.pending_decision else None,
            "pending_key": self.state.pending_key,
            "approval_armed": self.state.approval_armed,
            "last_trade_ts": self.state.last_trade_ts,
            "force_close_requested": self.state.force_close_requested,
            "next_exit_attempt_ts": self.state.next_exit_attempt_ts,
            "trades": [self._record_dict(t) for t in self.state.trades[-200:]],
            "session_stats": {
                "total_trades": self.state.session_stats.total_trades,
                "wins": self.state.session_stats.wins,
                "losses": self.state.session_stats.losses,
                "win_rate": self.state.session_stats.win_rate,
                "net_pnl_usd": self.state.session_stats.net_pnl_usd,
                "avg_win_usd": self.state.session_stats.avg_win_usd,
                "avg_loss_usd": self.state.session_stats.avg_loss_usd,
                "profit_factor": min(self.state.session_stats.profit_factor, 99.99),
                "best_trade_pnl_usd": self.state.session_stats.best_trade_pnl_usd,
                "worst_trade_pnl_usd": self.state.session_stats.worst_trade_pnl_usd,
                "fill_ratio": round(self.state.session_stats.fill_ratio, 3),
                "avg_fill_time_ms": round(self.state.session_stats.avg_fill_time_ms, 1),
                "avg_slippage_bps": round(self.state.session_stats.avg_slippage_bps, 2),
            },
            "cfg": {
                "size_usd": self.cfg.size_usd,
                "min_abs_bias": self.cfg.min_abs_bias,
                "min_abs_obi": self.cfg.min_abs_obi,
                "min_price": self.cfg.min_price,
                "max_price": self.cfg.max_price,
                "cooldown_sec": self.cfg.cooldown_sec,
                "max_trades_per_day": self.cfg.max_trades_per_day,
                "eval_interval_sec": self.cfg.eval_interval_sec,
                "tp_pct": self.cfg.tp_pct,
                "sl_pct": self.cfg.sl_pct,
                "max_hold_sec": self.cfg.max_hold_sec,
                "reverse_exit_enabled": self.cfg.reverse_exit_enabled,
                "reverse_exit_bias": self.cfg.reverse_exit_bias,
                "live_manual_approval": self.cfg.live_manual_approval,
                "live_entry_require_fill": self.cfg.live_entry_require_fill,
                "live_entry_fill_timeout_sec": self.cfg.live_entry_fill_timeout_sec,
                "live_entry_fill_poll_sec": self.cfg.live_entry_fill_poll_sec,
            },
        }

    def _persist_execution(self, rec: TradeRecord):
        path = (self.cfg.executions_log_file or "").strip()
        if not path:
            return
        payload = {
            "ts": rec.ts_iso,
            "action": rec.action,
            "mode": rec.mode,
            "coin": rec.coin,
            "timeframe": rec.timeframe,
            "side": rec.side,
            "token_id": rec.token_id,
            "price": rec.price,
            "size_usd": rec.size_usd,
            "shares": rec.shares,
            "status": rec.status,
            "reason": rec.reason,
            "order_id": rec.order_id,
            "pnl_usd": rec.pnl_usd,
            "pnl_pct": rec.pnl_pct,
            "fill_elapsed_ms": rec.fill_elapsed_ms,
            "slippage_bps": rec.slippage_bps,
        }
        try:
            with open(path, "a", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=False) + "\n")
        except Exception:
            pass

    def preflight(self) -> dict:
        if self.mode != TradeMode.LIVE:
            return {"ok": True, "checks": [{"name": "mode", "status": "ok", "detail": self.mode.value}]}
        if not isinstance(self.executor, LiveExecutor):
            return {"ok": False, "checks": [{"name": "executor", "status": "error", "detail": "live executor missing"}]}
        return self.executor.preflight()

    def _reset_state(self, log):
        cancelled = False
        if self.state.open_order_id:
            cancelled = self.executor.cancel(self.state.open_order_id)
        self._clear_position_state()
        self.state.pending_decision = None
        self.state.pending_sig = ""
        self.state.pending_key = ""
        self.state.approval_armed = False
        self.state.force_close_requested = False
        self.state.next_exit_attempt_ts = 0.0
        if cancelled:
            log("  [TRADER] reset: open order cancel requested and local state cleared")
        else:
            log("  [TRADER] reset: local state cleared")

    def process_control_commands(self, feed_state, log):
        cmds = self._read_control_commands() + self._drain_runtime_commands()
        for cmd in cmds:
            if cmd == "status":
                self._log_status(feed_state, log)
            elif cmd == "help":
                log(f"  [TRADER] {self.control_help()}")
            elif cmd == "approve":
                if self.state.pending_decision is None:
                    log("  [TRADER] approve ignored: no pending trade")
                else:
                    if self.state.approval_armed:
                        continue
                    self.state.approval_armed = True
                    log("  [TRADER] approval armed for pending trade")
            elif cmd == "reject":
                self.state.pending_decision = None
                self.state.pending_sig = ""
                self.state.pending_key = ""
                self.state.approval_armed = False
                log("  [TRADER] pending trade rejected")
            elif cmd == "close":
                if self.state.open_position is None:
                    log("  [TRADER] close ignored: no open position")
                else:
                    self.state.force_close_requested = True
                    log("  [TRADER] manual close requested")
            elif cmd in {"reset", "clear"}:
                self._reset_state(log)
            else:
                log(f'  [TRADER] unknown command "{cmd}" (use help)')

    def _entry_success(self, status: str) -> bool:
        if self.mode == TradeMode.LIVE:
            if self.cfg.live_entry_require_fill:
                return status in {"filled", "partial_filled", "partial_filled_cancelled", "partial_filled_open"}
            return status in {
                "posted",
                "filled",
                "partial_filled",
                "partial_filled_cancelled",
                "partial_filled_open",
            }
        return status == "paper"

    def _track_entry_fill_metrics(self, rec: TradeRecord):
        if rec.mode != TradeMode.LIVE.value:
            return

        stats = self.state.session_stats
        stats.fill_attempts += 1
        status_lower = (rec.status or "").lower()
        if "partial" in status_lower:
            stats.fill_partial += 1
        elif status_lower == "filled":
            stats.fill_successes += 1

        has_fill = "filled" in status_lower and "unfilled" not in status_lower
        if rec.fill_elapsed_ms is not None and rec.fill_elapsed_ms >= 0 and has_fill:
            stats.total_fill_time_ms += rec.fill_elapsed_ms
            stats.fill_count_timed += 1

        if rec.slippage_bps is not None:
            stats.total_slippage_bps += rec.slippage_bps
            stats.slippage_count += 1

    def _exit_success(self, status: str) -> bool:
        return status in {"paper_exit", "exit_filled", "exit_dust_cleared"}

    def maybe_open_position(self, feed_state, coin: str, timeframe: str, log):
        if (
            self.mode == TradeMode.LIVE
            and self.cfg.live_manual_approval
            and self.state.pending_decision is not None
            and self.state.approval_armed
        ):
            age = time.time() - self.state.pending_decision.ts
            if age <= self.cfg.pending_approval_ttl_sec:
                ok, reason = self._allowed()
                if not ok:
                    self._maybe_log_skip(log, reason)
                    return
                decision = self.state.pending_decision
                self.state.pending_decision = None
                self.state.pending_sig = ""
                self.state.pending_key = ""
                self.state.approval_armed = False
                rec = self.executor.execute_entry(decision, coin, timeframe)
                self.state.trades.append(rec)
                if len(self.state.trades) > 500:
                    self.state.trades = self.state.trades[-500:]
                self._track_entry_fill_metrics(rec)

                if self._entry_success(rec.status):
                    self.state.last_trade_ts = time.time()
                    self.state.trades_today += 1
                    shares = (
                        rec.shares
                        if rec.shares is not None and rec.shares > 0
                        else rec.size_usd / max(rec.price, 0.01)
                    )
                    # Use current market price as entry_price (not stale decision price)
                    # This prevents instant SL trigger when fill takes time
                    mark = self._mark_price(feed_state, rec.side)
                    effective_entry = mark if mark is not None else rec.price
                    self.state.open_position = OpenPosition(
                        side=rec.side,
                        token_id=rec.token_id,
                        entry_price=effective_entry,
                        size_usd=rec.size_usd,
                        shares=shares,
                        entry_ts=time.time(),
                        entry_bias=decision.bias,
                        entry_obi=decision.obi,
                        entry_order_id=rec.order_id,
                    )
                    self.state.open_side = rec.side
                    self.state.open_order_id = rec.order_id
                    self._persist_execution(rec)
                    if effective_entry != rec.price:
                        log(f"  [TRADER] entry_price adjusted: order={rec.price:.3f} → mark={effective_entry:.3f}")
                    if self._on_entry:
                        try:
                            self._on_entry(rec)
                        except Exception as _cb_err:
                            print(f"  [TRADER] entry callback error: {_cb_err}")
                else:
                    status_lower = rec.status.lower()
                    if "balance" in status_lower or "allowance" in status_lower:
                        self.state.last_trade_ts = time.time() + 600 - self.cfg.cooldown_sec
                        log("  [TRADER] balance/allowance error — pausing entries for 10 min")
                    else:
                        self.state.last_trade_ts = time.time()

                log(
                    "  [TRADER] "
                    f"{rec.mode.upper()} {rec.status} {rec.action} | {rec.coin} {rec.timeframe} | "
                    f"{rec.side} @ {rec.price:.3f} | ${rec.size_usd:.2f} | {rec.reason}"
                    + (f" | order={rec.order_id}" if rec.order_id else "")
                )
                return

            self.state.pending_decision = None
            self.state.pending_sig = ""
            self.state.pending_key = ""
            self.state.approval_armed = False
            log(
                "  [TRADER] pending approval expired: "
                f">{self.cfg.pending_approval_ttl_sec}s without execution"
            )

        decision, skip_reason = self._build_decision(feed_state)
        if decision is None:
            if skip_reason:
                self._maybe_log_skip(log, skip_reason)
            return

        ok, reason = self._allowed()
        if not ok:
            self._maybe_log_skip(log, reason)
            return

        if self.mode == TradeMode.LIVE and self.cfg.live_manual_approval:
            sig = self._decision_signature(decision)
            key = self._decision_key(decision)
            if self.state.pending_decision is None or key != self.state.pending_key:
                self.state.pending_decision = decision
                self.state.pending_sig = sig
                self.state.pending_key = key
                self.state.approval_armed = False
                log(
                    "  [TRADER] pending live trade | "
                    f"{decision.side} @ {decision.price:.3f} | {decision.reason}"
                )
                log(
                    "  [TRADER] waiting approval: "
                    f'echo "approve" > {self.control_file}'
                )
                self._approval_beep()
                return

            # Keep pending decision fresh at the latest price while retaining approval.
            self.state.pending_decision = decision
            self.state.pending_sig = sig

            if not self.state.approval_armed:
                return

            decision = self.state.pending_decision
            self.state.pending_decision = None
            self.state.pending_sig = ""
            self.state.pending_key = ""
            self.state.approval_armed = False

        rec = self.executor.execute_entry(decision, coin, timeframe)
        self.state.trades.append(rec)
        if len(self.state.trades) > 500:
            self.state.trades = self.state.trades[-500:]
        self._track_entry_fill_metrics(rec)

        if self._entry_success(rec.status):
            self.state.last_trade_ts = time.time()
            self.state.trades_today += 1
            shares = (
                rec.shares
                if rec.shares is not None and rec.shares > 0
                else rec.size_usd / max(rec.price, 0.01)
            )
            # Use current market price as entry_price (not stale decision price)
            # This prevents instant SL trigger when fill takes time
            mark = self._mark_price(feed_state, rec.side)
            effective_entry = mark if mark is not None else rec.price
            self.state.open_position = OpenPosition(
                side=rec.side,
                token_id=rec.token_id,
                entry_price=effective_entry,
                size_usd=rec.size_usd,
                shares=shares,
                entry_ts=time.time(),
                entry_bias=decision.bias,
                entry_obi=decision.obi,
                entry_order_id=rec.order_id,
            )
            self.state.open_side = rec.side
            self.state.open_order_id = rec.order_id
            self._persist_execution(rec)
            if effective_entry != rec.price:
                log(f"  [TRADER] entry_price adjusted: order={rec.price:.3f} → mark={effective_entry:.3f}")
            if self._on_entry:
                try:
                    self._on_entry(rec)
                except Exception as _cb_err:
                    print(f"  [TRADER] entry callback error: {_cb_err}")
        else:
            # Failed entry: apply cooldown to prevent rapid-fire retries.
            # For balance errors, use a long pause (10 min); otherwise use normal cooldown.
            status_lower = rec.status.lower()
            if "balance" in status_lower or "allowance" in status_lower:
                self.state.last_trade_ts = time.time() + 600 - self.cfg.cooldown_sec
                log("  [TRADER] balance/allowance error — pausing entries for 10 min")
            else:
                self.state.last_trade_ts = time.time()

        log(
            "  [TRADER] "
            f"{rec.mode.upper()} {rec.status} {rec.action} | {rec.coin} {rec.timeframe} | "
            f"{rec.side} @ {rec.price:.3f} | ${rec.size_usd:.2f} | {rec.reason}"
            + (f" | order={rec.order_id}" if rec.order_id else "")
        )

    def _exit_trigger(self, feed_state) -> tuple[str, float, float | None, float | None] | None:
        pos = self.state.open_position
        if pos is None:
            return None

        if self.state.force_close_requested:
            mark = self._mark_price(feed_state, pos.side)
            self.state.force_close_requested = False
            if mark is None:
                mark = pos.entry_price
            pnl_usd, pnl_pct = pnl_with_fees(pos.entry_price, mark, pos.shares)
            return "manual_close", mark, pnl_usd, pnl_pct

        # Window close safety (second highest priority after manual close)
        import window as window_mod
        winfo = window_mod.get_window_info(self.timeframe)
        if winfo.exit_forced:
            mark = self._mark_price(feed_state, pos.side)
            if mark is None:
                mark = pos.entry_price
            pnl_usd, pnl_pct = pnl_with_fees(pos.entry_price, mark, pos.shares)
            return (
                f"window_close: {winfo.remaining_sec}s remaining in {self.timeframe} window",
                mark,
                pnl_usd,
                pnl_pct,
            )

        mark = self._mark_price(feed_state, pos.side)
        if mark is None:
            return None

        pnl_usd, pnl_pct = pnl_with_fees(pos.entry_price, mark, pos.shares)
        hold_sec = time.time() - pos.entry_ts

        if pos.high_water_mark_price <= 0:
            pos.high_water_mark_price = pos.entry_price
        if mark > pos.high_water_mark_price:
            pos.high_water_mark_price = mark

        # Trailing stop check
        if self.cfg.trailing_stop_enabled and pos.entry_price > 0:
            hwm_pnl_usd, hwm_pnl_pct = pnl_with_fees(pos.entry_price, pos.high_water_mark_price, pos.shares)
            if hwm_pnl_pct >= self.cfg.trailing_stop_activation_pct:
                trail_level = pos.high_water_mark_price * (1 - self.cfg.trailing_stop_distance_pct / 100.0)
                if mark <= trail_level:
                    return (
                        f"trailing_stop: mark={mark:.3f} <= trail={trail_level:.3f} "
                        f"(hwm={pos.high_water_mark_price:.3f}, peak_pnl={hwm_pnl_pct:+.1f}%)",
                        mark, pnl_usd, pnl_pct,
                    )

        if not self.cfg.auto_exit_enabled:
            return None

        if pnl_pct is not None and pnl_pct >= self.cfg.tp_pct:
            return f"tp_hit {pnl_pct:+.1f}% >= {self.cfg.tp_pct:.1f}%", mark, pnl_usd, pnl_pct
        if pnl_pct is not None and pnl_pct <= -self.cfg.sl_pct:
            return f"sl_hit {pnl_pct:+.1f}% <= -{self.cfg.sl_pct:.1f}%", mark, pnl_usd, pnl_pct
        if hold_sec >= self.cfg.max_hold_sec:
            return (
                f"time_stop hold={self._format_duration(hold_sec)} >= {self._format_duration(self.cfg.max_hold_sec)}",
                mark,
                pnl_usd,
                pnl_pct,
            )
        if self.cfg.reverse_exit_enabled:
            bias = self._current_bias(feed_state)
            if bias is not None:
                if pos.side == "Up" and bias <= -self.cfg.reverse_exit_bias:
                    return (
                        f"reverse_bias {bias:+.1f} <= -{self.cfg.reverse_exit_bias:.1f}",
                        mark,
                        pnl_usd,
                        pnl_pct,
                    )
                if pos.side == "Down" and bias >= self.cfg.reverse_exit_bias:
                    return (
                        f"reverse_bias {bias:+.1f} >= +{self.cfg.reverse_exit_bias:.1f}",
                        mark,
                        pnl_usd,
                        pnl_pct,
                    )
        return None

    def maybe_close_position(self, feed_state, coin: str, timeframe: str, log):
        pos = self.state.open_position
        if pos is None:
            return

        now = time.time()
        if now < self.state.next_exit_attempt_ts:
            return

        trigger = self._exit_trigger(feed_state)
        if trigger is None:
            return

        reason, exit_price, pnl_usd, pnl_pct = trigger
        rec = self.executor.close_position(
            pos,
            exit_price=exit_price,
            reason=reason,
            coin=coin,
            timeframe=timeframe,
            pnl_usd=pnl_usd,
            pnl_pct=pnl_pct,
        )
        self.state.trades.append(rec)
        if len(self.state.trades) > 500:
            self.state.trades = self.state.trades[-500:]

        if self._exit_success(rec.status):
            self._clear_position_state()
            self.state.last_trade_ts = time.time()
            # Circuit breaker tracking
            if rec.pnl_usd is not None:
                self.state.session_pnl_usd += rec.pnl_usd
                # Session stats tracking
                stats = self.state.session_stats
                stats.total_trades += 1
                stats.net_pnl_usd += rec.pnl_usd
                stats.pnl_series.append(rec.pnl_usd)
                if len(stats.pnl_series) > 1000:
                    stats.pnl_series = stats.pnl_series[-1000:]
                if rec.pnl_usd > 0:
                    stats.wins += 1
                else:
                    stats.losses += 1
                if rec.pnl_usd > stats.best_trade_pnl_usd:
                    stats.best_trade_pnl_usd = rec.pnl_usd
                if rec.pnl_usd < stats.worst_trade_pnl_usd:
                    stats.worst_trade_pnl_usd = rec.pnl_usd
                if rec.pnl_usd < 0:
                    self.state.consecutive_losses += 1
                else:
                    self.state.consecutive_losses = 0
                # Check circuit breaker triggers
                if self.state.session_pnl_usd <= -self.cfg.max_daily_loss_usd:
                    self.state.circuit_breaker_active = True
                    self.state.circuit_breaker_reason = f"daily loss ${self.state.session_pnl_usd:.2f} <= -${self.cfg.max_daily_loss_usd:.2f}"
                elif self.state.consecutive_losses >= self.cfg.max_consecutive_losses:
                    self.state.circuit_breaker_active = True
                    self.state.circuit_breaker_reason = f"{self.state.consecutive_losses} consecutive losses"
            self.state.next_exit_attempt_ts = 0.0
            self._persist_execution(rec)
            if self._on_exit:
                try:
                    self._on_exit(rec)
                except Exception as _cb_err:
                    print(f"  [TRADER] exit callback error: {_cb_err}")
        else:
            if rec.status in {"exit_partial_cancelled", "exit_partial_open"}:
                self._persist_execution(rec)
                if self._on_exit:
                    try:
                        self._on_exit(rec)
                    except Exception as _cb_err:
                        print(f"  [TRADER] exit callback error: {_cb_err}")
                # Keep position open and shrink tracked shares by matched amount.
                if rec.shares is not None and rec.shares > 0:
                    pos.shares = max(0.0, pos.shares - rec.shares)
                min_size = None
                if self.mode == TradeMode.LIVE and isinstance(self.executor, LiveExecutor):
                    min_size = self.executor._get_min_order_size(pos.token_id)
                if min_size is not None and pos.shares < min_size:
                    self._clear_position_state()
                    self.state.last_trade_ts = time.time()
                    self.state.next_exit_attempt_ts = 0.0
                    log(
                        "  [TRADER] partial exit residual below min order size; "
                        f"position cleared (remaining={pos.shares:.4f}, min_size={min_size:.2f})"
                    )
                    return
                self.state.next_exit_attempt_ts = time.time() + 2
                if pos.shares <= 0.02:
                    self._clear_position_state()
                    log("  [TRADER] exit residual below dust threshold; position cleared")
                    return
                log(f"  [TRADER] partial exit matched; remaining_shares={pos.shares:.4f}")
                return

            # Shorter retry for balance settlement race condition (tokens not yet credited)
            if rec.status == "exit_no_token_balance":
                self.state.next_exit_attempt_ts = time.time() + 3
            elif rec.status == "exit_blocked_min_size":
                self.state.next_exit_attempt_ts = time.time() + max(30, self.cfg.exit_retry_backoff_sec)
            else:
                self.state.next_exit_attempt_ts = time.time() + self.cfg.exit_retry_backoff_sec

        pnl_txt = ""
        if rec.pnl_pct is not None and rec.pnl_usd is not None:
            pnl_txt = f" | pnl={rec.pnl_pct:+.1f}% (${rec.pnl_usd:+.2f})"

        log(
            "  [TRADER] "
            f"{rec.mode.upper()} {rec.status} {rec.action} | {rec.coin} {rec.timeframe} | "
            f"{rec.side} @ {rec.price:.3f} | ${rec.size_usd:.2f} | {rec.reason}"
            + pnl_txt
            + (f" | order={rec.order_id}" if rec.order_id else "")
        )


def _execute_complete_set_arb(
    executor: "LiveExecutor",
    up_token_id: str,
    dn_token_id: str,
    up_price: float,
    dn_price: float,
    max_size_usd: float,
    cfg: TradingConfig,
    log,
) -> dict:
    """Execute a complete-set arbitrage: buy both UP and DOWN tokens.

    Returns dict with keys: ok, up_status, dn_status, edge_pct, detail
    """
    from py_clob_client.clob_types import OrderArgs, OrderType
    from py_clob_client.order_builder.constants import BUY

    _ = cfg  # reserved for future cfg-dependent arb controls

    result = {
        "ok": False,
        "up_status": "not_started",
        "dn_status": "not_started",
        "edge_pct": 0.0,
        "detail": "",
    }

    # Calculate edge
    pm_sum = up_price + dn_price
    net_edge_pct = (1.0 - pm_sum - 2 * config.PM_TAKER_FEE) * 100
    result["edge_pct"] = net_edge_pct

    if net_edge_pct < config.PM_ARB_MIN_EDGE_PCT:
        result["detail"] = f"edge vanished: {net_edge_pct:.2f}% < {config.PM_ARB_MIN_EDGE_PCT}%"
        log(f"  [ARB] aborted: {result['detail']}")
        return result

    # Fetch available USDC balance before sizing
    available_usd = None
    try:
        from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
        params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=-1)
        data = executor.client.get_balance_allowance(params)
        bal_raw = str((data or {}).get("balance", "")).strip()
        if bal_raw:
            available_usd = float(bal_raw) / 1e6  # USDC has 6 decimals
    except Exception as e:
        log(f"  [ARB] WARNING: could not fetch balance: {e}")

    # Size calculation: min of max_size, config cap, and half the available balance
    # (need to buy 2 legs, so each leg gets at most balance/2 with 5% buffer)
    size_usd = max_size_usd
    if available_usd is not None:
        safe_per_leg = available_usd * 0.95 / 2.0
        size_usd = min(size_usd, safe_per_leg)
        log(f"  [ARB] balance=${available_usd:.2f}, safe_per_leg=${safe_per_leg:.2f}, size=${size_usd:.2f}")

    if size_usd < 1.0:
        result["detail"] = f"insufficient balance for arb: ${available_usd:.2f}" if available_usd is not None else "size too small"
        log(f"  [ARB] aborted: {result['detail']}")
        return result

    up_shares = round(size_usd / max(up_price, 0.01), 2)
    dn_shares = round(size_usd / max(dn_price, 0.01), 2)

    # Check min order sizes
    up_min = executor._get_min_order_size(up_token_id)
    dn_min = executor._get_min_order_size(dn_token_id)
    if up_min and up_shares < up_min:
        result["detail"] = f"UP shares {up_shares:.2f} < min {up_min:.2f}"
        log(f"  [ARB] aborted: {result['detail']}")
        return result
    if dn_min and dn_shares < dn_min:
        result["detail"] = f"DN shares {dn_shares:.2f} < min {dn_min:.2f}"
        log(f"  [ARB] aborted: {result['detail']}")
        return result

    # Leg 1: Buy UP token
    up_order_id = None
    try:
        signed = executor.client.create_order(
            OrderArgs(token_id=up_token_id, price=round(up_price, 3), size=up_shares, side=BUY)
        )
        resp = executor.client.post_order(signed, OrderType.GTC)
        up_order_id = resp.get("orderID") or resp.get("id")
        if up_order_id:
            result["up_status"] = "posted"
            log(f"  [ARB] UP leg posted: order_id={up_order_id}, {up_shares} shares @ {up_price:.3f}")
        else:
            result["up_status"] = "failed"
            result["detail"] = "UP order post returned no ID"
            log("  [ARB] UP leg failed: no order ID in response")
            return result
    except Exception as e:
        result["up_status"] = f"error: {e}"
        result["detail"] = f"UP order error: {e}"
        log(f"  [ARB] UP leg error: {e}")
        return result

    # Leg 2: Buy DOWN token
    dn_order_id = None
    try:
        signed = executor.client.create_order(
            OrderArgs(token_id=dn_token_id, price=round(dn_price, 3), size=dn_shares, side=BUY)
        )
        resp = executor.client.post_order(signed, OrderType.GTC)
        dn_order_id = resp.get("orderID") or resp.get("id")
        if dn_order_id:
            result["dn_status"] = "posted"
            log(f"  [ARB] DN leg posted: order_id={dn_order_id}, {dn_shares} shares @ {dn_price:.3f}")
        else:
            result["dn_status"] = "failed"
            result["detail"] = "DN order post returned no ID"
            # Try to cancel UP leg
            if up_order_id:
                try:
                    executor.client.cancel(up_order_id)
                    log("  [ARB] UP leg cancelled (rollback)")
                except Exception:
                    log("  [ARB] WARNING: UP leg cancel failed (rollback)")
            return result
    except Exception as e:
        result["dn_status"] = f"error: {e}"
        result["detail"] = f"DN order error: {e}"
        log(f"  [ARB] DN leg error: {e}")
        # Try to cancel UP leg
        if up_order_id:
            try:
                executor.client.cancel(up_order_id)
                log("  [ARB] UP leg cancelled (rollback)")
            except Exception:
                log("  [ARB] WARNING: UP leg cancel failed (rollback)")
        return result

    # Both legs posted successfully
    result["ok"] = True
    result["up_status"] = "posted"
    result["dn_status"] = "posted"
    result["detail"] = (
        f"Both legs posted. UP: {up_shares} shares @ {up_price:.3f}, "
        f"DN: {dn_shares} shares @ {dn_price:.3f}, "
        f"edge={net_edge_pct:+.2f}%"
    )
    log(f"  [ARB] complete-set arb executed! {result['detail']}")

    return result


async def trading_loop(feed_state, engine: TradingEngine, coin: str, timeframe: str, log):
    def feeds_ready() -> tuple[bool, str]:
        now = time.time()
        if not feed_state.binance_ws_connected:
            return False, "waiting Binance WS connection"
        if not feed_state.binance_ob_ready or feed_state.mid <= 0:
            return False, "waiting Binance orderbook"
        if feed_state.binance_ob_last_ok_ts <= 0:
            return False, "waiting Binance orderbook"
        if (now - feed_state.binance_ob_last_ok_ts) > engine.cfg.binance_ob_stale_sec:
            return False, "waiting Binance orderbook (stale)"
        if not feed_state.klines:
            return False, "waiting Binance candles"
        if not feed_state.pm_connected:
            return False, "waiting Polymarket WS connection"
        if not feed_state.pm_prices_ready:
            return False, "waiting Polymarket prices"
        if feed_state.pm_last_update_ts > 0:
            pm_age = now - feed_state.pm_last_update_ts
            if pm_age > 120:
                return False, f"PM prices stale ({int(pm_age)}s old)"
        return True, ""

    log(
        "  [TRADER] started | "
        f"mode={engine.mode.value} size=${engine.cfg.size_usd:.2f} "
        f"min_bias={engine.cfg.min_abs_bias:.1f} min_obi={engine.cfg.min_abs_obi:.2f} "
        f"tp={engine.cfg.tp_pct:.1f}% sl={engine.cfg.sl_pct:.1f}% "
        f"max_hold={engine.cfg.max_hold_sec}s "
        f"manual_approval={engine.cfg.live_manual_approval} "
        f"require_fill={engine.cfg.live_entry_require_fill} "
        f"fill_timeout={engine.cfg.live_entry_fill_timeout_sec}s"
    )
    log(f"  [TRADER] {engine.control_help()}")
    last_gate_reason = ""
    last_window_start_ts = 0
    _last_arb_ts = 0.0
    feed_gate_start_ts: float = 0.0
    feed_gate_last_log_ts: float = 0.0
    FEED_GATE_TIMEOUT = 120
    FEED_GATE_LOG_EVERY = 15

    import window as window_mod

    while True:
        try:
            # Window transition MUST run before feed gate to avoid deadlock
            # when PM prices are stale/resolved (all >= 0.99)
            winfo = window_mod.get_window_info(timeframe)
            if last_window_start_ts and winfo.start_ts != last_window_start_ts:
                log(
                    f"  [TRADER] window transition: new {timeframe} window started "
                    f"(start={winfo.start_ts}, end={winfo.end_ts})"
                )
                # Heartbeat: snapshot of current state for diagnostics
                _hb_bias = ind.bias_score(
                    feed_state.bids, feed_state.asks, feed_state.mid,
                    feed_state.trades, feed_state.klines,
                ) if feed_state.mid and feed_state.klines else 0.0
                _hb_obi = ind.obi(
                    feed_state.bids, feed_state.asks, feed_state.mid,
                ) if feed_state.mid else 0.0
                log(
                    f"  [TRADER] heartbeat: bias={_hb_bias:+.1f} obi={_hb_obi:+.3f} "
                    f"pm_up={feed_state.pm_up} pm_dn={feed_state.pm_dn} "
                    f"mid={feed_state.mid:.2f} trades_today={engine.state.trades_today}"
                )
                # Reset PM prices and re-fetch tokens for the new market window
                feed_state.pm_up = None
                feed_state.pm_dn = None
                feed_state.pm_prices_ready = False
                feed_state.pm_all_filtered = False
                feed_state.pm_all_filtered_ts = 0.0
                try:
                    import feeds as feeds_mod
                    try:
                        new_up, new_dn = await asyncio.wait_for(
                            asyncio.to_thread(feeds_mod.fetch_pm_tokens, coin, timeframe),
                            timeout=15.0,
                        )
                    except asyncio.TimeoutError:
                        log("  [TRADER] WARNING: fetch_pm_tokens timed out (15s)")
                        new_up, new_dn = None, None
                    if new_up and new_dn:
                        feed_state.pm_up_id = new_up
                        feed_state.pm_dn_id = new_dn
                        feed_state.pm_reconnect_requested = True
                        log(
                            f"  [TRADER] new PM tokens: up={new_up[:12]}.. dn={new_dn[:12]}.. "
                            f"— WS reconnect requested"
                        )
                    else:
                        log("  [TRADER] WARNING: failed to fetch new PM tokens for this window")
                except Exception as e:
                    log(f"  [TRADER] PM token refresh error: {e}")
                    if engine._on_error:
                        try:
                            engine._on_error("PM TOKEN REFRESH", str(e))
                        except Exception:
                            pass
            last_window_start_ts = winfo.start_ts

            ready, gate_reason = feeds_ready()
            if not ready:
                now_fg = time.time()
                if feed_gate_start_ts == 0.0:
                    feed_gate_start_ts = now_fg

                elapsed = now_fg - feed_gate_start_ts

                if gate_reason != last_gate_reason:
                    log(f"  [TRADER] feed gate: {gate_reason}")
                    last_gate_reason = gate_reason
                    feed_gate_last_log_ts = now_fg
                elif now_fg - feed_gate_last_log_ts >= FEED_GATE_LOG_EVERY:
                    log(
                        f"  [TRADER] feed gate: still waiting ({int(elapsed)}s) — {gate_reason} | "
                        f"pm_conn={feed_state.pm_connected} up={feed_state.pm_up} dn={feed_state.pm_dn}"
                        + (f" ALL_FILTERED" if getattr(feed_state, 'pm_all_filtered', False) else "")
                    )
                    feed_gate_last_log_ts = now_fg

                if elapsed >= FEED_GATE_TIMEOUT:
                    if feed_state.pm_connected and (
                        not feed_state.pm_prices_ready
                        or getattr(feed_state, 'pm_all_filtered', False)
                    ):
                        winfo_fg = window_mod.get_window_info(timeframe)
                        sleep_sec = min(winfo_fg.remaining_sec + 5, 600)
                        log(
                            f"  [TRADER] feed gate TIMEOUT ({int(elapsed)}s): "
                            f"market likely resolved. Sleeping {sleep_sec}s until next window."
                        )
                        if engine._on_error:
                            try:
                                engine._on_error("FEED GATE TIMEOUT", f"market resolved, sleep {sleep_sec}s")
                            except Exception:
                                pass
                        await asyncio.sleep(sleep_sec)
                    else:
                        log(f"  [TRADER] feed gate TIMEOUT ({int(elapsed)}s): {gate_reason}. Resetting timer.")
                        if engine._on_error:
                            try:
                                engine._on_error("FEED GATE TIMEOUT", f"{gate_reason} ({int(elapsed)}s)")
                            except Exception:
                                pass
                    feed_gate_start_ts = 0.0
                    feed_gate_last_log_ts = 0.0

                await asyncio.sleep(engine.cfg.eval_interval_sec)
                continue

            if last_gate_reason:
                log("  [TRADER] feed gate: ready (Binance + Polymarket)")
                last_gate_reason = ""
                feed_gate_start_ts = 0.0
                feed_gate_last_log_ts = 0.0

            # Complete-set arbitrage execution
            if (
                config.PM_ARB_ENABLED
                and feed_state.pm_up is not None
                and feed_state.pm_dn is not None
                and feed_state.pm_up_id
                and feed_state.pm_dn_id
            ):
                pm_sum = feed_state.pm_up + feed_state.pm_dn
                net_edge_pct = (1.0 - pm_sum - 2 * config.PM_TAKER_FEE) * 100

                if net_edge_pct >= config.PM_ARB_MIN_EDGE_PCT:
                    now = time.time()
                    # Check cooldown
                    if now - _last_arb_ts >= config.PM_ARB_COOLDOWN_SEC:
                        # Check that engine is in live mode and has executor
                        if engine.mode == TradeMode.LIVE and isinstance(engine.executor, LiveExecutor):
                            log(
                                f"  [ARB] executing complete-set arb! "
                                f"UP={feed_state.pm_up:.3f} DN={feed_state.pm_dn:.3f} "
                                f"sum={pm_sum:.3f} edge={net_edge_pct:+.2f}%"
                            )

                            def _do_arb():
                                return _execute_complete_set_arb(
                                    engine.executor,
                                    feed_state.pm_up_id,
                                    feed_state.pm_dn_id,
                                    feed_state.pm_up,
                                    feed_state.pm_dn,
                                    config.PM_ARB_MAX_SIZE_USD,
                                    engine.cfg,
                                    log,
                                )

                            arb_result = await asyncio.to_thread(_do_arb)
                            _last_arb_ts = now

                            if engine._on_error:
                                try:
                                    if arb_result.get("ok"):
                                        engine._on_error("ARB_SUCCESS", arb_result.get("detail", ""))
                                    else:
                                        engine._on_error("ARB_FAILED", arb_result.get("detail", ""))
                                except Exception:
                                    pass
                elif pm_sum < config.PM_COMPLETE_SET_ALERT:
                    # Log-only for near-misses
                    log(
                        f"  [ARB] complete-set edge detected (below threshold)! "
                        f"UP={feed_state.pm_up:.3f} + DN={feed_state.pm_dn:.3f} = {pm_sum:.3f} "
                        f"(net edge: {net_edge_pct:+.1f}%, need ≥{config.PM_ARB_MIN_EDGE_PCT}%)"
                    )

            # Engine operations may perform blocking I/O (CLOB calls + fill polling + sleeps).
            # Run them off the event loop to keep FastAPI/UI/Telegram responsive.
            def _engine_cycle():
                engine.process_control_commands(feed_state, log)
                engine.maybe_close_position(feed_state, coin, timeframe, log)
                engine.maybe_open_position(feed_state, coin, timeframe, log)

            await asyncio.to_thread(_engine_cycle)
        except Exception as e:
            log(f"  [TRADER] loop error: {e}")
            if engine._on_error:
                try:
                    engine._on_error("TRADING LOOP", str(e))
                except Exception:
                    pass
        await asyncio.sleep(engine.cfg.eval_interval_sec)
