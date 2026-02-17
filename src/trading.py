import asyncio
import json
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from queue import Empty, SimpleQueue

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
    max_trades_per_day: int = 3
    eval_interval_sec: int = 5
    control_file: str = ".traderctl"
    approval_beep_enabled: bool = True
    approval_sound_command: str = ""
    executions_log_file: str = "executions.log.jsonl"
    binance_ob_stale_sec: int = 12
    live_manual_approval: bool = True
    pending_approval_ttl_sec: int = 30
    live_entry_require_fill: bool = True
    live_entry_fill_timeout_sec: int = 20
    live_entry_fill_poll_sec: float = 1.0
    live_cancel_unfilled_entry: bool = True
    live_exit_size_buffer_pct: float = 0.5
    exit_retry_backoff_sec: int = 15
    auto_exit_enabled: bool = True
    tp_pct: float = 15.0
    sl_pct: float = 8.0
    max_hold_sec: int = 900
    reverse_exit_enabled: bool = True
    reverse_exit_bias: float = 60.0


@dataclass
class TradeDecision:
    side: str
    token_id: str
    price: float
    bias: float
    obi: float
    reason: str
    ts: float


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


class PaperExecutor:
    def __init__(self, cfg: TradingConfig):
        self.cfg = cfg

    def execute_entry(self, decision: TradeDecision, coin: str, timeframe: str) -> TradeRecord:
        shares = self.cfg.size_usd / max(decision.price, 0.01)
        return TradeRecord(
            ts_iso=datetime.now(timezone.utc).isoformat(),
            action="entry",
            mode=TradeMode.PAPER.value,
            coin=coin,
            timeframe=timeframe,
            side=decision.side,
            token_id=decision.token_id,
            price=decision.price,
            size_usd=self.cfg.size_usd,
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
    def __init__(self, cfg: TradingConfig):
        self.cfg = cfg
        self.client = None
        self._min_size_cache: dict[str, float] = {}
        self._conditional_allowance_checked: set[str] = set()
        self._init_client()

    def _init_client(self):
        private_key = os.environ.get("PM_PRIVATE_KEY")
        funder = os.environ.get("PM_FUNDER")
        sig_type = int(os.environ.get("PM_SIGNATURE_TYPE", "0"))

        if os.environ.get("PM_ENABLE_LIVE") != "1":
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

    def execute_entry(self, decision: TradeDecision, coin: str, timeframe: str) -> TradeRecord:
        from py_clob_client.clob_types import OrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY

        price = round(decision.price, 3)
        size = round(self.cfg.size_usd / max(price, 0.01), 2)
        order_id = None
        status = "failed"
        shares: float | None = None
        verify_note = ""

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
            resp = self.client.post_order(signed_order, OrderType.GTC)
            order_id = resp.get("orderID") or resp.get("id")
            if not order_id:
                status = "failed"
            elif not self.cfg.live_entry_require_fill:
                status = "posted"
                shares = size
            else:
                outcome, state, matched, total, err = self._wait_for_fill(order_id)
                verify_note = f"fill_check={state}"
                if matched is not None:
                    denom = total if (total is not None and total > 0) else size
                    verify_note = f"{verify_note}, filled={matched:.2f}/{denom:.2f}"

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
            size_usd=self.cfg.size_usd,
            shares=shares,
            status=status,
            reason=reason,
            order_id=order_id,
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
        size_raw = position.shares * (1.0 - self.cfg.live_exit_size_buffer_pct / 100.0)
        size = round(max(0.01, size_raw), 2)
        order_id = None
        status = "exit_failed"
        reason = f"{reason}, sell_size={size:.2f}"

        self._ensure_conditional_allowance(position.token_id)
        bal_info = self._get_conditional_balance_allowance(position.token_id)
        if bal_info is not None:
            bal_raw = str(bal_info.get("balance", "")).strip()
            if bal_raw == "0":
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
                    status="exit_no_token_balance",
                    reason=f"{reason}, conditional_balance=0",
                    order_id=None,
                    pnl_usd=pnl_usd,
                    pnl_pct=pnl_pct,
                )

        min_size = self._get_min_order_size(position.token_id)
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

        try:
            signed_order = self.client.create_order(
                OrderArgs(
                    token_id=position.token_id,
                    price=price,
                    size=size,
                    side=SELL,
                )
            )
            resp = self.client.post_order(signed_order, OrderType.GTC)
            order_id = resp.get("orderID") or resp.get("id")
            status = "exit_posted" if order_id else "exit_failed"
        except Exception as e:  # pragma: no cover
            status = self._format_exception_status("exit_error", e)

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
    ) -> tuple[str, str, float | None, float | None, str]:
        timeout = max(1.0, float(self.cfg.live_entry_fill_timeout_sec))
        poll = max(0.2, float(self.cfg.live_entry_fill_poll_sec))
        deadline = time.time() + timeout

        last_state = "open"
        last_matched = None
        last_size = None
        last_err = ""
        while time.time() < deadline:
            try:
                payload = self.client.get_order(order_id)
                state, state_desc, matched, size = self._order_state(payload)
                if state_desc:
                    last_state = state_desc
                if matched is not None:
                    last_matched = matched
                if size is not None:
                    last_size = size
                if state == "filled":
                    return "filled", last_state, last_matched, last_size, ""
                if state == "partial_terminal":
                    return "partial", last_state, last_matched, last_size, ""
                if state == "terminal_unfilled":
                    return "terminal_unfilled", last_state, last_matched, last_size, ""
            except Exception as e:
                last_err = str(e)
            time.sleep(poll)

        timeout_state = f"timeout({int(timeout)}s):{last_state}"
        if last_matched is not None and last_matched > 0:
            return "partial", timeout_state, last_matched, last_size, last_err
        return "unfilled", timeout_state, last_matched, last_size, last_err

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

    def _ensure_conditional_allowance(self, token_id: str):
        if token_id in self._conditional_allowance_checked:
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
            if not self._allowances_positive(allowances):
                self.client.update_balance_allowance(params)
                time.sleep(0.3)
        except Exception:
            pass

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
    def __init__(self, mode: TradeMode, cfg: TradingConfig):
        self.mode = mode
        self.cfg = cfg
        self.state = TraderState()
        self.executor = LiveExecutor(cfg) if mode == TradeMode.LIVE else PaperExecutor(cfg)
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

    def _build_decision(self, feed_state) -> TradeDecision | None:
        if not feed_state.mid or not feed_state.klines:
            return None
        if not feed_state.pm_up_id or not feed_state.pm_dn_id:
            return None
        if feed_state.pm_up is None or feed_state.pm_dn is None:
            return None

        bias = ind.bias_score(
            feed_state.bids, feed_state.asks, feed_state.mid, feed_state.trades, feed_state.klines
        )
        obi = ind.obi(feed_state.bids, feed_state.asks, feed_state.mid) if feed_state.mid else 0.0

        if abs(bias) < self.cfg.min_abs_bias:
            return None
        if abs(obi) < self.cfg.min_abs_obi:
            return None

        if bias > 0:
            side = "Up"
            token_id = feed_state.pm_up_id
            price = feed_state.pm_up
        else:
            side = "Down"
            token_id = feed_state.pm_dn_id
            price = feed_state.pm_dn

        if price is None or not (self.cfg.min_price <= price <= self.cfg.max_price):
            return None

        reason = f"bias={bias:+.1f}, obi={obi:+.3f}, px={price:.3f}"
        return TradeDecision(
            side=side,
            token_id=token_id,
            price=price,
            bias=bias,
            obi=obi,
            reason=reason,
            ts=time.time(),
        )

    def _allowed(self) -> tuple[bool, str]:
        self._rollover_day()
        now = time.time()

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
        if should_log:
            log(f"  [TRADER] skip: {reason}")
            self.state.last_skip_reason = reason
            self.state.last_skip_log_ts = now

    def _mark_price(self, feed_state, side: str) -> float | None:
        if side == "Up":
            return feed_state.pm_up
        return feed_state.pm_dn

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
                pnl_pct = (mark / open_pos.entry_price - 1.0) * 100.0
                pnl_usd = open_pos.shares * (mark - open_pos.entry_price)
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
        }
        try:
            with open(path, "a", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=False) + "\n")
        except Exception:
            pass

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

    def _exit_success(self, status: str) -> bool:
        return status in {"paper_exit", "exit_posted"}

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

                if self._entry_success(rec.status):
                    self.state.last_trade_ts = time.time()
                    self.state.trades_today += 1
                    shares = (
                        rec.shares
                        if rec.shares is not None and rec.shares > 0
                        else rec.size_usd / max(rec.price, 0.01)
                    )
                    self.state.open_position = OpenPosition(
                        side=rec.side,
                        token_id=rec.token_id,
                        entry_price=rec.price,
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

        decision = self._build_decision(feed_state)
        if decision is None:
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

        if self._entry_success(rec.status):
            self.state.last_trade_ts = time.time()
            self.state.trades_today += 1
            shares = (
                rec.shares
                if rec.shares is not None and rec.shares > 0
                else rec.size_usd / max(rec.price, 0.01)
            )
            self.state.open_position = OpenPosition(
                side=rec.side,
                token_id=rec.token_id,
                entry_price=rec.price,
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
            pnl_pct = (mark / pos.entry_price - 1.0) * 100.0 if pos.entry_price > 0 else None
            pnl_usd = pos.shares * (mark - pos.entry_price)
            return "manual_close", mark, pnl_usd, pnl_pct

        mark = self._mark_price(feed_state, pos.side)
        if mark is None:
            return None

        pnl_pct = (mark / pos.entry_price - 1.0) * 100.0 if pos.entry_price > 0 else None
        pnl_usd = pos.shares * (mark - pos.entry_price)
        hold_sec = time.time() - pos.entry_ts

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

        if self._exit_success(rec.status):
            self._clear_position_state()
            self.state.last_trade_ts = time.time()
            self.state.next_exit_attempt_ts = 0.0
            self._persist_execution(rec)
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
    while True:
        try:
            ready, gate_reason = feeds_ready()
            if not ready:
                if gate_reason != last_gate_reason:
                    log(f"  [TRADER] feed gate: {gate_reason}")
                    last_gate_reason = gate_reason
                await asyncio.sleep(engine.cfg.eval_interval_sec)
                continue
            if last_gate_reason:
                log("  [TRADER] feed gate: ready (Binance + Polymarket)")
                last_gate_reason = ""

            engine.process_control_commands(feed_state, log)
            engine.maybe_close_position(feed_state, coin, timeframe, log)
            engine.maybe_open_position(feed_state, coin, timeframe, log)
        except Exception as e:
            log(f"  [TRADER] loop error: {e}")
        await asyncio.sleep(engine.cfg.eval_interval_sec)
