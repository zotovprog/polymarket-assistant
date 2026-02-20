import sys
import os
import asyncio
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

from rich.console import Console
from rich.live   import Live

import config
import feeds
import dashboard
import trading

console = Console(force_terminal=True)


def pick(title: str, options: list[str]) -> str:
    console.print(f"\n[bold]{title}[/bold]")
    for i, o in enumerate(options, 1):
        console.print(f"  [{i}] {o}")
    while True:
        raw = input("  → ").strip()
        try:
            idx = int(raw) - 1
            if 0 <= idx < len(options):
                return options[idx]
        except ValueError:
            pass
        console.print("  [red]invalid – try again[/red]")


async def display_loop(state: feeds.State, coin: str, tf: str):
    await asyncio.sleep(2)
    refresh_interval = config.REFRESH_5M if tf == "5m" else config.REFRESH
    with Live(console=console, refresh_per_second=1, transient=False) as live:
        while True:
            if state.mid > 0 and state.klines:
                live.update(dashboard.render(state, coin, tf))
            await asyncio.sleep(refresh_interval)


async def terminal_command_loop(engine: "trading.TradingEngine"):
    if not sys.stdin or not sys.stdin.isatty():
        return
    console.print(
        "[dim][TRADER] terminal input enabled: "
        "y=approve n=reject s=status c=close r=reset h=help[/dim]"
    )
    while True:
        try:
            line = await asyncio.to_thread(sys.stdin.readline)
        except Exception:
            await asyncio.sleep(0.5)
            continue
        if line == "":
            await asyncio.sleep(0.5)
            continue
        engine.enqueue_command(line)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Polymarket Crypto Assistant (observe / paper / live)"
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--paper",
        action="store_true",
        help="Enable paper trading (simulated bets).",
    )
    mode.add_argument(
        "--live",
        action="store_true",
        help="Enable live trading (real orders on Polymarket CLOB).",
    )
    parser.add_argument(
        "--size-usd",
        type=float,
        default=5.0,
        help="Bet size in USD (minimum 5.0).",
    )
    parser.add_argument("--min-bias", type=float, default=75.0, help="Absolute bias threshold.")
    parser.add_argument("--min-obi", type=float, default=0.35, help="Absolute OBI threshold.")
    parser.add_argument("--min-price", type=float, default=0.20, help="Minimum contract price.")
    parser.add_argument("--max-price", type=float, default=0.70, help="Maximum contract price.")
    parser.add_argument("--cooldown-sec", type=int, default=900, help="Minimum seconds between bets.")
    parser.add_argument("--max-trades-per-day", type=int, default=20, help="Daily trade cap.")
    parser.add_argument("--eval-interval-sec", type=int, default=5, help="Signal evaluation interval.")
    parser.add_argument("--tp-pct", type=float, default=15.0, help="Take-profit in percent from entry.")
    parser.add_argument("--sl-pct", type=float, default=8.0, help="Stop-loss in percent from entry.")
    parser.add_argument("--max-hold-sec", type=int, default=900, help="Max position hold time in seconds.")
    parser.add_argument(
        "--reverse-exit-bias",
        type=float,
        default=60.0,
        help="Exit when bias reverses beyond this absolute threshold.",
    )
    parser.add_argument(
        "--disable-auto-exit",
        action="store_true",
        help="Disable TP/SL/time/reversal auto-exit logic.",
    )
    parser.add_argument(
        "--disable-reverse-exit",
        action="store_true",
        help="Disable reverse-bias auto-exit only.",
    )
    parser.add_argument(
        "--control-file",
        type=str,
        default=".traderctl",
        help="Path to control file for runtime trader commands.",
    )
    parser.add_argument(
        "--executions-log-file",
        type=str,
        default="executions.log.jsonl",
        help="Path to JSONL file where only successful entry/exit operations are appended.",
    )
    parser.add_argument(
        "--auto-approve",
        action="store_true",
        help="Live mode: auto-approve entries (no manual y/approve required).",
    )
    parser.add_argument(
        "--disable-approval-beep",
        action="store_true",
        help="Disable terminal beep when a pending live trade needs approval.",
    )
    parser.add_argument(
        "--approval-sound-command",
        type=str,
        default="",
        help="Optional shell command to play approval sound (overrides default beep).",
    )
    parser.add_argument(
        "--entry-fill-timeout-sec",
        type=int,
        default=20,
        help="Live mode: seconds to wait for entry order fill before marking unfilled.",
    )
    parser.add_argument(
        "--entry-fill-poll-sec",
        type=float,
        default=1.0,
        help="Live mode: polling interval for entry fill check.",
    )
    parser.add_argument(
        "--binance-ob-stale-sec",
        type=int,
        default=12,
        help="Seconds after last successful Binance orderbook poll before feed gate pauses trading.",
    )
    parser.add_argument(
        "--allow-posted-entry",
        action="store_true",
        help="Live mode: treat posted entry as open without waiting for fill (less safe).",
    )
    parser.add_argument(
        "--keep-unfilled-entry-open",
        action="store_true",
        help="Live mode: do not auto-cancel an entry that was not filled in time.",
    )
    parser.add_argument(
        "--confirm-live-token",
        type=str,
        default="",
        help=f'Required for --live: "{trading.LIVE_CONFIRM_TOKEN}"',
    )
    parser.add_argument(
        "--coin",
        type=str,
        default="",
        help=f"Optional coin shortcut: {', '.join(config.COINS)}",
    )
    parser.add_argument(
        "--timeframe",
        type=str,
        default="",
        help="Optional timeframe shortcut (must be valid for selected coin).",
    )
    return parser.parse_args()


async def main():
    args = parse_args()
    console.print("\n[bold magenta]═══ CRYPTO PREDICTION DASHBOARD ═══[/bold magenta]\n")

    mode = trading.TradeMode.OBSERVE
    if args.paper:
        mode = trading.TradeMode.PAPER
    elif args.live:
        mode = trading.TradeMode.LIVE

    if mode == trading.TradeMode.LIVE:
        if args.confirm_live_token != trading.LIVE_CONFIRM_TOKEN:
            console.print(
                f'[bold red]Refused[/bold red]: --live requires '
                f'--confirm-live-token "{trading.LIVE_CONFIRM_TOKEN}"'
            )
            return
        if os.environ.get("PM_ENABLE_LIVE") != "1":
            console.print(
                "[bold red]Refused[/bold red]: PM_ENABLE_LIVE=1 is required for --live."
            )
            return

    engine = None
    if mode != trading.TradeMode.OBSERVE:
        default_approval_sound = args.approval_sound_command.strip()
        if not default_approval_sound and sys.platform == "darwin":
            default_approval_sound = "afplay /System/Library/Sounds/Glass.aiff"

        cfg = trading.TradingConfig(
            size_usd=max(5.0, args.size_usd),
            min_abs_bias=max(0.0, args.min_bias),
            min_abs_obi=max(0.0, args.min_obi),
            min_price=max(0.01, args.min_price),
            max_price=min(0.99, args.max_price),
            cooldown_sec=max(10, args.cooldown_sec),
            max_trades_per_day=max(1, args.max_trades_per_day),
            eval_interval_sec=max(1, args.eval_interval_sec),
            control_file=args.control_file,
            executions_log_file=args.executions_log_file,
            binance_ob_stale_sec=max(3, args.binance_ob_stale_sec),
            live_manual_approval=not args.auto_approve,
            approval_beep_enabled=not args.disable_approval_beep,
            approval_sound_command=default_approval_sound,
            live_entry_require_fill=not args.allow_posted_entry,
            live_entry_fill_timeout_sec=max(1, args.entry_fill_timeout_sec),
            live_entry_fill_poll_sec=max(0.2, args.entry_fill_poll_sec),
            live_cancel_unfilled_entry=not args.keep_unfilled_entry_open,
            auto_exit_enabled=not args.disable_auto_exit,
            tp_pct=max(0.1, args.tp_pct),
            sl_pct=max(0.1, args.sl_pct),
            max_hold_sec=max(30, args.max_hold_sec),
            reverse_exit_enabled=not args.disable_reverse_exit,
            reverse_exit_bias=max(1.0, args.reverse_exit_bias),
        )
        try:
            engine = trading.TradingEngine(mode, cfg)
        except ValueError as e:
            console.print(f"[bold red]Trading init failed[/bold red]: {e}")
            return

        mode_color = "yellow" if mode == trading.TradeMode.PAPER else "red"
        console.print(
            f"[{mode_color}]Trading mode: {mode.value.upper()}[/]"
            f"  size=${cfg.size_usd:.2f}  min_bias={cfg.min_abs_bias:.1f}"
            f"  min_obi={cfg.min_abs_obi:.2f}  tp={cfg.tp_pct:.1f}%"
            f"  sl={cfg.sl_pct:.1f}%  hold={cfg.max_hold_sec}s"
            f"  require_fill={cfg.live_entry_require_fill}"
            f"  fill_timeout={cfg.live_entry_fill_timeout_sec}s"
            f"  manual_approval={cfg.live_manual_approval}"
        )
        if mode == trading.TradeMode.PAPER:
            console.print("[yellow]Paper mode: no real orders will be sent.[/yellow]")
        else:
            console.print("[bold red]LIVE mode: real orders may be sent.[/bold red]")
            if args.auto_approve:
                console.print("[bold red]AUTO-APPROVE ENABLED: entries will be sent automatically.[/bold red]")
        console.print(f"[dim]executions log: {cfg.executions_log_file} (successful operations only)[/dim]")
        console.print(f"[dim]{engine.control_help()}[/dim]")

    if args.coin:
        coin = args.coin.upper()
        if coin not in config.COINS:
            console.print(
                f"[bold red]Invalid --coin[/bold red]: {args.coin} "
                f"(allowed: {', '.join(config.COINS)})"
            )
            return
    else:
        coin = pick("Select coin:", config.COINS)

    if args.timeframe:
        tf = args.timeframe
        allowed_tfs = config.COIN_TIMEFRAMES[coin]
        if tf not in allowed_tfs:
            console.print(
                f"[bold red]Invalid --timeframe[/bold red]: {tf} for {coin} "
                f"(allowed: {', '.join(allowed_tfs)})"
            )
            return
    else:
        tf = pick("Select timeframe:", config.COIN_TIMEFRAMES[coin])

    console.print(f"\n[bold green]Starting {coin} {tf} …[/bold green]\n")

    state = feeds.State()

    state.pm_up_id, state.pm_dn_id = feeds.fetch_pm_tokens(coin, tf)
    if state.pm_up_id:
        console.print(f"  [PM] Up   → {state.pm_up_id[:24]}…")
        console.print(f"  [PM] Down → {state.pm_dn_id[:24]}…")
    else:
        console.print("  [yellow][PM] no market for this coin/timeframe – prices will not show[/yellow]")

    binance_sym = config.COIN_BINANCE[coin]
    kline_iv    = config.TF_KLINE[tf]
    console.print("  [Binance] bootstrapping candles …")
    await feeds.bootstrap(binance_sym, kline_iv, state)

    tasks = [
        feeds.ob_poller(binance_sym, state),
        feeds.binance_feed(binance_sym, kline_iv, state),
        feeds.pm_feed(state),
        display_loop(state, coin, tf),
    ]
    if engine is not None:
        tasks.append(trading.trading_loop(state, engine, coin, tf, console.print))
        tasks.append(terminal_command_loop(engine))

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
