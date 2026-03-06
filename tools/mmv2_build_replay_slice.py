#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import pandas as pd


def _parse_date(value: str) -> str:
    # Validates YYYY-MM-DD and returns canonical string.
    return str(date.fromisoformat(value))


def main() -> int:
    parser = argparse.ArgumentParser(description="Build replay slice from normalized Polymarket orderfills parquet dataset.")
    parser.add_argument("--dataset-root", default="data/normalized/poly_data_orderfills")
    parser.add_argument("--output", default="data/replay/poly_replay_slice.parquet")
    parser.add_argument("--token-id", action="append", required=True, help="Repeat for multiple token ids")
    parser.add_argument("--date-from", required=True, help="YYYY-MM-DD inclusive")
    parser.add_argument("--date-to", required=True, help="YYYY-MM-DD inclusive")
    parser.add_argument("--aggregate-sec", type=int, default=0, help="0 disables aggregation; >0 aggregates by N-second bins")
    args = parser.parse_args()

    date_from = _parse_date(args.date_from)
    date_to = _parse_date(args.date_to)
    token_ids = [str(t).strip() for t in args.token_id if str(t).strip()]
    if not token_ids:
        raise SystemExit("at least one --token-id must be provided")

    dataset_root = Path(args.dataset_root).resolve()
    if not dataset_root.exists():
        raise SystemExit(f"dataset root not found: {dataset_root}")

    columns = [
        "timestamp_sec",
        "transaction_hash",
        "token_asset_id",
        "price_prob",
        "usdc_amount_raw",
        "token_amount_raw",
        "maker_side",
        "taker_side",
        "self_trade",
        "price_out_of_range",
    ]
    day_dirs = sorted(
        p for p in dataset_root.glob("trade_day=*")
        if p.is_dir() and date_from <= p.name.split("=", 1)[1] <= date_to
    )
    if not day_dirs:
        raise SystemExit("no day partitions in requested range")

    frames: list[pd.DataFrame] = []
    for day_dir in day_dirs:
        trade_day = day_dir.name.split("=", 1)[1]
        for parquet_file in sorted(day_dir.glob("*.parquet")):
            frame = pd.read_parquet(parquet_file, columns=columns)
            if frame.empty:
                continue
            frame = frame[frame["token_asset_id"].isin(token_ids)]
            if frame.empty:
                continue
            frame = frame[frame["price_out_of_range"] == False]  # noqa: E712
            if frame.empty:
                continue
            frame["trade_day"] = trade_day
            frames.append(frame)

    if not frames:
        raise SystemExit("no rows matched filters")
    df = pd.concat(frames, ignore_index=True)
    if "price_out_of_range" in df.columns:
        df = df.drop(columns=["price_out_of_range"])

    df = df.sort_values(["timestamp_sec", "transaction_hash"]).reset_index(drop=True)
    if args.aggregate_sec and args.aggregate_sec > 0:
        sec = int(args.aggregate_sec)
        df["ts_bucket"] = (df["timestamp_sec"] // sec) * sec
        grouped = df.groupby(["ts_bucket", "token_asset_id"], as_index=False).agg(
            price_prob=("price_prob", "median"),
            usdc_amount_raw=("usdc_amount_raw", "sum"),
            token_amount_raw=("token_amount_raw", "sum"),
            trade_count=("transaction_hash", "count"),
        )
        out = grouped.rename(columns={"ts_bucket": "timestamp_sec"})
    else:
        out = df

    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(output, index=False)
    print(f"rows={len(out)} written={output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
