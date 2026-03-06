#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


EXPECTED_COLUMNS = (
    "timestamp",
    "maker",
    "makerAssetId",
    "makerAmountFilled",
    "taker",
    "takerAssetId",
    "takerAmountFilled",
    "transactionHash",
)


@dataclass
class TokenStats:
    count: int = 0
    usdc_volume_raw: float = 0.0
    first_ts: int | None = None
    last_ts: int | None = None


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _prepare_chunk(chunk: pd.DataFrame, chunk_id: int) -> tuple[pd.DataFrame, dict[str, int]]:
    stats = {
        "input_rows": int(len(chunk)),
        "valid_rows": 0,
        "written_rows": 0,
        "dropped_invalid": 0,
        "dropped_dupe_chunk": 0,
        "price_out_of_range": 0,
    }

    maker_asset = chunk["makerAssetId"].astype("string").fillna("")
    taker_asset = chunk["takerAssetId"].astype("string").fillna("")
    maker_amount = pd.to_numeric(chunk["makerAmountFilled"], errors="coerce")
    taker_amount = pd.to_numeric(chunk["takerAmountFilled"], errors="coerce")
    ts = pd.to_numeric(chunk["timestamp"], errors="coerce")

    maker_usdc = maker_asset == "0"
    taker_usdc = taker_asset == "0"
    valid = (maker_usdc ^ taker_usdc) & maker_amount.gt(0) & taker_amount.gt(0) & ts.notna()

    stats["valid_rows"] = int(valid.sum())
    stats["dropped_invalid"] = stats["input_rows"] - stats["valid_rows"]
    if stats["valid_rows"] == 0:
        return pd.DataFrame(), stats

    valid_df = chunk.loc[valid, [
        "timestamp",
        "transactionHash",
        "maker",
        "taker",
        "makerAssetId",
        "takerAssetId",
    ]].copy()
    valid_df["timestamp_sec"] = ts.loc[valid].astype("int64")
    valid_df["maker_amount_raw"] = maker_amount.loc[valid].astype("float64")
    valid_df["taker_amount_raw"] = taker_amount.loc[valid].astype("float64")
    valid_df["maker_usdc"] = maker_usdc.loc[valid].astype("bool")

    valid_df["token_asset_id"] = valid_df["takerAssetId"].where(valid_df["maker_usdc"], valid_df["makerAssetId"])
    valid_df["usdc_amount_raw"] = valid_df["maker_amount_raw"].where(valid_df["maker_usdc"], valid_df["taker_amount_raw"])
    valid_df["token_amount_raw"] = valid_df["taker_amount_raw"].where(valid_df["maker_usdc"], valid_df["maker_amount_raw"])
    valid_df["maker_side"] = valid_df["maker_usdc"].map({True: "BUY", False: "SELL"})
    valid_df["taker_side"] = valid_df["maker_usdc"].map({True: "SELL", False: "BUY"})
    valid_df["price_prob"] = valid_df["usdc_amount_raw"] / valid_df["token_amount_raw"]
    valid_df["self_trade"] = (valid_df["maker"] == valid_df["taker"]).astype("bool")
    valid_df["chunk_id"] = int(chunk_id)
    valid_df["trade_day"] = pd.to_datetime(valid_df["timestamp_sec"], unit="s", utc=True).dt.strftime("%Y-%m-%d")

    valid_df["price_out_of_range"] = (valid_df["price_prob"] <= 0.0) | (valid_df["price_prob"] > 1.0)
    stats["price_out_of_range"] = int(valid_df["price_out_of_range"].sum())

    dedupe_cols = [
        "timestamp_sec",
        "transactionHash",
        "maker",
        "taker",
        "token_asset_id",
        "maker_side",
        "usdc_amount_raw",
        "token_amount_raw",
    ]
    before = len(valid_df)
    valid_df = valid_df.drop_duplicates(subset=dedupe_cols)
    stats["dropped_dupe_chunk"] = int(before - len(valid_df))

    out = valid_df[[
        "timestamp_sec",
        "trade_day",
        "transactionHash",
        "maker",
        "taker",
        "token_asset_id",
        "maker_side",
        "taker_side",
        "price_prob",
        "usdc_amount_raw",
        "token_amount_raw",
        "price_out_of_range",
        "self_trade",
        "chunk_id",
    ]].copy()
    out = out.rename(columns={"transactionHash": "transaction_hash"})
    stats["written_rows"] = int(len(out))
    return out, stats


def _write_partitioned(df: pd.DataFrame, output_root: Path, part_idx_by_day: dict[str, int]) -> int:
    files = 0
    for trade_day, day_df in df.groupby("trade_day", sort=False):
        part_idx_by_day[trade_day] = part_idx_by_day.get(trade_day, 0) + 1
        part_idx = part_idx_by_day[trade_day]
        day_dir = output_root / f"trade_day={trade_day}"
        day_dir.mkdir(parents=True, exist_ok=True)
        target = day_dir / f"part-{part_idx:06d}.parquet"
        table = pa.Table.from_pandas(day_df, preserve_index=False)
        pq.write_table(table, target, compression="zstd")
        files += 1
    return files


def _update_token_stats(token_stats: dict[str, TokenStats], df: pd.DataFrame) -> None:
    grouped = df.groupby("token_asset_id", sort=False).agg(
        count=("token_asset_id", "size"),
        usdc_volume_raw=("usdc_amount_raw", "sum"),
        first_ts=("timestamp_sec", "min"),
        last_ts=("timestamp_sec", "max"),
    )
    for token, row in grouped.iterrows():
        entry = token_stats.get(token)
        if entry is None:
            token_stats[token] = TokenStats(
                count=int(row["count"]),
                usdc_volume_raw=float(row["usdc_volume_raw"]),
                first_ts=int(row["first_ts"]),
                last_ts=int(row["last_ts"]),
            )
            continue
        entry.count += int(row["count"])
        entry.usdc_volume_raw += float(row["usdc_volume_raw"])
        entry.first_ts = int(min(entry.first_ts or int(row["first_ts"]), int(row["first_ts"])))
        entry.last_ts = int(max(entry.last_ts or int(row["last_ts"]), int(row["last_ts"])))


def main() -> int:
    parser = argparse.ArgumentParser(description="Normalize warproxxx/poly_data orderFilled CSV into replay parquet dataset.")
    parser.add_argument("--input", default="data/raw/orderFilled_complete.csv.xz")
    parser.add_argument("--output", default="data/normalized/poly_data_orderfills")
    parser.add_argument("--chunksize", type=int, default=1_000_000)
    parser.add_argument("--max-chunks", type=int, default=0, help="0 means full file")
    parser.add_argument("--clean", action="store_true", help="Remove output dir before writing")
    args = parser.parse_args()

    input_path = Path(args.input).resolve()
    output_root = Path(args.output).resolve()
    if not input_path.exists():
        raise SystemExit(f"input file not found: {input_path}")
    if args.clean and output_root.exists():
        for item in output_root.rglob("*"):
            if item.is_file():
                item.unlink()
        for item in sorted(output_root.rglob("*"), reverse=True):
            if item.is_dir():
                item.rmdir()
    output_root.mkdir(parents=True, exist_ok=True)

    started = time.time()
    started_utc = _now_utc()
    token_stats: dict[str, TokenStats] = {}
    part_idx_by_day: dict[str, int] = {}
    processed_chunks = 0

    totals = defaultdict(int)
    files_written = 0

    reader = pd.read_csv(
        input_path,
        compression="xz",
        chunksize=max(1, int(args.chunksize)),
        dtype={
            "timestamp": "string",
            "maker": "string",
            "makerAssetId": "string",
            "makerAmountFilled": "string",
            "taker": "string",
            "takerAssetId": "string",
            "takerAmountFilled": "string",
            "transactionHash": "string",
        },
    )

    for chunk_id, chunk in enumerate(reader, start=1):
        if args.max_chunks and chunk_id > int(args.max_chunks):
            break
        processed_chunks += 1
        missing = [col for col in EXPECTED_COLUMNS if col not in chunk.columns]
        if missing:
            raise RuntimeError(f"missing expected columns: {missing}")

        normalized, chunk_stats = _prepare_chunk(chunk, chunk_id)
        for k, v in chunk_stats.items():
            totals[k] += int(v)

        if not normalized.empty:
            files_written += _write_partitioned(normalized, output_root, part_idx_by_day)
            _update_token_stats(token_stats, normalized)

        elapsed = max(1e-6, time.time() - started)
        rows = totals["input_rows"]
        rps = rows / elapsed
        print(
            f"[chunk {chunk_id}] input={chunk_stats['input_rows']} valid={chunk_stats['valid_rows']} "
            f"written={chunk_stats['written_rows']} dropped_invalid={chunk_stats['dropped_invalid']} "
            f"elapsed={elapsed:.1f}s rows/s={rps:,.0f}"
        )

    token_index = pd.DataFrame([
        {
            "token_asset_id": token,
            "trade_count": s.count,
            "usdc_volume_raw": s.usdc_volume_raw,
            "first_ts": s.first_ts,
            "last_ts": s.last_ts,
        }
        for token, s in token_stats.items()
    ])
    if not token_index.empty:
        token_index = token_index.sort_values(["trade_count", "usdc_volume_raw"], ascending=[False, False])
    token_index_path = output_root / "_token_index.parquet"
    token_index.to_parquet(token_index_path, index=False)

    finished_utc = _now_utc()
    duration = time.time() - started
    manifest = {
        "input_file": str(input_path),
        "input_size_bytes": input_path.stat().st_size,
        "output_root": str(output_root),
        "started_utc": started_utc,
        "finished_utc": finished_utc,
        "duration_sec": round(duration, 3),
        "chunks_processed": int(processed_chunks),
        "rows": {
            "input_rows": int(totals["input_rows"]),
            "valid_rows": int(totals["valid_rows"]),
            "written_rows": int(totals["written_rows"]),
            "dropped_invalid": int(totals["dropped_invalid"]),
            "dropped_dupe_chunk": int(totals["dropped_dupe_chunk"]),
            "price_out_of_range": int(totals["price_out_of_range"]),
        },
        "partitions": {
            "days": len(part_idx_by_day),
            "files_written": int(files_written),
        },
        "token_index": {
            "token_count": int(len(token_index)),
            "path": str(token_index_path),
        },
        "schema": [
            "timestamp_sec:int64",
            "trade_day:string",
            "transaction_hash:string",
            "maker:string",
            "taker:string",
            "token_asset_id:string",
            "maker_side:string",
            "taker_side:string",
            "price_prob:float64",
            "usdc_amount_raw:float64",
            "token_amount_raw:float64",
            "price_out_of_range:bool",
            "self_trade:bool",
            "chunk_id:int64",
        ],
    }

    manifest_path = output_root / "_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print(f"done: wrote {totals['written_rows']} rows to {output_root}")
    print(f"manifest: {manifest_path}")
    print(f"token index: {token_index_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
