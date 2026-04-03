# MMV2 Replay Dataset (poly_data)

This document describes how to use `orderFilled_complete.csv.xz` from `warproxxx/poly_data` with this repo.

## Raw file location

Expected raw input path:

`data/raw/orderFilled_complete.csv.xz`

## 1) Normalize raw CSV.XZ into parquet dataset

Run:

```bash
./.venv/bin/python tools/mmv2_normalize_poly_orderfills.py \
  --input data/raw/orderFilled_complete.csv.xz \
  --output data/normalized/poly_data_orderfills \
  --chunksize 1000000 \
  --clean
```

Output:

- Partitioned parquet: `data/normalized/poly_data_orderfills/trade_day=YYYY-MM-DD/*.parquet`
- Manifest: `data/normalized/poly_data_orderfills/_manifest.json`
- Token index: `data/normalized/poly_data_orderfills/_token_index.parquet`

Normalized columns:

- `timestamp_sec`
- `trade_day`
- `transaction_hash`
- `maker`
- `taker`
- `token_asset_id`
- `maker_side`
- `taker_side`
- `price_prob`
- `usdc_amount_raw`
- `token_amount_raw`
- `price_out_of_range`
- `self_trade`
- `chunk_id`

## 2) Build replay slice for target token/date window

Use token ids from `_token_index.parquet` or from your MMV2 run state.

Example:

```bash
./.venv/bin/python tools/mmv2_build_replay_slice.py \
  --dataset-root data/normalized/poly_data_orderfills \
  --token-id <TOKEN_ID> \
  --date-from 2026-01-01 \
  --date-to 2026-01-07 \
  --aggregate-sec 60 \
  --output data/replay/poly_replay_slice.parquet
```

This creates replay-ready slice parquet for downstream scenario checks.

## Notes

- This dataset is trade/fill history, not full live execution state.
- It is excellent for mode/pressure/reaction replay, but does not replace final paper/live checks for execution races.
