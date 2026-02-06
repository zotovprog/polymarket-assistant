# Changelog

All notable changes to the "High-Conviction Polymarket Signal Automator" project will be documented in this file.

## [Unreleased] - 2026-02-06

### Added
-   **Automated Execution Layer**: `main.py` now initializes `ClobClient` (via `py-clob-client`) to execute trades automatically.
    -   Added **Dry Run** mode (default) to simulate trades without spending real funds.
    -   Implemented `create_or_derive_api_creds` to handle L2 authentication automatically.
-   **Execution Filters**:
    -   **Conviction Threshold**: set to **8/10** (tuned down from 9). Only trades when Trend Score is ≥ 8.
    -   **Order Book Imbalance (OBI)**: Requires > 0.65 (65% buy/sell pressure) to confirm trade.
    -   **Price Safety**: Only buys contracts priced between $0.20 and $0.58.
-   **Risk Management**:
    -   **Position Sizing**: Hard cap of **$5.00 USDC** per trade.
    -   **Daily Loss Limit**: Stops trading if session loss exceeds **$15.00**.
-   **Resilience / Stability**:
    -   Added `while True` / `try-except` loops to `src/feeds.py` for both Binance and Polymarket websockets.
    -   Bot now automatically reconnects after network timeouts instead of crashing with `ConnectionClosedError`.
-   **Utilities**:
    -   `test_approve_and_smoke.py`: Script to verify Wallet connection, Token Approvals (USDC/CTF), and Market Read access before starting.
-   **Configuration**:
    -   `.env` support for secure Private Key management.
    -   `.gitignore` added to exclude sensitive files (`.env`, logs) from git.

### Changed
-   **Documentation**:
    -   Updated `README.md` with "Auto-Trading Setup" instructions.
    -   Updated `SETUP_GUIDE.md` to reflect Dry Run output.
    -   Added `VPS_PLAN.md` for 24/7 deployment recommendations.

### Fixed
-   Fixed `AttributeError` in approval scripts by using correct `update_balance_allowance` method from `py-clob-client`.
-   Fixed crash on Binance websocket timeout ("Internal Error 1011").
