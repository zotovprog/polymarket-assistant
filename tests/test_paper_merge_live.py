"""Live integration test: simulate paper session with fills, then trigger merge.

Bypasses web_server — directly creates MarketMaker with MockClobClient,
injects fills, then calls _liquidate_inventory to verify Phase 0 merge.
"""
import asyncio
import sys
import time
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
SRC = BASE / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))

import feeds
from mm.market_maker import MarketMaker
from mm.mm_config import MMConfig
from mm.types import MarketInfo, Fill


def make_mock_client(usdc: float = 20.0):
    """Create mock CLOB client from web_server module."""
    sys.path.insert(0, str(BASE))
    from web_server import MockClobClient
    return MockClobClient(fill_prob=0.15, usdc_balance=usdc)


async def run_merge_test():
    print("=" * 60)
    print("INTEGRATION TEST: Paper Merge Flow")
    print("=" * 60)

    # 1. Setup
    feed_state = feeds.State()
    feed_state.mid = 100000.0
    feed_state.bids = [(99999, 1.0)]
    feed_state.asks = [(100001, 1.0)]
    feed_state.klines = [
        {"t": time.time() - 300, "o": 99900, "h": 100100, "l": 99800, "c": 100000, "v": 100},
    ]

    client = make_mock_client(usdc=20.0)
    cfg = MMConfig()
    cfg.close_window_sec = 30.0

    mm = MarketMaker(feed_state, client, cfg)

    cond_id = "a1b2c3d4e5f60000000000000000000000000000000000000000000000000000"
    market = MarketInfo(
        coin="BTC", timeframe="5m",
        up_token_id="UP_TOKEN_ABC", dn_token_id="DN_TOKEN_XYZ",
        strike=100000.0,
        window_start=time.time() - 200,
        window_end=time.time() + 100,  # 100s remaining
        condition_id=cond_id,
    )
    mm.set_market(market)
    mm.inventory.initial_usdc = 20.0
    mm.inventory.usdc = 20.0

    print(f"\n[SETUP] condition_id={cond_id[:20]}...")
    print(f"[SETUP] Mock USDC balance: ${client._usdc_balance:.2f}")
    print(f"[SETUP] Inventory: UP={mm.inventory.up_shares} DN={mm.inventory.dn_shares}")

    # 2. Simulate BUY fills (as if orders were placed and filled)
    print("\n--- Simulating fills ---")

    # Buy 8 UP shares at 0.45
    fill_up = Fill(ts=time.time(), side="BUY", token_id="UP_TOKEN_ABC",
                   price=0.45, size=8.0, fee=0.0)
    mm.inventory.update_from_fill(fill_up, "up")
    mm.order_mgr._mock_token_balances["UP_TOKEN_ABC"] = 8.0
    client._usdc_balance -= 8.0 * 0.45  # Deduct collateral
    print(f"  FILL: BUY UP 8.0@0.45 = ${8*0.45:.2f}")

    # Buy 6 DN shares at 0.55
    fill_dn = Fill(ts=time.time(), side="BUY", token_id="DN_TOKEN_XYZ",
                   price=0.55, size=6.0, fee=0.0)
    mm.inventory.update_from_fill(fill_dn, "dn")
    mm.order_mgr._mock_token_balances["DN_TOKEN_XYZ"] = 6.0
    client._usdc_balance -= 6.0 * 0.55
    print(f"  FILL: BUY DN 6.0@0.55 = ${6*0.55:.2f}")

    print(f"\n[POST-FILLS]")
    print(f"  Inventory: UP={mm.inventory.up_shares:.1f} DN={mm.inventory.dn_shares:.1f}")
    print(f"  USDC (internal): ${mm.inventory.usdc:.2f}")
    print(f"  USDC (mock client): ${client._usdc_balance:.2f}")
    print(f"  UP avg entry: {mm.inventory.up_cost.avg_entry_price:.4f}")
    print(f"  DN avg entry: {mm.inventory.dn_cost.avg_entry_price:.4f}")
    print(f"  Mergeable pairs: {min(mm.inventory.up_shares, mm.inventory.dn_shares):.1f}")

    # 3. Trigger liquidation → should do Phase 0 merge first
    print("\n--- Triggering liquidation ---")
    before_usdc = mm.inventory.usdc
    before_mock = client._usdc_balance
    before_up = mm.inventory.up_shares
    before_dn = mm.inventory.dn_shares

    await mm._liquidate_inventory()

    after_usdc = mm.inventory.usdc
    after_mock = client._usdc_balance
    after_up = mm.inventory.up_shares
    after_dn = mm.inventory.dn_shares

    merge_amount = min(before_up, before_dn)  # Should be 6.0

    print(f"\n[POST-LIQUIDATION]")
    print(f"  Expected merge: {merge_amount:.1f} pairs")
    print(f"  Inventory: UP={after_up:.1f} (was {before_up:.1f}) DN={after_dn:.1f} (was {before_dn:.1f})")
    print(f"  USDC (internal): ${after_usdc:.2f} (was ${before_usdc:.2f})")
    print(f"  USDC (mock client): ${after_mock:.2f} (was ${before_mock:.2f})")
    print(f"  UP mock balance: {mm.order_mgr._mock_token_balances.get('UP_TOKEN_ABC', 0):.1f}")
    print(f"  DN mock balance: {mm.order_mgr._mock_token_balances.get('DN_TOKEN_XYZ', 0):.1f}")

    # 4. Verify merge happened
    print("\n--- Verification ---")
    errors = []

    # Merged 6 pairs (min of 8, 6)
    expected_up = max(0.0, before_up - merge_amount)  # 8 - 6 = 2
    expected_dn = max(0.0, before_dn - merge_amount)  # 6 - 6 = 0
    expected_usdc = before_usdc + merge_amount          # + $6

    if abs(after_up - expected_up) > 0.5:
        errors.append(f"UP shares: expected {expected_up:.1f}, got {after_up:.1f}")
    if abs(after_dn - expected_dn) > 0.5:
        errors.append(f"DN shares: expected {expected_dn:.1f}, got {after_dn:.1f}")
    if abs(after_usdc - expected_usdc) > 0.5:
        errors.append(f"USDC internal: expected ${expected_usdc:.2f}, got ${after_usdc:.2f}")
    if abs(after_mock - (before_mock + merge_amount)) > 0.5:
        errors.append(f"USDC mock: expected ${before_mock + merge_amount:.2f}, got ${after_mock:.2f}")

    # Mock token balances should reflect merge
    mock_up = mm.order_mgr._mock_token_balances.get("UP_TOKEN_ABC", 0)
    mock_dn = mm.order_mgr._mock_token_balances.get("DN_TOKEN_XYZ", 0)
    if abs(mock_up - expected_up) > 0.5:
        errors.append(f"Mock UP balance: expected {expected_up:.1f}, got {mock_up:.1f}")
    if abs(mock_dn - expected_dn) > 0.5:
        errors.append(f"Mock DN balance: expected {expected_dn:.1f}, got {mock_dn:.1f}")

    if errors:
        print("  FAILED:")
        for e in errors:
            print(f"    - {e}")
        return False
    else:
        print("  ALL CHECKS PASSED!")
        print(f"  Merge: {merge_amount:.0f} pairs → ${merge_amount:.2f} USDC")
        print(f"  Remaining: UP={after_up:.1f} DN={after_dn:.1f}")
        return True


async def run_budget_test():
    print("\n" + "=" * 60)
    print("INTEGRATION TEST: Order Collateral Budget Cap")
    print("=" * 60)

    from mm.quote_engine import QuoteEngine
    from mm.order_manager import OrderManager
    from mm.types import Inventory, Quote

    cfg = MMConfig()
    qe = QuoteEngine(cfg)

    inv = Inventory(up_shares=0, dn_shares=0, usdc=20.0, initial_usdc=20.0)

    # Simulate 2 active BUY orders: UP 15@0.45=$6.75, DN 12@0.55=$6.60
    active_buys = [
        Quote(side="BUY", token_id="UP", price=0.45, size=15.0),
        Quote(side="BUY", token_id="DN", price=0.55, size=12.0),
    ]
    order_collateral = sum(OrderManager.required_collateral(q) for q in active_buys)
    print(f"\n  Active BUY collateral: ${order_collateral:.2f}")
    print(f"  Budget: ${inv.initial_usdc:.2f}")
    print(f"  Remaining: ${max(0, inv.initial_usdc - order_collateral):.2f}")

    # Without collateral
    q_no_cap = qe.generate_all_quotes(
        0.50, 0.50, "UP", "DN", inv,
        usdc_budget=20.0, order_collateral=0.0)
    # With collateral
    q_capped = qe.generate_all_quotes(
        0.50, 0.50, "UP", "DN", inv,
        usdc_budget=20.0, order_collateral=order_collateral)

    no_cap_up = q_no_cap["up"][0].size if q_no_cap["up"][0] else 0
    capped_up = q_capped["up"][0].size if q_capped["up"][0] else 0
    no_cap_dn = q_no_cap["dn"][0].size if q_no_cap["dn"][0] else 0
    capped_dn = q_capped["dn"][0].size if q_capped["dn"][0] else 0

    print(f"\n  Without collateral: UP bid={no_cap_up:.1f} DN bid={no_cap_dn:.1f}")
    print(f"  With collateral:    UP bid={capped_up:.1f} DN bid={capped_dn:.1f}")

    errors = []
    if capped_up >= no_cap_up and no_cap_up > 0:
        errors.append(f"UP bid not reduced: {capped_up:.1f} >= {no_cap_up:.1f}")
    if capped_dn >= no_cap_dn and no_cap_dn > 0:
        errors.append(f"DN bid not reduced: {capped_dn:.1f} >= {no_cap_dn:.1f}")

    if errors:
        print("\n  FAILED:")
        for e in errors:
            print(f"    - {e}")
        return False
    else:
        print("\n  ALL CHECKS PASSED!")
        print(f"  Budget cap correctly reduces bid sizes when orders are open")
        return True


async def main():
    ok1 = await run_merge_test()
    ok2 = await run_budget_test()

    print("\n" + "=" * 60)
    if ok1 and ok2:
        print("ALL INTEGRATION TESTS PASSED")
    else:
        print("SOME TESTS FAILED")
        sys.exit(1)
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
