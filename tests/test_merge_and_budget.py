"""Tests for CTF Merge Exit and USDC Budget Cap Fix.

Tests cover:
1. fetch_pm_tokens 3-tuple return
2. merge_positions condition_id validation (approvals.py)
3. OrderManager.merge_positions paper/live branching
4. Phase 0 merge in _liquidate_inventory
5. order_collateral in generate_all_quotes budget formula
6. MarketInfo condition_id propagation
"""
import asyncio
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
import pytest
import pytest_asyncio

# Force asyncio mode for all async tests
pytestmark = pytest.mark.anyio

# ── Path setup ────────────────────────────────────────────────
BASE = Path(__file__).resolve().parent.parent
SRC = BASE / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))


# ═══════════════════════════════════════════════════════════════
# 1. feeds.fetch_pm_tokens — 3-tuple
# ═══════════════════════════════════════════════════════════════

class TestFetchPmTokens:
    """Test that fetch_pm_tokens returns 3-tuple with condition_id."""

    @patch("feeds.fetch_pm_event_data")
    def test_success_returns_3_tuple(self, mock_event):
        import feeds
        mock_event.return_value = {
            "markets": [{
                "clobTokenIds": '["tok_up_123", "tok_dn_456"]',
                "conditionId": "0xabc123def456" + "0" * 52,
            }],
        }
        result = feeds.fetch_pm_tokens("BTC", "5m")
        assert len(result) == 3
        up, dn, cond = result
        assert up == "tok_up_123"
        assert dn == "tok_dn_456"
        assert cond == "0xabc123def456" + "0" * 52

    @patch("feeds.fetch_pm_event_data")
    def test_missing_condition_id_returns_empty_string(self, mock_event):
        import feeds
        mock_event.return_value = {
            "markets": [{
                "clobTokenIds": '["tok_up", "tok_dn"]',
                # No conditionId key
            }],
        }
        _, _, cond = feeds.fetch_pm_tokens("BTC", "5m")
        assert cond == ""

    @patch("feeds.fetch_pm_event_data")
    def test_no_event_data_returns_3_nones(self, mock_event):
        import feeds
        mock_event.return_value = None
        result = feeds.fetch_pm_tokens("BTC", "5m")
        assert result == (None, None, None)

    @patch("feeds.fetch_pm_event_data")
    def test_bad_json_returns_3_nones(self, mock_event):
        import feeds
        mock_event.return_value = {"markets": [{"clobTokenIds": "not json"}]}
        result = feeds.fetch_pm_tokens("BTC", "5m")
        assert result == (None, None, None)

    @patch("feeds.fetch_pm_event_data")
    def test_unpack_3_tuple(self, mock_event):
        """Callers must be able to unpack as 3-tuple."""
        import feeds
        mock_event.return_value = {
            "markets": [{
                "clobTokenIds": '["a", "b"]',
                "conditionId": "cond_abc",
            }],
        }
        up, dn, cond = feeds.fetch_pm_tokens("BTC", "5m")
        assert up == "a"
        assert dn == "b"
        assert cond == "cond_abc"


# ═══════════════════════════════════════════════════════════════
# 2. approvals.merge_positions — condition_id validation
# ═══════════════════════════════════════════════════════════════

class TestMergePositionsValidation:
    """Test condition_id hex validation in merge_positions."""

    def test_invalid_hex_returns_error(self):
        from mm.approvals import merge_positions
        result = merge_positions("0x" + "ab" * 32, "not_hex_at_all!", 10.0)
        assert result["success"] is False
        assert "invalid condition_id" in result.get("error", "").lower() or \
               "condition_id must be 32 bytes" in result.get("error", "")

    def test_short_condition_id_returns_error(self):
        from mm.approvals import merge_positions
        result = merge_positions("0x" + "ab" * 32, "0xabcdef", 10.0)
        assert result["success"] is False
        assert "32 bytes" in result.get("error", "")

    def test_empty_key_returns_error(self):
        from mm.approvals import merge_positions
        cond = "0x" + "ab" * 32
        result = merge_positions("", cond, 10.0)
        assert result["success"] is False
        assert "missing private key" in result.get("error", "")

    def test_zero_amount_returns_error(self):
        from mm.approvals import merge_positions
        cond = "0x" + "ab" * 32
        result = merge_positions("0x" + "cd" * 32, cond, 0.0)
        assert result["success"] is False
        assert "amount too small" in result.get("error", "")

    def test_valid_condition_id_with_0x_prefix(self):
        """Valid 32-byte hex with 0x prefix should pass validation (will fail at RPC)."""
        from mm.approvals import merge_positions
        cond = "0x" + "ab" * 32  # 64 hex chars = 32 bytes
        # This will fail at w3.is_connected() since no real RPC, but validates hex OK
        result = merge_positions("0x" + "cd" * 32, cond, 10.0)
        # Should NOT fail with "invalid condition_id" — will fail at RPC
        assert "invalid condition_id" not in result.get("error", "")

    def test_valid_condition_id_without_0x_prefix(self):
        """Valid 32-byte hex without 0x prefix."""
        from mm.approvals import merge_positions
        cond = "ab" * 32
        result = merge_positions("0x" + "cd" * 32, cond, 10.0)
        assert "invalid condition_id" not in result.get("error", "")


# ═══════════════════════════════════════════════════════════════
# 3. OrderManager.merge_positions — paper vs live
# ═══════════════════════════════════════════════════════════════

class TestOrderManagerMerge:
    """Test OrderManager.merge_positions for paper and live modes."""

    def _make_mock_client(self, usdc: float = 100.0):
        """Create a mock CLOB client (paper mode)."""
        client = MagicMock()
        client._orders = {}
        client._usdc_balance = usdc
        return client

    def _make_om(self, client):
        from mm.order_manager import OrderManager
        from mm.mm_config import MMConfig
        return OrderManager(client, MMConfig())

    @pytest.mark.anyio
    async def test_paper_merge_credits_usdc(self):
        client = self._make_mock_client(usdc=50.0)
        om = self._make_om(client)
        result = await om.merge_positions("cond_abc", 25.0, "")
        assert result["success"] is True
        assert result["amount_usdc"] == 25.0
        assert client._usdc_balance == 75.0  # 50 + 25

    @pytest.mark.anyio
    async def test_paper_merge_zero_amount(self):
        client = self._make_mock_client(usdc=50.0)
        om = self._make_om(client)
        result = await om.merge_positions("cond_abc", 0.0, "")
        assert result["success"] is True
        assert client._usdc_balance == 50.0  # 50 + 0

    @pytest.mark.anyio
    async def test_live_merge_delegates_to_approvals(self):
        """Live mode should call approvals.merge_positions via thread."""
        from mm.order_manager import OrderManager
        from mm.mm_config import MMConfig
        client = MagicMock(spec=[])  # No _orders → live mode
        om = OrderManager(client, MMConfig())
        with patch("mm.order_manager.asyncio.to_thread",
                   new_callable=AsyncMock,
                   return_value={"success": True, "tx_hash": "0x123", "amount_usdc": 10.0}) as mock_thread:
            result = await om.merge_positions("cond_abc", 10.0, "0xkey123")
            assert result["success"] is True
            mock_thread.assert_called_once()


# ═══════════════════════════════════════════════════════════════
# 4. Phase 0 merge in _liquidate_inventory
# ═══════════════════════════════════════════════════════════════

class TestLiquidatePhase0Merge:
    """Test Phase 0 merge logic in MarketMaker._liquidate_inventory."""

    def _make_mm(self, up_bal=10.0, dn_bal=8.0, condition_id="abc123" + "0" * 58,
                 up_shares=10.0, dn_shares=8.0, usdc=50.0):
        """Create a MarketMaker with mock client and preset balances."""
        from mm.market_maker import MarketMaker
        from mm.mm_config import MMConfig
        from mm.types import MarketInfo
        import feeds

        # Mock feed state
        feed_state = feeds.State()
        feed_state.mid = 100000.0
        feed_state.bids = [(99999, 1)]
        feed_state.asks = [(100001, 1)]

        # Mock client (paper mode)
        client = MagicMock()
        client._orders = {}
        client._usdc_balance = usdc
        client.cancel_all = MagicMock(return_value={"success": True})

        cfg = MMConfig()
        mm = MarketMaker(feed_state, client, cfg)

        market = MarketInfo(
            coin="BTC", timeframe="5m",
            up_token_id="up_tok_123", dn_token_id="dn_tok_456",
            strike=100000.0, window_start=0, window_end=0,
            condition_id=condition_id,
        )
        mm.set_market(market)
        mm.inventory.up_shares = up_shares
        mm.inventory.dn_shares = dn_shares
        mm.inventory.usdc = usdc
        mm.inventory.initial_usdc = 100.0
        mm.inventory.up_cost.record_buy(0.50, up_shares, 0.0)
        mm.inventory.dn_cost.record_buy(0.50, dn_shares, 0.0)

        # Set mock token balances
        mm.order_mgr._mock_token_balances["up_tok_123"] = up_bal
        mm.order_mgr._mock_token_balances["dn_tok_456"] = dn_bal

        return mm

    @pytest.mark.anyio
    async def test_merge_both_sides(self):
        """Should merge min(UP, DN) pairs."""
        mm = self._make_mm(up_bal=10.0, dn_bal=8.0)
        await mm._liquidate_inventory()

        # Merged 8 pairs (min of 10, 8)
        assert mm.inventory.up_shares == pytest.approx(2.0, abs=0.1)
        assert mm.inventory.dn_shares == pytest.approx(0.0, abs=0.1)
        assert mm.inventory.usdc == pytest.approx(58.0, abs=0.1)  # 50 + 8

    @pytest.mark.anyio
    async def test_merge_equal_balances(self):
        """Equal balances → merge all."""
        mm = self._make_mm(up_bal=5.0, dn_bal=5.0,
                          up_shares=5.0, dn_shares=5.0, usdc=20.0)
        await mm._liquidate_inventory()

        assert mm.inventory.up_shares == pytest.approx(0.0, abs=0.1)
        assert mm.inventory.dn_shares == pytest.approx(0.0, abs=0.1)
        assert mm.inventory.usdc == pytest.approx(25.0, abs=0.1)  # 20 + 5

    @pytest.mark.anyio
    async def test_merge_skipped_below_threshold(self):
        """Merge skipped if min(UP, DN) < 1.0."""
        mm = self._make_mm(up_bal=0.5, dn_bal=0.5,
                          up_shares=0.5, dn_shares=0.5, usdc=20.0)
        await mm._liquidate_inventory()

        # No merge — shares unchanged (liquidation SELLs may happen)
        # usdc should not increase by merge amount
        assert mm.inventory.usdc <= 20.1  # No merge credit

    @pytest.mark.anyio
    async def test_merge_skipped_no_condition_id(self):
        """Merge skipped if condition_id is empty."""
        mm = self._make_mm(up_bal=10.0, dn_bal=10.0, condition_id="",
                          up_shares=10.0, dn_shares=10.0, usdc=20.0)
        await mm._liquidate_inventory()

        # No merge — inventory should not be reduced by merge
        assert mm.inventory.usdc <= 20.1

    @pytest.mark.anyio
    async def test_merge_does_not_go_negative(self):
        """Inventory stays >= 0 even if on-chain > internal tracking."""
        # Internal shares less than on-chain balance
        mm = self._make_mm(up_bal=10.0, dn_bal=10.0,
                          up_shares=5.0, dn_shares=5.0, usdc=20.0)
        await mm._liquidate_inventory()

        # Merged 10 (from on-chain), but internal was only 5
        # max(0.0, 5 - 10) = 0.0
        assert mm.inventory.up_shares >= 0.0
        assert mm.inventory.dn_shares >= 0.0

    @pytest.mark.anyio
    async def test_mock_balances_updated_after_merge(self):
        """Mock token balances reduced after merge."""
        mm = self._make_mm(up_bal=10.0, dn_bal=8.0)
        await mm._liquidate_inventory()

        # After merging 8 pairs:
        assert mm.order_mgr._mock_token_balances["up_tok_123"] == pytest.approx(2.0, abs=0.1)
        assert mm.order_mgr._mock_token_balances["dn_tok_456"] == pytest.approx(0.0, abs=0.1)

    @pytest.mark.anyio
    async def test_cost_basis_updated_after_merge(self):
        """Cost basis record_sell called for merged amount."""
        mm = self._make_mm(up_bal=10.0, dn_bal=10.0,
                          up_shares=10.0, dn_shares=10.0, usdc=20.0)
        await mm._liquidate_inventory()

        # After merging 10: cost basis shares should be 0
        assert mm.inventory.up_cost.total_shares == pytest.approx(0.0, abs=0.1)
        assert mm.inventory.dn_cost.total_shares == pytest.approx(0.0, abs=0.1)


# ═══════════════════════════════════════════════════════════════
# 5. order_collateral in generate_all_quotes
# ═══════════════════════════════════════════════════════════════

class TestOrderCollateralBudget:
    """Test USDC budget cap accounts for open order collateral."""

    def _make_inventory(self, up=0.0, dn=0.0, usdc=100.0, initial=100.0):
        from mm.types import Inventory
        inv = Inventory(up_shares=up, dn_shares=dn, usdc=usdc, initial_usdc=initial)
        if up > 0:
            inv.up_cost.record_buy(0.50, up, 0.0)
        if dn > 0:
            inv.dn_cost.record_buy(0.50, dn, 0.0)
        return inv

    def test_no_collateral_full_budget(self):
        """Without order_collateral, full budget available."""
        from mm.quote_engine import QuoteEngine
        from mm.mm_config import MMConfig
        qe = QuoteEngine(MMConfig())
        inv = self._make_inventory(up=0, dn=0, usdc=100, initial=100)

        quotes = qe.generate_all_quotes(
            0.50, 0.50, "up_tok", "dn_tok", inv,
            usdc_budget=100.0, order_collateral=0.0)

        up_bid, _ = quotes["up"]
        dn_bid, _ = quotes["dn"]
        assert up_bid is not None
        assert dn_bid is not None

    def test_collateral_reduces_budget(self):
        """order_collateral reduces available budget → smaller bids."""
        from mm.quote_engine import QuoteEngine
        from mm.mm_config import MMConfig
        qe = QuoteEngine(MMConfig())
        inv = self._make_inventory(up=0, dn=0, usdc=100, initial=100)

        # Full budget
        q_full = qe.generate_all_quotes(
            0.50, 0.50, "up_tok", "dn_tok", inv,
            usdc_budget=100.0, order_collateral=0.0)
        # Budget with 95 collateral locked → only $5 remaining → $2.5 per side
        q_capped = qe.generate_all_quotes(
            0.50, 0.50, "up_tok", "dn_tok", inv,
            usdc_budget=100.0, order_collateral=95.0)

        full_up_size = q_full["up"][0].size if q_full["up"][0] else 0
        capped_up_size = q_capped["up"][0].size if q_capped["up"][0] else 0

        assert capped_up_size < full_up_size

    def test_collateral_exceeds_budget_no_bids(self):
        """If collateral + locked >= budget → no bids generated."""
        from mm.quote_engine import QuoteEngine
        from mm.mm_config import MMConfig
        qe = QuoteEngine(MMConfig())
        inv = self._make_inventory(up=0, dn=0, usdc=100, initial=100)

        quotes = qe.generate_all_quotes(
            0.50, 0.50, "up_tok", "dn_tok", inv,
            usdc_budget=100.0, order_collateral=100.0)

        # Both bids should be None (no budget remaining)
        assert quotes["up"][0] is None
        assert quotes["dn"][0] is None

    def test_collateral_with_existing_inventory(self):
        """Budget = initial_usdc - position_cost - order_collateral."""
        from mm.quote_engine import QuoteEngine
        from mm.mm_config import MMConfig
        qe = QuoteEngine(MMConfig())
        # 50 shares * 0.50 avg = $25 locked per side = $50 total
        inv = self._make_inventory(up=50, dn=50, usdc=0, initial=100)

        # Budget: 100 - 25 (up) - 25 (dn) - 30 (orders) = 20
        quotes = qe.generate_all_quotes(
            0.50, 0.50, "up_tok", "dn_tok", inv,
            usdc_budget=100.0, order_collateral=30.0)

        # Should have small bids (20/2 = $10 per side)
        up_bid = quotes["up"][0]
        if up_bid is not None:
            assert up_bid.size * up_bid.price <= 11.0  # ~$10 + rounding

    def test_zero_budget_means_no_limit(self):
        """usdc_budget=0 means no budget cap (unlimited)."""
        from mm.quote_engine import QuoteEngine
        from mm.mm_config import MMConfig
        qe = QuoteEngine(MMConfig())
        inv = self._make_inventory(up=0, dn=0, usdc=1000, initial=0)

        quotes = qe.generate_all_quotes(
            0.50, 0.50, "up_tok", "dn_tok", inv,
            usdc_budget=0.0, order_collateral=999.0)

        # No limit → bids should exist
        assert quotes["up"][0] is not None
        assert quotes["dn"][0] is not None

    def test_required_collateral_buy(self):
        """BUY collateral = size * price."""
        from mm.order_manager import OrderManager
        from mm.types import Quote
        q = Quote(side="BUY", token_id="tok", price=0.45, size=20.0)
        assert OrderManager.required_collateral(q) == pytest.approx(9.0)

    def test_required_collateral_sell(self):
        """SELL collateral = size * (1 - price)."""
        from mm.order_manager import OrderManager
        from mm.types import Quote
        q = Quote(side="SELL", token_id="tok", price=0.45, size=20.0)
        assert OrderManager.required_collateral(q) == pytest.approx(11.0)


# ═══════════════════════════════════════════════════════════════
# 6. MarketInfo condition_id propagation
# ═══════════════════════════════════════════════════════════════

class TestMarketInfoConditionId:
    """Test that condition_id flows through MarketInfo correctly."""

    def test_condition_id_default_empty(self):
        from mm.types import MarketInfo
        m = MarketInfo(coin="BTC", timeframe="5m",
                      up_token_id="u", dn_token_id="d",
                      strike=100.0, window_start=0, window_end=0)
        assert m.condition_id == ""

    def test_condition_id_set(self):
        from mm.types import MarketInfo
        cond = "0x" + "ab" * 32
        m = MarketInfo(coin="BTC", timeframe="5m",
                      up_token_id="u", dn_token_id="d",
                      strike=100.0, window_start=0, window_end=0,
                      condition_id=cond)
        assert m.condition_id == cond

    def test_condition_id_truthy_for_guard(self):
        """Non-empty condition_id should be truthy (used in if guard)."""
        from mm.types import MarketInfo
        m = MarketInfo(coin="BTC", timeframe="5m",
                      up_token_id="u", dn_token_id="d",
                      strike=100.0, window_start=0, window_end=0,
                      condition_id="abc")
        assert bool(m.condition_id) is True

    def test_empty_condition_id_falsy(self):
        from mm.types import MarketInfo
        m = MarketInfo(coin="BTC", timeframe="5m",
                      up_token_id="u", dn_token_id="d",
                      strike=100.0, window_start=0, window_end=0,
                      condition_id="")
        assert bool(m.condition_id) is False


# ═══════════════════════════════════════════════════════════════
# 7. Integration scenario: full paper trading cycle
# ═══════════════════════════════════════════════════════════════

class TestIntegrationPaperMerge:
    """End-to-end: buy UP+DN, then merge during liquidation."""

    @pytest.mark.anyio
    async def test_buy_then_merge_paper(self):
        """Simulate: buy UP and DN, then liquidate → merge should fire first."""
        from mm.market_maker import MarketMaker
        from mm.mm_config import MMConfig
        from mm.types import MarketInfo, Fill
        import feeds
        import time

        feed_state = feeds.State()
        feed_state.mid = 100000.0
        feed_state.bids = [(99999, 1)]
        feed_state.asks = [(100001, 1)]

        # Paper client
        client = MagicMock()
        client._orders = {}
        client._usdc_balance = 100.0
        client.cancel_all = MagicMock(return_value={"success": True})

        cfg = MMConfig()
        mm = MarketMaker(feed_state, client, cfg)

        cond_id = "a1b2c3d4e5f6" + "0" * 52  # 64 hex chars
        market = MarketInfo(
            coin="BTC", timeframe="5m",
            up_token_id="UP_TOK", dn_token_id="DN_TOK",
            strike=100000.0,
            window_start=time.time() - 100,
            window_end=time.time() - 1,  # Expired
            condition_id=cond_id,
        )
        mm.set_market(market)

        # Simulate having bought both sides
        mm.inventory.up_shares = 20.0
        mm.inventory.dn_shares = 15.0
        mm.inventory.usdc = 65.0  # spent 35 buying
        mm.inventory.initial_usdc = 100.0
        mm.inventory.up_cost.record_buy(0.50, 20.0, 0.0)
        mm.inventory.dn_cost.record_buy(0.50, 15.0, 0.0)
        mm.order_mgr._mock_token_balances["UP_TOK"] = 20.0
        mm.order_mgr._mock_token_balances["DN_TOK"] = 15.0

        # Run liquidation
        await mm._liquidate_inventory()

        # Phase 0: merged 15 pairs (min of 20, 15)
        assert mm.inventory.up_shares == pytest.approx(5.0, abs=0.5)
        assert mm.inventory.dn_shares == pytest.approx(0.0, abs=0.5)
        assert mm.inventory.usdc == pytest.approx(80.0, abs=0.5)  # 65 + 15

        # Mock USDC should also reflect merge
        assert client._usdc_balance == pytest.approx(115.0, abs=0.5)  # 100 + 15

        # Mock token balances reduced
        assert mm.order_mgr._mock_token_balances["UP_TOK"] == pytest.approx(5.0, abs=0.5)
        assert mm.order_mgr._mock_token_balances["DN_TOK"] == pytest.approx(0.0, abs=0.5)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
