"""Tests for Safe-based merge/redeem execution."""
import os, sys
from pathlib import Path
BASE = Path(__file__).resolve().parent.parent
SRC = BASE / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))

from unittest.mock import MagicMock, patch, PropertyMock
import pytest

from mm_shared.safe_exec import (
    _compute_safe_tx_hash,
    _sign_safe_tx_hash,
    safe_merge_positions,
    safe_redeem_positions,
    safe_exec_transaction,
)


FAKE_KEY = "0x" + "ab" * 32
FAKE_SAFE = "0x" + "cd" * 20
FAKE_CONDITION = "0x" + "ef" * 32


class TestComputeSafeTxHash:
    def test_returns_32_bytes(self):
        w3 = MagicMock()
        w3.to_checksum_address = lambda x: x
        w3.keccak = lambda data: b"\x00" * 32
        result = _compute_safe_tx_hash(
            w3, FAKE_SAFE, FAKE_SAFE, 0, b"", 0, 0, 0, 0,
            "0x" + "00" * 20, "0x" + "00" * 20, 0,
        )
        assert len(result) == 32


class TestSignSafeTxHash:
    def test_returns_65_bytes(self):
        from eth_account import Account
        acct = Account.create()
        sig = _sign_safe_tx_hash(b"\x00" * 32, acct.key.hex())
        assert len(sig) == 65
        # v should be 27 or 28
        assert sig[-1] in (27, 28)


class TestSafeExecTransaction:
    @patch("mm_shared.safe_exec._init_w3")
    def test_rejects_non_owner(self, mock_init):
        w3 = MagicMock()
        mock_init.return_value = w3

        # EOA address from FAKE_KEY
        from eth_account import Account
        eoa = Account.from_key(FAKE_KEY)
        w3.eth.account.from_key.return_value = eoa

        safe_contract = MagicMock()
        safe_contract.functions.getOwners.return_value.call.return_value = ["0xDEAD"]
        safe_contract.functions.getThreshold.return_value.call.return_value = 1
        w3.eth.contract.return_value = safe_contract
        w3.to_checksum_address = lambda x: x

        result = safe_exec_transaction(FAKE_KEY, FAKE_SAFE, FAKE_SAFE, b"\x00")
        assert result["success"] is False
        assert "not a Safe owner" in result["error"]

    @patch("mm_shared.safe_exec._init_w3")
    def test_rejects_threshold_gt_1(self, mock_init):
        w3 = MagicMock()
        mock_init.return_value = w3

        from eth_account import Account
        eoa = Account.from_key(FAKE_KEY)
        w3.eth.account.from_key.return_value = eoa

        safe_contract = MagicMock()
        safe_contract.functions.getOwners.return_value.call.return_value = [eoa.address]
        safe_contract.functions.getThreshold.return_value.call.return_value = 2
        w3.eth.contract.return_value = safe_contract
        w3.to_checksum_address = lambda x: x

        result = safe_exec_transaction(FAKE_KEY, FAKE_SAFE, FAKE_SAFE, b"\x00")
        assert result["success"] is False
        assert "Threshold is 2" in result["error"]


class TestSafeMergePositions:
    def test_rejects_zero_amount(self):
        result = safe_merge_positions(FAKE_KEY, FAKE_SAFE, FAKE_CONDITION, 0.0)
        assert result["success"] is False
        assert "too small" in result["error"]

    def test_rejects_bad_condition_id(self):
        result = safe_merge_positions(FAKE_KEY, FAKE_SAFE, "0xabc", 5.0)
        assert result["success"] is False
        assert "32 bytes" in result["error"]


class TestSafeRedeemPositions:
    def test_rejects_bad_condition_id(self):
        result = safe_redeem_positions(FAKE_KEY, FAKE_SAFE, "short")
        assert result["success"] is False
        assert "32 bytes" in result["error"]
