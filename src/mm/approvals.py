"""On-chain token approvals for Polymarket neg-risk binary options.

Before SELL orders work, the CTF contract must authorize the exchange operators
to transfer conditional tokens on your behalf. This requires actual on-chain
setApprovalForAll transactions on Polygon (costs gas in POL/MATIC).

These approvals are one-time - once set, they persist until explicitly revoked.
"""
from __future__ import annotations

import logging

log = logging.getLogger("mm.approvals")

# Polygon contract addresses
USDC_E = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
CTF_CONTRACT = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"  # Conditional Tokens

# Exchange contracts that need approval to transfer tokens
NEG_RISK_EXCHANGE = "0xC5d563A36AE78145C45a50134d48A1215220f80a"
NEG_RISK_ADAPTER = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"
CTF_EXCHANGE = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"

# Minimal ABIs
ERC20_APPROVE_ABI = [
    {
        "name": "approve",
        "type": "function",
        "inputs": [
            {"name": "_spender", "type": "address"},
            {"name": "_value", "type": "uint256"},
        ],
        "outputs": [{"name": "", "type": "bool"}],
    },
    {
        "name": "allowance",
        "type": "function",
        "inputs": [
            {"name": "_owner", "type": "address"},
            {"name": "_spender", "type": "address"},
        ],
        "outputs": [{"name": "", "type": "uint256"}],
    },
]

ERC1155_APPROVAL_ABI = [
    {
        "name": "setApprovalForAll",
        "type": "function",
        "inputs": [
            {"name": "operator", "type": "address"},
            {"name": "approved", "type": "bool"},
        ],
        "outputs": [],
    },
    {
        "name": "isApprovedForAll",
        "type": "function",
        "inputs": [
            {"name": "account", "type": "address"},
            {"name": "operator", "type": "address"},
        ],
        "outputs": [{"name": "", "type": "bool"}],
    },
]

# Max uint256 for unlimited approval
MAX_UINT256 = 2**256 - 1

POLYGON_RPC = "https://polygon-bor-rpc.publicnode.com"


def _get_gas_params(w3) -> dict:
    """Dynamic EIP-1559 gas params based on current network price."""
    try:
        current_gas = w3.eth.gas_price
    except Exception:
        current_gas = w3.to_wei(50, "gwei")
    return {
        "maxFeePerGas": max(w3.to_wei(100, "gwei"), current_gas * 2),
        "maxPriorityFeePerGas": max(w3.to_wei(30, "gwei"), current_gas),
    }


def _do_approvals(private_key: str, rpc_url: str = POLYGON_RPC) -> dict:
    """Check and set required on-chain approvals for neg-risk trading."""
    try:
        from web3 import Web3
        from web3.middleware import ExtraDataToPOAMiddleware
    except ImportError:
        log.error("web3 not installed - cannot set on-chain approvals")
        return {"error": "web3 not installed"}

    key = private_key.strip()
    if not key:
        return {"error": "missing private key"}
    if not key.startswith("0x"):
        key = f"0x{key}"

    result: dict[str, str | bool] = {}
    all_ok = True

    w3 = Web3(Web3.HTTPProvider(rpc_url))
    try:
        w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
    except Exception:
        # Already injected or not needed for this web3 version.
        pass

    if not w3.is_connected():
        log.error("Cannot connect to Polygon RPC")
        return {"error": "RPC connection failed"}

    account = w3.eth.account.from_key(key)
    pub_key = account.address
    log.info("Checking approvals for %s", pub_key)

    balance = w3.eth.get_balance(pub_key)
    pol_balance = float(w3.from_wei(balance, "ether"))
    log.info("POL balance: %.4f", pol_balance)
    if pol_balance < 0.01:
        log.warning("Low POL balance (%.4f) - may not have enough for gas", pol_balance)

    ctf = w3.eth.contract(
        address=w3.to_checksum_address(CTF_CONTRACT),
        abi=ERC1155_APPROVAL_ABI,
    )
    usdc = w3.eth.contract(
        address=w3.to_checksum_address(USDC_E),
        abi=ERC20_APPROVE_ABI,
    )

    operators = [
        ("CTF_Exchange", CTF_EXCHANGE),
        ("Neg_Risk_Exchange", NEG_RISK_EXCHANGE),
        ("Neg_Risk_Adapter", NEG_RISK_ADAPTER),
    ]

    for name, operator_addr in operators:
        operator = w3.to_checksum_address(operator_addr)
        is_approved = ctf.functions.isApprovedForAll(pub_key, operator).call()

        if is_approved:
            log.info("CTF -> %s: OK", name)
            result[f"ctf_{name}"] = "ok"
            continue

        log.info("CTF -> %s: setting approval...", name)
        try:
            nonce = w3.eth.get_transaction_count(pub_key)
            tx = ctf.functions.setApprovalForAll(operator, True).build_transaction({
                "chainId": 137,
                "from": pub_key,
                "nonce": nonce,
                "gas": 100_000,
                **_get_gas_params(w3),
            })
            signed = w3.eth.account.sign_transaction(tx, private_key=key)
            tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
            receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
            if receipt["status"] == 1:
                log.info("CTF -> %s: approved (tx=%s...)", name, tx_hash.hex()[:16])
                result[f"ctf_{name}"] = "approved"
            else:
                log.error("CTF -> %s: tx failed", name)
                result[f"ctf_{name}"] = "tx_failed"
                all_ok = False
        except Exception as e:
            log.error("CTF -> %s: error: %s", name, e)
            result[f"ctf_{name}"] = "error"
            all_ok = False

    for name, operator_addr in operators:
        operator = w3.to_checksum_address(operator_addr)
        allowance = usdc.functions.allowance(pub_key, operator).call()

        if allowance > 10**18:
            log.info("USDC -> %s: OK", name)
            result[f"usdc_{name}"] = "ok"
            continue

        log.info("USDC -> %s: setting approval...", name)
        try:
            nonce = w3.eth.get_transaction_count(pub_key)
            tx = usdc.functions.approve(operator, MAX_UINT256).build_transaction({
                "chainId": 137,
                "from": pub_key,
                "nonce": nonce,
                "gas": 100_000,
                **_get_gas_params(w3),
            })
            signed = w3.eth.account.sign_transaction(tx, private_key=key)
            tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
            receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
            if receipt["status"] == 1:
                log.info("USDC -> %s: approved (tx=%s...)", name, tx_hash.hex()[:16])
                result[f"usdc_{name}"] = "approved"
            else:
                log.error("USDC -> %s: tx failed", name)
                result[f"usdc_{name}"] = "tx_failed"
                all_ok = False
        except Exception as e:
            log.error("USDC -> %s: error: %s", name, e)
            result[f"usdc_{name}"] = "error"
            all_ok = False

    result["all_ok"] = all_ok
    return result
