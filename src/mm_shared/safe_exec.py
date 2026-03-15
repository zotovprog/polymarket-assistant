"""Gnosis Safe execTransaction helper for Polymarket funder/proxy wallets.

When using a Gnosis Safe as the trading wallet (PM_FUNDER), conditional tokens
are held BY the Safe. merge/redeem must be called FROM the Safe, not the EOA.
This module wraps those calls via Safe.execTransaction().

Requirements:
- EOA (PM_PRIVATE_KEY) must be an owner of the Safe with threshold=1
- web3, eth_abi, eth_account (all come with `pip install web3`)
"""
from __future__ import annotations

import logging

log = logging.getLogger("mm.safe_exec")

# ── Constants ──────────────────────────────────────────────────
POLYGON_RPC = "https://polygon-bor-rpc.publicnode.com"
CHAIN_ID = 137

CTF_CONTRACT = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
USDC_E = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

# EIP-712 type hashes from Safe.sol
DOMAIN_SEPARATOR_TYPEHASH = bytes.fromhex(
    "47e79534a245952e8b16893a336b85a3d9ea9fa8c573f3d803afb92a79469218"
)
SAFE_TX_TYPEHASH = bytes.fromhex(
    "bb8310d486368db6bd6f849402fdd73ad53d316b5a4b2644ad6efe0f941286d8"
)

# ── ABIs ───────────────────────────────────────────────────────
GNOSIS_SAFE_ABI = [
    {
        "name": "execTransaction",
        "type": "function",
        "inputs": [
            {"name": "to", "type": "address"},
            {"name": "value", "type": "uint256"},
            {"name": "data", "type": "bytes"},
            {"name": "operation", "type": "uint8"},
            {"name": "safeTxGas", "type": "uint256"},
            {"name": "baseGas", "type": "uint256"},
            {"name": "gasPrice", "type": "uint256"},
            {"name": "gasToken", "type": "address"},
            {"name": "refundReceiver", "type": "address"},
            {"name": "signatures", "type": "bytes"},
        ],
        "outputs": [{"name": "success", "type": "bool"}],
    },
    {
        "name": "nonce",
        "type": "function",
        "inputs": [],
        "outputs": [{"name": "", "type": "uint256"}],
    },
    {
        "name": "getOwners",
        "type": "function",
        "inputs": [],
        "outputs": [{"name": "", "type": "address[]"}],
    },
    {
        "name": "getThreshold",
        "type": "function",
        "inputs": [],
        "outputs": [{"name": "", "type": "uint256"}],
    },
]

CTF_MERGE_ABI = [
    {
        "name": "mergePositions",
        "type": "function",
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "partition", "type": "uint256[]"},
            {"name": "amount", "type": "uint256"},
        ],
        "outputs": [],
    },
]

CTF_REDEEM_ABI = [
    {
        "name": "redeemPositions",
        "type": "function",
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "indexSets", "type": "uint256[]"},
        ],
        "outputs": [],
    },
]


# ── Helpers ────────────────────────────────────────────────────

def _init_w3(rpc_url: str = POLYGON_RPC):
    from web3 import Web3
    from web3.middleware import ExtraDataToPOAMiddleware

    w3 = Web3(Web3.HTTPProvider(rpc_url))
    try:
        w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
    except Exception:
        pass
    if not w3.is_connected():
        raise ConnectionError("Cannot connect to Polygon RPC")
    return w3


def _get_gas_params(w3) -> dict:
    try:
        current_gas = w3.eth.gas_price
    except Exception:
        current_gas = w3.to_wei(50, "gwei")
    return {
        "maxFeePerGas": max(w3.to_wei(100, "gwei"), current_gas * 2),
        "maxPriorityFeePerGas": max(w3.to_wei(30, "gwei"), current_gas),
    }


def _compute_safe_tx_hash(
    w3,
    safe_address: str,
    to: str,
    value: int,
    data: bytes,
    operation: int,
    safe_tx_gas: int,
    base_gas: int,
    gas_price: int,
    gas_token: str,
    refund_receiver: str,
    nonce: int,
    chain_id: int = CHAIN_ID,
) -> bytes:
    """Compute EIP-712 Safe transaction hash."""
    from eth_abi import encode as abi_encode

    domain_separator = w3.keccak(
        DOMAIN_SEPARATOR_TYPEHASH
        + abi_encode(
            ["uint256", "address"],
            [chain_id, w3.to_checksum_address(safe_address)],
        )
    )

    data_hash = w3.keccak(data) if data else w3.keccak(b"")
    safe_tx_hash = w3.keccak(
        SAFE_TX_TYPEHASH
        + abi_encode(
            ["address", "uint256", "bytes32", "uint8", "uint256",
             "uint256", "uint256", "address", "address", "uint256"],
            [
                w3.to_checksum_address(to),
                value,
                data_hash,
                operation,
                safe_tx_gas,
                base_gas,
                gas_price,
                w3.to_checksum_address(gas_token),
                w3.to_checksum_address(refund_receiver),
                nonce,
            ],
        )
    )

    return w3.keccak(b"\x19\x01" + domain_separator + safe_tx_hash)


def _sign_safe_tx_hash(tx_hash: bytes, private_key: str) -> bytes:
    """Sign Safe tx hash with EOA key. Returns 65 bytes: r || s || v."""
    from eth_account import Account

    sig = Account._sign_hash(tx_hash, private_key)
    return sig.r.to_bytes(32, "big") + sig.s.to_bytes(32, "big") + sig.v.to_bytes(1, "big")


# ── Core: execute call through Safe ───────────────────────────

def safe_exec_transaction(
    private_key: str,
    safe_address: str,
    to: str,
    data: bytes,
    value: int = 0,
    operation: int = 0,
    gas_limit: int = 500_000,
    rpc_url: str = POLYGON_RPC,
) -> dict:
    """Execute a transaction through a Gnosis Safe proxy.

    EOA (private_key) must be an owner with threshold=1.
    """
    key = private_key.strip()
    if not key.startswith("0x"):
        key = f"0x{key}"

    w3 = _init_w3(rpc_url)
    account = w3.eth.account.from_key(key)
    eoa_address = account.address

    safe_addr = w3.to_checksum_address(safe_address)
    safe = w3.eth.contract(address=safe_addr, abi=GNOSIS_SAFE_ABI)

    # Sanity checks
    owners = safe.functions.getOwners().call()
    threshold = safe.functions.getThreshold().call()
    log.info("Safe %s: owners=%s, threshold=%d", safe_addr[:10], owners, threshold)

    if eoa_address not in owners:
        return {"success": False, "error": f"EOA {eoa_address} is not a Safe owner"}
    if threshold != 1:
        return {"success": False, "error": f"Threshold is {threshold}, only threshold=1 supported"}

    safe_nonce = safe.functions.nonce().call()

    # No gas refund params
    safe_tx_gas = 0
    base_gas = 0
    gas_price_param = 0
    gas_token = ZERO_ADDRESS
    refund_receiver = ZERO_ADDRESS

    # Compute EIP-712 hash
    tx_hash = _compute_safe_tx_hash(
        w3,
        safe_address=safe_addr,
        to=w3.to_checksum_address(to),
        value=value,
        data=data,
        operation=operation,
        safe_tx_gas=safe_tx_gas,
        base_gas=base_gas,
        gas_price=gas_price_param,
        gas_token=gas_token,
        refund_receiver=refund_receiver,
        nonce=safe_nonce,
    )

    # Sign with EOA
    signature = _sign_safe_tx_hash(tx_hash, key)

    # Build outer tx
    gas_params = _get_gas_params(w3)
    eoa_nonce = w3.eth.get_transaction_count(eoa_address)

    outer_tx = safe.functions.execTransaction(
        w3.to_checksum_address(to),
        value,
        data,
        operation,
        safe_tx_gas,
        base_gas,
        gas_price_param,
        gas_token,
        refund_receiver,
        signature,
    ).build_transaction({
        "chainId": CHAIN_ID,
        "from": eoa_address,
        "nonce": eoa_nonce,
        "gas": gas_limit,
        **gas_params,
    })

    signed_outer = w3.eth.account.sign_transaction(outer_tx, private_key=key)
    sent_hash = w3.eth.send_raw_transaction(signed_outer.raw_transaction)
    log.info("Safe execTransaction submitted: %s", sent_hash.hex())

    receipt = w3.eth.wait_for_transaction_receipt(sent_hash, timeout=180)
    if receipt["status"] == 1:
        log.info("Safe execTransaction OK: %s (gas used: %d)", sent_hash.hex(), receipt["gasUsed"])
        return {"success": True, "tx_hash": sent_hash.hex(), "gas_used": receipt["gasUsed"]}
    else:
        log.error("Safe execTransaction REVERTED: %s", sent_hash.hex())
        return {"success": False, "tx_hash": sent_hash.hex(), "error": "tx reverted"}


# ── High-level: merge via Safe ────────────────────────────────

def safe_merge_positions(
    private_key: str,
    safe_address: str,
    condition_id: str,
    amount_shares: float,
    rpc_url: str = POLYGON_RPC,
) -> dict:
    """Merge YES+NO pairs back into USDC, via Safe.execTransaction()."""
    w3 = _init_w3(rpc_url)

    ctf = w3.eth.contract(
        address=w3.to_checksum_address(CTF_CONTRACT),
        abi=CTF_MERGE_ABI,
    )

    amount_wei = int(amount_shares * 1e6)
    if amount_wei <= 0:
        return {"success": False, "error": "amount too small"}

    cond_hex = condition_id.replace("0x", "").strip()
    if len(cond_hex) != 64:
        return {"success": False, "error": f"condition_id must be 32 bytes, got {len(cond_hex) // 2}"}

    _encode = getattr(ctf, 'encode_abi', None) or getattr(ctf, 'encodeABI')
    inner_data = _encode(
        fn_name="mergePositions",
        args=[
            w3.to_checksum_address(USDC_E),
            b"\x00" * 32,
            bytes.fromhex(cond_hex),
            [1, 2],
            amount_wei,
        ],
    )

    log.info("Merging %.2f pairs via Safe %s (condition=%s...)",
             amount_shares, safe_address[:10], cond_hex[:12])

    result = safe_exec_transaction(
        private_key=private_key,
        safe_address=safe_address,
        to=CTF_CONTRACT,
        data=bytes.fromhex(inner_data[2:]),
        gas_limit=300_000,
        rpc_url=rpc_url,
    )

    if result.get("success"):
        result["amount_usdc"] = amount_shares
    return result


# ── High-level: redeem via Safe ───────────────────────────────

def safe_redeem_positions(
    private_key: str,
    safe_address: str,
    condition_id: str,
    rpc_url: str = POLYGON_RPC,
) -> dict:
    """Redeem winning resolved conditional tokens back into USDC, via Safe."""
    w3 = _init_w3(rpc_url)

    ctf = w3.eth.contract(
        address=w3.to_checksum_address(CTF_CONTRACT),
        abi=CTF_REDEEM_ABI,
    )

    cond_hex = condition_id.replace("0x", "").strip()
    if len(cond_hex) != 64:
        return {"success": False, "error": f"condition_id must be 32 bytes, got {len(cond_hex) // 2}"}

    _encode = getattr(ctf, 'encode_abi', None) or getattr(ctf, 'encodeABI')
    inner_data = _encode(
        fn_name="redeemPositions",
        args=[
            w3.to_checksum_address(USDC_E),
            b"\x00" * 32,
            bytes.fromhex(cond_hex),
            [1, 2],
        ],
    )

    log.info("Redeeming via Safe %s (condition=%s...)", safe_address[:10], cond_hex[:12])

    return safe_exec_transaction(
        private_key=private_key,
        safe_address=safe_address,
        to=CTF_CONTRACT,
        data=bytes.fromhex(inner_data[2:]),
        gas_limit=300_000,
        rpc_url=rpc_url,
    )
