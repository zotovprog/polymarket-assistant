from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class PaperExchangeDiagnostics:
    order_count: int
    balance: float
    token_balances: dict[str, float]


def snapshot_paper_exchange(client: Any) -> PaperExchangeDiagnostics | None:
    if not hasattr(client, "_orders"):
        return None
    balances = getattr(client, "_mock_token_balances", None)
    return PaperExchangeDiagnostics(
        order_count=len(getattr(client, "_orders", {})),
        balance=float(getattr(client, "balance", 0.0)),
        token_balances=dict(balances or {}),
    )
