"""
Trace Chain — Bulk Sequential Chain-Following Engine

Given N wallets, follows each wallet's earliest outgoing SOL transfer
hop-by-hop until the chain terminates. Returns N final wallets.

Different from trace_bfs.py (BFS forward/backward):
  - trace_bfs: finds ALL sub-wallets (tree/fan-out)
  - trace_chain: follows ONE path per wallet (sequential chain)

Usage:
    from trace_chain import TraceJobManager
    manager = TraceJobManager()
    job = manager.start_job(chat_id, wallets, after_ts, helius, on_batch_done)
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Optional, Callable, Awaitable

from helius import HeliusClient, is_on_curve
from config import (
    TRACE_LOOP_THRESHOLD,
    TRACE_BATCH_SIZE,
    TRACE_BATCH_THRESHOLD,
)

logger = logging.getLogger(__name__)


# ─── Dataclasses ─────────────────────────────────────────────

@dataclass
class ChainTraceResult:
    """Result of following one wallet's chain to its end."""
    start_wallet: str
    final_wallet: str
    hop_count: int
    wallet_chain: list[str] = field(default_factory=list)
    sig_chain: list[str] = field(default_factory=list)
    stopped_reason: str = ""  # "no_transfer", "loop", "off_curve", "cancelled"


@dataclass
class TraceJob:
    """A running trace job owned by a chat."""
    chat_id: int
    cancelled: bool = False
    wallets: list[str] = field(default_factory=list)


# ─── Core: Follow single chain ──────────────────────────────

async def trace_single_chain(
    helius: HeliusClient,
    wallet: str,
    after_ts: int,
    seen_transitions: set[str],
    cancel_check: Callable[[], bool],
) -> ChainTraceResult:
    """
    Follow one wallet's earliest outgoing SOL transfer, hop by hop.

    Stops when:
      - No outgoing transfer found
      - Loop detected (same wallet visited 20+ times)
      - Destination is off-curve (PDA) — falls back to previous hop
      - Job cancelled

    Args:
        helius: HeliusClient instance
        wallet: Starting wallet address
        after_ts: Unix timestamp — only follow transfers after this time
        seen_transitions: Shared set of "{sig}_{blocktime}_{to}" cursor keys
        cancel_check: Callable that returns True if job was cancelled
    """
    result = ChainTraceResult(
        start_wallet=wallet,
        final_wallet=wallet,
        hop_count=0,
        wallet_chain=[wallet],
        sig_chain=[],
    )

    visit_count: dict[str, int] = {wallet: 1}
    current = wallet

    while True:
        if cancel_check():
            result.stopped_reason = "cancelled"
            break

        transfer = await _get_earliest_outgoing_transfer(
            helius, current, after_ts, seen_transitions,
        )

        if not transfer:
            result.stopped_reason = "no_transfer"
            break

        sig = transfer["signature"]
        blocktime = transfer["timestamp"]
        to_addr = transfer["to_wallet"]
        cursor_key = f"{sig}_{blocktime}_{to_addr}"

        # Add to seen transitions (cursor tracking)
        seen_transitions.add(cursor_key)

        # On-curve check — if off-curve, stop at current wallet
        if not is_on_curve(to_addr):
            result.stopped_reason = "off_curve"
            break

        # Loop detection
        visit_count[to_addr] = visit_count.get(to_addr, 0) + 1
        if visit_count[to_addr] >= TRACE_LOOP_THRESHOLD:
            result.stopped_reason = "loop"
            result.final_wallet = to_addr
            result.wallet_chain.append(to_addr)
            result.sig_chain.append(sig)
            result.hop_count += 1
            break

        # Advance
        result.wallet_chain.append(to_addr)
        result.sig_chain.append(sig)
        result.hop_count += 1
        result.final_wallet = to_addr
        current = to_addr

        # Use the transfer's timestamp as the new after_ts for next hop
        after_ts = blocktime

    return result


async def _get_earliest_outgoing_transfer(
    helius: HeliusClient,
    wallet: str,
    after_ts: int,
    seen_transitions: set[str],
) -> Optional[dict]:
    """
    Get the earliest outgoing SOL transfer from a wallet.

    Unlike get_outgoing_sol_transfers():
      - No MIN_SPLIT_SOL filter (any amount)
      - No on-curve filter (caller handles)
      - Returns single earliest match
      - Uses seen_transitions for cursor dedup

    Returns:
        dict with keys: to_wallet, amount_sol, timestamp, signature
        or None if no transfer found
    """
    txns = await helius.get_parsed_transactions(wallet, limit=50, tx_type="")
    if not txns:
        return None

    # Collect all outgoing native transfers with timestamps
    candidates: list[dict] = []

    for tx in txns:
        tx_source = tx.get("source", "UNKNOWN")
        if tx_source != "SYSTEM_PROGRAM":
            continue

        tx_timestamp = tx.get("timestamp")
        if not tx_timestamp:
            continue

        # Only transfers after the cutoff
        if tx_timestamp < after_ts:
            continue

        signature = tx.get("signature", "")

        for nt in tx.get("nativeTransfers", []):
            from_addr = nt.get("fromUserAccount", "")
            to_addr = nt.get("toUserAccount", "")
            lamports = nt.get("amount", 0)

            if from_addr != wallet:
                continue
            if to_addr == wallet:
                continue
            if lamports <= 0:
                continue

            cursor_key = f"{signature}_{tx_timestamp}_{to_addr}"
            if cursor_key in seen_transitions:
                continue

            candidates.append({
                "to_wallet": to_addr,
                "amount_sol": lamports / 1_000_000_000,
                "timestamp": tx_timestamp,
                "signature": signature,
            })

    if not candidates:
        return None

    # Sort by timestamp ascending, return earliest
    candidates.sort(key=lambda c: c["timestamp"])
    return candidates[0]


# ─── Batch Processing ───────────────────────────────────────

async def trace_batch(
    helius: HeliusClient,
    wallets: list[str],
    after_ts: int,
    cancel_check: Callable[[], bool],
    on_batch_done: Optional[Callable[[list[ChainTraceResult], int], Awaitable[None]]] = None,
) -> list[ChainTraceResult]:
    """
    Trace multiple wallets in batches (sequential to respect rate limits).

    If >= TRACE_BATCH_THRESHOLD wallets → split into batches of TRACE_BATCH_SIZE.
    Otherwise process all at once.

    Args:
        helius: HeliusClient instance
        wallets: List of wallet addresses to trace
        after_ts: Unix timestamp cutoff
        cancel_check: Returns True if job cancelled
        on_batch_done: Callback after each batch completes

    Returns:
        List of ChainTraceResult (one per wallet)
    """
    all_results: list[ChainTraceResult] = []
    seen_transitions: set[str] = set()

    batch_size = TRACE_BATCH_SIZE if len(wallets) >= TRACE_BATCH_THRESHOLD else len(wallets)

    for batch_idx in range(0, len(wallets), batch_size):
        if cancel_check():
            break

        batch = wallets[batch_idx:batch_idx + batch_size]
        batch_results: list[ChainTraceResult] = []

        for wallet in batch:
            if cancel_check():
                break

            result = await trace_single_chain(
                helius, wallet, after_ts, seen_transitions, cancel_check,
            )
            batch_results.append(result)

        all_results.extend(batch_results)

        if on_batch_done and batch_results:
            batch_num = batch_idx // batch_size
            await on_batch_done(batch_results, batch_num)

    return all_results


# ─── Job Manager ─────────────────────────────────────────────

class TraceJobManager:
    """
    Manages one active trace job at a time.
    Per-chat ownership — only the chat that started can cancel.
    """

    def __init__(self):
        self._active_job: Optional[TraceJob] = None
        self._task: Optional[asyncio.Task] = None

    def is_busy(self) -> bool:
        """Check if a trace job is currently running."""
        if self._task and not self._task.done():
            return True
        self._active_job = None
        self._task = None
        return False

    def start_job(
        self,
        chat_id: int,
        wallets: list[str],
        after_ts: int,
        helius: HeliusClient,
        on_batch_done: Optional[Callable[[list[ChainTraceResult], int], Awaitable[None]]] = None,
    ) -> TraceJob:
        """Start a new trace job. Raises if already busy."""
        if self.is_busy():
            raise RuntimeError("A trace job is already running")

        job = TraceJob(chat_id=chat_id, wallets=wallets)
        self._active_job = job

        async def _run():
            return await trace_batch(
                helius, wallets, after_ts,
                cancel_check=lambda: job.cancelled,
                on_batch_done=on_batch_done,
            )

        self._task = asyncio.create_task(_run())

        def _handle_task_exception(task: asyncio.Task) -> None:
            if task.cancelled():
                return
            exc = task.exception()
            if exc:
                logger.error(f"Trace job error: {exc}", exc_info=exc)

        self._task.add_done_callback(_handle_task_exception)
        return job

    def cancel_job(self, chat_id: int) -> bool:
        """Cancel the active job if owned by this chat. Returns True if cancelled."""
        if not self._active_job or self._active_job.chat_id != chat_id:
            return False
        self._active_job.cancelled = True
        if self._task and not self._task.done():
            self._task.cancel()
        return True

    def get_task(self) -> Optional[asyncio.Task]:
        """Get the underlying asyncio.Task (for running_tasks integration)."""
        return self._task
