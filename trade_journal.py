"""
Trade journal — the ground-truth dataset the learning layer trains on.

Every trade writes two things:
  1. an ENTRY row when the position opens (the feature snapshot + the signal
     that caused it), keyed by a trade_id;
  2. an EXIT patch when the position closes (real exit price, realised PnL net
     of commission, duration, win/loss).

We persist to a local JSONL file (append-only, reliable, survives even if
Supabase is down) and best-effort mirror to Supabase for the dashboard. The
JSONL file is what learn.py reads. Without this file there is no learning —
the system would have no memory of what worked.
"""
from __future__ import annotations

import json
import os
import threading
import time
from typing import Any, Dict, Optional

import config

_LOCK = threading.Lock()
JOURNAL_PATH = os.getenv("TRADE_JOURNAL_PATH", "trade_journal.jsonl")


def _append(record: Dict[str, Any]) -> None:
    """Append one JSON line to the local journal (thread-safe)."""
    line = json.dumps(record, default=str)
    with _LOCK:
        with open(JOURNAL_PATH, "a", encoding="utf-8") as fh:
            fh.write(line + "\n")


def record_entry(
    trade_id: str,
    symbol: str,
    action: str,
    entry_price: float,
    quantity: float,
    leverage: int,
    sl_price: float,
    tp_price: float,
    features: Optional[Dict[str, float]] = None,
    signal: Optional[Dict[str, Any]] = None,
) -> None:
    """Log the moment a position opens, including WHY (features + signal)."""
    record = {
        "trade_id": trade_id,
        "event": "ENTRY",
        "symbol": symbol,
        "action": action,
        "entry_price": entry_price,
        "quantity": quantity,
        "leverage": leverage,
        "sl_price": sl_price,
        "tp_price": tp_price,
        "features": features or {},
        "signal": signal or {},
        "entry_ts": int(time.time() * 1000),
        "status": "OPEN",
    }
    _append(record)

    if config.supabase:
        try:
            config.supabase.table("trade_journal").insert({
                "trade_id": trade_id,
                "symbol": symbol,
                "action": action,
                "entry_price": entry_price,
                "quantity": quantity,
                "leverage": leverage,
                "sl_price": sl_price,
                "tp_price": tp_price,
                "features": features or {},
                "signal": signal or {},
                "status": "OPEN",
            }).execute()
        except Exception:
            pass  # dashboard is a mirror; the JSONL file is the source of truth


def record_exit(
    trade_id: str,
    symbol: str,
    exit_price: float,
    realized_pnl: float,
    commission: float,
    entry_ts: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Log the moment a position closes with the REAL, commission-adjusted result.
    net_pnl = realised PnL reported by the exchange minus total commission.
    Returns the computed outcome dict so the caller can log/telemeter it.
    """
    net_pnl = realized_pnl - abs(commission)
    now = int(time.time() * 1000)
    duration_s = round((now - entry_ts) / 1000.0, 1) if entry_ts else None

    outcome = {
        "trade_id": trade_id,
        "event": "EXIT",
        "symbol": symbol,
        "exit_price": exit_price,
        "realized_pnl": round(realized_pnl, 6),
        "commission": round(abs(commission), 6),
        "net_pnl": round(net_pnl, 6),
        "outcome": "WIN" if net_pnl > 0 else "LOSS",
        "duration_s": duration_s,
        "exit_ts": now,
        "status": "CLOSED",
    }
    _append(outcome)

    if config.supabase:
        try:
            config.supabase.table("trade_journal").update({
                "exit_price": exit_price,
                "realized_pnl": outcome["realized_pnl"],
                "commission": outcome["commission"],
                "net_pnl": outcome["net_pnl"],
                "outcome": outcome["outcome"],
                "duration_s": duration_s,
                "status": "CLOSED",
            }).eq("trade_id", trade_id).execute()
        except Exception:
            pass

    return outcome
