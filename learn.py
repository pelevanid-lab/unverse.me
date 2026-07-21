"""
Learning / edge-measurement layer.

Reads the trade journal (trade_journal.jsonl), pairs each ENTRY with its EXIT,
and answers the only question that matters: *after commission, does this
system make money, and which features actually drive the winners?*

It then proposes updated feature weights for signal_engine — nudging weights
toward the features that correlated with net-positive trades. This is the
"learning": the weights are not hand-tuned, they are pulled from real results.

Usage:
    python learn.py                 # print the edge report
    python learn.py --propose       # also print proposed signal_engine weights
    python learn.py --min-trades 30 # require N closed trades before proposing

Honest caveats printed by the tool itself: with few trades the numbers are
noise. Do not trust a proposal built on a handful of trades.
"""
from __future__ import annotations

import argparse
import json
import math
import os
from collections import defaultdict
from typing import Dict, List

import signal_engine

JOURNAL_PATH = os.getenv("TRADE_JOURNAL_PATH", "trade_journal.jsonl")

FEATURE_KEYS = ["imbalance", "cvd_1m", "cvd_5m"]


def load_closed_trades(path: str) -> List[dict]:
    """Pair ENTRY and EXIT rows by trade_id; return only completed trades."""
    entries: Dict[str, dict] = {}
    exits: Dict[str, dict] = {}
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            tid = row.get("trade_id")
            if not tid:
                continue
            if row.get("event") == "ENTRY":
                entries[tid] = row
            elif row.get("event") == "EXIT":
                exits[tid] = row

    trades = []
    for tid, exit_row in exits.items():
        entry_row = entries.get(tid)
        if not entry_row:
            continue
        trades.append({
            "trade_id": tid,
            "symbol": entry_row.get("symbol"),
            "action": entry_row.get("action"),
            "features": entry_row.get("features", {}),
            "net_pnl": float(exit_row.get("net_pnl", 0) or 0),
            "commission": float(exit_row.get("commission", 0) or 0),
            "outcome": exit_row.get("outcome"),
            "duration_s": exit_row.get("duration_s"),
        })
    return trades


def edge_report(trades: List[dict]) -> dict:
    """Compute the core edge metrics, all net of commission."""
    n = len(trades)
    if n == 0:
        return {"n": 0}

    wins = [t for t in trades if t["net_pnl"] > 0]
    losses = [t for t in trades if t["net_pnl"] <= 0]
    total_net = sum(t["net_pnl"] for t in trades)
    total_fees = sum(t["commission"] for t in trades)
    gross = total_net + total_fees

    avg_win = sum(t["net_pnl"] for t in wins) / len(wins) if wins else 0.0
    avg_loss = sum(t["net_pnl"] for t in losses) / len(losses) if losses else 0.0
    win_rate = len(wins) / n

    # Expectancy per trade (net). Positive => the system has a real edge.
    expectancy = total_net / n

    return {
        "n": n,
        "win_rate": win_rate,
        "wins": len(wins),
        "losses": len(losses),
        "total_net_pnl": total_net,
        "gross_pnl": gross,
        "total_fees": total_fees,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "expectancy_per_trade": expectancy,
        "fee_drag_pct": (total_fees / abs(gross) * 100) if gross else 0.0,
    }


def feature_correlations(trades: List[dict]) -> Dict[str, float]:
    """Point-biserial-ish correlation between each entry feature and net PnL.

    Positive => higher feature value tended to accompany more profitable trades.
    This is what drives the weight proposal.
    """
    corrs = {}
    pnls = [t["net_pnl"] for t in trades]
    mean_pnl = sum(pnls) / len(pnls)
    std_pnl = math.sqrt(sum((p - mean_pnl) ** 2 for p in pnls) / len(pnls)) or 1e-9

    for key in FEATURE_KEYS:
        xs = []
        for t in trades:
            val = t["features"].get(key)
            # For SHORT trades, flip feature sign so "helpful direction" aligns.
            if t["action"] == "SHORT" and key.startswith("cvd"):
                val = -(val or 0)
            if key == "imbalance" and val is not None:
                val = (val - 0.5) * 2  # centre to [-1, 1]
                if t["action"] == "SHORT":
                    val = -val
            xs.append(float(val) if val is not None else 0.0)
        mean_x = sum(xs) / len(xs)
        std_x = math.sqrt(sum((x - mean_x) ** 2 for x in xs) / len(xs)) or 1e-9
        cov = sum((xs[i] - mean_x) * (pnls[i] - mean_pnl) for i in range(len(xs))) / len(xs)
        corrs[key] = cov / (std_x * std_pnl)
    return corrs


def propose_weights(corrs: Dict[str, float], learning_rate: float = 0.5) -> Dict[str, float]:
    """Nudge current weights toward features that correlate with profit.

    new = clamp( current * (1 + lr * corr), floor .. ceil ). Conservative on
    purpose: one report should never wildly rewrite the strategy.
    """
    proposed = {}
    for key, base in signal_engine.DEFAULT_WEIGHTS.items():
        corr = corrs.get(key, 0.0)
        new = base * (1 + learning_rate * corr)
        proposed[key] = round(max(0.0, min(new, 3.0)), 3)
    return proposed


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--propose", action="store_true", help="print proposed weights")
    ap.add_argument("--min-trades", type=int, default=30)
    ap.add_argument("--path", default=JOURNAL_PATH)
    args = ap.parse_args()

    trades = load_closed_trades(args.path)
    rep = edge_report(trades)

    print("=" * 56)
    print("  EDGE REPORT (all figures net of commission)")
    print("=" * 56)
    if rep["n"] == 0:
        print(f"No closed trades found in {args.path}.")
        print("Run the system (testnet or live-with-approval) to collect data.")
        return

    print(f"Closed trades      : {rep['n']}")
    print(f"Win rate           : {rep['win_rate']*100:.1f}%  ({rep['wins']}W / {rep['losses']}L)")
    print(f"Total NET PnL      : {rep['total_net_pnl']:+.4f} USDT")
    print(f"  gross            : {rep['gross_pnl']:+.4f} USDT")
    print(f"  fees paid        : {rep['total_fees']:.4f} USDT  ({rep['fee_drag_pct']:.1f}% of gross)")
    print(f"Avg win / loss     : {rep['avg_win']:+.4f} / {rep['avg_loss']:+.4f} USDT")
    print(f"Expectancy / trade : {rep['expectancy_per_trade']:+.4f} USDT")
    print("-" * 56)
    verdict = "POSITIVE edge (net)" if rep["expectancy_per_trade"] > 0 else "NEGATIVE / no edge (net)"
    print(f"VERDICT            : {verdict}")

    corrs = feature_correlations(trades)
    print("-" * 56)
    print("Feature -> net-PnL correlation (direction-adjusted):")
    for k, v in corrs.items():
        print(f"  {k:10s} : {v:+.3f}")

    if args.propose:
        print("-" * 56)
        if rep["n"] < args.min_trades:
            print(f"NOT proposing weights: only {rep['n']} trades "
                  f"(need >= {args.min_trades}). Small samples are noise.")
        else:
            proposed = propose_weights(corrs)
            print("Proposed signal_engine weights (copy into DEFAULT_WEIGHTS "
                  "only if this holds up over more data):")
            print(f"  {json.dumps(proposed)}")


if __name__ == "__main__":
    main()
