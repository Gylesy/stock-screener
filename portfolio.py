"""Portfolio analysis layer.

Given a list of positions, compute per-position metrics, an overall
Portfolio Health Score (0–100), and a set of trade suggestions driven by
the board composite score and current unrealised P&L.

Position input shape:
    {
        "ticker": "AAPL",
        "quantity": 50,
        "avg_buy_price": 165.00,
        "current_price": 297.63,
        "composite_score": 61.2,   # optional but recommended
        "sector": "Technology",    # optional, used for diversity
    }
"""

from __future__ import annotations

import sqlite3
from typing import Dict, List, Optional, Tuple


SIGNAL_BUY = "Buy More"
SIGNAL_HOLD = "Hold"
SIGNAL_TRIM = "Trim"
SIGNAL_SELL = "Sell"


def _signal_for(composite: Optional[float], pnl_pct: float) -> Tuple[str, str]:
    """Decide buy/hold/trim/sell for a single position."""
    if composite is None:
        return SIGNAL_HOLD, "No board score available"
    # Strong sell — weak board conviction
    if composite < 45:
        return SIGNAL_SELL, f"Weak board score ({composite:.1f})"
    # Stop-loss
    if pnl_pct <= -25 and composite < 60:
        return SIGNAL_SELL, f"Stop-loss: P&L {pnl_pct:.1f}% and board only {composite:.1f}"
    # Strong buy — high conviction and not deeply underwater
    if composite >= 75 and pnl_pct > -20:
        return SIGNAL_BUY, f"High board conviction ({composite:.1f})"
    # Take profit — big winner with only moderate ongoing conviction
    if pnl_pct >= 50 and composite < 70:
        return SIGNAL_TRIM, f"Take profit: up {pnl_pct:.1f}% with board {composite:.1f}"
    # Lock in some gains — moderate board, decent gain
    if 45 <= composite < 60 and pnl_pct > 20:
        return SIGNAL_TRIM, f"Moderate board ({composite:.1f}) with +{pnl_pct:.1f}% — trim"
    return SIGNAL_HOLD, "Board score and P&L within normal range"


def analyse_position(p: dict) -> dict:
    qty = float(p["quantity"])
    avg = float(p["avg_buy_price"])
    cur = float(p["current_price"])
    cost = qty * avg
    value = qty * cur
    pnl = value - cost
    pnl_pct = (pnl / cost * 100) if cost else 0.0
    composite = p.get("composite_score")
    signal, reason = _signal_for(composite, pnl_pct)
    return {
        **p,
        "cost_basis": cost,
        "current_value": value,
        "unrealised_pnl": pnl,
        "unrealised_pnl_pct": pnl_pct,
        "signal": signal,
        "signal_reason": reason,
    }


def _health_score(analysis: dict) -> Tuple[float, Dict[str, float]]:
    """Compute weighted health score + return the component sub-scores for transparency."""
    positions = analysis["positions"]
    if not positions:
        return 0.0, {}

    # 40%: average composite score
    comps = [p["composite_score"] for p in positions if p.get("composite_score") is not None]
    avg_comp = (sum(comps) / len(comps)) if comps else 50.0

    # 20%: concentration penalty — 100 if perfectly even, lower if one position dominates
    total_val = sum(p["current_value"] for p in positions)
    if total_val > 0:
        max_w = max(p["current_value"] / total_val for p in positions)
        concentration = max(0.0, (1.0 - max_w)) * 100.0
    else:
        concentration = 50.0

    # 20%: sector diversity — distinct sectors / position count, capped at 1.0
    sectors = {p.get("sector") for p in positions if p.get("sector")}
    diversity = min(len(sectors), len(positions)) / max(len(positions), 1) * 100.0

    # 10%: profitable position ratio
    winners = sum(1 for p in positions if p["unrealised_pnl"] > 0)
    win_ratio = winners / len(positions) * 100.0

    # 10%: overall return — anchored at 50 for 0% return; ±50pp from there gives 0/100
    total_cost = sum(p["cost_basis"] for p in positions)
    overall_pnl_pct = ((total_val - total_cost) / total_cost * 100) if total_cost else 0
    overall = max(0.0, min(100.0, 50.0 + overall_pnl_pct))

    components = {
        "avg_composite": round(avg_comp, 1),
        "concentration": round(concentration, 1),
        "sector_diversity": round(diversity, 1),
        "winner_ratio": round(win_ratio, 1),
        "overall_return": round(overall, 1),
    }
    score = (
        avg_comp * 0.40
        + concentration * 0.20
        + diversity * 0.20
        + win_ratio * 0.10
        + overall * 0.10
    )
    return round(score, 1), components


def _enrich_from_db(positions: List[dict], db_path: str) -> List[dict]:
    """Fill composite_score and sector for positions from the latest run in screener.db."""
    tickers = [p["ticker"] for p in positions if p.get("ticker")]
    if not tickers:
        return positions
    placeholders = ",".join(["?"] * len(tickers))
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        f"""SELECT ticker, score_composite, sector
            FROM daily_metrics
            WHERE run_date = (SELECT MAX(run_date) FROM daily_metrics)
              AND ticker IN ({placeholders})""",
        tickers,
    ).fetchall()
    conn.close()
    ctx = {r["ticker"]: dict(r) for r in rows}
    enriched = []
    for p in positions:
        c = ctx.get(p.get("ticker", ""), {})
        out = dict(p)
        if out.get("composite_score") is None and c.get("score_composite") is not None:
            out["composite_score"] = c["score_composite"]
        if not out.get("sector") and c.get("sector"):
            out["sector"] = c["sector"]
        enriched.append(out)
    return enriched


def analyse_portfolio(positions: List[dict], db_path: Optional[str] = None) -> dict:
    if db_path:
        positions = _enrich_from_db(positions, db_path)
    analysed = [analyse_position(p) for p in positions]
    total_value = sum(p["current_value"] for p in analysed)
    total_cost = sum(p["cost_basis"] for p in analysed)
    total_pnl = total_value - total_cost
    total_pnl_pct = (total_pnl / total_cost * 100) if total_cost else 0
    winners = [p for p in analysed if p["unrealised_pnl"] > 0]
    losers = [p for p in analysed if p["unrealised_pnl"] < 0]

    analysis = {
        "positions": analysed,
        "total_value": total_value,
        "total_cost": total_cost,
        "total_pnl": total_pnl,
        "total_pnl_pct": total_pnl_pct,
        "n_positions": len(analysed),
        "n_winners": len(winners),
        "n_losers": len(losers),
    }
    score, components = _health_score(analysis)
    analysis["health_score"] = score
    analysis["health_components"] = components
    return analysis


def generate_trade_suggestions(analysis: dict) -> List[dict]:
    """Convert non-Hold signals into actionable suggested-quantity trades."""
    suggestions = []
    for p in analysis["positions"]:
        sig = p["signal"]
        if sig == SIGNAL_HOLD:
            continue
        qty = float(p["quantity"])
        if sig == SIGNAL_BUY:
            suggested = max(1, int(qty * 0.25))
            action = "BUY"
        elif sig == SIGNAL_TRIM:
            suggested = max(1, int(qty * 0.50))
            action = "TRIM"
        elif sig == SIGNAL_SELL:
            suggested = int(qty)
            action = "SELL"
        else:
            continue
        if suggested <= 0:
            continue
        suggestions.append({
            "ticker": p["ticker"],
            "action": action,
            "suggested_quantity": suggested,
            "current_quantity": int(qty),
            "rationale": p["signal_reason"],
            "current_pnl_pct": p["unrealised_pnl_pct"],
            "composite_score": p.get("composite_score"),
        })
    # Sort: SELL first (most urgent), then TRIM, then BUY
    order = {"SELL": 0, "TRIM": 1, "BUY": 2}
    suggestions.sort(key=lambda s: order.get(s["action"], 9))
    return suggestions
