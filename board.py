"""Board of Investors scoring system.

Each persona is a function `score_<name>(metrics: dict) -> float` returning a
score in [0, 100]. The composite is the equal-weighted mean of all 12.

All component scores are 0–100. Missing inputs are treated as neutral (50) so
sparse data doesn't unfairly penalise. Each persona may apply a "hard filter"
cap (e.g. if profitability is below their floor) which limits the final score
regardless of other signals.
"""

from __future__ import annotations

import math
from datetime import date, datetime
from typing import Optional


# ---------------------------------------------------------------------------
# Helpers


def _safe(v) -> Optional[float]:
    """Coerce v to a finite float; None/NaN/inf/string return None."""
    if v is None:
        return None
    if isinstance(v, bool):
        return float(v)
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if math.isnan(f) or math.isinf(f):
        return None
    return f


def _bands(value: Optional[float], *args: float) -> float:
    """Step function. args = threshold1, score_if_<thr1, threshold2, score_if_<thr2, ..., default.

    Example: _bands(pe, 20, 100, 30, 70, 40, 40, 10) means:
        if pe is None -> 50
        elif pe < 20  -> 100
        elif pe < 30  -> 70
        elif pe < 40  -> 40
        else          -> 10
    """
    if value is None:
        return 50.0
    if len(args) < 1 or len(args) % 2 != 1:
        raise ValueError("_bands requires (threshold, score) pairs + one default")
    default = args[-1]
    pairs = args[:-1]
    for i in range(0, len(pairs), 2):
        threshold, score = pairs[i], pairs[i + 1]
        if value < threshold:
            return float(score)
    return float(default)


def _macd_score(b, true_score: float = 70.0, false_score: float = 30.0) -> float:
    if b is None:
        return 50.0
    return float(true_score) if bool(b) else float(false_score)


def _rating_score(rating: Optional[str],
                  sb: float = 100, buy: float = 75,
                  hold: float = 45, sell: float = 15) -> float:
    if not rating:
        return 50.0
    r = rating.strip().lower()
    if r == "strong buy":
        return float(sb)
    if r in ("buy", "outperform"):
        return float(buy)
    if r == "hold":
        return float(hold)
    if r in ("sell", "underperform", "strong sell"):
        return float(sell)
    return 50.0


def _golden_cross_recent(date_str: Optional[str], days: int = 90) -> bool:
    if not date_str:
        return False
    try:
        d = datetime.fromisoformat(str(date_str)[:10]).date()
    except ValueError:
        return False
    return (date.today() - d).days <= days


def _weighted(*pairs: tuple[float, float]) -> float:
    """Combine (score, weight) pairs. Weights normalised to 1."""
    total_w = sum(w for _, w in pairs)
    if total_w == 0:
        return 0.0
    return sum(s * w for s, w in pairs) / total_w


def _cap_to_100(x: float) -> float:
    if x > 100:
        return 100.0
    if x < 0:
        return 0.0
    return x


def _round1(x: float) -> float:
    return round(_cap_to_100(x), 1)


# ---------------------------------------------------------------------------
# 1. Warren Buffett


def score_buffett(m: dict) -> float:
    pe = _safe(m.get("pe_ratio"))
    eps_yoy = _safe(m.get("eps_growth_yoy"))
    roe = _safe(m.get("roe"))
    pm = _safe(m.get("profit_margin"))
    upside = _safe(m.get("upside_pct"))
    ret_1y = _safe(m.get("ret_1y"))

    value = _bands(pe, 20, 100, 30, 70, 40, 40, 10)
    growth = _bands(eps_yoy, 0, 10, 10, 50, 20, 75, 100) if eps_yoy is not None else 50.0
    # Profitability
    roe_s = _bands(roe, 10, 20, 15, 50, 20, 75, 100) if roe is not None else 50.0
    pm_s = _bands(pm, 5, 20, 10, 50, 20, 75, 100) if pm is not None else 50.0
    profitability = (roe_s + pm_s) / 2
    # Analyst
    analyst = _bands(upside, 10, 25, 20, 50, 30, 75, 100) if upside is not None else 50.0
    # Momentum
    macd_d = _macd_score(m.get("macd_daily_rising"), 70, 30)
    ret_s = _bands(ret_1y, 0, 20, 10, 50, 20, 75, 100) if ret_1y is not None else 50.0
    momentum = (macd_d + ret_s) / 2

    score = _weighted(
        (value, 0.10), (growth, 0.25), (profitability, 0.30),
        (analyst, 0.15), (momentum, 0.20),
    )
    if (pm is not None and pm < 5) or (roe is not None and roe < 10):
        score = min(score, 40.0)
    return _round1(score)


# ---------------------------------------------------------------------------
# 2. Benjamin Graham


def score_graham(m: dict) -> float:
    pe = _safe(m.get("pe_ratio"))
    peg = _safe(m.get("peg_ratio"))
    eps_yoy = _safe(m.get("eps_growth_yoy"))
    roa = _safe(m.get("roa"))
    upside = _safe(m.get("upside_pct"))
    ret_1y = _safe(m.get("ret_1y"))

    pe_s = _bands(pe, 10, 100, 15, 85, 20, 65, 30, 40, 10)
    peg_s = _bands(peg, 0.5, 100, 1, 80, 1.5, 55, 2, 30, 10)
    value = (pe_s + peg_s) / 2

    if eps_yoy is None:
        growth = 50.0
    elif eps_yoy < 0:
        growth = 20.0
    elif eps_yoy < 10:
        growth = 60.0
    else:
        growth = 75.0

    if roa is None:
        profitability = 50.0
    elif roa < 0:
        profitability = 10.0
    elif roa < 5:
        profitability = 40.0
    elif roa < 10:
        profitability = 70.0
    else:
        profitability = 100.0

    analyst = _bands(upside, 10, 25, 25, 55, 40, 80, 100) if upside is not None else 50.0

    # Graham likes unloved stocks
    if ret_1y is None:
        momentum = 50.0
    elif ret_1y < 0:
        momentum = 70.0
    elif ret_1y < 10:
        momentum = 55.0
    else:
        momentum = 40.0

    score = _weighted(
        (value, 0.40), (growth, 0.15), (profitability, 0.10),
        (analyst, 0.25), (momentum, 0.10),
    )
    if (pe is not None and pe > 30) or (peg is not None and peg > 2):
        score = min(score, 35.0)
    return _round1(score)


# ---------------------------------------------------------------------------
# 3. Peter Lynch


def score_lynch(m: dict) -> float:
    peg = _safe(m.get("peg_ratio"))
    rev_g = _safe(m.get("revenue_growth_pct"))
    eps_yoy = _safe(m.get("eps_growth_yoy"))
    eps_qoq = _safe(m.get("eps_growth_qoq"))
    pm = _safe(m.get("profit_margin"))
    upside = _safe(m.get("upside_pct"))

    value = _bands(peg, 0.5, 100, 1, 85, 1.5, 60, 2, 35, 10)

    rev_s = _bands(rev_g, 5, 30, 10, 60, 20, 80, 100) if rev_g is not None else 50.0
    eps_s = _bands(eps_yoy, 5, 30, 15, 60, 25, 80, 100) if eps_yoy is not None else 50.0
    growth = (rev_s + eps_s) / 2

    profitability = _bands(pm, 3, 20, 8, 50, 15, 75, 100) if pm is not None else 50.0

    analyst = _bands(upside, 5, 25, 15, 50, 25, 75, 100) if upside is not None else 50.0

    if eps_qoq is None:
        momentum = 50.0
    elif eps_qoq < 0:
        momentum = 20.0
    elif eps_qoq < 10:
        momentum = 65.0
    else:
        momentum = 100.0

    score = _weighted(
        (value, 0.20), (growth, 0.35), (profitability, 0.15),
        (analyst, 0.20), (momentum, 0.10),
    )
    if peg is not None and peg > 2 and eps_yoy is not None and eps_yoy < 5:
        score = min(score, 30.0)
    return _round1(score)


# ---------------------------------------------------------------------------
# 4. John Templeton


def score_templeton(m: dict) -> float:
    pe = _safe(m.get("pe_ratio"))
    rev_g = _safe(m.get("revenue_growth_pct"))
    roa = _safe(m.get("roa"))
    upside = _safe(m.get("upside_pct"))
    rsi = _safe(m.get("rsi_14"))
    ret_1y = _safe(m.get("ret_1y"))

    value = _bands(pe, 10, 100, 15, 85, 20, 65, 30, 40, 10)

    if rev_g is None:
        growth = 50.0
    elif rev_g < 0:
        growth = 30.0
    elif rev_g < 5:
        growth = 55.0
    else:
        growth = 70.0

    if roa is None:
        profitability = 50.0
    elif roa < 0:
        profitability = 15.0
    elif roa < 4:
        profitability = 45.0
    elif roa < 8:
        profitability = 70.0
    else:
        profitability = 100.0

    analyst = _bands(upside, 15, 30, 30, 65, 50, 85, 100) if upside is not None else 50.0

    if rsi is None:
        rsi_s = 50.0
    elif rsi < 30:
        rsi_s = 100.0
    elif rsi < 40:
        rsi_s = 85.0
    elif rsi < 50:
        rsi_s = 65.0
    elif rsi < 60:
        rsi_s = 45.0
    else:
        rsi_s = 20.0
    momentum = rsi_s + (15.0 if (ret_1y is not None and ret_1y < 0) else 0.0)
    momentum = min(momentum, 100.0)

    score = _weighted(
        (value, 0.30), (growth, 0.10), (profitability, 0.10),
        (analyst, 0.30), (momentum, 0.20),
    )
    if rsi is not None and rsi > 65 and ret_1y is not None and ret_1y > 30:
        score = min(score, 45.0)
    return _round1(score)


# ---------------------------------------------------------------------------
# 5. George Soros


def score_soros(m: dict) -> float:
    fpe = _safe(m.get("forward_pe"))
    eps_yoy = _safe(m.get("eps_growth_yoy"))
    roe = _safe(m.get("roe"))
    upside = _safe(m.get("upside_pct"))
    rsi = _safe(m.get("rsi_14"))
    macd_w = m.get("macd_weekly_rising")
    macd_m = m.get("macd_monthly_rising")

    value = _bands(fpe, 20, 70, 35, 50, 30) if fpe is not None else 50.0
    growth = _bands(eps_yoy, 5, 40, 15, 60, 80) if eps_yoy is not None else 50.0
    profitability = _bands(roe, 8, 40, 15, 60, 80) if roe is not None else 50.0
    analyst = _bands(upside, 10, 40, 20, 60, 80) if upside is not None else 50.0

    # Momentum mix
    w_score = _macd_score(macd_w, 100, 0)
    m_score = _macd_score(macd_m, 100, 0)
    if rsi is None:
        rsi_s = 50.0
    elif 50 <= rsi < 70:
        rsi_s = 100.0
    elif 40 <= rsi < 50:
        rsi_s = 75.0
    elif 70 <= rsi < 80:
        rsi_s = 60.0
    elif 30 <= rsi < 40:
        rsi_s = 50.0
    else:
        rsi_s = 20.0
    momentum = _weighted((w_score, 0.35), (m_score, 0.35), (rsi_s, 0.30))
    if _golden_cross_recent(m.get("golden_cross_date"), days=90):
        momentum = min(momentum + 20, 100.0)

    score = _weighted(
        (value, 0.05), (growth, 0.10), (profitability, 0.05),
        (analyst, 0.10), (momentum, 0.70),
    )
    if macd_w is False and macd_m is False:
        score = min(score, 35.0)
    return _round1(score)


# ---------------------------------------------------------------------------
# 6. Charlie Munger


def score_munger(m: dict) -> float:
    fpe = _safe(m.get("forward_pe"))
    rev_g = _safe(m.get("revenue_growth_pct"))
    eps_yoy = _safe(m.get("eps_growth_yoy"))
    croci = _safe(m.get("croci_approx"))
    roe = _safe(m.get("roe"))
    pm = _safe(m.get("profit_margin"))
    upside = _safe(m.get("upside_pct"))
    ret_1y = _safe(m.get("ret_1y"))

    value = _bands(fpe, 15, 100, 25, 75, 35, 50, 20) if fpe is not None else 50.0

    rev_s = _bands(rev_g, 0, 10, 10, 50, 20, 75, 100) if rev_g is not None else 50.0
    eps_s = _bands(eps_yoy, 0, 10, 10, 50, 20, 75, 100) if eps_yoy is not None else 50.0
    growth = (rev_s + eps_s) / 2

    croci_s = _bands(croci, 6, 25, 12, 55, 20, 80, 100) if croci is not None else 50.0
    roe_s = _bands(roe, 10, 20, 15, 50, 20, 75, 100) if roe is not None else 50.0
    pm_s = _bands(pm, 5, 20, 10, 50, 20, 75, 100) if pm is not None else 50.0
    profitability = _weighted((croci_s, 0.40), (roe_s, 0.40), (pm_s, 0.20))

    rating_s = _rating_score(m.get("analyst_rating"), sb=100, buy=75, hold=45, sell=15)
    upside_s = _bands(upside, 10, 25, 20, 50, 30, 75, 100) if upside is not None else 50.0
    analyst = (rating_s + upside_s) / 2

    macd_d = _macd_score(m.get("macd_daily_rising"), 70, 30)
    ret_s = _bands(ret_1y, 0, 20, 10, 50, 20, 75, 100) if ret_1y is not None else 50.0
    momentum = (macd_d + ret_s) / 2

    score = _weighted(
        (value, 0.15), (growth, 0.20), (profitability, 0.35),
        (analyst, 0.15), (momentum, 0.15),
    )
    if (croci is not None and croci < 5) or (pm is not None and pm < 8):
        score = min(score, 40.0)
    return _round1(score)


# ---------------------------------------------------------------------------
# 7. Jim Simons


def score_simons(m: dict) -> float:
    peg = _safe(m.get("peg_ratio"))
    eps_qoq = _safe(m.get("eps_growth_qoq"))
    sortino = _safe(m.get("sortino_ratio"))
    upside = _safe(m.get("upside_pct"))
    rsi = _safe(m.get("rsi_14"))
    ret_3m = _safe(m.get("ret_3m"))

    value = _bands(peg, 1, 80, 2, 55, 30) if peg is not None else 50.0
    growth = _bands(eps_qoq, 0, 15, 5, 50, 15, 75, 100) if eps_qoq is not None else 50.0
    if sortino is None:
        profitability = 50.0
    elif sortino < 0:
        profitability = 10.0
    elif sortino < 0.5:
        profitability = 35.0
    elif sortino < 1:
        profitability = 55.0
    elif sortino < 2:
        profitability = 80.0
    else:
        profitability = 100.0
    analyst = _bands(upside, 10, 25, 20, 50, 30, 75, 100) if upside is not None else 50.0

    if rsi is None:
        rsi_s = 50.0
    elif 40 <= rsi < 60:
        rsi_s = 100.0
    elif (35 <= rsi < 40) or (60 <= rsi < 70):
        rsi_s = 75.0
    elif (30 <= rsi < 35) or (70 <= rsi < 75):
        rsi_s = 50.0
    else:
        rsi_s = 20.0
    macd_d = _macd_score(m.get("macd_daily_rising"), 80, 20)
    macd_w = _macd_score(m.get("macd_weekly_rising"), 80, 20)
    ret_s = _bands(ret_3m, 0, 20, 5, 50, 10, 75, 100) if ret_3m is not None else 50.0
    momentum = (rsi_s + macd_d + macd_w + ret_s) / 4

    score = _weighted(
        (value, 0.10), (growth, 0.15), (profitability, 0.20),
        (analyst, 0.10), (momentum, 0.45),
    )
    return _round1(score)


# ---------------------------------------------------------------------------
# 8. Philip Fisher


def score_fisher(m: dict) -> float:
    fpe = _safe(m.get("forward_pe"))
    rev_g = _safe(m.get("revenue_growth_pct"))
    eps_yoy = _safe(m.get("eps_growth_yoy"))
    eps_qoq = _safe(m.get("eps_growth_qoq"))
    pm = _safe(m.get("profit_margin"))
    roe = _safe(m.get("roe"))
    upside = _safe(m.get("upside_pct"))
    ret_1y = _safe(m.get("ret_1y"))
    ret_6m = _safe(m.get("ret_6m"))

    value = _bands(fpe, 20, 80, 35, 60, 40) if fpe is not None else 50.0

    rev_s = _bands(rev_g, 3, 15, 8, 40, 15, 65, 25, 85, 100) if rev_g is not None else 50.0
    eps_s = _bands(eps_yoy, 3, 15, 8, 40, 15, 65, 25, 85, 100) if eps_yoy is not None else 50.0
    growth = (rev_s + eps_s) / 2
    if eps_qoq is not None and eps_qoq > 0:
        growth = min(growth + 10, 100.0)

    pm_s = _bands(pm, 6, 25, 12, 55, 20, 80, 100) if pm is not None else 50.0
    roe_s = _bands(roe, 10, 20, 15, 50, 20, 75, 100) if roe is not None else 50.0
    profitability = (pm_s + roe_s) / 2

    rating_s = _rating_score(m.get("analyst_rating"), sb=100, buy=75, hold=45, sell=15)
    upside_s = _bands(upside, 10, 25, 20, 50, 30, 75, 100) if upside is not None else 50.0
    analyst = (rating_s + upside_s) / 2

    ret_1y_s = _bands(ret_1y, 0, 20, 10, 50, 20, 75, 100) if ret_1y is not None else 50.0
    ret_6m_s = _bands(ret_6m, 0, 20, 10, 50, 20, 75, 100) if ret_6m is not None else 50.0
    momentum = (ret_1y_s + ret_6m_s) / 2

    score = _weighted(
        (value, 0.05), (growth, 0.40), (profitability, 0.20),
        (analyst, 0.20), (momentum, 0.15),
    )
    if (rev_g is not None and rev_g < 3) and (eps_yoy is not None and eps_yoy < 3):
        score = min(score, 30.0)
    return _round1(score)


# ---------------------------------------------------------------------------
# 9. Jack Bogle


def score_bogle(m: dict) -> float:
    pe = _safe(m.get("pe_ratio"))
    eps_yoy = _safe(m.get("eps_growth_yoy"))
    rev_g = _safe(m.get("revenue_growth_pct"))
    roe = _safe(m.get("roe"))
    pm = _safe(m.get("profit_margin"))
    upside = _safe(m.get("upside_pct"))
    ret_1y = _safe(m.get("ret_1y"))

    value = _bands(pe, 15, 100, 20, 80, 25, 60, 30, 40, 20)

    # Growth: requires both EPS YoY > 10 AND rev growth > 5 to get 100
    eps_pass = eps_yoy is not None and eps_yoy > 10
    rev_pass = rev_g is not None and rev_g > 5
    if eps_pass and rev_pass:
        growth = 100.0
    elif eps_pass or rev_pass:
        growth = 60.0
    elif eps_yoy is None and rev_g is None:
        growth = 50.0
    else:
        growth = 20.0

    roe_s = _bands(roe, 10, 20, 15, 50, 20, 75, 100) if roe is not None else 50.0
    pm_s = _bands(pm, 5, 20, 10, 50, 20, 75, 100) if pm is not None else 50.0
    profitability = (roe_s + pm_s) / 2

    analyst = _bands(upside, 10, 20, 20, 45, 30, 70, 100) if upside is not None else 50.0

    momentum = _bands(ret_1y, 0, 15, 8, 45, 15, 70, 100) if ret_1y is not None else 50.0

    score = _weighted(
        (value, 0.25), (growth, 0.20), (profitability, 0.20),
        (analyst, 0.20), (momentum, 0.15),
    )
    return _round1(score)


# ---------------------------------------------------------------------------
# 10. Carl Icahn


def score_icahn(m: dict) -> float:
    pe = _safe(m.get("pe_ratio"))
    rev_g = _safe(m.get("revenue_growth_pct"))
    roa = _safe(m.get("roa"))
    upside = _safe(m.get("upside_pct"))
    ret_6m = _safe(m.get("ret_6m"))

    pe_s = _bands(pe, 10, 100, 15, 85, 20, 65, 30, 40, 10)
    up_val_s = _bands(upside, 10, 20, 25, 55, 40, 80, 100) if upside is not None else 50.0
    value = (pe_s + up_val_s) / 2

    if rev_g is None:
        growth = 50.0
    elif rev_g < 0:
        growth = 25.0
    elif rev_g < 3:
        growth = 40.0
    elif rev_g < 8:
        growth = 55.0
    else:
        growth = 70.0

    if roa is None:
        profitability = 50.0
    elif roa < 0:
        profitability = 10.0
    elif roa < 5:
        profitability = 40.0
    elif roa < 10:
        profitability = 70.0
    else:
        profitability = 100.0

    up_an_s = _bands(upside, 15, 30, 25, 65, 40, 85, 100) if upside is not None else 50.0
    rating_s = _rating_score(m.get("analyst_rating"), sb=100, buy=75, hold=45, sell=15)
    analyst = (up_an_s + rating_s) / 2

    if ret_6m is None:
        momentum = 50.0
    elif ret_6m < 0:
        momentum = 80.0
    elif ret_6m < 5:
        momentum = 60.0
    else:
        momentum = 40.0

    score = _weighted(
        (value, 0.35), (growth, 0.10), (profitability, 0.15),
        (analyst, 0.30), (momentum, 0.10),
    )
    if (pe is not None and pe > 35) and (upside is not None and upside < 15):
        score = min(score, 35.0)
    return _round1(score)


# ---------------------------------------------------------------------------
# 11. Louis Navellier


def score_navellier(m: dict) -> float:
    fpe = _safe(m.get("forward_pe"))
    eps_yoy = _safe(m.get("eps_growth_yoy"))
    eps_qoq = _safe(m.get("eps_growth_qoq"))
    rev_g = _safe(m.get("revenue_growth_pct"))
    roe = _safe(m.get("roe"))
    pm = _safe(m.get("profit_margin"))
    upside = _safe(m.get("upside_pct"))
    sortino = _safe(m.get("sortino_ratio"))

    value = _bands(fpe, 15, 90, 25, 70, 35, 50, 25) if fpe is not None else 50.0

    eps_yoy_s = _bands(eps_yoy, 0, 10, 8, 40, 15, 65, 25, 85, 100) if eps_yoy is not None else 50.0
    eps_qoq_s = _bands(eps_qoq, 0, 10, 3, 40, 8, 60, 15, 80, 100) if eps_qoq is not None else 50.0
    rev_s = _bands(rev_g, 3, 25, 8, 55, 15, 80, 100) if rev_g is not None else 50.0
    growth = _weighted((eps_yoy_s, 0.40), (eps_qoq_s, 0.40), (rev_s, 0.20))

    roe_s = _bands(roe, 6, 20, 12, 50, 20, 75, 100) if roe is not None else 50.0
    pm_s = _bands(pm, 5, 20, 10, 50, 20, 75, 100) if pm is not None else 50.0
    profitability = (roe_s + pm_s) / 2

    rating_s = _rating_score(m.get("analyst_rating"), sb=100, buy=80, hold=45, sell=10)
    upside_s = _bands(upside, 5, 20, 15, 50, 25, 75, 100) if upside is not None else 50.0
    analyst = _weighted((rating_s, 0.60), (upside_s, 0.40))

    if sortino is None:
        momentum = 50.0
    elif sortino < 0:
        momentum = 10.0
    elif sortino < 0.75:
        momentum = 40.0
    elif sortino < 1.5:
        momentum = 70.0
    else:
        momentum = 100.0

    score = _weighted(
        (value, 0.10), (growth, 0.35), (profitability, 0.25),
        (analyst, 0.25), (momentum, 0.05),
    )
    if (eps_yoy is not None and eps_yoy < 0) and (eps_qoq is not None and eps_qoq < 0):
        score = min(score, 30.0)
    return _round1(score)


# ---------------------------------------------------------------------------
# 12. Luke Lango


def score_lango(m: dict) -> float:
    fpe = _safe(m.get("forward_pe"))
    rev_g = _safe(m.get("revenue_growth_pct"))
    eps_yoy = _safe(m.get("eps_growth_yoy"))
    eps_qoq = _safe(m.get("eps_growth_qoq"))
    pm = _safe(m.get("profit_margin"))
    upside = _safe(m.get("upside_pct"))
    ret_3m = _safe(m.get("ret_3m"))

    value = _bands(fpe, 25, 80, 50, 60, 40) if fpe is not None else 50.0

    rev_s = _bands(rev_g, 5, 15, 10, 40, 20, 65, 30, 85, 100) if rev_g is not None else 50.0
    eps_s = _bands(eps_yoy, 0, 15, 10, 40, 20, 65, 30, 85, 100) if eps_yoy is not None else 50.0
    growth = _weighted((rev_s, 0.50), (eps_s, 0.50))
    # Revenue QoQ isn't computed; use EPS QoQ as a proxy for "accelerating QoQ"
    if eps_qoq is not None and eps_qoq > 0:
        growth = min(growth + 15, 100.0)

    if pm is None:
        profitability = 50.0
    elif pm < 0:
        profitability = 25.0
    elif pm < 3:
        profitability = 45.0
    elif pm < 10:
        profitability = 70.0
    else:
        profitability = 100.0
    # Forgive negative margins when revenue growth is explosive
    if rev_g is not None and rev_g > 30 and pm is not None and pm < 0:
        profitability = max(profitability, 50.0)

    upside_s = _bands(upside, 10, 20, 20, 50, 35, 75, 100) if upside is not None else 50.0
    rating_s = _rating_score(m.get("analyst_rating"), sb=100, buy=75, hold=45, sell=15)
    analyst = _weighted((upside_s, 0.60), (rating_s, 0.40))

    macd_w = _macd_score(m.get("macd_weekly_rising"), 100, 0)
    macd_m = _macd_score(m.get("macd_monthly_rising"), 100, 0)
    ret_s = _bands(ret_3m, 0, 20, 5, 50, 15, 75, 100) if ret_3m is not None else 50.0
    momentum = _weighted((macd_w, 0.40), (macd_m, 0.30), (ret_s, 0.30))

    score = _weighted(
        (value, 0.05), (growth, 0.45), (profitability, 0.10),
        (analyst, 0.15), (momentum, 0.25),
    )
    if (rev_g is not None and rev_g < 5) and (eps_yoy is not None and eps_yoy < 5):
        score = min(score, 25.0)
    return _round1(score)


# ---------------------------------------------------------------------------
# Composite


PERSONAS = {
    "score_buffett": score_buffett,
    "score_graham": score_graham,
    "score_lynch": score_lynch,
    "score_templeton": score_templeton,
    "score_soros": score_soros,
    "score_munger": score_munger,
    "score_simons": score_simons,
    "score_fisher": score_fisher,
    "score_bogle": score_bogle,
    "score_icahn": score_icahn,
    "score_navellier": score_navellier,
    "score_lango": score_lango,
}


def composite_score(all_scores: dict) -> float:
    individual = [v for k, v in all_scores.items()
                  if k != "score_composite" and isinstance(v, (int, float))]
    if not individual:
        return 0.0
    return round(sum(individual) / len(individual), 1)


def score_all(metrics: dict) -> dict:
    out = {name: fn(metrics) for name, fn in PERSONAS.items()}
    out["score_composite"] = composite_score(out)
    return out


if __name__ == "__main__":
    # Smoke check: hypothetical mid-cap quality stock
    demo = {
        "pe_ratio": 18, "forward_pe": 16, "peg_ratio": 1.1,
        "revenue_growth_pct": 12, "eps_growth_yoy": 14, "eps_growth_qoq": 8,
        "roa": 12, "roe": 22, "profit_margin": 14, "croci_approx": 18,
        "analyst_rating": "Buy", "target_price": 120, "upside_pct": 18,
        "latest_close": 100, "rsi_14": 58,
        "macd_daily_rising": True, "macd_weekly_rising": True, "macd_monthly_rising": True,
        "golden_cross_date": None,
        "ret_1w": 1, "ret_1m": 4, "ret_3m": 9, "ret_6m": 14, "ret_1y": 22,
        "sortino_ratio": 1.4,
    }
    for k, v in score_all(demo).items():
        print(f"{k}: {v}")
