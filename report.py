"""HTML report generation.

Reads the most recent `daily_metrics` rows from SQLite and renders a styled,
sortable HTML report:
- Top 20 Board Picks (across the whole universe)
- Sector breakdown of those top 20
- Board Consensus Leaders (most bullish / selective / contrarian)
- US Markets (S&P 500 + NASDAQ 100) section
- UK Markets (FTSE 100 + FTSE 250) section
"""

from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from jinja2 import Template


# ---------------------------------------------------------------------------
# Column configuration


COLUMNS = [
    ("ticker", "Ticker"),
    ("company_name", "Company"),
    ("sector", "Sector"),
    ("pe_ratio", "P/E"),
    ("forward_pe", "Fwd P/E"),
    ("peg_ratio", "PEG"),
    ("revenue_growth_pct", "Rev Gr %"),
    ("eps_growth_yoy", "EPS YoY %"),
    ("eps_growth_qoq", "EPS QoQ %"),
    ("roa", "ROA %"),
    ("roe", "ROE %"),
    ("profit_margin", "Margin %"),
    ("croci_approx", "CROCI %"),
    ("analyst_rating", "Rating"),
    ("target_price", "Target"),
    ("upside_pct", "Upside %"),
    ("latest_close", "Close"),
    ("sma_50", "SMA50"),
    ("sma_200", "SMA200"),
    ("rsi_14", "RSI(14)"),
    ("macd_daily_rising", "MACD D"),
    ("macd_weekly_rising", "MACD W"),
    ("macd_monthly_rising", "MACD M"),
    ("golden_cross_date", "Golden X"),
    ("ret_1w", "1w %"),
    ("ret_1m", "1m %"),
    ("ret_3m", "3m %"),
    ("ret_6m", "6m %"),
    ("ret_ytd", "YTD %"),
    ("ret_1y", "1y %"),
    ("sortino_ratio", "Sortino"),
    ("next_earnings_date", "Next Earn"),
]

NUMERIC_COLS = {
    "pe_ratio", "forward_pe", "peg_ratio",
    "revenue_growth_pct", "eps_growth_yoy", "eps_growth_qoq",
    "roa", "roe", "profit_margin", "croci_approx",
    "target_price", "upside_pct",
    "latest_close", "sma_50", "sma_200", "rsi_14",
    "ret_1w", "ret_1m", "ret_3m", "ret_6m", "ret_ytd", "ret_1y",
    "sortino_ratio",
}

BOOL_COLS = {"macd_daily_rising", "macd_weekly_rising", "macd_monthly_rising"}

PERSONA_KEYS = [
    "score_buffett", "score_graham", "score_lynch", "score_templeton",
    "score_soros", "score_munger", "score_simons", "score_fisher",
    "score_bogle", "score_icahn", "score_navellier", "score_lango",
]
PERSONA_LABEL = {
    "score_buffett": "Buffett", "score_graham": "Graham", "score_lynch": "Lynch",
    "score_templeton": "Templeton", "score_soros": "Soros", "score_munger": "Munger",
    "score_simons": "Simons", "score_fisher": "Fisher", "score_bogle": "Bogle",
    "score_icahn": "Icahn", "score_navellier": "Navellier", "score_lango": "Lango",
}

BOARD_COLUMNS = [
    ("ticker", "Ticker"),
    ("sector", "Sector"),
    ("score_composite", "Composite"),
    ("top3", "Top 3 Endorsers"),
] + [(k, PERSONA_LABEL[k]) for k in PERSONA_KEYS]

SCORE_KEYS = {k for k, _ in BOARD_COLUMNS if k.startswith("score_")}
BOARD_TEXT_COLS = {"ticker", "sector", "top3"}

TOP20_COLUMNS = [
    ("rank", "Rank"),
    ("ticker_display", "Ticker"),
    ("company_name", "Company"),
    ("sector", "Sector"),
    ("indices", "Index"),
    ("score_composite", "Composite"),
    ("consensus", "Consensus"),
] + [(k, PERSONA_LABEL[k]) for k in PERSONA_KEYS]
TOP20_TEXT_COLS = {"rank", "ticker_display", "company_name", "sector", "indices"}

TEXT_COLS = {"ticker", "company_name", "sector", "analyst_rating", "golden_cross_date",
             "next_earnings_date"}

INDEX_PILL_CLASS = {
    "SP500": "pill-sp500",
    "NASDAQ100": "pill-nasdaq",
    "FTSE100": "pill-ftse100",
    "FTSE250": "pill-ftse250",
    "RUSSELL2000": "pill-russell",
}


# ---------------------------------------------------------------------------
# Cell helpers


def _fmt(key: str, value) -> Optional[str]:
    if value is None:
        return None
    if key in BOOL_COLS:
        return "Yes" if int(value) == 1 else "No"
    if key in NUMERIC_COLS or key in SCORE_KEYS:
        try:
            return f"{float(value):.2f}"
        except (TypeError, ValueError):
            return None
    return str(value)


def _sort_key(key: str, value) -> str:
    if value is None:
        return ""
    if key in BOOL_COLS:
        return str(int(value))
    if key in NUMERIC_COLS or key in SCORE_KEYS:
        try:
            return str(float(value))
        except (TypeError, ValueError):
            return ""
    return str(value)


def _score_class(value) -> str:
    if value is None:
        return ""
    try:
        v = float(value)
    except (TypeError, ValueError):
        return ""
    if v > 75:
        return "score-green"
    if v >= 50:
        return "score-amber"
    return "score-red"


def _composite_class(value) -> str:
    if value is None:
        return ""
    try:
        v = float(value)
    except (TypeError, ValueError):
        return ""
    if v > 80:
        return "score-dark-green"
    if v >= 75:
        return "score-green"
    if v >= 70:
        return "score-amber"
    return "score-red"


def _consensus_class(n: Optional[int]) -> str:
    if n is None:
        return ""
    if n >= 10:
        return "score-dark-green"
    if n >= 7:
        return "score-green"
    if n >= 4:
        return "score-amber"
    return "score-red"


def _macd_class(value) -> str:
    if value is None:
        return ""
    return "macd-yes" if int(value) == 1 else "macd-no"


def _return_class(value) -> str:
    if value is None:
        return ""
    try:
        v = float(value)
    except (TypeError, ValueError):
        return ""
    if v > 0:
        return "ret-pos"
    if v < 0:
        return "ret-neg"
    return ""


def _row_class(weekly: Optional[int], monthly: Optional[int]) -> str:
    if monthly == 1 and weekly == 1:
        return "row-green"
    if monthly == 1:
        return "row-amber"
    if monthly == 0:
        return "row-red"
    return ""


def _rsi_class(value) -> str:
    if value is None:
        return ""
    try:
        v = float(value)
    except (TypeError, ValueError):
        return ""
    return "rsi-good" if 40 <= v <= 70 else "rsi-bad"


def _index_pills_html(indices_str: str) -> str:
    parts = []
    for idx in sorted(s for s in (indices_str or "").split(",") if s):
        cls = INDEX_PILL_CLASS.get(idx, "pill-other")
        parts.append(f'<span class="pill {cls}">{idx}</span>')
    return " ".join(parts)


def _consensus_count(row: dict, threshold: float = 70.0) -> int:
    n = 0
    for k in PERSONA_KEYS:
        v = row.get(k)
        if v is not None:
            try:
                if float(v) > threshold:
                    n += 1
            except (TypeError, ValueError):
                pass
    return n


def _top3_endorsers(row: dict) -> str:
    """Return the three top-scoring personas for a ticker as 'Fisher 96.5 · Buffett 93.2 · ...'."""
    pairs: List[Tuple[str, float]] = []
    for k in PERSONA_KEYS:
        v = row.get(k)
        if v is None:
            continue
        try:
            pairs.append((PERSONA_LABEL[k], float(v)))
        except (TypeError, ValueError):
            continue
    pairs.sort(key=lambda x: x[1], reverse=True)
    return " · ".join(f"{name} {score:.1f}" for name, score in pairs[:3])


def _score_spread(row: dict) -> Optional[float]:
    vals = []
    for k in PERSONA_KEYS:
        v = row.get(k)
        if v is None:
            continue
        try:
            vals.append(float(v))
        except (TypeError, ValueError):
            continue
    if len(vals) < 2:
        return None
    return max(vals) - min(vals)


# ---------------------------------------------------------------------------
# Row preparation


def _prepare_row(raw: dict) -> dict:
    out = {}
    cell_classes: Dict[str, str] = {}
    sort_keys: Dict[str, str] = {}
    for key, _ in COLUMNS:
        out[key] = _fmt(key, raw.get(key))
        sort_keys[key] = _sort_key(key, raw.get(key))
        if key == "rsi_14":
            cls = _rsi_class(raw.get(key))
            if cls:
                cell_classes[key] = cls
    out["_row_class"] = _row_class(raw.get("macd_weekly_rising"), raw.get("macd_monthly_rising"))
    out["_cell_classes"] = cell_classes
    out["_sort_keys"] = sort_keys
    return out


def _prepare_board_row(raw: dict) -> dict:
    out = {}
    cell_classes: Dict[str, str] = {}
    sort_keys: Dict[str, str] = {}
    top3 = _top3_endorsers(raw)
    enriched = {**raw, "top3": top3}
    for key, _ in BOARD_COLUMNS:
        out[key] = _fmt(key, enriched.get(key)) if key != "top3" else top3
        sort_keys[key] = _sort_key(key, enriched.get(key))
        if key == "score_composite":
            cls = _composite_class(raw.get(key))
            if cls:
                cell_classes[key] = cls
        elif key in SCORE_KEYS:
            cls = _score_class(raw.get(key))
            if cls:
                cell_classes[key] = cls
    out["_cell_classes"] = cell_classes
    out["_sort_keys"] = sort_keys
    out["_composite_raw"] = raw.get("score_composite")
    return out


def _prepare_top20_row(raw: dict, rank: int, indices_str: str) -> dict:
    out = {}
    cell_classes: Dict[str, str] = {}
    sort_keys: Dict[str, str] = {}

    consensus = _consensus_count(raw)
    is_ftse = raw["ticker"].upper().endswith(".L")
    ticker_display = f"{raw['ticker']} \U0001F1EC\U0001F1E7" if is_ftse else raw["ticker"]
    indices_html = _index_pills_html(indices_str)

    raw_indices_sort = ",".join(sorted(s for s in (indices_str or "").split(",") if s))

    pre = {
        **raw,
        "rank": rank,
        "ticker_display": ticker_display,
        "indices": indices_html,
        "consensus": f"{consensus}/12",
    }

    for key, _ in TOP20_COLUMNS:
        if key == "rank":
            out[key] = str(rank)
            sort_keys[key] = str(rank)
        elif key == "ticker_display":
            out[key] = ticker_display
            sort_keys[key] = raw["ticker"]
        elif key == "indices":
            out[key] = indices_html
            sort_keys[key] = raw_indices_sort
        elif key == "consensus":
            out[key] = f"{consensus}/12"
            sort_keys[key] = str(consensus)
            cls = _consensus_class(consensus)
            if cls:
                cell_classes[key] = cls
        elif key == "score_composite":
            out[key] = _fmt(key, raw.get(key))
            sort_keys[key] = _sort_key(key, raw.get(key))
            cls = _composite_class(raw.get(key))
            if cls:
                cell_classes[key] = cls
        elif key in SCORE_KEYS:
            out[key] = _fmt(key, raw.get(key))
            sort_keys[key] = _sort_key(key, raw.get(key))
            cls = _score_class(raw.get(key))
            if cls:
                cell_classes[key] = cls
        elif key == "rsi_14":
            out[key] = _fmt(key, raw.get(key))
            sort_keys[key] = _sort_key(key, raw.get(key))
            cls = _rsi_class(raw.get(key))
            if cls:
                cell_classes[key] = cls
        elif key in ("macd_weekly_rising", "macd_monthly_rising"):
            out[key] = _fmt(key, raw.get(key))
            sort_keys[key] = _sort_key(key, raw.get(key))
            cls = _macd_class(raw.get(key))
            if cls:
                cell_classes[key] = cls
        elif key == "ret_1y":
            out[key] = _fmt(key, raw.get(key))
            sort_keys[key] = _sort_key(key, raw.get(key))
            cls = _return_class(raw.get(key))
            if cls:
                cell_classes[key] = cls
        else:
            out[key] = _fmt(key, pre.get(key))
            sort_keys[key] = _sort_key(key, pre.get(key))

    if rank == 1:
        row_cls = "podium-1"
    elif rank == 2:
        row_cls = "podium-2"
    elif rank == 3:
        row_cls = "podium-3"
    elif rank % 2 == 0:
        row_cls = "zebra-even"
    else:
        row_cls = "zebra-odd"

    out["_row_class"] = row_cls
    out["_cell_classes"] = cell_classes
    out["_sort_keys"] = sort_keys
    return out


# ---------------------------------------------------------------------------
# DB loading


def _load_latest(db_path: str, run_date: Optional[str]) -> Tuple[List[dict], Dict[str, str]]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    if run_date is None:
        cur.execute("SELECT MAX(run_date) FROM daily_metrics")
        run_date = cur.fetchone()[0]
    if run_date is None:
        conn.close()
        return [], {}

    cur.execute("SELECT * FROM daily_metrics WHERE run_date = ?", (run_date,))
    rows = [dict(r) for r in cur.fetchall()]

    cur.execute("SELECT ticker, indices FROM ticker_universe")
    indices = {r["ticker"]: r["indices"] or "" for r in cur.fetchall()}

    conn.close()
    return rows, indices


# ---------------------------------------------------------------------------
# Consensus leaders


def _consensus_leaders(rows: List[dict]) -> Dict[str, dict]:
    """Compute most-bullish/most-selective/most-contrarian stats from the latest run."""
    persona_totals = {k: [] for k in PERSONA_KEYS}
    contrarian = None  # (ticker, spread, max_persona, max_score, min_persona, min_score)

    for r in rows:
        present = []
        for k in PERSONA_KEYS:
            v = r.get(k)
            if v is None:
                continue
            try:
                fv = float(v)
            except (TypeError, ValueError):
                continue
            persona_totals[k].append(fv)
            present.append((PERSONA_LABEL[k], fv))
        if len(present) >= 2:
            present.sort(key=lambda x: x[1])
            spread = present[-1][1] - present[0][1]
            if contrarian is None or spread > contrarian[1]:
                contrarian = (
                    r["ticker"], spread,
                    present[-1][0], present[-1][1],
                    present[0][0], present[0][1],
                )

    persona_avg = {
        PERSONA_LABEL[k]: (sum(v) / len(v) if v else None)
        for k, v in persona_totals.items()
    }
    valid = {k: v for k, v in persona_avg.items() if v is not None}
    if not valid:
        return {}

    bullish_name = max(valid, key=valid.get)
    selective_name = min(valid, key=valid.get)
    return {
        "bullish": {
            "name": bullish_name,
            "value": f"{valid[bullish_name]:.1f}",
            "detail": f"Mean score across {len(rows)} tickers",
        },
        "selective": {
            "name": selective_name,
            "value": f"{valid[selective_name]:.1f}",
            "detail": f"Mean score across {len(rows)} tickers",
        },
        "contrarian": ({
            "ticker": contrarian[0],
            "spread": f"{contrarian[1]:.1f}",
            "detail": (f"{contrarian[2]} {contrarian[3]:.1f} vs "
                       f"{contrarian[4]} {contrarian[5]:.1f}"),
        } if contrarian else None),
    }


# ---------------------------------------------------------------------------
# Section building


def _build_section(section_id: str, title: str, raw_rows: List[dict]) -> dict:
    rows = [_prepare_row(r) for r in raw_rows]
    board_rows = [_prepare_board_row(r) for r in raw_rows]
    board_rows.sort(
        key=lambda b: float(b["_composite_raw"]) if b["_composite_raw"] is not None else -1,
        reverse=True,
    )
    top_picks = []
    for b in board_rows[:5]:
        comp = b.get("_composite_raw")
        top_picks.append({
            "ticker": b["ticker"],
            "score": f"{float(comp):.1f}" if comp is not None else "N/A",
        })
    return {
        "id": section_id,
        "title": title,
        "rows": rows,
        "board_rows": board_rows,
        "top_picks": top_picks,
    }


def _build_top20(rows: List[dict], indices: Dict[str, str]) -> Tuple[List[dict], Dict[str, int]]:
    sortable = [
        r for r in rows
        if r.get("score_composite") is not None
    ]
    sortable.sort(key=lambda r: float(r["score_composite"]), reverse=True)
    top = sortable[:20]
    sector_counts: Dict[str, int] = {}
    out_rows = []
    for i, r in enumerate(top, start=1):
        idx_str = indices.get(r["ticker"], "")
        out_rows.append(_prepare_top20_row(r, i, idx_str))
        sec = r.get("sector") or "Unknown"
        sector_counts[sec] = sector_counts.get(sec, 0) + 1
    return out_rows, sector_counts


# ---------------------------------------------------------------------------
# HTML template


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Daily Screener — {{ run_date }}</title>
<style>
  html { scroll-behavior: smooth; }
  body { font-family: -apple-system, Helvetica, Arial, sans-serif; margin: 24px; color: #222; }
  h1 { margin-bottom: 4px; }
  h2 { margin-top: 32px; border-bottom: 2px solid #ddd; padding-bottom: 4px; scroll-margin-top: 16px; }
  h3 { margin-top: 18px; color: #555; }
  .meta { color: #666; font-size: 0.9em; margin-bottom: 16px; }
  .subtitle { color: #555; font-size: 13px; margin-top: -2px; margin-bottom: 6px; }
  .explainer { color: #777; font-size: 12px; margin-bottom: 12px; }
  .summary { background: #f3f5f8; padding: 12px 16px; border-radius: 6px; margin-bottom: 24px; }
  .summary span { margin-right: 18px; }
  .controls {
    display: flex; flex-wrap: wrap; align-items: center;
    gap: 12px; margin: 8px 0 4px;
  }
  .controls .spacer { flex: 1; }
  button.csv {
    background: #2c6cb0; color: #fff; border: 0; padding: 6px 12px;
    border-radius: 4px; cursor: pointer; font-size: 0.9em;
  }
  button.csv:hover { background: #1f5390; }
  .search-wrap { position: relative; display: inline-flex; align-items: center; }
  .search-input {
    width: 300px; padding: 6px 28px 6px 10px;
    font-size: 12px; font-family: inherit;
    border: 1px solid #ccc; border-radius: 3px;
    background: #fff; outline: none;
    transition: border-color 150ms ease, box-shadow 150ms ease;
  }
  .search-input:focus {
    border-color: #2c6cb0;
    box-shadow: 0 1px 4px rgba(44, 108, 176, 0.18);
  }
  .search-clear {
    position: absolute; right: 6px; top: 50%; transform: translateY(-50%);
    background: transparent; border: 0; color: #888; font-size: 16px;
    cursor: pointer; padding: 0 4px; line-height: 1;
  }
  .search-clear:hover { color: #333; }
  .count-line { font-size: 11px; color: #777; margin: 2px 0 8px; }
  button.collapse-toggle {
    background: #f0f3f6; color: #444; border: 1px solid #d8dde2;
    padding: 2px 10px; border-radius: 3px;
    font-family: inherit; font-size: 12px; cursor: pointer;
    margin-left: 10px; vertical-align: middle;
  }
  button.collapse-toggle:hover { background: #e3e8ed; color: #222; }
  .collapsible {
    overflow: hidden; max-height: 0; transition: max-height 300ms ease;
  }
  .collapsible.open { max-height: 50000px; }
  .jump-nav {
    position: fixed; top: 12px; right: 16px;
    display: flex; gap: 6px; z-index: 100;
    background: rgba(255, 255, 255, 0.96); padding: 6px;
    border-radius: 6px;
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.12);
  }
  .jump-nav a {
    font-size: 11px; padding: 4px 10px; border-radius: 3px;
    background: #2c6cb0; color: #fff; text-decoration: none;
    font-weight: 600;
  }
  .jump-nav a:hover { background: #1f5390; }
  .table-wrap { overflow: auto; max-height: 80vh; max-width: 100%; }
  table { border-collapse: collapse; width: 100%; font-size: 12px; }
  th, td { padding: 4px 8px; border: 1px solid #e2e2e2; text-align: right; white-space: nowrap; }
  th {
    background: #fff; cursor: pointer; user-select: none;
    position: sticky; top: 0; z-index: 2;
    border-bottom: 2px solid #c5c8cc;
  }
  th.text, td.text { text-align: left; }
  th:hover { background: #f3f5f8; }
  th.sort-asc::after  { content: " \\25B2"; font-size: 0.7em; }
  th.sort-desc::after { content: " \\25BC"; font-size: 0.7em; }
  tr.no-results td { text-align: center; color: #888; padding: 14px; font-style: italic; }
  tr.row-green  { background: #e6f7ea; }
  tr.row-amber  { background: #fff5d6; }
  tr.row-red    { background: #fde4e4; }
  tr.zebra-even { background: #fafafa; }
  tr.zebra-odd  { background: #ffffff; }
  tr.podium-1, tr.podium-2, tr.podium-3 { background: #fffbea; }
  tr.podium-1 td:first-child { border-left: 4px solid #d4af37; }
  tr.podium-2 td:first-child { border-left: 4px solid #b0b0b0; }
  tr.podium-3 td:first-child { border-left: 4px solid #cd7f32; }
  td.rsi-good { background: #c8e6c9; }
  td.rsi-bad  { background: #ffcdd2; }
  td.score-dark-green { background: #43a047; color: #fff; font-weight: 700; }
  td.score-green { background: #c8e6c9; font-weight: 600; }
  td.score-amber { background: #fff0b3; }
  td.score-red   { background: #ffcdd2; }
  td.macd-yes { background: #c8e6c9; color: #1b5e20; font-weight: 600; }
  td.macd-no  { background: #ffcdd2; color: #b71c1c; }
  td.ret-pos { background: #d8f0db; }
  td.ret-neg { background: #fde0e0; }
  td.na { color: #aaa; }
  .pill {
    display: inline-block; padding: 1px 6px; margin: 1px 2px 1px 0;
    border-radius: 3px; font-size: 10px; font-weight: 600; color: #fff;
  }
  .pill-sp500    { background: #1f5390; }
  .pill-nasdaq   { background: #6a1b9a; }
  .pill-ftse100  { background: #c62828; }
  .pill-ftse250  { background: #ef6c00; }
  .pill-russell  { background: #2e7d32; }
  .pill-other    { background: #555; }
  .top-picks {
    background: #eef5ff; border-left: 4px solid #2c6cb0;
    padding: 10px 14px; margin: 8px 0 16px; border-radius: 4px;
    font-size: 13px;
  }
  .top-picks strong { color: #1f5390; }
  .top-picks .pick { display: inline-block; margin-right: 14px; }
  .top-picks .pick .sc {
    display: inline-block; background: #2c6cb0; color: #fff;
    padding: 1px 6px; border-radius: 3px; margin-left: 4px; font-size: 11px;
  }
  .sector-bars { margin: 12px 0 24px; max-width: 720px; }
  .sector-bars .row { display: flex; align-items: center; margin: 3px 0; }
  .sector-bars .lbl { width: 200px; font-size: 12px; color: #444; }
  .sector-bars .barwrap { flex: 1; background: #f0f3f6; height: 22px; border-radius: 3px; }
  .sector-bars .bar { background: #2c6cb0; height: 100%; border-radius: 3px;
                      color: #fff; font-size: 11px; padding: 0 6px;
                      display: flex; align-items: center; }
  .leaders { display: grid; grid-template-columns: repeat(3, 1fr);
             gap: 12px; margin: 16px 0 28px; }
  .leader-card {
    background: #f3f5f8; padding: 12px 16px; border-radius: 6px;
    border-left: 4px solid #2c6cb0;
  }
  .leader-card .leader-label {
    font-size: 11px; color: #666; text-transform: uppercase;
    letter-spacing: 0.5px; margin-bottom: 4px;
  }
  .leader-card .leader-value { font-size: 18px; font-weight: 700; color: #1f5390; }
  .leader-card .leader-detail { font-size: 11px; color: #777; margin-top: 4px; }
  /* Portfolio Dashboard */
  .portfolio-summary {
    display: flex; flex-wrap: wrap; gap: 14px; margin: 14px 0 22px;
    align-items: stretch;
  }
  .health-card {
    background: #f3f5f8; padding: 14px 20px; border-radius: 6px;
    border-left: 4px solid #2c6cb0; min-width: 200px;
  }
  .health-card .label {
    font-size: 11px; color: #666; text-transform: uppercase;
    letter-spacing: 0.5px; margin-bottom: 4px;
  }
  .health-card .score { font-size: 32px; font-weight: 700; line-height: 1; }
  .health-card .score.green { color: #2e7d32; }
  .health-card .score.amber { color: #b27000; }
  .health-card .score.red   { color: #b71c1c; }
  .health-card .components { font-size: 10px; color: #888; margin-top: 6px; }
  .stat-card {
    background: #fafafa; padding: 10px 14px; border-radius: 6px;
    border: 1px solid #e5e7eb; min-width: 130px;
  }
  .stat-card .label {
    font-size: 10px; color: #666; text-transform: uppercase;
    letter-spacing: 0.5px;
  }
  .stat-card .val { font-size: 18px; font-weight: 700; color: #222; margin-top: 2px; }
  .stat-card .val.pos { color: #2e7d32; }
  .stat-card .val.neg { color: #c62828; }
  td.signal-buy   { background: #c8e6c9; color: #1b5e20; font-weight: 700; }
  td.signal-hold  { background: #f0f3f6; color: #444; }
  td.signal-trim  { background: #fff0b3; color: #6b4d00; font-weight: 600; }
  td.signal-sell  { background: #ffcdd2; color: #b71c1c; font-weight: 700; }
  td.pnl-pos { background: #d8f0db; color: #1b5e20; }
  td.pnl-neg { background: #fde0e0; color: #b71c1c; }
  .action-chip {
    display: inline-block; padding: 2px 8px; border-radius: 3px;
    font-size: 11px; font-weight: 700;
  }
  .action-chip.BUY  { background: #2e7d32; color: #fff; }
  .action-chip.TRIM { background: #f9a825; color: #fff; }
  .action-chip.SELL { background: #c62828; color: #fff; }
</style>
</head>
<body>
<div id="top"></div>

<nav class="jump-nav">
  {% if portfolio %}<a href="#portfolio">Portfolio</a>{% endif %}
  <a href="#top20">Top 20</a>
  <a href="#us-markets">US Markets</a>
  <a href="#uk-markets">UK Markets</a>
  <a href="#top">↑ Top</a>
</nav>

<h1>Daily Stock Screener</h1>
<div class="meta">Run date: {{ run_date }} · Generated {{ generated_at }}</div>

<div class="summary">
  <strong>Summary:</strong>
  <span>Total processed: <b>{{ total_processed }}</b></span>
  {% for name, n in index_counts.items() %}
    <span>{{ name }}: <b>{{ n }}</b></span>
  {% endfor %}
</div>

{% if portfolio %}
<section id="portfolio">
<h2>Portfolio Dashboard</h2>
<div class="explainer">
  {{ portfolio.n_positions }} positions · {{ portfolio.n_winners }} winners · {{ portfolio.n_losers }} losers.
  Signals derived from Board composite score and current unrealised P&amp;L.
</div>

<div class="portfolio-summary">
  <div class="health-card">
    <div class="label">Portfolio Health Score</div>
    <div class="score {{ portfolio.health_class }}">{{ portfolio.health_score }}</div>
    <div class="components">
      Composite {{ portfolio.health_components.avg_composite }} ·
      Concentration {{ portfolio.health_components.concentration }} ·
      Diversity {{ portfolio.health_components.sector_diversity }} ·
      Winners {{ portfolio.health_components.winner_ratio }} ·
      Return {{ portfolio.health_components.overall_return }}
    </div>
  </div>
  <div class="stat-card">
    <div class="label">Total Value</div>
    <div class="val">{{ portfolio.total_value_fmt }}</div>
  </div>
  <div class="stat-card">
    <div class="label">Cost Basis</div>
    <div class="val">{{ portfolio.total_cost_fmt }}</div>
  </div>
  <div class="stat-card">
    <div class="label">Unrealised P&amp;L</div>
    <div class="val {{ portfolio.pnl_class }}">{{ portfolio.total_pnl_fmt }}</div>
  </div>
  <div class="stat-card">
    <div class="label">P&amp;L %</div>
    <div class="val {{ portfolio.pnl_class }}">{{ portfolio.total_pnl_pct_fmt }}</div>
  </div>
  <div class="stat-card">
    <div class="label">Positions</div>
    <div class="val">{{ portfolio.n_positions }}</div>
  </div>
</div>

<h3>Holdings</h3>
<div class="controls">
  <div class="search-wrap">
    <input type="text" class="search-input" data-search-for="tbl-portfolio"
           placeholder="Search ticker..." />
    <button class="search-clear" data-clears-for="tbl-portfolio" style="display:none">×</button>
  </div>
  <div class="spacer"></div>
  <button class="csv" data-table="tbl-portfolio" data-filename="portfolio.csv">Export CSV</button>
</div>
<div class="count-line" data-count-for="tbl-portfolio"></div>
<div class="table-wrap">
<table id="tbl-portfolio">
  <thead>
    <tr>
      <th class="text">Ticker</th>
      <th class="text">Sector</th>
      <th>Qty</th>
      <th>Avg Buy</th>
      <th>Current</th>
      <th>Value</th>
      <th>P&amp;L</th>
      <th>P&amp;L %</th>
      <th>Composite</th>
      <th class="text">Signal</th>
      <th class="text">Reason</th>
    </tr>
  </thead>
  <tbody>
    {% for h in portfolio.holdings %}
    <tr>
      <td class="text"><b>{{ h.ticker }}</b></td>
      <td class="text">{{ h.sector or "—" }}</td>
      <td data-sort="{{ h.quantity }}">{{ h.quantity }}</td>
      <td data-sort="{{ h.avg_buy_price }}">{{ h.avg_buy_fmt }}</td>
      <td data-sort="{{ h.current_price }}">{{ h.current_fmt }}</td>
      <td data-sort="{{ h.current_value }}">{{ h.value_fmt }}</td>
      <td class="{{ h.pnl_class }}" data-sort="{{ h.unrealised_pnl }}">{{ h.pnl_fmt }}</td>
      <td class="{{ h.pnl_class }}" data-sort="{{ h.unrealised_pnl_pct }}">{{ h.pnl_pct_fmt }}</td>
      <td class="{{ h.composite_class }}" data-sort="{{ h.composite_score if h.composite_score is not none else '' }}">
        {{ h.composite_fmt }}
      </td>
      <td class="text signal-{{ h.signal_key }}">{{ h.signal }}</td>
      <td class="text" style="white-space:normal; max-width:280px;">{{ h.signal_reason }}</td>
    </tr>
    {% endfor %}
  </tbody>
</table>
</div>

{% if portfolio.suggestions %}
<h3>Trade Suggestions</h3>
<div class="controls">
  <div class="search-wrap">
    <input type="text" class="search-input" data-search-for="tbl-suggestions"
           placeholder="Search ticker..." />
    <button class="search-clear" data-clears-for="tbl-suggestions" style="display:none">×</button>
  </div>
  <div class="spacer"></div>
  <button class="csv" data-table="tbl-suggestions" data-filename="trade_suggestions.csv">Export CSV</button>
</div>
<div class="count-line" data-count-for="tbl-suggestions"></div>
<div class="table-wrap">
<table id="tbl-suggestions">
  <thead>
    <tr>
      <th class="text">Action</th>
      <th class="text">Ticker</th>
      <th>Current Qty</th>
      <th>Suggested Qty</th>
      <th>P&amp;L %</th>
      <th>Composite</th>
      <th class="text">Rationale</th>
    </tr>
  </thead>
  <tbody>
    {% for s in portfolio.suggestions %}
    <tr>
      <td class="text"><span class="action-chip {{ s.action }}">{{ s.action }}</span></td>
      <td class="text"><b>{{ s.ticker }}</b></td>
      <td data-sort="{{ s.current_quantity }}">{{ s.current_quantity }}</td>
      <td data-sort="{{ s.suggested_quantity }}">{{ s.suggested_quantity }}</td>
      <td class="{{ s.pnl_class }}" data-sort="{{ s.current_pnl_pct }}">{{ s.pnl_pct_fmt }}</td>
      <td class="{{ s.composite_class }}" data-sort="{{ s.composite_score if s.composite_score is not none else '' }}">
        {{ s.composite_fmt }}
      </td>
      <td class="text" style="white-space:normal; max-width:340px;">{{ s.rationale }}</td>
    </tr>
    {% endfor %}
  </tbody>
</table>
</div>
{% endif %}
</section>
{% endif %}

{% if top20_rows %}
<section id="top20">
<h2>Today's Top 20 Board Picks</h2>
<div class="subtitle">Run date: {{ run_date }}</div>
<div class="explainer">
  Ranked by Composite Panel Score across 12 investor personas. Showing
  highest-conviction candidates from the full {{ total_processed }}-ticker universe.
</div>
<div class="controls">
  <div class="search-wrap">
    <input type="text" class="search-input" data-search-for="tbl-top20"
           placeholder="Search ticker, company or sector..." />
    <button class="search-clear" data-clears-for="tbl-top20" style="display:none">×</button>
  </div>
  <div class="spacer"></div>
  <button class="csv" data-table="tbl-top20" data-filename="top20.csv">Export Top 20 CSV</button>
</div>
<div class="count-line" data-count-for="tbl-top20"></div>
<div class="table-wrap">
<table id="tbl-top20">
  <thead>
    <tr>
      {% for key, label in top20_columns %}
        <th data-key="{{ key }}" {% if key in top20_text_cols %}class="text"{% endif %}>{{ label }}</th>
      {% endfor %}
    </tr>
  </thead>
  <tbody>
  {% for row in top20_rows %}
    <tr class="{{ row._row_class }}">
      {% for key, _ in top20_columns %}
        {% set cell = row[key] %}
        {% set cls = row._cell_classes.get(key, '') %}
        {% if cell is none or cell == '' %}
          {% if key == 'indices' %}
            <td class="text {{ cls }}">{{ cell|safe }}</td>
          {% else %}
            <td class="na {{ cls }}">N/A</td>
          {% endif %}
        {% elif key == 'indices' %}
          <td class="text {{ cls }}" data-sort="{{ row._sort_keys[key] }}">{{ cell|safe }}</td>
        {% elif key in top20_text_cols %}
          <td class="text {{ cls }}" data-sort="{{ row._sort_keys[key] }}">{{ cell }}</td>
        {% else %}
          <td class="{{ cls }}" data-sort="{{ row._sort_keys[key] }}">{{ cell }}</td>
        {% endif %}
      {% endfor %}
    </tr>
  {% endfor %}
  </tbody>
</table>
</div>

<h3>Sector Breakdown of Top 20</h3>
<div class="sector-bars">
  {% for sector, count in sector_breakdown %}
    <div class="row">
      <div class="lbl">{{ sector }}</div>
      <div class="barwrap">
        <div class="bar" style="width: {{ (count / max_sector_count * 100)|round(1) }}%;">{{ count }}</div>
      </div>
    </div>
  {% endfor %}
</div>

{% if leaders %}
<h3>Board Consensus Leaders</h3>
<div class="leaders">
  <div class="leader-card">
    <div class="leader-label">Most Bullish Member</div>
    <div class="leader-value">{{ leaders.bullish.name }} ({{ leaders.bullish.value }})</div>
    <div class="leader-detail">{{ leaders.bullish.detail }}</div>
  </div>
  <div class="leader-card">
    <div class="leader-label">Most Selective Member</div>
    <div class="leader-value">{{ leaders.selective.name }} ({{ leaders.selective.value }})</div>
    <div class="leader-detail">{{ leaders.selective.detail }}</div>
  </div>
  {% if leaders.contrarian %}
  <div class="leader-card">
    <div class="leader-label">Most Contrarian Pick</div>
    <div class="leader-value">{{ leaders.contrarian.ticker }} (Δ {{ leaders.contrarian.spread }})</div>
    <div class="leader-detail">{{ leaders.contrarian.detail }}</div>
  </div>
  {% endif %}
</div>
{% endif %}
</section>
{% endif %}

{% for section in sections %}
<section id="{{ section.id }}">
<h2>{{ section.title }} <small style="color:#888;font-weight:normal;">({{ section.rows|length }})</small></h2>

{% if section.top_picks %}
<div class="top-picks">
  <strong>Top Board Picks:</strong>
  {% for pick in section.top_picks %}
    <span class="pick">{{ pick.ticker }}<span class="sc">{{ pick.score }}</span></span>
  {% endfor %}
</div>
{% endif %}

<h3>Metrics
  <button class="collapse-toggle" data-toggles="collapse-tbl-{{ section.id }}">▶ Show</button>
</h3>
<div class="collapsible" id="collapse-tbl-{{ section.id }}">
<div class="controls">
  <div class="search-wrap">
    <input type="text" class="search-input" data-search-for="tbl-{{ section.id }}"
           placeholder="Search ticker, company or sector..." />
    <button class="search-clear" data-clears-for="tbl-{{ section.id }}" style="display:none">×</button>
  </div>
  <div class="spacer"></div>
  <button class="csv" data-table="tbl-{{ section.id }}" data-filename="{{ section.id }}.csv">
    Export CSV
  </button>
</div>
<div class="count-line" data-count-for="tbl-{{ section.id }}"></div>
<div class="table-wrap">
<table id="tbl-{{ section.id }}">
  <thead>
    <tr>
      {% for key, label in columns %}
        <th data-key="{{ key }}" {% if key in text_cols %}class="text"{% endif %}>{{ label }}</th>
      {% endfor %}
    </tr>
  </thead>
  <tbody>
  {% for row in section.rows %}
    <tr class="{{ row._row_class }}">
      {% for key, _ in columns %}
        {% set cell = row[key] %}
        {% set cls = row._cell_classes.get(key, '') %}
        {% if cell is none %}
          <td class="na {{ cls }}">N/A</td>
        {% elif key in text_cols %}
          <td class="text {{ cls }}">{{ cell }}</td>
        {% else %}
          <td class="{{ cls }}" data-sort="{{ row._sort_keys[key] }}">{{ cell }}</td>
        {% endif %}
      {% endfor %}
    </tr>
  {% endfor %}
  </tbody>
</table>
</div>

</div>
</div>

<h3>Board Scores
  <button class="collapse-toggle" data-toggles="collapse-tbl-{{ section.id }}-board">▶ Show</button>
</h3>
<div class="collapsible" id="collapse-tbl-{{ section.id }}-board">
<div class="controls">
  <div class="search-wrap">
    <input type="text" class="search-input" data-search-for="tbl-{{ section.id }}-board"
           placeholder="Search ticker, company or sector..." />
    <button class="search-clear" data-clears-for="tbl-{{ section.id }}-board" style="display:none">×</button>
  </div>
  <div class="spacer"></div>
  <button class="csv" data-table="tbl-{{ section.id }}-board" data-filename="{{ section.id }}-board.csv">
    Export CSV
  </button>
</div>
<div class="count-line" data-count-for="tbl-{{ section.id }}-board"></div>
<div class="table-wrap">
<table id="tbl-{{ section.id }}-board">
  <thead>
    <tr>
      {% for key, label in board_columns %}
        <th data-key="{{ key }}" {% if key in board_text_cols %}class="text"{% endif %}>{{ label }}</th>
      {% endfor %}
    </tr>
  </thead>
  <tbody>
  {% for row in section.board_rows %}
    <tr>
      {% for key, _ in board_columns %}
        {% set cell = row[key] %}
        {% set cls = row._cell_classes.get(key, '') %}
        {% if cell is none or cell == '' %}
          <td class="na {{ cls }}">N/A</td>
        {% elif key in board_text_cols %}
          <td class="text {{ cls }}" data-sort="{{ row._sort_keys[key] }}">{{ cell }}</td>
        {% else %}
          <td class="{{ cls }}" data-sort="{{ row._sort_keys[key] }}">{{ cell }}</td>
        {% endif %}
      {% endfor %}
    </tr>
  {% endfor %}
  </tbody>
</table>
</div>
</div>
</section>
{% endfor %}

<script>
// Click-to-sort tables
document.querySelectorAll('table').forEach(function(table) {
  const ths = table.querySelectorAll('thead th');
  ths.forEach(function(th, colIdx) {
    th.addEventListener('click', function() {
      const tbody = table.querySelector('tbody');
      const rows = Array.from(tbody.querySelectorAll('tr'));
      const currentAsc = th.classList.contains('sort-asc');
      ths.forEach(function(o) { o.classList.remove('sort-asc', 'sort-desc'); });
      th.classList.add(currentAsc ? 'sort-desc' : 'sort-asc');
      const dir = currentAsc ? -1 : 1;
      rows.sort(function(a, b) {
        const av = a.children[colIdx];
        const bv = b.children[colIdx];
        const asn = parseFloat(av.getAttribute('data-sort'));
        const bsn = parseFloat(bv.getAttribute('data-sort'));
        if (!isNaN(asn) && !isNaN(bsn)) return (asn - bsn) * dir;
        const at = av.textContent.trim();
        const bt = bv.textContent.trim();
        return at.localeCompare(bt) * dir;
      });
      rows.forEach(function(r) { tbody.appendChild(r); });
    });
  });
});

// CSV export — only export visible (filtered) rows
document.querySelectorAll('button.csv').forEach(function(btn) {
  btn.addEventListener('click', function() {
    const table = document.getElementById(btn.dataset.table);
    const allRows = Array.from(table.querySelectorAll('tr'));
    const rows = allRows.filter(function(r) {
      return r.style.display !== 'none' && !r.classList.contains('no-results');
    });
    const csv = rows.map(function(r) {
      const cells = Array.from(r.querySelectorAll('th, td'));
      return cells.map(function(c) {
        const txt = c.textContent.trim().replace(/"/g, '""');
        return '"' + txt + '"';
      }).join(',');
    }).join('\\n');
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url; a.download = btn.dataset.filename;
    document.body.appendChild(a); a.click(); a.remove();
    URL.revokeObjectURL(url);
  });
});

// Filter / search
function dataRowsOf(tbody) {
  return Array.from(tbody.querySelectorAll('tr')).filter(function(r) {
    return !r.classList.contains('no-results');
  });
}

function filterTable(tableId) {
  const input = document.querySelector('[data-search-for="' + tableId + '"]');
  const table = document.getElementById(tableId);
  if (!input || !table) return;
  const tbody = table.querySelector('tbody');
  if (!tbody) return;

  // Remove any prior no-results row before counting
  tbody.querySelectorAll('tr.no-results').forEach(function(r) { r.remove(); });
  const rows = dataRowsOf(tbody);

  const q = (input.value || '').trim().toLowerCase();
  let shown = 0;
  rows.forEach(function(r) {
    const matches = q === '' || r.textContent.toLowerCase().indexOf(q) !== -1;
    r.style.display = matches ? '' : 'none';
    if (matches) shown++;
  });

  if (shown === 0 && q !== '') {
    const cols = table.querySelectorAll('thead th').length;
    const tr = document.createElement('tr');
    tr.className = 'no-results';
    const td = document.createElement('td');
    td.colSpan = cols;
    td.textContent = 'No results for "' + input.value + '"';
    tr.appendChild(td);
    tbody.appendChild(tr);
  }

  const counter = document.querySelector('[data-count-for="' + tableId + '"]');
  if (counter) counter.textContent = 'Showing ' + shown + ' of ' + rows.length + ' tickers';

  const clear = document.querySelector('[data-clears-for="' + tableId + '"]');
  if (clear) clear.style.display = q ? 'inline-block' : 'none';
}

document.querySelectorAll('input.search-input').forEach(function(input) {
  const tableId = input.dataset.searchFor;
  input.addEventListener('input', function() { filterTable(tableId); });
  filterTable(tableId); // initialise count
});

document.querySelectorAll('button.search-clear').forEach(function(btn) {
  btn.addEventListener('click', function() {
    const tableId = btn.dataset.clearsFor;
    const input = document.querySelector('[data-search-for="' + tableId + '"]');
    if (input) {
      input.value = '';
      filterTable(tableId);
      input.focus();
    }
  });
});

// Collapse / expand with sessionStorage persistence
function setCollapseUI(content, btn, isOpen) {
  if (isOpen) {
    content.classList.add('open');
    if (btn) btn.innerHTML = '▼ Hide';
  } else {
    content.classList.remove('open');
    if (btn) btn.innerHTML = '▶ Show';
  }
}

document.querySelectorAll('.collapsible').forEach(function(content) {
  const id = content.id;
  const btn = document.querySelector('[data-toggles="' + id + '"]');
  const stored = sessionStorage.getItem('collapse-state-' + id);
  const isOpen = stored === 'open'; // default closed
  setCollapseUI(content, btn, isOpen);
});

document.querySelectorAll('button.collapse-toggle').forEach(function(btn) {
  btn.addEventListener('click', function() {
    const id = btn.dataset.toggles;
    const content = document.getElementById(id);
    if (!content) return;
    const willOpen = !content.classList.contains('open');
    setCollapseUI(content, btn, willOpen);
    sessionStorage.setItem('collapse-state-' + id, willOpen ? 'open' : 'closed');
  });
});
</script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Entry point


def _money(v: float, currency: str = "") -> str:
    sign = "-" if v < 0 else ""
    a = abs(v)
    if a >= 1_000_000:
        return f"{sign}{currency}{a / 1_000_000:.2f}M"
    if a >= 1_000:
        return f"{sign}{currency}{a / 1_000:.1f}K"
    return f"{sign}{currency}{a:.0f}"


def _signal_key(label: str) -> str:
    s = label.lower()
    if "buy" in s:
        return "buy"
    if "trim" in s:
        return "trim"
    if "sell" in s:
        return "sell"
    return "hold"


def _health_class(score: float) -> str:
    if score >= 75:
        return "green"
    if score >= 50:
        return "amber"
    return "red"


def _pnl_class(v: float) -> str:
    if v > 0:
        return "pnl-pos"
    if v < 0:
        return "pnl-neg"
    return ""


def _prepare_portfolio(analysis: dict, currency: str = "") -> dict:
    holdings = []
    for p in analysis["positions"]:
        comp = p.get("composite_score")
        holdings.append({
            "ticker": p["ticker"],
            "sector": p.get("sector"),
            "quantity": p["quantity"],
            "avg_buy_price": p["avg_buy_price"],
            "current_price": p["current_price"],
            "current_value": p["current_value"],
            "unrealised_pnl": p["unrealised_pnl"],
            "unrealised_pnl_pct": p["unrealised_pnl_pct"],
            "composite_score": comp,
            "signal": p["signal"],
            "signal_key": _signal_key(p["signal"]),
            "signal_reason": p["signal_reason"],
            "avg_buy_fmt": f"{p['avg_buy_price']:.2f}",
            "current_fmt": f"{p['current_price']:.2f}",
            "value_fmt": _money(p["current_value"], currency),
            "pnl_fmt": _money(p["unrealised_pnl"], currency),
            "pnl_pct_fmt": f"{p['unrealised_pnl_pct']:+.1f}%",
            "pnl_class": _pnl_class(p["unrealised_pnl"]),
            "composite_fmt": f"{comp:.1f}" if comp is not None else "N/A",
            "composite_class": _composite_class(comp) if comp is not None else "",
        })
    suggestions = []
    for s in analysis.get("suggestions", []):
        comp = s.get("composite_score")
        suggestions.append({
            **s,
            "pnl_pct_fmt": f"{s['current_pnl_pct']:+.1f}%",
            "pnl_class": _pnl_class(s["current_pnl_pct"]),
            "composite_fmt": f"{comp:.1f}" if comp is not None else "N/A",
            "composite_class": _composite_class(comp) if comp is not None else "",
        })

    return {
        "holdings": holdings,
        "suggestions": suggestions,
        "n_positions": analysis["n_positions"],
        "n_winners": analysis["n_winners"],
        "n_losers": analysis["n_losers"],
        "total_value": analysis["total_value"],
        "total_cost": analysis["total_cost"],
        "total_pnl": analysis["total_pnl"],
        "total_pnl_pct": analysis["total_pnl_pct"],
        "total_value_fmt": _money(analysis["total_value"], currency),
        "total_cost_fmt": _money(analysis["total_cost"], currency),
        "total_pnl_fmt": _money(analysis["total_pnl"], currency),
        "total_pnl_pct_fmt": f"{analysis['total_pnl_pct']:+.1f}%",
        "pnl_class": _pnl_class(analysis["total_pnl"]).replace("pnl-pos", "pos").replace("pnl-neg", "neg"),
        "health_score": analysis["health_score"],
        "health_class": _health_class(analysis["health_score"]),
        "health_components": analysis["health_components"],
    }


def generate_report(db_path: str, output_path: str,
                    include_portfolio: bool = False,
                    portfolio_data: Optional[dict] = None,
                    run_date: Optional[str] = None,
                    portfolio_currency: str = "") -> str:
    """Render an HTML report.

    The Portfolio Dashboard section is only included when
    `include_portfolio=True` AND `portfolio_data` is provided. Pass
    `include_portfolio=False` (the default) to produce the public report.
    """
    rows, indices = _load_latest(db_path, run_date)

    us_raw: List[dict] = []
    uk_raw: List[dict] = []
    index_counts: Dict[str, int] = {}

    for r in rows:
        idx_str = indices.get(r["ticker"], "")
        idx_set = {s for s in idx_str.split(",") if s}
        for name in idx_set:
            index_counts[name] = index_counts.get(name, 0) + 1
        if idx_set & {"SP500", "NASDAQ100"}:
            us_raw.append(r)
        if idx_set & {"FTSE100", "FTSE250"}:
            uk_raw.append(r)

    sections = [
        _build_section("us-markets", "US Markets (S&P 500 + NASDAQ 100)", us_raw),
        _build_section("uk-markets", "UK Markets (FTSE 100 + FTSE 250)", uk_raw),
    ]

    top20_rows, sector_counts = _build_top20(rows, indices)
    sector_breakdown = sorted(sector_counts.items(), key=lambda kv: kv[1], reverse=True)
    max_sector_count = max((c for _, c in sector_breakdown), default=1)

    leaders = _consensus_leaders(rows)

    portfolio_ctx = (
        _prepare_portfolio(portfolio_data, portfolio_currency)
        if (include_portfolio and portfolio_data)
        else None
    )

    template = Template(HTML_TEMPLATE)
    html = template.render(
        columns=COLUMNS,
        text_cols=TEXT_COLS,
        board_columns=BOARD_COLUMNS,
        board_text_cols=BOARD_TEXT_COLS,
        top20_columns=TOP20_COLUMNS,
        top20_text_cols=TOP20_TEXT_COLS,
        top20_rows=top20_rows,
        sector_breakdown=sector_breakdown,
        max_sector_count=max_sector_count,
        leaders=leaders,
        sections=sections,
        portfolio=portfolio_ctx,
        run_date=run_date or "(latest)",
        generated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        total_processed=len(rows),
        index_counts=dict(sorted(index_counts.items())),
    )

    with open(output_path, "w") as f:
        f.write(html)
    return output_path


def _top_n_for_email(db_path: str, n: int = 10,
                     run_date: Optional[str] = None) -> List[dict]:
    rows, indices = _load_latest(db_path, run_date)
    sortable = [r for r in rows if r.get("score_composite") is not None]
    sortable.sort(key=lambda r: float(r["score_composite"]), reverse=True)
    return [
        {
            "rank": i,
            "ticker": r["ticker"],
            "is_uk": r["ticker"].upper().endswith(".L"),
            "company_name": r.get("company_name") or "",
            "sector": r.get("sector") or "",
            "indices": indices.get(r["ticker"], ""),
            "score_composite": float(r["score_composite"]),
            "top3": _top3_endorsers(r),
        }
        for i, r in enumerate(sortable[:n], start=1)
    ]


def _summary_portfolio_block(portfolio_data: dict) -> dict:
    """Compact portfolio summary for the email — Health Score + signal counts + top 3 by value."""
    positions = sorted(
        portfolio_data["positions"],
        key=lambda p: p["current_value"],
        reverse=True,
    )
    signals = {"Buy More": 0, "Hold": 0, "Trim": 0, "Sell": 0}
    for p in positions:
        signals[p["signal"]] = signals.get(p["signal"], 0) + 1
    return {
        "health_score": portfolio_data["health_score"],
        "health_class": _health_class(portfolio_data["health_score"]),
        "n_positions": portfolio_data["n_positions"],
        "n_winners": portfolio_data["n_winners"],
        "signals": signals,
        "top3": positions[:3],
    }


SUMMARY_EMAIL_TEMPLATE = """<!DOCTYPE html>
<html><head><meta charset="utf-8"></head>
<body style="font-family: -apple-system, Helvetica, Arial, sans-serif; max-width: 720px; margin: 0 auto; padding: 24px; color: #222;">
<h1 style="margin:0 0 4px;">StockBoard {{ heading_suffix }}</h1>
<div style="color:#666; font-size:13px; margin-bottom: 18px;">Run date: {{ run_date }}</div>

{% if portfolio %}
<div style="background:#f3f5f8; border-left:4px solid #2c6cb0; padding:14px 18px; border-radius:6px; margin-bottom:20px;">
  <div style="font-size:11px; text-transform:uppercase; color:#666; letter-spacing:0.5px; margin-bottom:4px;">Portfolio Health Score</div>
  <div style="font-size:32px; font-weight:700; color:{{ portfolio_color }}; line-height:1;">{{ portfolio.health_score }}</div>
  <div style="font-size:12px; color:#444; margin-top:8px;">
    Positions: <b>{{ portfolio.n_positions }}</b> ·
    Winners: <b>{{ portfolio.n_winners }}</b> ·
    Signals: BUY <b>{{ portfolio.signals['Buy More'] }}</b>
    · TRIM <b>{{ portfolio.signals['Trim'] }}</b>
    · SELL <b>{{ portfolio.signals['Sell'] }}</b>
    · HOLD <b>{{ portfolio.signals['Hold'] }}</b>
  </div>
  <div style="margin-top:10px;">
    <div style="font-size:11px; text-transform:uppercase; color:#666; letter-spacing:0.5px; margin-bottom:4px;">Top 3 Holdings by Value</div>
    <table style="border-collapse:collapse; font-size:12px; width:100%;">
      <thead>
        <tr style="background:#fff;">
          <th style="text-align:left; padding:4px 8px; border-bottom:1px solid #ddd;">Ticker</th>
          <th style="text-align:right; padding:4px 8px; border-bottom:1px solid #ddd;">Value</th>
          <th style="text-align:right; padding:4px 8px; border-bottom:1px solid #ddd;">P&amp;L %</th>
          <th style="text-align:left;  padding:4px 8px; border-bottom:1px solid #ddd;">Signal</th>
        </tr>
      </thead>
      <tbody>
      {% for p in portfolio.top3 %}
        <tr>
          <td style="padding:4px 8px;"><b>{{ p.ticker }}</b></td>
          <td style="padding:4px 8px; text-align:right;">{{ p.current_value|round|int }}</td>
          <td style="padding:4px 8px; text-align:right; color:{{ '#1b5e20' if p.unrealised_pnl_pct > 0 else '#b71c1c' }};">
            {{ '%+.1f'|format(p.unrealised_pnl_pct) }}%
          </td>
          <td style="padding:4px 8px;">{{ p.signal }}</td>
        </tr>
      {% endfor %}
      </tbody>
    </table>
  </div>
</div>
{% endif %}

<h2 style="margin:0 0 6px; font-size:16px;">Today's Top 10 Board Picks</h2>
<table style="border-collapse:collapse; font-size:12px; width:100%;">
  <thead>
    <tr style="background:#fff;">
      <th style="text-align:right; padding:4px 8px; border-bottom:2px solid #ccc; width:32px;">#</th>
      <th style="text-align:left;  padding:4px 8px; border-bottom:2px solid #ccc;">Ticker</th>
      <th style="text-align:left;  padding:4px 8px; border-bottom:2px solid #ccc;">Company</th>
      <th style="text-align:left;  padding:4px 8px; border-bottom:2px solid #ccc;">Sector</th>
      <th style="text-align:right; padding:4px 8px; border-bottom:2px solid #ccc;">Composite</th>
      <th style="text-align:left;  padding:4px 8px; border-bottom:2px solid #ccc;">Top 3 Endorsers</th>
    </tr>
  </thead>
  <tbody>
    {% for r in top_n %}
    <tr style="background: {{ '#fffbea' if r.rank <= 3 else ('#fafafa' if loop.index0 % 2 == 0 else '#fff') }};">
      <td style="padding:4px 8px; text-align:right;">{{ r.rank }}</td>
      <td style="padding:4px 8px;"><b>{{ r.ticker }}</b>{% if r.is_uk %} 🇬🇧{% endif %}</td>
      <td style="padding:4px 8px;">{{ r.company_name[:30] }}</td>
      <td style="padding:4px 8px;">{{ r.sector }}</td>
      <td style="padding:4px 8px; text-align:right;"><b>{{ '%.1f'|format(r.score_composite) }}</b></td>
      <td style="padding:4px 8px;">{{ r.top3 }}</td>
    </tr>
    {% endfor %}
  </tbody>
</table>

<div style="margin: 20px 0 8px; text-align:center;">
  {% if full_report_url %}
  <a href="{{ full_report_url }}"
     style="display:inline-block; padding:10px 22px; background:#2c6cb0; color:#fff;
            font-weight:600; text-decoration:none; border-radius:4px; font-size:13px;">
    View Full Report →
  </a>
  {% endif %}
  {% if attachment_note %}
  <div style="font-size:12px; color:#777; margin-top:10px;">{{ attachment_note }}</div>
  {% endif %}
</div>

<div style="color:#aaa; font-size:11px; margin-top:18px; text-align:center;">
  Generated by StockBoard · {{ generated_at }}
</div>
</body>
</html>
"""


def generate_summary_email_html(
    db_path: str,
    full_report_url: Optional[str] = None,
    portfolio_data: Optional[dict] = None,
    heading_suffix: str = "Daily Report",
    attachment_note: Optional[str] = None,
    run_date: Optional[str] = None,
    top_n: int = 10,
) -> str:
    """Build a compact email-body HTML summary (Top N + optional Portfolio block)."""
    rows, _ = _load_latest(db_path, run_date)
    if run_date is None:
        run_date = max((r["run_date"] for r in rows), default="(latest)")
    top = _top_n_for_email(db_path, n=top_n, run_date=run_date)

    portfolio_summary = _summary_portfolio_block(portfolio_data) if portfolio_data else None
    portfolio_color = "#222"
    if portfolio_summary:
        cls = portfolio_summary["health_class"]
        portfolio_color = {"green": "#2e7d32", "amber": "#b27000", "red": "#b71c1c"}.get(cls, "#222")

    tpl = Template(SUMMARY_EMAIL_TEMPLATE)
    return tpl.render(
        heading_suffix=heading_suffix,
        run_date=run_date,
        portfolio=portfolio_summary,
        portfolio_color=portfolio_color,
        top_n=top,
        full_report_url=full_report_url,
        attachment_note=attachment_note,
        generated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )


if __name__ == "__main__":
    import sys
    db = sys.argv[1] if len(sys.argv) > 1 else "screener.db"
    out = sys.argv[2] if len(sys.argv) > 2 else "reports/daily_report.html"
    path = generate_report(db, out)
    print(f"Report written to {path}")
