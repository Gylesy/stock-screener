"""Main screener pipeline.

Fetches data for the configured ticker universe, computes a comprehensive
metric set, writes rows to SQLite, and triggers HTML report generation.

Usage:
    python screener.py            # full universe (slow!)
    python screener.py --smoke    # fixed 10-ticker sample
"""

from __future__ import annotations

import argparse
import math
import os
import sqlite3
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

import pandas as pd
import yfinance as yf

import tickers as tickers_mod
import report as report_mod
import board as board_mod


DB_PATH = "screener.db"
FAILED_LOG = "failed_tickers.log"
REPORT_PATH = "reports/daily_report.html"
BATCH_SIZE = 50
TICKER_DELAY_S = 1.0
BATCH_DELAY_S = 5.0
ESTIMATED_SECONDS_PER_TICKER = 4.0  # rough — includes throttle + network

RATING_MAP = {
    "strong_buy": "Strong Buy",
    "buy": "Buy",
    "outperform": "Outperform",
    "hold": "Hold",
    "underperform": "Underperform",
    "sell": "Sell",
    "strong_sell": "Strong Sell",
}


@dataclass
class Metrics:
    ticker: str
    company_name: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    pe_ratio: Optional[float] = None
    forward_pe: Optional[float] = None
    peg_ratio: Optional[float] = None
    revenue_growth_pct: Optional[float] = None
    eps_growth_yoy: Optional[float] = None
    eps_growth_qoq: Optional[float] = None
    roa: Optional[float] = None
    roe: Optional[float] = None
    profit_margin: Optional[float] = None
    croci_approx: Optional[float] = None
    analyst_rating: Optional[str] = None
    target_price: Optional[float] = None
    upside_pct: Optional[float] = None
    latest_close: Optional[float] = None
    sma_50: Optional[float] = None
    sma_200: Optional[float] = None
    rsi_14: Optional[float] = None
    macd_daily_rising: Optional[bool] = None
    macd_weekly_rising: Optional[bool] = None
    macd_monthly_rising: Optional[bool] = None
    golden_cross_date: Optional[str] = None
    ret_1w: Optional[float] = None
    ret_1m: Optional[float] = None
    ret_3m: Optional[float] = None
    ret_6m: Optional[float] = None
    ret_ytd: Optional[float] = None
    ret_1y: Optional[float] = None
    sortino_ratio: Optional[float] = None
    next_earnings_date: Optional[str] = None
    error: Optional[str] = field(default=None, repr=False)


# ---------------------------------------------------------------------------
# Helpers


def _clean(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


def _info_get(info: dict, key: str) -> Any:
    return _clean(info.get(key)) if isinstance(info, dict) else None


def _first_value(df: pd.DataFrame, names: List[str]) -> Optional[float]:
    if df is None or df.empty:
        return None
    for n in names:
        if n in df.index:
            s = df.loc[n].dropna()
            if not s.empty:
                return float(s.iloc[0])
    return None


# ---------------------------------------------------------------------------
# Technical indicators


def _rsi(close: pd.Series, period: int = 14) -> Optional[float]:
    if close is None or len(close) < period + 1:
        return None
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    latest = rsi.iloc[-1]
    return None if pd.isna(latest) else float(latest)


def _macd_rising(close: pd.Series, min_len: int = 35) -> Optional[bool]:
    if close is None or len(close) < min_len:
        return None
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    if pd.isna(macd.iloc[-1]) or pd.isna(signal.iloc[-1]):
        return None
    return bool(macd.iloc[-1] > signal.iloc[-1])


def _sortino(close: pd.Series) -> Optional[float]:
    if close is None or len(close) < 30:
        return None
    returns = close.pct_change().dropna()
    downside = returns[returns < 0]
    if downside.empty or downside.std() == 0:
        return None
    return float((returns.mean() / downside.std()) * math.sqrt(252))


def _golden_cross_date(close: pd.Series) -> Optional[str]:
    if close is None or len(close) < 201:
        return None
    sma50 = close.rolling(50).mean()
    sma200 = close.rolling(200).mean()
    above = sma50 > sma200
    crossover = above & ~above.shift(1, fill_value=False)
    crosses = crossover[crossover]
    if crosses.empty:
        return None
    return str(crosses.index[-1].date())


def _pct_return_over(close: pd.Series, days: int) -> Optional[float]:
    if close is None or len(close) <= days:
        return None
    end = float(close.iloc[-1])
    start = float(close.iloc[-1 - days])
    if start == 0:
        return None
    return (end - start) / start * 100


def _ytd_return(close: pd.Series) -> Optional[float]:
    if close is None or close.empty:
        return None
    year = close.index[-1].year
    ytd = close[close.index.year == year]
    if len(ytd) < 2:
        return None
    start = float(ytd.iloc[0])
    end = float(ytd.iloc[-1])
    if start == 0:
        return None
    return (end - start) / start * 100


# ---------------------------------------------------------------------------
# Fundamentals helpers


def _eps_growth_qoq(tk: yf.Ticker) -> Optional[float]:
    for attr in ("quarterly_income_stmt", "quarterly_financials"):
        df = getattr(tk, attr, None)
        if df is None or getattr(df, "empty", True):
            continue
        for row in ("Diluted EPS", "Basic EPS"):
            if row in df.index:
                series = df.loc[row].dropna()
                if len(series) >= 2:
                    curr = float(series.iloc[0])
                    prev = float(series.iloc[1])
                    if prev == 0:
                        return None
                    return (curr - prev) / abs(prev) * 100
    return None


def _next_earnings_date(tk: yf.Ticker) -> Optional[str]:
    try:
        cal = tk.calendar
    except Exception:
        cal = None
    if isinstance(cal, dict):
        d = cal.get("Earnings Date")
        if isinstance(d, list) and d:
            return str(d[0])
        if d:
            return str(d)
    if isinstance(cal, pd.DataFrame) and not cal.empty and "Earnings Date" in cal.index:
        s = cal.loc["Earnings Date"].dropna()
        if not s.empty:
            return str(s.iloc[0])
    try:
        ed = tk.earnings_dates
        if isinstance(ed, pd.DataFrame) and not ed.empty:
            future = ed[ed.index > pd.Timestamp.now(tz=ed.index.tz)]
            if not future.empty:
                return str(future.index.min().date())
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Metric computation


def compute_metrics(ticker: str) -> Metrics:
    m = Metrics(ticker=ticker)
    try:
        tk = yf.Ticker(ticker)
        info = tk.info or {}

        m.company_name = _info_get(info, "longName") or _info_get(info, "shortName")
        m.sector = _info_get(info, "sector")
        m.industry = _info_get(info, "industry")

        m.pe_ratio = _info_get(info, "trailingPE")
        m.forward_pe = _info_get(info, "forwardPE")
        m.peg_ratio = _info_get(info, "pegRatio") or _info_get(info, "trailingPegRatio")

        rg = _info_get(info, "revenueGrowth")
        m.revenue_growth_pct = rg * 100 if rg is not None else None
        eg = _info_get(info, "earningsGrowth")
        m.eps_growth_yoy = eg * 100 if eg is not None else None
        m.eps_growth_qoq = _eps_growth_qoq(tk)

        roa = _info_get(info, "returnOnAssets")
        m.roa = roa * 100 if roa is not None else None
        roe = _info_get(info, "returnOnEquity")
        m.roe = roe * 100 if roe is not None else None
        pm = _info_get(info, "profitMargins")
        m.profit_margin = pm * 100 if pm is not None else None

        ocf = _first_value(tk.cashflow, [
            "Operating Cash Flow",
            "Total Cash From Operating Activities",
        ])
        bs = tk.balance_sheet
        ta = _first_value(bs, ["Total Assets"])
        cl = _first_value(bs, ["Current Liabilities", "Total Current Liabilities"])
        if ocf is not None and ta is not None and cl is not None and (ta - cl) != 0:
            m.croci_approx = (ocf / (ta - cl)) * 100

        rec_key = _info_get(info, "recommendationKey")
        if rec_key:
            m.analyst_rating = RATING_MAP.get(str(rec_key).lower(), str(rec_key).title())
        m.target_price = _info_get(info, "targetMeanPrice")

        # Daily history
        daily = tk.history(period="400d", auto_adjust=False)
        close = daily["Close"] if not daily.empty else None

        if close is not None and not close.empty:
            m.latest_close = float(close.iloc[-1])
            if m.target_price and m.latest_close:
                m.upside_pct = (m.target_price - m.latest_close) / m.latest_close * 100

            if len(close) >= 50:
                m.sma_50 = float(close.tail(50).mean())
            if len(close) >= 200:
                m.sma_200 = float(close.tail(200).mean())

            m.rsi_14 = _rsi(close)
            m.macd_daily_rising = _macd_rising(close)
            m.golden_cross_date = _golden_cross_date(close)

            m.ret_1w = _pct_return_over(close, 5)
            m.ret_1m = _pct_return_over(close, 21)
            m.ret_3m = _pct_return_over(close, 63)
            m.ret_6m = _pct_return_over(close, 126)
            m.ret_ytd = _ytd_return(close)
            m.ret_1y = _pct_return_over(close, 252) if len(close) > 252 else _pct_return_over(close, len(close) - 1)

            m.sortino_ratio = _sortino(close)

            weekly = close.resample("W").last().dropna()
            m.macd_weekly_rising = _macd_rising(weekly, min_len=35)

        # Monthly history — fetch separately for enough lookback
        monthly = tk.history(period="3y", interval="1mo", auto_adjust=False)
        if not monthly.empty:
            m.macd_monthly_rising = _macd_rising(monthly["Close"].dropna(), min_len=30)

        m.next_earnings_date = _next_earnings_date(tk)
    except Exception as e:
        m.error = f"{type(e).__name__}: {e}"
    return m


# ---------------------------------------------------------------------------
# Database


SCHEMA = """
CREATE TABLE IF NOT EXISTS daily_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_date TEXT NOT NULL,
    ticker TEXT NOT NULL,
    company_name TEXT, sector TEXT, industry TEXT,
    pe_ratio REAL, forward_pe REAL, peg_ratio REAL,
    revenue_growth_pct REAL, eps_growth_yoy REAL, eps_growth_qoq REAL,
    roa REAL, roe REAL, profit_margin REAL, croci_approx REAL,
    analyst_rating TEXT, target_price REAL, upside_pct REAL,
    latest_close REAL, sma_50 REAL, sma_200 REAL, rsi_14 REAL,
    macd_daily_rising INTEGER, macd_weekly_rising INTEGER, macd_monthly_rising INTEGER,
    golden_cross_date TEXT,
    ret_1w REAL, ret_1m REAL, ret_3m REAL, ret_6m REAL, ret_ytd REAL, ret_1y REAL,
    sortino_ratio REAL,
    next_earnings_date TEXT,
    score_buffett REAL, score_graham REAL, score_lynch REAL, score_templeton REAL,
    score_soros REAL, score_munger REAL, score_simons REAL, score_fisher REAL,
    score_bogle REAL, score_icahn REAL, score_navellier REAL, score_lango REAL,
    score_composite REAL
);
CREATE INDEX IF NOT EXISTS ix_daily_metrics_run_date ON daily_metrics(run_date);
CREATE INDEX IF NOT EXISTS ix_daily_metrics_ticker ON daily_metrics(ticker);

CREATE TABLE IF NOT EXISTS ticker_universe (
    ticker TEXT PRIMARY KEY,
    name TEXT,
    sector TEXT,
    indices TEXT
);
"""

METRIC_COLUMNS = [
    "ticker", "company_name", "sector", "industry",
    "pe_ratio", "forward_pe", "peg_ratio",
    "revenue_growth_pct", "eps_growth_yoy", "eps_growth_qoq",
    "roa", "roe", "profit_margin", "croci_approx",
    "analyst_rating", "target_price", "upside_pct",
    "latest_close", "sma_50", "sma_200", "rsi_14",
    "macd_daily_rising", "macd_weekly_rising", "macd_monthly_rising",
    "golden_cross_date",
    "ret_1w", "ret_1m", "ret_3m", "ret_6m", "ret_ytd", "ret_1y",
    "sortino_ratio", "next_earnings_date",
]

SCORE_COLUMNS = [
    "score_buffett", "score_graham", "score_lynch", "score_templeton",
    "score_soros", "score_munger", "score_simons", "score_fisher",
    "score_bogle", "score_icahn", "score_navellier", "score_lango",
    "score_composite",
]

INSERT_COLUMNS = METRIC_COLUMNS + SCORE_COLUMNS


def init_db(path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    # Idempotently add score columns to pre-existing DBs.
    for col in SCORE_COLUMNS:
        try:
            conn.execute(f"ALTER TABLE daily_metrics ADD COLUMN {col} REAL")
        except sqlite3.OperationalError:
            pass  # column already exists
    conn.commit()
    return conn


def upsert_universe(conn: sqlite3.Connection, universe: Dict[str, Set[str]],
                    name_by_ticker: Dict[str, Optional[str]],
                    sector_by_ticker: Dict[str, Optional[str]]) -> None:
    """Rebuild ticker_universe from scratch so promotions/demotions are reflected."""
    rows = [
        (t, name_by_ticker.get(t), sector_by_ticker.get(t), ",".join(sorted(idxs)))
        for t, idxs in universe.items()
    ]
    conn.execute("DELETE FROM ticker_universe")
    conn.executemany(
        "INSERT INTO ticker_universe(ticker, name, sector, indices) VALUES(?, ?, ?, ?)",
        rows,
    )
    conn.commit()


def insert_metrics(conn: sqlite3.Connection, run_date: str, batch: List[dict]) -> None:
    placeholders = ",".join(["?"] * (len(INSERT_COLUMNS) + 1))
    cols = "run_date," + ",".join(INSERT_COLUMNS)
    rows = []
    for d in batch:
        row = [run_date] + [
            (int(d[c]) if isinstance(d.get(c), bool) else d.get(c))
            for c in INSERT_COLUMNS
        ]
        rows.append(row)
    conn.executemany(f"INSERT INTO daily_metrics({cols}) VALUES({placeholders})", rows)
    conn.commit()


# ---------------------------------------------------------------------------
# Pipeline


def _log_failure(ticker: str, error: str) -> None:
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with open(FAILED_LOG, "a") as f:
        f.write(f"{ts}\t{ticker}\t{error}\n")


def _format_eta(seconds: float) -> str:
    h, rem = divmod(int(seconds), 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h{m:02d}m"
    if m:
        return f"{m}m{s:02d}s"
    return f"{s}s"


def run_pipeline(universe: Dict[str, Set[str]], db_path: str = DB_PATH) -> Dict[str, Any]:
    all_tickers = sorted(universe.keys())
    total = len(all_tickers)
    n_batches = math.ceil(total / BATCH_SIZE)
    eta = total * ESTIMATED_SECONDS_PER_TICKER + n_batches * BATCH_DELAY_S
    print(f"Universe: {total} tickers across {n_batches} batches of {BATCH_SIZE}")
    print(f"Estimated runtime: ~{_format_eta(eta)}")
    print()

    conn = init_db(db_path)
    run_date = datetime.now(timezone.utc).isoformat(timespec="seconds")

    name_by_ticker: Dict[str, Optional[str]] = {}
    sector_by_ticker: Dict[str, Optional[str]] = {}
    succeeded = 0
    failed = 0

    for bi in range(n_batches):
        batch_tickers = all_tickers[bi * BATCH_SIZE:(bi + 1) * BATCH_SIZE]
        print(f"Batch {bi + 1}/{n_batches} ({len(batch_tickers)} tickers)")
        batch_results: List[dict] = []
        for i, t in enumerate(batch_tickers):
            m = compute_metrics(t)
            if m.error:
                # Single retry
                time.sleep(TICKER_DELAY_S)
                m = compute_metrics(t)
            if m.error:
                failed += 1
                _log_failure(t, m.error or "unknown")
                print(f"  [{bi + 1}.{i + 1}] {t}: FAILED ({m.error})")
            else:
                succeeded += 1
                name_by_ticker[t] = m.company_name
                sector_by_ticker[t] = m.sector
                payload = asdict(m)
                payload.update(board_mod.score_all(payload))
                batch_results.append(payload)
                print(f"  [{bi + 1}.{i + 1}] {t}: ok close={m.latest_close} rsi={m.rsi_14} "
                      f"comp={payload.get('score_composite')}")
            time.sleep(TICKER_DELAY_S)

        if batch_results:
            insert_metrics(conn, run_date, batch_results)
        print(f"  Batch done. Running totals: {succeeded} ok, {failed} failed")

        if bi + 1 < n_batches:
            time.sleep(BATCH_DELAY_S)

    upsert_universe(conn, universe, name_by_ticker, sector_by_ticker)
    conn.close()

    return {
        "run_date": run_date,
        "total": total,
        "succeeded": succeeded,
        "failed": failed,
    }


# ---------------------------------------------------------------------------
# Entry point


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="FTSE/S&P/NASDAQ daily screener")
    p.add_argument("--smoke", action="store_true",
                   help="Run with a fixed 10-ticker smoke-test universe.")
    p.add_argument("--smoke-ftse100", action="store_true",
                   help="Run with a 5-ticker FTSE100 smoke universe (SHEL, AZN, HSBA, ULVR, BP).")
    p.add_argument("--db", default=DB_PATH, help="SQLite DB path")
    p.add_argument("--report", default=REPORT_PATH, help="HTML report output path")
    p.add_argument("--no-report", action="store_true", help="Skip HTML report generation")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    if args.smoke_ftse100:
        print("== SMOKE TEST MODE: FTSE 100 (5 tickers) ==")
        universe = tickers_mod.ftse100_smoke_universe()
    elif args.smoke:
        print("== SMOKE TEST MODE: 10 tickers ==")
        universe = tickers_mod.smoke_universe()
    else:
        print("Building universe from Wikipedia...")
        universe = tickers_mod.get_universe()

    summary = run_pipeline(universe, db_path=args.db)

    print()
    print(f"Run complete. {summary['succeeded']}/{summary['total']} ok, {summary['failed']} failed.")
    print(f"Run date: {summary['run_date']}")

    if not args.no_report:
        os.makedirs(os.path.dirname(args.report) or ".", exist_ok=True)
        report_mod.generate_report(db_path=args.db, output_path=args.report,
                                   run_date=summary["run_date"])
        print(f"HTML report written to {args.report}")
    return 0 if summary["failed"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
