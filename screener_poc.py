"""FTSE 250 stock screener proof-of-concept."""

from __future__ import annotations

import time
import math
from typing import Any

import pandas as pd
import yfinance as yf
from tabulate import tabulate
from colorama import Fore, Style, init as colorama_init


TICKERS = [
    "AEP.L", "ATYM.L", "AJB.L", "GDWN.L", "GNC.L", "HOC.L",
    "ITH.L", "KLR.L", "MGNS.L", "PAF.L", "WOSG.L",
]


def safe_get(d: dict, key: str) -> Any:
    val = d.get(key) if isinstance(d, dict) else None
    if val is None:
        return None
    if isinstance(val, float) and math.isnan(val):
        return None
    return val


def compute_rsi(close: pd.Series, period: int = 14) -> float | None:
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
    if pd.isna(latest):
        return None
    return float(latest)


def compute_sortino(close: pd.Series) -> float | None:
    if close is None or len(close) < 30:
        return None
    returns = close.pct_change().dropna()
    if returns.empty:
        return None
    downside = returns[returns < 0]
    if downside.empty or downside.std() == 0:
        return None
    mean_ret = returns.mean()
    downside_std = downside.std()
    return float((mean_ret / downside_std) * math.sqrt(252))


def compute_macd_rising(close: pd.Series) -> bool | None:
    if close is None or len(close) < 35:
        return None
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    if pd.isna(macd.iloc[-1]) or pd.isna(signal.iloc[-1]):
        return None
    return bool(macd.iloc[-1] > signal.iloc[-1])


def first_value(df: pd.DataFrame, row_names: list[str]) -> float | None:
    """Get the most-recent value of the first matching row from a yfinance financial DataFrame."""
    if df is None or df.empty:
        return None
    for name in row_names:
        if name in df.index:
            series = df.loc[name].dropna()
            if not series.empty:
                return float(series.iloc[0])
    return None


def compute_metrics(ticker: str) -> dict:
    metrics: dict = {"ticker": ticker}
    try:
        tk = yf.Ticker(ticker)
        info = tk.info or {}

        metrics["market_cap"] = safe_get(info, "marketCap")

        roa = safe_get(info, "returnOnAssets")
        metrics["roa"] = roa * 100 if roa is not None else None

        metrics["pe"] = safe_get(info, "trailingPE")
        metrics["forward_pe"] = safe_get(info, "forwardPE")

        rev_growth = safe_get(info, "revenueGrowth")
        metrics["revenue_growth"] = rev_growth * 100 if rev_growth is not None else None

        trailing_eps = safe_get(info, "trailingEps")
        prev_eps = safe_get(info, "previousEps") or safe_get(info, "priorEps")
        eps_growth = None
        if trailing_eps is not None and prev_eps is not None and prev_eps != 0:
            eps_growth = (trailing_eps - prev_eps) / abs(prev_eps) * 100
        else:
            eg = safe_get(info, "earningsGrowth")
            if eg is not None:
                eps_growth = eg * 100
        metrics["eps_growth"] = eps_growth

        ocf = first_value(tk.cashflow, ["Operating Cash Flow", "Total Cash From Operating Activities"])
        bs = tk.balance_sheet
        total_assets = first_value(bs, ["Total Assets"])
        curr_liab = first_value(bs, ["Current Liabilities", "Total Current Liabilities"])
        croci = None
        if ocf is not None and total_assets is not None and curr_liab is not None:
            denom = total_assets - curr_liab
            if denom != 0:
                croci = (ocf / denom) * 100
        metrics["croci"] = croci

        hist = tk.history(period="1y", auto_adjust=False)
        close = hist["Close"] if not hist.empty else None

        metrics["sortino"] = compute_sortino(close) if close is not None else None
        metrics["latest_close"] = float(close.iloc[-1]) if close is not None and not close.empty else None
        metrics["sma_50"] = float(close.tail(50).mean()) if close is not None and len(close) >= 50 else None
        metrics["rsi_14"] = compute_rsi(close) if close is not None else None

        one_year_return = None
        if close is not None and len(close) >= 2:
            first_price = float(close.iloc[0])
            last_price = float(close.iloc[-1])
            if first_price != 0:
                one_year_return = (last_price - first_price) / first_price * 100
        metrics["one_year_return"] = one_year_return

        metrics["macd_rising"] = compute_macd_rising(close) if close is not None else None
        metrics["error"] = None
    except Exception as e:
        metrics["error"] = str(e)
    return metrics


def fmt_num(v: Any) -> str:
    if v is None:
        return "N/A"
    if isinstance(v, bool):
        return str(v)
    if isinstance(v, (int, float)):
        if math.isnan(v) or math.isinf(v):
            return "N/A"
        if isinstance(v, int) or abs(v) >= 1e6:
            return f"{v:,.0f}"
        return f"{v:.2f}"
    return str(v)


def fmt_rsi(v: Any) -> str:
    if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
        return "N/A"
    s = f"{v:.2f}"
    if 40 <= v <= 70:
        return f"{Fore.GREEN}{s}{Style.RESET_ALL}"
    return f"{Fore.RED}{s}{Style.RESET_ALL}"


def main() -> None:
    colorama_init()
    rows = []
    succeeded = 0
    failed = 0

    for i, ticker in enumerate(TICKERS):
        print(f"Fetching {ticker} ({i + 1}/{len(TICKERS)})...")
        m = compute_metrics(ticker)
        if m.get("error"):
            print(f"  ! {ticker} failed: {m['error']}")
            failed += 1
        else:
            succeeded += 1
        rows.append([
            m["ticker"],
            fmt_num(m.get("market_cap")),
            fmt_num(m.get("roa")),
            fmt_num(m.get("pe")),
            fmt_num(m.get("forward_pe")),
            fmt_num(m.get("revenue_growth")),
            fmt_num(m.get("eps_growth")),
            fmt_num(m.get("croci")),
            fmt_num(m.get("sortino")),
            fmt_num(m.get("latest_close")),
            fmt_num(m.get("sma_50")),
            fmt_rsi(m.get("rsi_14")),
            fmt_num(m.get("one_year_return")),
            fmt_num(m.get("macd_rising")),
        ])
        time.sleep(0.5)

    headers = [
        "Ticker", "Market Cap", "ROA %", "P/E", "Fwd P/E",
        "Rev Growth %", "EPS Growth %", "CROCI %", "Sortino",
        "Close", "50d SMA", "RSI(14)", "1Y Return %", "MACD Rising",
    ]
    print()
    print(tabulate(rows, headers=headers, tablefmt="github"))
    print()
    print(f"Processed: {succeeded} succeeded, {failed} failed (of {len(TICKERS)} tickers)")


if __name__ == "__main__":
    main()
