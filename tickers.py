"""Ticker universe management.

Fetches index constituents from Wikipedia and combines them into a single
deduplicated universe, tracking which index each ticker belongs to.
"""

from __future__ import annotations

import csv
import os
from typing import Dict, List, Set

import requests
from bs4 import BeautifulSoup


WIKI_HEADERS = {
    "User-Agent": "Mozilla/5.0 (stock-screener/1.0; +https://github.com/local) "
                  "Python/requests",
}

SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
NASDAQ100_URL = "https://en.wikipedia.org/wiki/Nasdaq-100"
FTSE100_URL = "https://en.wikipedia.org/wiki/FTSE_100_Index"
FTSE250_URL = "https://en.wikipedia.org/wiki/FTSE_250_Index"
RUSSELL1000_WIKI_URL = "https://en.wikipedia.org/wiki/Russell_1000_Index"
RUSSELL1000_IWB_CSV_URL = (
    "https://www.ishares.com/us/products/239707/ishares-russell-1000-etf/"
    "1467271812596.ajax?fileType=csv&fileName=IWB_holdings&dataType=fund"
)
RUSSELL2000_CSV = "russell2000.csv"
WATCHLIST_CSV = "watchlist.csv"

# Tickers that aren't real equities (ETF holdings CSVs include placeholders).
_NON_EQUITY_PREFIXES = ("USD", "EUR", "GBP", "JPY", "CASH", "MARGIN", "XTSLA")
_NON_EQUITY_KEYWORDS = ("FUTURE", "FORWARD", "OPTION", "SWAP", "INDEX")


def _fetch_html(url: str) -> str:
    resp = requests.get(url, headers=WIKI_HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.text


def _table_by_id(html: str, table_id: str) -> BeautifulSoup | None:
    soup = BeautifulSoup(html, "html.parser")
    return soup.find("table", {"id": table_id})


def _find_column_index(table, header_names: List[str]) -> int | None:
    head = table.find("tr")
    if head is None:
        return None
    cells = head.find_all(["th", "td"])
    for idx, cell in enumerate(cells):
        text = cell.get_text(strip=True).lower()
        if any(name.lower() == text for name in header_names):
            return idx
    return None


def _extract_column(table, col_idx: int) -> List[str]:
    values: List[str] = []
    rows = table.find_all("tr")[1:]
    for row in rows:
        cells = row.find_all(["td", "th"])
        if len(cells) <= col_idx:
            continue
        text = cells[col_idx].get_text(strip=True)
        if text:
            values.append(text)
    return values


def fetch_sp500() -> List[str]:
    html = _fetch_html(SP500_URL)
    table = _table_by_id(html, "constituents")
    if table is None:
        return []
    col = _find_column_index(table, ["Symbol"])
    if col is None:
        return []
    # Wikipedia uses "BRK.B" style; yfinance uses "BRK-B".
    return [t.replace(".", "-") for t in _extract_column(table, col)]


def fetch_nasdaq100() -> List[str]:
    html = _fetch_html(NASDAQ100_URL)
    table = _table_by_id(html, "constituents")
    if table is None:
        # Fallback: scan all wikitables for one with a Ticker column.
        soup = BeautifulSoup(html, "html.parser")
        for t in soup.find_all("table", class_="wikitable"):
            idx = _find_column_index(t, ["Ticker", "Symbol"])
            if idx is not None:
                return [s.replace(".", "-") for s in _extract_column(t, idx)]
        return []
    col = _find_column_index(table, ["Ticker", "Symbol"])
    if col is None:
        return []
    return [t.replace(".", "-") for t in _extract_column(table, col)]


def _scrape_ftse_index(url: str, index_label: str) -> List[str]:
    """Shared scraper for FTSE 100 / FTSE 250 Wikipedia pages.

    Both pages publish a constituents table with an 'EPIC' or 'Ticker' column.
    Returns symbols with the .L suffix applied; empty list on scrape failure
    (with an error printed to stderr).
    """
    try:
        html = _fetch_html(url)
    except Exception as e:
        print(f"[tickers] {index_label}: failed to fetch {url}: {e}")
        return []
    soup = BeautifulSoup(html, "html.parser")
    for table in soup.find_all("table", class_="wikitable"):
        idx = _find_column_index(table, ["Ticker", "EPIC", "Symbol"])
        if idx is None:
            continue
        raw = _extract_column(table, idx)
        if not raw:
            continue
        out: List[str] = []
        for sym in raw:
            sym = sym.upper().strip()
            if not sym.endswith(".L"):
                sym = f"{sym}.L"
            out.append(sym)
        return out
    print(f"[tickers] {index_label}: no constituents table found at {url}")
    return []


def fetch_ftse100() -> List[str]:
    return _scrape_ftse_index(FTSE100_URL, "FTSE100")


def fetch_ftse250() -> List[str]:
    return _scrape_ftse_index(FTSE250_URL, "FTSE250")


def _fetch_russell1000_wikipedia() -> List[str]:
    """Try the Wikipedia Russell 1000 page first — most years it has no full
    constituent table, so this is expected to return [] and trigger the fallback.
    """
    try:
        html = _fetch_html(RUSSELL1000_WIKI_URL)
    except Exception:
        return []
    soup = BeautifulSoup(html, "html.parser")
    for table in soup.find_all("table", class_="wikitable"):
        idx = _find_column_index(table, ["Symbol", "Ticker"])
        if idx is None:
            continue
        raw = _extract_column(table, idx)
        # Sanity check: a real constituent table should have >500 rows.
        if len(raw) >= 500:
            return [t.upper().replace(".", "-") for t in raw if t.strip()]
    return []


def _parse_iwb_csv(content: str) -> List[str]:
    """Parse the iShares IWB holdings CSV.

    The file has 8–10 metadata lines at the top (fund name, date, NAV) then a
    header row whose first cell is literally "Ticker", then one row per holding.
    """
    import csv
    import io

    reader = csv.reader(io.StringIO(content))
    rows = [r for r in reader if r]
    header_idx = None
    for i, row in enumerate(rows):
        if row and row[0].strip().strip('"').lower() == "ticker":
            header_idx = i
            break
    if header_idx is None:
        return []

    headers = [h.strip().lower() for h in rows[header_idx]]
    ticker_col = 0
    asset_col = None
    for i, h in enumerate(headers):
        if h == "ticker":
            ticker_col = i
        elif h in ("asset class", "asset_class"):
            asset_col = i

    out: List[str] = []
    for row in rows[header_idx + 1:]:
        if not row or ticker_col >= len(row):
            continue
        ticker = row[ticker_col].strip().strip('"').upper()
        if not ticker or ticker == "-":
            continue
        if asset_col is not None and asset_col < len(row):
            asset = row[asset_col].strip().strip('"').lower()
            if asset and "equity" not in asset:
                continue
        if ticker.startswith(_NON_EQUITY_PREFIXES):
            continue
        if any(k in ticker for k in _NON_EQUITY_KEYWORDS):
            continue
        # Tickers with embedded "-" can be legit (BRK-B, BF-B) so don't blanket reject.
        # Tickers with periods are rare in the iShares feed; normalise just in case.
        ticker = ticker.replace(".", "-")
        out.append(ticker)
    return out


def fetch_russell1000() -> List[str]:
    """Constituents of the Russell 1000.

    Tries the Wikipedia page first (which usually has no full constituent
    table); falls back to parsing the iShares IWB holdings CSV.
    """
    tickers = _fetch_russell1000_wikipedia()
    if not tickers:
        try:
            resp = requests.get(
                RUSSELL1000_IWB_CSV_URL,
                headers=WIKI_HEADERS,
                timeout=30,
            )
            resp.raise_for_status()
            tickers = _parse_iwb_csv(resp.text)
        except Exception as e:
            print(f"[tickers] Russell 1000 iShares fallback failed: {e}")
            return []
    print(f"[tickers] Russell 1000: {len(tickers)} tickers fetched")
    return tickers


def load_watchlist(filepath: str = WATCHLIST_CSV) -> List[str]:
    """Load manually-curated tickers from a CSV (one ticker per row, header skipped).

    Rows where the ticker cell is empty or starts with `#` are ignored.
    Returns [] silently if the file doesn't exist.
    """
    if not os.path.exists(filepath):
        print(f"[tickers] Watchlist: not found, skipping ({filepath})")
        return []
    tickers: List[str] = []
    with open(filepath, newline="") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if not row:
                continue
            sym = row[0].strip()
            if i == 0 and sym.lower() in {"symbol", "ticker"}:
                continue
            if not sym or sym.startswith("#"):
                continue
            tickers.append(sym.upper())
    print(f"[tickers] Watchlist: {len(tickers)} tickers loaded")
    return tickers


def load_russell2000() -> List[str]:
    # Russell 2000 constituents aren't published on Wikipedia. Drop a CSV with
    # one symbol per line (header optional) at RUSSELL2000_CSV to populate.
    if not os.path.exists(RUSSELL2000_CSV):
        return []
    tickers: List[str] = []
    with open(RUSSELL2000_CSV, newline="") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if not row:
                continue
            sym = row[0].strip()
            if i == 0 and sym.lower() in {"symbol", "ticker"}:
                continue
            if sym:
                tickers.append(sym.replace(".", "-"))
    return tickers


def get_universe(verbose: bool = True) -> Dict[str, Set[str]]:
    """Return a mapping of ticker -> set of indices it belongs to."""
    sources = {
        "SP500": fetch_sp500(),
        "NASDAQ100": fetch_nasdaq100(),
        "RUSSELL1000": fetch_russell1000(),
        "RUSSELL2000": load_russell2000(),
        "FTSE100": fetch_ftse100(),
        "FTSE250": fetch_ftse250(),
        "WATCHLIST": load_watchlist(),
    }
    if verbose:
        for name, tickers in sources.items():
            print(f"  {name}: {len(tickers)} tickers fetched")
    universe: Dict[str, Set[str]] = {}
    for index_name, tickers in sources.items():
        for t in tickers:
            universe.setdefault(t, set()).add(index_name)
    return universe


def smoke_universe() -> Dict[str, Set[str]]:
    """Fixed 10-ticker universe used by `screener.py --smoke`."""
    return {
        "AAPL":   {"SP500", "NASDAQ100"},
        "MSFT":   {"SP500", "NASDAQ100"},
        "AMZN":   {"SP500", "NASDAQ100"},
        "GOOGL":  {"SP500", "NASDAQ100"},
        "NVDA":   {"SP500", "NASDAQ100"},
        "AJB.L":  {"FTSE250"},
        "WOSG.L": {"FTSE250"},
        "KLR.L":  {"FTSE250"},
        "PAF.L":  {"FTSE250"},
        "HOC.L":  {"FTSE250"},
    }


def ftse100_smoke_universe() -> Dict[str, Set[str]]:
    """5-ticker FTSE 100 smoke universe (used by `screener.py --smoke-ftse100`)."""
    return {
        "SHEL.L": {"FTSE100"},
        "AZN.L":  {"FTSE100"},
        "HSBA.L": {"FTSE100"},
        "ULVR.L": {"FTSE100"},
        "BP.L":   {"FTSE100"},
    }


def russell1000_smoke_universe() -> Dict[str, Set[str]]:
    """5-ticker Russell 1000 smoke universe (used by `screener.py --smoke-russell1000`).
    These tickers are intentionally outside SP500/NASDAQ100 so we exercise the
    new index membership tagging.
    """
    return {
        "PLTR": {"RUSSELL1000"},
        "UBER": {"RUSSELL1000"},
        "LMND": {"RUSSELL1000"},
        "ZETA": {"RUSSELL1000"},
        "IREN": {"RUSSELL1000"},
    }


def _print_breakdown(universe: Dict[str, Set[str]]) -> None:
    """Print a detailed breakdown of who belongs to what."""
    buckets: Dict[str, int] = {}
    per_index: Dict[str, int] = {}
    for indices in universe.values():
        key = "+".join(sorted(indices))
        buckets[key] = buckets.get(key, 0) + 1
        for idx in indices:
            per_index[idx] = per_index.get(idx, 0) + 1

    print()
    print(f"Total unique tickers: {len(universe)}")
    print()
    print("By index (total membership):")
    for idx in sorted(per_index):
        print(f"  {idx:<15} {per_index[idx]:>5}")
    print()
    print("By index combination:")
    for key in sorted(buckets):
        print(f"  {key:<30} {buckets[key]:>5}")


if __name__ == "__main__":
    u = get_universe(verbose=True)
    _print_breakdown(u)
