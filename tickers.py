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
RUSSELL2000_CSV = "russell2000.csv"


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
        "RUSSELL2000": load_russell2000(),
        "FTSE100": fetch_ftse100(),
        "FTSE250": fetch_ftse250(),
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
