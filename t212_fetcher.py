"""Trading 212 portfolio fetcher.

Reads `T212_API_KEY` from the environment (or .env), fetches open positions
and cash via the Trading 212 v0 REST API, normalises tickers to the form
used elsewhere in this project, and writes `portfolio.json` at the repo root.

T212 docs: https://t212public-api-docs.redoc.ly
"""

from __future__ import annotations

import base64
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Optional

import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


T212_BASE = os.environ.get("T212_BASE_URL") or "https://live.trading212.com/api/v0"
PORTFOLIO_ENDPOINT = "/equity/positions"
CASH_ENDPOINT = "/equity/account/cash"
OUTPUT_PATH = "portfolio.json"

MAX_RETRIES = 3
RETRY_DELAY_RATE_LIMIT_S = 5
RETRY_DELAY_CONNECTION_S = 2
REQUEST_TIMEOUT_S = 30


class T212Error(Exception):
    """Raised for any T212 fetch failure with a human-readable message."""


def get_auth_header(api_key: str, api_secret: str) -> str:
    """Build the HTTP Basic auth header value: `Basic base64(key:secret)`."""
    credentials = f"{api_key}:{api_secret}"
    encoded = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")
    return f"Basic {encoded}"


# ---------------------------------------------------------------------------
# Ticker normalisation


def normalise_t212_ticker(t212_ticker: str) -> str:
    """Map T212 internal ticker → screener-side ticker.

    Examples:
        AAPL_US_EQ      -> AAPL
        GOOGL_US_EQ     -> GOOGL
        AZN_LON_EQ      -> AZN.L
        SHEL_LON_EQ     -> SHEL.L
        BRK/B_US_EQ     -> BRK-B
        BF/B_US_EQ      -> BF-B
    """
    if not t212_ticker:
        return ""
    parts = t212_ticker.split("_")
    symbol = parts[0].replace("/", "-")
    exchange = parts[1].upper() if len(parts) > 1 else ""
    if "LON" in exchange:
        return f"{symbol}.L"
    return symbol


# ---------------------------------------------------------------------------
# HTTP


def _api_get(path: str, auth_header: str) -> Any:
    """GET with retry/backoff. Raises T212Error on permanent failure."""
    url = f"{T212_BASE}{path}"
    headers = {"Authorization": auth_header, "Accept": "application/json"}
    last_err: Optional[str] = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT_S)
        except requests.exceptions.Timeout:
            last_err = "request timeout"
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY_CONNECTION_S)
                continue
            raise T212Error("T212 API unreachable — request timed out")
        except requests.exceptions.ConnectionError as e:
            last_err = f"connection error: {e}"
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY_CONNECTION_S)
                continue
            raise T212Error("T212 API unreachable — check internet connection")

        if resp.status_code == 200:
            return resp.json()

        if resp.status_code == 401:
            raise T212Error(
                "T212 API key invalid or expired — generate a new key in the "
                "Trading 212 app (Settings → API)"
            )
        if resp.status_code == 403:
            raise T212Error(
                "Insufficient permissions — enable Account Data in T212 API settings"
            )
        if resp.status_code == 429:
            last_err = "rate limited (429)"
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY_RATE_LIMIT_S)
                continue
            raise T212Error("T212 rate-limited after retries — try again later")

        snippet = resp.text[:200] if resp.text else ""
        raise T212Error(f"T212 {path} returned HTTP {resp.status_code}: {snippet}")

    raise T212Error(f"T212 {path} failed after {MAX_RETRIES} attempts: {last_err}")


# ---------------------------------------------------------------------------
# Normalisation


def _normalise_position(pos: dict) -> dict:
    """Map a T212 `/equity/positions` row to the project's portfolio schema.

    Native fields (averagePricePaid, currentPrice) are in the instrument
    currency. `walletImpact` is in the account currency. We expose effective
    per-share prices in account currency so portfolio totals are comparable
    across positions, and keep the native fields alongside for reference.
    """
    instrument = pos.get("instrument") or {}
    raw_ticker = instrument.get("ticker", "")
    norm = normalise_t212_ticker(raw_ticker)

    qty = float(pos.get("quantity") or 0)
    native_avg = float(pos.get("averagePricePaid") or 0)
    native_cur = float(pos.get("currentPrice") or 0)

    wallet = pos.get("walletImpact") or {}
    cost = float(wallet.get("totalCost") or 0)
    value = float(wallet.get("currentValue") or 0)
    pnl = float(wallet.get("unrealizedProfitLoss") or (value - cost))
    fx_pnl = float(wallet.get("fxImpact") or 0)

    avg_buy_price = (cost / qty) if qty else 0.0
    current_price = (value / qty) if qty else 0.0
    pnl_pct = (pnl / cost * 100) if cost else 0.0

    return {
        "ticker": norm,
        "t212_ticker": raw_ticker,
        "company_name": instrument.get("name"),
        "native_currency": instrument.get("currency"),
        "quantity": qty,
        "avg_buy_price": avg_buy_price,
        "current_price": current_price,
        "current_value": value,
        "cost_basis": cost,
        "unrealised_pnl": pnl,
        "unrealised_pnl_pct": pnl_pct,
        "fx_pnl": fx_pnl,
        "native_avg_price": native_avg,
        "native_current_price": native_cur,
        "first_purchase_date": pos.get("createdAt"),
    }


# ---------------------------------------------------------------------------
# Public entry point


def fetch_t212_portfolio() -> dict:
    """Fetch portfolio + cash from T212 and return a normalised dict."""
    api_key = (os.environ.get("T212_API_KEY") or "").strip()
    api_secret = (os.environ.get("T212_API_SECRET") or "").strip()
    if not api_key or not api_secret:
        raise T212Error(
            "T212_API_KEY and T212_API_SECRET are both required. "
            "Generate both from Trading 212 app → Settings → API"
        )

    auth_header = get_auth_header(api_key, api_secret)
    print(f"Using Basic Auth: {auth_header[:14]}...")

    positions_raw = _api_get(PORTFOLIO_ENDPOINT, auth_header) or []
    if not isinstance(positions_raw, list):
        positions_raw = []

    cash_raw = _api_get(CASH_ENDPOINT, auth_header) or {}
    if not isinstance(cash_raw, dict):
        cash_raw = {}

    positions = [_normalise_position(p) for p in positions_raw]
    free_cash = float(cash_raw.get("free") or 0)
    invested = float(cash_raw.get("invested") or 0)

    return {
        "positions": positions,
        "cash": free_cash,
        "total_value": invested + free_cash,
        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


# ---------------------------------------------------------------------------
# CLI


def _print_positions(positions: list) -> None:
    if not positions:
        print("(no open positions)")
        return
    try:
        from tabulate import tabulate
    except ImportError:
        for p in positions:
            print(p)
        return
    rows = []
    for p in positions:
        rows.append([
            p["ticker"],
            p["t212_ticker"],
            f"{p['quantity']:.4f}",
            f"{p['avg_buy_price']:.4f}",
            f"{p['current_price']:.4f}",
            f"{p['unrealised_pnl']:+,.2f}",
            f"{p['unrealised_pnl_pct']:+.2f}%",
        ])
    print(tabulate(
        rows,
        headers=["Ticker", "T212 Ticker", "Qty", "Avg Buy", "Current", "P&L", "P&L %"],
        tablefmt="github",
    ))


def main() -> int:
    try:
        result = fetch_t212_portfolio()
    except T212Error as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1
    except Exception as e:  # defensive — anything unexpected
        print(f"Unexpected error: {type(e).__name__}: {e}", file=sys.stderr)
        return 1

    with open(OUTPUT_PATH, "w") as f:
        json.dump(result, f, indent=2)

    _print_positions(result["positions"])
    print()
    print(f"Portfolio saved: {len(result['positions'])} positions, "
          f"cash {result['cash']:.2f} (invested+cash total {result['total_value']:.2f})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
