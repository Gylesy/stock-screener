"""Demo: build a synthetic portfolio, analyse it, and render the HTML report
with the Portfolio Dashboard section.

Looks up the current price, composite score and sector for each holding from
screener.db (latest run_date). For .L tickers the latest_close is in pence so
we divide by 100 to bring it into £ space alongside avg_buy_price.
"""

from __future__ import annotations

import sqlite3
from typing import List

from tabulate import tabulate

import portfolio
import report


DB_PATH = "screener.db"
PUBLIC_REPORT_PATH = "reports/daily_report.html"
PRIVATE_REPORT_PATH = "reports/portfolio_report.html"

POSITIONS_INPUT = [
    {"ticker": "AAPL",  "quantity": 50,  "avg_buy_price": 165.00},
    {"ticker": "MSFT",  "quantity": 30,  "avg_buy_price": 380.00},
    {"ticker": "NVDA",  "quantity": 20,  "avg_buy_price": 480.00},
    {"ticker": "AMZN",  "quantity": 25,  "avg_buy_price": 175.00},
    {"ticker": "GOOGL", "quantity": 40,  "avg_buy_price": 140.00},
    {"ticker": "AJB.L", "quantity": 500, "avg_buy_price": 1.45},
    {"ticker": "HOC.L", "quantity": 300, "avg_buy_price": 5.80},
    {"ticker": "KLR.L", "quantity": 200, "avg_buy_price": 20.50},
]


def _load_ticker_context(db_path: str, tickers: List[str]) -> dict:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT MAX(run_date) FROM daily_metrics")
    run_date = cur.fetchone()[0]
    placeholders = ",".join(["?"] * len(tickers))
    cur.execute(
        f"""SELECT ticker, latest_close, score_composite, sector
            FROM daily_metrics
            WHERE run_date = ? AND ticker IN ({placeholders})""",
        (run_date, *tickers),
    )
    ctx = {r["ticker"]: dict(r) for r in cur.fetchall()}
    conn.close()
    return ctx


def _build_positions() -> List[dict]:
    tickers = [p["ticker"] for p in POSITIONS_INPUT]
    ctx = _load_ticker_context(DB_PATH, tickers)
    enriched = []
    for p in POSITIONS_INPUT:
        c = ctx.get(p["ticker"], {})
        price = c.get("latest_close")
        if price is None:
            raise RuntimeError(
                f"{p['ticker']} not in screener.db — run screener.py first"
            )
        # FTSE tickers report close in pence; bring into pounds to match avg_buy_price
        if p["ticker"].upper().endswith(".L"):
            price = price / 100.0
        enriched.append({
            **p,
            "current_price": float(price),
            "composite_score": c.get("score_composite"),
            "sector": c.get("sector"),
        })
    return enriched


def _print_holdings(analysis: dict) -> None:
    rows = []
    for p in analysis["positions"]:
        rows.append([
            p["ticker"],
            (p.get("sector") or "—")[:18],
            int(p["quantity"]),
            f"{p['avg_buy_price']:.2f}",
            f"{p['current_price']:.2f}",
            f"{p['current_value']:,.0f}",
            f"{p['unrealised_pnl']:+,.0f}",
            f"{p['unrealised_pnl_pct']:+.1f}%",
            f"{p['composite_score']:.1f}" if p.get("composite_score") is not None else "N/A",
            p["signal"],
        ])
    headers = ["Ticker", "Sector", "Qty", "Avg Buy", "Current",
               "Value", "P&L", "P&L %", "Composite", "Signal"]
    print(tabulate(rows, headers=headers, tablefmt="github"))


def _print_suggestions(suggestions: list) -> None:
    if not suggestions:
        print("(no trade suggestions — all positions on Hold)")
        return
    rows = []
    for s in suggestions:
        rows.append([
            s["action"],
            s["ticker"],
            s["current_quantity"],
            s["suggested_quantity"],
            f"{s['current_pnl_pct']:+.1f}%",
            f"{s['composite_score']:.1f}" if s.get("composite_score") is not None else "N/A",
            s["rationale"],
        ])
    headers = ["Action", "Ticker", "Current Qty", "Suggested",
               "P&L %", "Composite", "Rationale"]
    print(tabulate(rows, headers=headers, tablefmt="github"))


def main() -> None:
    positions = _build_positions()
    analysis = portfolio.analyse_portfolio(positions)
    suggestions = portfolio.generate_trade_suggestions(analysis)
    analysis["suggestions"] = suggestions

    print(f"Portfolio Health Score: {analysis['health_score']}")
    print(f"  Components: {analysis['health_components']}")
    print()
    print(
        f"Total value: {analysis['total_value']:,.0f}  "
        f"Cost: {analysis['total_cost']:,.0f}  "
        f"P&L: {analysis['total_pnl']:+,.0f} "
        f"({analysis['total_pnl_pct']:+.1f}%)  "
        f"Winners {analysis['n_winners']}/{analysis['n_positions']}"
    )
    print()
    print("Holdings")
    print("--------")
    _print_holdings(analysis)
    print()
    print("Trade Suggestions")
    print("-----------------")
    _print_suggestions(suggestions)
    print()

    import os

    # 1. Public daily_report.html — NO Portfolio Dashboard
    report.generate_report(
        db_path=DB_PATH,
        output_path=PUBLIC_REPORT_PATH,
        include_portfolio=False,
    )
    # 2. Private portfolio_report.html — INCLUDES Portfolio Dashboard
    report.generate_report(
        db_path=DB_PATH,
        output_path=PRIVATE_REPORT_PATH,
        include_portfolio=True,
        portfolio_data=analysis,
    )

    # Match the real section, not the CSS comment "/* Portfolio Dashboard */"
    public_size = os.path.getsize(PUBLIC_REPORT_PATH)
    private_size = os.path.getsize(PRIVATE_REPORT_PATH)
    marker = '<section id="portfolio">'
    with open(PUBLIC_REPORT_PATH) as f:
        public_has_portfolio = marker in f.read()
    with open(PRIVATE_REPORT_PATH) as f:
        private_has_portfolio = marker in f.read()

    print()
    print(f"{PUBLIC_REPORT_PATH:<36}  {public_size:>10,} bytes  "
          f"portfolio={'YES (BUG!)' if public_has_portfolio else 'no'}")
    print(f"{PRIVATE_REPORT_PATH:<36}  {private_size:>10,} bytes  "
          f"portfolio={'yes' if private_has_portfolio else 'NO (BUG!)'}")

    assert not public_has_portfolio, "daily_report.html should NOT contain Portfolio Dashboard"
    assert private_has_portfolio, "portfolio_report.html SHOULD contain Portfolio Dashboard"

    print()
    print("Two-report split working correctly")


if __name__ == "__main__":
    main()
