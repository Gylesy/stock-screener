# Stock Screener

Daily fundamentals + technicals screener for the S&P 500, NASDAQ 100, Russell 2000 and FTSE 250. Pulls data from Yahoo Finance via `yfinance`, persists each run to a local SQLite database, and renders a styled HTML report.

## Layout

```
screener.py       # Main pipeline (run this)
tickers.py        # Universe management — Wikipedia scrapers + russell2000.csv loader
report.py         # HTML report generation (Jinja2)
screener_poc.py   # Original FTSE-250 POC (kept for reference)
screener.db       # SQLite store (created on first run)
failed_tickers.log
reports/daily_report.html
russell2000.csv   # Optional, see "Russell 2000" below
```

## Setup

```bash
python3 -m pip install -r requirements.txt
```

## Run

Full universe (slow — S&P 500 + NASDAQ 100 + FTSE 250 is ~700 unique tickers and will take a couple of hours with throttling):

```bash
python screener.py
```

Smoke test (10 mixed US/UK tickers, finishes in ~1 min):

```bash
python screener.py --smoke
```

Flags:

| Flag           | Default                       | Notes                                            |
|----------------|-------------------------------|--------------------------------------------------|
| `--smoke`      | off                           | Use the fixed 10-ticker test universe           |
| `--db`         | `screener.db`                 | SQLite path                                     |
| `--report`     | `reports/daily_report.html`   | Output HTML                                     |
| `--no-report`  | off                           | Skip HTML generation                            |

## Russell 2000

Wikipedia doesn't publish a constituent table. Drop a CSV named `russell2000.csv` in the project root (first column = symbol, header optional) and `tickers.load_russell2000()` will pick it up.

## Database schema

`daily_metrics` — one row per (ticker, run). New runs append; history is kept.

`ticker_universe` — `ticker, name, sector, indices`. Indices is a comma-separated list (`SP500,NASDAQ100`).

## HTML report

Generated at `reports/daily_report.html`. Open it in a browser.

- Two sections: **US Markets** (S&P 500 + NASDAQ 100, deduped) and **UK Markets** (FTSE 250).
- Columns are click-to-sort.
- RSI cells: green background when 40–70, red otherwise.
- Row colour:
  - **Green**: MACD rising on both weekly and monthly timeframes.
  - **Amber**: MACD rising on monthly only.
  - **Red**: MACD falling on monthly.
- "Export CSV" button on each section downloads that table.

## Metric definitions

### Identity
| Field | Source |
| --- | --- |
| `ticker`, `company_name`, `sector`, `industry` | `yfinance.Ticker.info` |

### Valuation
| Field | Source |
| --- | --- |
| `pe_ratio` | `info.trailingPE` |
| `forward_pe` | `info.forwardPE` |
| `peg_ratio` | `info.pegRatio` (falls back to `trailingPegRatio`) |

### Growth
| Field | Formula |
| --- | --- |
| `revenue_growth_pct` | `revenueGrowth × 100` |
| `eps_growth_yoy` | `earningsGrowth × 100` |
| `eps_growth_qoq` | `(EPS_q0 − EPS_q1) / abs(EPS_q1) × 100` from quarterly income statement |

### Profitability
| Field | Formula |
| --- | --- |
| `roa` | `returnOnAssets × 100` |
| `roe` | `returnOnEquity × 100` |
| `profit_margin` | `profitMargins × 100` |
| `croci_approx` | `operatingCashflow / (totalAssets − currentLiabilities) × 100` |

### Analyst
| Field | Formula |
| --- | --- |
| `analyst_rating` | `recommendationKey` mapped to a human label |
| `target_price` | `targetMeanPrice` |
| `upside_pct` | `(target_price − latest_close) / latest_close × 100` |

### Price & technicals (from 400 days of daily closes)
| Field | Formula |
| --- | --- |
| `latest_close` | Most recent daily close |
| `sma_50`, `sma_200` | Simple moving averages |
| `rsi_14` | Standard 14-day Wilder RSI |
| `macd_daily_rising` | MACD line > signal line on daily |
| `macd_weekly_rising` | Same, computed on weekly-resampled closes |
| `macd_monthly_rising` | Same, computed on 3-year monthly history |
| `golden_cross_date` | Most recent date SMA-50 crossed above SMA-200 |
| `ret_1w / 1m / 3m / 6m / 1y` | Returns over 5 / 21 / 63 / 126 / 252 trading days |
| `ret_ytd` | Return from first close of current calendar year |

### Risk
`sortino_ratio` — `mean(daily_return) / stdev(negative daily_returns) × sqrt(252)`, target return 0%.

### Earnings
`next_earnings_date` — first future date from `Ticker.calendar` or `Ticker.earnings_dates`.

## Throttling

The script sleeps 1s between tickers and 5s between batches of 50. Per-ticker retry is single-attempt; failed tickers go to `failed_tickers.log`.

## Automated Scheduling

The repo ships two GitHub Actions workflows:

- `.github/workflows/daily_screener.yml` — runs the full pipeline every weekday at **21:30 UTC** (after NYSE close at 21:00 UTC and well after the LSE close at 16:30 BST), emails the HTML report via Resend, commits the new `reports/daily_report.html` and `screener.db` back to the repo, and uploads the report as a 30-day workflow artifact. Can also be triggered manually with a `smoke_only` toggle.
- `.github/workflows/smoke_test.yml` — runs `python screener.py --smoke` on every push and PR to `main`, asserts the HTML report exists and is >10KB, and that `daily_metrics` has at least one row.

### Setup

1. Go to your GitHub repo → **Settings → Secrets and variables → Actions**.
2. Add these secrets (all under "Repository secrets"):
   - `RESEND_API_KEY` — sign up free at [resend.com](https://resend.com), then **Dashboard → API Keys → Create API Key**.
   - `REPORT_EMAIL_FROM` — must be an address on a **verified domain** in Resend (see [Resend's domain verification guide](https://resend.com/docs/dashboard/domains/introduction)). Without a verified domain, Resend will reject the send.
   - `REPORT_EMAIL_TO` — where you want the daily report delivered.
   - `T212_API_KEY` — *optional*, your Trading 212 personal API key (Trading 212 app → Settings → API). The pipeline will run without it.
3. The pipeline will then run automatically at 21:30 UTC every weekday.
4. To trigger manually: **Actions tab → Daily Stock Screener → Run workflow**.
5. To run a smoke test on-demand: trigger manually with `smoke_only` set to `true`.

`GITHUB_TOKEN` is provided automatically by Actions — no setup needed. The `daily_screener.yml` workflow declares `permissions: contents: write` so the bot can commit the daily artifacts back to the repo. If a run fails, a separate `notify-on-failure` job sends a plain-text "FAILED" email containing the workflow run URL.

### Resend free tier

Resend's free tier allows 3,000 emails/month and 100 emails/day — more than enough for a weekday daily run (~22/month).

### Email size note

The full HTML report is 4–5 MB. Most modern email clients (Gmail web, Apple Mail, Outlook desktop) display it correctly, but some corporate mail servers may clip large messages. If clipping happens, the report is also:

- committed to `reports/daily_report.html` in the repo on every successful run
- uploaded as a workflow artifact (retained for 30 days) — find it under the run page in the Actions tab.

### Daily commits

The workflow commits the regenerated HTML report and SQLite DB back to `main` on every successful run. Over a year that's ~250 commits — if you'd rather avoid pushing the DB back to the repo (it grows over time), drop `screener.db` from the `git add` step in `daily_screener.yml`.
