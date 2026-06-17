# Alpaca Score Buyer

Simple Railway-friendly buyer service.

It reads the Screener sheet directly:

| Column | Meaning |
|---|---|
| A | symbol |
| G | score |

When a row has `G == BUY_SCORE` (default `100`), it attempts to buy the symbol in column A using `ORDER_FRACTION` (default `0.02`, or 2%) of available buying power.

## What it intentionally does

- Reads the screener directly from columns A:G.
- Buys only symbols where score equals 100 by default.
- Re-fetches Alpaca buying power before each symbol.
- Uses margin-aware buying-power fields by default:
  - `daytrading_buying_power`
  - `regt_buying_power`
  - `buying_power`
  - `cash`
  - `non_marginable_buying_power`
- Skips symbols already held in Alpaca.
- Skips symbols with open buy orders.
- Skips order submission when notional is below `$1`.
- Uses bid-to-market limit-order chasing:
  - bid
  - 40% toward ask
  - 70% toward ask
  - ask/current market reference
- Runs forever as a FastAPI service with a background loop.

## What it intentionally does not do

- No market-open gate.
- No Dashboard tab strength cell.
- No W3:AB22 wrapped-symbol grid.
- No Google Sheet writes.
- No score formula calculation in Python.
- No selling.

## Railway variables

Required:

```text
GOOGLE_SERVICE_ACCOUNT_JSON=...
GOOGLE_SHEET_ID=...
GOOGLE_WORKSHEET_NAME=Screener
ALPACA_API_KEY=...
ALPACA_SECRET_KEY=...
ALPACA_PAPER=true
```

Recommended defaults:

```text
SCREENER_RANGE=A:G
START_ROW=2
SYMBOL_COL_INDEX=1
SCORE_COL_INDEX=7
BUY_SCORE=100
ORDER_FRACTION=0.02
MIN_NOTIONAL=1
BUYING_POWER_FIELDS=daytrading_buying_power,regt_buying_power,buying_power,cash,non_marginable_buying_power
BID_TO_MARKET_STEPS=0.0,0.4,0.7,1.0
STEP_TIMEOUT_SECONDS=5
TOTAL_CHASE_TIMEOUT_SECONDS=30
ORDER_POLL_INTERVAL_SECONDS=2
TREAT_PARTIAL_FILL_AS_SUCCESS=true
EXTENDED_HOURS=false
CYCLE_SLEEP_SECONDS=10
REQUEST_TIMEOUT_SECONDS=10
REQUEST_RETRIES=3
REQUEST_SLEEP_SECONDS=0.25
RATE_LIMIT_SLEEP_SECONDS=10
ERROR_SLEEP_SECONDS=15
LOG_LEVEL=INFO
```

Set `MAX_ORDERS_PER_CYCLE` to a positive number if you want a hard cap per cycle. The default `0` means no explicit cap.

## Deploy

```bash
git init
git add .
git commit -m "Initial Alpaca score buyer"
git branch -M main
git remote add origin <your-new-github-repo-url>
git push -u origin main
```

Then deploy the repo on Railway. The Dockerfile starts:

```bash
uvicorn main:app --host 0.0.0.0 --port $PORT
```

## Health check

Open `/healthz` on the Railway URL to see the last cycle summary.
