# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**yaefinance** — FastAPI service that provides real-time finance/trading data via yfinance. Caches results as JSON files on disk and refreshes on a 6-hour schedule via APScheduler.

## Commands

```bash
uv sync                 # install dependencies
uv run python main.py   # start dev server (0.0.0.0:8000, auto-reload)
```

No test suite exists yet.

## Architecture

```
main.py        → FastAPI app entry point, registers routers, attaches scheduler lifespan
models.py      → Pydantic response models (stock calendars, earnings, dividends, splits, market-wide calendars)
storage.py     → JSON file I/O layer; watchlist + per-stock + calendar persistence under data/
routes/
  stocks.py    → /stocks/{ticker}/… endpoints (calendar, earnings, dividends, splits) — auto-fetches on cache miss
  calendars.py → /calendar/earnings — market-wide earnings calendar with date filtering
  admin.py     → /admin/… — watchlist CRUD, manual sync trigger
jobs/
  scheduler.py → APScheduler lifespan: 6h interval syncs + initial sync on startup
  fetch_stock.py    → yfinance Ticker data extraction per symbol → writes to data/stocks/{TICKER}.json
  fetch_calendars.py → yfinance Calendars paginated fetch → merges into data/earnings.json (grouped by day)
```

**Data flow:** API request → `storage.read_json` → cache hit returns data; cache miss → `sync_single_stock` fetches from yfinance, writes JSON, then returns. Background scheduler periodically refreshes all watchlist tickers and market calendars.

**Storage:** All state lives in `data/` (gitignored). Watchlist at `data/watchlist.json`, per-stock at `data/stocks/{TICKER}.json`, calendars at `data/{name}.json`. Writes use atomic temp-file + `os.replace`.

## Key Conventions

- Python 3.13, managed with `uv`
- All modules use `from __future__ import annotations`
- Pydantic v2 models for request/response schemas
- `_nan_to_none` helper used in both fetch jobs to sanitize pandas NaN values
