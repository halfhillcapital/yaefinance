from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from jobs.fetch_stock import sync_single_stock
from models import DividendRecord, EarningsDate, SplitRecord, StockCalendar
from storage import add_to_watchlist, read_json, stock_path

router = APIRouter(prefix="/stocks")


def _load_stock(ticker: str) -> dict:
    """Load stock data from JSON, auto-fetching if not yet cached."""
    path = stock_path(ticker)
    data = read_json(path)
    if data is None:
        add_to_watchlist(ticker)
        sync_single_stock(ticker)
        data = read_json(path)
    if not isinstance(data, dict):
        raise HTTPException(status_code=502, detail=f"Failed to fetch data for {ticker}")
    return data


@router.get("/{ticker}/calendar", response_model=StockCalendar)
def get_stock_calendar(ticker: str):
    data = _load_stock(ticker)
    cal = data.get("calendar")
    if not cal:
        raise HTTPException(status_code=404, detail=f"No calendar data for {ticker}")
    return StockCalendar(**cal)


@router.get("/{ticker}/earnings", response_model=list[EarningsDate])
def get_stock_earnings(
    ticker: str,
    limit: int = Query(12, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    data = _load_stock(ticker)
    earnings = data.get("earnings", [])
    return [EarningsDate(**e) for e in earnings[offset : offset + limit]]


@router.get("/{ticker}/dividends", response_model=list[DividendRecord])
def get_stock_dividends(ticker: str):
    data = _load_stock(ticker)
    return [DividendRecord(**d) for d in data.get("dividends", [])]


@router.get("/{ticker}/splits", response_model=list[SplitRecord])
def get_stock_splits(ticker: str):
    data = _load_stock(ticker)
    return [SplitRecord(**s) for s in data.get("splits", [])]
