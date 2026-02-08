from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

from jobs.fetch_calendars import sync_all_calendars
from jobs.fetch_stock import sync_all_stocks, sync_single_stock
from storage import add_to_watchlist, read_watchlist, remove_from_watchlist, stock_path

router = APIRouter(prefix="/admin")


class AddTickersRequest(BaseModel):
    tickers: list[str]


@router.get("/watchlist")
def get_watchlist() -> list[str]:
    return read_watchlist()


@router.post("/watchlist")
def add_tickers(req: AddTickersRequest) -> list[str]:
    for ticker in req.tickers:
        add_to_watchlist(ticker)
        sync_single_stock(ticker.upper())
    return read_watchlist()


@router.delete("/watchlist/{ticker}")
def remove_ticker(ticker: str) -> list[str]:
    remove_from_watchlist(ticker)
    path = stock_path(ticker)
    if path.exists():
        path.unlink()
    return read_watchlist()


@router.post("/sync")
def trigger_sync() -> dict:
    sync_all_stocks()
    sync_all_calendars()
    return {"status": "ok"}
