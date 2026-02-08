from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Query

from models import EarningsCalendarItem
from storage import calendar_path, read_json

router = APIRouter(prefix="/calendar")


@router.get("/earnings", response_model=dict[str, list[EarningsCalendarItem]])
def get_earnings_calendar(
    start: str | None = Query(None, description="Start date YYYY-MM-DD"),
    end: str | None = Query(None, description="End date YYYY-MM-DD"),
):
    data = read_json(calendar_path("earnings")) or {}
    if isinstance(data, list):
        data = {}  # guard against old flat-list format
    if not start and not end:
        return {
            day: [EarningsCalendarItem(**i) for i in items]
            for day, items in data.items()
        }
    filtered: dict[str, list[EarningsCalendarItem]] = {}
    for day, items in data.items():
        try:
            dt = datetime.strptime(day, "%A, %m/%d/%Y")
        except ValueError:
            continue
        if start and dt < datetime.fromisoformat(start):
            continue
        if end and dt > datetime.fromisoformat(end):
            continue
        filtered[day] = [EarningsCalendarItem(**i) for i in items]
    return filtered


