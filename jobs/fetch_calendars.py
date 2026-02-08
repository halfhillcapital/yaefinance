from __future__ import annotations

import logging

import pandas as pd
import yfinance as yf

from storage import calendar_path, read_json, write_json

log = logging.getLogger(__name__)


def _nan_to_none(val):
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (ValueError, TypeError):
        pass
    return val


def _df_to_records(df: pd.DataFrame) -> list[dict]:
    df = df.reset_index()
    records = df.to_dict(orient="records")
    return [
        {k: _nan_to_none(v) for k, v in row.items()}
        for row in records
    ]


def _day_key(dt_str: str | None) -> str | None:
    """Format a date string as 'DayName, MM/DD/YYYY'."""
    if dt_str is None:
        return None
    try:
        from datetime import datetime
        dt = datetime.fromisoformat(str(dt_str))
        return dt.strftime("%A, %m/%d/%Y")
    except (ValueError, TypeError):
        return None


def sync_earnings_calendar() -> None:
    from datetime import datetime

    log.info("Syncing market earnings calendar")
    try:
        cal = yf.Calendars()

        # Paginate to collect all results (yfinance caps at 100 per call)
        items: list[dict] = []
        offset = 0
        while True:
            df = cal.get_earnings_calendar(
                limit=100, offset=offset,
                market_cap=500_000_000, filter_most_active=False,
            )
            if df is None or df.empty:
                break
            records = _df_to_records(df)
            for r in records:
                items.append({
                    "symbol": r.get("Symbol") or r.get("index", ""),
                    "company": r.get("Company"),
                    "marketcap": r.get("Marketcap"),
                    "event_name": r.get("Event Name"),
                    "date": r.get("Event Start Date"),
                    "timing": r.get("Timing"),
                    "eps_estimate": r.get("EPS Estimate"),
                    "reported_eps": r.get("Reported EPS"),
                    "surprise_pct": r.get("Surprise(%)"),
                })
            if len(records) < 100:
                break
            offset += 100

        if not items:
            log.warning("No earnings calendar data returned")
            return

        # Group new items by day
        new_by_day: dict[str, dict[str, dict]] = {}
        for item in items:
            key = _day_key(item.get("date"))
            if key is None:
                continue
            new_by_day.setdefault(key, {})
            new_by_day[key][item["symbol"]] = item

        # Merge with existing data
        existing = read_json(calendar_path("earnings")) or {}
        if isinstance(existing, list):
            existing = {}  # migrate from old flat-list format

        for day, sym_map in new_by_day.items():
            if day not in existing:
                existing[day] = []
            existing_by_sym = {e["symbol"]: e for e in existing[day]}
            existing_by_sym.update(sym_map)
            existing[day] = list(existing_by_sym.values())

        # Sort keys by date descending (latest first)
        sorted_data = dict(
            sorted(existing.items(), key=lambda kv: datetime.strptime(kv[0], "%A, %m/%d/%Y"), reverse=True)
        )

        write_json(calendar_path("earnings"), sorted_data)
        log.info("Synced %d earnings calendar items across %d days", sum(len(v) for v in sorted_data.values()), len(sorted_data))
    except Exception:
        log.error("Failed to sync earnings calendar", exc_info=True)


def sync_all_calendars() -> None:
    sync_earnings_calendar()
