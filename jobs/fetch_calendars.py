from __future__ import annotations

import logging

import pandas as pd
import yfinance as yf
import curl_cffi as curl

from storage import calendar_path, read_json, write_json
from jobs.parsers.forexfactory import extract_calendar_table, parse_economic_calendar

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
                market_cap=1_000_000_000, filter_most_active=False,
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

        # Group new items by day → company → symbol
        new_by_day: dict[str, dict[str, dict[str, dict]]] = {}
        for item in items:
            key = _day_key(item.get("date"))
            if key is None:
                continue
            company = item.get("company") or item["symbol"]
            new_by_day.setdefault(key, {}).setdefault(company, {})
            new_by_day[key][company][item["symbol"]] = item

        # Merge with existing data (dict[day, dict[company, list[item]]])
        existing = read_json(calendar_path("earnings")) or {}
        if isinstance(existing, list):
            existing = {}  # migrate from old flat-list format

        for day, company_map in new_by_day.items():
            if day not in existing:
                existing[day] = {}
            # Migrate old flat-list format for this day
            if isinstance(existing[day], list):
                migrated: dict[str, list[dict]] = {}
                for e in existing[day]:
                    c = e.get("company") or e["symbol"]
                    migrated.setdefault(c, []).append(e)
                existing[day] = migrated
            for company, sym_map in company_map.items():
                existing_items = existing[day].get(company, [])
                existing_by_sym = {e["symbol"]: e for e in existing_items}
                existing_by_sym.update(sym_map)
                existing[day][company] = list(existing_by_sym.values())

        # Sort keys by date descending (latest first)
        sorted_data = dict(
            sorted(existing.items(), key=lambda kv: datetime.strptime(kv[0], "%A, %m/%d/%Y"), reverse=True)
        )

        write_json(calendar_path("earnings"), sorted_data)
        total = sum(len(e) for day in sorted_data.values() for e in day.values())
        log.info("Synced %d earnings calendar items across %d days", total, len(sorted_data))
    except Exception:
        log.error("Failed to sync earnings calendar", exc_info=True)

def sync_economics_calendar() -> None:
    try:
        log.info("Syncing economic events calendar")
        url = "https://www.forexfactory.com/calendar"
        response = curl.get(url, impersonate="chrome")
        if response.status_code != 200:
            log.warning("Failed to fetch economics calendar: HTTP %d", response.status_code)
            return
        table = extract_calendar_table(response.text)
        events = parse_economic_calendar(table)
        if not events:
            log.warning("No economic events found in calendar data")
            return

        # Group by date
        by_day: dict[str, list[dict]] = {}
        for ev in events:
            day = ev.get("date") or "Unknown"
            by_day.setdefault(day, []).append(ev)

        # Merge with existing data (keyed by event name within each day)
        existing = read_json(calendar_path("economics")) or {}
        if not isinstance(existing, dict):
            existing = {}

        for day, day_events in by_day.items():
            prev = {e["event"]: e for e in existing.get(day, []) if e.get("event")}
            for ev in day_events:
                if ev.get("event"):
                    prev[ev["event"]] = ev
            existing[day] = list(prev.values())

        write_json(calendar_path("economics"), existing)
        total = sum(len(v) for v in existing.values())
        log.info("Synced %d economic events across %d days", total, len(existing))
    except Exception:
        log.error("Failed to sync economic events calendar", exc_info=True)


def sync_all_calendars() -> None:
    sync_earnings_calendar()
    sync_economics_calendar()
