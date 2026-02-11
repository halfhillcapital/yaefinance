from __future__ import annotations

import logging
from datetime import datetime, timedelta

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


def _merge_record(old: dict, new: dict) -> dict:
    """Merge new into old, keeping existing values when new value is None."""
    return {k: v if v is not None else old.get(k) for k, v in new.items()}


def _df_to_records(df: pd.DataFrame) -> list[dict]:
    df = df.reset_index()
    records = df.to_dict(orient="records")
    return [
        {k: _nan_to_none(v) for k, v in row.items()}
        for row in records
    ]


def _day_key(dt) -> str | None:
    """Format a datetime-like value as 'DayName, MM/DD/YYYY'."""
    if dt is None:
        return None
    try:
        return dt.strftime("%A, %m/%d/%Y")
    except (ValueError, TypeError, AttributeError):
        return None


def sync_earnings_calendar() -> None:
    log.info("Syncing market earnings calendar")
    try:
        yesterday = datetime.now() - timedelta(days=1)
        cal = yf.Calendars(start=yesterday)

        # Paginate and group by day → company → symbol
        new_by_day: dict[str, dict[str, dict[str, dict]]] = {}
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
                sym = r.get("Symbol") or r.get("index", "")
                company = r.get("Company") or sym
                item = {
                    "symbol": sym,
                    "marketcap": r.get("Marketcap"),
                    "event_name": r.get("Event Name"),
                    "date": r.get("Event Start Date"),
                    "timing": r.get("Timing"),
                    "eps_estimate": r.get("EPS Estimate"),
                    "reported_eps": r.get("Reported EPS"),
                    "surprise_pct": r.get("Surprise(%)"),
                }
                key = _day_key(item["date"])
                if key is None:
                    continue
                new_by_day.setdefault(key, {}).setdefault(company, {})
                if sym in new_by_day[key][company]:
                    new_by_day[key][company][sym] = _merge_record(new_by_day[key][company][sym], item)
                else:
                    new_by_day[key][company][sym] = item
            if len(records) < 100:
                break
            offset += 100

        if not new_by_day:
            log.warning("No earnings calendar data returned")
            return

        # Merge with existing data (dict[day, dict[company, list[item]]])
        existing: dict[str, dict[str, list[dict]]] = read_json(calendar_path("earnings")) or {}  # type: ignore[assignment]

        for day, company_map in new_by_day.items():
            if day not in existing:
                existing[day] = {}
            for company, sym_map in company_map.items():
                existing_items = existing[day].get(company, [])
                existing_by_sym = {e["symbol"]: e for e in existing_items}
                for sym, new_item in sym_map.items():
                    if sym in existing_by_sym:
                        existing_by_sym[sym] = _merge_record(existing_by_sym[sym], new_item)
                    else:
                        existing_by_sym[sym] = new_item
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
                    if ev["event"] in prev:
                        prev[ev["event"]] = _merge_record(prev[ev["event"]], ev)
                    else:
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
