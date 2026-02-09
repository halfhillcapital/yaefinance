from __future__ import annotations

from lxml import html
from lxml.html import tostring


_IMPACT_MAP = {
    "red": "High",
    "ora": "Medium",
    "yel": "Low",
    "gra": "Non-Economic",
}


def _text(el: html.HtmlElement | None) -> str | None:
    """Return stripped text_content of *el*, or None if blank/missing."""
    if el is None:
        return None
    t = el.text_content().strip()
    return t or None


def _impact_label(td: html.HtmlElement) -> str | None:
    """Extract impact level from the icon class on the <span> inside *td*."""
    for span in td.iter("span"):
        for cls in span.classes:
            if cls.startswith("icon--ff-impact-"):
                suffix = cls.rsplit("-", 1)[-1]
                return _IMPACT_MAP.get(suffix)
    return None


def extract_calendar_table(page_html: str) -> str:
    """Extract the raw ``<table class="calendar__table">`` from a full page."""
    doc = html.fromstring(page_html)
    tables = doc.find_class("calendar__table")
    if not tables:
        raise ValueError("No <table class='calendar__table'> found in HTML")
    return str(tostring(tables[0], encoding="unicode"))


def parse_economic_calendar(raw_html: str) -> list[dict]:
    """Parse a ForexFactory economic-calendar HTML table into a flat list.

    Each returned dict contains:
        date, time, currency, impact, event, actual, forecast, previous
    """
    doc = html.fromstring(raw_html)
    rows = doc.xpath('//tr[@data-event-id]')

    events: list[dict] = []
    current_date: str | None = None
    current_time: str | None = None

    for row in rows:
        # --- date (only present on first row of each day) ---
        date_td = row.find_class("calendar__date")
        if date_td:
            current_date = _text(date_td[0])

        # --- time (empty means same as previous row) ---
        time_td = row.find_class("calendar__time")
        if time_td:
            t = _text(time_td[0])
            if t:
                current_time = t

        # --- currency ---
        cur_td = row.find_class("calendar__currency")
        currency = _text(cur_td[0]) if cur_td else None

        # --- impact ---
        imp_td = row.find_class("calendar__impact")
        impact = _impact_label(imp_td[0]) if imp_td else None

        # --- event title ---
        title_spans = row.find_class("calendar__event-title")
        event_name = _text(title_spans[0]) if title_spans else None

        # --- actual / forecast / previous ---
        actual = _text(row.find_class("calendar__actual")[0]) if row.find_class("calendar__actual") else None
        forecast = _text(row.find_class("calendar__forecast")[0]) if row.find_class("calendar__forecast") else None
        previous = _text(row.find_class("calendar__previous")[0]) if row.find_class("calendar__previous") else None

        events.append(
            {
                "date": current_date,
                "time": current_time,
                "currency": currency,
                "impact": impact,
                "event": event_name,
                "actual": actual,
                "forecast": forecast,
                "previous": previous,
            }
        )

    return events
