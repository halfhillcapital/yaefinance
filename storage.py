from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

DATA_DIR = Path("data")
WATCHLIST_PATH = DATA_DIR / "watchlist.json"
STOCKS_DIR = DATA_DIR / "stocks"


def _ensure_dirs() -> None:
    STOCKS_DIR.mkdir(parents=True, exist_ok=True)


def read_json(path: Path) -> dict | list | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def write_json(path: Path, data: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, default=str, ensure_ascii=False)
        os.replace(tmp, path)
    except BaseException:
        os.unlink(tmp)
        raise


def read_watchlist() -> list[str]:
    data = read_json(WATCHLIST_PATH)
    if isinstance(data, list):
        return data
    return []


def write_watchlist(tickers: list[str]) -> None:
    _ensure_dirs()
    write_json(WATCHLIST_PATH, sorted(set(tickers)))


def add_to_watchlist(ticker: str) -> None:
    tickers = read_watchlist()
    upper = ticker.upper()
    if upper not in tickers:
        tickers.append(upper)
        write_watchlist(tickers)


def remove_from_watchlist(ticker: str) -> None:
    tickers = read_watchlist()
    upper = ticker.upper()
    tickers = [t for t in tickers if t != upper]
    write_watchlist(tickers)


def stock_path(ticker: str) -> Path:
    return STOCKS_DIR / f"{ticker.upper()}.json"


def calendar_path(name: str) -> Path:
    return DATA_DIR / f"{name}.json"
