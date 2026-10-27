"""Fixed risk-history boundaries shared by all historical views."""
from calendar import monthrange
from datetime import date, timedelta
from bisect import bisect_right
import math


def history_years(start: str | None, years: int, today: date | None = None) -> int:
    if not start:
        return years
    opening, today = date.fromisoformat(start), today or date.today()
    if opening > today or opening.year < 2000:
        raise ValueError('Risk history start must be between 2000 and today.')
    return max(years, math.ceil((today - opening).days / 365.25) + 1)


def history_marks(start: str, end: str) -> list[str]:
    opening, closing = date.fromisoformat(start), date.fromisoformat(end)
    marks = [(opening - timedelta(days=1)).isoformat()]
    for index in range(opening.year * 12 + opening.month - 1, closing.year * 12 + closing.month):
        year, month = divmod(index, 12)
        day = date(year, month + 1, monthrange(year, month + 1)[1])
        if opening <= day <= closing:
            marks.append(day.isoformat())
    if end > marks[-1]:
        marks.append(end)
    return marks


def covers_history(rows: list, marks: list[str]) -> bool:
    dates = sorted(day for day, price in rows if price is not None and math.isfinite(price) and price > 0)
    for mark in marks:
        index = bisect_right(dates, mark) - 1
        if index < 0 or (date.fromisoformat(mark) - date.fromisoformat(dates[index])).days > 7:
            return False
    return True
