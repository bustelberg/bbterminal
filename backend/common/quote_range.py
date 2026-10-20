"""Recent daily-close context, never an alternative price-match selection."""
import math


def recent_close_range(records: list[dict], as_of: str) -> dict:
    """Five distinct sessions strictly before the report date; no current-day bars."""
    by_date = {}
    for row in records:
        day = str(row['date'])[:10]
        close = row.get('close')
        if day < as_of and close is not None and math.isfinite(float(close)) and float(close) > 0:
            by_date[day] = float(close)
    days = sorted(by_date)[-5:]
    return {
        'count': len(days), 'start': days[0] if days else None,
        'end': days[-1] if days else None,
        'low': min(by_date[d] for d in days) if days else None,
        'high': max(by_date[d] for d in days) if days else None,
    }
