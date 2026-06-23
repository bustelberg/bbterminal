"""Per-exchange regular trading hours + conversion to Amsterdam wall-clock.

Backs the `/timezone` page: our universe spans ~50 exchanges across every
timezone, and we trade at the *previous day's close*. To know when a given
market's close is final (and therefore when we can act on it in our own
Amsterdam day), we need each exchange's local closing time expressed in
Amsterdam time — and because both the exchange and Amsterdam may observe
daylight-saving on different schedules, the offset differs between summer and
winter. This module owns:

  * `EXCHANGE_HOURS` — a hand-maintained map of DB `exchange_code` → IANA
    timezone + the regular continuous-session local open/close (+ optional
    lunch break, + a non-Mon–Fri trading week note). These are the *regular*
    session times; pre-/post-market and call auctions are deliberately
    excluded. Verify against the exchange's own calendar before relying on a
    value for execution.
  * `exchange_amsterdam_hours(code)` — converts those local times to
    Amsterdam wall-clock for a representative winter date (Amsterdam on CET)
    and summer date (Amsterdam on CEST), reporting any day rollover.

Keyed by the `gurufocus_exchange.exchange_code` values seeded in
`supabase/migrations/20260101000000_initial_schema.sql`.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from zoneinfo import ZoneInfo

_AMS = ZoneInfo("Europe/Amsterdam")
# Representative dates: mid-January = Amsterdam on standard time (CET, UTC+1);
# mid-July = Amsterdam on summer time (CEST, UTC+2). Converting each exchange's
# local session times on these two dates captures the steady-state summer vs
# winter Amsterdam offset, accounting for each zone observing DST separately.
_WINTER_REF = date(2025, 1, 15)
_SUMMER_REF = date(2025, 7, 15)


@dataclass(frozen=True)
class ExchangeSession:
    """Regular trading session for one exchange, in its own local wall-clock."""
    timezone: str            # IANA tz name
    open: str                # "HH:MM" local
    close: str               # "HH:MM" local
    lunch_start: str | None = None   # "HH:MM" local, if the exchange breaks for lunch
    lunch_end: str | None = None
    trading_week: str = "Mon–Fri"    # most are Mon–Fri; Gulf/Israel trade Sun–Thu


# Regular continuous-session hours. Sources: each exchange's published trading
# calendar (as of 2025). Times are LOCAL to the exchange's timezone.
EXCHANGE_HOURS: dict[str, ExchangeSession] = {
    # ── North America (ET / CT) ──
    "NYSE": ExchangeSession("America/New_York", "09:30", "16:00"),
    "NASDAQ": ExchangeSession("America/New_York", "09:30", "16:00"),
    "CBOE": ExchangeSession("America/New_York", "09:30", "16:00"),
    "TSX": ExchangeSession("America/Toronto", "09:30", "16:00"),
    "TSXV": ExchangeSession("America/Toronto", "09:30", "16:00"),
    "MEX": ExchangeSession("America/Mexico_City", "08:30", "15:00"),
    "BMV": ExchangeSession("America/Mexico_City", "08:30", "15:00"),
    # ── Latin America ──
    "BSP": ExchangeSession("America/Sao_Paulo", "10:00", "17:00"),
    "BOG": ExchangeSession("America/Bogota", "09:30", "15:55"),
    "XSGO": ExchangeSession("America/Santiago", "09:30", "16:00"),
    # ── Western / Northern Europe ──
    "LSE": ExchangeSession("Europe/London", "08:00", "16:30"),
    "DUB": ExchangeSession("Europe/Dublin", "08:00", "16:30"),
    "XLIS": ExchangeSession("Europe/Lisbon", "08:00", "16:30"),
    "XTER": ExchangeSession("Europe/Berlin", "09:00", "17:30"),
    "FRA": ExchangeSession("Europe/Berlin", "08:00", "20:00"),
    "XPAR": ExchangeSession("Europe/Paris", "09:00", "17:30"),
    "XAMS": ExchangeSession("Europe/Amsterdam", "09:00", "17:30"),
    "XBRU": ExchangeSession("Europe/Brussels", "09:00", "17:30"),
    "MIL": ExchangeSession("Europe/Rome", "09:00", "17:30"),
    "XMAD": ExchangeSession("Europe/Madrid", "09:00", "17:30"),
    "XSWX": ExchangeSession("Europe/Zurich", "09:00", "17:30"),
    "WBO": ExchangeSession("Europe/Vienna", "09:00", "17:30"),
    "OSTO": ExchangeSession("Europe/Stockholm", "09:00", "17:30"),
    "OCSE": ExchangeSession("Europe/Copenhagen", "09:00", "17:00"),
    "OSL": ExchangeSession("Europe/Oslo", "09:00", "16:30"),
    "OHEL": ExchangeSession("Europe/Helsinki", "10:00", "18:30"),
    "WAR": ExchangeSession("Europe/Warsaw", "09:00", "17:00"),
    "XPRA": ExchangeSession("Europe/Prague", "09:00", "16:20"),
    "BUD": ExchangeSession("Europe/Budapest", "09:00", "17:00"),
    "ATH": ExchangeSession("Europe/Athens", "10:00", "17:20"),
    "IST": ExchangeSession("Europe/Istanbul", "10:00", "18:00"),
    "MIC": ExchangeSession("Europe/Moscow", "10:00", "18:45"),
    # ── Asia-Pacific (many break for lunch) ──
    "TSE": ExchangeSession("Asia/Tokyo", "09:00", "15:30", "11:30", "12:30"),
    "HKSE": ExchangeSession("Asia/Hong_Kong", "09:30", "16:00", "12:00", "13:00"),
    "SHSE": ExchangeSession("Asia/Shanghai", "09:30", "15:00", "11:30", "13:00"),
    "SZSE": ExchangeSession("Asia/Shanghai", "09:30", "15:00", "11:30", "13:00"),
    "TPE": ExchangeSession("Asia/Taipei", "09:00", "13:30"),
    "ROCO": ExchangeSession("Asia/Taipei", "09:00", "13:30"),
    "XKRX": ExchangeSession("Asia/Seoul", "09:00", "15:30"),
    "NSE": ExchangeSession("Asia/Kolkata", "09:15", "15:30"),
    "BSE": ExchangeSession("Asia/Kolkata", "09:15", "15:30"),
    "BOM": ExchangeSession("Asia/Kolkata", "09:15", "15:30"),
    "SGX": ExchangeSession("Asia/Singapore", "09:00", "17:00"),
    "XKLS": ExchangeSession("Asia/Kuala_Lumpur", "09:00", "17:00", "12:30", "14:30"),
    "ISX": ExchangeSession("Asia/Jakarta", "09:00", "15:50", "12:00", "13:30"),
    "BKK": ExchangeSession("Asia/Bangkok", "10:00", "16:30", "12:30", "14:30"),
    "PHS": ExchangeSession("Asia/Manila", "09:30", "15:30", "12:00", "13:30"),
    "ASX": ExchangeSession("Australia/Sydney", "10:00", "16:00"),
    "NZSE": ExchangeSession("Pacific/Auckland", "10:00", "16:45"),
    # ── Middle East / Africa (Gulf + Israel trade Sun–Thu) ──
    "SAU": ExchangeSession("Asia/Riyadh", "10:00", "15:00", trading_week="Sun–Thu"),
    "DSMD": ExchangeSession("Asia/Qatar", "09:30", "13:15", trading_week="Sun–Thu"),
    "KUW": ExchangeSession("Asia/Kuwait", "09:00", "12:30", trading_week="Sun–Thu"),
    "ADX": ExchangeSession("Asia/Dubai", "10:00", "15:00"),
    "DFM": ExchangeSession("Asia/Dubai", "10:00", "15:00"),
    "XTAE": ExchangeSession("Asia/Jerusalem", "10:00", "17:15", trading_week="Sun–Thu"),
    "CAI": ExchangeSession("Africa/Cairo", "10:00", "14:30", trading_week="Sun–Thu"),
    "JSE": ExchangeSession("Africa/Johannesburg", "09:00", "17:00"),
}


def _to_amsterdam(hhmm: str, tz_name: str, ref: date) -> dict:
    """Convert a local "HH:MM" on `ref` (in `tz_name`) to Amsterdam wall-clock.

    Returns `{"time": "HH:MM", "day_offset": int}` where `day_offset` is the
    Amsterdam calendar-date delta vs `ref` (−1 = previous day, +1 = next day) —
    far-Eastern opens, for instance, can fall on the previous Amsterdam day."""
    h, m = (int(x) for x in hhmm.split(":"))
    local = datetime(ref.year, ref.month, ref.day, h, m, tzinfo=ZoneInfo(tz_name))
    ams = local.astimezone(_AMS)
    return {"time": f"{ams.hour:02d}:{ams.minute:02d}", "day_offset": (ams.date() - ref).days}


def _observes_dst(tz_name: str) -> bool:
    """True if the exchange's own timezone shifts its UTC offset across the
    year (i.e. observes daylight-saving)."""
    tz = ZoneInfo(tz_name)
    jan = datetime(_WINTER_REF.year, 1, 15, 12, tzinfo=tz).utcoffset()
    jul = datetime(_SUMMER_REF.year, 7, 15, 12, tzinfo=tz).utcoffset()
    return jan != jul


def exchange_amsterdam_hours(code: str) -> dict | None:
    """Full hours payload for one exchange code, or None if we have no session
    data for it. Includes the local session, both Amsterdam-season variants of
    open/close, and whether the exchange observes its own DST."""
    sess = EXCHANGE_HOURS.get(code)
    if sess is None:
        return None

    def variant(ref: date) -> dict:
        return {
            "open": _to_amsterdam(sess.open, sess.timezone, ref),
            "close": _to_amsterdam(sess.close, sess.timezone, ref),
        }

    return {
        "timezone": sess.timezone,
        "local_open": sess.open,
        "local_close": sess.close,
        "lunch_start": sess.lunch_start,
        "lunch_end": sess.lunch_end,
        "trading_week": sess.trading_week,
        "observes_dst": _observes_dst(sess.timezone),
        # Amsterdam wall-clock for each Amsterdam season (winter = CET, summer = CEST).
        "amsterdam_winter": variant(_WINTER_REF),
        "amsterdam_summer": variant(_SUMMER_REF),
    }


def known_exchange_codes() -> set[str]:
    """Every exchange_code we have session data for."""
    return set(EXCHANGE_HOURS.keys())
