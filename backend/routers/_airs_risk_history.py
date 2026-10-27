"""Observed account losses from AIRS performance, never holdings-value or vendor returns."""
from calendar import monthrange
from datetime import date
from math import isfinite

from ._airs_value_series import _page


def calculate_history(rows: list[dict], portefeuille: str, *, as_of: date | None = None) -> dict:
    # AIRS resets cumulative returns each January. Do not splice those levels across years.
    valid = {}
    invalid = 0
    cutoff = as_of or date.today()
    for row in rows:
        try:
            day = date.fromisoformat(str(row['periode'])[:10])
            if day > cutoff:
                continue  # AIRS may carry future-period placeholders; they are not observations.
            value = float(row['cumulatief_rendement'])
            if not isfinite(value) or value < -100:
                raise ValueError('invalid return')
        except (KeyError, TypeError, ValueError):
            invalid += 1
            continue
        if float(row.get('beginvermogen') or 0) or float(row.get('eindvermogen') or 0):
            valid[day] = value
    result = {'portefeuille': portefeuille, 'points': [], 'months': [], 'warnings': [],
              'source': 'AIRS cumulatief_rendement', 'observations': 0,
              'max_drawdown_pct': None, 'current_drawdown_pct': None,
              'worst_month_pct': None, 'negative_months': 0,
              'peak_date': None, 'trough_date': None, 'recovery_date': None,
              'first_date': None, 'last_date': None, 'year': None}
    if not valid:
        result['warnings'].append('No funded AIRS return observations are stored for this account.')
        return result
    year = max(valid).year
    observations = sorted((d, r) for d, r in valid.items() if d.year == year)
    result.update(year=year, observations=len(observations),
                  first_date=observations[0][0].isoformat(), last_date=observations[-1][0].isoformat())
    # A known January origin is a reference, not a measured observation. If January is absent,
    # start at the first actual observation and explicitly disclose the unobserved opening.
    points = ([(date(year, 1, 1), 0.0)]
              if observations[0][0].month == 1 and observations[0][0] > date(year, 1, 1) else [])
    points += observations
    peak = None
    peak_date = None
    worst = 0.0
    worst_peak_index = None
    for day, value in points:
        level = 1 + value / 100
        if peak is None or level >= peak:
            peak, peak_date = level, day.isoformat()
        drawdown = (level / peak - 1) * 100 if peak and peak > 0 else 0.0
        result['points'].append({'date': day.isoformat(), 'cumulative_pct': value,
                                 'drawdown_pct': round(drawdown, 6),
                                 'reference': day not in valid})
        if drawdown < worst:
            worst = drawdown
            result.update(peak_date=peak_date, trough_date=day.isoformat())
            worst_peak_index = peak
    if len(observations) >= 2:
        result['max_drawdown_pct'] = round(worst, 6)
        result['current_drawdown_pct'] = result['points'][-1]['drawdown_pct']
    if result['trough_date']:
        result['recovery_date'] = next((d.isoformat() for d, r in observations
            if d.isoformat() > result['trough_date'] and 1 + r / 100 >= worst_peak_index), None)
    month_ends = {d.month: r for d, r in observations if d.day == monthrange(year, d.month)[1]}
    for month, value in sorted(month_ends.items()):
        previous = 0.0 if month == 1 else month_ends.get(month - 1)
        if previous is None or previous <= -100:
            continue
        result['months'].append({'month': f'{year}-{month:02}',
                                'return_pct': round(((1 + value / 100) / (1 + previous / 100) - 1) * 100, 6)})
    monthly = [r['return_pct'] for r in result['months']]
    if monthly:
        result['worst_month_pct'] = min(monthly)
        result['negative_months'] = sum(r < 0 for r in monthly)
    result['warnings'].append('Observed-date drawdown only: losses between stored observations may be larger. Latest stored calendar year only.')
    if observations[0][0].month != 1:
        result['warnings'].append('January is missing; drawdown starts at the first stored observation, not the start of the year.')
    if any((right[0] - left[0]).days > 35 for left, right in zip(observations, observations[1:])):
        result['warnings'].append('There are gaps longer than 35 days between stored observations; no missing returns are interpolated.')
    if len(observations) < 2:
        result['warnings'].append('At least two observed dates are needed to report drawdown.')
    if invalid:
        result['warnings'].append(f'{invalid} invalid or missing return observation(s) excluded.')
    if observations[-1][0].day != monthrange(year, observations[-1][0].month)[1]:
        result['warnings'].append('The latest observation is a partial month; it is excluded from complete-month statistics.')
    return result


def account_risk_history(portefeuille: str) -> dict:
    return calculate_history(_page('airs_performance',
        'periode,beginvermogen,eindvermogen,cumulatief_rendement',
        eq={'portefeuille': portefeuille}, order='periode', tiebreak=None), portefeuille)
