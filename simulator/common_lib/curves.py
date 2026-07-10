from datetime import date, timedelta

import numpy as np
import pandas as pd
from scipy.interpolate import PchipInterpolator

_AGE_BUCKETS = [1, 3, 7, 14, 30, 60, 90, 180, 365, 1000, 1800]


def build_curve(anchor_points: dict, n_days: int = 1800) -> np.ndarray:
    """
    Interpolate a full D1-D{n_days} curve from sparse {dx: value} anchor points.

    Uses PCHIP (monotone cubic) interpolation so the curve never overshoots between
    anchors. Values are clipped to [0, 1].

    anchor_points: e.g. {1: 0.40, 7: 0.20, 30: 0.11, 180: 0.05, 365: 0.03}
    Returns: np.ndarray of shape (n_days,) where index 0 = D1, index n-1 = D{n_days}
    """
    if not anchor_points:
        raise ValueError("anchor_points must not be empty")

    days = np.array(sorted(anchor_points.keys()), dtype=float)
    values = np.array([anchor_points[d] for d in days], dtype=float)

    interp = PchipInterpolator(days, values, extrapolate=True)
    all_days = np.arange(1, n_days + 1, dtype=float)
    curve = interp(all_days)
    return np.clip(curve, 0.0, 1.0)


def average_actuals_anchors(
    df: pd.DataFrame,
    metric: str,
    start_date: date,
    end_date: date,
) -> dict:
    """
    Average pre-computed daily rates across cohorts in [start_date, end_date].

    df      : live_retention or live_conversion DataFrame from BigQuery.
              retention  columns: install_dt, dx, platform, retention_rate
              conversion columns: install_dt, dx_check_dx, platform, conversion_rate
    metric  : 'retention' or 'conversion'
    Returns : {'ios': {1: 0.42, 3: 0.31, ...}, 'android': {...}}
    """
    df = df.copy()
    df['install_dt'] = pd.to_datetime(df['install_dt']).dt.date

    filtered = df[(df['install_dt'] >= start_date) & (df['install_dt'] <= end_date)]
    if filtered.empty:
        return {'ios': {}, 'android': {}}

    if metric == 'retention':
        dx_col, rate_col = 'dx', 'retention_rate'
    else:
        dx_col, rate_col = 'dx_check_dx', 'conversion_rate'

    grouped = (
        filtered
        .groupby(['platform', dx_col])[rate_col]
        .mean()
        .reset_index()
    )

    result = {}
    for platform in ('ios', 'android'):
        sub = grouped[grouped['platform'] == platform].dropna(subset=[rate_col])
        result[platform] = {int(row[dx_col]): round(float(row[rate_col]), 4) for _, row in sub.iterrows()}

    return result


def average_arpdau_from_actuals(
    actuals: pd.DataFrame,
    start_date: date,
    end_date: date,
) -> dict:
    """
    Compute DAU-weighted average ARPDAU per platform over [start_date, end_date].

    Returns a single average value per platform/metric (not per month), so the
    caller can apply it uniformly across all forecast months.

    actuals columns: dt, platform, dau, iap_revenue, ad_revenue
    Returns: {
        'ios':     {'iap': 0.42, 'ad': 0.08},
        'android': {'iap': 0.30, 'ad': 0.06},
    }
    """
    df = actuals.copy()
    df['dt'] = pd.to_datetime(df['dt']).dt.date
    filtered = df[(df['dt'] >= start_date) & (df['dt'] <= end_date)]
    if filtered.empty:
        return {'ios': {'iap': None, 'ad': None}, 'android': {'iap': None, 'ad': None}}

    grouped = (
        filtered
        .groupby('platform')[['dau', 'iap_revenue', 'ad_revenue']]
        .sum()
        .reset_index()
    )
    grouped['iap_arpdau'] = (grouped['iap_revenue'] / grouped['dau'].replace(0, np.nan)).round(4)
    grouped['ad_arpdau']  = (grouped['ad_revenue']  / grouped['dau'].replace(0, np.nan)).round(4)

    result = {'ios': {'iap': None, 'ad': None}, 'android': {'iap': None, 'ad': None}}
    for _, row in grouped.iterrows():
        p = row['platform']
        if p in result:
            result[p]['iap'] = float(row['iap_arpdau'])
            result[p]['ad']  = float(row['ad_arpdau'])
    return result


def monthly_arpdau_from_actuals(
    actuals: pd.DataFrame,
    start_date: date,
    end_date: date,
) -> dict:
    """
    Compute DAU-weighted ARPDAU per platform per calendar month over [start_date, end_date].

    Only months that have actual data are included in the result dicts.

    Returns: {
        'ios':     {'iap': {'2026-01': 0.42, ...}, 'ad': {'2026-01': 0.08, ...}},
        'android': {'iap': {...}, 'ad': {...}},
    }
    """
    df = actuals.copy()
    df['dt'] = pd.to_datetime(df['dt'])
    filtered = df[(df['dt'].dt.date >= start_date) & (df['dt'].dt.date <= end_date)]
    if filtered.empty:
        return {'ios': {'iap': {}, 'ad': {}}, 'android': {'iap': {}, 'ad': {}}}

    filtered = filtered.copy()
    filtered['month'] = filtered['dt'].dt.strftime('%Y-%m')

    grouped = (
        filtered
        .groupby(['platform', 'month'])[['dau', 'iap_revenue', 'ad_revenue']]
        .sum()
        .reset_index()
    )
    grouped['iap_arpdau'] = (grouped['iap_revenue'] / grouped['dau'].replace(0, np.nan)).round(4)
    grouped['ad_arpdau']  = (grouped['ad_revenue']  / grouped['dau'].replace(0, np.nan)).round(4)

    result = {'ios': {'iap': {}, 'ad': {}}, 'android': {'iap': {}, 'ad': {}}, 'iap_net_factor': {}}
    for _, row in grouped.iterrows():
        p = row['platform']
        m = row['month']
        if p in result:
            if pd.notna(row['iap_arpdau']): result[p]['iap'][m] = float(row['iap_arpdau'])
            if pd.notna(row['ad_arpdau']):  result[p]['ad'][m]  = float(row['ad_arpdau'])

    if 'iap_net_revenue' in filtered.columns and 'iap_revenue' in filtered.columns:
        totals = (
            filtered.groupby('month')[['iap_revenue', 'iap_net_revenue']]
            .sum()
            .reset_index()
        )
        for _, row in totals.iterrows():
            gross = row['iap_revenue']
            if pd.notna(gross) and gross > 0 and pd.notna(row['iap_net_revenue']):
                result['iap_net_factor'][row['month']] = round(float(row['iap_net_revenue']) / float(gross), 4)

    return result


def anchors_from_df(df, platform: str, month: str = None) -> dict:
    """
    Extract {dx: value} dict from a DataFrame with columns ['month', 'dx', 'ios', 'android'].
    If month is None, uses the most recent month present in the data.
    Skips rows where the platform value is blank or NaN.
    """
    if month is None:
        month = df["month"].max()
    sub = df[df["month"] == month]
    mask = sub[platform].replace("", np.nan).notna()
    sub = sub[mask]
    return dict(zip(sub["dx"].astype(int), sub[platform].astype(float)))


def derive_age_distribution(
    installs_df: pd.DataFrame,
    platform: str,
    retention_curve: np.ndarray,
    anchor_date: date,
) -> dict:
    """
    Derive the age distribution of the existing user base at anchor_date.

    For each historical install cohort, estimates how many users survive to anchor_date
    using the retention curve, then normalises into fractional buckets at _AGE_BUCKETS.

    installs_df : columns dt (date-like), platform (str), new_installs (int)
    retention_curve : 1800-day array where index i = D(i+1) survival rate
    anchor_date : the day the anchor_dau was observed (typically forecast_start - 1)

    Returns {bucket_dx: fraction} bucketed at _AGE_BUCKETS, fractions sum to 1.0.
    Returns {} if no usable data.
    """
    df = installs_df.copy()
    df['dt'] = pd.to_datetime(df['dt']).dt.date
    df = df[df['platform'] == platform].copy()
    df['age'] = df['dt'].apply(lambda d: (anchor_date - d).days)
    df = df[(df['age'] >= 1)]

    n = len(retention_curve)
    df['surviving'] = df.apply(
        lambda r: float(r['new_installs']) * retention_curve[min(int(r['age']) - 1, n - 1)],
        axis=1,
    )
    total = df['surviving'].sum()
    if total <= 0:
        return {}

    # Bucket each age into the nearest _AGE_BUCKETS representative
    def _bucket(age: int) -> int:
        return min(_AGE_BUCKETS, key=lambda b: abs(b - age))

    df['bucket'] = df['age'].apply(_bucket)
    bucketed = df.groupby('bucket')['surviving'].sum() / total
    return {int(b): round(float(f), 6) for b, f in bucketed.items() if f > 1e-6}
