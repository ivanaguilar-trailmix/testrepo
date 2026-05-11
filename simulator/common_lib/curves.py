from datetime import date

import numpy as np
import pandas as pd
from scipy.interpolate import PchipInterpolator


def build_curve(anchor_points: dict, n_days: int = 365) -> np.ndarray:
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
