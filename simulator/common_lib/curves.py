import numpy as np
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
