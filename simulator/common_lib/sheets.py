import io

import pandas as pd
from pathlib import Path

INPUTS_DIR = Path(__file__).parent.parent / "config" / "inputs"


def _normalize_month(df: pd.DataFrame) -> pd.DataFrame:
    if 'month' in df.columns:
        df = df.copy()
        df['month'] = pd.to_datetime(df['month']).dt.strftime('%Y-%m')
    return df


def _load_csv(name: str) -> pd.DataFrame:
    path = INPUTS_DIR / f"{name}.csv"
    if not path.exists():
        raise FileNotFoundError(
            f"Input file not found: {path}\nRun setup_sheet.py first."
        )
    return _normalize_month(pd.read_csv(path))


def load_inputs() -> dict[str, pd.DataFrame]:
    """
    Read CPI, UA spend, and team cost inputs from local CSV files in config/inputs/.

    Returns dict with keys: cpi, ua_spend, team_cost.
    Retention, conversion, and ARPDAU are now loaded from actuals via the panel.
    """
    return {
        "cpi":       _load_csv("cpi"),
        "ua_spend":  _load_csv("ua_spend"),
        "team_cost": _load_csv("team_cost"),
    }


def get_inputs_dir() -> str:
    return str(INPUTS_DIR)


def load_age_distribution_csv(content: bytes) -> dict:
    """
    Parse an age-distribution CSV with columns dx (int) and pct (float, as %).
    Returns {dx: fraction} normalised so fractions sum to 1.0.

    Example CSV:
        dx,pct
        7,4.0
        30,15.0
        60,18.0
        90,22.0
        180,20.0
        365,11.0
    """
    df = pd.read_csv(io.BytesIO(content))
    df.columns = [c.strip().lower() for c in df.columns]
    df['dx']  = df['dx'].astype(int)
    df['pct'] = df['pct'].astype(float)
    total = df['pct'].sum()
    if total <= 0:
        raise ValueError("pct column sums to zero")
    df['pct'] = df['pct'] / total * 100  # normalise to 100%
    return {int(row['dx']): round(float(row['pct']) / 100, 6) for _, row in df.iterrows()}
