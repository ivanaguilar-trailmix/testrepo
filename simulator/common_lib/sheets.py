import pandas as pd
from pathlib import Path

INPUTS_DIR = Path(__file__).parent.parent / "config" / "inputs"


def get_inputs_dir() -> str:
    return str(INPUTS_DIR)


def aggregate_marketing(marketing_df: pd.DataFrame) -> dict:
    """
    Aggregate daily per-platform marketing data to monthly CPI and UA spend.

    Returns dict with keys:
      'cpi'      — DataFrame(month, platform, cpi)   — avg daily CPI per month
      'ua_spend' — DataFrame(month, total_budget, ios_pct) — summed spend + iOS split
    """
    df = marketing_df.copy()
    df['dt'] = pd.to_datetime(df['dt'])
    df['month'] = df['dt'].dt.strftime('%Y-%m')

    platform_map = {'iOS': 'ios', 'Android': 'android'}
    df['platform'] = df['display_platform_name'].map(platform_map)
    df = df[df['platform'].notna()]

    cpi_df = (
        df[df['usd_cost'] > 0]
        .groupby(['month', 'platform'])['cpi']
        .mean()
        .reset_index()
    )

    spend_platform = (
        df.groupby(['month', 'platform'])['usd_cost']
        .sum()
        .unstack(fill_value=0.0)
        .reset_index()
    )
    spend_platform.columns.name = None
    ios_col = spend_platform['ios']     if 'ios'     in spend_platform.columns else pd.Series(0.0, index=spend_platform.index)
    and_col = spend_platform['android'] if 'android' in spend_platform.columns else pd.Series(0.0, index=spend_platform.index)
    total = ios_col + and_col
    spend_platform['total_budget'] = total
    spend_platform['ios_pct'] = (ios_col / total * 100).where(total > 0, 50.0)
    ua_df = spend_platform[['month', 'total_budget', 'ios_pct']]

    return {'cpi': cpi_df, 'ua_spend': ua_df}
