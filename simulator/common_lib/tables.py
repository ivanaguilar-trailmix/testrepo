"""
Monthly P&L summary table: actuals + forecast combined.
"""
from __future__ import annotations

import calendar
import pandas as pd

from common_lib.simulation import load_result, load_scenario

IAP_NET_FACTOR = 0.70   # fallback for actuals rows (no per-month input available)
AD_NET_FACTOR  = 0.85

_YELLOW = '#FFFDE7'
_GREEN  = '#C8E6C9'
_HEADER = '#1B5E20'


def _monthly_actuals(actuals: pd.DataFrame, before, n_months: int) -> pd.DataFrame:
    df = actuals.copy()
    df['dt'] = pd.to_datetime(df['dt'])

    daily = (
        df.groupby('dt')
        .agg(dau=('dau', 'sum'), iap_revenue=('iap_revenue', 'sum'), ad_revenue=('ad_revenue', 'sum'))
        .reset_index()
    )
    daily['month'] = daily['dt'].dt.to_period('M')

    target  = pd.Period(before, 'M') - 1
    periods = [target - i for i in range(n_months - 1, -1, -1)]
    daily   = daily[daily['month'].isin(periods)]

    monthly = (
        daily.groupby('month')
        .agg(
            avg_dau=('dau', 'mean'),
            total_dau=('dau', 'sum'),
            iap_revenue=('iap_revenue', 'sum'),
            ad_revenue=('ad_revenue', 'sum'),
        )
        .reset_index()
    )
    monthly['date']          = monthly['month'].dt.to_timestamp()
    monthly['revenue_gross'] = monthly['iap_revenue'] + monthly['ad_revenue']
    monthly['revenue_net']   = monthly['iap_revenue'] * IAP_NET_FACTOR + monthly['ad_revenue'] * AD_NET_FACTOR
    monthly['arpdau']        = monthly['revenue_gross'] / monthly['total_dau']
    monthly['is_forecast']   = False
    return monthly


def _monthly_forecast(result: pd.DataFrame) -> pd.DataFrame:
    df = result[result['platform'] == 'combined'].copy()
    df['date']  = pd.to_datetime(df['date'])
    df['month'] = df['date'].dt.to_period('M')

    monthly = (
        df.groupby('month')
        .agg(
            avg_dau=('dau', 'mean'),
            total_dau=('dau', 'sum'),
            iap_revenue=('iap_revenue', 'sum'),
            ad_revenue=('ad_revenue', 'sum'),
        )
        .reset_index()
    )
    monthly['date']          = monthly['month'].dt.to_timestamp()
    monthly['revenue_gross'] = monthly['iap_revenue'] + monthly['ad_revenue']
    monthly['revenue_net']   = monthly['iap_revenue'] * IAP_NET_FACTOR + monthly['ad_revenue'] * AD_NET_FACTOR
    monthly['arpdau']        = monthly['revenue_gross'] / monthly['total_dau']
    monthly['is_forecast']   = True
    return monthly


def monthly_table(
    scenario: str,
    actuals: pd.DataFrame,
    historical_marketing=None,
    n_actuals: int = 6,
    monthly_iap_net_factor: dict = None,
) -> 'pd.io.formats.style.Styler':
    """
    Build a styled monthly P&L table combining actuals and forecast.

    Parameters
    ----------
    scenario : str
        Saved scenario name (result must exist).
    actuals : pd.DataFrame
        Daily actuals with columns: dt, platform, dau, iap_revenue, ad_revenue.
    historical_marketing : dict {month_str: float}, optional
        Actual UA spend for historical months.
    n_actuals : int
        How many historical months to show before the forecast start.
    monthly_iap_net_factor : dict {month_str: float}, optional
        Per-month IAP gross-to-net factor for forecast rows. Falls back to 0.70.
    """
    _, forecast_start, _, ios_inp, and_inp, *_ = load_scenario(scenario)
    result = load_result(scenario)

    act_df = _monthly_actuals(actuals, forecast_start, n_actuals)
    fct_df = _monthly_forecast(result)

    # Blend partial-month actuals into the first forecast row when forecast
    # starts mid-month (e.g. forecast_start = May 11 → add May 1–10 actuals).
    if forecast_start.day > 1:
        _act = actuals.copy()
        _act['dt'] = pd.to_datetime(_act['dt'])
        month_first = forecast_start.replace(day=1)
        partial = _act[
            (_act['dt'].dt.date >= month_first) &
            (_act['dt'].dt.date <  forecast_start)
        ]
        if not partial.empty:
            dp = (
                partial.groupby('dt')
                .agg(dau=('dau', 'sum'), iap=('iap_revenue', 'sum'), ad=('ad_revenue', 'sum'))
                .reset_index()
            )
            fct_month = pd.Period(forecast_start, 'M')
            mask = fct_df['month'] == fct_month
            if mask.any():
                idx = fct_df.index[mask][0]
                fct_df.loc[idx, 'iap_revenue']  += dp['iap'].sum()
                fct_df.loc[idx, 'ad_revenue']   += dp['ad'].sum()
                fct_df.loc[idx, 'total_dau']    += dp['dau'].sum()
                fct_df.loc[idx, 'revenue_gross'] = (fct_df.loc[idx, 'iap_revenue'] +
                                                     fct_df.loc[idx, 'ad_revenue'])
                days = calendar.monthrange(forecast_start.year, forecast_start.month)[1]
                fct_df.loc[idx, 'avg_dau'] = fct_df.loc[idx, 'total_dau'] / days
                fct_df.loc[idx, 'arpdau']  = (fct_df.loc[idx, 'revenue_gross'] /
                                               fct_df.loc[idx, 'total_dau'])

    df = pd.concat([act_df, fct_df], ignore_index=True).sort_values('date').reset_index(drop=True)
    df['month_str'] = df['month'].astype(str)

    # Apply per-month IAP net factor to forecast rows; actuals always use constant.
    def _iap_net(row):
        if row['is_forecast'] and monthly_iap_net_factor:
            return float(monthly_iap_net_factor.get(row['month_str'], IAP_NET_FACTOR))
        return IAP_NET_FACTOR

    df['iap_net_factor'] = df.apply(_iap_net, axis=1)
    df['revenue_net']    = df['iap_revenue'] * df['iap_net_factor'] + df['ad_revenue'] * AD_NET_FACTOR

    # Actuals maps: all available months, all platforms combined.
    _all = actuals.copy()
    _all['dt'] = pd.to_datetime(_all['dt'])
    _all['month_str'] = _all['dt'].dt.strftime('%Y-%m')

    dau_actuals_map = (
        _all.groupby(['dt', 'month_str'])['dau'].sum()
        .reset_index()
        .groupby('month_str')['dau'].mean()
        .round().astype(int)
        .to_dict()
    )

    _rev_monthly = _all.groupby('month_str').agg(
        iap=('iap_revenue', 'sum'), ad=('ad_revenue', 'sum')
    )
    rev_gross_actuals_map = (_rev_monthly['iap'] + _rev_monthly['ad']).to_dict()
    rev_net_actuals_map   = (
        _rev_monthly['iap'] * IAP_NET_FACTOR + _rev_monthly['ad'] * AD_NET_FACTOR
    ).to_dict()

    df['dau_actuals']       = df['month_str'].map(dau_actuals_map)
    df['rev_gross_actuals'] = df['month_str'].map(rev_gross_actuals_map)
    df['rev_net_actuals']   = df['month_str'].map(rev_net_actuals_map)

    def _mkt(row):
        m = row['month_str']
        if row['is_forecast']:
            return (ios_inp.monthly_ua_spend.get(m, 0) or 0) + (and_inp.monthly_ua_spend.get(m, 0) or 0)
        if historical_marketing:
            return historical_marketing.get(m, float('nan'))
        return float('nan')

    df['marketing_cost']      = df.apply(_mkt, axis=1)
    df['game_margin']         = df['revenue_net']    - df['marketing_cost'].fillna(0)
    df['game_margin_actuals'] = df['rev_net_actuals'] - df['marketing_cost'].fillna(0)
    df['cumulative_margin']   = df['game_margin'].cumsum()

    def _fmt(x):
        return f'${x:,.0f}' if pd.notna(x) else '—'

    def _fmt_dau(x):
        return f'{int(x):,}' if pd.notna(x) else '—'

    disp = pd.DataFrame({
        'Date':               df['date'].dt.strftime('%Y-%m-%d'),
        'DAU Actuals (avg)':  df['dau_actuals'],
        'DAU (avg)':          df['avg_dau'].round().astype(int),
        'ARPDAU':             df['arpdau'],
        'Gross Rev (Act)':    df['rev_gross_actuals'],
        'Revenue (Gross)':    df['revenue_gross'],
        'Net Rev (Act)':      df['rev_net_actuals'],
        'Revenue (Net)':      df['revenue_net'],
        'Marketing Cost':     df['marketing_cost'],
        'Game Margin (Act)':  df['game_margin_actuals'],
        'Game Margin':        df['game_margin'],
        'Cumul. Margin':      df['cumulative_margin'],
        '_fc':                df['is_forecast'],
    })
    is_fc = disp['_fc'].tolist()
    disp  = disp.drop(columns=['_fc'])

    def _row_bg(row):
        c = f'background-color: {_YELLOW}' if is_fc[row.name] else ''
        return [c] * len(row)

    styler = (
        disp.style
        .apply(_row_bg, axis=1)
        .map(lambda _: f'background-color: {_GREEN}', subset=['Game Margin (Act)', 'Game Margin', 'Cumul. Margin'])
        .format({
            'DAU Actuals (avg)': _fmt_dau,
            'DAU (avg)':        '{:,}',
            'ARPDAU':           '${:.2f}',
            'Gross Rev (Act)':  _fmt,
            'Revenue (Gross)':  _fmt,
            'Net Rev (Act)':    _fmt,
            'Revenue (Net)':    _fmt,
            'Marketing Cost':   _fmt,
            'Game Margin (Act)': _fmt,
            'Game Margin':      _fmt,
            'Cumul. Margin':    _fmt,
        })
        .set_table_styles([
            {'selector': 'th',
             'props': [('background-color', _HEADER), ('color', 'white'),
                       ('font-weight', 'bold'), ('padding', '6px 14px'),
                       ('text-align', 'center')]},
            {'selector': 'td',
             'props': [('padding', '4px 14px'), ('text-align', 'right')]},
            {'selector': 'td:first-child',
             'props': [('text-align', 'left')]},
        ])
    )
    return styler, disp
