"""
Monthly P&L summary table: actuals + forecast combined.
"""
from __future__ import annotations

import calendar
import pandas as pd

from common_lib.simulation import load_result, load_scenario

IAP_NET_FACTOR = 0.70
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
    team_cost=None,
    historical_marketing=None,
    n_actuals: int = 6,
) -> 'pd.io.formats.style.Styler':
    """
    Build a styled monthly P&L table combining actuals and forecast.

    Parameters
    ----------
    scenario : str
        Saved scenario name (result must exist).
    actuals : pd.DataFrame
        Daily actuals with columns: dt, platform, dau, iap_revenue, ad_revenue.
    team_cost : float or dict {month_str: float}, optional
        Monthly team/headcount cost.  float = same every month.
    historical_marketing : dict {month_str: float}, optional
        Actual UA spend for historical months (e.g. {'2025-10': 574515}).
        Forecast months always use the scenario's ua_spend.
    n_actuals : int
        How many historical months to show before the forecast start.
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
                fct_df.loc[idx, 'revenue_net']   = (fct_df.loc[idx, 'iap_revenue'] * IAP_NET_FACTOR +
                                                     fct_df.loc[idx, 'ad_revenue']  * AD_NET_FACTOR)
                days = calendar.monthrange(forecast_start.year, forecast_start.month)[1]
                fct_df.loc[idx, 'avg_dau'] = fct_df.loc[idx, 'total_dau'] / days
                fct_df.loc[idx, 'arpdau']  = (fct_df.loc[idx, 'revenue_gross'] /
                                               fct_df.loc[idx, 'total_dau'])

    df = pd.concat([act_df, fct_df], ignore_index=True).sort_values('date').reset_index(drop=True)
    df['month_str'] = df['month'].astype(str)

    def _mkt(row):
        m = row['month_str']
        if row['is_forecast']:
            return (ios_inp.monthly_ua_spend.get(m, 0) or 0) + (and_inp.monthly_ua_spend.get(m, 0) or 0)
        if historical_marketing:
            return historical_marketing.get(m, float('nan'))
        return float('nan')

    def _team(m):
        if team_cost is None:
            return float('nan')
        if isinstance(team_cost, dict):
            return float(team_cost.get(m, float('nan')))
        return float(team_cost)

    df['marketing_cost'] = df.apply(_mkt, axis=1)
    df['team_cost']      = df['month_str'].apply(_team)
    df['profit']         = (
        df['revenue_net']
        - df['marketing_cost'].fillna(0)
        - df['team_cost'].fillna(0)
    )

    disp = pd.DataFrame({
        'Date':            df['date'].dt.strftime('%Y-%m-%d'),
        'DAU':             df['avg_dau'].round().astype(int),
        'ARPDAU':          df['arpdau'],
        'Revenue (Gross)': df['revenue_gross'],
        'Revenue (Net)':   df['revenue_net'],
        'Marketing Cost':  df['marketing_cost'],
        'Team Cost':       df['team_cost'],
        'Profit':          df['profit'],
        '_fc':             df['is_forecast'],
    })
    is_fc = disp['_fc'].tolist()
    disp  = disp.drop(columns=['_fc'])

    def _row_bg(row):
        c = f'background-color: {_YELLOW}' if is_fc[row.name] else ''
        return [c] * len(row)

    def _fmt(x):
        return f'${x:,.0f}' if pd.notna(x) else '—'

    styler = (
        disp.style
        .apply(_row_bg, axis=1)
        .map(lambda _: f'background-color: {_GREEN}', subset=['Profit'])
        .format({
            'DAU':             '{:,}',
            'ARPDAU':          '${:.2f}',
            'Revenue (Gross)': _fmt,
            'Revenue (Net)':   _fmt,
            'Marketing Cost':  _fmt,
            'Team Cost':       _fmt,
            'Profit':          _fmt,
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
    return styler
