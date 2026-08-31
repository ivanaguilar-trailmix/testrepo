"""
Monthly P&L summary table: actuals + forecast combined.
"""
from __future__ import annotations

import calendar
import io
import zipfile
from datetime import datetime
from pathlib import Path

import pandas as pd

from common_lib.simulation import load_result, load_scenario, list_scenarios, SCENARIOS_DIR

_EXPORTS_DIR = Path(__file__).parent.parent / "exports"

IAP_NET_FACTOR = 0.70   # forecast-only fallback when no per-month iap_net_factor input is set
AD_NET_FACTOR  = 0.85   # forecast-only; actuals use the real net revenue figures from BQ

_YELLOW = '#FFFDE7'
_GREEN  = '#C8E6C9'
_HEADER = '#1B5E20'


def _monthly_actuals(actuals: pd.DataFrame, before, n_months: int) -> pd.DataFrame:
    df = actuals.copy()
    df['dt'] = pd.to_datetime(df['dt'])

    daily = (
        df.groupby('dt')
        .agg(
            dau=('dau', 'sum'),
            iap_revenue=('iap_revenue', 'sum'),
            ad_revenue=('ad_revenue', 'sum'),
            iap_net_revenue=('iap_net_revenue', 'sum'),
            ad_net_revenue=('ad_net_revenue', 'sum'),
        )
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
            iap_net_revenue=('iap_net_revenue', 'sum'),
            ad_net_revenue=('ad_net_revenue', 'sum'),
        )
        .reset_index()
    )
    monthly['date']          = monthly['month'].dt.to_timestamp()
    monthly['revenue_gross'] = monthly['iap_revenue'] + monthly['ad_revenue']
    # Real net revenue from BQ (usd_net_iap_revenue / usd_ad_revenue_est), not the fixed-ratio
    # estimate used for forecast rows — actuals have genuine measured net figures already.
    monthly['revenue_net']   = monthly['iap_net_revenue'] + monthly['ad_net_revenue']
    monthly['arpdau']        = monthly['revenue_gross'] / monthly['total_dau']
    monthly['is_forecast']   = False
    return monthly


def _monthly_forecast(result: pd.DataFrame) -> pd.DataFrame:
    df = result[result['platform'] == 'combined'].copy()
    df['date']  = pd.to_datetime(df['date'])
    df['month'] = df['date'].dt.to_period('M')
    if 'boost_dau' not in df.columns:
        # Results saved before the boost DAU breakdown was added — treat as 0.
        df['boost_dau'] = 0.0

    monthly = (
        df.groupby('month')
        .agg(
            avg_dau=('dau', 'mean'),
            total_dau=('dau', 'sum'),
            avg_boost_dau=('boost_dau', 'mean'),
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
    monthly_ua_budget: dict = None,
    marketing_actuals: dict = None,
) -> 'pd.io.formats.style.Styler':
    """
    Build a styled monthly P&L table combining actuals and forecast.

    Parameters
    ----------
    scenario : str
        Saved scenario name (result must exist).
    actuals : pd.DataFrame
        Daily actuals with columns: dt, platform, dau, iap_revenue, ad_revenue,
        iap_net_revenue, ad_net_revenue (real net figures from BQ, used directly
        for actuals rows' Net Rev — not estimated via IAP_NET_FACTOR/AD_NET_FACTOR).
    historical_marketing : dict {month_str: float}, optional
        UA spend for historical months (from the UI panel — planned budget).
    n_actuals : int
        How many historical months to show before the forecast start.
    monthly_iap_net_factor : dict {month_str: float}, optional
        Per-month IAP gross-to-net factor for forecast rows. Falls back to 0.70.
    monthly_ua_budget : dict {month_str: float}, optional
        Combined (ios+android) UA spend for all months. If provided, used as
        primary source for forecast-row marketing cost, overriding the
        per-platform values stored in the scenario file.
    marketing_actuals : dict {month_str: float}, optional
        Real marketing spend from marketing.pkl. When provided, used as the
        source for Mktg Cost (Act) in preference to the UA Budget panel values.
    """
    scenario_data = load_scenario(scenario)
    forecast_start = scenario_data[1]
    ios_inp = scenario_data[3]
    and_inp = scenario_data[4]
    # Prefer the passed-in budget; fall back to what the scenario file stored.
    _ua_budget = monthly_ua_budget if monthly_ua_budget is not None else (scenario_data[10] or {})

    result = load_result(scenario)
    fct_df = _monthly_forecast(result)

    # Derive the actuals cutoff from the result's actual first period, not the
    # scenario's saved forecast_start — the two can differ if forecast_start was
    # changed in the UI after the last save, which would otherwise produce
    # duplicate rows (one actuals row + one forecast row for the same month).
    if not fct_df.empty:
        actuals_before = fct_df['month'].min().to_timestamp()
    else:
        actuals_before = forecast_start

    act_df = _monthly_actuals(actuals, actuals_before, n_actuals)

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
                # arpdau stays as the pure input value — blending affects revenue/DAU totals
                # but the forecast ARPDAU column should reflect the user-entered rate only

    df = pd.concat([act_df, fct_df], ignore_index=True).sort_values('date').reset_index(drop=True)
    df['month_str'] = df['month'].astype(str)

    # Apply per-month IAP net factor to forecast rows only — actuals rows already carry real net
    # revenue from _monthly_actuals() (iap_net_revenue + ad_net_revenue) and must not be
    # overwritten by the ratio estimate, which is a forecast-only approximation.
    def _iap_net(row):
        if row['is_forecast'] and monthly_iap_net_factor:
            return float(monthly_iap_net_factor.get(row['month_str'], IAP_NET_FACTOR))
        return IAP_NET_FACTOR

    df['iap_net_factor'] = df.apply(_iap_net, axis=1)
    forecast_mask = df['is_forecast']
    df.loc[forecast_mask, 'revenue_net'] = (
        df.loc[forecast_mask, 'iap_revenue'] * df.loc[forecast_mask, 'iap_net_factor']
        + df.loc[forecast_mask, 'ad_revenue'] * AD_NET_FACTOR
    )

    # Boost DAU only exists for forecast rows — NaN on actuals (column absent from act_df).
    df['boost_dau_pct'] = df['avg_boost_dau'] / df['avg_dau'] * 100

    # Actuals maps: all available months, all platforms combined.
    _all = actuals.copy()
    _all['dt'] = pd.to_datetime(_all['dt'])
    _all['month_str'] = _all['dt'].dt.strftime('%Y-%m')

    _daily_dau_by_month = (
        _all.groupby(['dt', 'month_str'])['dau'].sum()
        .reset_index()
        .rename(columns={'dau': 'daily_dau'})
    )
    dau_actuals_map = (
        _daily_dau_by_month.groupby('month_str')['daily_dau'].mean()
        .round().astype(int)
        .to_dict()
    )
    _total_dau_monthly = _daily_dau_by_month.groupby('month_str')['daily_dau'].sum()

    _rev_monthly = _all.groupby('month_str').agg(
        iap=('iap_revenue', 'sum'), ad=('ad_revenue', 'sum'),
        iap_net=('iap_net_revenue', 'sum'), ad_net=('ad_net_revenue', 'sum'),
    )
    rev_gross_actuals_map  = (_rev_monthly['iap'] + _rev_monthly['ad']).to_dict()
    # Real net revenue from BQ, not the fixed-ratio estimate (same fix as _monthly_actuals above).
    rev_net_actuals_map    = (_rev_monthly['iap_net'] + _rev_monthly['ad_net']).to_dict()
    arpdau_actuals_map     = (
        (_rev_monthly['iap'] + _rev_monthly['ad']) / _total_dau_monthly
    ).to_dict()

    df['dau_actuals']        = df['month_str'].map(dau_actuals_map)
    df['rev_gross_actuals']  = df['month_str'].map(rev_gross_actuals_map)
    df['rev_net_actuals']    = df['month_str'].map(rev_net_actuals_map)
    df['arpdau_actuals']     = df['month_str'].map(arpdau_actuals_map)

    def _mkt(row):
        m = row['month_str']
        if row['is_forecast']:
            if _ua_budget:
                return float(_ua_budget.get(m, 0) or 0)
            return (ios_inp.monthly_ua_spend.get(m, 0) or 0) + (and_inp.monthly_ua_spend.get(m, 0) or 0)
        if historical_marketing:
            return historical_marketing.get(m, float('nan'))
        return float('nan')

    def _mkt_actuals(row):
        m = row['month_str']
        # Only show a value for months where we have actual DAU/revenue data.
        if m not in dau_actuals_map:
            return float('nan')
        # Priority: real spend from marketing.pkl → UA Budget panel → historical_marketing
        if marketing_actuals:
            val = marketing_actuals.get(m)
            return float(val) if val is not None else float('nan')
        if _ua_budget:
            val = _ua_budget.get(m)
            return float(val) if val is not None else float('nan')
        if historical_marketing:
            return historical_marketing.get(m, float('nan'))
        return float('nan')

    df['marketing_cost']         = df.apply(_mkt, axis=1)
    df['marketing_cost_actuals'] = df.apply(_mkt_actuals, axis=1)
    df['game_margin']            = df['revenue_net']    - df['marketing_cost'].fillna(0)
    df['game_margin_actuals']    = df['rev_net_actuals'] - df['marketing_cost_actuals'].fillna(0)
    df['cumulative_margin']      = df['game_margin'].cumsum()

    def _fmt(x):
        return f'${x:,.0f}' if pd.notna(x) else '—'

    def _fmt_dau(x):
        return f'{int(x):,}' if pd.notna(x) else '—'

    def _fmt_arpdau(x):
        return f'${x:.2f}' if pd.notna(x) else '—'

    def _fmt_pct(x):
        return f'{x:.1f}%' if pd.notna(x) else '—'

    disp = pd.DataFrame({
        'Date':               df['date'].dt.strftime('%Y-%m-%d'),
        'DAU (Act)':          df['dau_actuals'],
        'DAU':                df['avg_dau'].round().astype(int),
        'Boost DAU':          df['avg_boost_dau'].round(),
        'Boost DAU %':        df['boost_dau_pct'],
        'ARPDAU (Act)':       df['arpdau_actuals'],
        'ARPDAU':             df['arpdau'],
        'Gross Rev (Act)':    df['rev_gross_actuals'],
        'Gross Rev':          df['revenue_gross'],
        'Net Rev (Act)':      df['rev_net_actuals'],
        'Net Rev':            df['revenue_net'],
        'Mktg Cost (Act)':    df['marketing_cost_actuals'],
        'Mktg Cost':          df['marketing_cost'],
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
            'DAU (Act)':          _fmt_dau,
            'DAU':                '{:,}',
            'Boost DAU':          _fmt_dau,
            'Boost DAU %':        _fmt_pct,
            'ARPDAU (Act)':       _fmt_arpdau,
            'ARPDAU':             '${:.2f}',
            'Gross Rev (Act)':    _fmt,
            'Gross Rev':          _fmt,
            'Net Rev (Act)':      _fmt,
            'Net Rev':            _fmt,
            'Mktg Cost (Act)':    _fmt,
            'Mktg Cost':          _fmt,
            'Game Margin (Act)':  _fmt,
            'Game Margin':        _fmt,
            'Cumul. Margin':      _fmt,
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


def comparison_table(
    months=('2027-03', '2027-12', '2029-12'),
    actuals: pd.DataFrame = None,
) -> pd.DataFrame:
    """
    Compare DAU and cumulative game margin across all selectable scenarios at
    specific months.

    Calls monthly_table for each scenario so the numbers are guaranteed to match
    the P&L table exactly (same actuals base, same partial-month blending, same
    IAP net factor). historical_marketing and n_actuals are read from the scenario
    JSON so no extra parameters are needed.
    """
    from datetime import date as _date

    rows = []
    for name in list_scenarios():
        try:
            scenario_tuple         = load_scenario(name)
            forecast_start         = scenario_tuple[1]
            actuals_from_str       = scenario_tuple[7]
            historical_marketing   = scenario_tuple[9] or None
            monthly_iap_net_factor = scenario_tuple[12] or None
            margin_targets         = scenario_tuple[13] or {}

            # Derive n_actuals from actuals_from stored in the scenario JSON.
            if actuals_from_str and actuals is not None:
                af    = _date.fromisoformat(actuals_from_str)
                n_act = max(1, (forecast_start.year - af.year) * 12
                               + (forecast_start.month - af.month))
            else:
                n_act = 6

            _, disp = monthly_table(
                scenario=name,
                actuals=actuals,
                historical_marketing=historical_marketing,
                n_actuals=n_act,
                monthly_iap_net_factor=monthly_iap_net_factor,
            )

            disp = disp.copy()
            disp['_month'] = disp['Date'].str[:7]

            row = {'Scenario': name}
            for m in months:
                sub = disp[disp['_month'] == m]
                if not sub.empty:
                    margin_val = sub['Cumul. Margin'].iloc[0]
                    row[f'DAU {m}']          = sub['DAU'].iloc[0]
                    row[f'Cumul Margin {m}']  = margin_val
                    target = margin_targets.get(m)
                    if target is not None:
                        pct = (margin_val - target) / abs(target) * 100 if target != 0 else 0.0
                        row[f'vs Target {m}'] = pct
                    else:
                        row[f'vs Target {m}'] = None
                else:
                    row[f'DAU {m}']          = None
                    row[f'Cumul Margin {m}']  = None
                    row[f'vs Target {m}']     = None
            rows.append(row)
        except Exception as e:
            rows.append({'Scenario': name, 'note': str(e)})

    df = pd.DataFrame(rows).set_index('Scenario')

    def _fmt_dau(x):
        return f'{int(x):,}' if pd.notna(x) else '—'

    def _fmt_margin(x):
        return f'${x:,.0f}' if pd.notna(x) else '—'

    def _fmt_target(x):
        if pd.isna(x):
            return '—'
        mark = '✓' if x >= 0 else '✗'
        return f'{mark} {x:+.1f}%'

    def _target_color(x):
        if pd.isna(x):
            return ''
        return f'color: {"#27ae60" if x >= 0 else "#e74c3c"}'

    fmt = {}
    target_cols = []
    for col in df.columns:
        if col.startswith('DAU '):
            fmt[col] = _fmt_dau
        elif col.startswith('Cumul Margin '):
            fmt[col] = _fmt_margin
        elif col.startswith('vs Target '):
            fmt[col] = _fmt_target
            target_cols.append(col)

    styler = (
        df.style
        .format(fmt, na_rep='—')
        .map(_target_color, subset=target_cols)
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
    return styler, df


def installs_table(result: pd.DataFrame):
    """
    Monthly UA-driven vs external Boost installs, combined platforms, forecast horizon only.

    Returns a Styler with install source as rows and month as columns — deliberately wide
    rather than tall (one column per forecast month), so callers should wrap the rendered
    HTML in a horizontally scrollable container (e.g. `<div style="overflow-x:auto">`).
    """
    df = result[result['platform'] == 'combined'].copy()
    df['month'] = pd.to_datetime(df['date']).dt.to_period('M').astype(str)
    # Older saved results (before boost_installs was tracked separately) lack the column.
    df['boost_installs'] = df['boost_installs'].fillna(0.0) if 'boost_installs' in df else 0.0
    df['ua_installs']    = df['new_installs'] - df['boost_installs']

    monthly = df.groupby('month')[['ua_installs', 'boost_installs']].sum()
    out = monthly.T
    out.index = ['UA Installs', 'Boost Installs']

    styler = (
        out.style
        .format('{:,.0f}')
        .set_table_styles([
            {'selector': 'th',
             'props': [('background-color', _HEADER), ('color', 'white'),
                       ('font-weight', 'bold'), ('padding', '6px 14px'),
                       ('text-align', 'center')]},
            {'selector': 'td',
             'props': [('padding', '4px 14px'), ('text-align', 'right')]},
            {'selector': 'th.row_heading',
             'props': [('text-align', 'left'), ('position', 'sticky'), ('left', '0')]},
        ])
    )
    return styler


def export_all_tables(
    actuals: pd.DataFrame,
    months=('2027-03', '2027-12', '2029-12'),
) -> Path:
    """
    Export every selectable scenario's P&L table plus the comparison summary,
    bundled with the source scenario JSON files, into a timestamped zip.

    Zip contents:
      <scenario_name>_pl_table.csv  — one per scenario
      comparison_table.csv          — cross-scenario DAU + cumulative margin
      scenarios/<scenario>.json     — source scenario files

    Returns the path to the zip file.
    """
    from datetime import date as _date

    _EXPORTS_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    zip_path  = _EXPORTS_DIR / f"export_{timestamp}.zip"

    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:

        for name in list_scenarios():
            try:
                scenario_tuple         = load_scenario(name)
                forecast_start         = scenario_tuple[1]
                actuals_from_str       = scenario_tuple[7]
                historical_marketing   = scenario_tuple[9] or None
                monthly_iap_net_factor = scenario_tuple[12] or None

                if actuals_from_str:
                    af    = _date.fromisoformat(actuals_from_str)
                    n_act = max(1, (forecast_start.year - af.year) * 12
                                   + (forecast_start.month - af.month))
                else:
                    n_act = 6

                _, disp = monthly_table(
                    scenario=name,
                    actuals=actuals,
                    historical_marketing=historical_marketing,
                    n_actuals=n_act,
                    monthly_iap_net_factor=monthly_iap_net_factor,
                )

                safe_name = name.replace(' ', '_').replace('/', '-')
                buf = io.StringIO()
                disp.to_csv(buf, index=False)
                zf.writestr(f"{safe_name}_pl_table.csv", buf.getvalue())
                print(f"  added: {safe_name}_pl_table.csv")
            except Exception as e:
                print(f"  skipped {name!r}: {e}")

        buf = io.StringIO()
        _, comp_df = comparison_table(actuals=actuals, months=months)
        comp_df.to_csv(buf)
        zf.writestr("comparison_table.csv", buf.getvalue())
        print(f"  added: comparison_table.csv")

        for json_path in sorted(SCENARIOS_DIR.glob("*.json")):
            zf.write(json_path, f"scenarios/{json_path.name}")
            print(f"  added: scenarios/{json_path.name}")

    print(f"\nExport written to: {zip_path}")
    return zip_path
