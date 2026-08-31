"""
Plotting helpers for monthly performance notebooks (dau.ipynb, payers.ipynb).
"""
import pandas as pd
import plotly.graph_objects as go


def select_repeat_purchase_offset(repeat_purchase_df: pd.DataFrame, n: int, max_dt=None) -> pd.DataFrame:
    """Slice one offset (e.g. n=7 -> the 'rate_d7' column) out of repeat_purchase_df into a
    (dt, users) frame ready for chart_rate_trend / chart_absolute_and_pct_change - switching
    which offset you're charting is then just changing `n`.

    Also trims the trailing `n` days, which are right-censored (not enough future data yet to
    know whether those purchasers repeat-bought) and would otherwise show a false drop to 0%.

    repeat_purchase_df: output of the repeat-purchase-rate build cell - a 'dt' column plus one
    'rate_dN' column per offset.
    max_dt: the last fully-known date in the underlying activity data. Defaults to
    repeat_purchase_df['dt'].max() if not given.
    """
    if max_dt is None:
        max_dt = repeat_purchase_df['dt'].max()
    valid = repeat_purchase_df[repeat_purchase_df['dt'] <= max_dt - pd.Timedelta(days=n)]
    return valid[['dt', f'rate_d{n}']].rename(columns={f'rate_d{n}': 'users'})


def chart_absolute_and_pct_change(total_by_dt: pd.DataFrame, metric_label: str, year: int = 2026, freq: str = 'D', pre_aggregated: bool = False) -> None:
    """Dual-axis: absolute count (left) vs period-over-period % change (right), one metric,
    restricted to `year`. freq='D' (default) plots the daily values with day-over-day % change.
    freq='W' resamples to weekly averages *of the daily values* (mean of the daily counts within
    each week, not a fresh weekly-unique-user count - a different, larger number) with
    week-over-week % change. Unsmoothed % change will show weekly seasonality at freq='D' -
    that's expected, not a bug; freq='W' is exactly the smoothing that removes it.

    pre_aggregated=True (only meaningful with freq='W'): total_by_dt already holds one row per
    week (e.g. a true weekly-unique-user count computed from the raw data, not an average of
    daily counts) - skips the resample/mean step and plots the rows as given.

    total_by_dt must have columns 'dt' (datetime64) and 'users'.
    """

    period_label = {'D': 'Day/Day', 'W': 'Week/Week'}[freq]
    resampled = freq == 'W' and not pre_aggregated

    df = total_by_dt[total_by_dt['dt'].dt.year == year].sort_values('dt').set_index('dt')[['users']]
    if resampled:
        df = df.resample('W').mean().reset_index()
    else:
        df = df.reset_index()
    df['pct_change'] = df['users'].pct_change() * 100

    agg_note = ', weekly avg' if resampled else (', true weekly count' if freq == 'W' else '')
    value_name = f'{metric_label} (absolute{agg_note})'

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df['dt'], y=df['users'], mode='lines', name=value_name,
        line=dict(color='steelblue'), yaxis='y1',
        hovertemplate='Date: %{x|%Y-%m-%d}<br>' + f'{metric_label}: ' + '%{y:,.0f}<extra></extra>',
    ))
    fig.add_trace(go.Scatter(
        x=df['dt'], y=df['pct_change'], mode='lines', name=f'{metric_label} {period_label.lower()} % change',
        line=dict(color='crimson', width=1), yaxis='y2', opacity=0.6,
        hovertemplate='Date: %{x|%Y-%m-%d}<br>Change: %{y:+.1f}%<extra></extra>',
    ))

    fig.update_layout(
        title=f"{metric_label} — Absolute vs {period_label} % Change ({year}{agg_note.title()})",
        xaxis=dict(title='Week' if freq == 'W' else 'Date', dtick='M1', tickformat='%b %Y'),
        yaxis=dict(title=f'{metric_label} (absolute)', color='steelblue'),
        yaxis2=dict(title=f'{period_label} % Change', color='crimson', overlaying='y', side='right', ticksuffix='%', zeroline=True, zerolinecolor='grey', zerolinewidth=1),
        height=500, width=1100,
        legend=dict(orientation='h', yanchor='bottom', y=-0.3, xanchor='center', x=0.5),
    )
    fig.show()


def chart_actual_vs_predictions(actual_df: pd.DataFrame, prediction_series: list, start=None, end=None, title: str = 'Actual vs Predicted DAU (Monthly Avg)') -> None:
    """Two separate charts: (1) absolute monthly-average DAU for actual + each prediction
    vintage, (2) each prediction's % deviation from actual. Deviation is only plotted for
    months where actual data exists (an inner merge on 'dt') - forecast months beyond that
    only show up in the absolute chart.

    actual_df: columns 'dt' (datetime64, month start) and 'users'.
    prediction_series: list of (label, df) tuples, each df with columns 'dt' and 'users'.
    start/end: optional bounds (anything pd.Timestamp() accepts) to restrict the x-range of
    every series, e.g. start='2026-07-01', end='2027-03-31'.
    """
    def _clip(df):
        if start is not None:
            df = df[df['dt'] >= pd.Timestamp(start)]
        if end is not None:
            df = df[df['dt'] <= pd.Timestamp(end)]
        return df

    actual_df = _clip(actual_df)
    prediction_series = [(label, _clip(df)) for label, df in prediction_series]

    # Shades of blue throughout (not red/green) - deviation is neither "good" nor "bad"
    # here, just a difference from actual, so avoid colors that read as positive/negative.
    abs_colors = ['navy', 'dodgerblue', 'lightskyblue']
    dev_colors = ['navy', 'dodgerblue', 'lightskyblue']
    dash_styles = ['dash', 'dot', 'dashdot']

    # --- Chart 1: absolute values ---
    fig_abs = go.Figure()
    fig_abs.add_trace(go.Scatter(
        x=actual_df['dt'], y=actual_df['users'], mode='lines+markers', name='Actual',
        line=dict(color='steelblue', width=3),
        hovertemplate='Month: %{x|%Y-%m}<br>Actual: %{y:,.0f}<extra></extra>',
    ))
    for i, (label, df) in enumerate(prediction_series):
        fig_abs.add_trace(go.Scatter(
            x=df['dt'], y=df['users'], mode='lines', name=label,
            line=dict(color=abs_colors[i % len(abs_colors)], dash=dash_styles[i % len(dash_styles)]),
            hovertemplate=f'Month: %{{x|%Y-%m}}<br>{label}: ' + '%{y:,.0f}<extra></extra>',
        ))
    fig_abs.update_layout(
        title=f'{title} — Absolute',
        xaxis=dict(title='Month', dtick='M1', tickformat='%b %Y'),
        yaxis_title='DAU (monthly avg)',
        height=500, width=1100,
        legend=dict(orientation='h', yanchor='bottom', y=-0.35, xanchor='center', x=0.5),
    )
    fig_abs.show()

    # --- Chart 2: % deviation from actual (grouped bars, one group per month) ---
    # Left join on the prediction's own months (not just the actual/prediction overlap), so
    # months without actual data still show up on the axis - just with no bar drawn.
    fig_dev = go.Figure()
    for i, (label, df) in enumerate(prediction_series):
        merged = pd.merge(df, actual_df, on='dt', how='left', suffixes=('_pred', '_actual'))
        if merged.empty:
            continue
        merged['deviation_pct'] = (merged['users_pred'] - merged['users_actual']) / merged['users_actual'] * 100
        fig_dev.add_trace(go.Bar(
            x=merged['dt'], y=merged['deviation_pct'], name=label,
            marker=dict(color=dev_colors[i % len(dev_colors)]),
            customdata=merged[['users_actual', 'users_pred']],
            text=merged['deviation_pct'].round(2), texttemplate='%{text:+.2f}%', textposition='outside',
            textfont=dict(size=14), constraintext='none', cliponaxis=False,
            hovertemplate=(
                f'Month: %{{x|%Y-%m}}<br>{label} deviation: %{{y:+.1f}}%<br>'
                'Actual: %{customdata[0]:,.0f}<br>Forecast: %{customdata[1]:,.0f}<extra></extra>'
            ),
        ))
    fig_dev.update_layout(
        title=f'{title} — Deviation from Actual',
        barmode='group',
        xaxis=dict(title='Month', dtick='M1', tickformat='%b %Y'),
        yaxis_title='Deviation from Actual',
        yaxis=dict(ticksuffix='%', zeroline=True, zerolinecolor='grey', zerolinewidth=1),
        height=500, width=1100,
        legend=dict(orientation='h', yanchor='bottom', y=-0.35, xanchor='center', x=0.5),
    )
    fig_dev.show()


def chart_rate_trend(total_by_dt: pd.DataFrame, metric_label: str, freq: str = 'D', rolling_windows=(7, 28), year: int = None) -> None:
    """Single-axis trend view for a rate metric (e.g. repeat-purchase rate), where day-over-day
    %% change is too noisy to show trend direction (chart_absolute_and_pct_change is a better
    fit for count metrics, not rates that bounce around day to day).

    freq='D' (default) plots the raw daily rate (thin, faded) plus one rolling-average trend
    line per entry in `rolling_windows` - the moving average is what actually shows whether
    the metric is trending up or down. freq='W' instead plots a single already-smoothed
    weekly-average line with no rolling overlay (redundant once resampled).

    total_by_dt must have columns 'dt' (datetime64) and 'users' (holds the rate value, e.g.
    a percentage - same column name as other chart_* helpers here for consistency).
    """
    df = total_by_dt.sort_values('dt')
    if year is not None:
        df = df[df['dt'].dt.year == year]

    fig = go.Figure()
    trend_colors = ['navy', 'dodgerblue', 'lightskyblue']

    if freq == 'W':
        weekly = df.set_index('dt')[['users']].resample('W').mean().reset_index()
        fig.add_trace(go.Scatter(
            x=weekly['dt'], y=weekly['users'], mode='lines+markers', name=f'{metric_label} (weekly avg)',
            line=dict(color='steelblue', width=2),
            hovertemplate='Week: %{x|%Y-%m-%d}<br>' + f'{metric_label}: ' + '%{y:.1f}%<extra></extra>',
        ))
        title_suffix = 'Weekly Avg'
    else:
        fig.add_trace(go.Scatter(
            x=df['dt'], y=df['users'], mode='lines', name=f'{metric_label} (daily)',
            line=dict(color='lightsteelblue', width=1), opacity=0.6,
            hovertemplate='Date: %{x|%Y-%m-%d}<br>' + f'{metric_label}: ' + '%{y:.1f}%<extra></extra>',
        ))
        for i, w in enumerate(rolling_windows):
            rolling = df.set_index('dt')['users'].rolling(w, min_periods=max(1, w // 2)).mean().reset_index()
            fig.add_trace(go.Scatter(
                x=rolling['dt'], y=rolling['users'], mode='lines', name=f'{w}-day avg',
                line=dict(color=trend_colors[i % len(trend_colors)], width=2),
                hovertemplate='Date: %{x|%Y-%m-%d}<br>' + f'{w}-day avg: ' + '%{y:.1f}%<extra></extra>',
            ))
        title_suffix = 'Daily + Rolling Trend'

    fig.update_layout(
        title=f"{metric_label} ({title_suffix}{f', {year}' if year is not None else ''})",
        xaxis=dict(title='Week' if freq == 'W' else 'Date', dtick='M1', tickformat='%b %Y'),
        yaxis=dict(title=metric_label, ticksuffix='%'),
        height=500, width=1100,
        legend=dict(orientation='h', yanchor='bottom', y=-0.3, xanchor='center', x=0.5),
    )
    fig.show()
