"""
Chart functions for the game simulator.
"""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio

from datetime import date

from common_lib.curves import build_curve
from common_lib.simulation import load_result, load_scenario

COLORS          = {'ios': '#007AFF', 'android': '#34C759', 'combined': '#FF9500'}
SCENARIO_COLORS = ['#5B8DEF', '#34C759', '#FF9500', '#FF6B6B']
SCENARIO_STYLES = ['solid', 'dash', 'dot']

_CHART_META = {
    'dau':      ('dau',      'dau',           'DAU Forecast',             ',.0f'),
    'installs': ('installs', 'new_installs',  'New Installs per Day',     ',.0f'),
    'revenue':  ('revenue',  'total_revenue', 'Daily Revenue (IAP + Ad)', '$,.2f'),
    'payers':   ('payers',   'payer_dau',     'Payer DAU',                ',.0f'),
}

_actuals: pd.DataFrame | None = None


def configure(actuals_df: pd.DataFrame) -> None:
    """Call once after loading actuals to enable actuals overlays in charts."""
    global _actuals
    _actuals = actuals_df
    pio.renderers.default = 'notebook'


def _build_daily_fig(
    metric: str, y_col: str, y_label: str,
    dfs: dict, title: str, fmt: str = ',.0f',
) -> go.Figure:
    def _iso(series) -> list[str]:
        return pd.to_datetime(series).dt.strftime('%Y-%m-%d').tolist()

    def _fmt_dates(series) -> list[str]:
        return pd.to_datetime(series).dt.strftime('%b %d, %Y').tolist()

    fig = go.Figure()
    for i, (name, df) in enumerate(dfs.items()):
        style = SCENARIO_STYLES[i % len(SCENARIO_STYLES)]
        for platform in ('ios', 'android', 'combined'):
            sub   = df[df['platform'] == platform]
            color = COLORS[platform]
            label = f'{name} / {platform}'
            fig.add_trace(go.Scatter(
                x=_iso(sub['date']), y=sub[y_col], name=label,
                customdata=_fmt_dates(sub['date']),
                line=dict(color=color, dash=style),
                legendgroup=label,
                hovertemplate=f'{label} (%{{customdata}}): %{{y:{fmt}}}<extra></extra>',
            ))

    if _actuals is not None:
        if metric == 'dau':
            for platform, grp in _actuals.groupby('platform'):
                fig.add_trace(go.Scatter(
                    x=_iso(grp['dt']), y=grp['dau'], name=f'actual / {platform}',
                    customdata=_fmt_dates(grp['dt']),
                    mode='markers', marker=dict(color=COLORS.get(platform, 'grey'), size=4, opacity=0.6),
                    legendgroup=f'actual/{platform}',
                    hovertemplate=f'actual / {platform} (%{{customdata}}): %{{y:,.0f}}<extra></extra>',
                ))
            combined_dau = _actuals.groupby('dt')['dau'].sum().reset_index()
            fig.add_trace(go.Scatter(
                x=_iso(combined_dau['dt']), y=combined_dau['dau'], name='actual / combined',
                customdata=_fmt_dates(combined_dau['dt']),
                mode='markers', marker=dict(color=COLORS['combined'], size=4, opacity=0.6),
                legendgroup='actual/combined',
                hovertemplate='actual / combined (%{customdata}): %{y:,.0f}<extra></extra>',
            ))

        if metric == 'revenue':
            for platform, grp in _actuals.groupby('platform'):
                grp    = grp.sort_values('dt')
                rev    = grp['iap_revenue'] + grp['ad_revenue']
                rev_ma = rev.rolling(7, min_periods=1).mean()
                color  = COLORS.get(platform, 'grey')
                x_iso  = _iso(grp['dt'])
                dates  = _fmt_dates(grp['dt'])
                fig.add_trace(go.Scatter(
                    x=x_iso, y=rev, name=f'actual / {platform}',
                    customdata=dates,
                    mode='markers', marker=dict(color=color, size=4, opacity=0.4),
                    legendgroup=f'actual/{platform}',
                    hovertemplate=f'actual / {platform} (%{{customdata}}): %{{y:$,.2f}}<extra></extra>',
                ))
                fig.add_trace(go.Scatter(
                    x=x_iso, y=rev_ma, name=f'actual 7MA / {platform}',
                    customdata=dates,
                    mode='lines', line=dict(color=color, width=2),
                    legendgroup=f'actual/{platform}',
                    hovertemplate=f'7MA / {platform} (%{{customdata}}): %{{y:$,.2f}}<extra></extra>',
                ))
            combined_rev = (
                _actuals.groupby('dt')[['iap_revenue', 'ad_revenue']]
                .sum().reset_index().sort_values('dt')
            )
            combined_rev['total']    = combined_rev['iap_revenue'] + combined_rev['ad_revenue']
            combined_rev['total_ma'] = combined_rev['total'].rolling(7, min_periods=1).mean()
            cx_iso = _iso(combined_rev['dt'])
            cdates = _fmt_dates(combined_rev['dt'])
            fig.add_trace(go.Scatter(
                x=cx_iso, y=combined_rev['total'], name='actual / combined',
                customdata=cdates,
                mode='markers', marker=dict(color=COLORS['combined'], size=4, opacity=0.4),
                legendgroup='actual/combined',
                hovertemplate='actual / combined (%{customdata}): %{y:$,.2f}<extra></extra>',
            ))
            fig.add_trace(go.Scatter(
                x=cx_iso, y=combined_rev['total_ma'], name='actual 7MA / combined',
                customdata=cdates,
                mode='lines', line=dict(color=COLORS['combined'], width=2),
                legendgroup='actual/combined',
                hovertemplate='7MA / combined (%{customdata}): %{y:$,.2f}<extra></extra>',
            ))

    fig.update_layout(
        title_text=f'{y_label} — {title}',
        height=500, width=1450,
        margin=dict(t=60, b=30), hovermode='closest',
        xaxis=dict(tickformat='%b %Y'),
    )
    return fig


def _blend_partial_actuals(monthly: pd.DataFrame, forecast_start: date) -> pd.DataFrame:
    """Add pre-forecast-start actuals into the first forecast month when it started mid-month."""
    if _actuals is None or forecast_start.day <= 1:
        return monthly
    month_str = forecast_start.strftime('%Y-%m')
    if month_str not in monthly['month'].values:
        return monthly
    acts = _actuals.copy()
    acts['dt'] = pd.to_datetime(acts['dt'])
    month_first = pd.Timestamp(forecast_start.replace(day=1))
    forecast_ts = pd.Timestamp(forecast_start)
    partial = acts[(acts['dt'] >= month_first) & (acts['dt'] < forecast_ts)]
    if partial.empty:
        return monthly
    extra = partial['iap_revenue'].sum() + partial['ad_revenue'].sum()
    monthly = monthly.copy()
    monthly.loc[monthly['month'] == month_str, 'total_revenue'] += extra
    return monthly


def _build_monthly_fig(dfs: dict, title: str, forecast_start: date = None) -> go.Figure:
    fig = go.Figure()
    for i, (name, df) in enumerate(dfs.items()):
        comb = df[df['platform'] == 'combined'].copy()
        comb['month'] = comb['date'].dt.to_period('M').astype(str)
        monthly = comb.groupby('month')['total_revenue'].sum().reset_index()
        if forecast_start is not None:
            monthly = _blend_partial_actuals(monthly, forecast_start)
        fig.add_trace(go.Bar(
            x=monthly['month'], y=monthly['total_revenue'],
            name=name, marker_color=SCENARIO_COLORS[i % len(SCENARIO_COLORS)],
            hovertemplate=f'{name} — %{{x}}: $%{{y:,.0f}}<extra></extra>',
        ))
    fig.update_layout(
        title_text=f'Monthly Total Revenue — {title}', barmode='group',
        height=400, width=1450,
        margin=dict(t=60, b=30), hovermode='closest',
        yaxis=dict(tickprefix='$', tickformat=',.0f'), xaxis_title='Month',
    )
    return fig


def plot(scenarios, chart: str = 'all') -> None:
    """
    Load saved simulation results and render charts via fig.show().

    scenarios : str or list[str]
    chart     : 'all' | 'dau' | 'installs' | 'revenue' | 'payers' | 'monthly'
    """
    if isinstance(scenarios, str):
        scenarios = [scenarios]
    dfs   = {name: load_result(name) for name in scenarios}
    title = ' vs '.join(scenarios)
    show  = {chart} if chart != 'all' else {'dau', 'installs', 'revenue', 'payers', 'monthly'}

    for key in ('dau', 'installs', 'revenue', 'payers'):
        if key in show:
            metric, y_col, y_label, fmt = _CHART_META[key]
            _build_daily_fig(metric, y_col, y_label, dfs, title, fmt).show()
    if 'monthly' in show:
        _build_monthly_fig(dfs, title).show()


def build_chart_widget(scenarios, chart: str) -> go.FigureWidget:
    """
    Return a FigureWidget for the given chart key — no Output widget needed.
    chart: 'dau' | 'installs' | 'revenue' | 'payers' | 'monthly'
    """
    if isinstance(scenarios, str):
        scenarios = [scenarios]
    dfs   = {name: load_result(name) for name in scenarios}
    title = ' vs '.join(scenarios)

    if chart == 'monthly':
        # Load forecast_start from first scenario for partial-month blending
        try:
            _, fs, *_ = load_scenario(scenarios[0])
        except Exception:
            fs = None
        fig = _build_monthly_fig(dfs, title, forecast_start=fs)
    else:
        metric, y_col, y_label, fmt = _CHART_META[chart]
        fig = _build_daily_fig(metric, y_col, y_label, dfs, title, fmt)

    return go.FigureWidget(fig)


def _resolve_named_anchors(scenarios_or_anchors) -> list[tuple[str, dict]]:
    """Normalise scenario names or a raw anchors dict into [(label, anchors), ...]."""
    if isinstance(scenarios_or_anchors, dict):
        return [('current', scenarios_or_anchors)]
    if isinstance(scenarios_or_anchors, str):
        scenarios_or_anchors = [scenarios_or_anchors]
    result = []
    for name in scenarios_or_anchors:
        _, _, _, _, _, curve_anchors, *_ = load_scenario(name)
        if curve_anchors is None:
            print(f"Warning: '{name}' has no saved curve anchors — skipped.")
            continue
        result.append((name, curve_anchors))
    return result


def _plot_single_curve(metric: str, named_anchors: list[tuple[str, dict]]) -> None:
    dx = list(range(1, 366))
    fig = go.Figure()
    for i, (label, anchors) in enumerate(named_anchors):
        for platform in ('ios', 'android'):
            color       = COLORS[platform]
            style       = SCENARIO_STYLES[i % len(SCENARIO_STYLES)]
            raw         = {int(k): v for k, v in anchors[platform][metric].items()}
            curve       = build_curve(raw)
            trace_label = f'{label} / {platform}' if len(named_anchors) > 1 else platform
            fig.add_trace(go.Scatter(
                x=dx, y=curve * 100,
                name=trace_label,
                line=dict(color=color, dash=style),
                hovertemplate=f'{trace_label}: %{{y:.2f}}%<extra></extra>',
            ))
    fig.update_layout(
        title_text=f'{metric.capitalize()} Curve (D1–D365)',
        height=400, width=900,
        margin=dict(t=60, b=30),
        hovermode='x unified',
        yaxis=dict(ticksuffix='%', title=f'{metric.capitalize()} rate'),
        xaxis_title='Day since install',
    )
    fig.show()


def plot_retention(scenarios_or_anchors) -> None:
    """
    Plot interpolated retention curves for iOS and Android.

    scenarios_or_anchors:
      - str or list[str] : load curve_anchors from saved scenario JSON files
      - dict             : use directly, e.g. panel.get_curve_anchors()
    """
    named = _resolve_named_anchors(scenarios_or_anchors)
    if named:
        _plot_single_curve('retention', named)


def plot_conversion(scenarios_or_anchors) -> None:
    """
    Plot interpolated conversion curves for iOS and Android.

    scenarios_or_anchors:
      - str or list[str] : load curve_anchors from saved scenario JSON files
      - dict             : use directly, e.g. panel.get_curve_anchors()
    """
    named = _resolve_named_anchors(scenarios_or_anchors)
    if named:
        _plot_single_curve('conversion', named)
