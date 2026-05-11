"""
Chart functions for the game simulator.
"""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go

from common_lib.curves import build_curve
from common_lib.simulation import load_result, load_scenario

COLORS          = {'ios': '#007AFF', 'android': '#34C759', 'combined': '#FF9500'}
SCENARIO_COLORS = ['#5B8DEF', '#34C759', '#FF9500', '#FF6B6B']
SCENARIO_STYLES = ['solid', 'dash', 'dot']

_actuals: pd.DataFrame | None = None


def configure(actuals_df: pd.DataFrame) -> None:
    """Call once after loading actuals to enable actuals overlays in charts."""
    global _actuals
    _actuals = actuals_df


def plot(scenarios, chart: str = 'all') -> None:
    """
    Load saved simulation results and render charts.

    scenarios : str or list[str]
    chart     : 'all' | 'dau' | 'installs' | 'revenue' | 'payers' | 'monthly'
    """
    if isinstance(scenarios, str):
        scenarios = [scenarios]

    dfs   = {name: load_result(name) for name in scenarios}
    title = ' vs '.join(scenarios)
    show  = {chart} if chart != 'all' else {'dau', 'installs', 'revenue', 'payers', 'monthly'}

    def _daily_fig(metric: str, y_col: str, y_label: str, fmt: str = ',.0f') -> None:
        fig = go.Figure()
        for i, (name, df) in enumerate(dfs.items()):
            style = SCENARIO_STYLES[i % len(SCENARIO_STYLES)]
            for platform in ('ios', 'android', 'combined'):
                sub   = df[df['platform'] == platform]
                color = COLORS[platform]
                label = f'{name} / {platform}'
                fig.add_trace(go.Scatter(
                    x=sub['date'], y=sub[y_col], name=label,
                    line=dict(color=color, dash=style),
                    legendgroup=label,
                    hovertemplate=f'{label}: %{{y:{fmt}}}<extra></extra>',
                ))

        if _actuals is not None:
            if metric == 'dau':
                for platform, grp in _actuals.groupby('platform'):
                    fig.add_trace(go.Scatter(
                        x=grp['dt'], y=grp['dau'], name=f'actual / {platform}',
                        mode='markers', marker=dict(color=COLORS.get(platform, 'grey'), size=4, opacity=0.6),
                        legendgroup=f'actual/{platform}',
                        hovertemplate=f'actual / {platform} (%{{x|%b %d}}): %{{y:,.0f}}<extra></extra>',
                    ))
                combined_dau = _actuals.groupby('dt')['dau'].sum().reset_index()
                fig.add_trace(go.Scatter(
                    x=combined_dau['dt'], y=combined_dau['dau'], name='actual / combined',
                    mode='markers', marker=dict(color=COLORS['combined'], size=4, opacity=0.6),
                    legendgroup='actual/combined',
                    hovertemplate='actual / combined (%{x|%b %d}): %{y:,.0f}<extra></extra>',
                ))

            if metric == 'revenue':
                for platform, grp in _actuals.groupby('platform'):
                    grp   = grp.sort_values('dt')
                    rev   = grp['iap_revenue'] + grp['ad_revenue']
                    rev_ma = rev.rolling(7, min_periods=1).mean()
                    color  = COLORS.get(platform, 'grey')
                    fig.add_trace(go.Scatter(
                        x=grp['dt'], y=rev, name=f'actual / {platform}',
                        mode='markers', marker=dict(color=color, size=4, opacity=0.4),
                        legendgroup=f'actual/{platform}',
                        hovertemplate=f'actual / {platform} (%{{x|%b %d}}): %{{y:$,.2f}}<extra></extra>',
                    ))
                    fig.add_trace(go.Scatter(
                        x=grp['dt'], y=rev_ma, name=f'actual 7MA / {platform}',
                        mode='lines', line=dict(color=color, width=2),
                        legendgroup=f'actual/{platform}',
                        hovertemplate=f'7MA / {platform} (%{{x|%b %d}}): %{{y:$,.2f}}<extra></extra>',
                    ))
                combined_rev = (
                    _actuals.groupby('dt')[['iap_revenue', 'ad_revenue']]
                    .sum().reset_index().sort_values('dt')
                )
                combined_rev['total']    = combined_rev['iap_revenue'] + combined_rev['ad_revenue']
                combined_rev['total_ma'] = combined_rev['total'].rolling(7, min_periods=1).mean()
                fig.add_trace(go.Scatter(
                    x=combined_rev['dt'], y=combined_rev['total'], name='actual / combined',
                    mode='markers', marker=dict(color=COLORS['combined'], size=4, opacity=0.4),
                    legendgroup='actual/combined',
                    hovertemplate='actual / combined (%{x|%b %d}): %{y:$,.2f}<extra></extra>',
                ))
                fig.add_trace(go.Scatter(
                    x=combined_rev['dt'], y=combined_rev['total_ma'], name='actual 7MA / combined',
                    mode='lines', line=dict(color=COLORS['combined'], width=2),
                    legendgroup='actual/combined',
                    hovertemplate='7MA / combined (%{x|%b %d}): %{y:$,.2f}<extra></extra>',
                ))

        fig.update_layout(
            title_text=f'{y_label} — {title}',
            height=600, width=1400, margin=dict(t=60, b=30), hovermode='closest',
        )
        fig.show()

    if 'dau'      in show: _daily_fig('dau',      'dau',           'DAU Forecast')
    if 'installs' in show: _daily_fig('installs', 'new_installs',  'New Installs per Day')
    if 'revenue'  in show: _daily_fig('revenue',  'total_revenue', 'Daily Revenue (IAP + Ad)', fmt='$,.2f')
    if 'payers'   in show: _daily_fig('payers',   'payer_dau',     'Payer DAU')

    if 'monthly' in show:
        fig2 = go.Figure()
        for i, (name, df) in enumerate(dfs.items()):
            comb = df[df['platform'] == 'combined'].copy()
            comb['month'] = comb['date'].dt.to_period('M').astype(str)
            monthly = comb.groupby('month')['total_revenue'].sum().reset_index()
            fig2.add_trace(go.Bar(
                x=monthly['month'], y=monthly['total_revenue'],
                name=name, marker_color=SCENARIO_COLORS[i % len(SCENARIO_COLORS)],
                hovertemplate=f'{name}: $%{{y:,.0f}}<extra></extra>',
            ))
        fig2.update_layout(
            title_text=f'Monthly Total Revenue — {title}', barmode='group',
            height=400, margin=dict(t=60, b=30), hovermode='closest',
            yaxis=dict(tickprefix='$', tickformat=',.0f'), xaxis_title='Month',
        )
        fig2.show()


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
