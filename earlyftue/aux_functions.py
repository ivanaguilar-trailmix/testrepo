import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots


def compute_weighted_progression(data, measure_col, dimension_cols=['install_dt', 'days_since_install'], min_bucket_size=50):
    """
    Compute weighted average progression metric for a given measure across dimensions.

    Parameters:
    -----------
    data : pd.DataFrame
        Source data containing user_id, the measure column, and dimension columns
    measure_col : str
        Column name to compute weighted average for (e.g., 'max_level', 'max_gameday')
    dimension_cols : list
        Dimensions to group by (default: ['install_dt', 'days_since_install'])
    min_bucket_size : int
        Minimum users per bucket to include (default: 50)

    Returns:
    --------
    pd.DataFrame
        Aggregated data with weighted average and cohort user counts
    """

    # Step 1: Count unique users per dimension + measure bucket
    agg = data.groupby(dimension_cols + [measure_col]).agg(
        unique_users=('user_id', 'nunique')
    ).reset_index()

    # Step 2: Total unique users per dimension combination
    dimension_total_users = data.groupby(dimension_cols).agg(
        total_unique_users=('user_id', 'nunique')
    ).reset_index()

    # Step 3: Merge and compute percentage share
    agg = agg.merge(dimension_total_users, on=dimension_cols)
    agg['percentage_of_users'] = agg['unique_users'] / agg['total_unique_users']

    # Step 4: Compute weighted average
    weighted_avg = agg.groupby(dimension_cols, group_keys=False).apply(
        lambda x: (x[measure_col] * x['unique_users']).sum() / x['unique_users'].sum(),
        include_groups=False
    ).reset_index()

    weighted_avg.columns = dimension_cols + [f'weighted_avg_{measure_col}']
    agg = agg.merge(weighted_avg, on=dimension_cols)

    # Step 5: Drop small buckets
    agg = agg[agg['unique_users'] >= min_bucket_size]

    # Step 6: Collapse to one row per dimension combination
    agg = agg.groupby(dimension_cols).agg(
        cohort_users=('unique_users', 'sum'),
        **{f'weighted_avg_{measure_col}': ('weighted_avg_' + measure_col, 'first')}
    ).reset_index()

    return agg


def weighted_quantiles(group, quantiles=[0.1, 0.5, 0.9], measure_col='max_level'):
    """Return P10 / P50 / P90 of measure_col, using user counts as weights.

    Sorts by the measure, accumulates weights, then uses searchsorted to find
    the value at each quantile threshold — equivalent to a weighted percentile.
    """
    levels = group[measure_col].values
    weights = group['users'].values
    sorted_idx = np.argsort(levels)
    levels, weights = levels[sorted_idx], weights[sorted_idx]
    cum_weights = np.cumsum(weights)
    total = cum_weights[-1]
    result = {}
    for q in quantiles:
        idx = np.searchsorted(cum_weights, q * total)
        result[f'p{int(q * 100)}'] = levels[min(idx, len(levels) - 1)]
    return pd.Series(result)


def add_event_annotations(fig, events_config, x_col='max_level', ftue_col='FTUE_flag', data=None, show_annotations=True):
    if not show_annotations:
        return fig

    subplot_axes = set()
    all_y_vals = []

    for trace in fig.data:
        xaxis = getattr(trace, 'xaxis', None) or 'x'
        yaxis = getattr(trace, 'yaxis', None) or 'y'
        subplot_axes.add((xaxis, yaxis))
        if trace.y is not None:
            for v in trace.y:
                try:
                    val = float(v)
                    if not np.isnan(val):
                        all_y_vals.append(val)
                except (TypeError, ValueError):
                    pass

    global_y_min = min(all_y_vals) if all_y_vals else 0.0
    global_y_max = max(all_y_vals) if all_y_vals else 1.0
    y_bottom = min(0.0, global_y_min)
    y_top = global_y_max + abs(global_y_max - y_bottom) * 0.05

    ftue_flags_in_data = set()
    if data is not None and ftue_col in data.columns:
        ftue_flags_in_data = set(data[ftue_col].unique())

    for ftue_type, events in events_config.items():
        if ftue_flags_in_data and ftue_type not in ftue_flags_in_data:
            continue

        for event in events:
            for (xaxis, yaxis) in subplot_axes:
                if ftue_type == 'B.new':
                    text_list = ['', event['name']]
                    text_pos = 'top center'
                else:
                    text_list = [event['name'], '']
                    text_pos = 'top center'
                fig.add_trace(go.Scatter(
                    x=[event['level'], event['level']],
                    y=[y_bottom, y_top],
                    mode='lines+text',
                    line=dict(color=event['color'], dash='dash', width=1),
                    opacity=0.7,
                    legendgroup=ftue_type,
                    showlegend=False,
                    text=text_list,
                    textposition=text_pos,
                    xaxis=xaxis,
                    yaxis=yaxis,
                    hoverinfo='skip',
                ))

    return fig


def add_median_lines(fig, level_pcts_by_group, x_col='p50_max_level', ftue_col='FTUE_flag', platform_col='platform'):
    """Add vertical dotted lines at median (P50) values to a plotly figure."""
    platforms = sorted(level_pcts_by_group[platform_col].unique())
    platform_to_xaxis = {platform: f"x{i+1}" if i > 0 else "x" for i, platform in enumerate(platforms)}

    color_map = {'A.old': 'blue', 'B.new': 'red'}

    medians = level_pcts_by_group[['FTUE_flag', 'platform', 'p50_max_level']].drop_duplicates()

    for _, row in medians.iterrows():
        ftue_flag = row['FTUE_flag']
        platform = row['platform']
        median_val = row['p50_max_level']
        xaxis = platform_to_xaxis.get(platform, 'x')

        fig.add_shape(
            type="line",
            x0=median_val, x1=median_val,
            y0=0, y1=1,
            yref="paper",
            xref=xaxis,
            line=dict(color=color_map.get(ftue_flag, 'gray'), dash="dot", width=2),
            opacity=0.7,
        )

    return fig


def plot_percentile_comparison(level_pcts_by_group, percentile='all'):
    """
    Compare A.old vs B.new level percentiles by days since install.

    percentile: 'p10', 'p50', 'p90' — single metric line chart + B.new-minus-A.old diff bar
                'all'               — band chart with P10-P90 shaded region and P50 median line
    """
    df = level_pcts_by_group.sort_values('days_since_install')
    platforms = sorted(df['platform'].unique())

    COLORS = {
        'A.old': {'solid': 'rgba(59,130,246,1)',  'band': 'rgba(59,130,246,0.15)'},
        'B.new': {'solid': 'rgba(239,68,68,1)',   'band': 'rgba(239,68,68,0.15)'},
    }

    if percentile == 'all':
        fig = make_subplots(
            rows=1, cols=len(platforms),
            subplot_titles=platforms,
            shared_yaxes=True,
        )

        for col_idx, platform in enumerate(platforms, 1):
            pdf = df[df['platform'] == platform]
            show_legend = col_idx == 1

            for ftue_flag, c in COLORS.items():
                gdf = pdf[pdf['FTUE_flag'] == ftue_flag].sort_values('days_since_install')

                # P10 — invisible, anchor for fill
                fig.add_trace(go.Scatter(
                    x=gdf['days_since_install'], y=gdf['p10_max_level'],
                    mode='lines', line=dict(width=0),
                    showlegend=False, legendgroup=ftue_flag,
                    hovertemplate='P10: %{y:.0f}<extra></extra>',
                ), row=1, col=col_idx)

                # P90 — fills down to P10
                fig.add_trace(go.Scatter(
                    x=gdf['days_since_install'], y=gdf['p90_max_level'],
                    mode='lines', line=dict(width=0),
                    fill='tonexty', fillcolor=c['band'],
                    name=f'{ftue_flag} P10–P90', legendgroup=ftue_flag,
                    showlegend=show_legend,
                    hovertemplate='P90: %{y:.0f}<extra></extra>',
                ), row=1, col=col_idx)

                # P50 — solid median line
                fig.add_trace(go.Scatter(
                    x=gdf['days_since_install'], y=gdf['p50_max_level'],
                    mode='lines+markers', line=dict(width=2.5, color=c['solid']),
                    name=f'{ftue_flag} P50 (median)', legendgroup=ftue_flag,
                    showlegend=show_legend,
                    hovertemplate='P50: %{y:.0f}<extra></extra>',
                ), row=1, col=col_idx)

        fig.update_layout(
            title='Player level P10 / P50 / P90 by days since install: A.old vs B.new',
            width=1200, height=500,
        )
        fig.update_xaxes(title_text='Days since install')
        fig.update_yaxes(title_text='Max level', col=1)
        fig.show()

    else:
        col = f'{percentile}_max_level'

        pivot = df.pivot_table(
            index=['days_since_install', 'platform'],
            columns='FTUE_flag',
            values=col,
        ).reset_index()
        pivot['diff'] = pivot['B.new'] - pivot['A.old']
        pivot['pct_diff'] = (pivot['diff'] / pivot['A.old'] * 100).round(1)

        fig1 = px.line(
            df, x='days_since_install', y=col,
            color='FTUE_flag', facet_col='platform',
            markers=True,
            color_discrete_map={'A.old': 'rgba(59,130,246,1)', 'B.new': 'rgba(239,68,68,1)'},
            title=f'{percentile.upper()} Level Progression: A.old vs B.new',
            width=1200, height=500,
            labels={'days_since_install': 'Days since install', col: 'Max level'},
        )
        fig1.show()

        fig2 = px.bar(
            pivot, x='days_since_install', y='diff',
            color='platform', facet_row='platform', barmode='group',
            title=f'{percentile.upper()} Level Difference (B.new − A.old)',
            width=1200, height=600,
            hover_data={'diff': ':.2f', 'pct_diff': True, 'A.old': True, 'B.new': True},
            labels={'diff': 'Level difference', 'days_since_install': 'Days since install'},
        )
        fig2.show()
