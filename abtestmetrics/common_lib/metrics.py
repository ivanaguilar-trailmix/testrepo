"""
Aggregation and plotting helpers for AB test metric comparisons.
"""
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


def _attach_test_control_diffs(data_metrics_agg: pd.DataFrame, metric_name: str) -> pd.DataFrame:
    """
    Given a dt/variant-grain frame with a metric_name value column, attach the Test-vs-control
    absolute and relative differences (abs_diff, rel_diff_pct) to the Test rows. Control rows
    carry 0 for both diff columns so they plot as a flat baseline.

    Shared by aggregate_metric (raw per-user pipeline) and aggregate_segment_metric (the
    pre-aggregated ab_dt_segment_metrics table) so the two paths can't compute this differently.
    """
    metric_pivot = data_metrics_agg.pivot(index='dt', columns='variant', values=metric_name)
    metric_pivot['abs_diff'] = (metric_pivot['Test'] - metric_pivot['control']).abs()
    metric_pivot['rel_diff_pct'] = ((metric_pivot['Test'] - metric_pivot['control']) / metric_pivot['control'] * 100).round(2)

    data_metrics_agg = data_metrics_agg.merge(
        metric_pivot[['abs_diff', 'rel_diff_pct']],
        left_on='dt',
        right_index=True,
        how='left'
    )

    # The diffs above are Test-vs-control, so they only apply to the Test row.
    # Control stays flat at 0 rather than duplicating Test's diff.
    data_metrics_agg.loc[data_metrics_agg['variant'] == 'control', ['abs_diff', 'rel_diff_pct']] = 0

    # px.line draws points in row order, not sorted by x - groupby (aggregate_metric's path)
    # sorts its keys for free, but a caller reading straight from BigQuery (no ORDER BY
    # guaranteed) won't be sorted, and would draw a zigzagging line. Sort defensively here so
    # every caller gets a plottable frame regardless of what order its input arrived in.
    return data_metrics_agg.sort_values(['dt', 'variant']).reset_index(drop=True)


def aggregate_metric(data_metrics: pd.DataFrame, metric_name: str, agg_func) -> pd.DataFrame:
    """
    Aggregate metric_name by date and variant, and attach the Test-vs-control
    absolute and relative differences (abs_diff, rel_diff_pct) to the Test rows.
    Control rows carry 0 for both diff columns so they plot as a flat baseline.
    """
    data_metrics_agg = data_metrics.groupby(['dt', 'variant']).agg(
        **{metric_name: (metric_name, agg_func)},
        n_unique_players=('user_id', 'nunique'),
        n_unique_payers=('user_id', lambda x: data_metrics.loc[x.index][data_metrics.loc[x.index, 'iap_rev'] > 0]['user_id'].nunique())
    ).reset_index()

    data_metrics_agg = data_metrics_agg[data_metrics_agg['variant'] != 'NotAssigned']

    return _attach_test_control_diffs(data_metrics_agg, metric_name)


def aggregate_segment_metric(data_segment_metrics: pd.DataFrame, metric_name: str) -> pd.DataFrame:
    """
    Build the same dt/variant-grain shape aggregate_metric() produces, but from the
    pre-aggregated ab_dt_segment_metrics table (sql/ab_dt_segment_metrics.sql) instead of raw
    per-user rows - this is the STATIC snapshot as of that table's last scheduled refresh,
    vs aggregate_metric()'s LIVE pull which always reflects late-arriving events up to now.

    avg_active_metric already *is* what aggregate_metric(..., agg_func='mean') computes (mean
    of metric_name among active users - see the dbt model's own prep_for_pivot/UNPIVOT), so no
    further aggregation is needed here, just a reshape.

    n_unique_payers isn't available at this pre-aggregated grain (no payer-count column in the
    source table), so it's NaN here rather than approximated from a different column.

    The source table also carries a pre-assignment baseline window (dt before anyone's
    assigned_dt, kept there for randomization-bias checking) where avg_active_metric is a real
    value but dau_assigned is 0 by construction - a different population than the actual
    Test/control comparison, and not something aggregate_metric()'s own pipeline ever shows
    (its dt < assigned_dt rows get routed to 'NotAssigned' and dropped). Dropped here the same
    way, so both paths cover the same date range.
    """
    df = data_segment_metrics[data_segment_metrics['metric'] == metric_name]

    data_metrics_agg = df.rename(columns={
        'avg_active_metric': metric_name,
        'dau_assigned': 'n_unique_players',
    })[['dt', 'variant', metric_name, 'n_unique_players']].copy()
    data_metrics_agg = data_metrics_agg[data_metrics_agg['n_unique_players'] > 0]
    data_metrics_agg['n_unique_payers'] = float('nan')

    return _attach_test_control_diffs(data_metrics_agg, metric_name)


def _build_cumulative_value(data_metrics_agg: pd.DataFrame, metric_name: str, test_start_date: str) -> pd.DataFrame:
    """
    Each variant's own running total of metric_name since test_start_date (not a cross-variant
    diff - see _build_cumulative_diff for that). Keeps n_unique_players/n_unique_payers so the
    chart can still show them on hover.
    """
    since_start = data_metrics_agg[pd.to_datetime(data_metrics_agg['dt']) >= pd.Timestamp(test_start_date)].copy()
    since_start = since_start.sort_values('dt')
    since_start[f'cumulative_{metric_name}'] = since_start.groupby('variant')[metric_name].cumsum()
    return since_start


def _build_cumulative_diff(data_metrics_agg: pd.DataFrame, metric_name: str, test_start_date: str) -> pd.DataFrame:
    """
    Cumulative Test-vs-control difference since test_start_date, in the same dt/variant shape
    as aggregate_metric's abs_diff/rel_diff_pct: the real value on Test rows, 0 on control rows,
    so control plots as a flat baseline.

    Unlike aggregate_metric's abs_diff (which takes .abs() of each day's difference), this is
    SIGNED - cumsum(Test) - cumsum(control) - so the chart shows whether the accumulated effect
    is trending positive or negative over the test, not just its day-to-day magnitude.
    """
    since_start = data_metrics_agg[pd.to_datetime(data_metrics_agg['dt']) >= pd.Timestamp(test_start_date)].copy()
    pivot = since_start.pivot(index='dt', columns='variant', values=metric_name).sort_index()

    cumulative_test = pivot['Test'].cumsum()
    cumulative_control = pivot['control'].cumsum()
    cumulative_abs_diff = cumulative_test - cumulative_control
    cumulative_rel_diff_pct = (cumulative_abs_diff / cumulative_control * 100).round(2)

    cumulative = since_start[['dt', 'variant']].drop_duplicates().merge(
        pd.DataFrame({
            'dt': pivot.index,
            'cumulative_abs_diff': cumulative_abs_diff.values,
            'cumulative_rel_diff_pct': cumulative_rel_diff_pct.values,
        }),
        on='dt',
        how='left',
    )
    cumulative.loc[cumulative['variant'] == 'control', ['cumulative_abs_diff', 'cumulative_rel_diff_pct']] = 0

    # The values above are computed correctly (via the sorted pivot), but this merge's row
    # order follows since_start's original order, not the sorted one - px.line draws points in
    # row order, so an unsorted frame here draws a zigzagging line even with correct values.
    return cumulative.sort_values(['dt', 'variant']).reset_index(drop=True)


def _get_overall_diff(data_metrics_agg: pd.DataFrame, metric_name: str, test_start_date: str):
    """
    Total Test-vs-control diff over the whole period since test_start_date. Derived from
    _build_cumulative_diff's LAST row rather than summed independently, so this is guaranteed
    to match the cumulative relative-diff chart's final point exactly, not just approximately.
    Returns (total_test, total_control, overall_abs_diff, overall_rel_diff_pct).
    """
    since_start = data_metrics_agg[pd.to_datetime(data_metrics_agg['dt']) >= pd.Timestamp(test_start_date)]
    total_control = since_start.loc[since_start['variant'] == 'control', metric_name].sum()

    cumulative = _build_cumulative_diff(data_metrics_agg, metric_name, test_start_date)
    last_test_row = cumulative[cumulative['variant'] == 'Test'].sort_values('dt').iloc[-1]
    overall_abs_diff = last_test_row['cumulative_abs_diff']
    overall_rel_diff_pct = last_test_row['cumulative_rel_diff_pct']

    # Derived from total_control + the already-computed diff, not summed independently, so it
    # can't drift out of sync with overall_abs_diff.
    total_test = total_control + overall_abs_diff

    return total_test, total_control, overall_abs_diff, overall_rel_diff_pct


def _add_date_markers(fig, test_start_date: str = None, assignment_start_date: str = None) -> None:
    """
    Add vlines for the two dates that matter to an AB test read: when randomization began
    (assignment_start_date) and when the treatment itself actually started (test_start_date).
    They're commonly different - see [[project_abtestmetrics]] - so they get distinct colors.
    """
    if assignment_start_date is not None:
        fig.add_vline(
            x=pd.Timestamp(assignment_start_date).timestamp() * 1000,
            line_dash='dash',
            line_color='blue',
            annotation_text='Assignment Start',
            annotation_position='top left')

    if test_start_date is not None:
        fig.add_vline(
            x=pd.Timestamp(test_start_date).timestamp() * 1000,
            line_dash='dash',
            line_color='red',
            annotation_text='Test Start',
            annotation_position='top right')


def plot_metric(data_metrics_agg: pd.DataFrame, metric_name: str, show_overall_diff: bool = True, show_value: bool = True, show_rel: bool = True, show_value_cumulative: bool = True, show_rel_cumulative: bool = True, test_start_date: str = None, assignment_start_date: str = None, width: int = 1200, height: int = 600) -> None:
    """Plot metric_name by variant. Five independently-toggleable pieces:
    show_overall_diff - a single KPI tile: total Test vs total control since test_start_date
    show_value - the metric's daily value by variant
    show_rel - daily relative (%) difference, Test vs control
    show_value_cumulative - each variant's own running total since test_start_date
    show_rel_cumulative - cumulative relative (%) difference since test_start_date

    show_overall_diff's rel_diff_pct is always identical to the last point of the
    show_rel_cumulative chart - both derive from the same cumulative sums (see
    _get_overall_diff), so they can't disagree.

    test_start_date marks when the treatment itself started (red line); assignment_start_date
    marks when randomization began (blue line), if it's earlier. Only show_value/show_rel plot
    dates before test_start_date, so assignment_start_date is only drawn on those two - it would
    fall outside the plotted range on the cumulative charts, which start at test_start_date.

    show_overall_diff and the two cumulative charts need test_start_date to know where to start
    accumulating and are silently skipped if it isn't provided.
    """
    if test_start_date is None:
        show_overall_diff = False
        show_value_cumulative = False
        show_rel_cumulative = False

    if show_overall_diff:
        total_test, total_control, overall_abs_diff, overall_rel_diff_pct = _get_overall_diff(data_metrics_agg, metric_name, test_start_date)

        tile_width = max(width // 3, 340)
        tile_height = max(height // 3, 220)

        fig_tile = go.Figure(go.Indicator(
            mode='number+delta',
            value=total_test,
            number={'valueformat': ',.3f', 'font': {'size': 40}},
            delta={'reference': total_control, 'relative': True, 'valueformat': '.2%', 'font': {'size': 20}},
            title={'text': f'{metric_name}<br>Test vs Control (since Test Start)', 'font': {'size': 14}},
            domain={'x': [0, 1], 'y': [0, 1]},
        ))
        # A visible bordered card, rather than text floating on whatever background the
        # notebook theme happens to use - Indicator traces have no background of their own.
        fig_tile.add_shape(
            type='rect',
            x0=0, y0=0, x1=1, y1=1,
            xref='paper', yref='paper',
            line={'color': '#d0d0d0', 'width': 1},
            fillcolor='white',
            layer='below',
        )
        fig_tile.update_layout(
            width=tile_width,
            height=tile_height,
            paper_bgcolor='white',
            font={'color': '#1a1a1a'},
            margin={'l': 30, 'r': 30, 't': 60, 'b': 20},
        )
        fig_tile.show()

    if show_value:
        fig = px.line(data_metrics_agg, x='dt', y=metric_name, color='variant',
                      title=f'{metric_name} by Variant',
                      labels={'dt': 'Date', metric_name: metric_name, 'variant': 'Variant'},
                      markers=True,
                      width=width,
                      height=height,
                      hover_data=['n_unique_players', 'n_unique_payers'])

        _add_date_markers(fig, test_start_date, assignment_start_date)

        fig.show()

    if show_rel:
        fig_rel = px.line(data_metrics_agg, x='dt', y='rel_diff_pct', color='variant',
                           title=f'{metric_name} - Relative Difference (%) by Variant',
                           labels={'dt': 'Date', 'rel_diff_pct': 'Relative Difference (%)', 'variant': 'Variant'},
                           markers=True,
                           width=width,
                           height=height,
                           hover_data=['n_unique_players', 'n_unique_payers'])

        _add_date_markers(fig_rel, test_start_date, assignment_start_date)

        fig_rel.show()

    if show_value_cumulative:
        cumulative_value = _build_cumulative_value(data_metrics_agg, metric_name, test_start_date)
        cumulative_col = f'cumulative_{metric_name}'

        fig_value_cum = px.line(cumulative_value, x='dt', y=cumulative_col, color='variant',
                                 title=f'{metric_name} - Cumulative Value by Variant (since Test Start)',
                                 labels={'dt': 'Date', cumulative_col: f'Cumulative {metric_name}', 'variant': 'Variant'},
                                 markers=True,
                                 width=width,
                                 height=height,
                                 hover_data=['n_unique_players', 'n_unique_payers'])

        _add_date_markers(fig_value_cum, test_start_date)

        fig_value_cum.show()

    if show_rel_cumulative:
        cumulative_diff = _build_cumulative_diff(data_metrics_agg, metric_name, test_start_date)

        fig_rel_cum = px.line(cumulative_diff, x='dt', y='cumulative_rel_diff_pct', color='variant',
                               title=f'{metric_name} - Cumulative Relative Difference (%) by Variant (since Test Start)',
                               labels={'dt': 'Date', 'cumulative_rel_diff_pct': 'Cumulative Relative Difference (%)', 'variant': 'Variant'},
                               markers=True,
                               width=width,
                               height=height)

        _add_date_markers(fig_rel_cum, test_start_date)

        fig_rel_cum.show()
