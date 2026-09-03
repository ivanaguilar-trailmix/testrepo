"""
Aggregation and plotting helpers for AB test metric comparisons.

Copied from testrepo/abtestmetrics/common_lib/metrics.py (aggregate_metric,
aggregate_ratio_metric, plot_metric and their private helpers only - the
aggregate_segment_metric/aggregate_segment_ratio_metric pair, which reads from the
pre-aggregated ab_dt_segment_metrics table, isn't used here) so this notebook doesn't reach
across into a sibling project's folder at runtime. Keep in sync by hand if the source changes -
there's no shared package between the two projects.

Local addition not in the original: an optional test_end_date marker (see _add_date_markers/
plot_metric) - this notebook's LiveOps event has a fixed end date worth marking on every chart,
which abtestmetrics' own tests don't need since their tests don't have a hard stop.
"""
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Fixed variant->color mapping, rather than relying on px's per-figure automatic assignment -
# _plot_paired_row combines two independently-built subplots into one row and needs both to
# agree on which color means Test vs control, since only one of them keeps a legend.
# Deliberately two blue-green tones rather than red/blue (or red/green) - neither should read as
# "good" or "bad" on its own, that judgment belongs to the diff charts, not the variant identity.
# Same hue family but a lightness gap so the two lines stay visually distinct even where they
# cross. Muted/desaturated per Stephen Few's color guidance (Practical Rules for Using Color in
# Charts, perceptualedge.com) - soft colors for the bulk of the data, saving bright/dark saturated
# colors for whatever specifically needs to grab attention, which neither variant line is.
_VARIANT_COLORS = {'Test': '#2E5E67', 'control': '#6FA98C'}

# norm.ppf(0.975), i.e. the two-tailed 95% z-score - hardcoded rather than pulling in scipy for
# one constant.
_Z_95 = 1.959963984540054

# Neutral soft gray for every CI band, rather than a translucent version of the variant's own
# line color - a same-hue tint read as too close to the line itself to register as a distinct
# "uncertainty" layer. Gray reads as uncertainty regardless of which variant it's shading.
_CI_BAND_COLOR = 'rgba(120,120,120,0.15)'

# plotly_white: white/light plot area, thin gray gridlines, no colored chart-junk background.
# Deliberately light rather than dark - Stephen Few's dashboard-design guidance is explicit that
# analytical displays meant for sustained, precise reading should use a light (white/off-white)
# background, not a dark one; dark backgrounds read as "look-at-me" presentation displays, not
# quantitative-analysis tools.
_PLOTLY_TEMPLATE = 'plotly_white'

# Off-white rather than stark #ffffff, so the chart still reads as a distinct panel against
# whatever surrounds it (notebook cell, VS Code, etc.) without going dark to get that separation.
_CHART_BG_COLOR = '#f7f7f5'


def _add_value_ci(data_metrics_agg: pd.DataFrame, metric_name: str) -> pd.DataFrame:
    """
    Attach a 95% CI (ci_lower_{metric_name}/ci_upper_{metric_name}) for metric_name's own
    per-dt/variant value: metric_name +/- z * sd_{metric_name}/sqrt(n_unique_players).

    This assumes metric_name is itself a mean (aggregate_metric's agg_func='mean') - a CI on a
    sum or other non-mean aggregate wouldn't use this formula.
    """
    se = data_metrics_agg[f'sd_{metric_name}'] / np.sqrt(data_metrics_agg['n_unique_players'])
    data_metrics_agg[f'ci_lower_{metric_name}'] = data_metrics_agg[metric_name] - _Z_95 * se
    data_metrics_agg[f'ci_upper_{metric_name}'] = data_metrics_agg[metric_name] + _Z_95 * se
    return data_metrics_agg


def _attach_test_control_diffs(data_metrics_agg: pd.DataFrame, metric_name: str) -> pd.DataFrame:
    """
    Given a dt/variant-grain frame with a metric_name value column, attach the Test-vs-control
    absolute and relative differences (abs_diff, rel_diff_pct) to the Test rows. Control rows
    carry 0 for both diff columns so they plot as a flat baseline.

    Also attaches a 95% CI for rel_diff_pct (rel_diff_ci_lower/rel_diff_ci_upper), but only when
    the input carries sd_{metric_name} and n_unique_players - the standard-error-of-the-
    difference formula (combine both variants' SE: sqrt(sd_test^2/n_test + sd_control^2/n_control),
    then divide by the control mean to land on the same % scale as rel_diff_pct). Silently skipped
    for callers without those columns - e.g. aggregate_ratio_metric, matching production's own
    choice not to put a CI on ratio metrics.
    """
    metric_pivot = data_metrics_agg.pivot(index='dt', columns='variant', values=metric_name)
    metric_pivot['abs_diff'] = (metric_pivot['Test'] - metric_pivot['control']).abs()
    metric_pivot['rel_diff_pct'] = ((metric_pivot['Test'] - metric_pivot['control']) / metric_pivot['control'] * 100).round(2)

    diff_cols = ['abs_diff', 'rel_diff_pct']

    sd_col = f'sd_{metric_name}'
    has_ci_inputs = sd_col in data_metrics_agg.columns and 'n_unique_players' in data_metrics_agg.columns
    if has_ci_inputs:
        sd_pivot = data_metrics_agg.pivot(index='dt', columns='variant', values=sd_col)
        n_pivot = data_metrics_agg.pivot(index='dt', columns='variant', values='n_unique_players')
        se_absolute = np.sqrt(sd_pivot['Test'] ** 2 / n_pivot['Test'] + sd_pivot['control'] ** 2 / n_pivot['control'])
        se_relative_pct = se_absolute / metric_pivot['control'] * 100
        metric_pivot['rel_diff_ci_lower'] = (metric_pivot['rel_diff_pct'] - _Z_95 * se_relative_pct).round(2)
        metric_pivot['rel_diff_ci_upper'] = (metric_pivot['rel_diff_pct'] + _Z_95 * se_relative_pct).round(2)
        diff_cols += ['rel_diff_ci_lower', 'rel_diff_ci_upper']

    data_metrics_agg = data_metrics_agg.merge(
        metric_pivot[diff_cols],
        left_on='dt',
        right_index=True,
        how='left'
    )

    # The diffs above are Test-vs-control, so they only apply to the Test row.
    # Control stays flat at 0 rather than duplicating Test's diff.
    data_metrics_agg.loc[data_metrics_agg['variant'] == 'control', diff_cols] = 0

    # px.line draws points in row order, not sorted by x - groupby (aggregate_metric's path)
    # sorts its keys for free, but a caller reading straight from BigQuery (no ORDER BY
    # guaranteed) won't be sorted, and would draw a zigzagging line. Sort defensively here so
    # every caller gets a plottable frame regardless of what order its input arrived in.
    return data_metrics_agg.sort_values(['dt', 'variant']).reset_index(drop=True)


def _total_users_assigned(assignment_data: pd.DataFrame, dts) -> pd.DataFrame:
    """
    Cumulative count of distinct assigned users per variant, evaluated as of each dt - a
    fixed-cohort-as-of-that-date denominator, not that day's active-user count. Needed so
    cumulative charts can be normalized consistently.

    assignment_data is one row per user with user_id/variant/assigned_dt - independent of
    activity, unlike data_metrics, so a user assigned but never active still counts.
    """
    per_user = assignment_data[['user_id', 'variant', 'assigned_dt']].drop_duplicates()
    daily_new = (
        per_user.groupby(['variant', 'assigned_dt']).size().rename('n').reset_index()
        .sort_values(['variant', 'assigned_dt'])
    )
    daily_new['assigned_dt'] = pd.to_datetime(daily_new['assigned_dt'])
    daily_new['total_users_assigned'] = daily_new.groupby('variant')['n'].cumsum()

    unique_dts = pd.DataFrame({'dt': pd.to_datetime(pd.Series(dts)).unique()}).sort_values('dt')

    totals = []
    for variant, group in daily_new.groupby('variant'):
        merged = pd.merge_asof(
            unique_dts,
            group[['assigned_dt', 'total_users_assigned']].rename(columns={'assigned_dt': 'dt'}),
            on='dt',
            direction='backward',
        )
        merged['variant'] = variant
        totals.append(merged)

    return pd.concat(totals, ignore_index=True)


def aggregate_metric(data_metrics: pd.DataFrame, metric_name: str, agg_func, assignment_data: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate metric_name by date and variant, and attach the Test-vs-control
    absolute and relative differences (abs_diff, rel_diff_pct) to the Test rows.
    Control rows carry 0 for both diff columns so they plot as a flat baseline.

    assignment_data (one row per user with variant/assigned_dt) is used to attach
    total_users_assigned, the fixed-cohort denominator plot_metric's cumulative charts need -
    see _total_users_assigned. It doesn't affect the non-cumulative value/rel charts at all.

    Also computes sd_{metric_name} (sample stddev of the raw per-user values, same groupby as
    the mean/sum above - no separate pass over the data) so _add_value_ci/_attach_test_control_diffs
    can attach CIs. Only meaningful when agg_func='mean'.
    """
    data_metrics_agg = data_metrics.groupby(['dt', 'variant']).agg(
        **{metric_name: (metric_name, agg_func), f'sum_{metric_name}': (metric_name, 'sum'), f'sd_{metric_name}': (metric_name, 'std')},
        n_unique_players=('user_id', 'nunique'),
        n_unique_payers=('user_id', lambda x: data_metrics.loc[x.index][data_metrics.loc[x.index, 'iap_rev'] > 0]['user_id'].nunique())
    ).reset_index()

    data_metrics_agg = data_metrics_agg[data_metrics_agg['variant'] != 'NotAssigned']
    data_metrics_agg = _add_value_ci(data_metrics_agg, metric_name)

    data_metrics_agg['dt'] = pd.to_datetime(data_metrics_agg['dt'])
    totals = _total_users_assigned(assignment_data, data_metrics_agg['dt'])
    data_metrics_agg = data_metrics_agg.merge(totals, on=['dt', 'variant'], how='left')

    return _attach_test_control_diffs(data_metrics_agg, metric_name)


def aggregate_ratio_metric(data_metrics: pd.DataFrame, numerator: str, denominator: str, ratio_name: str) -> pd.DataFrame:
    """
    Aggregate a ratio metric (e.g. energy_velocity = energy_spent / energy_earned) by date and
    variant, and attach the Test-vs-control diffs, same shape as aggregate_metric().

    Sums numerator and denominator separately per dt/variant, then divides - rather than
    averaging a per-user ratio, which would weight every user's ratio equally regardless of how
    much they actually spent/earned.
    """
    data_metrics_agg = data_metrics.groupby(['dt', 'variant']).agg(
        **{numerator: (numerator, 'sum'), denominator: (denominator, 'sum')},
        n_unique_players=('user_id', 'nunique'),
        n_unique_payers=('user_id', lambda x: data_metrics.loc[x.index][data_metrics.loc[x.index, 'iap_rev'] > 0]['user_id'].nunique())
    ).reset_index()

    data_metrics_agg = data_metrics_agg[data_metrics_agg['variant'] != 'NotAssigned']

    data_metrics_agg[ratio_name] = np.where(
        data_metrics_agg[denominator].notna() & (data_metrics_agg[denominator] != 0),
        data_metrics_agg[numerator] / data_metrics_agg[denominator],
        np.nan
    )
    data_metrics_agg = data_metrics_agg.drop(columns=[numerator, denominator])

    return _attach_test_control_diffs(data_metrics_agg, ratio_name)


def _build_cumulative_value(data_metrics_agg: pd.DataFrame, metric_name: str, test_start_date: str) -> pd.DataFrame:
    """
    Each variant's own running total of metric_name since test_start_date (not a cross-variant
    diff - see _build_cumulative_diff for that). Keeps n_unique_players/n_unique_payers so the
    chart can still show them on hover.

    cumulative_{metric_name} = cumsum(sum_{metric_name}) / total_users_assigned - a
    fixed-assigned-cohort denominator, not that day's active-user count.
    """
    since_start = data_metrics_agg[pd.to_datetime(data_metrics_agg['dt']) >= pd.Timestamp(test_start_date)].copy()
    since_start = since_start.sort_values('dt')
    since_start[f'cumulative_{metric_name}'] = (
        since_start.groupby('variant')[f'sum_{metric_name}'].cumsum() / since_start['total_users_assigned']
    )
    return since_start


def _build_cumulative_diff(data_metrics_agg: pd.DataFrame, metric_name: str, test_start_date: str) -> pd.DataFrame:
    """
    Cumulative Test-vs-control difference since test_start_date, in the same dt/variant shape
    as aggregate_metric's abs_diff/rel_diff_pct: the real value on Test rows, 0 on control rows,
    so control plots as a flat baseline.

    Unlike aggregate_metric's abs_diff (which takes .abs() of each day's difference), this is
    SIGNED - cumulative_test - cumulative_control - so the chart shows whether the accumulated
    effect is trending positive or negative over the test, not just its day-to-day magnitude.
    """
    cumulative_col = f'cumulative_{metric_name}'
    cumulative_value = _build_cumulative_value(data_metrics_agg, metric_name, test_start_date)
    pivot = cumulative_value.pivot(index='dt', columns='variant', values=cumulative_col).sort_index()

    cumulative_abs_diff = pivot['Test'] - pivot['control']
    cumulative_rel_diff_pct = (cumulative_abs_diff / pivot['control'] * 100).round(2)

    cumulative = cumulative_value[['dt', 'variant']].drop_duplicates().merge(
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
    # order follows cumulative_value's original order, not the sorted one - px.line draws points
    # in row order, so an unsorted frame here draws a zigzagging line even with correct values.
    return cumulative.sort_values(['dt', 'variant']).reset_index(drop=True)


def _get_overall_diff(data_metrics_agg: pd.DataFrame, metric_name: str, test_start_date: str):
    """
    Total Test-vs-control diff over the whole period since test_start_date. Derived from
    _build_cumulative_value/_build_cumulative_diff's LAST rows rather than summed independently,
    so this is guaranteed to match the cumulative charts' final points exactly.
    Returns (total_test, total_control, overall_abs_diff, overall_rel_diff_pct).
    """
    cumulative_col = f'cumulative_{metric_name}'
    cumulative_value = _build_cumulative_value(data_metrics_agg, metric_name, test_start_date)
    last_control_row = cumulative_value[cumulative_value['variant'] == 'control'].sort_values('dt').iloc[-1]
    total_control = last_control_row[cumulative_col]

    cumulative = _build_cumulative_diff(data_metrics_agg, metric_name, test_start_date)
    last_test_row = cumulative[cumulative['variant'] == 'Test'].sort_values('dt').iloc[-1]
    overall_abs_diff = last_test_row['cumulative_abs_diff']
    overall_rel_diff_pct = last_test_row['cumulative_rel_diff_pct']

    # Derived from total_control + the already-computed diff, not summed independently, so it
    # can't drift out of sync with overall_abs_diff.
    total_test = total_control + overall_abs_diff

    return total_test, total_control, overall_abs_diff, overall_rel_diff_pct


def _add_date_markers(fig, test_start_date: str = None, assignment_start_date: str = None, test_end_date: str = None, row: int = None, col: int = None) -> None:
    """
    Add vlines for the dates that matter to an AB test read: when randomization began
    (assignment_start_date), when the treatment itself actually started (test_start_date), and -
    local addition, not in the upstream abtestmetrics version - when the test/event actually
    ends (test_end_date). Each gets its own color so they stay distinguishable when more than
    one lands on the same chart.

    row/col target a specific subplot on a make_subplots figure (see _plot_paired_row); left
    unset (None) for a plain single-chart figure, matching add_vline's own default.
    """
    subplot_kwargs = {} if row is None else {'row': row, 'col': col}

    if assignment_start_date is not None:
        fig.add_vline(
            x=pd.Timestamp(assignment_start_date).timestamp() * 1000,
            line_dash='dash',
            line_color='blue',
            annotation_text='Assignment Start',
            annotation_position='top left',
            **subplot_kwargs)

    if test_start_date is not None:
        fig.add_vline(
            x=pd.Timestamp(test_start_date).timestamp() * 1000,
            line_dash='dash',
            line_color='red',
            annotation_text='Test Start',
            annotation_position='top right',
            **subplot_kwargs)

    if test_end_date is not None:
        fig.add_vline(
            x=pd.Timestamp(test_end_date).timestamp() * 1000,
            line_dash='dot',
            line_color='#555555',
            annotation_text='Test End',
            annotation_position='bottom right',
            **subplot_kwargs)


def _line_traces(df: pd.DataFrame, y_col: str, y_format: str, hover_cols: list, show_legend: bool) -> list:
    """
    One go.Scatter per variant for y_col vs dt, colored via the fixed _VARIANT_COLORS map so
    color coding stays consistent across the two subplots _plot_paired_row combines into a row.
    """
    traces = []
    for variant, group in df.sort_values('dt').groupby('variant'):
        hovertemplate = f'Variant={variant}<br>Date=%{{x}}<br>{y_col}=%{{y:{y_format}}}'
        customdata = None
        if hover_cols:
            customdata = group[hover_cols].to_numpy()
            for i, col in enumerate(hover_cols):
                hovertemplate += f'<br>{col}=%{{customdata[{i}]}}'
        hovertemplate += '<extra></extra>'

        traces.append(go.Scatter(
            x=group['dt'], y=group[y_col],
            mode='lines+markers',
            name=variant,
            legendgroup=variant,
            showlegend=show_legend,
            line={'color': _VARIANT_COLORS.get(variant)},
            customdata=customdata,
            hovertemplate=hovertemplate,
        ))
    return traces


def _ci_band_traces(df: pd.DataFrame, lower_col: str, upper_col: str) -> list:
    """
    One shaded go.Scatter band per variant for [lower_col, upper_col], filled with the shared
    neutral _CI_BAND_COLOR (not a tint of the variant's own line color) so the band reads as a
    distinct "uncertainty" layer rather than blending into the line it surrounds.

    Returns [] whenever lower_col/upper_col aren't present, or a variant's slice is entirely NaN
    - callers that don't carry sd/n (e.g. ratio metrics) get no band, with no special-casing
    needed at the plot_metric call site.
    """
    if lower_col not in df.columns or upper_col not in df.columns:
        return []

    traces = []
    for variant, group in df.sort_values('dt').groupby('variant'):
        if group[[lower_col, upper_col]].isna().all().all():
            continue
        traces.append(go.Scatter(
            x=pd.concat([group['dt'], group['dt'][::-1]]),
            y=pd.concat([group[upper_col], group[lower_col][::-1]]),
            fill='toself',
            fillcolor=_CI_BAND_COLOR,
            line={'color': 'rgba(255,255,255,0)'},
            legendgroup=variant,
            showlegend=False,
            hoverinfo='skip',
            name=f'{variant} 95% CI',
        ))
    return traces


def _panel_traces(panel: dict, show_legend: bool) -> list:
    """CI band traces (if panel declares ci_cols and the data has them) drawn first, so the
    line traces from _line_traces render on top of their own band rather than under it."""
    traces = []
    ci_cols = panel.get('ci_cols')
    if ci_cols:
        traces += _ci_band_traces(panel['df'], *ci_cols)
    traces += _line_traces(panel['df'], panel['y'], panel['y_format'], panel['hover_cols'], show_legend)
    return traces


def _has_ci_data(df: pd.DataFrame, ci_cols) -> bool:
    """True only if ci_cols is a real (lower, upper) pair and the data actually carries them,
    non-null - the same condition _ci_band_traces uses to decide whether it draws a band at all,
    so a panel's y-axis only ever gets clamped when a band is actually there to clamp against."""
    if not ci_cols:
        return False
    lower_col, upper_col = ci_cols
    return (
        lower_col in df.columns and upper_col in df.columns
        and not df[[lower_col, upper_col]].isna().all().all()
    )


def _line_data_range(df: pd.DataFrame, y_col: str, pad_frac: float = 0.08) -> list:
    """
    [min, max] of y_col's own values (the line, not its CI band), with a little padding, for use
    as an explicit y-axis range. Plotly's autorange otherwise fits every trace including the CI
    band - since the band is necessarily wider than the line, that can stretch the axis enough to
    flatten out the line's own real day-to-day movement.
    """
    values = df[y_col].dropna()
    lo, hi = values.min(), values.max()
    pad = (hi - lo) * pad_frac
    if pad == 0:
        pad = max(abs(hi), 1) * pad_frac
    return [lo - pad, hi + pad]


def _plot_single(panel: dict, width: int, height: int, test_start_date: str = None, assignment_start_date: str = None, test_end_date: str = None) -> None:
    """Fallback for when only one side of a value/diff pair is toggled on - same look as
    _plot_paired_row, just built from the same panel dict shape for consistency."""
    fig = go.Figure(_panel_traces(panel, show_legend=True))
    fig.update_layout(title=panel['title'], xaxis_title='Date', yaxis_title=panel['ylabel'],
                       legend_title_text='Variant', width=width, height=height, template=_PLOTLY_TEMPLATE,
                       paper_bgcolor=_CHART_BG_COLOR, plot_bgcolor=_CHART_BG_COLOR)
    if _has_ci_data(panel['df'], panel.get('ci_cols')):
        fig.update_yaxes(range=_line_data_range(panel['df'], panel['y']))
    _add_date_markers(fig, test_start_date, assignment_start_date, test_end_date)
    fig.show()


def _plot_paired_row(left: dict, right: dict, width: int, height: int, test_start_date: str = None, assignment_start_date: str = None, test_end_date: str = None) -> None:
    """
    Combine a "value" chart and its paired "diff" chart into one row (1x2 subplots) so they
    render side by side in Jupyter instead of stacked - used for both the daily (value/rel) and
    cumulative (value_cumulative/rel_cumulative) pairs.

    Only the left subplot's traces carry a legend entry - both sides share the same fixed
    variant->color mapping (_VARIANT_COLORS), so one legend already describes both.
    """
    fig = make_subplots(rows=1, cols=2, subplot_titles=(left['title'], right['title']))

    for trace in _panel_traces(left, show_legend=True):
        fig.add_trace(trace, row=1, col=1)
    for trace in _panel_traces(right, show_legend=False):
        fig.add_trace(trace, row=1, col=2)

    fig.update_xaxes(title_text='Date', row=1, col=1)
    fig.update_xaxes(title_text='Date', row=1, col=2)
    fig.update_yaxes(title_text=left['ylabel'], row=1, col=1)
    fig.update_yaxes(title_text=right['ylabel'], row=1, col=2)

    if _has_ci_data(left['df'], left.get('ci_cols')):
        fig.update_yaxes(range=_line_data_range(left['df'], left['y']), row=1, col=1)
    if _has_ci_data(right['df'], right.get('ci_cols')):
        fig.update_yaxes(range=_line_data_range(right['df'], right['y']), row=1, col=2)

    _add_date_markers(fig, test_start_date, assignment_start_date, test_end_date, row=1, col=1)
    _add_date_markers(fig, test_start_date, assignment_start_date, test_end_date, row=1, col=2)

    fig.update_layout(width=width, height=height, legend_title_text='Variant', template=_PLOTLY_TEMPLATE,
                       paper_bgcolor=_CHART_BG_COLOR, plot_bgcolor=_CHART_BG_COLOR)
    fig.show()


def _plot_pair(show_left: bool, left: dict, show_right: bool, right: dict, width: int, height: int, test_start_date: str = None, assignment_start_date: str = None, test_end_date: str = None) -> None:
    """Dispatch to the side-by-side row when both halves of a pair are on, or the single-chart
    fallback when only one is - keeps show_value/show_rel (and their cumulative equivalents)
    independently toggleable, as plot_metric has always allowed."""
    if show_left and show_right:
        _plot_paired_row(left, right, width, height, test_start_date, assignment_start_date, test_end_date)
    elif show_left:
        _plot_single(left, width, height, test_start_date, assignment_start_date, test_end_date)
    elif show_right:
        _plot_single(right, width, height, test_start_date, assignment_start_date, test_end_date)


def plot_metric(data_metrics_agg: pd.DataFrame, metric_name: str, show_overall_diff: bool = True, show_value: bool = True, show_rel: bool = True, show_value_cumulative: bool = True, show_rel_cumulative: bool = True, test_start_date: str = None, assignment_start_date: str = None, test_end_date: str = None, width: int = 1200, height: int = 600) -> None:
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
    marks when randomization began (blue line), if it's earlier; test_end_date marks when the
    test/event ends (dotted gray line) - all three are drawn on show_value/show_rel, but only
    test_start_date matters for where the cumulative charts start accumulating.

    show_overall_diff and the two cumulative charts need test_start_date to know where to start
    accumulating and are silently skipped if it isn't provided.

    show_value/show_rel draw a shaded 95% CI band (neutral gray, see _CI_BAND_COLOR) whenever
    data_metrics_agg carries the ci_lower_{metric_name}/ci_upper_{metric_name} and
    rel_diff_ci_lower/rel_diff_ci_upper columns aggregate_metric attaches - see
    _attach_test_control_diffs. Ratio metrics (aggregate_ratio_metric) don't carry those columns,
    so their charts render without a band. The two cumulative charts never get a CI band -
    propagating a valid one through a cumulative sum over a growing cohort denominator isn't the
    same formula as the daily case and hasn't been built.
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
        # Matches _CHART_BG_COLOR/the other charts' light background.
        fig_tile.add_shape(
            type='rect',
            x0=0, y0=0, x1=1, y1=1,
            xref='paper', yref='paper',
            line={'color': '#d0d0d0', 'width': 1},
            fillcolor=_CHART_BG_COLOR,
            layer='below',
        )
        fig_tile.update_layout(
            template=_PLOTLY_TEMPLATE,
            width=tile_width,
            height=tile_height,
            paper_bgcolor=_CHART_BG_COLOR,
            font={'color': '#1a1a1a'},
            margin={'l': 30, 'r': 30, 't': 60, 'b': 20},
        )
        fig_tile.show()

    if show_value or show_rel:
        value_panel = {
            'df': data_metrics_agg, 'y': metric_name, 'y_format': '.4f',
            'title': f'{metric_name} by Variant', 'ylabel': metric_name,
            'hover_cols': ['n_unique_players', 'n_unique_payers'],
            'ci_cols': (f'ci_lower_{metric_name}', f'ci_upper_{metric_name}'),
        }
        rel_panel = {
            'df': data_metrics_agg, 'y': 'rel_diff_pct', 'y_format': '.2f',
            'title': f'{metric_name} - Relative Difference (%) by Variant', 'ylabel': 'Relative Difference (%)',
            'hover_cols': ['n_unique_players', 'n_unique_payers'],
            'ci_cols': ('rel_diff_ci_lower', 'rel_diff_ci_upper'),
        }
        _plot_pair(show_value, value_panel, show_rel, rel_panel, width, height, test_start_date, assignment_start_date, test_end_date)

    if show_value_cumulative or show_rel_cumulative:
        cumulative_col = f'cumulative_{metric_name}'
        cumulative_value_panel = {
            'df': _build_cumulative_value(data_metrics_agg, metric_name, test_start_date),
            'y': cumulative_col, 'y_format': '.4f',
            'title': f'{metric_name} - Cumulative Value by Variant (since Test Start)',
            'ylabel': f'Cumulative {metric_name}',
            'hover_cols': ['n_unique_players', 'n_unique_payers'],
        }
        cumulative_rel_panel = {
            'df': _build_cumulative_diff(data_metrics_agg, metric_name, test_start_date),
            'y': 'cumulative_rel_diff_pct', 'y_format': '.2f',
            'title': f'{metric_name} - Cumulative Relative Difference (%) by Variant (since Test Start)',
            'ylabel': 'Cumulative Relative Difference (%)',
            'hover_cols': [],
        }
        # assignment_start_date is never drawn here (only on the daily pair above) - it would
        # fall outside the plotted range, which starts at test_start_date.
        _plot_pair(show_value_cumulative, cumulative_value_panel, show_rel_cumulative, cumulative_rel_panel, width, height, test_start_date, test_end_date=test_end_date)
