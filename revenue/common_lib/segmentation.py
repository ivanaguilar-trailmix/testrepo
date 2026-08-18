"""
Helpers for the payer/engagement segment movement analysis in segmentation.ipynb.

Purpose, in plain terms: users sit in a "tier" (e.g. a value or frequency segment) that
can go up or down over time. These functions figure out when someone moved tiers, roll
those individual moves up into daily totals, and draw the standard charts for looking at
that movement over time.
"""
import re
import numpy as np
import plotly.express as px


def add_order_prefix(df, columns):
    """
    Segment labels are just text (e.g. "$10-$19"), so sorting them alphabetically doesn't
    put them in the right tier order. This stamps a rank number in front of each label
    (e.g. "0_...", "1_...") so a plain sort lands in the correct low-to-high tier order.
    """
    for col in columns:
        uniques = df[col].dropna().unique().tolist()

        def sort_key(label):
            # Order by the first number in the label (e.g. 10 from "$10-$19"); labels
            # with no number at all (rare) are pushed to the end.
            match = re.search(r'-?\d+\.?\d*', str(label))
            return (0, float(match.group())) if match else (1, str(label))

        ordered = sorted(uniques, key=sort_key)
        width = len(str(len(ordered) - 1))
        prefix_map = {label: f'{i:0{width}d}_{label}' for i, label in enumerate(ordered)}
        df[col] = df[col].map(prefix_map)

    return df


def add_vlines_to_figure(fig, vlines_config):
    """
    Draws a vertical dashed line + label on a chart for each event passed in (e.g. a
    feature release or an issue date), so charts can be annotated with "what happened when"
    markers alongside the data.
    """
    for vline in vlines_config:
        fig.add_vline(
            x=vline['x'],
            line_dash=vline.get('line_dash', 'dash'),
            line_color=vline.get('line_color', 'gray'),
            line_width=vline.get('line_width', 1),
            annotation_text=vline['annotation_text'],
            annotation_position='top right',
            annotation_font_size=8,
            annotation_textangle=-90,  # vertical label
        )
    return fig


def add_segment_transition_columns(df, segment_col, date_col='dt', user_col='user_id'):
    """
    For every user/day row, works out what tier that user was in last time we saw them,
    how big a jump that was (up or down, and by how many tiers), and how long they had
    been sitting in the tier they just left. A user's very first row has nothing to
    compare against, so those four new columns come back empty (NaN) for it.

    Note: "last time we saw them" means their previous *active* day on record, not
    literally yesterday — a quiet stretch with no activity is not treated as a tier
    change, since tier membership carries over through inactive gaps.
    """
    df = df.sort_values([user_col, date_col]).reset_index(drop=True).copy()

    prev = df.groupby(user_col)[segment_col].shift(1)
    df[f'{segment_col}_prev'] = prev

    # Tiers are ranked by their own natural sort order, not parsed from the label text —
    # label formatting isn't consistent enough (e.g. "$10-$19" vs "lapsed_payer") to parse
    # reliably, and rank order works the same way for every segment column.
    rank_map = {v: i for i, v in enumerate(sorted(df[segment_col].dropna().unique()))}
    df[f'{segment_col}_jump_size'] = df[segment_col].map(rank_map) - prev.map(rank_map)

    # Group consecutive active days into "streaks" of unchanged tier, so we can measure
    # how long a streak lasted right up to the point it ends in a jump.
    is_new_streak = (df[segment_col] != prev) | prev.isna()
    df['_streak_id'] = is_new_streak.groupby(df[user_col]).cumsum()

    streak_stats = df.groupby([user_col, '_streak_id'], sort=False).agg(
        _streak_start_dt=(date_col, 'min'),
        _streak_active_days=(segment_col, 'size'),
    ).reset_index()
    streak_stats['_prev_streak_start_dt'] = streak_stats.groupby(user_col)['_streak_start_dt'].shift(1)
    streak_stats['_prev_streak_active_days'] = streak_stats.groupby(user_col)['_streak_active_days'].shift(1)

    df = df.merge(
        streak_stats[[user_col, '_streak_id', '_prev_streak_start_dt', '_prev_streak_active_days']],
        on=[user_col, '_streak_id'], how='left',
    )

    # Duration of the streak that just ended, recorded only on the row where the jump
    # happens (0 everywhere else) — active_days counts active-day rows in that streak,
    # real_days is the calendar span from the streak's first day to the jump.
    is_shift_row = is_new_streak & prev.notna()
    df[f'{segment_col}_shift_active_days'] = 0.0
    df[f'{segment_col}_shift_real_days'] = 0.0
    df.loc[is_shift_row, f'{segment_col}_shift_active_days'] = df.loc[is_shift_row, '_prev_streak_active_days']
    df.loc[is_shift_row, f'{segment_col}_shift_real_days'] = (
        df.loc[is_shift_row, date_col] - df.loc[is_shift_row, '_prev_streak_start_dt']
    ).dt.days

    # First-ever row for a user has no prior state to compare to — leave it blank (NaN)
    # rather than 0, since 0 is reserved for "no jump happened".
    is_first_row = prev.isna()
    df.loc[is_first_row, f'{segment_col}_shift_active_days'] = np.nan
    df.loc[is_first_row, f'{segment_col}_shift_real_days'] = np.nan

    return df.drop(columns=['_streak_id', '_prev_streak_start_dt', '_prev_streak_active_days'])


def _add_direction_metrics(agg, segment_col, upgrades_count_col, downgrades_count_col, total_users_day, prefix=''):
    """
    Adds the share/ratio/balance columns (upgrades_share, downgrades_share, share_ratio,
    net_share, balance_index, the two directional ratios, log_ratio_u_to_d, winner,
    x_times_bigger) for one direction of movement onto an agg dataframe that already has
    that direction's upgrades/downgrades counts. Used twice by get_segment_movements_agg —
    once for inflow (prefix='', arrivals into the tier), once for outflow (prefix='outflow_',
    departures from the tier) — so the edge-tier/continuity-correction handling is written
    once and can't drift between the two directions.
    """
    def col(name):
        return f'{segment_col}_{prefix}{name}'

    upgrades_share_col = col('upgrades_share')
    downgrades_share_col = col('downgrades_share')

    # Both shares are reported as positive magnitudes (the downgrades count is negative so
    # it plots below the x-axis; the share is deliberately flipped back to positive here).
    agg[upgrades_share_col] = agg[upgrades_count_col] / total_users_day
    agg[downgrades_share_col] = agg[downgrades_count_col] * -1 / total_users_day

    # net_share / balance_index use the day's proportional shares directly — well-defined
    # everywhere, including the edge tiers (e.g. inflow's lowest tier can only ever be
    # reached by a downgrade, so it's always exactly -1 there; outflow's lowest tier can
    # only ever be left via an upgrade, so it's always exactly +1). No correction needed.
    up_share = agg[upgrades_share_col].astype(float)
    down_share = agg[downgrades_share_col].astype(float)
    agg[col('net_share')] = up_share - down_share
    agg[col('balance_index')] = (up_share - down_share) / (up_share + down_share).replace(0, np.nan)

    # No movement at all this direction, this day, this tier (only possible since inflow
    # and outflow are merged together — a tier can have one side with zero rows) — the
    # ratio-family metrics below are meaningless here (there's nothing to compare), so
    # leave them blank rather than showing a misleading "tied" reading.
    has_movement = (agg[upgrades_count_col].astype(float) != 0) | (agg[downgrades_count_col].astype(float) != 0)

    # The remaining metrics — share_ratio, the two directional ratios, log_ratio_u_to_d,
    # x_times_bigger — all divide one side by the other, which breaks at the edge tiers:
    # the lowest tier's upgrades_count is *structurally* always 0 (never just 0 by chance),
    # and the highest tier's downgrades_count is always 0, so a plain ratio is either
    # infinite or undefined there. A continuity correction — add half a "phantom mover" to
    # both counts, the standard fix for zero-cell ratios (same idea as the Haldane-Anscombe
    # correction for odds ratios) — keeps these finite while barely nudging any day where
    # both sides already have real movement (0.5 is tiny next to typical daily counts).
    CONTINUITY = 0.5
    up = agg[upgrades_count_col].astype(float) + CONTINUITY
    down = agg[downgrades_count_col].abs().astype(float) + CONTINUITY

    agg[col('share_ratio')] = np.where(has_movement, down / up, np.nan)
    agg[col('ratio_downgrade_to_upgrade')] = np.where(has_movement, down / up, np.nan)
    agg[col('ratio_upgrade_to_downgrade')] = np.where(has_movement, up / down, np.nan)
    agg[col('log_ratio_u_to_d')] = np.where(has_movement, np.log(up / down), np.nan)

    agg[col('winner')] = np.where(
        ~has_movement, 'No Movement',
        np.where(up > down, 'Upgrades', np.where(down > up, 'Downgrades', 'Neutral'))
    )
    agg[col('x_times_bigger')] = np.where(
        ~has_movement, np.nan,
        np.where(up > down, agg[col('ratio_upgrade_to_downgrade')],
                 np.where(down > up, agg[col('ratio_downgrade_to_upgrade')], 1.0))
    )

    return agg


def get_segment_movements_agg(segment_col, df):
    """
    Takes the per-user-per-day tier data (after add_segment_transition_columns has run)
    and rolls it up into one row per day per tier, from two angles at once:
      - inflow (unprefixed columns, e.g. upgrades_count): who ARRIVED in this tier today,
        split by whether they arrived via an upgrade or a downgrade.
      - outflow (outflow_-prefixed columns, e.g. outflow_upgrades_count): who LEFT this
        tier today, split by whether they left via an upgrade or a downgrade.
      - net_flow: inflow arrivals minus outflow departures — is this tier gaining or
        losing people today, net of both directions?
    """
    prev_col = f'{segment_col}_prev'
    jump_col = f'{segment_col}_jump_size'
    active_days_col = f'{segment_col}_shift_active_days'
    real_days_col = f'{segment_col}_shift_real_days'

    upgrades_count_col = f'{segment_col}_upgrades_count'
    downgrades_count_col = f'{segment_col}_downgrades_count'
    outflow_upgrades_count_col = f'{segment_col}_outflow_upgrades_count'
    outflow_downgrades_count_col = f'{segment_col}_outflow_downgrades_count'

    # --- 1) Raw movement counts, from both ends ---
    # Movements only: a real shift happened (jump_size != 0) and there was a prior tier to
    # shift from (excludes each user's first-ever observed row). Inflow groups by the
    # destination tier (segment_col); outflow groups by the origin tier (prev_col) — same
    # rows, same up/down classification (jump_size > 0 / < 0), just a different "which
    # tier does this movement belong to".
    movements = df[
        ['user_id', 'dt', segment_col, prev_col, jump_col, active_days_col, real_days_col]
    ][(df[jump_col] != 0) & (df[prev_col].notna())]

    movements_agg = movements.groupby(['dt', segment_col]).agg(
        user_id_count=('user_id', 'nunique'),
        # count upgrades only, not downgrades (jump_size > 0)
        **{
            upgrades_count_col: (jump_col, lambda x: (x > 0).sum()),
            # reverse the sign on downgrades so charts can show them below the x-axis, not above
            downgrades_count_col: (jump_col, lambda x: (x < 0).sum() * -1),
            f'{segment_col}_jump_size_mean': (jump_col, 'mean'),
            f'{segment_col}_shift_active_days_mean': (active_days_col, 'mean'),
            f'{segment_col}_shift_real_days_mean': (real_days_col, 'mean'),
        },
    ).reset_index()

    movements_agg['balance'] = movements_agg[upgrades_count_col] + movements_agg[downgrades_count_col]
    movements_agg['ratio'] = movements_agg[downgrades_count_col] / movements_agg[upgrades_count_col].replace(0, np.nan)

    outflow_agg = movements.groupby(['dt', prev_col]).agg(
        outflow_user_id_count=('user_id', 'nunique'),
        **{
            outflow_upgrades_count_col: (jump_col, lambda x: (x > 0).sum()),
            outflow_downgrades_count_col: (jump_col, lambda x: (x < 0).sum() * -1),
        },
    ).reset_index().rename(columns={prev_col: segment_col})

    # Outer merge: a tier can have inflow with zero outflow that day, or vice versa — a
    # missing count means "zero movement that side", not missing data.
    movements_agg = movements_agg.merge(outflow_agg, on=['dt', segment_col], how='outer')
    count_cols = [
        'user_id_count', upgrades_count_col, downgrades_count_col,
        'outflow_user_id_count', outflow_upgrades_count_col, outflow_downgrades_count_col,
    ]
    movements_agg[count_cols] = movements_agg[count_cols].fillna(0)

    # --- 2) Daily shares ---
    # Total unique movers represented per day across this segment's tiers. Every movement
    # is exactly one inflow (into its destination) and one outflow (out of its origin), so
    # this total is the same whether you sum inflow or outflow counts across tiers — one
    # shared denominator for both directions' shares below.
    movements_agg['total_users_day'] = (
        movements_agg.groupby('dt')['user_id_count'].transform('sum')
    )

    # --- 3) Flow/balance metrics, computed identically for each direction ---
    movements_agg = _add_direction_metrics(
        movements_agg, segment_col, upgrades_count_col, downgrades_count_col,
        movements_agg['total_users_day'],
    )
    movements_agg = _add_direction_metrics(
        movements_agg, segment_col, outflow_upgrades_count_col, outflow_downgrades_count_col,
        movements_agg['total_users_day'], prefix='outflow_',
    )

    # --- 4) Net flow ---
    # Is this tier gaining or losing people today, net of both directions? Distinct from
    # balance_index (composition of one side) — this is the actual population change.
    movements_agg[f'{segment_col}_net_flow'] = movements_agg['user_id_count'] - movements_agg['outflow_user_id_count']

    return movements_agg


# One-line-per-segment metric charts available through the `metrics` argument of
# plot_movement_charts — each is a column suffix produced by get_segment_movements_agg.
# Add an entry here to get a sensible title/y-axis label/reference-line for a metric not
# listed; metrics without an entry still work, just with a generic title/label and no
# reference line.
METRIC_CHART_SPECS = {
    #'share_ratio': {'title': 'Movement share ratio by {label}', 'hline': 1},
    'balance_index': {'title': 'Balance index by {label}', 'y_label': 'Balance Index', 'hline': 0},
    'net_share': {'title': 'Net share (upgrades minus downgrades) by {label}', 'y_label': 'Net Share', 'hline': 0},
    'x_times_bigger': {'title': 'Winning side size multiple by {label}', 'y_label': 'Times Bigger (Winning Side)', 'hline': 1},
    'ratio_upgrade_to_downgrade': {'title': 'Upgrade-to-downgrade ratio by {label}', 'y_label': 'Upgrade ÷ Downgrade Ratio', 'hline': 1},
    'ratio_downgrade_to_upgrade': {'title': 'Downgrade-to-upgrade ratio by {label}', 'y_label': 'Downgrade ÷ Upgrade Ratio', 'hline': 1},
    'log_ratio_u_to_d': {'title': 'Log ratio of upgrades to downgrades by {label}', 'y_label': 'Log Ratio (Upgrades vs Downgrades)', 'hline': 0},
    'net_flow': {'title': 'Net flow (inflow minus outflow) by {label}', 'y_label': 'Net Flow (Users)', 'hline': 0},
    'outflow_balance_index': {'title': 'Outflow balance index by {label}', 'y_label': 'Outflow Balance Index', 'hline': 0},
    'outflow_x_times_bigger': {'title': 'Outflow winning side size multiple by {label}', 'y_label': 'Outflow Times Bigger', 'hline': 1},
}


def plot_movement_charts(
    df,
    segment_col,
    vlines_events=None,
    arpdau_df=None,
    title_prefix=None,
    overlay_arpdau=True,
    show_counts=True,
    show_shares=True,
    show_outflow_counts=False,
    show_outflow_shares=False,
    metrics=('share_ratio', 'balance_index'),
    width=1400,
    height=600,
):
    """
    Draws the standard set of charts for one segment's movements: how many people moved
    (counts), what share of users that represents (shares), plus one chart per entry in
    `metrics` — any column suffix produced by get_segment_movements_agg, e.g.
    'share_ratio', 'balance_index', or 'x_times_bigger' (see METRIC_CHART_SPECS for the
    known ones, their reference lines, and y-axis labels). show_counts/show_shares plot the
    inflow (arrivals) side; show_outflow_counts/show_outflow_shares are their departures-side
    equivalents (off by default — turn on for the segments you actually want the outflow
    view of, since it doubles the chart count). Pass metrics=[] to skip the single-metric
    charts entirely. ARPDAU can optionally be overlaid on a second axis for context on every
    chart drawn. width/height apply to every chart drawn by this call.

    df must be the output of get_segment_movements_agg for this segment_col.
    """
    upgrades_count_col = f'{segment_col}_upgrades_count'
    downgrades_count_col = f'{segment_col}_downgrades_count'
    upgrades_share_col = f'{segment_col}_upgrades_share'
    downgrades_share_col = f'{segment_col}_downgrades_share'
    outflow_upgrades_count_col = f'{segment_col}_outflow_upgrades_count'
    outflow_downgrades_count_col = f'{segment_col}_outflow_downgrades_count'
    outflow_upgrades_share_col = f'{segment_col}_outflow_upgrades_share'
    outflow_downgrades_share_col = f'{segment_col}_outflow_downgrades_share'
    label = title_prefix or segment_col.replace('_', ' ').title()

    def _add_vlines(fig):
        return add_vlines_to_figure(fig, vlines_events) if vlines_events else fig

    def _overlay_arpdau(fig):
        fig.add_scatter(
            x=arpdau_df['dt'],
            y=arpdau_df['arpdau'],
            mode='lines',
            name='ARPDAU',
            yaxis='y2',
            line=dict(color='black', width=2),
            hovertemplate='dt=%{x}<br>ARPDAU=%{y:.4f}<extra></extra>'
        )
        fig.update_layout(
            yaxis2=dict(title='ARPDAU', overlaying='y', side='right', showgrid=False)
        )
        return fig

    # Hover is kept to one consistent set on every chart — the point's value, and the
    # inflow/outflow/total movement counts for that (day, tier) — regardless of which
    # direction the chart itself is showing, so hovering a share_ratio point or an outflow
    # count point always gives the same "how many people were actually moving" context.
    total_movers_col = f'{segment_col}_total_movers'
    df = df.copy()
    df[total_movers_col] = df['user_id_count'] + df['outflow_user_id_count']
    HOVER_LABELS = {
        'user_id_count': 'Inflow Users',
        'outflow_user_id_count': 'Outflow Users',
        total_movers_col: 'Total Users Moving',
    }

    def _melt(value_vars):
        plot_df = df.melt(
            id_vars=['dt', segment_col, 'user_id_count', 'outflow_user_id_count', total_movers_col],
            value_vars=value_vars,
            var_name='movement_type',
            value_name='count'
        )
        plot_df['movement_type'] = plot_df['movement_type'].map({
            value_vars[0]: 'U',
            value_vars[1]: 'D',
        })
        return plot_df

    def _dual_line_chart(value_vars, title, yaxis_title):
        fig = px.line(
            _melt(value_vars),
            x='dt',
            y='count',
            color='movement_type',
            line_dash=segment_col,
            hover_data={'user_id_count': True, 'outflow_user_id_count': True, total_movers_col: True},
            color_discrete_map={'U': '#2ca02c', 'D': '#d62728'},
            labels={'count': 'Value', 'movement_type': 'Movement Type', **HOVER_LABELS},
            title=title,
            width=width,
            height=height
        )
        fig.update_layout(yaxis_title=yaxis_title, legend_title_text='')
        fig = _add_vlines(fig)
        if overlay_arpdau:
            fig = _overlay_arpdau(fig)
        fig.show()

    # 1) Counts (inflow — arrivals into the tier)
    if show_counts:
        _dual_line_chart(
            [upgrades_count_col, downgrades_count_col],
            title=f'Upgrades/Downgrades by {label}', yaxis_title='Movement Count',
        )

    # 2) Shares (inflow)
    if show_shares:
        _dual_line_chart(
            [upgrades_share_col, downgrades_share_col],
            title=f'Upgrades and Downgrades share by {label}', yaxis_title='Movement Share',
        )

    # 2b) Outflow counts/shares — departures from the tier, split the same way. Off by
    # default since most segments only need the inflow view; turn on per call for the
    # segments where "what's draining this tier" is the interesting question.
    if show_outflow_counts:
        _dual_line_chart(
            [outflow_upgrades_count_col, outflow_downgrades_count_col],
            title=f'Outflow Upgrades/Downgrades by {label}', yaxis_title='Outflow Movement Count',
        )

    if show_outflow_shares:
        _dual_line_chart(
            [outflow_upgrades_share_col, outflow_downgrades_share_col],
            title=f'Outflow Upgrades and Downgrades share by {label}', yaxis_title='Outflow Movement Share',
        )

    # 3) One chart per requested single-metric column (share ratio, balance index,
    # x_times_bigger, or anything else get_segment_movements_agg produced for this segment)
    for metric in metrics:
        metric_col = f'{segment_col}_{metric}'
        spec = METRIC_CHART_SPECS.get(metric, {})
        title = spec.get('title', '{metric} by {label}').format(label=label, metric=metric)
        y_label = spec.get('y_label', metric.replace('_', ' ').title())
        hline = spec.get('hline')

        fig = px.line(
            df,
            x='dt',
            y=metric_col,
            color=segment_col,
            title=title,
            width=width,
            height=height,
            labels={metric_col: 'Value', **HOVER_LABELS},
            hover_data={'user_id_count': True, 'outflow_user_id_count': True, total_movers_col: True}
        )
        fig.update_layout(yaxis_title=y_label, legend_title_text='')
        fig = _add_vlines(fig)
        if hline is not None:
            fig.add_hline(y=hline, line_width=1, line_dash='dash', line_color='black')
        if overlay_arpdau:
            fig = _overlay_arpdau(fig)
        fig.show()
