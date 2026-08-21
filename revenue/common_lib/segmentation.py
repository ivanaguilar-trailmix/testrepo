"""
Helpers for the payer/engagement segment movement analysis in segmentation.ipynb.

Purpose, in plain terms: users sit in a "tier" (e.g. a value or frequency segment) that
can go up or down over time. These functions figure out when someone moved tiers, roll
those individual moves up into daily totals, and draw the standard charts for looking at
that movement over time.
"""
import re
import numpy as np
import pandas as pd
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


def _add_direction_metrics(agg, segment_col, upgrades_count_col, downgrades_count_col,
                            total_upgrades_day, total_downgrades_day, prefix):
    """
    Adds the share/ratio/direction columns (upgrades_share, downgrades_share,
    direction_normalized, the two directional ratios, winner) for one direction of movement
    onto an agg dataframe that already has that direction's upgrades/downgrades counts. Used
    twice by get_segment_movements_agg —
    once for inflow (prefix='inflow_', arrivals into the tier), once for outflow
    (prefix='outflow_', departures from the tier) — so the edge-tier/continuity-correction
    handling is written once and can't drift between the two directions.
    """
    def col(name):
        return f'{segment_col}_{prefix}{name}'

    upgrades_share_col = col('upgrades_share')
    downgrades_share_col = col('downgrades_share')

    # Each share is a tier's cut of that day's TOTAL FOR ITS OWN DIRECTION only (all tiers'
    # upgrades summed for total_upgrades_day, all tiers' downgrades summed for
    # total_downgrades_day) — not one denominator shared between both directions. A tier
    # that structurally only ever receives downgrades (e.g. the lowest tier, everyone's
    # eventual downgrade destination) would otherwise dominate a shared denominator and
    # dilute every other tier's upgrades_share even though it contributes zero upgrades.
    # Splitting the denominator by direction means upgrades_share sums to 1 across tiers
    # each day, and downgrades_share sums to 1 across tiers each day — two independent
    # 100% breakdowns, not one shared pie. Both shares are reported as positive magnitudes
    # (the downgrades count is negative so it plots below the x-axis; the share is
    # deliberately flipped back to positive here).
    agg[upgrades_share_col] = agg[upgrades_count_col] / total_upgrades_day
    agg[downgrades_share_col] = agg[downgrades_count_col] * -1 / total_downgrades_day

    # direction_normalized: same signed-net-count idea as {segment_col}_{prefix}direction
    # (upgrades minus downgrades, raw headcount) but scaled to [-1, 1] by this tier's own
    # total movers that (day, tier) — computed straight from counts (not from the shares
    # above) so it stays a per-tier composition measure regardless of how the shares'
    # denominators are defined. Well-defined everywhere, including the edge tiers (e.g.
    # inflow's lowest tier can only ever be reached by a downgrade, so it's always exactly
    # -1 there; outflow's lowest tier can only ever be left via an upgrade, so it's always
    # exactly +1). No correction needed.
    up_count = agg[upgrades_count_col].astype(float)
    down_count = agg[downgrades_count_col].abs().astype(float)
    agg[col('direction_normalized')] = (up_count - down_count) / (up_count + down_count).replace(0, np.nan)

    # No movement at all this direction, this day, this tier (only possible since inflow
    # and outflow are merged together — a tier can have one side with zero rows) — the
    # ratio-family metrics below are meaningless here (there's nothing to compare), so
    # leave them blank rather than showing a misleading "tied" reading.
    has_movement = (agg[upgrades_count_col].astype(float) != 0) | (agg[downgrades_count_col].astype(float) != 0)

    # The two directional ratios divide one side by the other, which breaks at the edge
    # tiers: the lowest tier's upgrades_count is *structurally* always 0 (never just 0 by
    # chance), and the highest tier's downgrades_count is always 0, so a plain ratio is
    # either infinite or undefined there. A continuity correction — add half a "phantom
    # mover" to both counts, the standard fix for zero-cell ratios (same idea as the
    # Haldane-Anscombe correction for odds ratios) — keeps these finite while barely nudging
    # any day where both sides already have real movement (0.5 is tiny next to typical daily
    # counts).
    CONTINUITY = 0.5
    up = up_count + CONTINUITY
    down = down_count + CONTINUITY

    agg[col('ratio_downgrade_to_upgrade')] = np.where(has_movement, down / up, np.nan)
    agg[col('ratio_upgrade_to_downgrade')] = np.where(has_movement, up / down, np.nan)

    agg[col('winner')] = np.where(
        ~has_movement, 'No Movement',
        np.where(up > down, 'Upgrades', np.where(down > up, 'Downgrades', 'Neutral'))
    )

    return agg


def get_segment_movements_agg(segment_col, df):
    """
    Takes the per-user-per-day tier data (after add_segment_transition_columns has run)
    and rolls it up into one row per day per tier, from two angles at once:
      - inflow (inflow_-prefixed columns, e.g. inflow_upgrades_count): who ARRIVED in this
        tier today, split by whether they arrived via an upgrade or a downgrade.
      - outflow (outflow_-prefixed columns, e.g. outflow_upgrades_count): who LEFT this
        tier today, split by whether they left via an upgrade or a downgrade.
      - net_flow: inflow arrivals minus outflow departures — is this tier gaining or
        losing people today, net of both directions?
    """
    prev_col = f'{segment_col}_prev'
    jump_col = f'{segment_col}_jump_size'
    active_days_col = f'{segment_col}_shift_active_days'
    real_days_col = f'{segment_col}_shift_real_days'

    upgrades_count_col = f'{segment_col}_inflow_upgrades_count'
    downgrades_count_col = f'{segment_col}_inflow_downgrades_count'
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
        inflow_user_id_count=('user_id', 'nunique'),
        # count upgrades only, not downgrades (jump_size > 0)
        **{
            upgrades_count_col: (jump_col, lambda x: (x > 0).sum()),
            # reverse the sign on downgrades so charts can show them below the x-axis, not above
            downgrades_count_col: (jump_col, lambda x: (x < 0).sum() * -1),
            f'{segment_col}_shift_active_days_mean': (active_days_col, 'mean'),
            f'{segment_col}_shift_real_days_mean': (real_days_col, 'mean'),
        },
    ).reset_index()

    movements_agg[f'{segment_col}_inflow_direction'] = movements_agg[upgrades_count_col] + movements_agg[downgrades_count_col]

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
        'inflow_user_id_count', upgrades_count_col, downgrades_count_col,
        'outflow_user_id_count', outflow_upgrades_count_col, outflow_downgrades_count_col,
    ]
    movements_agg[count_cols] = movements_agg[count_cols].fillna(0)

    movements_agg[f'{segment_col}_outflow_direction'] = (
        movements_agg[outflow_upgrades_count_col] + movements_agg[outflow_downgrades_count_col]
    )

    # --- 2) Daily totals ---
    # total_users_day: total unique movers represented per day across this segment's tiers,
    # kept as an informational column (context for hover, not used to compute shares below).
    # Every movement is exactly one inflow (into its destination) and one outflow (out of
    # its origin), so this total is the same whether you sum inflow or outflow counts.
    movements_agg['total_users_day'] = (
        movements_agg.groupby('dt')['inflow_user_id_count'].transform('sum')
    )

    # total_upgrades_day / total_downgrades_day: the day's total upgrade-movements and
    # total downgrade-movements, each summed across all tiers — used as the (separate)
    # share denominators below. Every upgrade movement is counted once as an inflow
    # arrival (some destination tier) and once as an outflow departure (some origin tier),
    # so summing inflow's upgrades_count_col across tiers gives the same total as summing
    # outflow's outflow_upgrades_count_col across tiers — one pair of totals, shared by
    # both directions' _add_direction_metrics calls below (same reasoning as
    # total_users_day above, just split by direction instead of combined).
    total_upgrades_day = movements_agg.groupby('dt')[upgrades_count_col].transform('sum')
    total_downgrades_day = movements_agg.groupby('dt')[downgrades_count_col].transform('sum').abs()

    # --- 3) Flow/direction metrics, computed identically for each direction ---
    movements_agg = _add_direction_metrics(
        movements_agg, segment_col, upgrades_count_col, downgrades_count_col,
        total_upgrades_day, total_downgrades_day, prefix='inflow_',
    )
    movements_agg = _add_direction_metrics(
        movements_agg, segment_col, outflow_upgrades_count_col, outflow_downgrades_count_col,
        total_upgrades_day, total_downgrades_day, prefix='outflow_',
    )

    # --- 4) Net flow ---
    # Is this tier gaining or losing people today, net of both directions? Distinct from
    # direction_normalized (composition of one side) — this is the actual population change.
    movements_agg[f'{segment_col}_net_flow'] = movements_agg['inflow_user_id_count'] - movements_agg['outflow_user_id_count']

    return movements_agg


def get_segment_population_agg(segment_col, df, movements_agg):
    """
    Total population per tier per day — everyone currently in that tier, movers and
    non-movers alike — plus that tier's share of the day's total users. Unlike
    get_segment_movements_agg (which only ever sees users whose jump_size != 0 that day),
    this counts everyone, so a tier with zero movement that day still shows up at its full
    size instead of being invisible.

    df must be the transitioned per-user-per-day frame (add_segment_transition_columns'
    output) for this segment_col — only segment_col/dt/user_id are used, so the raw
    pre-transition frame would also work, but passing the same df you already built for
    get_segment_movements_agg keeps one input to track instead of two.

    movements_agg must be get_segment_movements_agg's output for the same segment_col — its
    inflow_user_id_count/outflow_user_id_count are merged in here so every population chart
    can show the same movement context on hover.
    """
    total_col = f'{segment_col}_total_users'
    total_day_col = f'{segment_col}_total_users_day'
    share_col = f'{segment_col}_share_pct'

    population_agg = df.groupby(['dt', segment_col])['user_id'].nunique().reset_index(name=total_col)
    population_agg[total_day_col] = population_agg.groupby('dt')[total_col].transform('sum')
    population_agg[share_col] = population_agg[total_col] / population_agg[total_day_col] * 100

    population_agg = population_agg.merge(
        movements_agg[['dt', segment_col, 'inflow_user_id_count', 'outflow_user_id_count']],
        on=['dt', segment_col], how='left',
    )
    # A tier with no arrivals/departures that day has no row in movements_agg at all —
    # that's "zero movement", not missing data.
    move_cols = ['inflow_user_id_count', 'outflow_user_id_count']
    population_agg[move_cols] = population_agg[move_cols].fillna(0)

    return population_agg


def plot_segment_population_charts(
    df,
    segment_col,
    vlines_events=None,
    title_prefix=None,
    show_total=True,
    show_share=True,
    width=1400,
    height=600,
):
    """
    Draws up to two charts off get_segment_population_agg's output: total users per tier
    (raw headcount, including non-movers — show_total) and each tier's share of the day's
    total users (% — show_share). Both default on; turn either off per call. Both charts'
    hover shows inflow/outflow/total users for that (day, tier), same as
    plot_movement_charts, so a population chart and a movement chart can be read side by
    side with consistent context.

    df must be the output of get_segment_population_agg for this segment_col.
    """
    label = title_prefix or segment_col.replace('_', ' ').title()
    total_col = f'{segment_col}_total_users'
    share_col = f'{segment_col}_share_pct'

    HOVER_LABELS = {
        'inflow_user_id_count': 'Inflow Users',
        'outflow_user_id_count': 'Outflow Users',
        total_col: 'Total Users',
    }
    HOVER_DATA = {'inflow_user_id_count': True, 'outflow_user_id_count': True, total_col: True}

    def _add_vlines(fig):
        return add_vlines_to_figure(fig, vlines_events) if vlines_events else fig

    if show_total:
        fig_total = px.line(
            df, x='dt', y=total_col, color=segment_col,
            title=f'Total Users by {label}',
            labels={total_col: 'Total Users', **HOVER_LABELS},
            hover_data=HOVER_DATA,
            width=width, height=height,
        )
        fig_total.update_layout(yaxis_title='Total Users', legend_title_text='')
        fig_total = _add_vlines(fig_total)
        fig_total.show()

    if show_share:
        fig_share = px.line(
            df, x='dt', y=share_col, color=segment_col,
            title=f'Population Share (%) by {label}',
            labels={share_col: 'Share of Users (%)', **HOVER_LABELS},
            hover_data=HOVER_DATA,
            width=width, height=height,
        )
        fig_share.update_layout(yaxis_title='Share of Users (%)', legend_title_text='')
        fig_share = _add_vlines(fig_share)
        fig_share.show()


def get_segment_performance_summary(segment_col, movements_agg, population_agg, freq='M'):
    """
    Period-over-period performance summary, one row per (period, tier): average population,
    average share of the day's total population, average/net net_flow (scaled to % of that
    tier's own population, so growth is comparable across differently-sized tiers), and the
    average inflow/outflow downgrade-to-upgrade ratios. Built to answer "is a tier's trend
    changing over time" — an all-time average can hide a recent reversal a period-by-period
    breakdown would catch.

    freq is any pandas Period alias ('M' monthly, 'W' weekly, etc.) — 'M' gives a compact
    table for eyeballing, 'W' gives finer resolution for a trend line chart
    (plot_segment_performance_summary).

    movements_agg/population_agg must be get_segment_movements_agg's/
    get_segment_population_agg's output for the same segment_col. Pass pre-filtered frames
    (e.g. dropping the first 30 days) if the segment is a rolling-window one where the start
    of any query window is a known ramp artifact, not real behavior — this function doesn't
    do that filtering itself, since it isn't universal across all segment types.

    Note: the two ratio columns are structurally extreme at edge tiers (the lowest tier can
    never have upgrades-in, the highest can never have downgrades-in), so they're
    uninformative there — expect large/near-zero values that don't reflect a real trend for
    those two tiers specifically.
    """
    pop = population_agg.copy()
    mov = movements_agg.copy()
    pop['dt'] = pd.to_datetime(pop['dt'])
    mov['dt'] = pd.to_datetime(mov['dt'])
    pop['period'] = pop['dt'].dt.to_period(freq).dt.start_time
    mov['period'] = mov['dt'].dt.to_period(freq).dt.start_time

    pop_summary = pop.groupby(['period', segment_col]).agg(
        avg_population=(f'{segment_col}_total_users', 'mean'),
        avg_share_pct=(f'{segment_col}_share_pct', 'mean'),
    )
    mov_summary = mov.groupby(['period', segment_col]).agg(
        avg_net_flow=(f'{segment_col}_net_flow', 'mean'),
        total_net_flow=(f'{segment_col}_net_flow', 'sum'),
        avg_inflow_ratio_downgrade_to_upgrade=(f'{segment_col}_inflow_ratio_downgrade_to_upgrade', 'mean'),
        avg_outflow_ratio_downgrade_to_upgrade=(f'{segment_col}_outflow_ratio_downgrade_to_upgrade', 'mean'),
    )

    summary = pop_summary.join(mov_summary, how='outer').reset_index()
    summary['net_flow_pct_of_pop'] = summary['avg_net_flow'] / summary['avg_population'] * 100

    return summary


def plot_segment_performance_summary(
    summary_df,
    segment_col,
    metrics=('net_flow_pct_of_pop', 'avg_inflow_ratio_downgrade_to_upgrade', 'avg_outflow_ratio_downgrade_to_upgrade'),
    title_prefix=None,
    width=1400,
    height=500,
):
    """
    Draws one trend-line chart per entry in `metrics` — period on the x-axis, one line per
    tier — off get_segment_performance_summary's output. Meant for spotting whether a tier's
    story (growth rate, inflow/outflow composition) is changing period over period, not just
    what it averaged over the whole window.

    summary_df must be the output of get_segment_performance_summary for this segment_col.
    """
    label = title_prefix or segment_col.replace('_', ' ').title()
    NICE_LABELS = {
        'net_flow_pct_of_pop': 'Net Flow (% of Tier Population)',
        'avg_inflow_ratio_downgrade_to_upgrade': 'Inflow Ratio (Downgrade ÷ Upgrade)',
        'avg_outflow_ratio_downgrade_to_upgrade': 'Outflow Ratio (Downgrade ÷ Upgrade)',
    }

    for metric in metrics:
        y_label = NICE_LABELS.get(metric, metric.replace('_', ' ').title())
        fig = px.line(
            summary_df, x='period', y=metric, color=segment_col,
            title=f'{y_label} by period, by {label}',
            markers=True,
            labels={metric: y_label},
            width=width, height=height,
        )
        fig.update_layout(yaxis_title=y_label, legend_title_text='')
        if metric in ('avg_inflow_ratio_downgrade_to_upgrade', 'avg_outflow_ratio_downgrade_to_upgrade'):
            fig.add_hline(y=1, line_width=1, line_dash='dash', line_color='black')
        elif metric == 'net_flow_pct_of_pop':
            fig.add_hline(y=0, line_width=1, line_dash='dash', line_color='black')
        fig.show()


# One-line-per-segment metric charts available through the `metrics` argument of
# plot_movement_charts — each is a column suffix produced by get_segment_movements_agg.
# Add an entry here to get a sensible title/y-axis label/reference-line for a metric not
# listed; metrics without an entry still work, just with a generic title/label and no
# reference line.
METRIC_CHART_SPECS = {
    'inflow_direction_normalized': {'title': 'Inflow direction (normalized) by {label}', 'y_label': 'Inflow Direction (Normalized)', 'hline': 0},
    'inflow_ratio_upgrade_to_downgrade': {'title': 'Inflow upgrade-to-downgrade ratio by {label}', 'y_label': 'Inflow Upgrade ÷ Downgrade Ratio', 'hline': 1},
    'inflow_ratio_downgrade_to_upgrade': {'title': 'Inflow downgrade-to-upgrade ratio by {label}', 'y_label': 'Inflow Downgrade ÷ Upgrade Ratio', 'hline': 1},
    'net_flow': {'title': 'Net flow (inflow minus outflow) by {label}', 'y_label': 'Net Flow (Users)', 'hline': 0},
    'outflow_direction_normalized': {'title': 'Outflow direction (normalized) by {label}', 'y_label': 'Outflow Direction (Normalized)', 'hline': 0},
    'outflow_ratio_upgrade_to_downgrade': {'title': 'Outflow upgrade-to-downgrade ratio by {label}', 'y_label': 'Outflow Upgrade ÷ Downgrade Ratio', 'hline': 1},
    'outflow_ratio_downgrade_to_upgrade': {'title': 'Outflow downgrade-to-upgrade ratio by {label}', 'y_label': 'Outflow Downgrade ÷ Upgrade Ratio', 'hline': 1},
}


# Dual-line (upgrades vs downgrades) charts available through the `metrics` argument of
# plot_movement_charts, alongside the single-line METRIC_CHART_SPECS entries above.
DUAL_LINE_CHART_SPECS = {
    'inflow_counts': {
        'upgrades_col': 'inflow_upgrades_count', 'downgrades_col': 'inflow_downgrades_count',
        'title': 'Inflow Upgrades/Downgrades by {label}', 'y_label': 'Inflow Movement Count',
    },
    'inflow_shares': {
        'upgrades_col': 'inflow_upgrades_share', 'downgrades_col': 'inflow_downgrades_share',
        'title': 'Inflow Upgrades and Downgrades share by {label}', 'y_label': 'Inflow Movement Share',
    },
    'outflow_counts': {
        'upgrades_col': 'outflow_upgrades_count', 'downgrades_col': 'outflow_downgrades_count',
        'title': 'Outflow Upgrades/Downgrades by {label}', 'y_label': 'Outflow Movement Count',
    },
    'outflow_shares': {
        'upgrades_col': 'outflow_upgrades_share', 'downgrades_col': 'outflow_downgrades_share',
        'title': 'Outflow Upgrades and Downgrades share by {label}', 'y_label': 'Outflow Movement Share',
    },
}


def plot_movement_charts(
    df,
    segment_col,
    vlines_events=None,
    arpdau_df=None,
    title_prefix=None,
    overlay_arpdau=True,
    metrics=('inflow_counts', 'inflow_shares', 'inflow_ratio_downgrade_to_upgrade', 'inflow_direction_normalized'),
    width=1400,
    height=600,
):
    """
    Draws one chart per entry in `metrics`. Most entries are a column suffix produced by
    get_segment_movements_agg — a single line per tier, e.g. 'inflow_ratio_downgrade_to_upgrade',
    'inflow_direction_normalized', or 'outflow_direction' (see METRIC_CHART_SPECS for the
    known ones, their reference lines, and y-axis labels). Four entries are special dual-line
    charts instead of a single column: 'inflow_counts'/'inflow_shares' (upgrades vs downgrades,
    arrivals side) and 'outflow_counts'/'outflow_shares' (their departures-side equivalents —
    see DUAL_LINE_CHART_SPECS). Pass metrics=[] to skip charts entirely. ARPDAU can optionally
    be overlaid on a second axis for context on every chart drawn. width/height apply to every
    chart drawn by this call.

    df must be the output of get_segment_movements_agg for this segment_col.
    """
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
    # direction the chart itself is showing, so hovering a ratio point or an outflow
    # count point always gives the same "how many people were actually moving" context.
    total_movers_col = f'{segment_col}_total_movers'
    df = df.copy()
    df[total_movers_col] = df['inflow_user_id_count'] + df['outflow_user_id_count']
    HOVER_LABELS = {
        'inflow_user_id_count': 'Inflow Users',
        'outflow_user_id_count': 'Outflow Users',
        total_movers_col: 'Total Users Moving',
    }

    def _melt(value_vars):
        plot_df = df.melt(
            id_vars=['dt', segment_col, 'inflow_user_id_count', 'outflow_user_id_count', total_movers_col],
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
            hover_data={'inflow_user_id_count': True, 'outflow_user_id_count': True, total_movers_col: True},
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

    # One chart per requested entry in `metrics` — either a dual-line upgrades/downgrades
    # chart (the four DUAL_LINE_CHART_SPECS keys) or a single-metric column suffix (a
    # directional ratio, direction_normalized, or anything else get_segment_movements_agg
    # produced for this segment).
    for metric in metrics:
        dual_spec = DUAL_LINE_CHART_SPECS.get(metric)
        if dual_spec is not None:
            _dual_line_chart(
                [f'{segment_col}_{dual_spec["upgrades_col"]}', f'{segment_col}_{dual_spec["downgrades_col"]}'],
                title=dual_spec['title'].format(label=label), yaxis_title=dual_spec['y_label'],
            )
            continue

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
            hover_data={'inflow_user_id_count': True, 'outflow_user_id_count': True, total_movers_col: True}
        )
        fig.update_layout(yaxis_title=y_label, legend_title_text='')
        fig = _add_vlines(fig)
        if hline is not None:
            fig.add_hline(y=hline, line_width=1, line_dash='dash', line_color='black')
        if overlay_arpdau:
            fig = _overlay_arpdau(fig)
        fig.show()
