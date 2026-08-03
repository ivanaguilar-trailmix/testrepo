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
            width=1500, height=600,
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


def compute_retention_significance(df, alpha=0.05, power=0.80):
    """
    Augment a retention dataframe with Wilson CIs and two-proportion z-test results.
    Each (dx, platform) pair is tested independently using its own cohort sizes.

    Added columns
    -------------
    ci_low, ci_high   Wilson 95% CI for each row's retention_rate
    z_stat, p_value   Pooled two-proportion z-test vs the other FTUE group
    sig_label         '***' p<0.001 / '**' p<0.01 / '*' p<0.05 / 'NS'
    n_needed          Min users per group to detect the observed effect (equal-size, 80% power)
    """
    from scipy import stats

    df = df.copy()

    # Wilson CI (vectorised)
    z_crit = stats.norm.ppf(1 - alpha / 2)
    n = df['cohort_size'].to_numpy(float)
    p = df['retention_rate'].to_numpy(float)
    denom = 1 + z_crit**2 / n
    center = (p + z_crit**2 / (2 * n)) / denom
    margin = z_crit * np.sqrt(p * (1 - p) / n + z_crit**2 / (4 * n**2)) / denom
    df['ci_low'] = np.clip(center - margin, 0, 1)
    df['ci_high'] = np.clip(center + margin, 0, 1)

    # Two-proportion z-test per (dx, platform)
    ftue_groups = sorted(df['FTUE_flag'].unique())
    group_a, group_b = ftue_groups[0], ftue_groups[1]

    z_alpha = stats.norm.ppf(1 - alpha / 2)
    z_beta = stats.norm.ppf(power)
    sig_rows = []

    for (dx, platform), grp in df.groupby(['dx', 'platform']):
        a = grp[grp['FTUE_flag'] == group_a]
        b = grp[grp['FTUE_flag'] == group_b]
        if a.empty or b.empty:
            continue

        n_a = float(a['cohort_size'].iloc[0])
        k_a = float(a['retained_size'].iloc[0])
        n_b = float(b['cohort_size'].iloc[0])
        k_b = float(b['retained_size'].iloc[0])
        p_a, p_b = k_a / n_a, k_b / n_b

        p_pool = (k_a + k_b) / (n_a + n_b)
        se = np.sqrt(p_pool * (1 - p_pool) * (1 / n_a + 1 / n_b))
        z = (p_a - p_b) / se if se > 0 else 0.0
        p_val = float(2 * (1 - stats.norm.cdf(abs(z))))

        if p_val < 0.001:    sig = '***'
        elif p_val < 0.01:   sig = '**'
        elif p_val < alpha:  sig = '*'
        else:                sig = 'NS'

        effect = abs(p_a - p_b)
        n_needed = (
            int(np.ceil((z_alpha + z_beta)**2 * (p_a * (1 - p_a) + p_b * (1 - p_b)) / effect**2))
            if effect > 1e-6 else None
        )

        sig_rows.append({
            'dx': dx, 'platform': platform,
            'z_stat': round(z, 3), 'p_value': p_val,
            'sig_label': sig, 'n_needed': n_needed,
        })

    return df.merge(pd.DataFrame(sig_rows), on=['dx', 'platform'], how='left')


def plot_retention_significance(df, title='', alpha=0.05, power=0.80):
    """
    Grouped bar chart of retention rates with Wilson 95% CIs and per-Dx significance annotations.

    For NS checkpoints, shows how many more users per group would be required to reach significance
    at the currently observed effect size (equal-group approximation, 80% power).

    Parameters
    ----------
    df     : retention_data_total or retention_data_total_na (dx, platform, FTUE_flag, cohort_size,
             retained_size, retention_rate)
    title  : chart title
    alpha  : significance level (default 0.05)
    power  : desired power for the n_needed calculation (default 0.80)

    Returns
    -------
    pd.DataFrame — input df augmented with significance columns.
    """
    df_sig = compute_retention_significance(df, alpha=alpha, power=power)

    platforms = sorted(df_sig['platform'].unique())
    ftue_groups = sorted(df_sig['FTUE_flag'].unique())
    dx_vals = sorted(df_sig.loc[df_sig['dx'] > 0, 'dx'].unique())
    dx_labels = ['D' + str(d) for d in dx_vals]

    COLORS = {
        ftue_groups[0]: 'rgba(99,110,250,0.85)',
        ftue_groups[1]: 'rgba(239,85,59,0.85)',
    }
    SIG_COLOR = {'***': '#1a7f37', '**': '#1a7f37', '*': '#d97706', 'NS': '#888888'}

    fig = make_subplots(
        rows=len(platforms), cols=1,
        subplot_titles=platforms,
        shared_xaxes=False,
        vertical_spacing=0.14,
    )

    for row_idx, platform in enumerate(platforms, 1):
        pdata = (
            df_sig[(df_sig['platform'] == platform) & (df_sig['dx'] > 0)]
            .copy()
            .sort_values('dx')
        )
        pdata['dx_cat'] = 'D' + pdata['dx'].astype(str)

        for ftue in ftue_groups:
            gdata = pdata[pdata['FTUE_flag'] == ftue]
            err_up = (gdata['ci_high'] - gdata['retention_rate']).values
            err_dn = (gdata['retention_rate'] - gdata['ci_low']).values

            fig.add_trace(go.Bar(
                x=gdata['dx_cat'],
                y=gdata['retention_rate'],
                name=ftue,
                legendgroup=ftue,
                showlegend=(row_idx == 1),
                marker_color=COLORS[ftue],
                error_y=dict(
                    type='data',
                    array=err_up,
                    arrayminus=err_dn,
                    visible=True,
                    color='rgba(0,0,0,0.35)',
                    thickness=1.5,
                    width=4,
                ),
                text=gdata['retention_rate'].apply(lambda v: f'{v:.1%}'),
                textposition='outside',
                hovertemplate=(
                    '%{x}: %{y:.2%}<br>'
                    'n=%{customdata[0]:,} | retained=%{customdata[1]:,}<extra>' + ftue + '</extra>'
                ),
                customdata=gdata[['cohort_size', 'retained_size']].values,
            ), row=row_idx, col=1)

        # Significance annotations — one per dx group
        sig_cols = ['dx', 'dx_cat', 'sig_label', 'p_value', 'n_needed']
        sig_summary = pdata.drop_duplicates('dx')[sig_cols].sort_values('dx')
        max_ci = pdata.groupby('dx')['ci_high'].max()

        xref = 'x' if row_idx == 1 else f'x{row_idx}'
        yref = 'y' if row_idx == 1 else f'y{row_idx}'

        for _, sr in sig_summary.iterrows():
            label = sr['sig_label']
            p_val = sr['p_value']
            p_text = 'p<0.001' if p_val < 0.001 else f'p={p_val:.3f}'
            ann_text = f'<b>{label}</b> {p_text}'

            if label == 'NS' and pd.notna(sr['n_needed']):
                current_min_n = int(pdata[pdata['dx'] == sr['dx']]['cohort_size'].min())
                shortfall = max(0, int(sr['n_needed']) - current_min_n)
                if shortfall > 0:
                    ann_text += f'<br>~{shortfall:,} more/group needed'
                else:
                    ann_text += '<br>(n sufficient — borderline)'

            y_ann = float(max_ci.get(sr['dx'], 0)) + 0.018

            fig.add_annotation(
                x=sr['dx_cat'], y=y_ann,
                text=ann_text,
                showarrow=False,
                font=dict(size=9, color=SIG_COLOR.get(label, '#888')),
                xref=xref, yref=yref,
                xanchor='center', yanchor='bottom',
                align='center',
            )

        yax = 'yaxis' if row_idx == 1 else f'yaxis{row_idx}'
        xax = 'xaxis' if row_idx == 1 else f'xaxis{row_idx}'
        fig.update_layout(**{
            yax: dict(tickformat='.0%', title='Retention rate'),
            xax: dict(categoryorder='array', categoryarray=dx_labels),
        })

    fig.update_layout(
        title=title,
        barmode='group',
        width=1200,
        height=520 * len(platforms),
        uniformtext_minsize=7,
        uniformtext_mode='hide',
    )
    fig.show()
    return df_sig


# ---------------------------------------------------------------------------
# Rate-metric forecasting pipeline: decomposition, robust outliers, change-point
# detection, logit-space SARIMAX forecast, uplift scenario. Shared by any daily
# rate/percentage series (return rate, loyalty segment share, etc.) so the same
# fixes/methodology apply everywhere instead of drifting across copies.
# ---------------------------------------------------------------------------

def decompose_series(series, period=7):
    """STL decomposition (trend/seasonal/resid) + ADF stationarity test on the residual.

    Returns (df, adf_stat, adf_p) where df has columns: value, trend, seasonal, resid, deseasonalized.
    """
    from statsmodels.tsa.seasonal import STL
    from statsmodels.tsa.stattools import adfuller

    stl_res = STL(series, period=period, robust=True).fit()
    df = pd.DataFrame({
        'value': series,
        'trend': stl_res.trend,
        'seasonal': stl_res.seasonal,
        'resid': stl_res.resid,
    })
    df['deseasonalized'] = df['value'] - df['seasonal']
    adf_stat, adf_p, *_ = adfuller(df['resid'].dropna())
    return df, adf_stat, adf_p


def flag_robust_outliers(resid, threshold=3.5):
    """MAD-based modified z-score (Iglewicz & Hoaglin). Returns (modified_z, is_outlier)."""
    med = resid.median()
    mad = (resid - med).abs().median()
    modified_z = 0.6745 * (resid - med) / mad
    return modified_z, modified_z.abs() > threshold


def detect_changepoints(deseasonalized, min_segment_days=14, penalty=None):
    """PELT change-point detection (regime shifts) on a standardized deseasonalized series.

    Returns (changepoint_dates, penalty_used). Penalty defaults to the standard BIC-style log(n).
    """
    import ruptures as rpt

    x = deseasonalized.dropna()
    x_std = (x - x.mean()) / x.std()
    if penalty is None:
        penalty = np.log(len(x_std))

    algo = rpt.Pelt(model='l2', min_size=min_segment_days).fit(x_std.values)
    bkps = algo.predict(pen=penalty)
    changepoint_dates = [x.index[i - 1] for i in bkps[:-1]]
    return changepoint_dates, penalty


def select_training_start(series_index, changepoint_dates, min_training_days=90):
    """Walk backward through detected regimes until the window has >= min_training_days.

    Avoids both an arbitrary flat lookback and fitting on a single, possibly too-short, latest regime.
    """
    series_end = series_index.max()
    training_start = series_index.min()
    for cp in sorted(changepoint_dates, reverse=True):
        if (series_end - cp).days >= min_training_days:
            training_start = cp
            break
    return training_start


def logit(p):
    return np.log(p / (1 - p))


def sigmoid(x):
    return 1 / (1 + np.exp(-x))


def fit_rate_forecast(train_series, order=(1, 1, 1), seasonal_order=(1, 1, 1, 7),
                       horizon=90, backtest_days=28, ci_alpha=0.20):
    """
    SARIMAX forecast fit on the logit-transformed series, so bands stay within (0, 1) —
    a plain fit on the raw rate can produce out-of-range bounds at longer horizons.

    Returns (backtest_mae, backtest_mape, forecast_df) — forecast_df has forecast/low/high
    columns indexed by date, for `horizon` days past the end of train_series.
    """
    from statsmodels.tsa.statespace.sarimax import SARIMAX
    import warnings
    warnings.filterwarnings('ignore', category=UserWarning)

    train_logit = logit(train_series)

    backtest_fit = SARIMAX(
        train_logit.iloc[:-backtest_days], order=order, seasonal_order=seasonal_order,
        enforce_stationarity=False, enforce_invertibility=False,
    ).fit(disp=False)
    backtest_pred = sigmoid(backtest_fit.get_forecast(steps=backtest_days).predicted_mean)
    backtest_true = train_series.iloc[-backtest_days:]
    backtest_mae = np.mean(np.abs(backtest_pred.values - backtest_true.values))
    backtest_mape = np.mean(np.abs((backtest_pred.values - backtest_true.values) / backtest_true.values))

    final_fit = SARIMAX(
        train_logit, order=order, seasonal_order=seasonal_order,
        enforce_stationarity=False, enforce_invertibility=False,
    ).fit(disp=False)
    forecast = final_fit.get_forecast(steps=horizon)
    forecast_dates = pd.date_range(train_series.index.max() + pd.Timedelta(days=1), periods=horizon, freq='D')

    forecast_df = pd.DataFrame({
        'dt': forecast_dates,
        'forecast': sigmoid(forecast.predicted_mean.values),
    }).set_index('dt')
    ci = forecast.conf_int(alpha=ci_alpha)
    forecast_df['low'] = sigmoid(ci.iloc[:, 0].values)
    forecast_df['high'] = sigmoid(ci.iloc[:, 1].values)

    return backtest_mae, backtest_mape, forecast_df


def fit_value_forecast(train_series, order=(1, 1, 1), seasonal_order=(1, 1, 1, 7),
                        horizon=90, backtest_days=28, ci_alpha=0.20):
    """
    SARIMAX forecast fit on the log-transformed series — for strictly-positive, unbounded
    metrics (e.g. revenue/ARPDAU) rather than [0,1] rates. Keeps forecast bands from going
    negative, the value-metric equivalent of what the logit transform does for rates.

    Returns (backtest_mae, backtest_mape, forecast_df) — same shape as fit_rate_forecast.
    """
    from statsmodels.tsa.statespace.sarimax import SARIMAX
    import warnings
    warnings.filterwarnings('ignore', category=UserWarning)

    train_log = np.log(train_series)

    backtest_fit = SARIMAX(
        train_log.iloc[:-backtest_days], order=order, seasonal_order=seasonal_order,
        enforce_stationarity=False, enforce_invertibility=False,
    ).fit(disp=False)
    backtest_pred = np.exp(backtest_fit.get_forecast(steps=backtest_days).predicted_mean)
    backtest_true = train_series.iloc[-backtest_days:]
    backtest_mae = np.mean(np.abs(backtest_pred.values - backtest_true.values))
    backtest_mape = np.mean(np.abs((backtest_pred.values - backtest_true.values) / backtest_true.values))

    final_fit = SARIMAX(
        train_log, order=order, seasonal_order=seasonal_order,
        enforce_stationarity=False, enforce_invertibility=False,
    ).fit(disp=False)
    forecast = final_fit.get_forecast(steps=horizon)
    forecast_dates = pd.date_range(train_series.index.max() + pd.Timedelta(days=1), periods=horizon, freq='D')

    forecast_df = pd.DataFrame({
        'dt': forecast_dates,
        'forecast': np.exp(forecast.predicted_mean.values),
    }).set_index('dt')
    ci = forecast.conf_int(alpha=ci_alpha)
    forecast_df['low'] = np.exp(ci.iloc[:, 0].values)
    forecast_df['high'] = np.exp(ci.iloc[:, 1].values)

    return backtest_mae, backtest_mape, forecast_df


def apply_uplift_scenario(forecast_df, uplift_pct, clip_upper=1.0):
    """
    Apply a relative uplift directly to a forecast_df's forecast/low/high.

    clip_upper: ceiling to clip at (1.0 for a [0,1] rate/proportion metric — the default).
    Pass None for unbounded metrics like revenue, which have no natural upper bound.
    """
    scenario_df = forecast_df.copy()
    for col in ['forecast', 'low', 'high']:
        uplifted = scenario_df[col] * (1 + uplift_pct)
        scenario_df[f'{col}_uplift'] = uplifted.clip(upper=clip_upper) if clip_upper is not None else uplifted
    return scenario_df


def plot_stl_decomposition(df, title):
    fig = make_subplots(rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.06,
                         subplot_titles=['Observed + trend', 'Weekly seasonal component', 'Residual'])
    fig.add_trace(go.Scatter(x=df.index, y=df['value'], name='Observed',
                              line=dict(color='rgba(148,163,184,0.6)')), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df['trend'], name='Trend',
                              line=dict(color='rgba(59,130,246,1)', width=2)), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df['seasonal'], name='Seasonal',
                              line=dict(color='rgba(16,185,129,1)')), row=2, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df['resid'], name='Residual',
                              line=dict(color='rgba(239,68,68,0.8)')), row=3, col=1)
    fig.update_layout(height=800, width=1400, title=title)
    return fig


def plot_outliers(df, is_outlier, title, y_col='value'):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df.index, y=df[y_col], name=y_col, line=dict(color='rgba(59,130,246,0.8)')))
    out = df[is_outlier]
    fig.add_trace(go.Scatter(x=out.index, y=out[y_col], name='Flagged outlier', mode='markers',
                              marker=dict(color='rgba(239,68,68,1)', size=7, symbol='x')))
    fig.update_layout(title=title, width=1400, height=500, xaxis_title='Date', yaxis_title=y_col)
    return fig


def plot_changepoints(df, changepoint_dates, title, y_col='value'):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df.index, y=df[y_col], name=y_col, line=dict(color='rgba(59,130,246,0.8)')))
    fig.add_trace(go.Scatter(x=df.index, y=df['trend'], name='Trend',
                              line=dict(color='rgba(16,185,129,0.9)', width=2)))
    for d in changepoint_dates:
        fig.add_vline(x=d, line=dict(color='rgba(239,68,68,0.6)', dash='dash', width=1.5))
    fig.update_layout(title=title, width=1400, height=550, xaxis_title='Date', yaxis_title=y_col)
    return fig


def plot_forecast_summary(full_series, train_series, forecast_df, training_start, title, ci_alpha=0.20):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=full_series.index, y=full_series, name='Observed (full history)',
                              line=dict(color='rgba(148,163,184,0.5)')))
    fig.add_trace(go.Scatter(x=train_series.index, y=train_series, name='Training window',
                              line=dict(color='rgba(59,130,246,1)')))
    fig.add_trace(go.Scatter(x=forecast_df.index, y=forecast_df['low'], mode='lines',
                              line=dict(width=0), showlegend=False))
    fig.add_trace(go.Scatter(x=forecast_df.index, y=forecast_df['high'], mode='lines',
                              line=dict(width=0), fill='tonexty', fillcolor='rgba(16,185,129,0.2)',
                              name=f'{int((1 - ci_alpha) * 100)}% interval'))
    fig.add_trace(go.Scatter(x=forecast_df.index, y=forecast_df['forecast'], name='Forecast',
                              line=dict(color='rgba(16,185,129,1)', width=2.5, dash='dash')))
    fig.add_vline(x=training_start, line=dict(color='rgba(100,100,100,0.4)', dash='dot'))
    fig.update_layout(title=title, width=1400, height=550, xaxis_title='Date', yaxis_title=full_series.name or 'value')
    return fig


def plot_uplift_scenario(full_series, scenario_df, uplift_pct, title, lookback_days=60):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=full_series.index[-lookback_days:], y=full_series.iloc[-lookback_days:],
                              name=f'Observed (last {lookback_days}d)', line=dict(color='rgba(148,163,184,0.6)')))
    fig.add_trace(go.Scatter(x=scenario_df.index, y=scenario_df['low'], mode='lines',
                              line=dict(width=0), showlegend=False))
    fig.add_trace(go.Scatter(x=scenario_df.index, y=scenario_df['high'], mode='lines',
                              line=dict(width=0), fill='tonexty', fillcolor='rgba(16,185,129,0.15)',
                              name='Baseline 80% interval'))
    fig.add_trace(go.Scatter(x=scenario_df.index, y=scenario_df['forecast'], name='Baseline forecast',
                              line=dict(color='rgba(16,185,129,1)', width=2.5, dash='dash')))
    fig.add_trace(go.Scatter(x=scenario_df.index, y=scenario_df['forecast_uplift'],
                              name=f'Uplift scenario ({uplift_pct:+.1%})',
                              line=dict(color='rgba(234,88,12,1)', width=2.5)))
    fig.update_layout(title=title, width=1400, height=550, xaxis_title='Date', yaxis_title=full_series.name or 'value')
    return fig


def compare_periods(series, period1_start, period1_end, period2_start, period2_end):
    """
    Average a daily series over two date ranges and compute the uplift between them.

    Returns a dict: period1_mean, period2_mean, period1_n, period2_n, abs_diff, pct_uplift
    (pct_uplift is relative: period2_mean / period1_mean - 1).
    """
    p1 = series.loc[str(period1_start):str(period1_end)]
    p2 = series.loc[str(period2_start):str(period2_end)]
    p1_mean = p1.mean()
    p2_mean = p2.mean()
    return {
        'period1_mean': p1_mean,
        'period2_mean': p2_mean,
        'period1_n': len(p1),
        'period2_n': len(p2),
        'abs_diff': p2_mean - p1_mean,
        'pct_uplift': (p2_mean / p1_mean - 1) if p1_mean else float('nan'),
    }


def plot_period_comparison(comparison, title, y_label='value'):
    """Bar chart comparing Period 1 vs Period 2 average, with the % uplift annotated."""
    fig = go.Figure(go.Bar(
        x=['Period 1', 'Period 2'],
        y=[comparison['period1_mean'], comparison['period2_mean']],
        text=[f"{comparison['period1_mean']:.4f}", f"{comparison['period2_mean']:.4f}"],
        textposition='outside',
        marker_color=['rgba(59,130,246,0.85)', 'rgba(16,185,129,0.85)'],
    ))
    fig.add_annotation(
        x=1, y=comparison['period2_mean'], yshift=30,
        text=f"{comparison['pct_uplift']:+.1%} vs Period 1",
        showarrow=False, font=dict(size=13, color='#333'),
    )
    fig.update_layout(title=title, width=700, height=450, yaxis_title=y_label, showlegend=False)
    return fig
