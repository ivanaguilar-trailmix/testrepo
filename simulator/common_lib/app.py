"""
Panel setup and callback wiring for the game simulator notebook.
"""
from __future__ import annotations

import traceback

import pandas as pd

from common_lib.curves import anchors_from_df, average_actuals_anchors, average_arpdau_from_actuals, build_curve
from common_lib.simulation import (
    PlatformInputs, SimulationEngine,
    load_scenario, list_scenarios,
    save_result, save_scenario,
)


def prefill_panel(panel, actuals: pd.DataFrame, anchor_dau: dict,
                  sheet_inputs: dict) -> None:
    """Populate panel with last-observed anchor DAU and CSV CPI/UA inputs."""
    panel.ios_panel.anchor_dau.value     = float(anchor_dau.get('ios',     0))
    panel.android_panel.anchor_dau.value = float(anchor_dau.get('android', 0))

    cpi_df = sheet_inputs['cpi']
    ua_df  = sheet_inputs['ua_spend']

    for platform, widget_panel in [('ios', panel.ios_panel), ('android', panel.android_panel)]:
        cpi_sub = cpi_df[cpi_df['platform'] == platform].dropna(subset=['cpi'])
        ua_sub  = ua_df[ua_df['platform'] == platform]
        widget_panel.set_monthly_values(
            monthly_cpi = dict(zip(cpi_sub['month'], cpi_sub['cpi'].astype(float))),
            monthly_ua  = dict(zip(ua_sub['month'],  ua_sub['budget'].astype(float))),
        )


def setup_callbacks(
    panel,
    engine: SimulationEngine,
    actuals: pd.DataFrame,
    live_retention:  pd.DataFrame = None,
    live_conversion: pd.DataFrame = None,
) -> None:
    """Wire all panel buttons to their callback functions."""

    def _build_inputs(platform: str, overrides: dict) -> PlatformInputs:
        anchors = panel.get_curve_anchors()
        arpdau  = panel.get_arpdau()
        return PlatformInputs(
            platform=platform,
            retention_curve=build_curve(anchors[platform]['retention']),
            conversion_curve=build_curve(anchors[platform]['conversion']),
            monthly_cpi=overrides['monthly_cpi'],
            monthly_iap_arpdau=arpdau[platform]['iap'],
            monthly_ad_arpdau=arpdau[platform]['ad'],
            monthly_ua_spend=overrides['monthly_ua_spend'],
            anchor_dau=overrides.get('anchor_dau'),
            avg_base_age=overrides.get('avg_base_age', 60),
        )

    def run_simulation():
        try:
            name     = panel.scenario_name.value
            start    = panel.get_forecast_start()
            n_months = int(panel.forecast_months.value)
            ios_inp  = _build_inputs('ios',     panel.get_ios_overrides())
            and_inp  = _build_inputs('android', panel.get_android_overrides())
            result   = engine.run(ios_inp, and_inp, forecast_start=start, scenario_name=name, n_months=n_months)
            path     = save_result(name, result)
            panel.set_status(f"Saved → {path.name}  |  call plot('{name}') to chart", 'green')
        except Exception:
            traceback.print_exc()
            panel.set_status("Error — see cell output", "red")

    def save_current_scenario():
        try:
            name          = panel.scenario_name.value
            start         = panel.get_forecast_start()
            n_months      = int(panel.forecast_months.value)
            ios_inp       = _build_inputs('ios',     panel.get_ios_overrides())
            and_inp       = _build_inputs('android', panel.get_android_overrides())
            curve_anchors = panel.get_curve_anchors()
            actuals_range = panel.get_actuals_range()
            path = save_scenario(
                name, start, ios_inp, and_inp,
                n_months=n_months,
                curve_anchors=curve_anchors,
                actuals_range=actuals_range,
            )
            panel.set_status(f'Saved to {path.name}', 'green')
            panel.load_dropdown.options = ['— new scenario —'] + list_scenarios()
        except Exception:
            traceback.print_exc()
            panel.set_status("Save error — see cell output", "red")

    def load_saved_scenario(name: str):
        try:
            _, start, n_months, ios_inp, and_inp, curve_anchors, actuals_range = load_scenario(name)
            panel.forecast_start.value             = start
            panel.forecast_months.value            = n_months
            panel.scenario_name.value              = name
            panel.ios_panel.anchor_dau.value       = ios_inp.anchor_dau or 0
            panel.android_panel.anchor_dau.value   = and_inp.anchor_dau or 0
            panel.ios_panel.avg_base_age.value     = ios_inp.avg_base_age
            panel.android_panel.avg_base_age.value = and_inp.avg_base_age
            panel.ios_panel.set_monthly_values(
                monthly_cpi = ios_inp.monthly_cpi,
                monthly_ua  = ios_inp.monthly_ua_spend,
            )
            panel.android_panel.set_monthly_values(
                monthly_cpi = and_inp.monthly_cpi,
                monthly_ua  = and_inp.monthly_ua_spend,
            )
            panel.arpdau_panel.set_values(
                ios_iap     = ios_inp.monthly_iap_arpdau,
                ios_ad      = ios_inp.monthly_ad_arpdau,
                android_iap = and_inp.monthly_iap_arpdau,
                android_ad  = and_inp.monthly_ad_arpdau,
            )
            if curve_anchors:
                panel.retention_panel.set_values(
                    ios=curve_anchors['ios']['retention'], android=curve_anchors['android']['retention'],
                )
                panel.conversion_panel.set_values(
                    ios=curve_anchors['ios']['conversion'], android=curve_anchors['android']['conversion'],
                )
            if actuals_range:
                panel.set_actuals_range(actuals_range)
            panel.set_status(f'Loaded: {name}', 'blue')
        except Exception:
            traceback.print_exc()
            panel.set_status("Load error — see cell output", "red")

    def set_anchor_from_actuals():
        start     = panel.get_forecast_start()
        available = actuals[actuals['dt'].dt.date <= start]
        if available.empty:
            panel.set_status(f"No actuals available on or before {start}", 'orange')
            return
        anchor_date = available['dt'].dt.date.max()
        day_data    = actuals[actuals['dt'].dt.date == anchor_date]
        found = []
        for platform, widget_panel in [('ios', panel.ios_panel), ('android', panel.android_panel)]:
            row = day_data[day_data['platform'] == platform]
            if not row.empty:
                widget_panel.anchor_dau.value = float(row['dau'].iloc[0])
                found.append(platform)
        if found:
            suffix = " (forecast start)" if anchor_date == start else " (most recent available)"
            panel.set_status(f"Anchor set from {anchor_date}{suffix} — {', '.join(found)}", 'green')
        else:
            panel.set_status(f"No actuals found for {anchor_date}", 'orange')

    def _load_single_curve(metric: str):
        actuals_panel = (
            panel.retention_actuals_panel if metric == 'retention'
            else panel.conversion_actuals_panel
        )
        curve_panel = (
            panel.retention_panel if metric == 'retention'
            else panel.conversion_panel
        )
        live_df = live_retention if metric == 'retention' else live_conversion

        if live_df is None:
            actuals_panel.set_status(
                f"live_{metric} not available — pass it to setup_callbacks()", 'red'
            )
            return
        try:
            start, end = actuals_panel.get_range(forecast_start=panel.get_forecast_start())
            anchors = average_actuals_anchors(live_df, metric, start, end)
            if not any(anchors.values()):
                actuals_panel.set_status(f"No {metric} data in {start} – {end}", 'orange')
                return
            curve_panel.set_values(ios=anchors['ios'], android=anchors['android'], is_baseline=True)
            actuals_panel.set_status(f"Loaded from {start} – {end}  |  yellow = overridden", 'green')
        except Exception as e:
            traceback.print_exc()
            actuals_panel.set_status(f"Error: {e}", 'red')

    def load_arpdau_from_actuals():
        try:
            start, end = panel.arpdau_actuals_panel.get_range(forecast_start=panel.get_forecast_start())
            avgs = average_arpdau_from_actuals(actuals, start, end)
            if avgs['ios']['iap'] is None and avgs['android']['iap'] is None:
                panel.arpdau_actuals_panel.set_status(f"No actuals data in {start} – {end}", 'orange')
                return
            # spread the average across every forecast month
            months = panel.arpdau_panel._months
            panel.arpdau_panel.set_values(
                ios_iap     = {m: avgs['ios']['iap']     for m in months} if avgs['ios']['iap']     is not None else {},
                ios_ad      = {m: avgs['ios']['ad']      for m in months} if avgs['ios']['ad']      is not None else {},
                android_iap = {m: avgs['android']['iap'] for m in months} if avgs['android']['iap'] is not None else {},
                android_ad  = {m: avgs['android']['ad']  for m in months} if avgs['android']['ad']  is not None else {},
                is_baseline = True,
            )
            panel.arpdau_actuals_panel.set_status(
                f"Avg from {start} – {end} applied to all months  |  yellow = overridden", 'green'
            )
        except Exception as e:
            traceback.print_exc()
            panel.arpdau_actuals_panel.set_status(f"Error: {e}", 'red')

    panel.on_run(run_simulation)
    panel.on_save(save_current_scenario)
    panel.on_load(load_saved_scenario)
    panel.on_set_anchor(set_anchor_from_actuals)
    panel.retention_actuals_panel.on_load(lambda: _load_single_curve('retention'))
    panel.conversion_actuals_panel.on_load(lambda: _load_single_curve('conversion'))
    panel.arpdau_actuals_panel.on_load(load_arpdau_from_actuals)
