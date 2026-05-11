"""
Panel setup and callback wiring for the game simulator notebook.
"""
from __future__ import annotations

import traceback

import pandas as pd

from common_lib.curves import anchors_from_df, build_curve
from common_lib.simulation import (
    PlatformInputs, SimulationEngine,
    load_scenario, list_scenarios,
    save_result, save_scenario,
)


def prefill_panel(panel, actuals: pd.DataFrame, anchor_dau: dict,
                  sheet_inputs: dict, anchors_from_df=anchors_from_df) -> None:
    """Populate panel with last-observed anchor DAU, CSV monthly inputs, and CSV curves."""
    panel.ios_panel.anchor_dau.value     = float(anchor_dau.get('ios',     0))
    panel.android_panel.anchor_dau.value = float(anchor_dau.get('android', 0))

    cpi_df    = sheet_inputs['cpi']
    arpdau_df = sheet_inputs['arpdau']
    ua_df     = sheet_inputs['ua_spend']

    for platform, widget_panel in [('ios', panel.ios_panel), ('android', panel.android_panel)]:
        cpi_sub    = cpi_df[cpi_df['platform'] == platform].dropna(subset=['cpi'])
        arpdau_sub = arpdau_df[arpdau_df['platform'] == platform]
        ua_sub     = ua_df[ua_df['platform'] == platform]
        widget_panel.set_monthly_values(
            monthly_cpi = dict(zip(cpi_sub['month'],    cpi_sub['cpi'].astype(float))),
            monthly_iap = dict(zip(arpdau_sub['month'], arpdau_sub['iap_arpdau'].astype(float))),
            monthly_ad  = dict(zip(arpdau_sub['month'], arpdau_sub['ad_arpdau'].astype(float))),
            monthly_ua  = dict(zip(ua_sub['month'],     ua_sub['budget'].astype(float))),
        )

    panel.retention_panel.set_values(
        ios     = anchors_from_df(sheet_inputs['retention'],  'ios'),
        android = anchors_from_df(sheet_inputs['retention'],  'android'),
    )
    panel.conversion_panel.set_values(
        ios     = anchors_from_df(sheet_inputs['conversion'], 'ios'),
        android = anchors_from_df(sheet_inputs['conversion'], 'android'),
    )


def setup_callbacks(panel, engine: SimulationEngine, actuals: pd.DataFrame) -> None:
    """Wire all panel buttons to their callback functions."""

    def _build_inputs(platform: str, overrides: dict) -> PlatformInputs:
        anchors = panel.get_curve_anchors()
        return PlatformInputs(
            platform=platform,
            retention_curve=build_curve(anchors[platform]['retention']),
            conversion_curve=build_curve(anchors[platform]['conversion']),
            monthly_cpi=overrides['monthly_cpi'],
            monthly_iap_arpdau=overrides['monthly_iap_arpdau'],
            monthly_ad_arpdau=overrides['monthly_ad_arpdau'],
            monthly_ua_spend=overrides['monthly_ua_spend'],
            anchor_dau=overrides.get('anchor_dau'),
        )

    def run_simulation():
        try:
            name     = panel.scenario_name.value
            start    = panel.get_forecast_start()
            n_months = int(panel.forecast_months.value)
            ios_inp = _build_inputs('ios',     panel.get_ios_overrides())
            and_inp = _build_inputs('android', panel.get_android_overrides())
            result  = engine.run(ios_inp, and_inp, forecast_start=start, scenario_name=name, n_months=n_months)
            path    = save_result(name, result)
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
            path = save_scenario(name, start, ios_inp, and_inp, n_months=n_months, curve_anchors=curve_anchors)
            panel.set_status(f'Saved to {path.name}', 'green')
            panel.load_dropdown.options = ['— new scenario —'] + list_scenarios()
        except Exception:
            traceback.print_exc()
            panel.set_status("Save error — see cell output", "red")

    def load_saved_scenario(name: str):
        try:
            _, start, n_months, ios_inp, and_inp, curve_anchors = load_scenario(name)
            panel.forecast_start.value           = start
            panel.forecast_months.value          = n_months
            panel.scenario_name.value            = name
            panel.ios_panel.anchor_dau.value     = ios_inp.anchor_dau or 0
            panel.android_panel.anchor_dau.value = and_inp.anchor_dau or 0
            panel.ios_panel.set_monthly_values(
                ios_inp.monthly_cpi, ios_inp.monthly_iap_arpdau, ios_inp.monthly_ad_arpdau,
                monthly_ua=ios_inp.monthly_ua_spend,
            )
            panel.android_panel.set_monthly_values(
                and_inp.monthly_cpi, and_inp.monthly_iap_arpdau, and_inp.monthly_ad_arpdau,
                monthly_ua=and_inp.monthly_ua_spend,
            )
            if curve_anchors:
                panel.retention_panel.set_values(
                    ios     = curve_anchors['ios']['retention'],
                    android = curve_anchors['android']['retention'],
                )
                panel.conversion_panel.set_values(
                    ios     = curve_anchors['ios']['conversion'],
                    android = curve_anchors['android']['conversion'],
                )
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

    panel.on_run(run_simulation)
    panel.on_save(save_current_scenario)
    panel.on_load(load_saved_scenario)
    panel.on_set_anchor(set_anchor_from_actuals)
