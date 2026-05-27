"""
Panel setup and callback wiring for the game simulator notebook.
"""
from __future__ import annotations

import asyncio
import logging
import traceback
from pathlib import Path

import ipywidgets as _w
import pandas as pd

_log_path = Path(__file__).parent.parent / 'simulator.log'
_handler  = logging.FileHandler(_log_path, encoding='utf-8')
_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
_logger   = logging.getLogger('simulator')
_logger.setLevel(logging.DEBUG)
if not _logger.handlers:
    _logger.addHandler(_handler)

from datetime import timedelta
from IPython.display import display as _display, Javascript as _Javascript

from common_lib.curves import average_actuals_anchors, average_arpdau_from_actuals, build_curve, derive_age_distribution
from common_lib.plots import build_chart_widget, build_curve_widget, build_curve_preview, configure as _configure_plots
from common_lib.sheets import load_inputs
from common_lib.simulation import (
    PlatformInputs, SimulationEngine,
    load_scenario, list_scenarios,
    save_result, save_scenario,
)
from common_lib.tables import monthly_table


def prefill_panel(panel, actuals: pd.DataFrame, anchor_dau: dict,
                  sheet_inputs: dict) -> None:
    """Populate panel with last-observed anchor DAU and CSV CPI/UA/team-cost inputs."""
    panel.ios_panel.anchor_dau.value     = float(anchor_dau.get('ios',     0))
    panel.android_panel.anchor_dau.value = float(anchor_dau.get('android', 0))

    cpi_df = sheet_inputs['cpi']
    ua_df  = sheet_inputs['ua_spend']

    for platform, widget_panel in [('ios', panel.ios_panel), ('android', panel.android_panel)]:
        cpi_sub = cpi_df[cpi_df['platform'] == platform].dropna(subset=['cpi'])
        widget_panel.set_monthly_values(
            monthly_cpi=dict(zip(cpi_sub['month'], cpi_sub['cpi'].astype(float))),
        )

    panel.ua_budget_panel.set_values(
        monthly_budget  = dict(zip(ua_df['month'], ua_df['total_budget'].astype(float))),
        monthly_ios_pct = dict(zip(ua_df['month'], ua_df['ios_pct'].astype(float))),
        is_baseline=True,
    )




def setup_callbacks(
    panel,
    engine: SimulationEngine,
    actuals: pd.DataFrame,
    live_retention:  pd.DataFrame = None,
    live_conversion: pd.DataFrame = None,
    default_scenario: str = None,
    installs: pd.DataFrame = None,
) -> None:
    """Wire all panel buttons to their callback functions."""
    _display(_Javascript("""
(function(){
  if(window._inputCopyPasteFixed) return;
  window._inputCopyPasteFixed = true;
  function stopIfInput(e){
    var t = e.target;
    if(t && (t.tagName==='INPUT' || t.tagName==='TEXTAREA')){
      e.stopPropagation();
    }
  }
  document.addEventListener('keydown', stopIfInput, true);
  document.addEventListener('keyup',   stopIfInput, true);
})();
"""))

    def _build_inputs(platform: str, overrides: dict) -> PlatformInputs:
        anchors = panel.get_curve_anchors()
        arpdau  = panel.get_arpdau()
        boost   = overrides.get('monthly_installs_boost_pct') or {}
        return PlatformInputs(
            platform=platform,
            retention_curve=build_curve(anchors[platform]['retention']),
            conversion_curve=build_curve(anchors[platform]['conversion']),
            monthly_cpi=overrides['monthly_cpi'],
            monthly_iap_arpdau=arpdau[platform]['iap'],
            monthly_ad_arpdau=arpdau[platform]['ad'],
            monthly_ua_spend=overrides['monthly_ua_spend'],
            anchor_dau=overrides.get('anchor_dau'),
            anchor_offset_pct=overrides.get('anchor_offset_pct', 0.0),
            avg_base_age=overrides.get('avg_base_age', 90),
            age_distribution=overrides.get('age_distribution') or None,
            monthly_installs_boost_pct=boost if boost else None,
        )

    def run_simulation():
        try:
            panel.set_status("Running simulation...", "blue")
            name        = panel.scenario_name.value
            start       = panel.get_forecast_start()
            n_months    = int(panel.forecast_months.value)
            actuals_from = panel.get_actuals_from()
            ios_inp     = _build_inputs('ios',     panel.get_ios_overrides())
            and_inp     = _build_inputs('android', panel.get_android_overrides())
            result      = engine.run(ios_inp, and_inp, forecast_start=start, scenario_name=name, n_months=n_months)
            save_result(name, result)

            filtered = actuals[actuals['dt'].dt.date >= actuals_from] if actuals_from else actuals
            _configure_plots(filtered)

            n_actuals = max(1, (start.year - actuals_from.year) * 12 + (start.month - actuals_from.month)) if actuals_from else 6

            curve_anchors = panel.get_curve_anchors()
            chart_ws = {k: build_chart_widget(name, k) for k in ('dau', 'installs', 'revenue', 'monthly')}
            arpdau_vals = panel.get_arpdau()
            table_styler, table_df = monthly_table(
                name, filtered,
                historical_marketing=panel.get_historical_marketing(),
                n_actuals=n_actuals,
                monthly_iap_net_factor=arpdau_vals.get('iap_net_factor'),
            )
            table_html = table_styler.to_html()

            from pathlib import Path as _Path
            _exports_dir = _Path(__file__).parent.parent / 'exports'
            _exports_dir.mkdir(exist_ok=True)
            export_btn    = _w.Button(description="Export CSV", button_style="",
                                      layout=_w.Layout(width="110px"))
            export_status = _w.HTML("")
            _df_ref = [table_df]

            def _on_export(_):
                try:
                    path = _exports_dir / f"{name}_pl_table.csv"
                    _df_ref[0].to_csv(path, index=False)
                    export_status.value = (
                        f"<span style='color:green;font-size:11px'>Saved: {path.name}</span>"
                    )
                except Exception as exc:
                    export_status.value = (
                        f"<span style='color:red;font-size:11px'>Error: {exc}</span>"
                    )

            export_btn.on_click(_on_export)
            table_w = _w.VBox([
                _w.HBox([export_btn, export_status],
                        layout=_w.Layout(align_items='center', margin='0 0 6px 0')),
                _w.HTML(f'<div style="overflow-x:auto">{table_html}</div>'),
            ])

            panel.set_chart_results(chart_ws, table_w)
            panel.set_status(f"Done — {name}  |  save the scenario to persist inputs", 'green')
        except Exception:
            _logger.exception("run_simulation failed")
            panel.set_status(f"Error — see {_log_path.name}", "red")

    def save_current_scenario():
        try:
            name          = panel.scenario_name.value
            start         = panel.get_forecast_start()
            n_months      = int(panel.forecast_months.value)
            ios_inp       = _build_inputs('ios',     panel.get_ios_overrides())
            and_inp       = _build_inputs('android', panel.get_android_overrides())
            curve_anchors   = panel.get_curve_anchors()
            actuals_range   = panel.get_actuals_range()
            actuals_from    = panel.get_actuals_from()
            ua_vals         = panel.ua_budget_panel.values
            arpdau_vals     = panel.get_arpdau()
            path = save_scenario(
                name, start, ios_inp, and_inp,
                n_months=n_months,
                curve_anchors=curve_anchors,
                actuals_range=actuals_range,
                monthly_team_cost=None,
                actuals_from=str(actuals_from) if actuals_from else None,
                selected_charts=None,
                historical_marketing=panel.get_historical_marketing(),
                monthly_ua_budget=ua_vals['monthly_budget'],
                monthly_ios_pct=ua_vals['monthly_ios_pct'],
                monthly_iap_net_factor=arpdau_vals.get('iap_net_factor'),
            )
            panel.set_status(f'Saved to {path.name}', 'green')
            panel.load_dropdown.options = ['— new scenario —'] + list_scenarios()
        except Exception:
            _logger.exception("save_scenario failed")
            panel.set_status(f"Save error — see {_log_path.name}", "red")

    def load_saved_scenario(name: str):
        try:
            _logger.debug("load_saved_scenario START name=%s", name)
            panel.set_status(f"Loading {name}...", "blue")
            (_, start, n_months, ios_inp, and_inp,
             curve_anchors, actuals_range, monthly_team_cost,
             actuals_from, selected_charts, historical_marketing,
             monthly_ua_budget, monthly_ios_pct,
             monthly_iap_net_factor) = load_scenario(name)

            panel.forecast_start.value             = start
            panel.forecast_months.value            = n_months
            panel.scenario_name.value              = name
            panel.ios_panel.anchor_dau.value            = ios_inp.anchor_dau or 0
            panel.android_panel.anchor_dau.value        = and_inp.anchor_dau or 0
            panel.ios_panel.anchor_offset_pct.value     = ios_inp.anchor_offset_pct
            panel.android_panel.anchor_offset_pct.value = and_inp.anchor_offset_pct
            panel.ios_panel.avg_base_age.value          = ios_inp.avg_base_age
            panel.android_panel.avg_base_age.value      = and_inp.avg_base_age
            if ios_inp.age_distribution:
                panel.ios_panel.set_age_distribution(ios_inp.age_distribution)
            if and_inp.age_distribution:
                panel.android_panel.set_age_distribution(and_inp.age_distribution)
            panel.ios_panel.set_monthly_values(
                monthly_cpi=ios_inp.monthly_cpi,
                monthly_boost_pct=ios_inp.monthly_installs_boost_pct or {},
            )
            panel.android_panel.set_monthly_values(
                monthly_cpi=and_inp.monthly_cpi,
                monthly_boost_pct=and_inp.monthly_installs_boost_pct or {},
            )
            panel.arpdau_panel.set_values(
                ios_iap        = ios_inp.monthly_iap_arpdau,
                ios_ad         = ios_inp.monthly_ad_arpdau,
                android_iap    = and_inp.monthly_iap_arpdau,
                android_ad     = and_inp.monthly_ad_arpdau,
                iap_net_factor = monthly_iap_net_factor,
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
            if actuals_from:
                panel.set_actuals_from(actuals_from)
            # UA budget: new format takes precedence; fall back to deriving from per-platform UA
            if monthly_ua_budget is not None:
                panel.ua_budget_panel.set_values(
                    monthly_ua_budget,
                    monthly_ios_pct or {m: 50 for m in monthly_ua_budget},
                    is_baseline=True,
                )
            else:
                combined_budget  = {}
                combined_ios_pct = {}
                for m, v in (ios_inp.monthly_ua_spend or {}).items():
                    and_v = (and_inp.monthly_ua_spend or {}).get(m, 0)
                    total = v + and_v
                    combined_budget[m]  = total
                    combined_ios_pct[m] = (v / total * 100) if total > 0 else 50
                for m, v in (historical_marketing or {}).items():
                    combined_budget.setdefault(m, v)
                    combined_ios_pct.setdefault(m, 50)
                if combined_budget:
                    panel.ua_budget_panel.set_values(combined_budget, combined_ios_pct, is_baseline=True)

            _logger.debug("load_saved_scenario COMPLETE name=%s", name)
            try:
                asyncio.get_running_loop()
                async def _wait_and_show_status(_name=name):
                    for _ in range(60):  # poll up to 6 s
                        await asyncio.sleep(0.1)
                        if not any([
                            panel.ios_panel._rows_timer,
                            panel.android_panel._rows_timer,
                            panel.ios_panel._boost_rows_timer,
                            panel.android_panel._boost_rows_timer,
                            panel.arpdau_panel._rows_timer,
                            panel.ua_budget_panel._rows_timer,
                        ]):
                            break
                    panel.resync_header_widgets()
                    await asyncio.sleep(0.2)  # let browser process send_state messages
                    panel.set_status(f'Loaded: {_name}', 'blue')
                asyncio.ensure_future(_wait_and_show_status())
            except RuntimeError:
                panel.set_status(f'Loaded: {name}', 'blue')
        except Exception:
            _logger.exception("load_scenario failed")
            panel.set_status(f"Load error — see {_log_path.name}", "red")

    def set_anchor_from_actuals():
        start     = panel.get_forecast_start()
        available = actuals[actuals['dt'].dt.date < start]
        if available.empty:
            panel.set_status(f"No actuals available before {start}", 'orange')
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
            _logger.exception("load_curve failed (%s)", metric)
            actuals_panel.set_status(f"Error: {e}", 'red')

    def load_arpdau_from_actuals():
        try:
            start, end = panel.arpdau_actuals_panel.get_range(forecast_start=panel.get_forecast_start())
            avgs = average_arpdau_from_actuals(actuals, start, end)
            if avgs['ios']['iap'] is None and avgs['android']['iap'] is None:
                panel.arpdau_actuals_panel.set_status(f"No actuals data in {start} – {end}", 'orange')
                return
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
            _logger.exception("load_arpdau_from_actuals failed")
            panel.arpdau_actuals_panel.set_status(f"Error: {e}", 'red')

    def _derive_age_dist(platform: str, widget_panel):
        if installs is None:
            widget_panel._age_dist_status.value = (
                "<span style='color:red;font-size:11px'>installs data not loaded — pass installs= to setup_callbacks()</span>"
            )
            return
        try:
            start       = panel.get_forecast_start()
            anchor_date = (actuals[actuals['dt'].dt.date < start]['dt'].dt.date.max()
                           if not actuals[actuals['dt'].dt.date < start].empty
                           else start - timedelta(days=1))
            anchors    = panel.get_curve_anchors()
            retention  = build_curve(anchors[platform]['retention'])
            dist       = derive_age_distribution(installs, platform, retention, anchor_date)
            widget_panel.set_age_distribution(dist)
        except Exception as e:
            _logger.exception("derive_age_dist failed (%s)", platform)
            widget_panel._age_dist_status.value = f"<span style='color:red;font-size:11px'>Error: {e}</span>"

    def _preview_curve(metric: str):
        curve_panel = panel.retention_panel if metric == 'retention' else panel.conversion_panel
        anchors = panel.get_curve_anchors()
        platform_anchors = {p: anchors[p][metric] for p in ('ios', 'android')}
        curve_panel.set_preview(build_curve_preview(platform_anchors, metric))

    panel.retention_panel.on_preview(lambda: _preview_curve('retention'))
    panel.conversion_panel.on_preview(lambda: _preview_curve('conversion'))

    panel.on_run(run_simulation)
    panel.on_save(save_current_scenario)
    panel.on_load(load_saved_scenario)
    panel.on_set_anchor(set_anchor_from_actuals)
    panel.retention_actuals_panel.on_load(lambda: _load_single_curve('retention'))
    panel.conversion_actuals_panel.on_load(lambda: _load_single_curve('conversion'))
    panel.arpdau_actuals_panel.on_load(load_arpdau_from_actuals)
    panel.ios_panel.on_derive(lambda: _derive_age_dist('ios',     panel.ios_panel))
    panel.android_panel.on_derive(lambda: _derive_age_dist('android', panel.android_panel))

    if default_scenario:
        load_saved_scenario(default_scenario)
