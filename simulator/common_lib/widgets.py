"""
ipywidgets UI components for the game simulator.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Callable, Optional

import ipywidgets as widgets
from IPython.display import display


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _float_input(value: float, description: str, step: float = 0.01) -> widgets.BoundedFloatText:
    return widgets.BoundedFloatText(
        value=value, min=0.0, max=1e9, step=step,
        description=description,
        style={"description_width": "140px"},
        layout=widgets.Layout(width="300px"),
    )


def _header(text: str) -> widgets.HTML:
    return widgets.HTML(f"<b style='font-size:13px'>{text}</b>")


def _fill_btn() -> widgets.Button:
    return widgets.Button(
        description="Fill ↓",
        layout=widgets.Layout(width="65px", height="22px"),
        style={"font_size": "11px"},
    )


def _month_sequence(n_months: int, start_month: Optional[str]) -> list[str]:
    today = date.today()
    year  = today.year  if start_month is None else int(start_month[:4])
    month = today.month if start_month is None else int(start_month[5:7])
    labels = []
    for i in range(n_months):
        m = month + i
        y = year + (m - 1) // 12
        m = ((m - 1) % 12) + 1
        labels.append(f"{y}-{m:02d}")
    return labels


# ---------------------------------------------------------------------------
# Per-platform monthly inputs panel
# ---------------------------------------------------------------------------

class PlatformPanel:
    MAX_MONTHS = 24

    def __init__(self, platform: str, n_months: int = 12, start_month: Optional[str] = None):
        self.platform = platform
        self._months = _month_sequence(n_months, start_month)

        self.anchor_dau = _float_input(0.0, "Anchor DAU", step=1)

        self._cpi_inputs   = []
        self._iap_inputs   = []
        self._ad_inputs    = []
        self._ua_inputs    = []
        self._month_labels = []
        self._data_rows    = []

        cpi_fill = _fill_btn()
        iap_fill = _fill_btn()
        ad_fill  = _fill_btn()
        ua_fill  = _fill_btn()

        def _col_hdr(text, btn):
            return widgets.HBox(
                [widgets.HTML(f"<b>{text}</b>", layout=widgets.Layout(flex='1')), btn],
                layout=widgets.Layout(width="110px"),
            )

        header_row = widgets.HBox([
            widgets.HTML("<b>Month</b>", layout=widgets.Layout(width="90px")),
            _col_hdr("CPI ($)",      cpi_fill),
            _col_hdr("IAP ARPDAU",   iap_fill),
            _col_hdr("Ad ARPDAU",    ad_fill),
            _col_hdr("UA Spend ($)", ua_fill),
        ])

        for _ in range(self.MAX_MONTHS):
            lbl   = widgets.Label("", layout=widgets.Layout(width="90px"))
            cpi_w = widgets.BoundedFloatText(value=0, min=0, max=1e9, step=0.01,  layout=widgets.Layout(width="110px"))
            iap_w = widgets.BoundedFloatText(value=0, min=0, max=1e9, step=0.01,  layout=widgets.Layout(width="110px"))
            ad_w  = widgets.BoundedFloatText(value=0, min=0, max=1e9, step=0.01,  layout=widgets.Layout(width="110px"))
            ua_w  = widgets.BoundedFloatText(value=0, min=0, max=1e9, step=1000,  layout=widgets.Layout(width="110px"))
            self._month_labels.append(lbl)
            self._cpi_inputs.append(cpi_w)
            self._iap_inputs.append(iap_w)
            self._ad_inputs.append(ad_w)
            self._ua_inputs.append(ua_w)
            self._data_rows.append(widgets.HBox([lbl, cpi_w, iap_w, ad_w, ua_w]))

        cpi_fill.on_click(lambda _: [
            w.__setattr__("value", self._cpi_inputs[0].value)
            for i, w in enumerate(self._cpi_inputs) if 0 < i < len(self._months)
        ])
        iap_fill.on_click(lambda _: [
            w.__setattr__("value", self._iap_inputs[0].value)
            for i, w in enumerate(self._iap_inputs) if 0 < i < len(self._months)
        ])
        ad_fill.on_click(lambda _: [
            w.__setattr__("value", self._ad_inputs[0].value)
            for i, w in enumerate(self._ad_inputs) if 0 < i < len(self._months)
        ])
        ua_fill.on_click(lambda _: [
            w.__setattr__("value", self._ua_inputs[0].value)
            for i, w in enumerate(self._ua_inputs) if 0 < i < len(self._months)
        ])

        self._apply_months(self._months)

        platform_label = "iOS" if platform == "ios" else "Android"
        self._box = widgets.VBox([
            _header(f"{platform_label} — Platform Inputs"),
            widgets.HBox([self.anchor_dau]),
            header_row,
            *self._data_rows,
        ], layout=widgets.Layout(border="1px solid #ddd", padding="10px", margin="4px"))

    def _apply_months(self, months: list[str]):
        for i, row in enumerate(self._data_rows):
            if i < len(months):
                self._month_labels[i].value = months[i]
                row.layout.display = ''
            else:
                row.layout.display = 'none'

    def update_months(self, start_month: str, n_months: int):
        new_months = _month_sequence(n_months, start_month)
        old_values = {
            m: {
                'cpi': self._cpi_inputs[i].value,
                'iap': self._iap_inputs[i].value,
                'ad':  self._ad_inputs[i].value,
                'ua':  self._ua_inputs[i].value,
            }
            for i, m in enumerate(self._months)
        }
        self._months = new_months
        self._apply_months(new_months)
        for i, m in enumerate(new_months):
            if m in old_values:
                self._cpi_inputs[i].value = old_values[m]['cpi']
                self._iap_inputs[i].value = old_values[m]['iap']
                self._ad_inputs[i].value  = old_values[m]['ad']
                self._ua_inputs[i].value  = old_values[m]['ua']
            else:
                self._cpi_inputs[i].value = 0
                self._iap_inputs[i].value = 0
                self._ad_inputs[i].value  = 0
                self._ua_inputs[i].value  = 0

    @property
    def values(self) -> dict:
        return {
            "monthly_cpi":        {m: round(w.value, 2) for m, w in zip(self._months, self._cpi_inputs)},
            "monthly_iap_arpdau": {m: round(w.value, 2) for m, w in zip(self._months, self._iap_inputs)},
            "monthly_ad_arpdau":  {m: round(w.value, 2) for m, w in zip(self._months, self._ad_inputs)},
            "monthly_ua_spend":   {m: round(w.value, 2) for m, w in zip(self._months, self._ua_inputs)},
            "anchor_dau":         round(self.anchor_dau.value, 2),
        }

    def set_monthly_values(self, monthly_cpi: dict, monthly_iap: dict, monthly_ad: dict,
                           monthly_ua: Optional[dict] = None):
        for m, cpi_w, iap_w, ad_w, ua_w in zip(
            self._months, self._cpi_inputs, self._iap_inputs, self._ad_inputs, self._ua_inputs
        ):
            if m in monthly_cpi: cpi_w.value = round(float(monthly_cpi[m]), 2)
            if m in monthly_iap: iap_w.value = round(float(monthly_iap[m]), 2)
            if m in monthly_ad:  ad_w.value  = round(float(monthly_ad[m]),  2)
            if monthly_ua and m in monthly_ua: ua_w.value = round(float(monthly_ua[m]), 2)

    def widget(self) -> widgets.VBox:
        return self._box


# ---------------------------------------------------------------------------
# Single-metric curve panel (retention OR conversion)
# ---------------------------------------------------------------------------

class CurvePanel:
    DX_POINTS = [1, 3, 7, 14, 30, 60, 90, 180, 365]

    def __init__(self, metric: str):
        self.metric = metric
        is_retention = metric == 'retention'
        label = "Retention" if is_retention else "Conversion"
        note  = "D0 is always 100%. " if is_retention else ""

        self._inputs = {'ios': [], 'android': []}

        header = widgets.HBox([
            widgets.HTML("<b>Day</b>",         layout=widgets.Layout(width="60px")),
            widgets.HTML("<b>iOS (%)</b>",     layout=widgets.Layout(width="130px")),
            widgets.HTML("<b>Android (%)</b>", layout=widgets.Layout(width="130px")),
        ])
        rows = [header]

        for dx in self.DX_POINTS:
            ios_w = widgets.BoundedFloatText(value=0, min=0, max=100, step=0.01, layout=widgets.Layout(width="130px"))
            and_w = widgets.BoundedFloatText(value=0, min=0, max=100, step=0.01, layout=widgets.Layout(width="130px"))
            self._inputs['ios'].append(ios_w)
            self._inputs['android'].append(and_w)
            rows.append(widgets.HBox([
                widgets.Label(f'D{dx}', layout=widgets.Layout(width="60px")),
                ios_w, and_w,
            ]))

        self._box = widgets.VBox([
            _header(f"{label} Curve Anchors"),
            widgets.HTML(f"<span style='font-size:11px;color:#888'>{note}Values as %. PCHIP-interpolated to D1–D365.</span>"),
            *rows,
        ], layout=widgets.Layout(padding="10px"))

    @property
    def values(self) -> dict:
        result = {}
        for platform in ('ios', 'android'):
            anchors = {dx: round(w.value / 100, 4) for dx, w in zip(self.DX_POINTS, self._inputs[platform])}
            if self.metric == 'retention':
                anchors[0] = 1.0
            result[platform] = anchors
        return result

    def set_values(self, ios: dict, android: dict):
        ios     = {int(k): v for k, v in ios.items()}
        android = {int(k): v for k, v in android.items()}
        for i, dx in enumerate(self.DX_POINTS):
            if dx in ios:     self._inputs['ios'][i].value     = round(float(ios[dx])     * 100, 2)
            if dx in android: self._inputs['android'][i].value = round(float(android[dx]) * 100, 2)

    def widget(self) -> widgets.VBox:
        return self._box


# ---------------------------------------------------------------------------
# Scenario controls
# ---------------------------------------------------------------------------

class ScenarioPanel:
    def __init__(self, saved_scenarios: list[str] = None):
        self._run_callbacks:        list[Callable] = []
        self._save_callbacks:       list[Callable] = []
        self._load_callbacks:       list[Callable] = []
        self._set_anchor_callbacks: list[Callable] = []

        # ---- top bar ----
        self.scenario_name = widgets.Text(
            value="scenario_1", description="Scenario name:",
            style={"description_width": "120px"}, layout=widgets.Layout(width="320px"),
        )
        self.forecast_start = widgets.DatePicker(
            description="Forecast start:", value=date.today(),
            style={"description_width": "120px"}, layout=widgets.Layout(width="280px"),
        )
        self.forecast_months = widgets.BoundedIntText(
            value=12, min=1, max=24, step=1,
            description="Months:",
            style={"description_width": "60px"}, layout=widgets.Layout(width="130px"),
        )

        # ---- load saved scenario ----
        saved = saved_scenarios or []
        self.load_dropdown = widgets.Dropdown(
            options=["— new scenario —"] + saved,
            description="Load saved:",
            style={"description_width": "120px"}, layout=widgets.Layout(width="320px"),
        )
        load_btn = widgets.Button(description="Load", button_style="info",
                                  layout=widgets.Layout(width="80px"))
        load_btn.on_click(self._on_load)

        # ---- platform panels ----
        start_month = date.today().strftime("%Y-%m")
        self.ios_panel     = PlatformPanel("ios",     n_months=12, start_month=start_month)
        self.android_panel = PlatformPanel("android", n_months=12, start_month=start_month)

        self.forecast_start.observe(self._on_forecast_params_change, names='value')
        self.forecast_months.observe(self._on_forecast_params_change, names='value')

        # ---- curve panels ----
        self.retention_panel  = CurvePanel('retention')
        self.conversion_panel = CurvePanel('conversion')

        # ---- tabbed input area ----
        input_tab = widgets.Tab(children=[
            widgets.HBox([self.ios_panel.widget(), self.android_panel.widget()]),
            self.retention_panel.widget(),
            self.conversion_panel.widget(),
        ])
        input_tab.set_title(0, 'Monthly inputs')
        input_tab.set_title(1, 'Retention')
        input_tab.set_title(2, 'Conversion')

        # ---- action buttons ----
        run_btn        = widgets.Button(description="Run simulation",          button_style="primary",
                                        layout=widgets.Layout(width="160px"))
        save_btn       = widgets.Button(description="Save scenario",           button_style="success",
                                        layout=widgets.Layout(width="160px"))
        set_anchor_btn = widgets.Button(description="Set anchor from actuals", button_style="warning",
                                        layout=widgets.Layout(width="200px"))
        self._status = widgets.HTML("")

        run_btn.on_click(self._on_run)
        save_btn.on_click(self._on_save)
        set_anchor_btn.on_click(self._on_set_anchor)

        self._box = widgets.VBox([
            _header("Game Simulator"),
            widgets.HBox([self.scenario_name, self.forecast_start, self.forecast_months]),
            widgets.HBox([self.load_dropdown, load_btn]),
            input_tab,
            widgets.HBox([run_btn, save_btn, set_anchor_btn]),
            self._status,
        ])

    # ---- public API ----

    def on_run(self, callback: Callable):
        self._run_callbacks = [callback]

    def on_save(self, callback: Callable):
        self._save_callbacks = [callback]

    def on_load(self, callback: Callable):
        self._load_callbacks = [callback]

    def on_set_anchor(self, callback: Callable):
        self._set_anchor_callbacks = [callback]

    def set_status(self, message: str, color: str = "green"):
        self._status.value = f"<span style='color:{color}'>{message}</span>"

    def get_ios_overrides(self) -> dict:
        return self.ios_panel.values

    def get_android_overrides(self) -> dict:
        return self.android_panel.values

    def get_curve_anchors(self) -> dict:
        ret  = self.retention_panel.values
        conv = self.conversion_panel.values
        return {
            'ios':     {'retention': ret['ios'],     'conversion': conv['ios']},
            'android': {'retention': ret['android'], 'conversion': conv['android']},
        }

    def get_forecast_start(self) -> date:
        val = self.forecast_start.value
        return val.date() if isinstance(val, datetime) else val

    def display(self):
        display(self._box)

    # ---- private ----

    def _on_forecast_params_change(self, _):
        start_val = self.forecast_start.value
        if start_val is None:
            return
        start_month = start_val.strftime("%Y-%m") if hasattr(start_val, 'strftime') else str(start_val)[:7]
        n_months = int(self.forecast_months.value)
        self.ios_panel.update_months(start_month, n_months)
        self.android_panel.update_months(start_month, n_months)

    def _on_run(self, _):
        import traceback
        self.set_status("Running...", "orange")
        try:
            for cb in self._run_callbacks:
                cb()
        except Exception as e:
            traceback.print_exc()
            self.set_status(f"Error: {e}", "red")

    def _on_save(self, _):
        for cb in self._save_callbacks:
            cb()

    def _on_set_anchor(self, _):
        import traceback
        try:
            for cb in self._set_anchor_callbacks:
                cb()
        except Exception as e:
            traceback.print_exc()
            self.set_status(f"Anchor error: {e}", "red")

    def _on_load(self, _):
        import traceback
        chosen = self.load_dropdown.value
        if chosen == "— new scenario —":
            return
        try:
            for cb in self._load_callbacks:
                cb(chosen)
        except Exception as e:
            traceback.print_exc()
            self.set_status(f"Load error: {e}", "red")
