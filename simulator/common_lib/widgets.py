"""
ipywidgets UI components for the game simulator.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
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


def _highlight(w: widgets.Widget, on: bool):
    if on:
        w.layout.border   = '2px solid #f9a825'
        w.style.background = '#fffde7'
    else:
        w.layout.border   = ''
        w.style.background = ''


# ---------------------------------------------------------------------------
# Per-platform monthly inputs panel  (CPI + UA spend only)
# ---------------------------------------------------------------------------

class PlatformPanel:
    MAX_MONTHS = 24

    def __init__(self, platform: str, n_months: int = 12, start_month: Optional[str] = None):
        self.platform = platform
        self._months = _month_sequence(n_months, start_month)

        self.anchor_dau   = _float_input(0.0, "Anchor DAU", step=1)
        self.avg_base_age = widgets.BoundedIntText(
            value=60, min=1, max=730, step=1,
            description="Avg base age:",
            style={"description_width": "100px"},
            layout=widgets.Layout(width="200px"),
        )
        self._set_anchor_btn = widgets.Button(
            description="Set from actuals", button_style="warning",
            layout=widgets.Layout(width="150px"),
        )
        self._set_anchor_callbacks: list[Callable] = []
        self._set_anchor_btn.on_click(lambda _: [cb() for cb in self._set_anchor_callbacks])

        self._cpi_inputs   = []
        self._ua_inputs    = []
        self._month_labels = []
        self._data_rows    = []

        cpi_fill = _fill_btn()
        ua_fill  = _fill_btn()

        def _col_hdr(text, btn):
            return widgets.VBox(
                [widgets.HTML(f"<b>{text}</b>"), btn],
                layout=widgets.Layout(width="110px"),
            )

        header_row = widgets.HBox([
            widgets.HTML("<b>Month</b>", layout=widgets.Layout(width="90px")),
            _col_hdr("CPI ($)",      cpi_fill),
            _col_hdr("UA Spend ($)", ua_fill),
        ])

        for _ in range(self.MAX_MONTHS):
            lbl   = widgets.Label("", layout=widgets.Layout(width="90px"))
            cpi_w = widgets.BoundedFloatText(value=0, min=0, max=1e9, step=0.01, layout=widgets.Layout(width="110px"))
            ua_w  = widgets.BoundedFloatText(value=0, min=0, max=1e9, step=1000, layout=widgets.Layout(width="110px"))
            self._month_labels.append(lbl)
            self._cpi_inputs.append(cpi_w)
            self._ua_inputs.append(ua_w)
            self._data_rows.append(widgets.HBox([lbl, cpi_w, ua_w]))

        cpi_fill.on_click(lambda _: [
            w.__setattr__("value", self._cpi_inputs[0].value)
            for i, w in enumerate(self._cpi_inputs) if 0 < i < len(self._months)
        ])
        ua_fill.on_click(lambda _: [
            w.__setattr__("value", self._ua_inputs[0].value)
            for i, w in enumerate(self._ua_inputs) if 0 < i < len(self._months)
        ])

        self._apply_months(self._months)

        platform_label = "iOS" if platform == "ios" else "Android"
        self._box = widgets.VBox([
            _header(f"{platform_label} — Platform Inputs"),
            widgets.HBox([self.anchor_dau, self.avg_base_age, self._set_anchor_btn]),
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
            m: {'cpi': self._cpi_inputs[i].value, 'ua': self._ua_inputs[i].value}
            for i, m in enumerate(self._months)
        }
        self._months = new_months
        self._apply_months(new_months)
        for i, m in enumerate(new_months):
            if m in old_values:
                self._cpi_inputs[i].value = old_values[m]['cpi']
                self._ua_inputs[i].value  = old_values[m]['ua']
            else:
                self._cpi_inputs[i].value = 0
                self._ua_inputs[i].value  = 0

    @property
    def values(self) -> dict:
        return {
            "monthly_cpi":    {m: round(w.value, 2) for m, w in zip(self._months, self._cpi_inputs)},
            "monthly_ua_spend": {m: round(w.value, 2) for m, w in zip(self._months, self._ua_inputs)},
            "anchor_dau":     round(self.anchor_dau.value, 2),
            "avg_base_age":   int(self.avg_base_age.value),
        }

    def on_set_anchor(self, callback: Callable):
        self._set_anchor_callbacks = [callback]

    def set_monthly_values(self, monthly_cpi: dict, monthly_ua: Optional[dict] = None):
        for m, cpi_w, ua_w in zip(self._months, self._cpi_inputs, self._ua_inputs):
            if m in monthly_cpi: cpi_w.value = round(float(monthly_cpi[m]), 2)
            if monthly_ua and m in monthly_ua: ua_w.value = round(float(monthly_ua[m]), 2)

    def widget(self) -> widgets.VBox:
        return self._box


# ---------------------------------------------------------------------------
# Single-metric curve panel (retention OR conversion) with override tracking
# ---------------------------------------------------------------------------

class CurvePanel:
    DX_POINTS = [1, 3, 7, 14, 30, 60, 90, 180, 365]

    def __init__(self, metric: str):
        self.metric = metric
        is_retention = metric == 'retention'
        label = "Retention" if is_retention else "Conversion"
        note  = "D0 is always 100%. " if is_retention else ""

        self._inputs:   dict[str, list[widgets.BoundedFloatText]] = {'ios': [], 'android': []}
        self._baseline: dict[str, dict[int, float]] = {'ios': {}, 'android': {}}
        self._loading = False

        header = widgets.HBox([
            widgets.HTML("<b>Day</b>",         layout=widgets.Layout(width="60px")),
            widgets.HTML("<b>iOS (%)</b>",     layout=widgets.Layout(width="130px")),
            widgets.HTML("<b>Android (%)</b>", layout=widgets.Layout(width="130px")),
        ])
        rows = [header]

        for i, dx in enumerate(self.DX_POINTS):
            ios_w = widgets.BoundedFloatText(value=0, min=0, max=100, step=0.01, layout=widgets.Layout(width="130px"))
            and_w = widgets.BoundedFloatText(value=0, min=0, max=100, step=0.01, layout=widgets.Layout(width="130px"))
            self._inputs['ios'].append(ios_w)
            self._inputs['android'].append(and_w)
            ios_w.observe(lambda change, _dx=dx, _p='ios':     self._on_change(_dx, _p, change), names='value')
            and_w.observe(lambda change, _dx=dx, _p='android': self._on_change(_dx, _p, change), names='value')
            rows.append(widgets.HBox([
                widgets.Label(f'D{dx}', layout=widgets.Layout(width="60px")),
                ios_w, and_w,
            ]))

        self._box = widgets.VBox([
            _header(f"{label} Curve Anchors"),
            widgets.HTML(f"<span style='font-size:11px;color:#888'>{note}Values as %. PCHIP-interpolated to D1–D365.</span>"),
            *rows,
        ], layout=widgets.Layout(padding="10px"))

    def _on_change(self, dx: int, platform: str, change):
        if self._loading:
            return
        baseline_val = self._baseline[platform].get(dx)
        w = self._inputs[platform][self.DX_POINTS.index(dx)]
        if baseline_val is not None:
            _highlight(w, abs(round(change['new'], 2) - round(baseline_val * 100, 2)) > 1e-9)
        else:
            _highlight(w, False)

    def _clear_highlights(self):
        for platform in ('ios', 'android'):
            for w in self._inputs[platform]:
                _highlight(w, False)

    @property
    def values(self) -> dict:
        result = {}
        for platform in ('ios', 'android'):
            anchors = {dx: round(w.value / 100, 4) for dx, w in zip(self.DX_POINTS, self._inputs[platform])}
            if self.metric == 'retention':
                anchors[0] = 1.0
            result[platform] = anchors
        return result

    def set_values(self, ios: dict, android: dict, is_baseline: bool = False):
        ios     = {int(k): v for k, v in ios.items()}
        android = {int(k): v for k, v in android.items()}
        self._loading = True
        try:
            for i, dx in enumerate(self.DX_POINTS):
                if dx in ios:     self._inputs['ios'][i].value     = round(float(ios[dx])     * 100, 2)
                if dx in android: self._inputs['android'][i].value = round(float(android[dx]) * 100, 2)
        finally:
            self._loading = False

        if is_baseline:
            self._baseline['ios']     = {dx: ios.get(dx, 0)     for dx in self.DX_POINTS}
            self._baseline['android'] = {dx: android.get(dx, 0) for dx in self.DX_POINTS}
            self._clear_highlights()
        else:
            for i, dx in enumerate(self.DX_POINTS):
                for platform, src in [('ios', ios), ('android', android)]:
                    if dx in src:
                        w   = self._inputs[platform][i]
                        bv  = self._baseline[platform].get(dx)
                        _highlight(w, bv is not None and abs(round(w.value, 2) - round(bv * 100, 2)) > 1e-9)

    def clear_baseline(self):
        self._baseline = {'ios': {}, 'android': {}}
        self._clear_highlights()

    def widget(self) -> widgets.VBox:
        return self._box


# ---------------------------------------------------------------------------
# ARPDAU panel — monthly IAP + Ad ARPDAU for both platforms with override tracking
# ---------------------------------------------------------------------------

class ARPDAUPanel:
    MAX_MONTHS = 24

    def __init__(self, n_months: int = 12, start_month: Optional[str] = None):
        self._months   = _month_sequence(n_months, start_month)
        self._baseline: dict[str, dict[str, dict[str, float]]] = {
            'ios':     {'iap': {}, 'ad': {}},
            'android': {'iap': {}, 'ad': {}},
        }
        self._loading = False

        # widget lists keyed by platform and metric
        self._inputs: dict[str, dict[str, list[widgets.BoundedFloatText]]] = {
            'ios':     {'iap': [], 'ad': []},
            'android': {'iap': [], 'ad': []},
        }
        self._month_labels: list[widgets.Label] = []
        self._data_rows:    list[widgets.HBox]  = []

        ios_iap_fill = _fill_btn()
        ios_ad_fill  = _fill_btn()
        and_iap_fill = _fill_btn()
        and_ad_fill  = _fill_btn()

        def _col_hdr(text, btn):
            return widgets.VBox(
                [widgets.HTML(f"<b>{text}</b>"), btn],
                layout=widgets.Layout(width="110px"),
            )

        header_row = widgets.HBox([
            widgets.HTML("<b>Month</b>",        layout=widgets.Layout(width="90px")),
            _col_hdr("iOS IAP ($)",   ios_iap_fill),
            _col_hdr("iOS Ad ($)",    ios_ad_fill),
            _col_hdr("And IAP ($)",   and_iap_fill),
            _col_hdr("And Ad ($)",    and_ad_fill),
        ])

        for _ in range(self.MAX_MONTHS):
            lbl         = widgets.Label("", layout=widgets.Layout(width="90px"))
            ios_iap_w   = widgets.BoundedFloatText(value=0, min=0, max=1e9, step=0.01, layout=widgets.Layout(width="110px"))
            ios_ad_w    = widgets.BoundedFloatText(value=0, min=0, max=1e9, step=0.01, layout=widgets.Layout(width="110px"))
            and_iap_w   = widgets.BoundedFloatText(value=0, min=0, max=1e9, step=0.01, layout=widgets.Layout(width="110px"))
            and_ad_w    = widgets.BoundedFloatText(value=0, min=0, max=1e9, step=0.01, layout=widgets.Layout(width="110px"))

            self._inputs['ios']['iap'].append(ios_iap_w)
            self._inputs['ios']['ad'].append(ios_ad_w)
            self._inputs['android']['iap'].append(and_iap_w)
            self._inputs['android']['ad'].append(and_ad_w)
            self._month_labels.append(lbl)
            self._data_rows.append(widgets.HBox([lbl, ios_iap_w, ios_ad_w, and_iap_w, and_ad_w]))

        # fill buttons — copy first visible row value to all others
        def _make_fill(platform, metric):
            def _fill(_):
                if not self._inputs[platform][metric]:
                    return
                v = self._inputs[platform][metric][0].value
                for i, w in enumerate(self._inputs[platform][metric]):
                    if 0 < i < len(self._months):
                        w.value = v
            return _fill

        ios_iap_fill.on_click(_make_fill('ios',     'iap'))
        ios_ad_fill.on_click( _make_fill('ios',     'ad'))
        and_iap_fill.on_click(_make_fill('android', 'iap'))
        and_ad_fill.on_click( _make_fill('android', 'ad'))

        # attach override observers
        for i, m in enumerate(_month_sequence(self.MAX_MONTHS, start_month)):
            for platform in ('ios', 'android'):
                for metric in ('iap', 'ad'):
                    w = self._inputs[platform][metric][i]
                    w.observe(
                        lambda change, _m=m, _p=platform, _k=metric: self._on_change(_m, _p, _k, change),
                        names='value',
                    )

        self._apply_months(self._months)

        self._box = widgets.VBox([
            _header("ARPDAU — Monthly Inputs"),
            widgets.HTML("<span style='font-size:11px;color:#888'>IAP and Ad ARPDAU per platform per month.</span>"),
            header_row,
            *self._data_rows,
        ], layout=widgets.Layout(padding="10px"))

    def _apply_months(self, months: list[str]):
        for i, row in enumerate(self._data_rows):
            if i < len(months):
                self._month_labels[i].value = months[i]
                row.layout.display = ''
            else:
                row.layout.display = 'none'

    def _on_change(self, month: str, platform: str, metric: str, change):
        if self._loading:
            return
        bv = self._baseline[platform][metric].get(month)
        try:
            i = self._months.index(month)
        except ValueError:
            return
        w = self._inputs[platform][metric][i]
        _highlight(w, bv is not None and abs(round(change['new'], 4) - round(bv, 4)) > 1e-9)

    def _clear_highlights(self):
        for platform in ('ios', 'android'):
            for metric in ('iap', 'ad'):
                for w in self._inputs[platform][metric]:
                    _highlight(w, False)

    def update_months(self, start_month: str, n_months: int):
        new_months = _month_sequence(n_months, start_month)
        old: dict[str, dict] = {}
        for i, m in enumerate(self._months):
            old[m] = {
                'ios_iap': self._inputs['ios']['iap'][i].value,
                'ios_ad':  self._inputs['ios']['ad'][i].value,
                'and_iap': self._inputs['android']['iap'][i].value,
                'and_ad':  self._inputs['android']['ad'][i].value,
            }
        self._months = new_months
        self._apply_months(new_months)
        for i, m in enumerate(new_months):
            v = old.get(m, {})
            self._inputs['ios']['iap'][i].value     = v.get('ios_iap', 0)
            self._inputs['ios']['ad'][i].value      = v.get('ios_ad',  0)
            self._inputs['android']['iap'][i].value = v.get('and_iap', 0)
            self._inputs['android']['ad'][i].value  = v.get('and_ad',  0)

    @property
    def values(self) -> dict:
        result = {}
        for platform in ('ios', 'android'):
            result[platform] = {
                'iap': {m: round(w.value, 4) for m, w in zip(self._months, self._inputs[platform]['iap'])},
                'ad':  {m: round(w.value, 4) for m, w in zip(self._months, self._inputs[platform]['ad'])},
            }
        return result

    def set_values(
        self,
        ios_iap:     dict,
        ios_ad:      dict,
        android_iap: dict,
        android_ad:  dict,
        is_baseline: bool = False,
    ):
        self._loading = True
        try:
            for i, m in enumerate(self._months):
                if m in ios_iap:     self._inputs['ios']['iap'][i].value     = round(float(ios_iap[m]),     4)
                if m in ios_ad:      self._inputs['ios']['ad'][i].value       = round(float(ios_ad[m]),      4)
                if m in android_iap: self._inputs['android']['iap'][i].value  = round(float(android_iap[m]), 4)
                if m in android_ad:  self._inputs['android']['ad'][i].value   = round(float(android_ad[m]),  4)
        finally:
            self._loading = False

        if is_baseline:
            self._baseline['ios']['iap']     = {m: ios_iap.get(m, 0)     for m in self._months}
            self._baseline['ios']['ad']      = {m: ios_ad.get(m, 0)      for m in self._months}
            self._baseline['android']['iap'] = {m: android_iap.get(m, 0) for m in self._months}
            self._baseline['android']['ad']  = {m: android_ad.get(m, 0)  for m in self._months}
            self._clear_highlights()
        else:
            for i, m in enumerate(self._months):
                for platform, src_iap, src_ad in [
                    ('ios',     ios_iap, ios_ad),
                    ('android', android_iap, android_ad),
                ]:
                    for metric, src in [('iap', src_iap), ('ad', src_ad)]:
                        if m in src:
                            w  = self._inputs[platform][metric][i]
                            bv = self._baseline[platform][metric].get(m)
                            _highlight(w, bv is not None and abs(round(w.value, 4) - round(bv, 4)) > 1e-9)

    def widget(self) -> widgets.VBox:
        return self._box


# ---------------------------------------------------------------------------
# Team cost panel — single-column monthly inputs
# ---------------------------------------------------------------------------

class TeamCostPanel:
    MAX_MONTHS = 24

    def __init__(self, n_months: int = 12, start_month: Optional[str] = None):
        self._months   = _month_sequence(n_months, start_month)
        self._baseline: dict[str, float] = {}
        self._loading  = False

        self._inputs:       list[widgets.BoundedFloatText] = []
        self._month_labels: list[widgets.Label]            = []
        self._data_rows:    list[widgets.HBox]             = []

        fill_btn = _fill_btn()

        def _col_hdr(text, btn):
            return widgets.VBox(
                [widgets.HTML(f"<b>{text}</b>"), btn],
                layout=widgets.Layout(width="150px"),
            )

        header_row = widgets.HBox([
            widgets.HTML("<b>Month</b>", layout=widgets.Layout(width="90px")),
            _col_hdr("Team Cost ($)",   fill_btn),
        ])

        for _ in range(self.MAX_MONTHS):
            lbl = widgets.Label("", layout=widgets.Layout(width="90px"))
            w   = widgets.BoundedFloatText(value=0, min=0, max=1e9, step=1000,
                                           layout=widgets.Layout(width="150px"))
            self._month_labels.append(lbl)
            self._inputs.append(w)
            self._data_rows.append(widgets.HBox([lbl, w]))

        fill_btn.on_click(lambda _: [
            w.__setattr__("value", self._inputs[0].value)
            for i, w in enumerate(self._inputs) if 0 < i < len(self._months)
        ])

        for i, m in enumerate(_month_sequence(self.MAX_MONTHS, start_month)):
            self._inputs[i].observe(
                lambda change, _m=m: self._on_change(_m, change),
                names='value',
            )

        self._apply_months(self._months)

        self._box = widgets.VBox([
            _header("Team Cost — Monthly Inputs"),
            widgets.HTML("<span style='font-size:11px;color:#888'>Monthly headcount/operating cost.</span>"),
            header_row,
            *self._data_rows,
        ], layout=widgets.Layout(padding="10px"))

    def _apply_months(self, months: list[str]):
        for i, row in enumerate(self._data_rows):
            if i < len(months):
                self._month_labels[i].value = months[i]
                row.layout.display = ''
            else:
                row.layout.display = 'none'

    def _on_change(self, month: str, change):
        if self._loading:
            return
        bv = self._baseline.get(month)
        try:
            i = self._months.index(month)
        except ValueError:
            return
        _highlight(self._inputs[i], bv is not None and abs(round(change['new'], 0) - round(bv, 0)) > 0.5)

    def _clear_highlights(self):
        for w in self._inputs:
            _highlight(w, False)

    def update_months(self, start_month: str, n_months: int):
        new_months = _month_sequence(n_months, start_month)
        old = {m: self._inputs[i].value for i, m in enumerate(self._months)}
        self._months = new_months
        self._apply_months(new_months)
        for i, m in enumerate(new_months):
            self._inputs[i].value = old.get(m, 0)

    @property
    def values(self) -> dict:
        return {m: round(w.value, 0) for m, w in zip(self._months, self._inputs)}

    def set_values(self, monthly_team_cost: dict, is_baseline: bool = False):
        self._loading = True
        try:
            for i, m in enumerate(self._months):
                if m in monthly_team_cost:
                    self._inputs[i].value = round(float(monthly_team_cost[m]), 0)
        finally:
            self._loading = False
        if is_baseline:
            self._baseline = {m: monthly_team_cost.get(m, 0) for m in self._months}
            self._clear_highlights()
        else:
            for i, m in enumerate(self._months):
                if m in monthly_team_cost:
                    bv = self._baseline.get(m)
                    _highlight(self._inputs[i],
                               bv is not None and abs(round(self._inputs[i].value, 0) - round(bv, 0)) > 0.5)

    def widget(self) -> widgets.VBox:
        return self._box


# ---------------------------------------------------------------------------
# Actuals date range selector
# ---------------------------------------------------------------------------

class ActualsRangePanel:
    _LABELS = {
        'retention':  'Retention',
        'conversion': 'Conversion',
        'arpdau':     'ARPDAU',
    }

    def __init__(self, metric: str):
        self._metric = metric
        self._load_callbacks: list[Callable] = []
        label = self._LABELS.get(metric, metric.capitalize())

        self._mode = widgets.ToggleButtons(
            options=[('Days back', 'days_back'), ('Date range', 'date_range')],
            value='days_back',
            style={'button_width': '100px', 'description_width': '0px'},
            layout=widgets.Layout(width='220px'),
        )
        self._days_back = widgets.BoundedIntText(
            value=90, min=7, max=730, step=1,
            description='Days back:',
            style={'description_width': '80px'},
            layout=widgets.Layout(width='180px'),
        )
        today = date.today()
        self._from_date = widgets.DatePicker(
            value=today - timedelta(days=90), description='From:',
            style={'description_width': '50px'}, layout=widgets.Layout(width='200px'),
        )
        self._to_date = widgets.DatePicker(
            value=today, description='To:',
            style={'description_width': '50px'}, layout=widgets.Layout(width='200px'),
        )
        self._load_btn = widgets.Button(
            description=f'Load {label} from actuals',
            button_style='info',
            layout=widgets.Layout(width='210px'),
        )
        self._load_btn.on_click(self._on_load)
        self._status = widgets.HTML("")

        self._days_back_box  = widgets.HBox([self._days_back])
        self._date_range_box = widgets.HBox([self._from_date, self._to_date])

        self._mode.observe(self._on_mode_change, names='value')
        self._on_mode_change(None)

        self._box = widgets.VBox([
            widgets.HBox([self._mode, self._load_btn]),
            self._days_back_box,
            self._date_range_box,
            self._status,
        ], layout=widgets.Layout(border='1px solid #b3d9ff', padding='8px', margin='0px 0px 8px 0px'))

    def _on_mode_change(self, _):
        if self._mode.value == 'days_back':
            self._days_back_box.layout.display  = ''
            self._date_range_box.layout.display = 'none'
        else:
            self._days_back_box.layout.display  = 'none'
            self._date_range_box.layout.display = ''

    def _on_load(self, _):
        for cb in self._load_callbacks:
            cb()

    def on_load(self, callback: Callable):
        self._load_callbacks = [callback]

    def set_status(self, message: str, color: str = 'green'):
        self._status.value = f"<span style='color:{color};font-size:11px'>{message}</span>"

    def get_range(self, forecast_start: Optional[date] = None) -> tuple[date, date]:
        if self._mode.value == 'days_back':
            ref   = forecast_start or date.today()
            end   = ref - timedelta(days=1)
            start = end - timedelta(days=int(self._days_back.value) - 1)
            return start, end
        else:
            from_val = self._from_date.value
            to_val   = self._to_date.value
            if hasattr(from_val, 'date'): from_val = from_val.date()
            if hasattr(to_val,   'date'): to_val   = to_val.date()
            return from_val, to_val

    def get_state(self) -> dict:
        return {
            'mode':      self._mode.value,
            'days_back': int(self._days_back.value),
            'from_date': str(self._from_date.value) if self._from_date.value else None,
            'to_date':   str(self._to_date.value)   if self._to_date.value   else None,
        }

    def set_state(self, state: dict):
        self._mode.value = state.get('mode', 'days_back')
        if state.get('days_back'):
            self._days_back.value = int(state['days_back'])
        if state.get('from_date'):
            self._from_date.value = date.fromisoformat(state['from_date'])
        if state.get('to_date'):
            self._to_date.value = date.fromisoformat(state['to_date'])

    def widget(self) -> widgets.VBox:
        return self._box


# ---------------------------------------------------------------------------
# Scenario controls
# ---------------------------------------------------------------------------

class ScenarioPanel:
    def __init__(self, saved_scenarios: list[str] = None):
        self._run_callbacks:  list[Callable] = []
        self._save_callbacks: list[Callable] = []
        self._load_callbacks: list[Callable] = []

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

        # ---- ARPDAU panel ----
        self.arpdau_panel = ARPDAUPanel(n_months=12, start_month=start_month)

        # ---- team cost panel ----
        self.team_cost_panel = TeamCostPanel(n_months=12, start_month=start_month)

        # ---- actuals range panels ----
        self.retention_actuals_panel  = ActualsRangePanel('retention')
        self.conversion_actuals_panel = ActualsRangePanel('conversion')
        self.arpdau_actuals_panel     = ActualsRangePanel('arpdau')

        # ---- tabbed input area ----
        input_tab = widgets.Tab(children=[
            widgets.HBox([self.ios_panel.widget(), self.android_panel.widget()]),
            widgets.VBox([self.retention_actuals_panel.widget(),  self.retention_panel.widget()]),
            widgets.VBox([self.conversion_actuals_panel.widget(), self.conversion_panel.widget()]),
            widgets.VBox([self.arpdau_actuals_panel.widget(),     self.arpdau_panel.widget()]),
            self.team_cost_panel.widget(),
        ])
        input_tab.set_title(0, 'Monthly inputs')
        input_tab.set_title(1, 'Retention')
        input_tab.set_title(2, 'Conversion')
        input_tab.set_title(3, 'ARPDAU')
        input_tab.set_title(4, 'Team Cost')

        # ---- output widgets (charts + table update after Run) ----
        self.charts_output = widgets.Output()
        self.table_output  = widgets.Output()

        # ---- action buttons ----
        run_btn  = widgets.Button(description="Run simulation", button_style="primary",
                                  layout=widgets.Layout(width="160px"))
        save_btn = widgets.Button(description="Save scenario",  button_style="success",
                                  layout=widgets.Layout(width="160px"))
        self._status = widgets.HTML("")

        run_btn.on_click(self._on_run)
        save_btn.on_click(self._on_save)

        self._box = widgets.VBox([
            _header("Game Simulator"),
            widgets.HBox([self.scenario_name, self.forecast_start, self.forecast_months]),
            widgets.HBox([self.load_dropdown, load_btn]),
            input_tab,
            widgets.HBox([run_btn, save_btn]),
            self._status,
        ])

    # ---- public API ----

    def on_run(self, callback: Callable):  self._run_callbacks  = [callback]
    def on_save(self, callback: Callable): self._save_callbacks = [callback]
    def on_load(self, callback: Callable): self._load_callbacks = [callback]

    def on_set_anchor(self, callback: Callable):
        self.ios_panel.on_set_anchor(callback)
        self.android_panel.on_set_anchor(callback)

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

    def get_arpdau(self) -> dict:
        return self.arpdau_panel.values

    def get_team_cost(self) -> dict:
        return self.team_cost_panel.values

    def get_actuals_range(self) -> dict:
        return {
            'retention':  self.retention_actuals_panel.get_state(),
            'conversion': self.conversion_actuals_panel.get_state(),
            'arpdau':     self.arpdau_actuals_panel.get_state(),
        }

    def set_actuals_range(self, state: dict):
        if state is None:
            return
        if 'retention'  in state: self.retention_actuals_panel.set_state(state['retention'])
        if 'conversion' in state: self.conversion_actuals_panel.set_state(state['conversion'])
        if 'arpdau'     in state: self.arpdau_actuals_panel.set_state(state['arpdau'])
        # backward compat: old flat format → apply to retention + conversion only
        if 'mode' in state:
            self.retention_actuals_panel.set_state(state)
            self.conversion_actuals_panel.set_state(state)

    def get_forecast_start(self) -> date:
        val = self.forecast_start.value
        return val.date() if isinstance(val, datetime) else val

    def display(self):
        display(self._box)
        display(self.charts_output)
        display(self.table_output)

    # ---- private ----

    def _on_forecast_params_change(self, _):
        start_val = self.forecast_start.value
        if start_val is None:
            return
        start_month = start_val.strftime("%Y-%m") if hasattr(start_val, 'strftime') else str(start_val)[:7]
        n_months    = int(self.forecast_months.value)
        self.ios_panel.update_months(start_month, n_months)
        self.android_panel.update_months(start_month, n_months)
        self.arpdau_panel.update_months(start_month, n_months)
        self.team_cost_panel.update_months(start_month, n_months)

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
