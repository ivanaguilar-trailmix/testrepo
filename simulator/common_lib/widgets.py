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


def _make_paste_section(columns: list[tuple[str, list]], months_ref: list) -> widgets.Accordion:
    """
    Collapsible bulk-paste area.
    columns = list of (label, BoundedFloatText list) — one entry per column.
    months_ref = the panel's live _months list (mutable reference, always current).
    Paste one value per line (or tab-separated rows from a spreadsheet).
    Non-numeric lines (headers etc.) are silently skipped.
    """
    textareas = []
    col_boxes  = []
    for label, _ in columns:
        ta = widgets.Textarea(
            placeholder=f"{label}\none per line",
            layout=widgets.Layout(width='115px', height='190px'),
        )
        textareas.append(ta)
        col_boxes.append(widgets.VBox([
            widgets.HTML(f"<b style='font-size:11px'>{label}</b>"), ta,
        ]))

    apply_btn = widgets.Button(description="Apply",  button_style="success", layout=widgets.Layout(width="70px"))
    clear_btn = widgets.Button(description="Clear",  button_style="",        layout=widgets.Layout(width="70px"))
    status    = widgets.HTML("")

    def _parse(text: str) -> list[float]:
        vals = []
        for raw in text.replace('\r', '\n').replace('\t', '\n').splitlines():
            s = raw.strip().lstrip('$').replace(',', '')
            if not s:
                continue
            try:
                vals.append(float(s))
            except ValueError:
                pass
        return vals

    def on_apply(_):
        n = len(months_ref)
        for ta, (_, inputs) in zip(textareas, columns):
            for i, v in enumerate(_parse(ta.value)[:n]):
                inputs[i].value = v
        status.value = "<span style='color:green;font-size:11px'>Applied</span>"

    def on_clear(_):
        for ta in textareas:
            ta.value = ''
        status.value = ""

    apply_btn.on_click(on_apply)
    clear_btn.on_click(on_clear)

    content = widgets.HBox([
        *col_boxes,
        widgets.VBox([apply_btn, clear_btn, status],
                     layout=widgets.Layout(padding='22px 0 0 8px')),
    ])
    acc = widgets.Accordion(children=[content])
    acc.set_title(0, 'Bulk paste')
    acc.selected_index = None
    return acc


def _highlight(w: widgets.Widget, on: bool):
    if on:
        w.layout.border   = '2px solid #f9a825'
        w.style.background = '#fffde7'
    else:
        w.layout.border   = ''
        w.style.background = ''


def _month_offset(ym: str, delta: int) -> str:
    """Return the month string 'YYYY-MM' that is delta months from ym."""
    y, m = int(ym[:4]), int(ym[5:7])
    m += delta
    y += (m - 1) // 12
    m = ((m - 1) % 12) + 1
    return f"{y}-{m:02d}"


def _month_count_inclusive(start_ym: str, end_ym: str) -> int:
    """Number of months from start_ym to end_ym, inclusive. Minimum 1."""
    sy, sm = int(start_ym[:4]), int(start_ym[5:7])
    ey, em = int(end_ym[:4]), int(end_ym[5:7])
    return max(1, (ey - sy) * 12 + (em - sm) + 1)


# ---------------------------------------------------------------------------
# Per-platform monthly inputs panel  (CPI + UA spend only)
# ---------------------------------------------------------------------------

class PlatformPanel:

    def __init__(self, platform: str, n_months: int = 12, start_month: Optional[str] = None):
        self.platform = platform
        self._months  = _month_sequence(n_months, start_month)
        self._loading = False

        self.anchor_dau = _float_input(0.0, "DAU", step=1)
        self._set_anchor_btn = widgets.Button(
            description="Set from actuals", button_style="warning",
            layout=widgets.Layout(width="150px"),
        )
        self._set_anchor_callbacks: list[Callable] = []
        self._set_anchor_btn.on_click(lambda _: [cb() for cb in self._set_anchor_callbacks])

        # age distribution table (one row per DX_POINTS entry)
        self._age_dist_inputs: list[widgets.BoundedFloatText] = []
        for _ in CurvePanel.DX_POINTS:
            w = widgets.BoundedFloatText(
                value=0.0, min=0.0, max=100.0, step=0.01,
                layout=widgets.Layout(width="100px"),
            )
            w.observe(lambda _c: self._update_age_total(), names='value')
            self._age_dist_inputs.append(w)
        self._age_total_label = widgets.HTML("")
        self._update_age_total()
        self._age_csv_upload = widgets.FileUpload(
            accept='.csv', multiple=False, description='Load CSV',
            layout=widgets.Layout(width='130px'),
        )
        self._age_csv_upload.observe(self._on_age_csv_upload, names='value')

        self._cpi_inputs:      list[widgets.BoundedFloatText] = []
        self._installs_labels: list[widgets.HTML]             = []
        self._month_labels:    list[widgets.Label]            = []
        self._data_rows:       list[widgets.HBox]             = []
        self._monthly_ua:      dict                           = {}

        cpi_fill = _fill_btn()

        def _col_hdr(text, btn):
            return widgets.VBox(
                [widgets.HTML(f"<b>{text}</b>"), btn],
                layout=widgets.Layout(width="110px"),
            )

        header_row = widgets.HBox([
            widgets.HTML("<b>Month</b>",       layout=widgets.Layout(width="90px")),
            _col_hdr("CPI ($)",                cpi_fill),
            widgets.HTML("<b>Installs/mo</b>", layout=widgets.Layout(width="100px", padding="4px 0 0 4px")),
        ])

        cpi_fill.on_click(lambda _: [
            w.__setattr__("value", self._cpi_inputs[0].value)
            for i, w in enumerate(self._cpi_inputs) if 0 < i < len(self._months)
        ])

        self._rows_box = widgets.VBox([])
        self._apply_months(self._months)

        paste_section = _make_paste_section(
            [("CPI ($)", self._cpi_inputs)],
            self._months,
        )

        platform_label = "iOS" if platform == "ios" else "Android"
        self._box = widgets.VBox([
            _header(f"{platform_label} — CPI"),
            header_row,
            self._rows_box,
            paste_section,
        ], layout=widgets.Layout(border="1px solid #ddd", padding="10px", margin="4px"))

    def _create_row(self) -> None:
        i        = len(self._data_rows)
        lbl      = widgets.Label("", layout=widgets.Layout(width="90px"))
        cpi_w    = widgets.BoundedFloatText(value=0, min=0, max=1e9, step=0.01, layout=widgets.Layout(width="110px"))
        inst_lbl = widgets.HTML("—", layout=widgets.Layout(width="100px", padding="4px 0 0 4px"))

        cpi_w.observe(lambda change, _i=i: self._refresh_installs_row(_i), names='value')

        self._month_labels.append(lbl)
        self._cpi_inputs.append(cpi_w)
        self._installs_labels.append(inst_lbl)
        self._data_rows.append(widgets.HBox([lbl, cpi_w, inst_lbl]))

    def _apply_months(self, months: list[str]):
        while len(self._data_rows) < len(months):
            self._create_row()
        for i, m in enumerate(months):
            self._month_labels[i].value = m
        self._rows_box.children = tuple(self._data_rows[:len(months)])

    def update_months(self, start_month: str, n_months: int):
        new_months = _month_sequence(n_months, start_month)
        old_cpi = {m: self._cpi_inputs[i].value for i, m in enumerate(self._months)}
        self._months[:] = new_months
        self._apply_months(new_months)
        self._loading = True
        try:
            for i, m in enumerate(new_months):
                new_cpi = old_cpi.get(m, 0)
                if self._cpi_inputs[i].value != new_cpi: self._cpi_inputs[i].value = new_cpi
        finally:
            self._loading = False
        for i in range(len(self._months)):
            self._refresh_installs_row(i)

    @property
    def values(self) -> dict:
        return {
            "monthly_cpi":      {m: round(w.value, 2) for m, w in zip(self._months, self._cpi_inputs)},
            "anchor_dau":       round(self.anchor_dau.value, 2),
            "age_distribution": self.age_distribution,
        }

    @property
    def age_distribution(self) -> dict:
        return {
            dx: round(w.value / 100, 6)
            for dx, w in zip(CurvePanel.DX_POINTS, self._age_dist_inputs)
            if w.value > 0
        }

    def set_age_distribution(self, dist: dict):
        dist = {int(k): float(v) for k, v in dist.items()}
        for i, dx in enumerate(CurvePanel.DX_POINTS):
            self._age_dist_inputs[i].value = round(dist.get(dx, 0.0) * 100, 2)

    def _update_age_total(self):
        total = sum(w.value for w in self._age_dist_inputs)
        color = "green" if abs(total - 100.0) < 0.5 else "red"
        self._age_total_label.value = f"<span style='color:{color};font-size:12px'>Total: {total:.1f}%</span>"

    def _on_age_csv_upload(self, change):
        if not change['new']:
            return
        try:
            file_obj = change['new'][0]
            try:
                content = bytes(file_obj['content'])
            except (TypeError, KeyError):
                content = bytes(file_obj.content)
            from common_lib.sheets import load_age_distribution_csv
            dist = load_age_distribution_csv(content)
            self.set_age_distribution(dist)
        except Exception as e:
            self._age_total_label.value = f"<span style='color:red;font-size:12px'>CSV error: {e}</span>"

    def on_set_anchor(self, callback: Callable):
        self._set_anchor_callbacks = [callback]

    def set_monthly_values(self, monthly_cpi: dict):
        for m, cpi_w in zip(self._months, self._cpi_inputs):
            if m in monthly_cpi: cpi_w.value = round(float(monthly_cpi[m]), 2)
        for i in range(len(self._months)):
            self._refresh_installs_row(i)

    def _refresh_installs_row(self, i: int):
        if i >= len(self._months):
            return
        m   = self._months[i]
        cpi = self._cpi_inputs[i].value
        ua  = self._monthly_ua.get(m, 0)
        self._installs_labels[i].value = f"{round(ua / cpi):,.0f}" if cpi > 0 and ua > 0 else "—"

    def set_ua_for_installs(self, monthly_ua: dict):
        self._monthly_ua = monthly_ua
        for i in range(len(self._months)):
            self._refresh_installs_row(i)

    def dau_widget(self) -> widgets.VBox:
        platform_label = "iOS" if self.platform == "ios" else "Android"
        header_row = widgets.HBox([
            widgets.HTML("<b>Day</b>",       layout=widgets.Layout(width="60px")),
            widgets.HTML("<b>% of base</b>", layout=widgets.Layout(width="110px")),
        ])
        rows = [
            widgets.HBox([
                widgets.Label(f"D{dx}", layout=widgets.Layout(width="60px")),
                w,
            ])
            for dx, w in zip(CurvePanel.DX_POINTS, self._age_dist_inputs)
        ]
        return widgets.VBox([
            _header(f"{platform_label} — DAU Parameters"),
            widgets.HBox([self.anchor_dau, self._set_anchor_btn]),
            widgets.HTML("<b style='font-size:12px'>Age distribution of existing base</b>"),
            widgets.HBox([self._age_csv_upload, self._age_total_label],
                         layout=widgets.Layout(align_items='center', margin='4px 0')),
            header_row,
            *rows,
        ], layout=widgets.Layout(border="1px solid #ddd", padding="10px", margin="4px"))

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

    def __init__(self, n_months: int = 12, start_month: Optional[str] = None):
        self._months   = _month_sequence(n_months, start_month)
        self._baseline: dict[str, dict[str, dict[str, float]]] = {
            'ios':     {'iap': {}, 'ad': {}},
            'android': {'iap': {}, 'ad': {}},
        }
        self._loading = False

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

        self._rows_box = widgets.VBox([])
        self._apply_months(self._months)

        paste_section = _make_paste_section(
            [
                ("iOS IAP ($)",  self._inputs['ios']['iap']),
                ("iOS Ad ($)",   self._inputs['ios']['ad']),
                ("And IAP ($)",  self._inputs['android']['iap']),
                ("And Ad ($)",   self._inputs['android']['ad']),
            ],
            self._months,
        )

        self._box = widgets.VBox([
            _header("ARPDAU — Monthly Inputs"),
            widgets.HTML("<span style='font-size:11px;color:#888'>IAP and Ad ARPDAU per platform per month.</span>"),
            header_row,
            self._rows_box,
            paste_section,
        ], layout=widgets.Layout(padding="10px"))

    def _create_row(self) -> None:
        i = len(self._data_rows)
        lbl         = widgets.Label("", layout=widgets.Layout(width="90px"))
        ios_iap_w   = widgets.BoundedFloatText(value=0, min=0, max=1e9, step=0.01, layout=widgets.Layout(width="110px"))
        ios_ad_w    = widgets.BoundedFloatText(value=0, min=0, max=1e9, step=0.01, layout=widgets.Layout(width="110px"))
        and_iap_w   = widgets.BoundedFloatText(value=0, min=0, max=1e9, step=0.01, layout=widgets.Layout(width="110px"))
        and_ad_w    = widgets.BoundedFloatText(value=0, min=0, max=1e9, step=0.01, layout=widgets.Layout(width="110px"))

        ios_iap_w.observe(lambda ch, _i=i: self._on_change_idx(_i, 'ios',     'iap', ch), names='value')
        ios_ad_w.observe( lambda ch, _i=i: self._on_change_idx(_i, 'ios',     'ad',  ch), names='value')
        and_iap_w.observe(lambda ch, _i=i: self._on_change_idx(_i, 'android', 'iap', ch), names='value')
        and_ad_w.observe( lambda ch, _i=i: self._on_change_idx(_i, 'android', 'ad',  ch), names='value')

        self._inputs['ios']['iap'].append(ios_iap_w)
        self._inputs['ios']['ad'].append(ios_ad_w)
        self._inputs['android']['iap'].append(and_iap_w)
        self._inputs['android']['ad'].append(and_ad_w)
        self._month_labels.append(lbl)
        self._data_rows.append(widgets.HBox([lbl, ios_iap_w, ios_ad_w, and_iap_w, and_ad_w]))

    def _apply_months(self, months: list[str]):
        while len(self._data_rows) < len(months):
            self._create_row()
        for i, m in enumerate(months):
            self._month_labels[i].value = m
        self._rows_box.children = tuple(self._data_rows[:len(months)])

    def _on_change_idx(self, i: int, platform: str, metric: str, change):
        if self._loading or i >= len(self._months):
            return
        m  = self._months[i]
        bv = self._baseline[platform][metric].get(m)
        w  = self._inputs[platform][metric][i]
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
        self._months[:] = new_months
        self._apply_months(new_months)
        self._loading = True
        try:
            for i, m in enumerate(new_months):
                v       = old.get(m, {})
                ios_iap = v.get('ios_iap', 0)
                ios_ad  = v.get('ios_ad',  0)
                and_iap = v.get('and_iap', 0)
                and_ad  = v.get('and_ad',  0)
                if self._inputs['ios']['iap'][i].value     != ios_iap: self._inputs['ios']['iap'][i].value     = ios_iap
                if self._inputs['ios']['ad'][i].value      != ios_ad:  self._inputs['ios']['ad'][i].value      = ios_ad
                if self._inputs['android']['iap'][i].value != and_iap: self._inputs['android']['iap'][i].value = and_iap
                if self._inputs['android']['ad'][i].value  != and_ad:  self._inputs['android']['ad'][i].value  = and_ad
        finally:
            self._loading = False

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

    def __init__(self, n_months: int = 12, start_month: Optional[str] = None,
                 label: str = "Team Cost"):
        self._months   = _month_sequence(n_months, start_month)
        self._baseline: dict[str, float] = {}
        self._loading  = False

        self._inputs:       list[widgets.BoundedFloatText] = []
        self._month_labels: list[widgets.Label]            = []
        self._data_rows:    list[widgets.HBox]             = []

        fill_btn = _fill_btn()
        self._load_csv_btn = widgets.Button(
            description="Load from CSV", button_style="info",
            layout=widgets.Layout(width="130px"),
        )
        self._load_csv_callbacks: list[Callable] = []
        self._load_csv_btn.on_click(lambda _: [cb() for cb in self._load_csv_callbacks])

        def _col_hdr(text, btn):
            return widgets.VBox(
                [widgets.HTML(f"<b>{text}</b>"), btn],
                layout=widgets.Layout(width="150px"),
            )

        header_row = widgets.HBox([
            widgets.HTML("<b>Month</b>", layout=widgets.Layout(width="90px")),
            _col_hdr(f"{label} ($)",    fill_btn),
        ])

        fill_btn.on_click(lambda _: [
            w.__setattr__("value", self._inputs[0].value)
            for i, w in enumerate(self._inputs) if 0 < i < len(self._months)
        ])

        self._rows_box = widgets.VBox([])
        self._apply_months(self._months)

        paste_section = _make_paste_section(
            [(f"{label} ($)", self._inputs)],
            self._months,
        )

        self._box = widgets.VBox([
            _header(f"{label} — Monthly Inputs"),
            widgets.HBox([
                widgets.HTML("<span style='font-size:11px;color:#888'>Monthly cost inputs.</span>"),
                self._load_csv_btn,
            ]),
            header_row,
            self._rows_box,
            paste_section,
        ], layout=widgets.Layout(padding="10px"))

    def _create_row(self) -> None:
        i = len(self._data_rows)
        lbl = widgets.Label("", layout=widgets.Layout(width="90px"))
        w   = widgets.BoundedFloatText(value=0, min=0, max=1e9, step=1000,
                                       layout=widgets.Layout(width="150px"))
        w.observe(lambda change, _i=i: self._on_change_idx(_i, change), names='value')
        self._month_labels.append(lbl)
        self._inputs.append(w)
        self._data_rows.append(widgets.HBox([lbl, w]))

    def _apply_months(self, months: list[str]):
        while len(self._data_rows) < len(months):
            self._create_row()
        for i, m in enumerate(months):
            self._month_labels[i].value = m
        self._rows_box.children = tuple(self._data_rows[:len(months)])

    def _on_change_idx(self, i: int, change):
        if self._loading or i >= len(self._months):
            return
        m  = self._months[i]
        bv = self._baseline.get(m)
        _highlight(self._inputs[i], bv is not None and abs(round(change['new'], 0) - round(bv, 0)) > 0.5)

    def _clear_highlights(self):
        for w in self._inputs:
            _highlight(w, False)

    def update_months(self, start_month: str, n_months: int):
        new_months = _month_sequence(n_months, start_month)
        old = {m: self._inputs[i].value for i, m in enumerate(self._months)}
        self._months[:] = new_months
        self._apply_months(new_months)
        self._loading = True
        try:
            for i, m in enumerate(new_months):
                new_val = old.get(m, 0)
                if self._inputs[i].value != new_val:
                    self._inputs[i].value = new_val
        finally:
            self._loading = False

    @property
    def values(self) -> dict:
        return {m: round(w.value, 0) for m, w in zip(self._months, self._inputs)}

    def on_load_csv(self, callback: Callable):
        self._load_csv_callbacks = [callback]

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
# Combined UA budget panel (total monthly spend + per-month iOS/Android split)
# ---------------------------------------------------------------------------

class UABudgetPanel:

    def __init__(self, n_months: int = 12, start_month: Optional[str] = None):
        self._months           = _month_sequence(n_months, start_month)
        self._baseline_budget:  dict[str, float] = {}
        self._baseline_ios_pct: dict[str, float] = {}
        self._loading = False

        self._budget_inputs:        list[widgets.BoundedFloatText] = []
        self._ios_pct_inputs:       list[widgets.BoundedFloatText] = []
        self._ios_spend_labels:     list[widgets.HTML]             = []
        self._android_spend_labels: list[widgets.HTML]             = []
        self._month_labels:         list[widgets.Label]            = []
        self._data_rows:            list[widgets.HBox]             = []
        self._change_callbacks:     list[Callable]                 = []

        budget_fill  = _fill_btn()
        ios_pct_fill = _fill_btn()

        def _col_hdr(text, btn, width="130px"):
            return widgets.VBox(
                [widgets.HTML(f"<b>{text}</b>"), btn],
                layout=widgets.Layout(width=width),
            )

        header_row = widgets.HBox([
            widgets.HTML("<b>Month</b>",     layout=widgets.Layout(width="90px")),
            _col_hdr("Total Budget ($)",     budget_fill,  "130px"),
            _col_hdr("iOS %",                ios_pct_fill, "80px"),
            widgets.HTML("<b>iOS $</b>",     layout=widgets.Layout(width="100px")),
            widgets.HTML("<b>Android $</b>", layout=widgets.Layout(width="100px")),
        ])

        budget_fill.on_click(lambda _: [
            w.__setattr__("value", self._budget_inputs[0].value)
            for i, w in enumerate(self._budget_inputs) if 0 < i < len(self._months)
        ])
        ios_pct_fill.on_click(lambda _: [
            w.__setattr__("value", self._ios_pct_inputs[0].value)
            for i, w in enumerate(self._ios_pct_inputs) if 0 < i < len(self._months)
        ])

        self._rows_box = widgets.VBox([])
        self._apply_months(self._months)

        paste_section = _make_paste_section(
            [("Budget ($)", self._budget_inputs), ("iOS %", self._ios_pct_inputs)],
            self._months,
        )

        self._box = widgets.VBox([
            _header("UA Budget — Monthly Inputs"),
            widgets.HTML("<span style='font-size:11px;color:#888'>Total UA spend per month. iOS% drives the platform split; Android gets the remainder.</span>"),
            header_row,
            self._rows_box,
            paste_section,
        ], layout=widgets.Layout(padding="10px"))

    def _create_row(self) -> None:
        i           = len(self._data_rows)
        lbl         = widgets.Label("", layout=widgets.Layout(width="90px"))
        budget_w    = widgets.BoundedFloatText(value=0,  min=0, max=1e9, step=1000, layout=widgets.Layout(width="130px"))
        ios_pct_w        = widgets.BoundedFloatText(value=50, min=0, max=100, step=1, layout=widgets.Layout(width="80px"))
        ios_spend_lbl    = widgets.HTML("$0", layout=widgets.Layout(width="100px", padding="4px 0 0 8px"))
        android_spend_lbl = widgets.HTML("$0", layout=widgets.Layout(width="100px", padding="4px 0 0 8px"))

        budget_w.observe(  lambda change, _i=i: self._on_change_idx(_i, 'budget',  change), names='value')
        ios_pct_w.observe( lambda change, _i=i: self._on_change_idx(_i, 'ios_pct', change), names='value')

        self._month_labels.append(lbl)
        self._budget_inputs.append(budget_w)
        self._ios_pct_inputs.append(ios_pct_w)
        self._ios_spend_labels.append(ios_spend_lbl)
        self._android_spend_labels.append(android_spend_lbl)
        self._data_rows.append(widgets.HBox([lbl, budget_w, ios_pct_w, ios_spend_lbl, android_spend_lbl]))

    def _apply_months(self, months: list[str]):
        while len(self._data_rows) < len(months):
            self._create_row()
        for i, m in enumerate(months):
            self._month_labels[i].value = m
        self._rows_box.children = tuple(self._data_rows[:len(months)])

    def _on_change_idx(self, i: int, col: str, change):
        if self._loading or i >= len(self._months):
            return
        m = self._months[i]
        if col == 'budget':
            bv = self._baseline_budget.get(m)
            _highlight(self._budget_inputs[i], bv is not None and abs(round(change['new'], 0) - round(bv, 0)) > 0.5)
        else:
            bv = self._baseline_ios_pct.get(m)
            _highlight(self._ios_pct_inputs[i], bv is not None and abs(round(change['new'], 1) - round(bv, 1)) > 0.1)
        self._update_spend_labels(i)
        for cb in self._change_callbacks:
            cb()

    def _update_spend_labels(self, i: int):
        budget = self._budget_inputs[i].value
        pct    = self._ios_pct_inputs[i].value
        ios_s  = round(budget * pct / 100)
        and_s  = round(budget * (1 - pct / 100))
        self._ios_spend_labels[i].value     = f"${ios_s:,.0f}"
        self._android_spend_labels[i].value = f"${and_s:,.0f}"

    def on_change(self, callback: Callable):
        self._change_callbacks.append(callback)

    def _clear_highlights(self):
        for w in self._budget_inputs:  _highlight(w, False)
        for w in self._ios_pct_inputs: _highlight(w, False)

    def update_months(self, start_month: str, n_months: int):
        new_months  = _month_sequence(n_months, start_month)
        old_budget  = {m: self._budget_inputs[i].value  for i, m in enumerate(self._months)}
        old_ios_pct = {m: self._ios_pct_inputs[i].value for i, m in enumerate(self._months)}
        self._months[:] = new_months
        self._apply_months(new_months)
        self._loading = True
        try:
            for i, m in enumerate(new_months):
                new_b = old_budget.get(m,   0)
                new_p = old_ios_pct.get(m, 50)
                if self._budget_inputs[i].value  != new_b: self._budget_inputs[i].value  = new_b
                if self._ios_pct_inputs[i].value != new_p: self._ios_pct_inputs[i].value = new_p
                self._update_spend_labels(i)
        finally:
            self._loading = False

    @property
    def values(self) -> dict:
        return {
            'monthly_budget':  {m: round(w.value, 0) for m, w in zip(self._months, self._budget_inputs)},
            'monthly_ios_pct': {m: round(w.value, 1) for m, w in zip(self._months, self._ios_pct_inputs)},
        }

    def get_ios_ua(self, forecast_ym: str) -> dict:
        return {
            m: round(b.value * p.value / 100, 0)
            for m, b, p in zip(self._months, self._budget_inputs, self._ios_pct_inputs)
            if m >= forecast_ym
        }

    def get_android_ua(self, forecast_ym: str) -> dict:
        return {
            m: round(b.value * (1 - p.value / 100), 0)
            for m, b, p in zip(self._months, self._budget_inputs, self._ios_pct_inputs)
            if m >= forecast_ym
        }

    def get_historical_ua(self, forecast_ym: str) -> dict:
        return {
            m: round(b.value, 0)
            for m, b in zip(self._months, self._budget_inputs)
            if m < forecast_ym
        }

    def set_values(self, monthly_budget: dict, monthly_ios_pct: dict, is_baseline: bool = False):
        self._loading = True
        try:
            for i, m in enumerate(self._months):
                new_b = float(monthly_budget.get(m,  self._budget_inputs[i].value))
                new_p = float(monthly_ios_pct.get(m, self._ios_pct_inputs[i].value))
                if self._budget_inputs[i].value  != new_b: self._budget_inputs[i].value  = new_b
                if self._ios_pct_inputs[i].value != new_p: self._ios_pct_inputs[i].value = new_p
                self._update_spend_labels(i)
        finally:
            self._loading = False
        for cb in self._change_callbacks:
            cb()
        if is_baseline:
            self._baseline_budget  = {m: float(monthly_budget.get(m,  0))  for m in self._months}
            self._baseline_ios_pct = {m: float(monthly_ios_pct.get(m, 50)) for m in self._months}
            self._clear_highlights()
        else:
            for i, m in enumerate(self._months):
                if m in monthly_budget:
                    bv = self._baseline_budget.get(m)
                    _highlight(self._budget_inputs[i],
                               bv is not None and abs(round(self._budget_inputs[i].value, 0) - round(bv, 0)) > 0.5)
                if m in monthly_ios_pct:
                    bv = self._baseline_ios_pct.get(m)
                    _highlight(self._ios_pct_inputs[i],
                               bv is not None and abs(round(self._ios_pct_inputs[i].value, 1) - round(bv, 1)) > 0.1)

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
        self.team_cost_panel = TeamCostPanel(n_months=12, start_month=start_month, label="Team Cost")

        # ---- UA budget panel ----
        self.ua_budget_panel = UABudgetPanel(n_months=12, start_month=start_month)
        self.ua_budget_panel.on_change(self._sync_installs)

        # ---- actuals range panels ----
        self.retention_actuals_panel  = ActualsRangePanel('retention')
        self.conversion_actuals_panel = ActualsRangePanel('conversion')
        self.arpdau_actuals_panel     = ActualsRangePanel('arpdau')

        # ---- tabbed input area ----
        input_tab = widgets.Tab(children=[
            widgets.HBox([self.ios_panel.dau_widget(), self.android_panel.dau_widget()]),
            widgets.HBox([self.ios_panel.widget(), self.android_panel.widget()]),
            widgets.VBox([self.retention_actuals_panel.widget(),  self.retention_panel.widget()]),
            widgets.VBox([self.conversion_actuals_panel.widget(), self.conversion_panel.widget()]),
            widgets.VBox([self.arpdau_actuals_panel.widget(),     self.arpdau_panel.widget()]),
            self.team_cost_panel.widget(),
            self.ua_budget_panel.widget(),
        ])
        input_tab.set_title(0, 'DAU')
        input_tab.set_title(1, 'CPI')
        input_tab.set_title(2, 'Retention')
        input_tab.set_title(3, 'Conversion')
        input_tab.set_title(4, 'ARPDAU')
        input_tab.set_title(5, 'Team Cost')
        input_tab.set_title(6, 'UA Budget')

        # ---- result area (populated after Run via set_chart_results) ----
        self._chart_btns_box  = widgets.HBox([], layout=widgets.Layout(
            flex_wrap='wrap', margin='8px 0 4px 0',
        ))
        self._result_display  = widgets.VBox([])
        self._cached_results: dict = {}

        # ---- actuals history depth ----
        self._actuals_months = widgets.BoundedIntText(
            value=12, min=1, max=48, step=1,
            description='Actuals Months:',
            style={'description_width': '105px'},
            layout=widgets.Layout(width='190px'),
        )
        self._actuals_months.observe(self._on_forecast_params_change, names='value')
        self._on_forecast_params_change(None)  # initialise extended panels with correct month range

        # ---- chart checkboxes ----
        self._chart_checks = {
            'dau':      widgets.Checkbox(value=True,  description='DAU',      indent=False, layout=widgets.Layout(width='70px')),
            'installs': widgets.Checkbox(value=False, description='Installs', indent=False, layout=widgets.Layout(width='85px')),
            'revenue':  widgets.Checkbox(value=True,  description='Revenue',  indent=False, layout=widgets.Layout(width='85px')),
            'payers':   widgets.Checkbox(value=False, description='Payers',   indent=False, layout=widgets.Layout(width='75px')),
            'monthly':  widgets.Checkbox(value=True,  description='Monthly',  indent=False, layout=widgets.Layout(width='85px')),
        }

        # ---- action buttons ----
        run_btn  = widgets.Button(description="Run simulation", button_style="primary",
                                  layout=widgets.Layout(width="155px"))
        save_btn = widgets.Button(description="Save scenario",  button_style="success",
                                  layout=widgets.Layout(width="145px"))
        self._status = widgets.HTML("")

        run_btn.on_click(self._on_run)
        save_btn.on_click(self._on_save)

        divider = widgets.HTML("<span style='color:#ccc;padding:0 8px'>│</span>")

        self._box = widgets.VBox([
            _header("Game Simulator"),
            widgets.HBox([
                self.scenario_name, self.forecast_start,
                self.forecast_months, self._actuals_months,
            ]),
            widgets.HBox([
                self.load_dropdown, load_btn, divider,
                widgets.HTML("<span style='margin:4px 4px 0 4px;font-size:12px'><b>Charts:</b></span>"),
                *self._chart_checks.values(),
            ]),
            input_tab,
            widgets.HBox([run_btn, save_btn, self._status],
                         layout=widgets.Layout(align_items='center')),
            self._chart_btns_box,
            self._result_display,
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
        forecast_ym = self.get_forecast_start().strftime("%Y-%m")
        v = self.ios_panel.values
        v['monthly_ua_spend'] = self.ua_budget_panel.get_ios_ua(forecast_ym)
        return v

    def get_android_overrides(self) -> dict:
        forecast_ym = self.get_forecast_start().strftime("%Y-%m")
        v = self.android_panel.values
        v['monthly_ua_spend'] = self.ua_budget_panel.get_android_ua(forecast_ym)
        return v

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

    def get_actuals_from(self) -> date:
        start = self.get_forecast_start()
        ym    = _month_offset(start.strftime("%Y-%m"), -int(self._actuals_months.value))
        return date(int(ym[:4]), int(ym[5:7]), 1)

    def set_actuals_from(self, d):
        if d is None:
            return
        if isinstance(d, str):
            d = date.fromisoformat(d)
        if hasattr(d, 'date'):
            d = d.date()
        start = self.get_forecast_start()
        n = max(1, (start.year - d.year) * 12 + (start.month - d.month))
        self._actuals_months.value = n

    def set_selected_charts(self, charts: list):
        if not charts:
            return
        for key, cb in self._chart_checks.items():
            cb.value = key in charts

    def get_selected_charts(self) -> list[str]:
        return [k for k, cb in self._chart_checks.items() if cb.value]

    def get_historical_marketing(self) -> dict:
        forecast_ym = self.get_forecast_start().strftime("%Y-%m")
        return self.ua_budget_panel.get_historical_ua(forecast_ym)

    def set_historical_marketing(self, data: dict):
        pass  # handled in app.py during load_saved_scenario

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

    def set_chart_results(self, chart_widgets: dict, table_widget) -> None:
        """Populate the button-bar and result area after a successful Run."""
        _LABELS = {
            'dau': 'DAU', 'installs': 'Installs', 'revenue': 'Revenue',
            'payers': 'Payers', 'monthly': 'Monthly', 'table': 'P&L Table',
        }
        self._cached_results = {**chart_widgets, 'table': table_widget}
        btns = []
        for key in list(chart_widgets.keys()) + ['table']:
            btn = widgets.Button(
                description=_LABELS.get(key, key),
                button_style='info',
                layout=widgets.Layout(width='105px', margin='0 4px 4px 0'),
            )
            btn.on_click(lambda _, k=key: self._show_result(k))
            btns.append(btn)
        self._chart_btns_box.children = tuple(btns)
        first = next(iter(self._cached_results), None)
        if first:
            self._show_result(first)

    def _show_result(self, key: str) -> None:
        w = self._cached_results.get(key)
        if w is not None:
            self._result_display.children = (w,)

    def display(self):
        if getattr(self, '_displayed', False):
            return
        self._displayed = True
        display(self._box)

    # ---- private ----

    def _on_forecast_params_change(self, _):
        start_val = self.forecast_start.value
        if start_val is None:
            return
        forecast_ym = start_val.strftime("%Y-%m") if hasattr(start_val, 'strftime') else str(start_val)[:7]
        n_forecast  = int(self.forecast_months.value)

        # Forecast-range panels
        self.ios_panel.update_months(forecast_ym, n_forecast)
        self.android_panel.update_months(forecast_ym, n_forecast)
        self.arpdau_panel.update_months(forecast_ym, n_forecast)

        # Extended cost panels — need actuals_from
        if not hasattr(self, '_actuals_months'):
            return
        actuals_ym = self.get_actuals_from().strftime("%Y-%m")

        # Extended panels: actuals_from → end of forecast
        forecast_end_ym = _month_offset(forecast_ym, n_forecast - 1)
        n_extended = _month_count_inclusive(actuals_ym, forecast_end_ym)
        self.team_cost_panel.update_months(actuals_ym, n_extended)
        self.ua_budget_panel.update_months(actuals_ym, n_extended)
        self._sync_installs()

    def _sync_installs(self):
        forecast_ym = self.get_forecast_start().strftime("%Y-%m")
        self.ios_panel.set_ua_for_installs(self.ua_budget_panel.get_ios_ua(forecast_ym))
        self.android_panel.set_ua_for_installs(self.ua_budget_panel.get_android_ua(forecast_ym))

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
