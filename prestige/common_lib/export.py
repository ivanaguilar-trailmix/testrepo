import json
import re
import nbformat
from nbconvert import HTMLExporter
from nbconvert.preprocessors import Preprocessor
from pathlib import Path
from plotly.offline import get_plotlyjs

_DEFAULTS = {
    'toc_width': 250,       # sidebar width in px
    'toc_max_level': 2,     # deepest heading level shown in TOC (1=h1 only, 2=h1+h2, …)
    'buttons_visible': False,  # whether cell toggle buttons are shown on initial load
}


class _PlotlyHTMLPreprocessor(Preprocessor):
    """
    Replace plotly JSON outputs with inline HTML.
    Keeps original width/height so the chart renders at its natural size
    with the correct aspect ratio.
    """
    def preprocess_cell(self, cell, resources, index):
        new_outputs = []
        for i, output in enumerate(getattr(cell, 'outputs', [])):
            data = getattr(output, 'data', {})
            if 'application/vnd.plotly.v1+json' in data:
                fig = data['application/vnd.plotly.v1+json']
                uid = f'plotly-{index}-{i}'
                orig_w = fig.get('layout', {}).get('width',  900)
                orig_h = fig.get('layout', {}).get('height', 500)
                layout = dict(fig.get('layout', {}))
                layout['width']  = orig_w
                layout['height'] = orig_h
                html = (
                    f'<div id="{uid}" style="width:{orig_w}px;height:{orig_h}px;"></div>'
                    f'<script>Plotly.newPlot("{uid}",'
                    f'{json.dumps(fig.get("data", []))},'
                    f'{json.dumps(layout)},'
                    f'{{"responsive":false}});</script>'
                )
                new_outputs.append(nbformat.from_dict({
                    'output_type': 'display_data',
                    'data': {'text/html': html},
                    'metadata': {},
                }))
            else:
                new_outputs.append(output)
        cell['outputs'] = new_outputs
        return cell, resources


def _build_toc(html, max_level=2):
    headings = re.findall(
        rf'<(h[1-{max_level}])[^>]*id="([^"]+)"[^>]*>(.*?)</h[1-{max_level}]>',
        html, re.DOTALL
    )
    items = []
    for tag, anchor_id, raw_text in headings:
        level = int(tag[1])
        text = re.sub(r'<[^>]+>', '', raw_text).replace('¶', '').strip()
        indent = 'style="margin-left:{}px"'.format((level - 1) * 14)
        items.append(f'<li {indent}><a href="#{anchor_id}">{text}</a></li>')
    return (
        '<nav id="toc"><div id="toc-title">Contents</div>'
        '<div id="toc-controls"><span id="toggle-buttons-btn" onclick="toggleCellButtons()">Show buttons</span></div>'
        f'<ul>{"".join(items)}</ul></nav>'
    )


def _get_cell_config(nb):
    """
    Read per-cell display config from first-line markers.

    Code cells (first-line comment):
      # show          -> show code input by default
      # hide-output   -> hide output by default
      # show hide-output  -> combine both

    Markdown cells (first-line HTML comment):
      <!-- hide -->     -> hide cell content by default (toggle to show); also excluded from TOC
      <!-- hide-toc --> -> cell visible but headings excluded from TOC
    """
    code_config = {}
    md_config = {}
    code_idx = md_idx = 0
    for cell in nb.cells:
        if cell.cell_type == 'code':
            first_line = (cell.source.split('\n')[0] if cell.source else '').strip()
            code_config[code_idx] = {
                'showInput':  bool(re.search(r'#.*\bshow\b', first_line)),
                'hideOutput': bool(re.search(r'#.*\bhide-output\b', first_line)),
            }
            code_idx += 1
        elif cell.cell_type == 'markdown':
            first_line = (cell.source.split('\n')[0] if cell.source else '').strip()
            md_config[md_idx] = {
                'hideCell': first_line == '<!-- hide -->',
                'hideToc':  first_line in ('<!-- hide -->', '<!-- hide-toc -->'),
            }
            md_idx += 1
    return code_config, md_config


def export_notebook_html(notebook_path, output_path=None, toc_width=None, config=None):
    """
    Export a Jupyter notebook to a self-contained HTML file with:
    - Inline plotly.js (no CDN dependency)
    - Interactive plotly charts at their original aspect ratio
    - Fixed sidebar table of contents
    - Collapsible code inputs (hidden by default) and outputs (visible by default)
    - Per-cell overrides via first-line markers:
        Code cells:     # show, # hide-output
        Markdown cells: <!-- hide -->      (hides cell + excludes headings from TOC; toggle to show)
                        <!-- hide-toc -->  (cell visible but headings excluded from TOC)

    Parameters
    ----------
    notebook_path : str or Path
    output_path   : str or Path, optional — defaults to notebook_path with .html extension
    toc_width     : int, optional — sidebar width in px; overrides config value
    config        : dict, optional — keys override _DEFAULTS (see export_config.py for all options)

    Returns
    -------
    Path of the written HTML file.
    """
    notebook_path = Path(notebook_path)
    if output_path is None:
        output_path = notebook_path.with_suffix('.html')
    output_path = Path(output_path)

    cfg = {**_DEFAULTS, **(config or {})}
    if toc_width is not None:
        cfg['toc_width'] = toc_width
    toc_width       = cfg['toc_width']
    toc_max_level   = cfg['toc_max_level']
    buttons_visible = cfg['buttons_visible']

    nb = nbformat.read(notebook_path, as_version=4)
    cell_config, md_config = _get_cell_config(nb)

    exporter = HTMLExporter(template_name='classic')
    exporter.register_preprocessor(_PlotlyHTMLPreprocessor, enabled=True)
    body, _ = exporter.from_notebook_node(nb)

    # Embed plotly.js inline — fully self-contained, no network needed
    body = body.replace('</head>', f'<script>{get_plotlyjs()}</script>\n</head>')

    toc = _build_toc(body, max_level=toc_max_level)
    body = body.replace('<body>', f'<body>\n{toc}')

    styles = f"""<style>
body {{ margin: 0; padding: 0; }}
.container, #notebook-container {{
    width: calc(100vw - {toc_width}px) !important;
    max-width: calc(100vw - {toc_width}px) !important;
    padding: 0 24px !important;
    box-sizing: border-box !important;
    box-shadow: none !important;
    margin-right: 0 !important;
    margin-left: 0 !important;
}}
div#notebook {{ padding: 0 !important; }}
.prompt.input_prompt, .output_prompt {{ display: none !important; }}
.input_area {{ display: none; }}
.cell-toggle {{
    cursor: pointer;
    background: #f5f5f5;
    border: 1px solid #ddd;
    border-radius: 3px;
    padding: 2px 10px;
    font-size: 11px;
    color: #666;
    margin: 2px 0 4px 0;
    display: inline-block;
    user-select: none;
    align-self: flex-start;
    width: fit-content;
}}
.cell-toggle:hover {{ background: #e0e0e0; }}
#toc {{
    position: fixed; top: 0; right: 0;
    width: {toc_width - 10}px; height: 100vh;
    overflow-y: auto; background: #fafafa;
    border-left: 1px solid #e0e0e0;
    padding: 14px 12px; box-sizing: border-box;
    font-size: 12px; z-index: 999;
}}
#toc-title {{ font-weight: bold; font-size: 13px; margin-bottom: 8px; padding-bottom: 6px; border-bottom: 1px solid #ddd; }}
#toc-controls {{ margin-bottom: 8px; padding-bottom: 6px; border-bottom: 1px solid #eee; }}
#toggle-buttons-btn {{ cursor: pointer; font-size: 11px; color: #888; }}
#toggle-buttons-btn:hover {{ color: #0066cc; }}
#toc ul {{ list-style: none; padding: 0; margin: 0; }}
#toc li {{ margin: 3px 0; line-height: 1.4; }}
#toc a {{ text-decoration: none; color: #444; }}
#toc a:hover {{ color: #0066cc; }}
</style>"""

    js_buttons_visible = 'true' if buttons_visible else 'false'
    h_selector = ', '.join(f'h{i}' for i in range(1, toc_max_level + 1))

    collapse_script = (
        '<script>\n'
        '(function () {\n'
        '    var cellConfig = ' + json.dumps(cell_config) + ';\n'
        '    var mdConfig   = ' + json.dumps(md_config) + ';\n'
        '\n'
        '    function makeToggle(labelShow, labelHide, target, startHidden, onToggle) {\n'
        '        target.style.display = startHidden ? "none" : "block";\n'
        '        var hidden = startHidden;\n'
        '        var btn = document.createElement("span");\n'
        '        btn.className = "cell-toggle";\n'
        '        btn.textContent = hidden ? ("▶ " + labelShow) : ("▼ " + labelHide);\n'
        '        btn.onclick = function () {\n'
        '            hidden = !hidden;\n'
        '            target.style.display = hidden ? "none" : "block";\n'
        '            btn.textContent = hidden ? ("▶ " + labelShow) : ("▼ " + labelHide);\n'
        '            if (onToggle) onToggle(hidden);\n'
        '        };\n'
        '        return btn;\n'
        '    }\n'
        '\n'
        '    document.querySelectorAll(".cell.code_cell").forEach(function (cell, idx) {\n'
        '        var cfg = cellConfig[String(idx)] || {};\n'
        '\n'
        '        var inputDiv  = cell.querySelector(".input");\n'
        '        var innerCell = inputDiv && inputDiv.querySelector(":scope > .inner_cell");\n'
        '        var area      = inputDiv && inputDiv.querySelector(".input_area");\n'
        '        if (inputDiv && innerCell && area) {\n'
        '            inputDiv.insertBefore(\n'
        '                makeToggle("Show code", "Hide code", area, !cfg.showInput),\n'
        '                innerCell\n'
        '            );\n'
        '        }\n'
        '\n'
        '        var wrapper = cell.querySelector(":scope > .output_wrapper");\n'
        '        if (wrapper) {\n'
        '            cell.insertBefore(\n'
        '                makeToggle("Show output", "Hide output", wrapper, !!cfg.hideOutput),\n'
        '                wrapper\n'
        '            );\n'
        '        }\n'
        '    });\n'
        '\n'
        '    var _cellButtonsVisible = ' + js_buttons_visible + ';\n'
        '    window.toggleCellButtons = function () {\n'
        '        _cellButtonsVisible = !_cellButtonsVisible;\n'
        '        document.querySelectorAll(".cell-toggle").forEach(function (btn) {\n'
        '            btn.style.display = _cellButtonsVisible ? "inline-block" : "none";\n'
        '        });\n'
        '        document.getElementById("toggle-buttons-btn").textContent =\n'
        '            _cellButtonsVisible ? "Hide buttons" : "Show buttons";\n'
        '    };\n'
        '\n'
        '    document.querySelectorAll(".cell.text_cell").forEach(function (cell, idx) {\n'
        '        var cfg = mdConfig[String(idx)] || {};\n'
        '        if (!cfg.hideCell && !cfg.hideToc) return;\n'
        '        var innerCell = cell.querySelector(".inner_cell");\n'
        '        if (!innerCell) return;\n'
        '        var tocItems = [];\n'
        '        innerCell.querySelectorAll("' + h_selector + '").forEach(function (h) {\n'
        '            if (h.id) {\n'
        '                var a = document.querySelector(`#toc a[href="#${h.id}"]`);\n'
        '                if (a) { var li = a.closest("li"); if (li) { li.style.display = "none"; tocItems.push(li); } }\n'
        '            }\n'
        '        });\n'
        '        if (cfg.hideCell) {\n'
        '            cell.insertBefore(\n'
        '                makeToggle("Show note", "Hide note", innerCell, true, function (isHidden) {\n'
        '                    tocItems.forEach(function (li) { li.style.display = isHidden ? "none" : ""; });\n'
        '                }),\n'
        '                innerCell\n'
        '            );\n'
        '        }\n'
        '    });\n'
        '\n'
        '    document.querySelectorAll(".cell-toggle").forEach(function (btn) {\n'
        '        btn.style.display = _cellButtonsVisible ? "inline-block" : "none";\n'
        '    });\n'
        '}());\n'
        '</script>'
    )

    body = body.replace('</head>', styles + '\n</head>')
    body = body.replace('</body>', collapse_script + '\n</body>')

    output_path.write_text(body)
    print(f"Saved to {output_path}")
    return output_path
