"""
Notebook HTML Export — Configuration & Guide
============================================

USAGE
-----
Basic export (uses all defaults below):

    from common_lib.export import export_notebook_html
    export_notebook_html('notebook.ipynb')

Custom config — pass any subset of keys to override defaults:

    from common_lib.export import export_notebook_html
    from common_lib.export_config import DEFAULT_CONFIG

    export_notebook_html('notebook.ipynb', config={**DEFAULT_CONFIG, 'buttons_visible': True})

Or pass only the keys you want to change:

    export_notebook_html('notebook.ipynb', config={'toc_width': 300})


HOW IT WORKS
------------
The exporter performs these steps in order:

1. Reads the notebook with nbformat.
2. Converts Plotly chart outputs (application/vnd.plotly.v1+json) to inline HTML so
   charts render without a network connection. Width and height are preserved from the
   figure layout.
3. Converts the notebook to HTML using nbconvert's classic template.
4. Embeds plotly.js inline — the output HTML file is fully self-contained.
5. Builds a fixed sidebar table of contents from heading tags (h1…h<toc_max_level>).
6. Injects CSS and JavaScript for:
     - Collapsible code inputs (hidden by default, per-cell override available)
     - Collapsible code outputs (visible by default, per-cell override available)
     - Collapsible markdown cells (opt-in via cell marker)
     - TOC heading exclusion (opt-in via cell marker)
     - Global "Show/Hide buttons" toggle in the TOC sidebar
7. Writes the result to an HTML file.


CELL VISIBILITY MARKERS
-----------------------
Add one of these as the very first line of a cell to control how it appears in the export.

  Code cells (Python comment on line 1):
  ┌──────────────────┬────────────────────────────────────────────────────────────────┐
  │ Marker           │ Effect                                                         │
  ├──────────────────┼────────────────────────────────────────────────────────────────┤
  │ # show           │ Show code input by default (inputs are hidden by default)      │
  │ # hide-output    │ Hide output by default (outputs are visible by default)        │
  │ # show hide-output│ Both: show input, hide output                                │
  └──────────────────┴────────────────────────────────────────────────────────────────┘

  Markdown cells (HTML comment on line 1):
  ┌────────────────────┬──────────────────────────────────────────────────────────────┐
  │ Marker             │ Effect                                                       │
  ├────────────────────┼──────────────────────────────────────────────────────────────┤
  │ <!-- hide -->      │ Cell content hidden by default with a "▶ Show note" toggle.  │
  │                    │ Headings in the cell are also excluded from the TOC sidebar. │
  ├────────────────────┼──────────────────────────────────────────────────────────────┤
  │ <!-- hide-toc -->  │ Cell remains fully visible, but its headings are excluded    │
  │                    │ from the TOC sidebar (useful for intro/meta sections).       │
  └────────────────────┴──────────────────────────────────────────────────────────────┘

  Cells with no marker follow the notebook-wide defaults.


GLOBAL TOGGLE (TOC SIDEBAR)
----------------------------
A "Show buttons / Hide buttons" link appears at the top of the TOC sidebar.
Clicking it shows or hides all cell toggle buttons across the entire export.
The initial state is controlled by the `buttons_visible` config option.


CONFIGURATION OPTIONS
---------------------
Edit DEFAULT_CONFIG below and pass it as `config=` to export_notebook_html.
"""

DEFAULT_CONFIG = {
    # Width of the fixed TOC sidebar in pixels.
    'toc_width': 250,

    # Maximum heading depth shown in the TOC sidebar.
    # 1 = h1 only, 2 = h1 + h2, 3 = h1 + h2 + h3, etc.
    # Cells tagged <!-- hide --> or <!-- hide-toc --> will have their headings
    # excluded from the TOC regardless of this setting.
    'toc_max_level': 2,

    # Whether cell toggle buttons (Show/Hide code, output, note) are visible
    # when the page first loads.
    # False → buttons are hidden; user clicks "Show buttons" in the TOC to reveal them.
    # True  → buttons are visible immediately.
    'buttons_visible': False,
}
