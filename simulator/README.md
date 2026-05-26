# Game Simulator — Setup Guide

Forecast DAU, revenue, and P&L over a 12-month horizon by adjusting UA spend, CPI, retention, and conversion.

---

## Requirements

- Python 3.12 or later

---

## Setup

**1. Make the scripts executable** *(Mac / Linux only — skip if cloned from git)*
```bash
chmod +x setup.sh run.sh
```

**2. Create a virtual environment**
```bash
python3 -m venv .venv
source .venv/bin/activate        # Mac / Linux
# .venv\Scripts\activate         # Windows
```

**3. Install dependencies**
```bash
pip install -r requirements.txt
```

**4. Add the data files**

Place the following files inside the `data/` folder (shared separately):
```
data/actuals_dau.pkl
data/live_retention.pkl
data/live_conversion.pkl
```

---

## Run

```bash
voila game_simulator.ipynb
```

This opens the simulator in your browser. No coding required.

---

## Update inputs

Edit the CSV files in `config/inputs/` to change monthly UA spend and CPI assumptions:
- `config/inputs/ua_spend.csv`
- `config/inputs/cpi.csv`

Then re-run voila.

---

## Refresh data from BigQuery

Set `refresh_data = True` in the first cell of the notebook. Requires Google Cloud credentials and the BigQuery packages (see commented lines in `requirements.txt`).

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                           simulator.ipynb                                         │
│   1. load actuals (pkl)  ·  2. prefill_panel()  ·  3. setup_callbacks()          │
│   4. panel.display()                                                              │
└────────┬──────────────────────────────────────┬───────────────────────────────────┘
         │                                       │
         ▼                                       ▼
┌─────────────────────────┐     ┌────────────────────────────────────────────────────┐
│        app.py           │     │                DATA INGESTION                       │
│                         │     │                                                      │
│  prefill_panel()        │     │  sql.py ──► sql/actuals.sql    ──► BigQuery         │
│  setup_callbacks()      │     │        ──► sql/retention.sql   ──► BigQuery         │
│   · run_simulation      │     │        ──► sql/conversion.sql  ──► BigQuery         │
│   · save_scenario       │     │        cached ──► data/actuals_dau.pkl              │
│   · load_scenario       │     │                   data/live_retention.pkl           │
│   · set_anchor          │     │                   data/live_conversion.pkl          │
│   · load curves/arpdau  │     │                                                      │
│   · load csv costs      │     │  sheets.py ──► config/inputs/cpi.csv               │
│                         │     │            ──► config/inputs/ua_spend.csv           │
│  setup_sheet.py         │     │            ──► config/inputs/team_cost.csv          │
│  (one-off BQ → CSV)     │     │            ──► config/inputs/marketing_cost.csv     │
└────────┬────────────────┘     └────────────────────────────────────────────────────┘
         │
         ▼
┌────────────────────────────────────────────────────────┐
│                      widgets.py                         │
│                                                         │
│  ScenarioPanel                                          │
│  ├── PlatformPanel ×2    CPI + UA spend per month       │
│  ├── ARPDAUPanel         IAP + Ad ARPDAU per month      │
│  ├── CurvePanel ×2       retention / conversion anchors │
│  ├── TeamCostPanel       headcount cost (actuals+fcast) │
│  ├── TeamCostPanel       marketing cost (hist. actuals) │
│  └── ActualsRangePanel ×3  date-range selectors         │
│                                                         │
│  charts_output  Output widget                           │
│  table_output   Output widget                           │
└────────────────────────┬───────────────────────────────┘
                         │ on Run
                         ▼
┌────────────────────────────────────────────────────────────────────────────────┐
│                             COMPUTATION                                         │
│                                                                                 │
│  curves.py                          simulation.py                               │
│  build_curve()                      SimulationEngine.run()                      │
│  PCHIP interpolation      ──────►   PlatformInputs → PlatformResults           │
│  average_actuals_anchors()          _cohort_dau_and_payers()  (convolution)    │
│  average_arpdau_from_actuals()      _organic_base_decay()                      │
│                                     SimulationResults  (ios + android + comb.) │
└─────────────────────────────────────────────┬──────────────────────────────────┘
                                              │
                                              ▼
┌────────────────────────────────────────────────────────────────────────────────┐
│                              PERSISTENCE                                        │
│                                                                                 │
│  scenarios/*.json  ◄──► save_scenario() / load_scenario()   (simulation.py)   │
│  results/*.json    ◄──► save_result()   / load_result()     (simulation.py)   │
└─────────────────────────────────────────────┬──────────────────────────────────┘
                                              │
                                              ▼
┌────────────────────────────────────────────────────────────────────────────────┐
│                                OUTPUT                                           │
│                                                                                 │
│  plots.py                            tables.py                                 │
│  plot() → Plotly figures             monthly_table() → pandas Styler P&L       │
│   · DAU forecast                      · actuals rows + forecast rows           │
│   · New installs per day              · IAP net ×0.70 · Ad net ×0.85          │
│   · Daily revenue (IAP + Ad)          · partial-month blending at boundary     │
│   · Payer DAU                         · Marketing cost + Team cost             │
│   · Monthly revenue bar               · Profit = Revenue(net) − costs          │
│  plot_retention() / plot_conversion()                                           │
│                                       export.py → HTML / CSV export            │
└────────────────────────────────────────────────────────────────────────────────┘

                         ┌─────────────────────────┐
                         │  Voila (optional)         │
                         │  serves simulator.ipynb   │
                         │  as a browser app         │
                         └─────────────────────────-─┘
```

### Key data flows

- **Actuals** — BigQuery → `.pkl` cache → `app.py` → panel prefill + charts overlay
- **Curve inputs** — BigQuery → `.pkl` cache → actuals panels → `curves.py` PCHIP → `simulation.py`
- **Cost inputs** — CSV → `sheets.py` → panel; or typed directly in panel widgets
- **Scenario round-trip** — panel state → `save_scenario()` → `scenarios/*.json` → `load_scenario()` → panel restore
- **Simulation round-trip** — panel values → `SimulationEngine` → `save_result()` → `load_result()` → `plots.py` + `tables.py`
