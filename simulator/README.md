# Game Simulator — Setup Guide

Forecast DAU, revenue, and P&L over a 12-month horizon by adjusting UA spend, CPI, retention, and conversion.

---

## Requirements

- Python 3.12 or later

---

## Setup

**1. Create a virtual environment**
```bash
python3 -m venv .venv
source .venv/bin/activate        # Mac / Linux
# .venv\Scripts\activate         # Windows
```

**2. Install dependencies**
```bash
pip install -r requirements.txt
```

**3. Add the data files**

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
