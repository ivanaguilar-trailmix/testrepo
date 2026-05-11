import pandas as pd
from pathlib import Path

INPUTS_DIR = Path(__file__).parent.parent / "config" / "inputs"


def _load_csv(name: str) -> pd.DataFrame:
    path = INPUTS_DIR / f"{name}.csv"
    if not path.exists():
        raise FileNotFoundError(
            f"Input file not found: {path}\nRun setup_sheet.py first."
        )
    return pd.read_csv(path)


def load_inputs() -> dict[str, pd.DataFrame]:
    """
    Read CPI and UA spend inputs from local CSV files in config/inputs/.

    Returns dict with keys: cpi, ua_spend.
    Retention, conversion, and ARPDAU are now loaded from actuals via the panel.
    """
    return {
        "cpi":      _load_csv("cpi"),
        "ua_spend": _load_csv("ua_spend"),
    }


def get_inputs_dir() -> str:
    return str(INPUTS_DIR)
