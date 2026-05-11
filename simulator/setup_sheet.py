"""
Run once to create the local CSV input files for the simulator.
Files are saved to config/inputs/ and pre-populated with the anchor day structure.

Usage:
    python setup_sheet.py
"""
import csv
from pathlib import Path

INPUTS_DIR = Path(__file__).parent / "config" / "inputs"

def _write_csv(path: Path, rows: list[list]):
    with open(path, "w", newline="") as f:
        csv.writer(f).writerows(rows)


def setup_inputs():
    INPUTS_DIR.mkdir(parents=True, exist_ok=True)

    _write_csv(INPUTS_DIR / "retention.csv",
               [["month", "dx", "ios", "android"]])

    _write_csv(INPUTS_DIR / "conversion.csv",
               [["month", "dx", "ios", "android"]])

    _write_csv(INPUTS_DIR / "ua_spend.csv",
               [["month", "platform", "budget"]])

    _write_csv(INPUTS_DIR / "cpi.csv",
               [["month", "platform", "cpi"]])

    _write_csv(INPUTS_DIR / "arpdau.csv",
               [["month", "platform", "iap_arpdau", "ad_arpdau"]])

    print(f"Input files created in: {INPUTS_DIR}")
    for f in sorted(INPUTS_DIR.iterdir()):
        print(f"  {f.name}")


if __name__ == "__main__":
    setup_inputs()
