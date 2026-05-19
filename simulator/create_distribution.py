"""
Build a self-contained distribution zip of the Game Simulator.

Run from inside the simulator/ directory:
    python3 create_distribution.py

Output: simulator_YYYYMMDD.zip (written to the parent directory)
"""

import zipfile
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).parent

INCLUDE = [
    # Notebook
    "game_simulator.ipynb",
    # Scripts
    "setup.sh",
    "run.sh",
    "requirements.txt",
    # Source
    "common_lib",
    "sql",
    # Inputs
    "config",
    "data",
    # Saved state (scenarios + results let recipients load existing plans immediately)
    "scenarios",
    "results",
]

EXCLUDE_SUFFIXES = {".pyc", ".zip"}
EXCLUDE_DIRS    = {"__pycache__", ".venv", "archive"}
EXCLUDE_FILES   = {"create_distribution.py"}


def should_skip(path: Path) -> bool:
    if path.name.startswith("."):
        return True
    if path.suffix in EXCLUDE_SUFFIXES:
        return True
    if any(part in EXCLUDE_DIRS for part in path.parts):
        return True
    if path.name in EXCLUDE_FILES:
        return True
    return False


def collect(root: Path):
    for entry in INCLUDE:
        target = root / entry
        if not target.exists():
            print(f"  WARNING: {entry} not found, skipping")
            continue
        if target.is_file():
            if not should_skip(target):
                yield target
        else:
            for f in sorted(target.rglob("*")):
                if f.is_file() and not should_skip(f):
                    yield f


def main():
    timestamp = datetime.now().strftime("%Y%m%d")
    zip_path  = ROOT.parent / f"simulator_{timestamp}.zip"

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in collect(ROOT):
            arcname = Path("simulator") / path.relative_to(ROOT)
            zf.write(path, arcname)
            print(f"  + {arcname}")

    size_mb = zip_path.stat().st_size / 1_048_576
    print(f"\nCreated: {zip_path}  ({size_mb:.1f} MB)")
    print("\nRecipient instructions:")
    print("  unzip simulator_*.zip")
    print("  cd simulator")
    print("  bash setup.sh   # one-time")
    print("  bash run.sh     # each time")


if __name__ == "__main__":
    main()
