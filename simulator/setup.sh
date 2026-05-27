#!/bin/bash
set -e

echo "Setting up Game Simulator environment..."

if ! command -v python3 &>/dev/null; then
    echo "ERROR: python3 not found. Install Python 3.11+ from https://www.python.org/downloads/"
    exit 1
fi

python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip --quiet
pip install -r requirements.txt --quiet --no-cache-dir
jupyter trust game_simulator.ipynb
python3 -m compileall common_lib/ -q

echo ""
echo "Setup complete. Run ./run.sh to start the simulator."
