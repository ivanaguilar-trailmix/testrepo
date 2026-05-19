#!/bin/bash
set -e

if [ ! -d ".venv" ]; then
    echo "Environment not set up yet. Run ./setup.sh first."
    exit 1
fi

source .venv/bin/activate
voila game_simulator.ipynb
