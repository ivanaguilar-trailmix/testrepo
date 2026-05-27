#!/bin/bash
set -e

if [ ! -d ".venv" ]; then
    echo "Environment not set up yet. Run ./setup.sh first."
    exit 1
fi

source .venv/bin/activate
python3 -c "from common_lib.version import VERSION; print(f'Game Simulator v{VERSION}')"
voila game_simulator.ipynb --ServerApp.disable_check_xsrf=True --VoilaExecutor.iopub_timeout=120 --VoilaExecutor.startup_timeout=120
