@echo off

if not exist ".venv" (
    echo Environment not set up yet. Run setup.bat first.
    exit /b 1
)

call .venv\Scripts\activate
python -c "from common_lib.version import VERSION; print('Game Simulator v' + VERSION)"
voila game_simulator.ipynb --debug --show_tracebacks=True --ServerApp.disable_check_xsrf=True --VoilaExecutor.iopub_timeout=120 --VoilaExecutor.startup_timeout=120
