@echo off

if not exist ".venv" (
    echo Environment not set up yet. Run setup.bat first.
    exit /b 1
)

call .venv\Scripts\activate
voila game_simulator.ipynb --show_tracebacks=True
