@echo off

echo Setting up Game Simulator environment...

where python >nul 2>&1
if errorlevel 1 (
    echo ERROR: python not found. Install Python 3.11+ from https://www.python.org/downloads/
    exit /b 1
)

python -m venv .venv
call .venv\Scripts\activate
pip install --upgrade pip --quiet
pip install -r requirements.txt --quiet

echo.
echo Setup complete. Run run.bat to start the simulator.
