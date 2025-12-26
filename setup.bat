@echo off

echo === E-Commerce Data Pipeline Setup ===

REM Check Python version
python --version

REM Create virtual environment
echo Creating virtual environment...
python -m venv venv

REM Activate virtual environment
echo Activating virtual environment...
call venv\Scripts\activate.bat

REM Install dependencies
echo Installing dependencies...
python -m pip install --upgrade pip
pip install -r requirements.txt

REM Create directories
echo Creating required directories...
if not exist data\raw mkdir data\raw
if not exist data\staging mkdir data\staging
if not exist data\processed mkdir data\processed
if not exist logs mkdir logs
if not exist dashboards\screenshots mkdir dashboards\screenshots
if not exist config mkdir config

REM Copy environment file
echo Setting up environment...
if not exist .env (
    copy .env.example .env
    echo Created .env file. Please update with your database credentials.
) else (
    echo .env file already exists.
)

echo Setup complete!
echo.
echo Next steps:
echo 1. Update .env with your database credentials
echo 2. Run: python scripts/data_generation/generate_data.py
echo 3. Run: python scripts/pipeline_orchestrator.py
echo.

pause
