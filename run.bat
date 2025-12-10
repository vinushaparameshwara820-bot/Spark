@echo off
echo Starting Weather Analysis...
echo Using simple Python version (no Spark required)

REM Check if python is installed
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo Python is not installed. Please install Python to run this project.
    pause
    exit /b 1
)

REM Run the simple weather analysis script
python simple_weather_analysis.py

echo Weather Analysis completed.
pause