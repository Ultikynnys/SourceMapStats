@echo off
setlocal

REM ----------------------------------------
REM Migrate legacy CSV data into DuckDB
REM Uses migrate_csv_to_duckdb.py under the hood
REM ----------------------------------------

REM Switch to this script's directory
pushd "%~dp0"

REM Create and activate venv
if not exist venv (
    echo Creating virtual environment...
    python -m venv venv
) else (
    echo Virtual environment already exists.
)

echo Activating virtual environment...
call venv\Scripts\activate.bat

REM Upgrade pip and ensure dependencies
echo Upgrading pip and installing requirements...
python -m pip install --upgrade pip
if exist requirements.txt (
    pip install -r requirements.txt
) else (
    echo requirements.txt not found; installing 'duckdb' only...
    pip install duckdb
)

REM Decide default CSV if no args provided
set "DEFAULT_CSV=output.csv"
if not exist "%DEFAULT_CSV%" if exist "example_output.csv" set "DEFAULT_CSV=example_output.csv"

if "%~1"=="" (
    echo No arguments provided. Using default CSV: "%DEFAULT_CSV%"
    if not exist "%DEFAULT_CSV%" (
        echo Error: Default CSV "%DEFAULT_CSV%" not found.
        popd
        exit /b 1
    )
    echo Running migration: python migrate_csv_to_duckdb.py --csv "%DEFAULT_CSV%"
    python migrate_csv_to_duckdb.py --csv "%DEFAULT_CSV%"
    set ERR=%ERRORLEVEL%
) else (
    echo Running migration: python migrate_csv_to_duckdb.py %*
    python migrate_csv_to_duckdb.py %*
    set ERR=%ERRORLEVEL%
)

if not exist migrate_csv_to_duckdb.py (
    echo Error: migrate_csv_to_duckdb.py not found in %CD%
    echo Make sure the script exists.
    popd
    exit /b 1
)

if %ERR% NEQ 0 (
    echo Migration ended with errors. Exit code: %ERR%
) else (
    echo Migration completed successfully.
)

popd
exit /b %ERR%
