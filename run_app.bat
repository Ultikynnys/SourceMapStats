@echo off
REM ----------------------------------------
REM Create and activate a Python virtualenv
REM ----------------------------------------
if not exist venv (
    echo Creating virtual environment...
    python -m venv venv
) else (
    echo Virtual environment already exists.
)

echo.
echo Activating virtual environment...
call venv\Scripts\activate.bat

REM ----------------------------------------
REM Upgrade pip and install dependencies
REM ----------------------------------------
echo.
echo Upgrading pip and installing requirements...
python -m pip install --upgrade pip
if exist requirements.txt (
    pip install -r requirements.txt
) else (
    echo requirements.txt not found! Please add one before proceeding.
    goto end
)

REM ----------------------------------------
REM Determine local IPv4 address
REM ----------------------------------------
set "LOCAL_IP=127.0.0.1"
for /f "tokens=2 delims=:" %%A in ('ipconfig ^| findstr /c:"IPv4 Address"') do (
    set "LOCAL_IP=%%A"
    goto got_ip
)
:got_ip
REM Trim any leading spaces
set "LOCAL_IP=%LOCAL_IP: =%"

REM ----------------------------------------
REM Run the Flask app via Waitress
REM ----------------------------------------
echo.
echo Starting the Flask app on http://%LOCAL_IP%:5000
python app.py

:end
pause
