@echo off
REM Start the Flask backend in a new command window
start "update waitress" python -m pip install waitress
start "Flask Backend" python app.py

REM Wait for 5 seconds to allow the server to start
timeout /t 5 /nobreak >nul

REM Open the default browser to the frontend URL
start "Map Stats Visualization" http://127.0.0.1:5000

REM Optional: Pause the batch window so you can see any messages
pause
