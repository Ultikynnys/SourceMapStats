#!/usr/bin/env bash
set -euo pipefail

# ----------------------------------------
# Create and activate a Python virtualenv
# ----------------------------------------
if [ ! -d venv ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
else
    echo "Virtual environment already exists."
fi

echo
echo "Activating virtual environment..."
# shellcheck disable=SC1091
source venv/bin/activate

# ----------------------------------------
# Upgrade pip and install dependencies
# ----------------------------------------
echo
echo "Upgrading pip and installing requirements..."
pip install --upgrade pip
if [ -f requirements.txt ]; then
    pip install -r requirements.txt
else
    echo "requirements.txt not found! Please add one before proceeding."
    exit 1
fi

# ----------------------------------------
# Determine local IPv4 address
# ----------------------------------------
LOCAL_IP=$(hostname -I | awk '{print $1}')
if [ -z "$LOCAL_IP" ]; then
    LOCAL_IP="127.0.0.1"
fi

# ----------------------------------------
# Run the Flask app via Waitress
# ----------------------------------------
echo
echo "Starting the Flask app on http://$LOCAL_IP:5000"
exec python app.py
