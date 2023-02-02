echo off
cd %~dp0
pip install -r requirements.txt
pip uninstall python-valve -y