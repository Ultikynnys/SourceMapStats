echo off
cd %~dp0
pip install -r requirements.txt
git submodule init
git submodule update