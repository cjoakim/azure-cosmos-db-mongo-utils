#!/bin/bash

# Recreate the python virtual environment and reinstall libs on macOS or Linux.
# Also create necessary output directories.
# Chris Joakim, Microsoft, 2023

rm -rf venv/

echo 'creating new venv ...'
python3 -m venv ./venv/

echo 'activating new python venv ...'
source venv/bin/activate
python --version

echo 'upgrading pip ...'
python -m pip install --upgrade pip 

echo 'install pip-tools ...'
pip install --upgrade pip-tools

echo 'displaying python location and version'
which python
python --version

echo 'displaying pip location and version'
which pip
pip --version

echo 'pip-compile requirements.in ...'
pip-compile --output-file requirements.txt requirements.in

echo 'pip install requirements.txt ...'
pip install -q -r requirements.txt

echo 'pip list ...'
pip list

mkdir -p tmp
mkdir -p ~/AppData/Local/Temp/MongoMigrationAssessment

echo 'activating the venv...'
source venv/bin/activate

python --version

# pip list output 2023/05/13 on macOS:

# pip install requirements.txt ...
# pip list ...
# Package         Version
# --------------- ---------
# arrow           0.17.0
# attrs           22.2.0
# build           0.10.0
# certifi         2020.12.5
# chardet         4.0.0
# click           8.1.3
# dnspython       2.3.0
# docopt          0.6.2
# et-xmlfile      1.1.0
# flake8          3.8.4
# humanize        3.2.0
# idna            2.10
# iniconfig       2.0.0
# Jinja2          3.1.2
# MarkupSafe      2.1.2
# mccabe          0.6.1
# openpyxl        3.1.2
# packaging       23.0
# pip             23.1.2
# pip-tools       6.13.0
# pluggy          1.0.0
# psutil          5.8.0
# pycodestyle     2.6.0
# pyflakes        2.2.0
# pymongo         3.11.3
# pyproject_hooks 1.0.0
# pytest          7.2.2
# python-dateutil 2.8.1
# requests        2.25.1
# setuptools      67.6.1
# six             1.15.0
# urllib3         1.26.3
# wheel           0.40.0
# activating the venv...
# Python 3.11.3
