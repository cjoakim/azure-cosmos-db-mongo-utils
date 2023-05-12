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

mkdir -p artifacts/bicep
mkdir -p artifacts/spark
mkdir -p tmp
mkdir -p ~/AppData/Local/Temp/MongoMigrationAssessment

echo 'done'
