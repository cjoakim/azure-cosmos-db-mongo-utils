#!/bin/bash

# Excute the unit tests for the application.
# Chris Joakim, Microsoft, 2023

echo 'activating the python venv ...'
source venv/bin/activate
python --version

pytest
