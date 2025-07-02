#!/bin/bash
# Run start_driver in Python venv and with passed arguments

source .venv/bin/activate
python start_driver.py $1 $2 $3
