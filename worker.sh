#!/bin/bash
# Run as many start_worker processes in Python venv as set in $1

# Check if an argument is provided
if [ -z "$1" ]; then
  echo "Usage: $0 <number_of_times>"
  exit 1
fi

source .venv/bin/activate

for (( i=0; i<$1; i++ ))
do
  # Start process in the background
  python start_worker.py &
done

# Wait for all processes to finish
wait
