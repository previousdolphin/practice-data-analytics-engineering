#!/bin/bash
# Set up a Python virtual environment and start Jupyter Notebook.
# Works on macOS with Python 3 installed.

set -e

# Create virtual environment if it doesn't exist
if [ ! -d ".venv" ]; then
  python3 -m venv .venv
fi

# Activate environment
source .venv/bin/activate

# Ensure pip is up to date and install jupyter
pip install --upgrade pip
pip install jupyter

# Launch notebook server from repository root
cd ..
jupyter notebook
