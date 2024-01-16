#!/bin/bash

# Change environment
source atom/bin/activate

# Change directory and git pull latest code
cd atom-sqs-worker
git pull

# Update .env
python update_env.py

# Pip install requirements
pip install -r requirements.txt

# Run worker
python main.py
