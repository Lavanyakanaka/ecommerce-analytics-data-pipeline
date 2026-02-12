#!/bin/bash

echo "Setting up Ecommerce Data Pipeline Environment..."

python -m venv venv
source venv/bin/activate

pip install --upgrade pip
pip install -r requirements.txt

echo "Environment setup complete."
