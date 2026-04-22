#!/bin/sh

echo "Running ETL pipeline..."
python src/main.py

echo "Starting Flask app..."
python -m web.app