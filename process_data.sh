#!/bin/bash

# Animal Insights Data Processing Script
echo "Starting data processing pipeline..."

# Process GBIF XML data
echo "Step 1/3: Parsing GBIF XML data..."
python scripts/parse_gbif_xml.py
if [ $? -ne 0 ]; then
    echo "Error: GBIF XML parsing failed. Exiting."
    exit 1
fi

# Process IUCN JSON data
echo "Step 2/3: Parsing IUCN JSON data..."
python scripts/parse_iucn_json.py
if [ $? -ne 0 ]; then
    echo "Error: IUCN JSON parsing failed. Exiting."
    exit 1
fi

# Load data into DuckDB
echo "Step 3/3: Loading processed data into DuckDB..."
python scripts/load_to_duckdb.py
if [ $? -ne 0 ]; then
    echo "Error: Data loading failed. Exiting."
    exit 1
fi

echo "Data processing completed successfully!"