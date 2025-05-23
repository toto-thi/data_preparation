## Preparing Animal Data

This repository contains scripts for parsing and loading animal conservation and biodiversity data into a DuckDB database for analytical processing.

## Project Structure
- `data/`: Directory for raw and processed data files

    - Source XML files from GBIF, IUCN JSON data and CITES (csv format)

    - Processed CSV files

- `scripts/`: Python scripts for data processing

    - `parse_gbif_xml.py`: Extracts metadata from GBIF biodiversity XML files

    - `parse_iucn_json.py`: Processes IUCN conservation status data from JSON

    - `load_to_duckdb.py`: Loads processed CSV files into DuckDB database

- `animal_insights.duckdb`: DuckDB database file containing processed data (generated from `load_to_duckdb.py`)

## Getting started

1. Clone this repository
2. Install required dependencies
    ```bash
    pip install pandas duckdb
    ```
3. Run `process_data.sh` or you can do `python scripts/*`
    ```bash 
    chmod +x process_data.sh
    ./process_data.sh
    ```

## Database Schema

The DuckDB database contains the following tables:

- `iucn_species`: Conservation status information for various species
    - scientific_name, common_name, taxonomic information
    - assessment_year, conservation metrics and categories

- `cites_trade`: International wildlife trade records 
    - Year, species information, importing/exporting countries
    - Trade quantities, purposes, and sources

- `gbif_sightings`: Biodiversity publication metadata
    - Publication titles, authors, and dates
    - Publisher information and DOIs

## Usage with DBT

This data preparation repository is designed to work with the companion `dbt_animal_insights` project, which provides data transformation models for analytical insights.

After running the data preparation scripts, you can use the DBT project to transform the raw data into analytical models.