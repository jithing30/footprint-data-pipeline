#!/bin/bash
set -e

python /Users/radhin/Downloads/footprint-data-pipeline/ingestion/ingest_footprint.py

cd dbt/footprint_dbt
dbt run
dbt test
