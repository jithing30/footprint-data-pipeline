SELECT
    country,
    year,
    carbon_footprint,
    ingestion_date
FROM raw_footprint
WHERE carbon_footprint IS NOT NULL