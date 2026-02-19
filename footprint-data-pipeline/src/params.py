class Parameter:

    # ingestion url
    url = 'https://api.footprintnetwork.org/v1/data/all/'
    BASE_URL_TEMPLATE = "https://api.footprintnetwork.org/v1/data/all/{year}/earth"
    USER_NAME = 'username'
    API_KEY = "aQ66IaVon5tbb41km4ueFub61ef9vU9fcVsaNnsBtC803dNaAkB"

    # Storage paths
    RAW_DIR = "data/raw"
    BRONZE_LAYER = "data/bronze"
    SILVER_LAYER = "data/silver"
    DB_PATH = "data/gold/footprint.duckdb"

    # DBT
    DBT_DIR = "/Users/radhin/Downloads/footprint-data-pipeline/footprint_dbt"
