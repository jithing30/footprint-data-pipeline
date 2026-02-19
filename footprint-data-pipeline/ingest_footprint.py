from .utility import Utility as util
from .params import Parameter as param
from .logger import AppLogger


logger = AppLogger(name="ingestion")._logger
START_YEAR = 2010


def build_url(year: int) -> str:
    return param.BASE_URL_TEMPLATE.format(year=year)


def ingest_data(**kwargs):

    # Get year from kwargs, fallback to start year
    year = kwargs.get("year", START_YEAR)

    logger.info("Starting ingestion job")
    logger.info(f"Processing year: {year}")

    # build url for the requested year
    url = build_url(year)

    # get raw data from api
    api_data = util.fetch_data(url, year)
    if api_data is None:
        logger.error(f"API returned null/empty records for year : {year}")
        raise Exception(
            "No data received from API. Stopping pipeline."
        )

    # stage raw json with timestamp in raw layer
    util.save_json(year, api_data)
    logger.info(f"fetch data complete for year: {year}")

    # transform json to table format
    df = util.transform(api_data)
    bronze_path = f"{param.BRONZE_LAYER}/{year}_footprint.parquet"
    util.save_parquet(df=df, path=bronze_path)

    # clean data
    clean_df = util.select_data(df)
    silver_path = f"{param.SILVER_LAYER}/{year}_footprint.parquet"
    file = util.save_parquet(df=clean_df, path=silver_path)
    # TODO : perform incremental aggregation using delta tables

    logger.info(f"updating database for year : {year}")
    util.load_duckdb([file])

    logger.info(f"refresh DBT model for year : {year}")
    util.run_dbt(param.DBT_DIR)

    logger.info(f"Ingestion for year {year} completed")


if __name__ == "__main__":
    ingest_data(year=2023)
