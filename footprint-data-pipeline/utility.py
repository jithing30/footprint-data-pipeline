import requests
import pandas as pd
import duckdb
import json
from datetime import datetime
from .params import Parameter as param
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import subprocess, os


class Utility:

    @staticmethod
    # read ingestion api
    def fetch_data(url, year):
        headers = {"HTTP_ACCEPT": "application/json"}
        try:
            r = requests.get(url, auth=(param.USER_NAME, param.API_KEY), headers=headers)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            data = None
        return data

    @staticmethod
    def save_json(year, data):
        ts = datetime.utcnow().strftime("%Y-%m-%d")
        with open(f"{param.RAW_DIR}/{year}-footprint-{ts}.json", "w") as f:
            json.dump(data, f, indent=2)

    @staticmethod
    # get json/dict data and converts into dataframe
    def transform(json_data):
        df = pd.DataFrame.from_records(json_data)
        df["ingestion_date"] = datetime.utcnow().strftime("%Y-%m-%d")
        return df

    @staticmethod
    # aggregation/bussiness required columns
    def select_data(df):
        df = df.rename(columns={
            "countryName": "country",
            "carbon": "carbon_footprint"
        })
        df = df.loc[:, ["country", "year", "carbon_footprint", "ingestion_date"]]
        return df

    @staticmethod
    def save_parquet(df, path):
        df.to_parquet(path)
        return path

    @staticmethod
    def load_duckdb(files):
        con = duckdb.connect(param.DB_PATH)
        con.execute("""
            CREATE TABLE IF NOT EXISTS raw_footprint (
                country VARCHAR,
                year INTEGER,
                carbon_footprint DOUBLE,
                ingestion_date TIMESTAMP
            )
        """)
        for f in files:
            con.execute(f"""
                INSERT INTO raw_footprint
                SELECT * FROM read_parquet('{f}')
            """)
        con.close()

    @staticmethod
    def get_year_list(start_year, end_year=None):
        if end_year is None:
            # take current year as end year
            end_year = datetime.now().year

        years = range(start_year, end_year + 1)
        return years

    @staticmethod
    def run_dbt(dbt_dir):
        os.chdir(dbt_dir)
        subprocess.run(["dbt", "run"], check=True)
        # subprocess.run(["dbt", "test"], check=True)

    @staticmethod
    def create_spark():
        builder = (
            SparkSession.builder
                .appName("DeltaLakeApp")
                .master("local[*]")   # Remove in cluster
                .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension"
            )
                .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
                .config("spark.sql.shuffle.partitions", "200")
        )

        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        return spark
