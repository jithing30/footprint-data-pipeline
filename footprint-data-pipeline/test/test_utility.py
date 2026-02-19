import pandas as pd
from unittest.mock import patch, MagicMock, mock_open
from datetime import datetime
from utility import Utility


@patch("utility.requests.get")
def test_fetch_data_success(mock_get):

    mock_response = MagicMock()
    mock_response.json.return_value = {"data": "test"}
    mock_response.raise_for_status.return_value = None

    mock_get.return_value = mock_response

    result = Utility.fetch_data("http://test.com", 2024)

    assert result == {"data": "test"}
    mock_get.assert_called_once()


@patch("utility.requests.get")
def test_fetch_data_failure(mock_get):

    mock_get.side_effect = Exception("API error")

    result = Utility.fetch_data("http://test.com", 2024)

    assert result is None


@patch("utility.datetime")
def test_transform(mock_datetime):

    mock_datetime.utcnow.return_value = datetime(2025, 1, 1)

    json_data = [
        {"country": "NL", "year": 2024, "carbon": 10.5},
        {"country": "DE", "year": 2023, "carbon": 20.2}
    ]

    df = Utility.transform(json_data)

    assert isinstance(df, pd.DataFrame)
    assert "ingestion_date" in df.columns
    assert len(df) == 2


def test_select_data():

    data = {
        "countryName": ["NL"],
        "year": [2024],
        "carbon": [10.5],
        "ingestion_date": ["2025-01-01"]
    }

    df = pd.DataFrame(data)

    result = Utility.select_data(df)

    expected_cols = [
        "country",
        "year",
        "carbon_footprint",
        "ingestion_date"
    ]

    assert list(result.columns) == expected_cols
    assert result.iloc[0]["country"] == "NL"


@patch.object(pd.DataFrame, "to_parquet")
def test_save_parquet(mock_to_parquet):

    df = pd.DataFrame({"a": [1]})

    path = "test.parquet"

    result = Utility.save_parquet(df, path)

    mock_to_parquet.assert_called_once_with(path)
    assert result == path


@patch("utility.datetime")
def test_get_year_list_with_end_year(mock_datetime):

    mock_datetime.now.return_value = datetime(2025, 1, 1)

    years = list(Utility.get_year_list(2020, 2022))

    assert years == [2020, 2021, 2022]


@patch("utility.datetime")
def test_get_year_list_without_end_year(mock_datetime):

    mock_datetime.now.return_value = datetime(2025, 1, 1)

    years = list(Utility.get_year_list(2023))

    assert years == [2023, 2024, 2025]


@patch("utility.os.chdir")
@patch("utility.subprocess.run")
def test_run_dbt(mock_run, mock_chdir):

    Utility.run_dbt("/tmp/dbt")

    mock_chdir.assert_called_once_with("/tmp/dbt")

    mock_run.assert_called_once_with(
        ["dbt", "run"],
        check=True
    )


@patch("utility.configure_spark_with_delta_pip")
@patch("utility.SparkSession")
def test_create_spark(mock_spark, mock_configure):

    mock_builder = MagicMock()
    mock_spark.builder = mock_builder

    mock_session = MagicMock()
    mock_configure.return_value.getOrCreate.return_value = mock_session

    spark = Utility.create_spark()

    assert spark == mock_session

    mock_configure.assert_called_once()
