"""
Unit tests for ingestion/nyc_taxi_loader.py

Covers:
  - month_to_filename: filename convention
  - normalize_columns: camelCase → snake_case, missing column backfill, output order
  - download_parquet: success path, HTTP error, timeout, retry on transient failure
"""

import pytest
import pandas as pd
import requests as req

from unittest.mock import MagicMock, patch

from ingestion.nyc_taxi_loader import month_to_filename, normalize_columns, download_parquet


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

ALL_CAMEL_COLS = {
    "VendorID": 1,
    "tpep_pickup_datetime": "2024-01-01 08:00:00",
    "tpep_dropoff_datetime": "2024-01-01 08:20:00",
    "passenger_count": 2,
    "trip_distance": 3.5,
    "RatecodeID": 1,
    "store_and_fwd_flag": "N",
    "PULocationID": 100,
    "DOLocationID": 200,
    "payment_type": 1,
    "fare_amount": 14.0,
    "extra": 0.5,
    "mta_tax": 0.5,
    "tip_amount": 3.0,
    "tolls_amount": 0.0,
    "improvement_surcharge": 0.3,
    "total_amount": 18.3,
    "congestion_surcharge": 2.5,
    "Airport_fee": 0.0,
}

EXPECTED_SNAKE_COLS = [
    "vendor_id",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "ratecode_id",
    "store_and_fwd_flag",
    "pu_location_id",
    "do_location_id",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
]


@pytest.fixture(autouse=True)
def no_tenacity_wait(monkeypatch):
    """Patch tenacity's sleep so retry backoff is instant in all tests."""
    monkeypatch.setattr("tenacity.nap.time.sleep", lambda _: None)


# ──────────────────────────────────────────────────────────────────────────────
# month_to_filename
# ──────────────────────────────────────────────────────────────────────────────


class TestMonthToFilename:
    def test_standard_month(self):
        assert month_to_filename("2024-01") == "yellow_tripdata_2024-01.parquet"

    def test_december(self):
        assert month_to_filename("2024-12") == "yellow_tripdata_2024-12.parquet"

    def test_contains_parquet_extension(self):
        assert month_to_filename("2023-06").endswith(".parquet")


# ──────────────────────────────────────────────────────────────────────────────
# normalize_columns
# ──────────────────────────────────────────────────────────────────────────────


class TestNormalizeColumns:
    def test_camel_case_renamed_to_snake_case(self):
        df = pd.DataFrame([ALL_CAMEL_COLS])
        result = normalize_columns(df)
        assert "vendor_id" in result.columns
        assert "pu_location_id" in result.columns
        assert "do_location_id" in result.columns
        assert "ratecode_id" in result.columns
        assert "airport_fee" in result.columns
        assert "VendorID" not in result.columns
        assert "PULocationID" not in result.columns

    def test_output_column_order_matches_schema(self):
        df = pd.DataFrame([ALL_CAMEL_COLS])
        result = normalize_columns(df)
        assert list(result.columns) == EXPECTED_SNAKE_COLS

    def test_missing_optional_column_filled_with_none(self):
        row = {k: v for k, v in ALL_CAMEL_COLS.items() if k != "Airport_fee"}
        df = pd.DataFrame([row])
        result = normalize_columns(df)
        assert "airport_fee" in result.columns
        assert result["airport_fee"].iloc[0] is None

    def test_all_expected_columns_present_even_if_source_has_extras(self):
        row = {**ALL_CAMEL_COLS, "extra_column_from_source": "ignored"}
        df = pd.DataFrame([row])
        result = normalize_columns(df)
        assert list(result.columns) == EXPECTED_SNAKE_COLS
        assert "extra_column_from_source" not in result.columns

    def test_values_preserved_after_rename(self):
        df = pd.DataFrame([ALL_CAMEL_COLS])
        result = normalize_columns(df)
        assert result["vendor_id"].iloc[0] == 1
        assert result["pu_location_id"].iloc[0] == 100
        assert result["total_amount"].iloc[0] == 18.3


# ──────────────────────────────────────────────────────────────────────────────
# download_parquet
# ──────────────────────────────────────────────────────────────────────────────


class TestDownloadParquet:
    def test_success_returns_bytes(self):
        mock_content = b"PAR1" + b"\x00" * 64
        with patch("ingestion.nyc_taxi_loader.requests.get") as mock_get:
            resp = MagicMock()
            resp.content = mock_content
            resp.raise_for_status.return_value = None
            mock_get.return_value = resp

            result = download_parquet("2024-01")

        assert result == mock_content

    def test_url_contains_correct_filename(self):
        with patch("ingestion.nyc_taxi_loader.requests.get") as mock_get:
            resp = MagicMock()
            resp.content = b"data"
            resp.raise_for_status.return_value = None
            mock_get.return_value = resp

            download_parquet("2024-06")

        call_url = mock_get.call_args[0][0]
        assert "yellow_tripdata_2024-06.parquet" in call_url

    def test_http_error_raises_after_retries(self):
        with patch("ingestion.nyc_taxi_loader.requests.get") as mock_get:
            resp = MagicMock()
            resp.raise_for_status.side_effect = req.HTTPError("404 Not Found")
            mock_get.return_value = resp

            with pytest.raises(req.HTTPError):
                download_parquet("2024-99")

        # tenacity retries 3 times before raising
        assert mock_get.call_count == 3

    def test_timeout_raises_after_retries(self):
        with patch("ingestion.nyc_taxi_loader.requests.get") as mock_get:
            mock_get.side_effect = req.Timeout("connection timed out")

            with pytest.raises(req.Timeout):
                download_parquet("2024-01")

        assert mock_get.call_count == 3

    def test_retries_on_transient_error_then_succeeds(self):
        mock_content = b"recovered"
        good_resp = MagicMock()
        good_resp.content = mock_content
        good_resp.raise_for_status.return_value = None

        bad_resp = MagicMock()
        bad_resp.raise_for_status.side_effect = req.HTTPError("503 Service Unavailable")

        with patch(
            "ingestion.nyc_taxi_loader.requests.get",
            side_effect=[bad_resp, good_resp],
        ) as mock_get:
            result = download_parquet("2024-01")

        assert result == mock_content
        assert mock_get.call_count == 2
