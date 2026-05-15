"""Tests for date utility functions."""

from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import (
    IntegerType,
    StructField,
    StructType,
    TimestampType,
)

from utils.dates import build_event_date_from_struct, date_trunc_week_sunday

_DATE_STRUCT_SCHEMA = StructType(
    [
        StructField(
            "date",
            StructType(
                [
                    StructField("year", IntegerType(), True),
                    StructField("month", IntegerType(), True),
                    StructField("day", IntegerType(), True),
                ]
            ),
            True,
        ),
        StructField("created_at", TimestampType(), True),
    ]
)


class TestBuildEventDateFromStruct:
    """Test cases for build_event_date_from_struct function."""

    @pytest.mark.spark
    def test_valid_year_month_day_uses_struct(self, spark: SparkSession) -> None:
        """Valid year/month/day returns make_date(year, month, day)."""
        df = spark.createDataFrame(
            [
                {
                    "date": {"year": 2024, "month": 3, "day": 15},
                    "created_at": datetime(2020, 1, 1),
                }
            ],
            _DATE_STRUCT_SCHEMA,
        )
        result = df.withColumn(
            "event_date", build_event_date_from_struct("date", "created_at")
        )
        row = result.collect()[0]
        assert row["event_date"] == datetime(2024, 3, 15).date()

    @pytest.mark.spark
    def test_null_month_defaults_to_1(self, spark: SparkSession) -> None:
        """Null month is coalesced to 1."""
        df = spark.createDataFrame(
            [
                {
                    "date": {"year": 2024, "month": None, "day": 10},
                    "created_at": datetime(2020, 1, 1),
                }
            ],
            _DATE_STRUCT_SCHEMA,
        )
        result = df.withColumn(
            "event_date", build_event_date_from_struct("date", "created_at")
        )
        row = result.collect()[0]
        assert row["event_date"] == datetime(2024, 1, 10).date()

    @pytest.mark.spark
    def test_zero_month_defaults_to_1(self, spark: SparkSession) -> None:
        """Zero month (legacy sentinel) is coalesced to 1."""
        df = spark.createDataFrame(
            [
                {
                    "date": {"year": 2024, "month": 0, "day": 10},
                    "created_at": datetime(2020, 1, 1),
                }
            ],
            _DATE_STRUCT_SCHEMA,
        )
        result = df.withColumn(
            "event_date", build_event_date_from_struct("date", "created_at")
        )
        row = result.collect()[0]
        assert row["event_date"] == datetime(2024, 1, 10).date()

    @pytest.mark.spark
    def test_null_day_defaults_to_1(self, spark: SparkSession) -> None:
        """Null day is coalesced to 1."""
        df = spark.createDataFrame(
            [
                {
                    "date": {"year": 2024, "month": 6, "day": None},
                    "created_at": datetime(2020, 1, 1),
                }
            ],
            _DATE_STRUCT_SCHEMA,
        )
        result = df.withColumn(
            "event_date", build_event_date_from_struct("date", "created_at")
        )
        row = result.collect()[0]
        assert row["event_date"] == datetime(2024, 6, 1).date()

    @pytest.mark.spark
    def test_zero_day_defaults_to_1(self, spark: SparkSession) -> None:
        """Zero day (legacy sentinel) is coalesced to 1."""
        df = spark.createDataFrame(
            [
                {
                    "date": {"year": 2024, "month": 6, "day": 0},
                    "created_at": datetime(2020, 1, 1),
                }
            ],
            _DATE_STRUCT_SCHEMA,
        )
        result = df.withColumn(
            "event_date", build_event_date_from_struct("date", "created_at")
        )
        row = result.collect()[0]
        assert row["event_date"] == datetime(2024, 6, 1).date()

    @pytest.mark.spark
    def test_year_below_1920_falls_back_to_created_at(
        self, spark: SparkSession
    ) -> None:
        """Year < 1920 falls back to created_at date."""
        df = spark.createDataFrame(
            [
                {
                    "date": {"year": 1900, "month": 5, "day": 10},
                    "created_at": datetime(2023, 8, 20),
                }
            ],
            _DATE_STRUCT_SCHEMA,
        )
        result = df.withColumn(
            "event_date", build_event_date_from_struct("date", "created_at")
        )
        row = result.collect()[0]
        assert row["event_date"] == datetime(2023, 8, 20).date()

    @pytest.mark.spark
    def test_null_year_falls_back_to_created_at(self, spark: SparkSession) -> None:
        """Null year falls back to created_at date."""
        df = spark.createDataFrame(
            [
                {
                    "date": {"year": None, "month": 5, "day": 10},
                    "created_at": datetime(2023, 8, 20),
                }
            ],
            _DATE_STRUCT_SCHEMA,
        )
        result = df.withColumn(
            "event_date", build_event_date_from_struct("date", "created_at")
        )
        row = result.collect()[0]
        assert row["event_date"] == datetime(2023, 8, 20).date()

    @pytest.mark.spark
    def test_null_date_struct_falls_back_to_created_at(
        self, spark: SparkSession
    ) -> None:
        """Fully null date struct falls back to created_at."""
        df = spark.createDataFrame(
            [{"date": None, "created_at": datetime(2023, 4, 5)}],
            _DATE_STRUCT_SCHEMA,
        )
        result = df.withColumn(
            "event_date", build_event_date_from_struct("date", "created_at")
        )
        row = result.collect()[0]
        assert row["event_date"] == datetime(2023, 4, 5).date()


class TestDateTruncWeekSunday:
    """Test cases for date_trunc_week_sunday function"""

    @pytest.mark.spark
    def test_monday_truncates_to_previous_sunday(self, spark: SparkSession) -> None:
        """Test that Monday returns the previous Sunday"""
        # Monday, January 15, 2024 -> Sunday, January 14, 2024
        df = spark.createDataFrame([{"date": datetime(2024, 1, 15, 10, 30, 45)}])

        result_df = df.withColumn("week", date_trunc_week_sunday(f.col("date")))
        result = result_df.collect()[0]

        assert result["week"] == datetime(2024, 1, 14, 0, 0, 0)

    @pytest.mark.spark
    def test_sunday_truncates_to_itself(self, spark: SparkSession) -> None:
        """Test that Sunday returns itself at 00:00:00"""
        # Sunday, January 14, 2024 10:30:45 -> Sunday, January 14, 2024 00:00:00
        df = spark.createDataFrame([{"date": datetime(2024, 1, 14, 10, 30, 45)}])

        result_df = df.withColumn("week", date_trunc_week_sunday(f.col("date")))
        result = result_df.collect()[0]

        assert result["week"] == datetime(2024, 1, 14, 0, 0, 0)

    @pytest.mark.spark
    def test_saturday_truncates_to_previous_sunday(self, spark: SparkSession) -> None:
        """Test that Saturday returns the previous Sunday"""
        # Saturday, January 20, 2024 -> Sunday, January 14, 2024
        df = spark.createDataFrame([{"date": datetime(2024, 1, 20, 15, 45, 30)}])

        result_df = df.withColumn("week", date_trunc_week_sunday(f.col("date")))
        result = result_df.collect()[0]

        assert result["week"] == datetime(2024, 1, 14, 0, 0, 0)

    @pytest.mark.spark
    def test_tuesday_truncates_to_previous_sunday(self, spark: SparkSession) -> None:
        """Test that Tuesday returns the previous Sunday"""
        # Tuesday, January 16, 2024 -> Sunday, January 14, 2024
        df = spark.createDataFrame([{"date": datetime(2024, 1, 16, 8, 15, 0)}])

        result_df = df.withColumn("week", date_trunc_week_sunday(f.col("date")))
        result = result_df.collect()[0]

        assert result["week"] == datetime(2024, 1, 14, 0, 0, 0)

    @pytest.mark.spark
    def test_multiple_dates_same_week(self, spark: SparkSession) -> None:
        """Test that multiple dates in the same week return the same Sunday"""
        df = spark.createDataFrame(
            [
                {"date": datetime(2024, 1, 14, 10, 0, 0)},  # Sunday
                {"date": datetime(2024, 1, 15, 10, 0, 0)},  # Monday
                {"date": datetime(2024, 1, 16, 10, 0, 0)},  # Tuesday
                {"date": datetime(2024, 1, 17, 10, 0, 0)},  # Wednesday
                {"date": datetime(2024, 1, 18, 10, 0, 0)},  # Thursday
                {"date": datetime(2024, 1, 19, 10, 0, 0)},  # Friday
                {"date": datetime(2024, 1, 20, 10, 0, 0)},  # Saturday
            ]
        )

        result_df = df.withColumn("week", date_trunc_week_sunday(f.col("date")))
        results = result_df.collect()

        # All should return Sunday, January 14, 2024 00:00:00
        expected_week = datetime(2024, 1, 14, 0, 0, 0)
        for row in results:
            assert row["week"] == expected_week

    @pytest.mark.spark
    def test_different_weeks(self, spark: SparkSession) -> None:
        """Test that dates from different weeks return different Sundays"""
        df = spark.createDataFrame(
            [
                {"date": datetime(2024, 1, 15, 10, 0, 0)},  # Week of Jan 14
                {"date": datetime(2024, 1, 22, 10, 0, 0)},  # Week of Jan 21
                {"date": datetime(2024, 1, 29, 10, 0, 0)},  # Week of Jan 28
            ]
        )

        result_df = df.withColumn("week", date_trunc_week_sunday(f.col("date")))
        results = result_df.collect()

        assert results[0]["week"] == datetime(2024, 1, 14, 0, 0, 0)
        assert results[1]["week"] == datetime(2024, 1, 21, 0, 0, 0)
        assert results[2]["week"] == datetime(2024, 1, 28, 0, 0, 0)

    @pytest.mark.spark
    def test_time_component_is_zeroed(self, spark: SparkSession) -> None:
        """Test that time component is always set to 00:00:00"""
        df = spark.createDataFrame(
            [
                {"date": datetime(2024, 1, 15, 23, 59, 59)},
                {"date": datetime(2024, 1, 16, 1, 30, 45)},
            ]
        )

        result_df = df.withColumn("week", date_trunc_week_sunday(f.col("date")))
        results = result_df.collect()

        for row in results:
            week = row["week"]
            assert week.hour == 0
            assert week.minute == 0
            assert week.second == 0

    @pytest.mark.spark
    def test_leap_year_february(self, spark: SparkSession) -> None:
        """Test that leap year dates work correctly"""
        # February 29, 2024 (Thursday) -> Sunday, February 25, 2024
        df = spark.createDataFrame([{"date": datetime(2024, 2, 29, 12, 0, 0)}])

        result_df = df.withColumn("week", date_trunc_week_sunday(f.col("date")))
        result = result_df.collect()[0]

        assert result["week"] == datetime(2024, 2, 25, 0, 0, 0)

    @pytest.mark.spark
    def test_year_boundary(self, spark: SparkSession) -> None:
        """Test dates around year boundaries"""
        df = spark.createDataFrame(
            [
                {"date": datetime(2023, 12, 31, 23, 59, 59)},  # Sunday
                {"date": datetime(2024, 1, 1, 0, 0, 0)},  # Monday
            ]
        )

        result_df = df.withColumn("week", date_trunc_week_sunday(f.col("date")))
        results = result_df.collect()

        # Dec 31, 2023 is Sunday -> returns itself
        assert results[0]["week"] == datetime(2023, 12, 31, 0, 0, 0)
        # Jan 1, 2024 is Monday -> returns previous Sunday (Dec 31, 2023)
        assert results[1]["week"] == datetime(2023, 12, 31, 0, 0, 0)
