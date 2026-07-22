from __future__ import annotations

import os
import uuid
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


JOB_NAME = "aviation_spark_feature_job"
POSTGRES_DRIVER = "org.postgresql.Driver"


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


def jdbc_options() -> dict[str, str]:
    host = env("WAREHOUSE_HOST", "postgres-warehouse")
    db = env("WAREHOUSE_DB", "aviation_dw")
    user = env("WAREHOUSE_USER", "aviation")
    password = env("WAREHOUSE_PASSWORD", "aviation")
    return {
        "url": f"jdbc:postgresql://{host}:5432/{db}",
        "user": user,
        "password": password,
        "driver": POSTGRES_DRIVER,
    }


def csv_path(kind: str) -> str:
    if kind == "bts":
        year = int(env("BTS_YEAR", "2026"))
        month = int(env("BTS_MONTH", "4"))
        return env(
            "SPARK_BTS_PATH",
            f"/opt/spark/work-dir/data/input/bts/bts_on_time_{year}_{month:02d}.csv",
        )
    if kind == "bookings":
        return env(
            "SPARK_BOOKINGS_PATH",
            "/opt/spark/work-dir/data/generated/bookings/bookings.csv",
        )
    raise ValueError(f"Unknown csv kind: {kind}")


def numeric(column_name: str):
    return F.col(column_name).cast(DoubleType())


def normalized_code(column_name: str):
    return F.upper(F.trim(F.regexp_replace(F.col(column_name), "-T[0-9]+$", "")))


def write_jdbc(df, table_name: str, mode: str = "overwrite") -> None:
    options = jdbc_options()
    (
        df.write.format("jdbc")
        .option("url", options["url"])
        .option("dbtable", table_name)
        .option("user", options["user"])
        .option("password", options["password"])
        .option("driver", options["driver"])
        .mode(mode)
        .save()
    )


def main() -> None:
    started_at = datetime.utcnow()
    job_run_id = str(uuid.uuid4())
    spark = (
        SparkSession.builder.appName(JOB_NAME)
        .config("spark.sql.shuffle.partitions", env("SPARK_SHUFFLE_PARTITIONS", "8"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    status = "SUCCESS"
    error_message = None
    source_flight_rows = 0
    source_booking_rows = 0
    output_route_delay_rows = 0
    output_route_booking_rows = 0

    try:
        flights = (
            spark.read.option("header", True)
            .option("inferSchema", False)
            .csv(csv_path("bts"))
        )
        bookings = (
            spark.read.option("header", True)
            .option("inferSchema", False)
            .csv(csv_path("bookings"))
        )

        source_flight_rows = flights.count()
        source_booking_rows = bookings.count()

        flight_features = (
            flights.where(F.col("FlightDate").isNotNull())
            .withColumn("origin_airport", normalized_code("Origin"))
            .withColumn("destination_airport", normalized_code("Dest"))
            .withColumn("route", F.concat_ws("-", F.col("origin_airport"), F.col("destination_airport")))
            .withColumn("airline", normalized_code("Reporting_Airline"))
            .withColumn("dep_delay_minutes", numeric("DepDelayMinutes"))
            .withColumn("arr_delay_minutes", numeric("ArrDelayMinutes"))
            .withColumn("cancelled", numeric("Cancelled"))
            .withColumn("diverted", numeric("Diverted"))
            .withColumn("distance_miles", numeric("Distance"))
            .withColumn("carrier_delay", numeric("CarrierDelay"))
            .withColumn("weather_delay", numeric("WeatherDelay"))
            .withColumn("nas_delay", numeric("NASDelay"))
            .withColumn("security_delay", numeric("SecurityDelay"))
            .withColumn("late_aircraft_delay", numeric("LateAircraftDelay"))
            .groupBy("route", "airline")
            .agg(
                F.count("*").alias("flight_count"),
                F.sum(F.when(F.col("cancelled") == 1, 1).otherwise(0)).alias("cancelled_count"),
                F.sum(F.when(F.col("diverted") == 1, 1).otherwise(0)).alias("diverted_count"),
                F.avg("dep_delay_minutes").alias("avg_dep_delay_minutes"),
                F.avg("arr_delay_minutes").alias("avg_arr_delay_minutes"),
                F.expr("percentile_approx(arr_delay_minutes, 0.95)").alias("p95_arr_delay_minutes"),
                F.avg("distance_miles").alias("avg_distance_miles"),
                F.sum(F.coalesce(F.col("carrier_delay"), F.lit(0))).alias("carrier_delay_minutes"),
                F.sum(F.coalesce(F.col("weather_delay"), F.lit(0))).alias("weather_delay_minutes"),
                F.sum(F.coalesce(F.col("nas_delay"), F.lit(0))).alias("nas_delay_minutes"),
                F.sum(F.coalesce(F.col("security_delay"), F.lit(0))).alias("security_delay_minutes"),
                F.sum(F.coalesce(F.col("late_aircraft_delay"), F.lit(0))).alias("late_aircraft_delay_minutes"),
            )
            .withColumn("spark_processed_at", F.current_timestamp())
        )

        booking_clean = (
            bookings.withColumn("route", F.concat_ws("-", normalized_code("origin_airport"), normalized_code("destination_airport")))
            .withColumn("booking_status_clean", F.upper(F.trim(F.col("booking_status"))))
            .withColumn("booking_channel_clean", F.upper(F.regexp_replace(F.trim(F.col("booking_channel")), " ", "_")))
            .withColumn("ticket_price_clean", F.abs(numeric("ticket_price")))
            .withColumn(
                "booking_status_canonical",
                F.when(F.col("booking_status_clean").isin("CONFIRMED", "CONF", "BOOKED", "COMPLETE"), "CONFIRMED")
                .when(F.col("booking_status_clean").isin("CANCELLED", "CANCELED", "VOID", "CNCL"), "CANCELLED")
                .when(F.col("booking_status_clean").isin("PENDING", "PEND", "IN_PROGRESS", "AWAITING_PAYMENT"), "PENDING")
                .otherwise(F.col("booking_status_clean")),
            )
        )

        booking_features = (
            booking_clean.groupBy("route")
            .agg(
                F.count("*").alias("booking_event_count"),
                F.sum(F.when(F.col("booking_status_canonical") == "CONFIRMED", 1).otherwise(0)).alias("confirmed_booking_events"),
                F.sum(F.when(F.col("booking_status_canonical") == "CANCELLED", 1).otherwise(0)).alias("cancelled_booking_events"),
                F.sum(F.when(F.col("booking_status_canonical") == "PENDING", 1).otherwise(0)).alias("pending_booking_events"),
                F.countDistinct(F.when(F.col("customer_id") != "", F.col("customer_id"))).alias("distinct_customers"),
                F.sum("ticket_price_clean").alias("total_ticket_value"),
                F.avg("ticket_price_clean").alias("avg_ticket_price"),
                F.sum(F.when(F.col("booking_channel_clean") == "WEB", 1).otherwise(0)).alias("web_booking_events"),
                F.sum(F.when(F.col("booking_channel_clean") == "MOBILE_APP", 1).otherwise(0)).alias("mobile_booking_events"),
                F.sum(F.when(F.col("booking_channel_clean") == "AGENCY", 1).otherwise(0)).alias("agency_booking_events"),
                F.sum(F.when(F.col("booking_channel_clean") == "CALL_CENTER", 1).otherwise(0)).alias("call_center_booking_events"),
            )
            .withColumn("spark_processed_at", F.current_timestamp())
        )

        output_route_delay_rows = flight_features.count()
        output_route_booking_rows = booking_features.count()

        write_jdbc(flight_features, "mart.spark_route_delay_features")
        write_jdbc(booking_features, "mart.spark_route_booking_features")
    except Exception as exc:
        status = "FAILED"
        error_message = str(exc)[:2000]
        raise
    finally:
        completed_at = datetime.utcnow()
        audit_df = spark.createDataFrame(
            [
                (
                    job_run_id,
                    JOB_NAME,
                    source_flight_rows,
                    source_booking_rows,
                    output_route_delay_rows,
                    output_route_booking_rows,
                    status,
                    started_at,
                    completed_at,
                    error_message,
                )
            ],
            [
                "job_run_id",
                "job_name",
                "source_flight_rows",
                "source_booking_rows",
                "output_route_delay_rows",
                "output_route_booking_rows",
                "status",
                "started_at",
                "completed_at",
                "error_message",
            ],
        )
        write_jdbc(audit_df, "metadata.spark_job_audit", mode="append")
        spark.stop()


if __name__ == "__main__":
    main()
