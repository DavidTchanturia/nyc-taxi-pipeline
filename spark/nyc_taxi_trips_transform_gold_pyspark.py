"""
NYC Yellow Taxi — Bronze → Gold PySpark Pipeline
Processes a date range (YYYY-MM) in monthly batches.
"""

import argparse
import logging
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType

# ──────────────────────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bronze_to_gold")


# ──────────────────────────────────────────────────────────────
# ARG PARSING
# ──────────────────────────────────────────────────────────────
def parse_args():
    parser = argparse.ArgumentParser(description="Bronze to Gold pipeline for NYC Taxi")
    parser.add_argument("--project",      default="nyc-taxi-488014",         help="GCP project ID")
    parser.add_argument("--temp_bucket",  default="nyc-taxi-trips-bronze",   help="GCS bucket for BQ temp files (no gs:// prefix)")
    parser.add_argument("--start-date",   required=True,                     help="Start month (YYYY-MM), inclusive")
    parser.add_argument("--end-date",     required=True,                     help="End month (YYYY-MM), inclusive")
    parser.add_argument("--write-mode",   default="append",               help="BigQuery write mode for first batch: overwrite | append")
    return parser.parse_args()


# ──────────────────────────────────────────────────────────────
# MONTH RANGE HELPER
# ──────────────────────────────────────────────────────────────
def month_range(start_ym: str, end_ym: str) -> list[str]:
    """
    Returns an ordered list of YYYY-MM strings from start_ym to end_ym inclusive.
    E.g. month_range("2025-01", "2025-03") -> ["2025-01", "2025-02", "2025-03"]
    """
    def to_ym(s):
        y, m = s.split("-")
        return int(y), int(m)

    sy, sm = to_ym(start_ym)
    ey, em = to_ym(end_ym)

    if (sy, sm) > (ey, em):
        raise ValueError(f"start_date {start_ym} is after end_date {end_ym}")

    months = []
    y, m = sy, sm
    while (y, m) <= (ey, em):
        months.append(f"{y}-{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1
    return months


# ──────────────────────────────────────────────────────────────
# SPARK SESSION
# ──────────────────────────────────────────────────────────────
def build_spark(project: str, temp_bucket: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("nyc_taxi_bronze_to_gold")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )
    spark.conf.set("parentProject", project)
    spark.conf.set("temporaryGcsBucket", temp_bucket)
    spark.conf.set("viewsEnabled", "true")
    return spark


# ──────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────
def bq_read(spark: SparkSession, table: str):
    return spark.read.format("bigquery").option("table", table).load()


def bq_write(df, table: str, mode: str = "append"):
    (
        df.write
        .format("bigquery")
        .option("table", table)
        .mode(mode)
        .save()
    )
    log.info(f"Written to {table}  (mode={mode})")


def date_id(col_name: str):
    return (
        F.year(col_name) * 10000
        + F.month(col_name) * 100
        + F.dayofmonth(col_name)
    )


# ──────────────────────────────────────────────────────────────
# STATIC DIMENSION DATA
# ──────────────────────────────────────────────────────────────
VENDOR_MAP = {
    1: "Creative Mobile Technologies, LLC",
    2: "Curb Mobility, LLC",
    6: "Myle Technologies Inc",
    7: "Helix",
}

PAYMENT_MAP = {
    0: "Flex Fare trip",
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip",
}

RATE_CODE_MAP = {
    1: "Standard rate",
    2: "JFK",
    3: "Newark",
    4: "Nassau or Westchester",
    5: "Negotiated fare",
    6: "Group ride",
    99: "Null/unknown",
}


# ──────────────────────────────────────────────────────────────
# DIMENSION LOADERS
# ──────────────────────────────────────────────────────────────
def load_dim_vendor(spark, project, gold):
    rows = [(k, v) for k, v in VENDOR_MAP.items()]
    df = spark.createDataFrame(rows, ["vendor_id", "vendor_name"])
    bq_write(df, f"{project}.{gold}.dim_vendor", mode="overwrite")


def load_dim_payment_type(spark, project, gold):
    rows = [(k, v) for k, v in PAYMENT_MAP.items()]
    df = spark.createDataFrame(rows, ["payment_type_id", "payment_type_desc"])
    bq_write(df, f"{project}.{gold}.dim_payment_type", mode="overwrite")


def load_dim_rate_code(spark, project, gold):
    rows = [(k, v) for k, v in RATE_CODE_MAP.items()]
    df = spark.createDataFrame(rows, ["rate_code_id", "rate_code_desc"])
    bq_write(df, f"{project}.{gold}.dim_rate_code", mode="overwrite")


def load_dim_location(spark, project, bronze, gold):
    zone_df = bq_read(spark, f"{project}.{bronze}.taxi_zone_lookup")
    dim_location = zone_df.select(
        F.col("LocationID").cast(IntegerType()).alias("location_id"),
        F.col("Borough").alias("borough"),
        F.col("Zone").alias("zone"),
        F.col("service_zone"),
    )
    bq_write(dim_location, f"{project}.{gold}.dim_location", mode="overwrite")


def load_dim_date(spark, bronze_df, project, gold, mode):
    pickup_dates  = bronze_df.select(F.to_date("tpep_pickup_datetime").alias("full_date"))
    dropoff_dates = bronze_df.select(F.to_date("tpep_dropoff_datetime").alias("full_date"))
    dates_df = (
        pickup_dates.union(dropoff_dates)
        .distinct()
        .filter(F.col("full_date").isNotNull())
    )

    dim_date = dates_df.select(
        date_id("full_date").cast(IntegerType()).alias("date_id"),
        F.col("full_date"),
        F.year("full_date").cast(IntegerType()).alias("year"),
        F.month("full_date").cast(IntegerType()).alias("month"),
        F.date_format("full_date", "MMMM").alias("month_name"),
        F.dayofmonth("full_date").cast(IntegerType()).alias("day"),
        F.dayofweek("full_date").cast(IntegerType()).alias("day_of_week"),
        F.date_format("full_date", "EEEE").alias("day_name"),
        F.weekofyear("full_date").cast(IntegerType()).alias("week_of_year"),
        F.quarter("full_date").cast(IntegerType()).alias("quarter"),
        F.when(F.dayofweek("full_date").isin(1, 7), True)
         .otherwise(False)
         .alias("is_weekend"),
    )

    bq_write(dim_date, f"{project}.{gold}.dim_date", mode=mode)


def load_fact_trips(spark, bronze_df, project, gold, mode):
    import uuid as _uuid

    uuid_udf = F.udf(lambda: str(_uuid.uuid4()))

    fact = bronze_df.select(
        uuid_udf().alias("trip_id"),
        F.col("VendorID").cast(IntegerType()).alias("vendor_id"),
        date_id("tpep_pickup_datetime").cast(IntegerType()).alias("pickup_date_id"),
        date_id("tpep_dropoff_datetime").cast(IntegerType()).alias("dropoff_date_id"),
        F.col("PULocationID").cast(IntegerType()).alias("pickup_location_id"),
        F.col("DOLocationID").cast(IntegerType()).alias("dropoff_location_id"),
        F.col("payment_type").cast(IntegerType()).alias("payment_type_id"),
        F.col("RatecodeID").cast(IntegerType()).alias("rate_code_id"),
        F.col("store_and_fwd_flag"),
        F.col("tpep_pickup_datetime").alias("pickup_timestamp"),
        F.col("tpep_dropoff_datetime").alias("dropoff_timestamp"),
        (
            (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60.0
        ).cast(FloatType()).alias("trip_duration_minutes"),
        F.col("passenger_count").cast(IntegerType()).alias("passenger_count"),
        F.col("trip_distance").cast(FloatType()).alias("trip_distance"),
        F.col("fare_amount").cast("decimal(10,2)").alias("fare_amount"),
        F.col("extra").cast("decimal(10,2)").alias("extra"),
        F.col("mta_tax").cast("decimal(10,2)").alias("mta_tax"),
        F.col("tip_amount").cast("decimal(10,2)").alias("tip_amount"),
        F.col("tolls_amount").cast("decimal(10,2)").alias("tolls_amount"),
        F.col("improvement_surcharge").cast("decimal(10,2)").alias("improvement_surcharge"),
        F.col("congestion_surcharge").cast("decimal(10,2)").alias("congestion_surcharge"),
        F.col("airport_fee").cast("decimal(10,2)").alias("airport_fee"),
        F.col("total_amount").cast("decimal(10,2)").alias("total_amount"),
        F.col("ingestion_month"),
    ).filter(
        F.col("tpep_pickup_datetime").isNotNull() &
        F.col("tpep_dropoff_datetime").isNotNull() &
        (F.col("trip_distance") > 0) &
        (F.col("total_amount") > 0) &
        (F.unix_timestamp("tpep_dropoff_datetime") > F.unix_timestamp("tpep_pickup_datetime"))
    )

    bq_write(fact, f"{project}.{gold}.fact_trips", mode=mode)


# ──────────────────────────────────────────────────────────────
# PROCESS A SINGLE MONTH
# ──────────────────────────────────────────────────────────────
def process_month(spark, bronze_raw, month: str, project: str, gold: str, fact_mode: str, date_mode: str):
    """Filter bronze to a single month and load dim_date + fact_trips."""
    log.info(f"── Processing month: {month} (fact_mode={fact_mode}) ──")

    bronze_df = bronze_raw.filter(F.col("ingestion_month") == month).cache()
    row_count = bronze_df.count()
    log.info(f"  Bronze rows for {month}: {row_count:,}")

    if row_count == 0:
        log.warning(f"  No rows found for month={month}. Skipping.")
        bronze_df.unpersist()
        return

    load_dim_date(spark, bronze_df, project, gold, date_mode)
    load_fact_trips(spark, bronze_df, project, gold, fact_mode)

    bronze_df.unpersist()
    log.info(f"── Completed month: {month} ──")


# ──────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────
def main():
    args    = parse_args()
    PROJECT = args.project
    BRONZE  = "nyc_taxi_trips_bronze"
    GOLD    = "nyc_taxi_trips_gold"
    start_date = args.start_date
    end_date   = args.end_date
    base_mode  = args.write_mode

    months = month_range(start_date, end_date)
    log.info(f"Pipeline started — months={months}, write_mode={base_mode}")

    spark = build_spark(PROJECT, args.temp_bucket)

    # ── Static dimensions — always overwrite once at the start ──
    log.info("Loading static dimensions …")
    load_dim_vendor(spark, PROJECT, GOLD)
    load_dim_payment_type(spark, PROJECT, GOLD)
    load_dim_rate_code(spark, PROJECT, GOLD)
    load_dim_location(spark, PROJECT, BRONZE, GOLD)

    # ── Read bronze with BQ-side filter covering the entire date range ──
    # The `filter` option is pushed down to BigQuery Storage API as a row restriction,
    # so only the relevant months are transferred over the wire.
    bq_filter = (
        f"ingestion_month >= '{start_date}' AND ingestion_month <= '{end_date}'"
    )
    log.info(f"Reading bronze with BQ filter: {bq_filter}")
    bronze_raw = (
        spark.read
        .format("bigquery")
        .option("table", f"{PROJECT}.{BRONZE}.nyc_yellow_taxi_trips_bronze")
        .option("filter", bq_filter)
        .load()
    )

    # ── Iterate months ──────────────────────────────────────────
    for i, month in enumerate(months):
        fact_mode = base_mode if i == 0 else "append"
        date_mode = base_mode if i == 0 else "append"
        process_month(spark, bronze_raw, month, PROJECT, GOLD, fact_mode, date_mode)

    log.info("All months processed. Pipeline completed successfully.")
    spark.stop()

if __name__ == "__main__":
    main()