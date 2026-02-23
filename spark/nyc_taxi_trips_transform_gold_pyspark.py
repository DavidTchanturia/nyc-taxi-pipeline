"""
NYC Yellow Taxi — Bronze → Gold PySpark Pipeline
Target month : 2025-11
Project      : nyc-taxi-488014
Deploy on    : GCP Dataproc

Submit example:
  gcloud dataproc jobs submit pyspark bronze_to_gold_pipeline.py \
      --cluster=<CLUSTER_NAME> \
      --region=<REGION> \
      --jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-latest.jar \
      -- --temp_bucket=<YOUR_GCS_BUCKET>
"""

import argparse
import logging
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
    parser.add_argument("--project",    default="nyc-taxi-488014",                    help="GCP project ID")
    parser.add_argument("--temp_bucket",default="nyc-taxi-trips-bronze",              help="GCS bucket for BQ temp files (no gs:// prefix)")
    parser.add_argument("--month",      default="2025-11",                            help="Ingestion month filter (YYYY-MM)")
    parser.add_argument("--write_mode", default="append",                             help="BigQuery write mode: append | overwrite")
    return parser.parse_args()


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
    """Convert a timestamp column to a YYYYMMDD integer suitable for date_id FK."""
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
    """
    Build dim_date from the distinct pickup and dropoff dates present
    in the bronze slice — only rows we actually need.
    """
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
        F.dayofweek("full_date").cast(IntegerType()).alias("day_of_week"),  # 1=Sun, 7=Sat
        F.date_format("full_date", "EEEE").alias("day_name"),
        F.weekofyear("full_date").cast(IntegerType()).alias("week_of_year"),
        F.quarter("full_date").cast(IntegerType()).alias("quarter"),
        F.when(F.dayofweek("full_date").isin(1, 7), True)
         .otherwise(False)
         .alias("is_weekend"),
    )

    bq_write(dim_date, f"{project}.{gold}.dim_date", mode=mode)


# ──────────────────────────────────────────────────────────────
# FACT TABLE
# ──────────────────────────────────────────────────────────────
def load_fact_trips(spark, bronze_df, project, gold, mode):
    import uuid as _uuid

    uuid_udf = F.udf(lambda: str(_uuid.uuid4()))

    fact = bronze_df.select(
        # Surrogate key
        uuid_udf().alias("trip_id"),

        # Foreign keys
        F.col("VendorID").cast(IntegerType()).alias("vendor_id"),
        date_id("tpep_pickup_datetime").cast(IntegerType()).alias("pickup_date_id"),
        date_id("tpep_dropoff_datetime").cast(IntegerType()).alias("dropoff_date_id"),
        F.col("PULocationID").cast(IntegerType()).alias("pickup_location_id"),
        F.col("DOLocationID").cast(IntegerType()).alias("dropoff_location_id"),
        F.col("payment_type").cast(IntegerType()).alias("payment_type_id"),
        F.col("RatecodeID").cast(IntegerType()).alias("rate_code_id"),

        # Degenerate dimension
        F.col("store_and_fwd_flag"),

        # Timestamps & derived metric
        F.col("tpep_pickup_datetime").alias("pickup_timestamp"),
        F.col("tpep_dropoff_datetime").alias("dropoff_timestamp"),
        (
            (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60.0
        ).cast(FloatType()).alias("trip_duration_minutes"),

        # Measures
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

        # Pipeline metadata
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
# MAIN
# ──────────────────────────────────────────────────────────────
def main():
    args   = parse_args()
    PROJECT = args.project
    BRONZE  = "nyc_taxi_trips_bronze"
    GOLD    = "nyc_taxi_trips_gold"
    MONTH   = args.month
    MODE    = args.write_mode

    spark = build_spark(PROJECT, args.temp_bucket)
    log.info(f"Pipeline started — month={MONTH}, project={PROJECT}, write_mode={MODE}")

    # ── Read & filter bronze to target month ────────────────────
    bronze_raw = bq_read(spark, f"{PROJECT}.{BRONZE}.nyc_yellow_taxi_trips_bronze")
    bronze_df  = bronze_raw.filter(F.col("ingestion_month") == MONTH).cache()

    row_count = bronze_df.count()
    log.info(f"Bronze rows loaded for {MONTH}: {row_count:,}")

    if row_count == 0:
        log.warning(f"No rows found in bronze for month={MONTH}. Exiting early.")
        spark.stop()
        return

    # ── Small / static dimensions — always fully overwrite ──────
    log.info("Loading dim_vendor …")
    load_dim_vendor(spark, PROJECT, GOLD)

    log.info("Loading dim_payment_type …")
    load_dim_payment_type(spark, PROJECT, GOLD)

    log.info("Loading dim_rate_code …")
    load_dim_rate_code(spark, PROJECT, GOLD)

    log.info("Loading dim_location …")
    load_dim_location(spark, PROJECT, BRONZE, GOLD)

    # ── Date dimension — derived from the current bronze slice ──
    log.info("Loading dim_date …")
    load_dim_date(spark, bronze_df, PROJECT, GOLD, MODE)

    # ── Fact table ──────────────────────────────────────────────
    log.info("Loading fact_trips …")
    load_fact_trips(spark, bronze_df, PROJECT, GOLD, MODE)

    log.info("Pipeline completed successfully.")
    spark.stop()


if __name__ == "__main__":
    main()