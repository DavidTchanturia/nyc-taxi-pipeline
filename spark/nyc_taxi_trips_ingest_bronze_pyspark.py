"""
NYC Yellow Taxi Data Ingestion Pipeline
Ingests data for a range of months from NYC TLC and writes to:
  - GCS:       gs://nyc-taxi-trips-bronze/year=YYYY/month=MM/
  - BigQuery:  nyc-taxi-488014.nyc_taxi_trips_bronze.nyc_yellow_taxi_trips_bronze
Designed for Dataproc Serverless.

Args:
  --start-date   YYYY-MM  (e.g. 2025-01)
  --end-date     YYYY-MM  (e.g. 2025-03)
  --write-mode   overwrite | append  (default: overwrite)
"""

import os
import subprocess
import tempfile
import argparse
from datetime import datetime, timezone

import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────
SOURCE_URL_TEMPLATE = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
)

GCS_BRONZE_BUCKET = "gs://nyc-taxi-trips-bronze"
GCS_STAGING       = "gs://nyc-taxi-trips-bronze/staging"
BQ_TABLE          = "nyc-taxi-488014.nyc_taxi_trips_bronze.nyc_yellow_taxi_trips_bronze"


# ─────────────────────────────────────────────
# Schema casts
# ─────────────────────────────────────────────
COLUMN_CASTS = {
    "VendorID":               "integer",
    "tpep_pickup_datetime":   "timestamp",
    "tpep_dropoff_datetime":  "timestamp",
    "passenger_count":        "double",
    "trip_distance":          "double",
    "RatecodeID":             "double",
    "store_and_fwd_flag":     "string",
    "PULocationID":           "integer",
    "DOLocationID":           "integer",
    "payment_type":           "integer",
    "fare_amount":            "double",
    "extra":                  "double",
    "mta_tax":                "double",
    "tip_amount":             "double",
    "tolls_amount":           "double",
    "improvement_surcharge":  "double",
    "total_amount":           "double",
    "congestion_surcharge":   "double",
    "airport_fee":            "double",
}


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────
def month_range(start_ym: str, end_ym: str):
    """
    Yields (year, month) tuples from start_ym to end_ym inclusive.
    Both args in YYYY-MM format.
    """
    start = datetime.strptime(start_ym, "%Y-%m")
    end   = datetime.strptime(end_ym,   "%Y-%m")
    cur   = start
    while cur <= end:
        yield cur.year, cur.month
        # Advance by one month
        if cur.month == 12:
            cur = cur.replace(year=cur.year + 1, month=1)
        else:
            cur = cur.replace(month=cur.month + 1)


def download_to_gcs(url: str, gcs_staging: str) -> str:
    """
    downloads parquet file to staging bucket
    """
    print(f"[download] Fetching: {url}")
    response = requests.get(url, stream=True, timeout=180)
    response.raise_for_status()

    filename  = url.split("/")[-1]
    local_tmp = os.path.join(tempfile.gettempdir(), filename)

    with open(local_tmp, "wb") as f:
        for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
            f.write(chunk)

    gcs_path = f"{gcs_staging}/{filename}"
    print(f"[download] Uploading to GCS staging: {gcs_path}")
    result = subprocess.run(
        ["gsutil", "cp", local_tmp, gcs_path],
        check=True, capture_output=True, text=True
    )
    print(f"[download] gsutil output: {result.stdout or result.stderr}")
    os.remove(local_tmp)
    return gcs_path


def write_to_gcs(df, bronze_bucket: str, year: int, month: int, write_mode: str):
    """
    writes parquet file to bronze bucket
    """
    output_path = f"{bronze_bucket}/year={year}/month={month:02d}"
    print(f"[gcs] Writing parquet to: {output_path}")
    df.coalesce(4).write.mode(write_mode).parquet(output_path)
    print(f"[gcs] Done: {output_path}")


def write_to_bigquery(df, bq_table: str, write_mode: str):
    """write data to bigquery table"""
    print(f"[bigquery] Writing to: {bq_table}")
    (
        df.write
        .format("bigquery")
        .option("table",       bq_table)
        .option("writeMethod", "direct")
        # only overwrite each month partition
        .option("partitionField", "ingestion_month")
        .option("partitionType", "MONTH")
        .mode(write_mode)
        .save()
    )
    print(f"[bigquery] Done: {bq_table}")


def ingest_month(spark, year: int, month: int, args):
    """
    if multiple arguments are provided to the process, e.g date range.
    downloads data in batches in months
    """
    print(f"\n{'='*60}")
    print(f"[pipeline] Processing {year}-{month:02d}")
    print(f"{'='*60}")

    source_url  = SOURCE_URL_TEMPLATE.format(year=year, month=month)
    source_file = source_url.split("/")[-1]

    # 1. Download & stage
    gcs_parquet = download_to_gcs(source_url, args.gcs_staging)

    # 2. Read
    print(f"[read] Reading: {gcs_parquet}")
    df = spark.read.parquet(gcs_parquet)
    print(f"[read] Raw row count: {df.count():,}")

    # 3. Cast schema
    select_exprs = []
    for col_name, col_type in COLUMN_CASTS.items():
        if col_name in df.columns:
            select_exprs.append(F.col(col_name).cast(col_type).alias(col_name))
        else:
            select_exprs.append(F.lit(None).cast(col_type).alias(col_name))
    df = df.select(*select_exprs)

    # 4. Filter to target month
    df = df.filter(
        (F.year(F.col("tpep_pickup_datetime"))  == year) &
        (F.month(F.col("tpep_pickup_datetime")) == month)
    )
    print(f"[filter] Rows after date filter ({year}-{month:02d}): {df.count():,}")

    # 5. Metadata columns
    ingestion_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    df = (
        df
        .withColumn("ingestion_timestamp", F.to_timestamp(F.lit(ingestion_ts)))
        .withColumn("source_file",         F.lit(source_file))
        .withColumn("ingestion_month",     F.lit(f"{year}-{month:02d}"))
    )

    # 6. Cache & write to both sinks
    df.cache()
    df.count()

    write_to_gcs(df, args.bronze_bucket, year, month, args.write_mode)
    write_to_bigquery(df, args.bq_table, args.write_mode)

    df.unpersist()

    # 7. Clean up staging file
    subprocess.run(["gsutil", "rm", gcs_parquet], check=False)
    print(f"[pipeline] Completed {year}-{month:02d}")


def main():
    """main entry point of pyspark job"""
    parser = argparse.ArgumentParser(description="NYC Taxi Dual-Sink Ingestion Pipeline")
    parser.add_argument("--start-date",    required=True,  help="Start month YYYY-MM")
    parser.add_argument("--end-date",      required=True,  help="End month YYYY-MM")
    parser.add_argument("--bronze-bucket", default=GCS_BRONZE_BUCKET)
    parser.add_argument("--gcs-staging",   default=GCS_STAGING)
    parser.add_argument("--bq-table",      default=BQ_TABLE)
    parser.add_argument("--write-mode",    default="overwrite")
    args = parser.parse_args()

    months = list(month_range(args.start_date, args.end_date))
    print(f"[main] Months to process: {months}")

    spark = (
        SparkSession.builder
        .appName(f"nyc-taxi-ingestion-{args.start_date}-to-{args.end_date}")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    for year, month in months:
        ingest_month(spark, year, month, args)

    print("\n[done] All months processed successfully.")
    spark.stop()


if __name__ == "__main__":
    main()
