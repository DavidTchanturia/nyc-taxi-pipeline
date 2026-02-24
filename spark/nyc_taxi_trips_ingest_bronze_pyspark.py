"""
NYC Yellow Taxi Data Ingestion Pipeline (with structured logging)
Designed for Dataproc Serverless.
"""

import os
import subprocess
import tempfile
import argparse
import logging
from datetime import datetime, timezone

import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger("bronze_ingestion")


# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────
SOURCE_URL_TEMPLATE = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
)

GCS_BRONZE_BUCKET = "gs://nyc-taxi-trips-bronze"
GCS_STAGING       = "gs://nyc-taxi-trips-bronze/staging"
BQ_TABLE          = "nyc-taxi-488014.nyc_taxi_trips_bronze.nyc_yellow_taxi_trips_bronze"


COLUMN_CASTS = {
    "VendorID": "integer",
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "passenger_count": "double",
    "trip_distance": "double",
    "RatecodeID": "double",
    "store_and_fwd_flag": "string",
    "PULocationID": "integer",
    "DOLocationID": "integer",
    "payment_type": "integer",
    "fare_amount": "double",
    "extra": "double",
    "mta_tax": "double",
    "tip_amount": "double",
    "tolls_amount": "double",
    "improvement_surcharge": "double",
    "total_amount": "double",
    "congestion_surcharge": "double",
    "airport_fee": "double",
}


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────
def month_range(start_ym: str, end_ym: str):
    """calculates month ranges for which to run the pipeline"""
    start = datetime.strptime(start_ym, "%Y-%m")
    end = datetime.strptime(end_ym, "%Y-%m")
    cur = start
    while cur <= end:
        yield cur.year, cur.month
        cur = cur.replace(year=cur.year + (cur.month == 12),
                          month=1 if cur.month == 12 else cur.month + 1)


def download_to_gcs(url: str, gcs_staging: str) -> str:
    """downloads parquet file to bronze bucket"""
    log.info("Downloading: %s", url)
    response = requests.get(url, stream=True, timeout=180)
    response.raise_for_status()

    filename = url.split("/")[-1]
    local_tmp = os.path.join(tempfile.gettempdir(), filename)

    with open(local_tmp, "wb") as f:
        for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
            f.write(chunk)

    gcs_path = f"{gcs_staging}/{filename}"
    log.info("Uploading to staging: %s", gcs_path)

    subprocess.run(
        ["gsutil", "cp", local_tmp, gcs_path],
        check=True, capture_output=True, text=True
    )

    os.remove(local_tmp)
    return gcs_path


def write_to_gcs(df, bronze_bucket: str, year: int, month: int, write_mode: str):
    """writes parquet file to bronze bucket"""
    output_path = f"{bronze_bucket}/year={year}/month={month:02d}"
    log.info("Writing parquet → GCS: %s (mode=%s)", output_path, write_mode)
    df.coalesce(4).write.mode(write_mode).parquet(output_path)
    log.info("GCS write completed: %s", output_path)


def write_to_bigquery(df, bq_table: str, write_mode: str):
    """writes data to bigquery"""
    log.info("Writing → BigQuery: %s (mode=%s)", bq_table, write_mode)
    (
        df.write
        .format("bigquery")
        .option("table", bq_table)
        .option("writeMethod", "direct")
        .option("partitionField", "ingestion_month")
        .option("partitionType", "MONTH")
        .mode(write_mode)
        .save()
    )
    log.info("BigQuery write completed: %s", bq_table)


# ─────────────────────────────────────────────
# Core ingestion
# ─────────────────────────────────────────────
def ingest_month(spark, year: int, month: int, args):
    """ingest single month"""
    log.info("──────────────── Processing %s-%02d ────────────────", year, month)

    source_url = SOURCE_URL_TEMPLATE.format(year=year, month=month)
    source_file = source_url.split("/")[-1]

    # 1. Download
    gcs_parquet = download_to_gcs(source_url, args.gcs_staging)

    # 2. Read
    log.info("Reading parquet: %s", gcs_parquet)
    df = spark.read.parquet(gcs_parquet)
    raw_count = df.count()
    log.info("Raw rows: %,d", raw_count)

    if raw_count == 0:
        log.warning("No rows found for %s-%02d", year, month)
        return

    # 3. Cast schema
    select_exprs = []
    for col_name, col_type in COLUMN_CASTS.items():
        if col_name in df.columns:
            select_exprs.append(F.col(col_name).cast(col_type).alias(col_name))
        else:
            select_exprs.append(F.lit(None).cast(col_type).alias(col_name))
    df = df.select(*select_exprs)

    # 4. Filter month
    df = df.filter(
        (F.year("tpep_pickup_datetime") == year) &
        (F.month("tpep_pickup_datetime") == month)
    )
    filtered_count = df.count()
    log.info("Rows after month filter: %,d", filtered_count)

    # 5. Metadata
    ingestion_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    df = (
        df
        .withColumn("ingestion_timestamp", F.to_timestamp(F.lit(ingestion_ts)))
        .withColumn("source_file", F.lit(source_file))
        .withColumn("ingestion_month", F.lit(f"{year}-{month:02d}"))
    )

    df.cache()
    df.count()

    # 6. Writes
    write_to_gcs(df, args.bronze_bucket, year, month, args.write_mode)
    write_to_bigquery(df, args.bq_table, args.write_mode)

    df.unpersist()

    subprocess.run(["gsutil", "rm", gcs_parquet], check=False)
    log.info("Completed %s-%02d", year, month)


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    """main entry point for puspark job"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--bronze-bucket", default=GCS_BRONZE_BUCKET)
    parser.add_argument("--gcs-staging", default=GCS_STAGING)
    parser.add_argument("--bq-table", default=BQ_TABLE)
    parser.add_argument("--write-mode", default="overwrite")
    args = parser.parse_args()

    months = list(month_range(args.start_date, args.end_date))
    log.info("Pipeline started — months=%s", months)

    spark = SparkSession.builder.appName("nyc_taxi_ingestion").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    for year, month in months:
        ingest_month(spark, year, month, args)

    log.info("All months processed successfully.")
    spark.stop()


if __name__ == "__main__":
    main()