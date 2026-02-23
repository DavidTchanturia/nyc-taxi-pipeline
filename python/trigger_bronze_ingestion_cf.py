"""
Cloud Function — Trigger NYC Taxi Dataproc Serverless Batch

Expected JSON body:
  {
    "start_date": "2025-01",   # YYYY-MM
    "end_date":   "2025-03"    # YYYY-MM
  }

Optional:
  {
    "write_mode": "append"     # default: overwrite
  }
"""

import os
import json
import functions_framework
from google.cloud import dataproc_v1

# ─────────────────────────────────────────────
# Config — override via environment variables
# ─────────────────────────────────────────────
PROJECT_ID   = os.environ.get("PROJECT_ID",   "nyc-taxi-488014")
REGION       = os.environ.get("REGION",       "europe-west1")
SUBNETWORK   = os.environ.get("SUBNETWORK",   "")
SERVICE_ACCT = os.environ.get("DATAPROC_SA",  "dataproc-sa@nyc-taxi-488014.iam.gserviceaccount.com")
PYSPARK_FILE = os.environ.get(
    "PYSPARK_FILE",
    "gs://nyc-taxi-trips-spark-source-codes/spark/nyc_taxi_trips_ingest_bronze_pyspark.py"
)


@functions_framework.http
def trigger_dataproc_batch(request):
    """HTTP Cloud Function — submits a Dataproc Serverless batch job."""

    # ── Parse & validate request ─────────────────────────────────────────────
    try:
        body = request.get_json(force=True)
    except Exception:
        return ("Invalid JSON body", 400)

    start_date = (body or {}).get("start_date")
    end_date   = (body or {}).get("end_date")
    write_mode = (body or {}).get("write_mode", "overwrite")

    if not start_date or not end_date:
        return (
            json.dumps({"error": "start_date and end_date are required (YYYY-MM format)"}),
            400,
            {"Content-Type": "application/json"},
        )

    # check date format
    for label, val in [("start_date", start_date), ("end_date", end_date)]:
        parts = val.split("-")
        if len(parts) != 2 or not all(p.isdigit() for p in parts):
            return (
                json.dumps({"error": f"{label} must be YYYY-MM format"}),
                400,
                {"Content-Type": "application/json"},
            )

    # ── Build Dataproc batch spec ─────────────────────────────────────────────
    batch = {
        "pyspark_batch": {
            "main_python_file_uri": PYSPARK_FILE,
            "args": [
                "--start-date", start_date,
                "--end-date",   end_date,
                "--write-mode", write_mode,
            ],
        },
        "environment_config": {
            "execution_config": {
                "service_account": SERVICE_ACCT,
                **({"subnetwork_uri": SUBNETWORK} if SUBNETWORK else {}),
            }
        },
        "runtime_config": {
            "version": "2.2",
            "properties": {
                "spark.executor.instances":        "2",
                "spark.executor.cores":            "4",
                "spark.executor.memory":           "4g",
                "spark.driver.memory":             "4g",
                "spark.dataproc.driver.disk.size": "250g",
            },
        },
    }

    # ── Submit batch ──────────────────────────────────────────────────────────
    client = dataproc_v1.BatchControllerClient(
        client_options={"api_endpoint": f"{REGION}-dataproc.googleapis.com:443"}
    )

    request_obj = dataproc_v1.CreateBatchRequest(
        parent=f"projects/{PROJECT_ID}/locations/{REGION}",
        batch=batch,
    )

    operation = client.create_batch(request=request_obj)

    # operation.metadata contains the batch name immediately
    batch_name = operation.metadata.batch if hasattr(operation, "metadata") else "submitted"

    response_body = {
        "status":     "submitted",
        "batch":      str(batch_name),
        "start_date": start_date,
        "end_date":   end_date,
        "write_mode": write_mode,
    }

    return (
        json.dumps(response_body),
        200,
        {"Content-Type": "application/json"},
    )