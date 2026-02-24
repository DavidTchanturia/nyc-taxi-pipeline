"""
Cloud Function — Trigger NYC Taxi Dataproc Serverless Batch

Expected JSON body (all fields optional):
  {
    "start_date": "2025-01",   # YYYY-MM — defaults to current month - 3
    "end_date":   "2025-03",   # YYYY-MM — defaults to current month - 3
    "write_mode": "append"     # default: overwrite
  }
"""

import os
import json
from datetime import date
import functions_framework
from google.cloud import dataproc_v1

# ─────────────────────────────────────────────
# Config — sa and pyspark file path will be provided to cf from environment variables
# ─────────────────────────────────────────────
PROJECT_ID   = os.environ.get("PROJECT_ID",   "nyc-taxi-488014")
REGION       = os.environ.get("REGION",       "europe-west1")
SUBNETWORK   = os.environ.get("SUBNETWORK",   "")
SERVICE_ACCT = os.environ.get("DATAPROC_SA",  "")
PYSPARK_FILE = os.environ.get("PYSPARK_FILE", "")


def default_ingest_month() -> str:
    """Returns YYYY-MM for current month minus 3, with year rollover handling."""
    today = date.today()
    month = today.month - 3
    year  = today.year
    if month <= 0:
        month += 12
        year  -= 1
    return f"{year}-{month:02d}"


def validate_ym_format(label: str, val: str):
    """Returns an error response tuple if invalid, else None."""
    parts = val.split("-")
    if len(parts) != 2 or not all(p.isdigit() for p in parts):
        return (
            json.dumps({"error": f"{label} must be YYYY-MM format"}),
            400,
            {"Content-Type": "application/json"},
        )
    return None


@functions_framework.http
def trigger_dataproc_batch(request):
    """HTTP Cloud Function — submits a Dataproc Serverless batch job."""

    # ── Parse request ─────────────────────────────────────────────────────────
    body = request.get_json(force=True, silent=True) or {}


    # Default both dates to current month - 3 if not supplied
    default_ym = default_ingest_month()
    start_date = body.get("start_date", default_ym)
    end_date   = body.get("end_date",   default_ym)
    write_mode = body.get("write_mode", "overwrite")

    # ── Validate formats ──────────────────────────────────────────────────────
    for label, val in [("start_date", start_date), ("end_date", end_date)]:
        err = validate_ym_format(label, val)
        if err:
            return err

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
    batch_name = operation.metadata.batch if hasattr(operation, "metadata") else "submitted"

    return (
        json.dumps({
            "status":     "submitted",
            "batch":      str(batch_name),
            "start_date": start_date,
            "end_date":   end_date,
            "write_mode": write_mode,
        }),
        200,
        {"Content-Type": "application/json"},
    )
