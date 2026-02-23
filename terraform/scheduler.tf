resource "google_cloud_scheduler_job" "nyc_taxi_bronze_ingestion" {
  name        = "nyc-taxi-bronze-monthly-ingestion"
  description = "Triggers NYC Taxi bronze ingestion CF on the 1st of every month"
  schedule    = "0 0 1 * *"
  time_zone   = "UTC"
  project     = var.project_id
  region      = var.region

  http_target {
    uri         = "https://trigger-bronze-ingestion-344181310958.europe-west1.run.app"
    http_method = "POST"

    headers = {
      "Content-Type" = "application/json"
    }

    oidc_token {
      service_account_email = google_service_account.scheduler_sa.email
      audience              = "https://trigger-bronze-ingestion-344181310958.europe-west1.run.app"
    }
  }

  depends_on = [
    google_project_iam_member.scheduler_cf_invoker,
    google_project_iam_member.scheduler_run_invoker,
  ]
}