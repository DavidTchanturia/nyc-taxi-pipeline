resource "google_storage_bucket" "nyc-taxi-trips-bronze" {
  name          = "nyc-taxi-trips-bronze"
  location      = "europe-west1"
  project       = var.project_id
  force_destroy = true

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type          = "SetStorageClass"
      storage_class = "ARCHIVE"
    }
  }
}

resource "google_storage_bucket" "nyc-taxi-trips-spark-source-codes" {
  name          = "nyc-taxi-trips-spark-source-codes"
  location      = "europe-west1"
  project       = var.project_id
  force_destroy = true

  uniform_bucket_level_access = true
}