resource "google_storage_bucket" "bronze" {
  name          = "nyc-taxi-bronze"
  location      = "europe-west1"
  project       = var.project_id
  force_destroy = true

  uniform_bucket_level_access = true
}