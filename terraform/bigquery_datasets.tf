resource "google_bigquery_dataset" "nyc_taxi_trips_bronze" {
  project     = var.project_id
  dataset_id  = "nyc_taxi_trips_bronze"
  description = "Raw ingested NYC taxi trip data"
  location    = var.region
}

resource "google_bigquery_dataset" "nyc_taxi_trips_gold" {
  project     = var.project_id
  dataset_id  = "nyc_taxi_trips_gold"
  description = "Dimensional model - fact and dimension tables"
  location    = var.region
}