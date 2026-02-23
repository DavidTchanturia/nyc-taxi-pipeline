# bronze tables
resource "google_bigquery_table" "nyc_yellow_taxi_trips_bronze" {
  dataset_id          = google_bigquery_dataset.nyc_taxi_trips_bronze.dataset_id
  table_id            = "nyc_yellow_taxi_trips_bronze"
  project             = var.project_id
  deletion_protection = false

  time_partitioning {
    type  = "MONTH"
    field = "tpep_pickup_datetime"
  }

  schema = jsonencode([
    { name = "VendorID", type = "INTEGER", mode = "NULLABLE" },
    { name = "tpep_pickup_datetime", type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "tpep_dropoff_datetime", type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "passenger_count", type = "FLOAT", mode = "NULLABLE" },
    { name = "trip_distance", type = "FLOAT", mode = "NULLABLE" },
    { name = "RatecodeID", type = "FLOAT", mode = "NULLABLE" },
    { name = "store_and_fwd_flag", type = "STRING", mode = "NULLABLE" },
    { name = "PULocationID", type = "INTEGER", mode = "NULLABLE" },
    { name = "DOLocationID", type = "INTEGER", mode = "NULLABLE" },
    { name = "payment_type", type = "INTEGER", mode = "NULLABLE" },
    { name = "fare_amount", type = "FLOAT", mode = "NULLABLE" },
    { name = "extra", type = "FLOAT", mode = "NULLABLE" },
    { name = "mta_tax", type = "FLOAT", mode = "NULLABLE" },
    { name = "tip_amount", type = "FLOAT", mode = "NULLABLE" },
    { name = "tolls_amount", type = "FLOAT", mode = "NULLABLE" },
    { name = "improvement_surcharge", type = "FLOAT", mode = "NULLABLE" },
    { name = "total_amount", type = "FLOAT", mode = "NULLABLE" },
    { name = "congestion_surcharge", type = "FLOAT", mode = "NULLABLE" },
    { name = "airport_fee", type = "FLOAT", mode = "NULLABLE" },
    # metadata
    { name = "ingestion_timestamp", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "source_file", type = "STRING", mode = "NULLABLE" },
    { name = "ingestion_month", type = "STRING", mode = "NULLABLE" }
  ])
}

resource "google_bigquery_table" "bronze_taxi_zone_lookup" {
  dataset_id          = google_bigquery_dataset.nyc_taxi_trips_bronze.dataset_id
  table_id            = "taxi_zone_lookup"
  project             = var.project_id
  deletion_protection = false

  schema = jsonencode([
    { name = "LocationID", type = "INTEGER", mode = "REQUIRED" },
    { name = "Borough", type = "STRING", mode = "NULLABLE" },
    { name = "Zone", type = "STRING", mode = "NULLABLE" },
    { name = "service_zone", type = "STRING", mode = "NULLABLE" },
    { name = "ingestion_timestamp", type = "TIMESTAMP", mode = "REQUIRED" }
  ])
}

# gold_tables.tf

resource "google_bigquery_table" "dim_date" {
  dataset_id          = google_bigquery_dataset.nyc_taxi_trips_gold.dataset_id
  table_id            = "dim_date"
  project             = var.project_id
  deletion_protection = false

  schema = jsonencode([
    { name = "date_id", type = "INTEGER", mode = "REQUIRED" }, # YYYYMMDD int
    { name = "full_date", type = "DATE", mode = "REQUIRED" },
    { name = "year", type = "INTEGER", mode = "NULLABLE" },
    { name = "month", type = "INTEGER", mode = "NULLABLE" },
    { name = "month_name", type = "STRING", mode = "NULLABLE" },
    { name = "day", type = "INTEGER", mode = "NULLABLE" },
    { name = "day_of_week", type = "INTEGER", mode = "NULLABLE" },
    { name = "day_name", type = "STRING", mode = "NULLABLE" },
    { name = "week_of_year", type = "INTEGER", mode = "NULLABLE" },
    { name = "quarter", type = "INTEGER", mode = "NULLABLE" },
    { name = "is_weekend", type = "BOOLEAN", mode = "NULLABLE" }
  ])
}

resource "google_bigquery_table" "dim_location" {
  dataset_id          = google_bigquery_dataset.nyc_taxi_trips_gold.dataset_id
  table_id            = "dim_location"
  project             = var.project_id
  deletion_protection = false

  schema = jsonencode([
    { name = "location_id", type = "INTEGER", mode = "REQUIRED" },
    { name = "borough", type = "STRING", mode = "NULLABLE" },
    { name = "zone", type = "STRING", mode = "NULLABLE" },
    { name = "service_zone", type = "STRING", mode = "NULLABLE" }
  ])
}

resource "google_bigquery_table" "dim_vendor" {
  dataset_id          = google_bigquery_dataset.nyc_taxi_trips_gold.dataset_id
  table_id            = "dim_vendor"
  project             = var.project_id
  deletion_protection = false

  schema = jsonencode([
    { name = "vendor_id", type = "INTEGER", mode = "REQUIRED" },
    { name = "vendor_name", type = "STRING", mode = "NULLABLE" }
  ])
}

resource "google_bigquery_table" "dim_payment_type" {
  dataset_id          = google_bigquery_dataset.nyc_taxi_trips_gold.dataset_id
  table_id            = "dim_payment_type"
  project             = var.project_id
  deletion_protection = false

  schema = jsonencode([
    { name = "payment_type_id", type = "INTEGER", mode = "REQUIRED" },
    { name = "payment_type_desc", type = "STRING", mode = "NULLABLE" }
  ])
}

resource "google_bigquery_table" "dim_rate_code" {
  dataset_id          = google_bigquery_dataset.nyc_taxi_trips_gold.dataset_id
  table_id            = "dim_rate_code"
  project             = var.project_id
  deletion_protection = false

  schema = jsonencode([
    { name = "rate_code_id", type = "INTEGER", mode = "REQUIRED" },
    { name = "rate_code_desc", type = "STRING", mode = "NULLABLE" }
  ])
}

resource "google_bigquery_table" "fact_trips" {
  dataset_id          = google_bigquery_dataset.nyc_taxi_trips_gold.dataset_id
  table_id            = "fact_trips"
  project             = var.project_id
  deletion_protection = false

  time_partitioning {
    type  = "MONTH"
    field = "pickup_timestamp"
  }

  clustering = ["pickup_location_id", "dropoff_location_id", "vendor_id"]

  schema = jsonencode([
    # surrogate key
    { name = "trip_id", type = "STRING", mode = "REQUIRED" }, # UUID generated in Spark

    # foreign keys
    { name = "vendor_id", type = "INTEGER", mode = "NULLABLE" },
    { name = "pickup_date_id", type = "INTEGER", mode = "NULLABLE" },      # FK -> dim_date
    { name = "dropoff_date_id", type = "INTEGER", mode = "NULLABLE" },     # FK -> dim_date
    { name = "pickup_location_id", type = "INTEGER", mode = "NULLABLE" },  # FK -> dim_location
    { name = "dropoff_location_id", type = "INTEGER", mode = "NULLABLE" }, # FK -> dim_location
    { name = "payment_type_id", type = "INTEGER", mode = "NULLABLE" },     # FK -> dim_payment_type
    { name = "rate_code_id", type = "INTEGER", mode = "NULLABLE" },        # FK -> dim_rate_code

    # degenerate dimensions
    { name = "store_and_fwd_flag", type = "STRING", mode = "NULLABLE" },

    # timestamps
    { name = "pickup_timestamp", type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "dropoff_timestamp", type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "trip_duration_minutes", type = "FLOAT", mode = "NULLABLE" },

    # measures / facts
    { name = "passenger_count", type = "INTEGER", mode = "NULLABLE" },
    { name = "trip_distance", type = "FLOAT", mode = "NULLABLE" },
    { name = "fare_amount", type = "NUMERIC", mode = "NULLABLE" },
    { name = "extra", type = "NUMERIC", mode = "NULLABLE" },
    { name = "mta_tax", type = "NUMERIC", mode = "NULLABLE" },
    { name = "tip_amount", type = "NUMERIC", mode = "NULLABLE" },
    { name = "tolls_amount", type = "NUMERIC", mode = "NULLABLE" },
    { name = "improvement_surcharge", type = "NUMERIC", mode = "NULLABLE" },
    { name = "congestion_surcharge", type = "NUMERIC", mode = "NULLABLE" },
    { name = "airport_fee", type = "NUMERIC", mode = "NULLABLE" },
    { name = "total_amount", type = "NUMERIC", mode = "NULLABLE" },

    # pipeline metadata
    { name = "ingestion_month", type = "STRING", mode = "NULLABLE" }
  ])
}