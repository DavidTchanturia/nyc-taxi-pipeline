terraform {
  backend "gcs" {
    bucket = "nyc-taxi-tfstate"
    prefix = "terraform/state"
  }
}