locals {
  gcs_bucket_name = "dataflow_bucket_camilo_diaz"
  template_bucket = "dataflow_bucket_camilo_diaz"
  project_id      = "rosy-zoo-390619"
  df_job_name     = "test-from-terraform"
  region          = "us-west4"
  df_name         = "warranty_inference_engine"
}

provider "google-beta" {
  project = local.project_id
  region  = local.region
}

terraform {
  #required_version = ">= 0.13"
  required_providers {

    #google-beta = {
    #  source  = "hashicorp/google"
    #  version = "~> 4.77.0"
    #}

    google = {
      source  = "hashicorp/google"
      version = ">= 3.52.0"
    }

    docker = {
      source = "kreuzwerker/docker"
      version = "3.0.2"
    }
  }

}

resource "google_dataflow_flex_template_job" "ejecutar_job" {
  provider              = google-beta
  project               = local.project_id
  name                  = local.df_job_name
  on_delete             = "cancel"
  region                = local.region
  max_workers           = 1
  #temp_gcs_location     = "gs://dataflow-staging-us-west4-760721552379/temp"
  container_spec_gcs_path     = "gs://${local.template_bucket}/gitlab_test/itd-saptm-apachebeam/streaming.json"
  network               = "default"
  parameters = {
    bootstrap_servers = "35.193.114.205:9092"
    group_id = "test-consumer-group"
    output_topic = "test-kafka-output-dataflow"
    start_read_time = 0
    commit_offset_in_finalize = 1
    topics = "test-kafka-resume-job-3-partitions_tp2"
    columns_to_compare = "item_no|mfg_div_cd"
  }
  enable_streaming_engine = true
}
