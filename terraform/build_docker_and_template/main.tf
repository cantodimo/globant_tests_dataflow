locals {
  gcs_bucket_name = "dataflow_bucket_camilo_diaz"
  template_bucket = "dataflow_bucket_camilo_diaz"
  project_id      = "rosy-zoo-390619"
  df_job_name     = "test-from-terraform"
  region          = "us-west4"
  df_name         = "warranty_inference_engine"
}

terraform {
  #required_version = ">= 0.13"
  required_providers {
    docker = {
      source = "kreuzwerker/docker"
      version = "2.15.0"
    }
  }
}

provider "docker" {
}

resource "docker_image" "dataflow_docker_build" {
    name = "local_image:latest"

    build = {
        path = "itd-saptm-apachebeam_v2"
        dockerfile = "itd-saptm-apachebeam_v2.Dockerfile"
    }
}
