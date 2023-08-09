output "current_directory" {
  value = path.module
}

output "current_directory2" {
  value = path.root
}

output "current_directory3" {
  value = path.cwd
}

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
  alias = "private"
  #registry_auth {
  #  address = "gcr.io"
  #}
}

resource "docker_image" "dataflow_docker_build" {
    name = "local_image:latest"

    build {
        path = "itd-saptm-apachebeam_v2"
        dockerfile = "Dockerfile"
    }
}

#I belive that it doesnt work due the workload identity permission, maybe it need more setup con cloud sdk
#resource "docker_registry_image" "dataflow_docker_build" {
#    provider  = docker.private
#    name = "gcr.io/rosy-zoo-390619/gitlab_test/image_from_github:latest"

#    build {
#        context = "itd-saptm-apachebeam_v2"
#    }
#}
