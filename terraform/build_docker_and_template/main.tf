variable "template_metadata_path" {
  description = "path from metadata json file"
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

  full_json_file_path = "${path.module}/${var.template_metadata_path}"
  json_content = jsondecode(file(local.full_json_file_path))
  template_content = jsonencode({
        image = "gcr.io/rosy-zoo-390619/gitlab_test/image_from_github:latest"
        sdkInfo = {
            language = "PYTHON"
        }
        metadata = json_content
    })

    template_gcs_path = "gs://dataflow_bucket_camilo_diaz/gitlab_test/itd-saptm-apachebeam/${base64encode(md5(local.template_content))}/metadata.json"
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

# there is not equivalent from gcloud flex template build on terraform, so this reproduces its behavior
resource "google_storage_bucket_object" "flex_template_metadata" {
    depends_on   = [docker_image.dataflow_docker_build]
    bucket       = "dataflow_bucket_camilo_diaz"
    name         = local.template_gcs_path
    content_type = "application/json"

    content = local.template_content
}

#I belive that it doesnt work due the workload identity permission, maybe it need more setup con cloud sdk
#resource "docker_registry_image" "dataflow_docker_build" {
#    provider  = docker.private
#    name = "gcr.io/rosy-zoo-390619/gitlab_test/image_from_github:latest"

#    build {
#        context = "itd-saptm-apachebeam_v2"
#    }
#}
