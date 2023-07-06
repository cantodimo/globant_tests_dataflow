# (Optional) Enable to use Kaniko cache by default.
gcloud config set builds/use_kaniko True

export PROJECT="$(gcloud config get-value project)"
export TOPIC="messages_dataflow_streaming_docker"
export DATASET="dataflow_tests"
export TABLE="spliteable_and_stateful_pardo_test"
export BUCKET="dataflow_bucket_camilo_diaz"
export SUBSCRIPTION="$TOPIC"

export TEMPLATE_IMAGE="gcr.io/$PROJECT/dataflow_tests/test-spliteable-and-stateful-pardo:latest"

gcloud builds submit --tag "$TEMPLATE_IMAGE" .

export TEMPLATE_PATH="gs://$BUCKET/dataflow_tests/test-spliteable-and-stateful-pardo.json"

# Build the Flex Template.
gcloud dataflow flex-template build $TEMPLATE_PATH \
  --image "$TEMPLATE_IMAGE" \
  --sdk-language "PYTHON" \
  --metadata-file "metadata_kafka.json"

export REGION= "us-west1" #"us-east1"

# Run the Flex Template.
gcloud dataflow flex-template run "test-spliteable-and-stateful-pardo" \
    --template-file-gcs-location "$TEMPLATE_PATH" \
    --parameters input_subscription="projects/$PROJECT/subscriptions/$SUBSCRIPTION" \
    --parameters output_table="$PROJECT:$DATASET.$TABLE" \
    --region "$REGION" \
    --max-workers 2