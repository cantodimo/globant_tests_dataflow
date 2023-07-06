export PROJECT="$(gcloud config get-value project)"
export TOPIC="messages_dataflow_streaming_docker"
export DATASET="dataflow_tests"
export TABLE="spliteable_and_stateful_pardo_test"
export BUCKET="dataflow_bucket_camilo_diaz"
export SUBSCRIPTION="$TOPIC"

export TEMPLATE_IMAGE="gcr.io/$PROJECT/dataflow_tests/test-spliteable-and-stateful-pardo:latest"
export TEMPLATE_PATH="gs://$BUCKET/dataflow_tests/test-spliteable-and-stateful-pardo.json"

export REGION="us-east1" #"us-west1"

# Build the Flex Template.
gcloud dataflow flex-template build $TEMPLATE_PATH \
  --image "$TEMPLATE_IMAGE" \
  --sdk-language "PYTHON" \
  --metadata-file "metadata_kafka.json"
  --worker-region "$REGION"

echo "---------------------------------------"

# Run the Flex Template.
gcloud dataflow flex-template run "test-spliteable-and-stateful-pardo" \
    --template-file-gcs-location "$TEMPLATE_PATH" \
    --parameters input_subscription="projects/$PROJECT/subscriptions/$SUBSCRIPTION" \
    --parameters output_table="$PROJECT:$DATASET.$TABLE" \
    --region "$REGION" \
    --worker-region "$REGION" \
    --max-workers 2