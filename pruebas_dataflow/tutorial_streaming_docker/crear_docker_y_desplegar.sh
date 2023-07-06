# (Optional) Enable to use Kaniko cache by default.
gcloud config set builds/use_kaniko True

export PROJECT="$(gcloud config get-value project)"
export BUCKET="dataflow_bucket_camilo_diaz"
export TEMPLATE_IMAGE="gcr.io/$PROJECT/tutorial_dataflow/streaming-beam:latest"

export DATASET="dataflow_streaming_tutorial"
export TABLE="streaming_docker"
export TOPIC="messages_dataflow_streaming_docker"
export SUBSCRIPTION="$TOPIC"

# Build the image into Container Registry, this is roughly equivalent to:
#   gcloud auth configure-docker
#   docker image build -t $TEMPLATE_IMAGE .
#   docker push $TEMPLATE_IMAGE
gcloud builds submit --tag "$TEMPLATE_IMAGE" .

export TEMPLATE_PATH="gs://$BUCKET/tutorial_dataflow/streaming-beam.json"

# Build the Flex Template.
gcloud dataflow flex-template build $TEMPLATE_PATH \
  --image "$TEMPLATE_IMAGE" \
  --sdk-language "PYTHON" \
  --metadata-file "metadata.json"

  export REGION="us-central1"

# Run the Flex Template.
gcloud dataflow flex-template run "tutorial-docker" \
    --template-file-gcs-location "$TEMPLATE_PATH" \
    --parameters input_subscription="projects/$PROJECT/subscriptions/$SUBSCRIPTION" \
    --parameters output_table="$PROJECT:$DATASET.$TABLE" \
    --region "$REGION"