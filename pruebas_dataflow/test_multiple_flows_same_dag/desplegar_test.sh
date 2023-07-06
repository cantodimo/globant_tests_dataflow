# (Optional) Enable to use Kaniko cache by default.
#gcloud config set project rosy-zoo-390619 to avoid set it on cloud shel...

gcloud config set builds/use_kaniko True

export PROJECT="$(gcloud config get-value project)"
export BUCKET="dataflow_bucket_camilo_diaz"
export TEMPLATE_IMAGE="gcr.io/$PROJECT/test_multiple_flows_dataflow/streaming:latest"

export DATASET="dataflow_streaming_tutorial"
export TABLE="test_multiple_flows_dataflow"
export TOPIC="messages_dataflow_streaming_docker"
#export SUBSCRIPTION="$TOPIC"

# Build the image into Container Registry, this is roughly equivalent to:
#   gcloud auth configure-docker
#   docker image build -t $TEMPLATE_IMAGE .
#   docker push $TEMPLATE_IMAGE
gcloud builds submit --tag "$TEMPLATE_IMAGE" .

export TEMPLATE_PATH="gs://$BUCKET/test_multiple_flows_dataflow/streaming.json"

# Build the Flex Template.
gcloud dataflow flex-template build $TEMPLATE_PATH \
  --image "$TEMPLATE_IMAGE" \
  --sdk-language "PYTHON" \
  --metadata-file "metadata_kafka.json"

export REGION="us-east1"

# Run the Flex Template.
gcloud dataflow flex-template run "test-multiple-flows-dataflow" \
    --template-file-gcs-location "$TEMPLATE_PATH" \
    --parameters input_subscription="$TOPIC" \
    --parameters output_table="$PROJECT:$DATASET.$TABLE" \
    --max-workers 2 \
    --region "$REGION" #--update