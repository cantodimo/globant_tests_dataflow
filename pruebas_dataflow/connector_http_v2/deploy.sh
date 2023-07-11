# (Optional) Enable to use Kaniko cache by default.
gcloud config set project rosy-zoo-390619

gcloud config set builds/use_kaniko True

export PROJECT="$(gcloud config get-value project)"
export BUCKET="dataflow_bucket_camilo_diaz"
export TEMPLATE_IMAGE="gcr.io/$PROJECT/http_connector_v2/streaming:latest"

export DATASET="dataflow_tests"
export TABLE="http_connector_v2"
export TOPIC="http_connector_v2"

gcloud builds submit --tag "$TEMPLATE_IMAGE" .

export TEMPLATE_PATH="gs://$BUCKET/http_connector_v2/streaming.json"

# Build the Flex Template.
gcloud dataflow flex-template build $TEMPLATE_PATH \
  --image "$TEMPLATE_IMAGE" \
  --sdk-language "PYTHON" \
  --metadata-file "metadata.json"

export REGION="us-east1"

# Run the Flex Template.
gcloud dataflow flex-template run "http-connector-v2" \
    --template-file-gcs-location "$TEMPLATE_PATH" \
    --parameters input_subscription="projects/$PROJECT/subscriptions/$TOPIC" \
    --parameters output_table="$PROJECT:$DATASET.$TABLE" \
    --max-workers 2 \
    --sdk_location container \ 
    --region "$REGION" #--update