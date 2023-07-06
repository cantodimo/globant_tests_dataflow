export PROJECT="$(gcloud config get-value project)"
export BUCKET="dataflow_bucket_camilo_diaz"
export TEMPLATE_IMAGE="gcr.io/$PROJECT/tutorial_dataflow/streaming-kafka:latest"

export DATASET="dataflow_streaming_tutorial"
export TABLE="streaming_docker_kafka2"
export TOPIC="messages_dataflow_streaming_docker"
export SUBSCRIPTION="$TOPIC"


export TEMPLATE_PATH="gs://$BUCKET/tutorial_dataflow/streaming-kafka.json"
export REGION="us-east1"

# Run the Flex Template.
gcloud dataflow flex-template run "tutorial-docker-kafka" \
    --template-file-gcs-location "$TEMPLATE_PATH" \
    --parameters input_subscription="quickstart-events" \
    --parameters output_table="$PROJECT:$DATASET.$TABLE" \
    --max-workers 2 \
    --region "$REGION"