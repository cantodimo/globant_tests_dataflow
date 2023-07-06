# (Optional) Enable to use Kaniko cache by default.
gcloud config set builds/use_kaniko True

export PROJECT="$(gcloud config get-value project)"
export DATASET="dataflow_streaming_tutorial"
export TABLE="test_kafka_resume_job"

export TEMPLATE_IMAGE="gcr.io/$PROJECT/test_kafka_resume_job/streaming:latest"

## intercambio los dockerfile pa no molestar con el yaml
#mv Dockerfile Dockerfile_actual
#mv Dockerfile_viejo Dockerfile
#mv Dockerfile_actual Dockerfile_viejo

gcloud builds submit --tag "$TEMPLATE_IMAGE" .

export TEMPLATE_PATH="gs://$BUCKET/test_kafka_resume_job/streaming.json"

# Build the Flex Template.
gcloud dataflow flex-template build $TEMPLATE_PATH \
  --image "$TEMPLATE_IMAGE" \
  --sdk-language "PYTHON" \
  --metadata-file "metadata_kafka.json"

export REGION="us-central1"

# Run the Flex Template.
gcloud dataflow flex-template run "test-kafka-resume-job" \
    --template-file-gcs-location "$TEMPLATE_PATH" \
    --parameters input_subscription="test_kafka_resume_job" \
    --parameters output_table="$PROJECT:$DATASET.$TABLE" \
    --region "$REGION" \
    --max-workers 2 
    --update