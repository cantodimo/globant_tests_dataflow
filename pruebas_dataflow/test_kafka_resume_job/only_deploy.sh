export PROJECT="$(gcloud config get-value project)"
export BUCKET="dataflow_bucket_camilo_diaz"
export TEMPLATE_IMAGE="gcr.io/$PROJECT/test_kafka_resume_job/streaming:latest"

export DATASET="dataflow_streaming_tutorial"
export TABLE="test_kafka_resume_job_multiple_partitions_2"
#export TOPIC="test-kafka-resume-job"
export TOPIC="test-kafka-resume-job-3-partitions"


export TEMPLATE_PATH="gs://$BUCKET/test_kafka_resume_job/streaming.json"
export REGION="us-east1"
#export REGION="us-west1"

# Run the Flex Template.
gcloud dataflow flex-template run "test-kafka-resume-job6" \
    --template-file-gcs-location "$TEMPLATE_PATH" \
    --max-workers 2 \
    --region "$REGION" \
    --parameters input_topic="$TOPIC" \
    --parameters output_topic="test-kafka-output-dataflow" \
    --parameters group_id="test-consumer-group" \
    --parameters bootstrap_servers="35.193.114.205:9092" \
    --parameters commit_offset_in_finalize=1 \
    --parameters with_metadata=0 \
    --parameters allow_fail=0 \
    --parameters start_read_time=0 \
    --parameters delay_time=60 \
    --parameters messages_per_delay=30 \
    --parameters messages_per_fail=1000000
#    --update

    
