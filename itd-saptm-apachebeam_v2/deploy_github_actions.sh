export PROJECT="$(gcloud config get-value project)"
export BUCKET="dataflow_bucket_camilo_diaz"
export TEMPLATE_IMAGE="gcr.io/$PROJECT/gitlab_test/image_from_github:latest"

export TEMPLATE_PATH="gs://$BUCKET/gitlab_test/itd-saptm-apachebeam/streaming.json"

# Build the Flex Template.
gcloud dataflow flex-template build $TEMPLATE_PATH \
  --image "$TEMPLATE_IMAGE" \
  --sdk-language "PYTHON" \
  --metadata-file "metadata/itd-kf-kf-filtering.json"

#export REGION="us-east1"
export REGION="us-west4"

# Run the Flex Template.
gcloud dataflow flex-template run "itd-kf-kf-filtering" \
    --template-file-gcs-location "$TEMPLATE_PATH" \
    --max-workers 2 \
    --region "$REGION" \
    --parameters bootstrap_servers="35.193.114.205:9092" \
    --parameters group_id="test-consumer-group" \
    --parameters output_topic="test-kafka-output-dataflow" \
    --parameters start_read_time=0 \
    --parameters commit_offset_in_finalize=1 \
    --parameters topics="test-kafka-resume-job-3-partitions_tp2" \
    --parameters columns_to_compare="item_no|mfg_div_cd"
