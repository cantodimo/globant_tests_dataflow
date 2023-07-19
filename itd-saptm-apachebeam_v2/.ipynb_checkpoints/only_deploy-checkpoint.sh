# (Optional) Enable to use Kaniko cache by default.
gcloud config set project rosy-zoo-390619

gcloud config set builds/use_kaniko True

export PROJECT="$(gcloud config get-value project)"
export BUCKET="dataflow_bucket_camilo_diaz"
export TEMPLATE_IMAGE="gcr.io/$PROJECT/itd-saptm-apachebeam/streaming:latest"

export TEMPLATE_PATH="gs://$BUCKET/itd-saptm-apachebeam/streaming.json"
export REGION="us-east1"
#export REGION="us-west1"
#export REGION="us-central1"

# Run the Flex Template.
gcloud dataflow flex-template run "itd-kf-kf-filtering" \
    --template-file-gcs-location "$TEMPLATE_PATH" \
    --max-workers 2 \
    --region "$REGION" \
    --parameters bootstrap_servers="35.193.114.205:9092" \
    --parameters group_id="test-consumer-group" \
    --parameters topics="test-kafka-resume-job-3-partitions_tp2:test-kafka-resume-job-3-partitions_tp3" \
    --parameters output_topic="test-kafka-output-dataflow" \
    --parameters start_read_time=0 \
    --parameters commit_offset_in_finalize=1 \
    --parameters columns_to_compare="item_no|mfg_div_cd:phys_loc_cd|plnr_cd" \
    --update
