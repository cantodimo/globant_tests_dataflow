## los comandos que pide hacer el tutorial al inicio

gcloud config set project rosy-zoo-390619

export BUCKET="dataflow_bucket_camilo_diaz"
gsutil mb gs://$BUCKET

gcloud pubsub topics create $TOPIC
gcloud pubsub subscriptions create --topic $TOPIC $SUBSCRIPTION

export PROJECT="$(gcloud config get-value project)"
export DATASET="dataflow_streaming_tutorial"
export TABLE="streaming_docker"

bq mk --dataset "$PROJECT:$DATASET"

# no clonare toda esa mrda
#git clone https://github.com/GoogleCloudPlatform/python-docs-samples.git
#cd python-docs-samples/dataflow/flex-templates/streaming_beam
