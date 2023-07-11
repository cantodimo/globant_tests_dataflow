import random
import json

def publish_messages_with_batch_settings(project_id: str, topic_id: str) -> None:
    """Publishes multiple messages to a Pub/Sub topic with batch settings."""
    # [START pubsub_publisher_batch_settings]
    from concurrent import futures
    from google.cloud import pubsub_v1

    # TODO(developer)
    # project_id = "your-project-id"
    # topic_id = "your-topic-id"

    # Configure the batch to publish as soon as there are 10 messages
    # or 1 KiB of data, or 1 second has passed.
    batch_settings = pubsub_v1.types.BatchSettings(
        max_messages=100,  # default 100
        max_bytes=1024,  # default 1 MB
        max_latency=1,  # default 10 ms
    )
    publisher = pubsub_v1.PublisherClient(batch_settings)
    topic_path = publisher.topic_path(project_id, topic_id)
    publish_futures = []

    # Resolve the publish future in a separate thread.
    def callback(future: pubsub_v1.publisher.futures.Future) -> None:
        message_id = future.result()
        print(message_id)

    for n in range(1, 2):
        data_str = json.dumps({"id_request":n, "prob_continuar": 100 })
        # Data must be a bytestring
        data = data_str.encode("utf-8")
        publish_future = publisher.publish(topic_path, data)
        # Non-blocking. Allow the publisher client to batch multiple messages.
        publish_future.add_done_callback(callback)
        publish_futures.append(publish_future)

    futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)

    print(f"Published messages with batch settings to {topic_path}.")

publish_messages_with_batch_settings("rosy-zoo-390619", "http_connector_v2")