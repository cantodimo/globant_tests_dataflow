from __future__ import annotations

import argparse
import json
import logging
import time
from typing import Any

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.transforms.window as window
from apache_beam.io.kafka import ReadFromKafka

from apache_beam.coders.coders import NullableCoder

import requests

# Defines the BigQuery schema for the output table.
SCHEMA = ",".join(
    [
        "message:STRING",
        "aditional_data:STRING",
        "date:TIMESTAMP"
    ]
)

def call_api( body):
    url = "https://us-central1-rosy-zoo-390619.cloudfunctions.net/api_rest_test_kafka"
    payload = json.dumps(body)
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    return json.loads(response.text)

def parse_json_message(message, input_parameters):
    """Parse the input json message and add 'score' & 'processing_time' keys."""
    #row = json.loads(message)

    #try:
    #    v= int(message.value.decode('UTF-8'))
    #except:
    #    v= "no pudo leer el value1"

    try:
        v= int(message[1].decode('UTF-8'))
    except:
        v= "no pudo leer el value2"

    #try:
    #    k= str(message.key)
    #except:
    #    k= "no pudo leer el key1"

    try:
        k= str(message[0])
    except:
        k= "no pudo leer el key2"    

    logging.warning( "message received: " + str(v) + " -- " + str(k) + " -- " + str(input_parameters))

    r= call_api({"message": v})

    ## when we want to test a failure, we can set it to true when updating the job
    ## put it as a template parameter... 
    if input_parameters["allow_fail"]:  
        if r["status"]:
          return {
            "message": str(v),
            "aditional_data": str(k),
            "date": int(time.time())
          }
        else:
          raise ValueError('receive message with pair number: ' + str(v))

    else:
        return {
            "message": str(v) + " " + str(r["status"]), #row["url"],
            "aditional_data": str(k),
            "date": int(time.time())
          }

def run(
    input_subscription: str,
    output_table: str,
    group_id: str,
    start_read_time: int,
    commit_offset_in_finalize: int,
    with_metadata: int,
    bootstrap_servers: str,
    allow_fail:int,
    beam_args: list[str] = None,
) -> None:
    """Build and run the pipeline."""
    options = PipelineOptions(beam_args, save_main_session=True, streaming=True, )
    allow_fail= bool( allow_fail )
    commit_offset_in_finalize= bool( commit_offset_in_finalize )
    with_metadata= bool( with_metadata )
    
    ## to confirm that the job is receiving the correct parameters.. (I couldnt pass bool..)
    input_parameters= {
        "allow_fail": allow_fail,
        "commit_offset_in_finalize": commit_offset_in_finalize,
        "group_id": group_id,
        "start_read_time": start_read_time,
        "with_metadata": with_metadata
    }

    consumer_config={'bootstrap.servers': bootstrap_servers}
    if group_id is not None:
        consumer_config["group.id"]= group_id
        
    with beam.Pipeline(options=options) as pipeline:
        messages = (
            pipeline
            | "Read from KAFKA"
            >> ReadFromKafka(
            consumer_config= consumer_config,
            topics=[input_subscription],
            start_read_time=start_read_time,
            commit_offset_in_finalize=commit_offset_in_finalize
            #key_deserializer='org.apache.kafka.common.serialization.ByteBufferDeserializer', se putea
            #with_metadata=with_metadata
            ) #.with_output_types(NullableCoder)
            | "Parse JSON messages 1" >> beam.Map(parse_json_message, input_parameters )
        )

        # Output the results into BigQuery table.
        _ = messages | "Write to Big Query" >> beam.io.WriteToBigQuery(
            output_table, schema=SCHEMA
        )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output_table",
        help="Output BigQuery table for results specified as: "
        "PROJECT:DATASET.TABLE or DATASET.TABLE.",
    )
    parser.add_argument(
        "--input_subscription",
        help="Input Topics "
        '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."',
    )
    parser.add_argument(
        "--group_id",
        default=None,
        type=str,
        help="kafka config parameter",
    )
    parser.add_argument(
        "--start_read_time",
        default=None,
        type=int,
        help="kafka config parameter",
    ) 
    parser.add_argument(
        "--commit_offset_in_finalize",
        type=int,
        help="kafka config parameter",
    ) 
    parser.add_argument(
        "--with_metadata",
        type=int,
        help="kafka config parameter",
    ) 
    parser.add_argument(
        "--bootstrap_servers", #"35.193.114.205:9092"
        type=str,
        help="kafka config parameter",
    )     
    parser.add_argument(
        "--allow_fail",
        type=int,
        help="for test create error on some messages, put 0 for disable",
    )   

    args, beam_args = parser.parse_known_args()

    run(
        input_subscription=args.input_subscription,
        output_table=args.output_table,
        group_id=args.group_id,
        start_read_time= args.start_read_time,
        commit_offset_in_finalize= args.commit_offset_in_finalize,
        with_metadata= args.with_metadata,
        bootstrap_servers= args.bootstrap_servers,
        allow_fail= args.allow_fail,
        beam_args=beam_args,
    )