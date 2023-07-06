#!/usr/bin/env python
#
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""An Apache Beam streaming pipeline example.
It reads JSON encoded messages from Pub/Sub, transforms the message data and
writes the results to BigQuery.
"""

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
        #"last_date:TIMESTAMP",
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

def parse_json_message(message):
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

    logging.warning( "message received: " + str(v) + " -- " + str(k))

    r= call_api({"message": v})

    if False:
        if r["status"]:
          return {
            "message": str(v), #row["url"],
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


class parse_json_message2(beam.DoFn):
    def process(self, element ): # : beam.Row no funciona
        try:
            v= element.value.decode('UTF-8')
        except:
            v= "no pudo leer el value"

        try:
            k= str(element.key)
        except:
            k= "no pudo leer el key"

        return {
          "url": json.dumps({ "message.value": v, "key":k}), #row["url"],
          "score": 0.0, #1.0 if row["review"] == "positive" else 0.0,
          "processing_time": int(time.time())
        }

def run(
    input_subscription: str,
    output_table: str,
    window_interval_sec: int = 60,
    beam_args: list[str] = None,
) -> None:
    """Build and run the pipeline."""
    options = PipelineOptions(beam_args, save_main_session=True, streaming=True, )

    with beam.Pipeline(options=options) as pipeline:
        messages = (
            pipeline
            | "Read from KAFKA"
            #>> beam.io.ReadFromPubSub(
            #    subscription=input_subscription
            #).with_output_types(bytes)
            >> ReadFromKafka(
            consumer_config={'bootstrap.servers': "35.193.114.205:9092", "group.id": "test-consumer-group"},
            topics=[input_subscription],
            start_read_time=0,
            commit_offset_in_finalize=True,
            #key_deserializer='org.apache.kafka.common.serialization.ByteBufferDeserializer', se putea
            with_metadata=False) #.with_output_types(NullableCoder)
            #| "UTF-8 bytes to string" >> beam.Map(lambda msg: msg[1].decode("utf-8"))
            | "Parse JSON messages 1" >> beam.Map(parse_json_message )
            #| "Parse JSON messages 7" >> beam.ParDo( parse_json_message2() ) #.with_input_types( beam.Row ) #beam.coders.RowCoder )
            #| "Fixed-size windows"
            #>> beam.WindowInto(window.FixedWindows(window_interval_sec, 0))
            #| "Add URL keys" >> beam.WithKeys(lambda msg: msg["url"])
            #| "Group by URLs" >> beam.GroupByKey()
            #| "Get statistics"
            #>> beam.MapTuple(
            #    lambda url, messages: {
            #        "url": url,
            #        "num_reviews": len(messages),
            #        "score": sum(msg["score"] for msg in messages) / len(messages),
            #        "first_date": min(msg["processing_time"] for msg in messages),
            #        "last_date": max(msg["processing_time"] for msg in messages),
            #    }
            #)
        )

        # Output the results into BigQuery table.
        _ = messages | "Write to Big Query" >> beam.io.WriteToBigQuery(
            output_table, schema=SCHEMA
        )

        p2= (pipeline | "Read from KAFKA d2" >> ReadFromKafka(
            consumer_config={'bootstrap.servers': "35.193.114.205:9092",},
            topics=["quickstart-events2"],
            #start_read_time=0,
            #commit_offset_in_finalize=True,
            #key_deserializer='org.apache.kafka.common.serialization.ByteBufferDeserializer', se putea
            with_metadata=False)
        | "Parse JSON messages d2" >> beam.Map(parse_json_message ))

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
        help="Input PubSub subscription of the form "
        '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."',
    )
    parser.add_argument(
        "--window_interval_sec",
        default=60,
        type=int,
        help="Window interval in seconds for grouping incoming messages.",
    )
    args, beam_args = parser.parse_known_args()

    run(
        input_subscription=args.input_subscription,
        output_table=args.output_table,
        window_interval_sec=args.window_interval_sec,
        beam_args=beam_args,
    )