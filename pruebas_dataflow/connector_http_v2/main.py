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

from apache_beam.pvalue import AsList

import json
from datetime import datetime



from utils import http_connector


# Defines the BigQuery schema for the output table.
SCHEMA = ",".join(
    [   
        "id_request:STRING",
        "batch_size_sended:INTEGER",
        "position:INTEGER",
        "date_job:STRING",
        "datetime_get_page:STRING"
    ]
)

def test_format_function(element):
    logging.warning( "element on format_function: " + str(element) )
    if type(element) == list: ## has been grouped on a batch
        return json.dumps({"prob_continuar": element[0]["prob_continuar"], "element":element})
    else:
        return json.dumps({"prob_continuar": element["prob_continuar"], "element":element})


def format_output(out_http, date_job, batch_size):
    logging.warning( "out_http: " + str(out_http) )
    if batch_size == 1:
        batch_size_sended= 1 
        id_request= str(out_http[1]["id_request"])
    else:
        batch_size_sended= len( out_http[1] )
        id_request= ",".join( [ str(x["id_request"]) for x in out_http[1] ] )
        
    return {
        "id_request": id_request,
        "batch_size_sended": batch_size_sended,
        "position":0,
#        "prob_to_continue": int(out_http[1]["body"]["prob_continuar"]),
#        "position": int(1),
        
        "date_job": str([d for d in date_job]),
        "datetime_get_page": str(out_http[0]["datetime_get_page"])
    } 
    


def run(
    input_subscription: str,
    output_table: str,
    window_interval_sec: int,
    url: str,
    method: str,
    batch: str,
    beam_args: list[str] = None,
) -> None:
    
    ## latter I keep trying to pass a json on input parameters
    headers = {
        'Content-Type': 'application/json'
    }
    
    """Build and run the pipeline."""
    options = PipelineOptions(beam_args, save_main_session=True, streaming=True, )

    with beam.Pipeline(options=options) as pipeline:
        date_job_pcoll= ( 
            pipeline 
            | beam.Create([0]) 
            | beam.Map( lambda x: datetime.now().strftime("%Y-%m-%d_%H:%M:%S") )        
        )

        messages = (
            pipeline
            | "Read from pubsub"
            >> beam.io.ReadFromPubSub(
                subscription=input_subscription
            ).with_output_types(bytes)
            | "UTF-8 bytes to json" >> beam.Map(lambda msg: json.loads(msg.decode("utf-8")))
            #| "formtat input request" >> beam.Map( lambda x: { "body": x})
            #| "test pardo" >> beam.ParDo(test_splittable_pardo(num_pages_max= 10000), AsList(date_job_pcoll))
            | "http_connector" >> http_connector.HttpConnector(
                url=url,
                headers= headers,
                method=method,
                batch=batch,
                format_function=test_format_function
            )
            | "format output" >> beam.Map(format_output, AsList(date_job_pcoll), batch)
            | "Fixed-size windows"
            >> beam.WindowInto(window.FixedWindows(window_interval_sec, 0))
        )

        # Output the results into BigQuery table.
        _ = messages | "Write to Big Query" >> beam.io.WriteToBigQuery(
            output_table, schema=SCHEMA, batch_size=100
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
        help="Input PubSub subscription of the form "
        '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."',
    )
    parser.add_argument(
        "--window_interval_sec",
        default=10,
        type=int,
        help="Window interval in seconds for grouping incoming messages.",
    )
    parser.add_argument(
        "--url",
        type=str,
        help="url to make http calls",
    )
    #parser.add_argument(
    #    "--headers",
    #    type=json.loads,
    #    help="json string with headers",
    #)
    parser.add_argument(
        "--method",
        type=str,
        help="get, post",
    )
    parser.add_argument(
        "--batch",
        type=int,
        default= 1,
        help="size of the batch of records to process on the http call, must be >=1",
    )

    args, beam_args = parser.parse_known_args()

    run(
        input_subscription=args.input_subscription,
        output_table=args.output_table,
        window_interval_sec=args.window_interval_sec,
        url=args.url,
        method=args.method,
        batch=args.batch,
        beam_args=beam_args,
    )