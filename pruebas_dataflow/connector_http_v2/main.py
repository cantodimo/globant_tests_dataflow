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

from apache_beam import RestrictionProvider
from apache_beam.io.restriction_trackers import OffsetRange, OffsetRestrictionTracker
from apache_beam.io.range_trackers import OffsetRangeTracker
from apache_beam.io.iobase import RestrictionTracker
from apache_beam.pvalue import AsList

import requests
import random
from datetime import datetime

import logging



# Defines the BigQuery schema for the output table.
SCHEMA = ",".join(
    [
        "id_request:STRING",
        #"response_api:STRING",
        "prob_to_continue:INTEGER",
        "position:INTEGER",
        "date_job:STRING",
        "datetime_get_page:STRING"
    ]
)


class custom_RestrictionTracker(OffsetRestrictionTracker):
    # creo que toca mirar si el defender reminder es el que modifica el self _chekpointed,
    #  si es asi entonces depronto puedo poner como residual range 0 y_ range todo el intervalo depronto eso lo apaga
    def try_split(self, fraction_of_remainder):
        if not self._checkpointed:
            if self._last_claim_attempt is None:
                cur = self._range.start - 1
            else:
                cur = self._last_claim_attempt
            split_point = cur + 1  
            if split_point <= self._range.stop:
                if fraction_of_remainder == 0:
                    self._checkpointed = True
                self._range, residual_range = self._range.split_at(split_point)
                logging.warning( "inside try_split, fraction_of_remainder: " + str(fraction_of_remainder) + " -- self._checkpointed: " + str(self._checkpointed) + " -- self._last_claim_attempt: " + str(self._last_claim_attempt) + " -- self._range: " + str(self._range) + " -- residual_range: " +  str(residual_range) ) 
                #return self._range, residual_range

            # test to see if only when I call the defender_reminder() this condition is triggered 
            # (to try to stop this process when the api says no more pull)
            return OffsetRange(start=1, stop=self._range.stop), OffsetRange(start=self._range.stop, stop=self._range.stop)

    def is_bounded(self):
        return False

def call_api( data, url, method, headers):
    logging.warning( "data received: " + str(data) + " -- url: " + url + " -- method: " + method + " -- headers: " + str(headers) + " -- type: " + str(type(headers)) )
    ## data is a json containing the body and another info asociated to the request that we want to 
    ## propagate 

    #url = "https://us-central1-rosy-zoo-390619.cloudfunctions.net/api-rest-dummy"
    payload = json.dumps(data["body"])
    #headers = {
    #    'Content-Type': 'application/json'
    #}

    response = requests.request(method, url, headers=headers, data=payload)
    logging.warning( "response.text: " + str(response.text) + " -- type: " + str(type(response.text)) )
    return json.loads(response.text), data

class test_splittable_pardo(beam.DoFn, RestrictionProvider):
    def __init__(self, num_pages_max):
      self.num_pages_max= num_pages_max

    @beam.DoFn.unbounded_per_element()
    def process(self,
                element,
                date_job,
                tracker = beam.DoFn.RestrictionParam(),
                **unused_kwargs):

        prob_to_continue= element["prob_continuar"]
        id_request= element["id_request"]
        restriction = tracker.current_restriction()
        logging.warning( "element received " + str(element) + "-----" + str(restriction.start) + " -- " + str(restriction.stop))        
        for position in range(restriction.start, restriction.stop + 1):
            if tracker.try_claim(position):
                r= call_api({"id_request": id_request, "request_number":int(position), "prob_continuar":prob_to_continue })
                out= {
                    "id_request": str(id_request),
                    #"response_api": str(r),
                    "prob_to_continue": int(prob_to_continue),
                    "position": int(position),
                    "date_job": str([d for d in date_job]),
                    "datetime_get_page": r["datetime_get_page"]
                }
                if r["continue_polling"]:
                    logging.warning( "continue polling, id_request: " + str(id_request) + " --- " + str(position))
                    yield out

                else:
                    logging.warning( "last response api for id_request: " + str(id_request) + " -- num response api: " + str(position))
                    out["position"]= -1000  # flag to check on bigquery if the pardo stops only when the api
                    # said that was the last message
                    tracker.defer_remainder()
                    yield out

                
            else:
                logging.warning( "not claim on element (finish element), id_request: " + str(id_request) ) 
                return

    def create_tracker(self, restriction: OffsetRange) -> RestrictionTracker:
        #out= OffsetRestrictionTracker(restriction)
        out= custom_RestrictionTracker(restriction)
        logging.warning( "inside create tracker, restriction: " + str(restriction) + " -- returned: " + str(out) )
        return out

    def initial_restriction(self, element) -> OffsetRange:
        out= OffsetRange(start=1, stop=self.num_pages_max)
        logging.warning( "inside initial restriction, element: " + str(element) + " -- returned: " + str(out) )
        return out

    def restriction_size(self, element, restriction: OffsetRange):

        ### trying to stop the process of the element sending a size of 0 when this function is
        ### triggered and its current size is less that the initial, IT DOESNT WORK...
        #if restriction.size() < (self.num_pages_max -1):
        #    out= 0
        #else:
        #    out=  restriction.size()

        out=  restriction.size()
        logging.warning( "inside restriction_size, element: " + str(element) + " -- restriction: " + str(restriction) + " -- returned: " + str(out) )
        return out

class HttpConnector(beam.PTransform):
    def __init__(self, url, headers, method, batch):
        if batch < 1:
            raise ValueError('batch size must be >=1')
        self.batch= batch
        self.url= url
        self.method= method
        self.headers= headers

    def expand(self, pcoll):
        if self.batch == 1:
            ### if is not needed to create a batch, no modification is needed on the input
            input_pcoll= pcoll
        else:
            ## crear global window con aftercount= batch, y discard para no llenar la memoria
            input_pcoll= pcoll # mientras tanto

        out_pcoll= input_pcoll | beam.Map(call_api, self.url, self.method, self.headers)

        return out_pcoll

def format_output(out_http, date_job):
    return {
        "id_request": str(out_http[1]["body"]["id_request"]),
        "prob_to_continue": int(out_http[1]["body"]["prob_continuar"]),
        "position": int(1),
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
            | "formtat input request" >> beam.Map( lambda x: { "body": x})
            #| "test pardo" >> beam.ParDo(test_splittable_pardo(num_pages_max= 10000), AsList(date_job_pcoll))
            | "http_connector" >> HttpConnector(url=url, headers= headers, method=method, batch=batch)
            | "format output" >> beam.Map(format_output, AsList(date_job_pcoll))
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