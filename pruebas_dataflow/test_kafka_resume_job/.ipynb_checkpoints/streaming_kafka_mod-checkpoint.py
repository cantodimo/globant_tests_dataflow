from __future__ import annotations

import argparse
import json
import logging
import time
from typing import Any

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.transforms.window as window
from apache_beam.io.kafka import ReadFromKafka, WriteToKafka
from apache_beam.pvalue import AsList
from apache_beam.transforms.trigger import AfterCount, Repeatedly

from apache_beam.coders.coders import NullableCoder

import requests
import time
from datetime import datetime

import typing
import random


# Defines the BigQuery schema for the output table.
SCHEMA = ",".join(
    [
        "message:STRING",
        "aditional_data:STRING",
        "date:TIMESTAMP",
        "date_job:STRING"
    ]
)

def call_api( body, delay_time, messages_per_delay, messages_per_fail):
    
    ### INSTEAD HAVE TO CREATE A API, JUST SIMULATE THE BEHAVIOR HERE
    #url = "https://us-central1-rosy-zoo-390619.cloudfunctions.net/api_rest_test_kafka"
    #payload = json.dumps(body)
    #headers = {
    #    'Content-Type': 'application/json'
    #}

    #response = requests.request("POST", url, headers=headers, data=payload)
    #return json.loads(response.text)
    
    out= {}
    
    if int(body["message"])%messages_per_delay ==0:
        out["time_to_sleep"]= delay_time
        logging.warning( " adding sleep message received: " + str(body["message"]) )
    else:
        out["time_to_sleep"]=0
        
    if int(body["message"])%messages_per_fail ==0:
        out["status"]= False
        logging.warning( " making fail message received: " + str(body["message"]) )
    else:
        out["status"]= True   
        
    return out
    
    

def parse_json_message(message, input_parameters, date_job, delay_time, messages_per_delay, messages_per_fail):
    """Parse the input json message and add 'score' & 'processing_time' keys."""

    try:
        v= int(message[1].decode('UTF-8'))
        
    except:
        v= "no pudo leer el value" 
        
    try:
        k= str(message[0])
        
    except:
        k= "no pudo leer el key" 

    logging.warning( "message received: " + str(v) )

    #time.sleep(delay_time) 
    
    r= call_api({"message": v}, delay_time, messages_per_delay, messages_per_fail)
    
    time.sleep(r["time_to_sleep"])
    
    logging.warning( "message received after sleep: " + str(v) + " -- time sleeped: " + str(r["time_to_sleep"]) )

    ## when we want to test a failure, we can set it to true when updating the job
    ## put it as a template parameter... 
    if input_parameters["allow_fail"]:  
        if r["status"]:
          return [{
            "message": str(v),
            "aditional_data": str(k),
            "date": int(time.time()),
            "date_job": str([d for d in date_job])
          }]
        else:
          raise ValueError('receive message with pair number: ' + str(v))

    else:
        return [{
            "message": str(v), #row["url"],
            "aditional_data": str(k),
            "date": int(time.time()),
            "date_job": str([d for d in date_job])
          }]
    
def run(
    input_topic: str,
    output_topic:str,
    group_id: str,
    start_read_time: int,
    commit_offset_in_finalize: int,
    with_metadata: int,
    bootstrap_servers: str,
    allow_fail:int,
    delay_time,
    messages_per_delay,
    messages_per_fail,
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
        #date_job_pcoll= ( 
        #    pipeline 
        #    | beam.Create([0]) 
        #    | beam.Map( lambda x: datetime.now().strftime("%Y-%m-%d_%H:%M:%S") )        
        #)
        
        messages = (
            pipeline
            | "Read from KAFKA"
            >> ReadFromKafka(
            consumer_config= consumer_config,
            topics=[input_topic],
            start_read_time=start_read_time,
            commit_offset_in_finalize=commit_offset_in_finalize
            #key_deserializer='org.apache.kafka.common.serialization.ByteBufferDeserializer', se putea
            #with_metadata=with_metadata
            ) #.with_output_types(NullableCoder)
            
            | beam.WindowInto(
                #beam.window.FixedWindows(1),
                beam.window.GlobalWindows(),
                trigger=Repeatedly( AfterCount(1) ),
                accumulation_mode=beam.trigger.AccumulationMode.DISCARDING
                #allowed_lateness=2*24*60*60 creo que esto lo bloquea, no se escribe nada 
            )
            | "Parse JSON messages 1" >> beam.FlatMap(
                parse_json_message,
                input_parameters,
                #AsList(date_job_pcoll),
                ["date x"],
                delay_time,
                messages_per_delay,
                messages_per_fail
                
            )
            | "encode utf-8" >> beam.Map( 
                lambda x: ( b'unique_key', json.dumps(x).encode("utf-8") )
            ).with_output_types(typing.Tuple[bytes, bytes])
                
        )

        producer_config={'bootstrap.servers': bootstrap_servers}
        if group_id is not None:
            producer_config["group.id"]= group_id
            
        _ = (
            messages | "Write to kafka" >> WriteToKafka(
                producer_config= producer_config,
                topic= output_topic
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.WARNING) # probar si con esto se puede disminuir los logs del readfromkafka

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output_table",
        help="Output BigQuery table for results specified as: "
        "PROJECT:DATASET.TABLE or DATASET.TABLE.",
    )
    parser.add_argument(
        "--input_topic",
        help="Input Topic",
        type=str
    )
    parser.add_argument(
        "--output_topic",
        help="output_topic",
        type=str
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
    parser.add_argument(
        "--delay_time",
        type=int,
        default=0,
        help="seconds time sleep on map",
    )
    parser.add_argument(
        "--messages_per_delay",
        type=int,
        default=100000,
        help=" message%messages_per_delay ==0 is added delay_time",
    )
    parser.add_argument(
        "--messages_per_fail",
        type=int,
        default=100000,
        help=" message%messages_per_fail ==0 is added a fail status",
    )
    
    args, beam_args = parser.parse_known_args()

    run(
        input_topic=args.input_topic,
        output_topic=args.output_topic,
        group_id=args.group_id,
        start_read_time= args.start_read_time,
        commit_offset_in_finalize= args.commit_offset_in_finalize,
        with_metadata= args.with_metadata,
        bootstrap_servers= args.bootstrap_servers,
        allow_fail= args.allow_fail,
        delay_time= args.delay_time,
        messages_per_delay= args.messages_per_delay,
        messages_per_fail= args.messages_per_fail,
        beam_args=beam_args
    )
    
# revisar porque sale este log: The configuration 'group.id' was supplied but isn't a known config.