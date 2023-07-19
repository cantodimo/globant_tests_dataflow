from __future__ import print_function
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam
import argparse
import logging
import sys
sys.path.append('/itd-saptm-apachebeam')
sys.path.append('/itd-saptm-apachebeam/data_quality')
from data_quality.test_dq import test_dq
import time


class Message():
    record_id: str
    load_date_time: str
    stg_schema_name: str
    stg_table_name: str
    dq_name1: str
    dq_name2: str
    dq_street_line_1: str
    dq_street_line_2: str
    dq_street_line_3: str
    dq_city: str
    dq_region: str
    dq_postal_code: str
    dq_country: str
    cleansed_address: str
    

def logging_dq(element):
    logging.warning(test_dq.test_method(element[1].decode('UTF-8')))

def run(
    bootstrap_servers,
    group_id,
    topics,
    sasl_mechanism,
    security_protocol,
    username,
    password
    ) -> None:

    options = PipelineOptions(save_main_session=True, streaming=True) #, runner=beam.runners.DirectRunner() )

    with beam.Pipeline(options=options) as p:

        consumer_config = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
        }
        if sasl_mechanism is not None:
            consumer_config["sasl.mechanism"]= sasl_mechanism

        if sasl_mechanism is not None > 0:
            consumer_config["security.protocol"]= security_protocol
            
        if username is not None > 0 and password is not None> 0:
            credentials = 'org.apache.kafka.common.security.plain.PlainLoginModule required serviceName="Kafka" username="' + username+'" password="'+password+'";'
            consumer_config["sasl.jaas.config"]= credentials
            
        topics = topics.split(",")
        
        consumer_config_hardcoded = {
            "bootstrap.servers": "pkc-lgk0v.us-west1.gcp.confluent.cloud:9092",
            "group.id": "k_itd_ren_saptm_poc_dev_svc",
            "sasl.mechanism": "PLAIN",
            "security.protocol": "SASL_SSL",
            "sasl.jaas.config": 'org.apache.kafka.common.security.plain.PlainLoginModule required serviceName="Kafka" username="FQLECRTXM6NS7EHY" password="LJ+HrWWdLIuDSZH0TkLF6mXBbSUDL5RfaMR4GNMJNfx1gRtdjWb5vZEim8gjUiLk";'
        }

        stream_data = (p
                       | "Reading messages from Kafka" >> ReadFromKafka(
                    consumer_config=consumer_config_hardcoded,
                    topics=topics,
                    with_metadata=False,
                    max_num_records=2
                )

                       )
        process_data = (stream_data | "Display messages" >> beam.Map(logging_dq) )






if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    #todo: complete help description

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--bootstrap_servers",
        help="Example: localhost:9200",
        type= str
    )

    parser.add_argument(
        "--group_id",
        help="Example: group1",
        type=str
    )

    parser.add_argument(
        "--topics",
        help="Example: topic1,topic2",
        type=str
    )

    parser.add_argument(
        "--sasl_mechanism",
        help="Example: PLAIN",
        type=str,
        default= None
    )

    parser.add_argument(
        "--security_protocol",
        help="Example: SASL_SSL",
        type=str,
        default= None
    )

    parser.add_argument(
        "--username",
        help="Example: user1",
        type=str,
        default= None
    )

    parser.add_argument(
        "--password",
        help="Example: password1",
        type=str,
        default= None
    )


    args, beam_args = parser.parse_known_args()

    run(
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id,
        topics=args.topics,
        sasl_mechanism=args.sasl_mechanism,
        security_protocol=args.security_protocol,
        username=args.username,
        password=args.password
    )