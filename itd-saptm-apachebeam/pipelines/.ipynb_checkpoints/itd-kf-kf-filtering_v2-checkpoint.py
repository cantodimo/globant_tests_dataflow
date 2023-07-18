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

## add commnet to test push

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


def compare_messages(kafka_message, columns_to_compare):
    new_data_dict= kafka_message["message"]["data"]
    old_data_dict= kafka_message["message"]["beforeData"]
    new_data= [ new_data_dict[x] for x in columns_to_compare ]
    old_data= [ old_data_dict[x] for x in columns_to_compare ]
    if new_data == old_data:
        yield kafka_message

    
def logging_dq(element):
    logging.warning(test_dq.test_method(element[1].decode('UTF-8')))

def run(
    bootstrap_servers,
    group_id,
    topics,
    with_metadata,
    sasl_mechanism,
    security_protocol,
    username,
    password,
    columns_to_compare
    ) -> None:

    options = PipelineOptions(save_main_session=True, streaming=True)

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
        columns = columns_to_compare.split(",")
        
        for i in range(len(topics)):
            topic= topics[i]
            columns_to_compare_this_topic= columns[i]
            _ = (
                p| "Reading messages from Kafka topic: " + topic >> ReadFromKafka(
                    consumer_config=consumer_config,
                    topics=[topic],
                    with_metadata= with_metadata == 1
                )
                | "decode messages" >> beam.Map(lambda x: x.decode("utf-8"))
                | "comparing columns" >> beam.FlatMap(compare_messages, columns_to_compare_this_topic)
                | "Display messages" >> beam.Map(logging_dq)
            )






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
        "--with_metadata",
        help="Example: 1 for true 0 for false",
        type=int,
        default= 0
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
    
    parser.add_argument(
        "--columns_to_compare",
        help="""Example: 
        topic1_column1|topic1_column2,topic2_column1|topic2_column2""",
        type=str,
        default= None
    )    


    args, beam_args = parser.parse_known_args()

    run(
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id,
        topics=args.topics,
        with_metadata=args.with_metadata,
        sasl_mechanism=args.sasl_mechanism,
        security_protocol=args.security_protocol,
        username=args.username,
        password=args.password,
        columns_to_compare=args.columns_to_compare
    )
