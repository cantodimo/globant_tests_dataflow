from kafka import KafkaConsumer, KafkaProducer, TopicPartition  
import json
#topic= "test-kafka-resume-job"
#topic= "test-kafka-resume-job-multiple-partitions" #10 partitions
topic= "test-kafka-output-dataflow"
#topic= "test-kafka-resume-job-3-partitions"
client = ["35.193.114.205:9092"]

dic_messages={}


if True:
  tp = TopicPartition(topic ,0)
  consumer = KafkaConsumer(bootstrap_servers=client, api_version=(0,11,5))
  consumer.assign([tp])
  consumer.seek_to_beginning(tp)
  # obtain the last offset value
  lastOffset = consumer.end_offsets([tp])[tp]
  # consume the messages
  for message in consumer:
    print( "Offset:", message.offset)
    print( "Value:", message.value)
    
    value= json.loads(message.value.decode("utf-8"))["message"]
    if int(value) not in list(dic_messages.keys()):
        dic_messages[ int(value) ]= 1
    else:
        dic_messages[ int(value) ]= dic_messages[ int(value) ] + 1
        
    if message.offset == lastOffset - 1:
        break
    
with open("salida_consumer.txt", "w") as f:
    for k in sorted( list(dic_messages.keys()) ):
        f.write("value: " + str(k) + " count: " + str(dic_messages[k]) + "\n")