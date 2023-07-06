from kafka import KafkaConsumer, KafkaProducer, TopicPartition  
#topic= "test-kafka-resume-job"
topic= "test-kafka-resume-job-multiple-partitions" #10 partitions
client = ["35.193.114.205:9092"]

if True:
  tp = TopicPartition(topic ,2)
  consumer = KafkaConsumer(bootstrap_servers=client, api_version=(0,11,5))
  consumer.assign([tp])
  consumer.seek_to_beginning(tp)
  # obtain the last offset value
  lastOffset = consumer.end_offsets([tp])[tp]
  # consume the messages
  for message in consumer:
    print( "Offset:", message.offset)
    print( "Value:", message.value)
    if message.offset == lastOffset - 1:
      break