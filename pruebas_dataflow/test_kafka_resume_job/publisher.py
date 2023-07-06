from kafka import KafkaConsumer, KafkaProducer, TopicPartition  
#topic= "test-kafka-resume-job" #only one partition
topic= "test-kafka-resume-job-multiple-partitions" #10 partitions
client = ["35.193.114.205:9092"]

p= KafkaProducer(bootstrap_servers=client, api_version=(0,11,5))

for i in range(100,200,1):
  p.send(topic, key=str(int(i/10)).encode("utf-8") , value= str(i).encode("utf-8"))
  print(i)

p.flush()