# For Kafkaconsumer conceot to listen data from 'producer_a.py'
from kafka import KafkaConsumer
# Output is stored in json files
import json
# Cluster name
topic_name = 'top'
# Starting the kafka server at the consumer
consumer = KafkaConsumer(
    topic_name,
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='latest',
     enable_auto_commit=True,
     auto_commit_interval_ms=5000,
     fetch_max_bytes=128,
     max_poll_records=100,
     value_deserializer=lambda x: json.loads(x.decode('utf-8')))
# Prints the streamed and seggragated data
for message in consumer:
    tweet_got = json.loads(json.dumps(message.value))
    print(tweet_got )