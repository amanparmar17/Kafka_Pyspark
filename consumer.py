from kafka import KafkaConsumer
import json


TOPIC_NAME = "sinkTopic"
BOOTSTRAP_SERVERS = ["localhost:9092"]


if __name__ == "__main__":
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        group_id="first_consumer",
    )

    print("starting the consumer")
    for message in consumer:
        print(f"MESSAGE : {message.value}", end="\n=====\n")
