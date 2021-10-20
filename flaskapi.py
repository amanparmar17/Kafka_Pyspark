from flask import Flask, request
import random
from kafka import KafkaProducer
import json


TOPIC_NAME = "sampleTopic"
app = Flask(__name__)


def json_ser(data):
    return json.dumps(data).encode("utf-8")


producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"], value_serializer=json_ser
)


@app.route("/makerequest", methods=["POST"])
def makerequest():
    data = json.loads(request.get_json())
    data_producer = data

    # call the producers for both the topics
    producer.send(TOPIC_NAME, data_producer)

    producer.flush()
    return "success"


if __name__ == "__main__":
    app.run(debug=True, port=8000)

