import json
import logging
import os
import threading
import time
import uuid
from datetime import datetime, timezone

from flask import Flask, jsonify, request
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable


app = Flask(__name__)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logging.getLogger("kafka").setLevel(logging.WARNING)

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092").split(",")
TOPICS = {
    "movie": "movie-events",
    "user": "user-events",
    "payment": "payment-events",
}

producer = None


def get_producer():
    global producer

    if producer is None:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        )

    return producer


def consume(topic):
    while True:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=KAFKA_BROKERS,
                group_id="cinemaabyss-events-service",
                auto_offset_reset="earliest",
                value_deserializer=lambda value: json.loads(value.decode("utf-8")),
            )

            for message in consumer:
                logging.info(f"Consumed from {topic}: {message.value}")
        except (NoBrokersAvailable, KafkaError):
            logging.warning(f"Kafka is unavailable, retrying consumer for {topic}")
            time.sleep(3)


@app.get("/api/events/health")
def health():
    return jsonify(status=True)


@app.post("/api/events/<event_type>")
def create_event(event_type):
    if event_type not in TOPICS:
        return jsonify(error="Unknown event type"), 404

    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return jsonify(error="Invalid JSON"), 400

    event = {
        "id": str(uuid.uuid4()),
        "type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }

    try:
        metadata = get_producer().send(TOPICS[event_type], event).get(timeout=10)
    except (NoBrokersAvailable, KafkaError):
        return jsonify(error="Kafka is unavailable"), 503

    logging.info(f"Produced to {TOPICS[event_type]}: {event}")

    return jsonify(
        status="success",
        partition=metadata.partition,
        offset=metadata.offset,
        event=event,
    ), 201


def start_consumers():
    for topic in TOPICS.values():
        threading.Thread(target=consume, args=(topic,), daemon=True).start()


if __name__ == "__main__":
    start_consumers()
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8082")),
        threaded=True,
    )
