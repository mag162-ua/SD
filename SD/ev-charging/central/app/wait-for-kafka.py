#!/usr/bin/env python3
import sys
from kafka import KafkaProducer
import time

def wait_for_kafka(bootstrap_servers, max_attempts=30):
    for attempt in range(max_attempts):
        try:
            producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
            producer.close()
            print("Kafka is ready!")
            return True
        except Exception as e:
            print(f"Attempt {attempt + 1}/{max_attempts}: Kafka not ready - {e}")
            time.sleep(2)
    return False

if __name__ == "__main__":
    if not wait_for_kafka("kafka:9092"):
        sys.exit(1)