from kafka import KafkaProducer
import time

producer = KafkaProducer(bootstrap_servers="kafka:9092")

while True:
    producer.send("ev_charging_events", b"Mensaje de engine")
    print("Central -> Mensaje enviado")
    time.sleep(5)
