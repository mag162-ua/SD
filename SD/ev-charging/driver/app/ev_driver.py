from kafka import KafkaConsumer

consumer = KafkaConsumer("ev_charging_events", bootstrap_servers="kafka:9092", auto_offset_reset="earliest")
print("Driver escuchando...")

for msg in consumer:
    print("Driver recibió:", msg.value.decode())
