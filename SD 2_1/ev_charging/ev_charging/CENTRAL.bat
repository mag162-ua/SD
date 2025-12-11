TITLE CENTRAL

docker compose up -d central

docker exec -it ev_charging-central-1 python EV_Central.py 5000 kafka:9092
CMD /k
