TITLE CENTRAL
docker compose build --no-cache 
docker compose push

docker compose up -d zookeeper kafka
docker ps
docker compose run kafka-init
docker compose up -d central
docker exec -it ev-charging-central-1 python ev_central.py 5000 kafka:9092
CMD /k
