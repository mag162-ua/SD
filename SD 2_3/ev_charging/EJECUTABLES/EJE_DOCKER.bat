TITLE EJE_DOCKER

docker compose up -d zookeeper kafka
docker ps
docker compose run kafka-init
docker compose up -d central driver cp_engine cp_monitor --force-recreate
docker ps

ECHO ===========================================
ECHO Iniciando servicios de Kafka y Docker Compose...
ECHO ===========================================

pause
