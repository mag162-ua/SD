TITLE STOP_DOCKER

docker compose down --remove-orphans -v
docker ps -a

ECHO ===========================================
ECHO Deteniendo servicios de Kafka y Docker Compose...
ECHO ===========================================

pause