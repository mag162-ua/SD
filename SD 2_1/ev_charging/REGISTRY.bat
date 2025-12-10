:: REGISTRY.bat
docker compose up -d registry
TIMEOUT /T 2  # Espera 2 segundos
docker exec -it ev_charging-registry-1 python EV_Registry.py