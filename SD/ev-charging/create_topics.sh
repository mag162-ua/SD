#!/bin/bash

# Esperar a que Kafka esté listo
echo "Esperando a que Kafka esté listo..."
sleep 30

# Configuración
KAFKA_HOST="localhost:29092"
KAFKA_CONTAINER="kafka:9092"

# Función para crear tópicos
create_topic() {
    local topic_name=$1
    local partitions=$2
    local replication_factor=$3
    
    echo "Creando tópico: $topic_name"
    docker exec ev-charging-kafka-1 kafka-topics --create \
        --topic "$topic_name" \
        --partitions "$partitions" \
        --replication-factor "$replication_factor" \
        --bootstrap-server "$KAFKA_CONTAINER"
}

# Crear tópicos para el sistema EV Charging
echo "=== Creando tópicos para EV Charging System ==="

# Tópico principal para eventos de carga
create_topic "ev_charging_events" 3 1

# Tópico para comandos de control
create_topic "ev_charging_commands" 3 1

# Tópico para estado de los cargadores
create_topic "ev_charging_status" 3 1

# Tópico para transacciones de pago
create_topic "ev_payment_transactions" 3 1

# Tópico para métricas y monitoreo
create_topic "ev_charging_metrics" 3 1

# Tópico para alertas del sistema
create_topic "ev_charging_alerts" 3 1

# Listar todos los tópicos creados
echo "=== Listando todos los tópicos ==="
docker exec ev-charging-kafka-1 kafka-topics --list --bootstrap-server "$KAFKA_CONTAINER"

echo "=== Tópicos creados exitosamente ==="