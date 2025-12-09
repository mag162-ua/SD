# EV_API_Central.py
import os
import json
import logging
from flask import Flask, request, jsonify
from kafka import KafkaProducer
from datetime import datetime

app = Flask(__name__)

# Configuración
DATA_FILE = os.getenv('DATA_FILE', '/app/ev_central_data.json')
KAFKA_SERVER = os.getenv('KAFKA_SERVERS', 'kafka:9092')
TOPIC_COMMANDS = 'control_commands'

# Configurar Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('API_Central')

# Kafka Producer
producer = None
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    logger.info(f"✅ API conectada a Kafka en {KAFKA_SERVER}")
except Exception as e:
    logger.error(f"❌ Error conectando API a Kafka: {e}")

def load_data():
    """Lee el JSON compartido con la Central"""
    try:
        if os.path.exists(DATA_FILE):
            with open(DATA_FILE, 'r') as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"Error leyendo DB: {e}")
    return {"charging_points": [], "drivers": [], "transactions": []}

# --- ENDPOINTS ---

@app.route('/api/weather/alert', methods=['POST'])
def receive_weather_alert():
    data = request.get_json()
    cp_id = data.get('cp_id')
    temp = data.get('temperature')
    city = data.get('city')
    
    if not cp_id:
        return jsonify({"error": "Falta cp_id"}), 400

    logger.warning(f"❄️ ALERTA CLIMÁTICA para {cp_id} en {city}: {temp}ºC")

    # Enviar comando a la Central vía Kafka
    message = {
        'cp_id': cp_id,
        'command': 'STOP',
        'reason': f'ALERTA_CLIMA: {temp}ºC en {city}',
        'source': 'api_weather',
        'timestamp': datetime.now().isoformat()
    }
    
    if producer:
        producer.send(TOPIC_COMMANDS, message)
        producer.flush()
        return jsonify({"status": "Alerta enviada a Central"}), 200
    else:
        return jsonify({"error": "Kafka no disponible"}), 500

@app.route('/api/weather/clear', methods=['POST'])
def receive_weather_clear():
    data = request.get_json()
    cp_id = data.get('cp_id')
    
    # Enviar comando RESUME a la Central vía Kafka
    # Nota: Tu central debe tener lógica para manejar un comando 'RESUME' o 'START' especial
    message = {
        'cp_id': cp_id,
        'command': 'RESUME', 
        'reason': 'CLIMA_RESTABLECIDO',
        'source': 'api_weather',
        'timestamp': datetime.now().isoformat()
    }
    
    if producer:
        producer.send(TOPIC_COMMANDS, message)
        producer.flush()
        return jsonify({"status": "Restablecimiento enviado a Central"}), 200
    return jsonify({"error": "Kafka no disponible"}), 500

@app.route('/api/status', methods=['GET'])
def get_status():
    """Endpoint para el Front: Lee el estado actual"""
    data = load_data()
    return jsonify(data), 200

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "API Online"}), 200

if __name__ == '__main__':
    port = int(os.getenv('API_PORT', 8090))
    print(f"🚀 API_Central escuchando en puerto {port}")
    app.run(host='0.0.0.0', port=port)