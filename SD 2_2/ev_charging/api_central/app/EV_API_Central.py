# EV_API_Central.py
import os
import json
import time
import logging
from datetime import datetime, date
from decimal import Decimal
from flask import Flask, request, jsonify, render_template, Response, stream_with_context
from kafka import KafkaProducer
import psycopg2
from psycopg2 import extras

app = Flask(__name__)

# --- CONFIGURACIÓN ---
# Variables de entorno inyectadas por Docker
KAFKA_SERVER = os.getenv('KAFKA_SERVERS', 'kafka:9092')
TOPIC_COMMANDS = 'control_commands'
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_NAME = os.getenv("DB_NAME", "ev_db")
DB_USER = os.getenv("DB_USER", "user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('API_Central')

NOTIFICATIONS = []
# --- KAFKA PRODUCER ---
producer = None
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    logger.info(f"✅ API conectada a Kafka en {KAFKA_SERVER}")
except Exception as e:
    logger.error(f"❌ Error conectando API a Kafka: {e}")

# --- HELPER DATABASE (Solo lectura para el Dashboard) ---
def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except Exception as e:
        logger.error(f"Error conectando a DB: {e}")
        return None

def json_serializable(obj):
    """Convierte objetos de DB (Fechas, Decimales) a formato JSON compatible"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Type {type(obj)} not serializable")

def get_full_status():
    """Consulta PostgreSQL para obtener CPs y Transacciones"""
    conn = get_db_connection()
    if not conn:
        return {"charging_points": [], "transactions": [], "error": "DB Down"}
    
    try:
        cur = conn.cursor(cursor_factory=extras.RealDictCursor)
        
        # 1. Leer CPs
        cur.execute("SELECT * FROM charging_points ORDER BY cp_id ASC")
        cps = cur.fetchall()
        
        cur.execute("SELECT * FROM drivers ORDER BY registration_date DESC")
        drivers = cur.fetchall()

        # 2. Leer últimas Transacciones
        cur.execute("SELECT * FROM transactions ORDER BY created_at DESC LIMIT 10")
        txs = cur.fetchall()
        
        cur.execute("SELECT * FROM audit_logs ORDER BY timestamp DESC LIMIT 50")
        audit_logs = cur.fetchall()

        cur.execute("""
            SELECT 
                COALESCE(SUM(energy_consumed), 0) as hist_energy, 
                COALESCE(SUM(amount), 0) as hist_revenue 
            FROM transactions
        """)
        history = cur.fetchone()
        
        # B) Sumar lo que se está consumiendo AHORA MISMO (sesiones abiertas)
        #    que aún no está en transacciones.
        cur.execute("""
            SELECT 
                COALESCE(SUM(current_consumption), 0) as active_energy, 
                COALESCE(SUM(current_amount), 0) as active_revenue 
            FROM charging_points 
            WHERE status = 'SUMINISTRANDO'
        """)
        active = cur.fetchone()
        
        # C) Totales Generales = Historia + Activo
        total_kwh_global = float(history['hist_energy']) + float(active['active_energy'])
        total_eur_global = float(history['hist_revenue']) + float(active['active_revenue'])

        conn.close()
        
        # Estructura de datos para el Frontend
        data = {
            "charging_points": cps,
            "drivers": drivers,
            "transactions": txs,
            "audit_logs": audit_logs,
            "general_stats": {
                "total_energy": total_kwh_global,
                "total_revenue": total_eur_global,
                "active_cps": sum(1 for cp in cps if cp['status'] == 'SUMINISTRANDO'),
                "total_cps": len(cps)
            }
        }
        # Serializar y deserializar para manejar fechas/decimales
        return json.loads(json.dumps(data, default=json_serializable))

    except Exception as e:
        logger.error(f"Error consulta SQL: {e}")
        if conn: conn.close()
        return {"charging_points": [], "transactions": [], "drivers": []}

# --- ENDPOINTS ---

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/status', methods=['GET'])
def get_status():
    data = get_full_status()
    return jsonify(data), 200

# --- WEATHER ENDPOINTS (Llamados por EV_W) ---

@app.route('/api/weather/alert', methods=['POST'])
def receive_weather_alert():
    data = request.get_json()
    cp_id = data.get('cp_id')
    temp = data.get('temperature')
    city = data.get('city')
    
    logger.warning(f"❄️ ALERTA CLIMÁTICA para {cp_id} en {city}: {temp}ºC")

    NOTIFICATIONS.append({
        "type": "danger",
        "title": "❄️ ALERTA DE CLIMA",
        "message": f"Baja temperatura en {city} ({temp}ºC). CP-{cp_id} detenido.",
        "time": datetime.now().strftime("%H:%M:%S")
    })

    # --- MODIFICACIÓN AQUÍ ---
    # Enviamos 'FAILURE' en lugar de 'STOP'.
    # Esto indica a la Central que debe marcar el CP como AVERIADO/BROKEN.
    message = {
        'cp_id': cp_id,
        'command': 'FAILURE', 
        'reason': f'ALERTA_CLIMA: {temp}ºC en {city}',
        'temperature': temp,
        'source': 'api_weather',
        'timestamp': datetime.now().isoformat()
    }
    
    if producer:
        try:
            producer.send(TOPIC_COMMANDS, message)
            producer.flush()
            return jsonify({"status": "Avería reportada a Central"}), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    return jsonify({"error": "Kafka no disponible"}), 500

@app.route('/api/weather/clear', methods=['POST'])
def receive_weather_clear():
    data = request.get_json()
    cp_id = data.get('cp_id')
    temp = data.get('temperature')
    
    logger.info(f"☀️ CLIMA RESTABLECIDO para {cp_id}")

    NOTIFICATIONS.append({
        "type": "success",
        "title": "☀️ CLIMA RESTABLECIDO",
        "message": f"Temperatura segura. CP-{cp_id} operativo.",
        "time": datetime.now().strftime("%H:%M:%S")
    })

    # Enviamos comando RESUME a Central vía Kafka
    # La Central debe interpretar esto para poner el estado en AVAILABLE
    message = {
        'cp_id': cp_id,
        'command': 'RESUME',
        'reason': 'CLIMA_OK',
        'source': 'api_weather',
        'timestamp': datetime.now().isoformat()
    }
    
    if producer:
        try:
            producer.send(TOPIC_COMMANDS, message)
            producer.flush()
            return jsonify({"status": "Sent RESUME command"}), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    return jsonify({"error": "Kafka no disponible"}), 500

# --- SSE STREAM (Datos en tiempo real para Front) ---
@app.route('/api/stream')
def stream():
    def event_stream():
        while True:
            # Consultar DB
            data = get_full_status()

            if NOTIFICATIONS:
                data['notifications'] = list(NOTIFICATIONS) # Copiamos la lista
                NOTIFICATIONS.clear() # Vaciamos el buzón para no repetirlas

            # Formato Server-Sent Events
            yield f"data: {json.dumps(data)}\n\n"
            time.sleep(2) # Actualizar cada 2 segundos

    return Response(stream_with_context(event_stream()), mimetype="text/event-stream")

@app.route('/api/control/stop', methods=['POST'])
def stop_cp_remote():
    data = request.get_json()
    cp_id = data.get('cp_id')
    
    if not cp_id:
        return jsonify({"error": "Falta cp_id"}), 400

    logger.info(f"🛑 Solicitud de PARADA dashboard recibida para CP {cp_id}")

    # Mensaje idéntico al que genera la consola, pero con source='api_dashboard'
    message = {
        'cp_id': cp_id,
        'command': 'STOP',
        'reason': 'MANUAL_DASHBOARD_STOP',
        'source': 'api_dashboard', 
        'timestamp': datetime.now().isoformat()
    }
    
    if producer:
        try:
            producer.send(TOPIC_COMMANDS, message)
            producer.flush()
            return jsonify({"status": "Comando STOP enviado", "cp_id": cp_id}), 200
        except Exception as e:
            logger.error(f"Error Kafka: {e}")
            return jsonify({"error": str(e)}), 500
            
    return jsonify({"error": "Kafka no disponible"}), 500

@app.route('/api/control/resume', methods=['POST'])
def resume_cp_remote():
    data = request.get_json()
    cp_id = data.get('cp_id')
    
    if not cp_id:
        return jsonify({"error": "Falta cp_id"}), 400

    logger.info(f"▶️ Solicitud de REANUDAR dashboard recibida para CP {cp_id}")

    # Enviamos comando RESUME a Kafka
    message = {
        'cp_id': cp_id,
        'command': 'RESUME',
        'reason': 'MANUAL_DASHBOARD_RESUME',
        'source': 'api_dashboard', 
        'timestamp': datetime.now().isoformat()
    }
    
    if producer:
        try:
            producer.send(TOPIC_COMMANDS, message)
            producer.flush()
            return jsonify({"status": "Comando RESUME enviado", "cp_id": cp_id}), 200
        except Exception as e:
            logger.error(f"Error Kafka: {e}")
            return jsonify({"error": str(e)}), 500
            
    return jsonify({"error": "Kafka no disponible"}), 500

if __name__ == '__main__':
    port = int(os.getenv('API_PORT', 8090))
    print(f"🚀 API_Central escuchando en puerto {port}")
    app.run(host='0.0.0.0', port=port)