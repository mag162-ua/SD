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
CITY_TEMPERATURES = {}
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
        
        cps_list = [dict(cp) for cp in cps]
        
        for cp in cps_list:
            cp_loc = cp['location']
            cp['temperature'] = None # Por defecto
            
            # Buscar si alguna ciudad del clima coincide con la ubicación del CP
            # Ej: Si CP es "Tokyo" y Clima es "Tokyo,JP", hacemos match
            for weather_city, temp in CITY_TEMPERATURES.items():
                if cp_loc in weather_city or weather_city in cp_loc:
                    cp['temperature'] = temp
                    break

        # Estructura de datos para el Frontend
        data = {
            "charging_points": cps_list,
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

def get_cps_by_location(city_partial_name):
    """Busca CPs cuya ubicación contenga el nombre de la ciudad"""
    conn = get_db_connection()
    if not conn: return []
    
    try:
        cur = conn.cursor(cursor_factory=extras.RealDictCursor)
        # Búsqueda insensible a mayúsculas y parcial (ILIKE '%City%')
        query = "SELECT cp_id FROM charging_points WHERE location ILIKE %s"
        search_pattern = f"%{city_partial_name}%"
        
        cur.execute(query, (search_pattern,))
        results = cur.fetchall()
        conn.close()
        return [row['cp_id'] for row in results]
    except Exception as e:
        logger.error(f"Error buscando CPs por ciudad: {e}")
        if conn: conn.close()
        return []

@app.route('/api/weather/update', methods=['POST'])
def receive_weather_update():
    """Recibe actualizaciones periódicas de temperatura"""
    data = request.get_json()
    city = data.get('city')
    temp = data.get('temperature')
    
    if city is not None and temp is not None:
        # Guardamos en memoria: "Madrid,ES" -> 25.5
        CITY_TEMPERATURES[city] = temp
        return jsonify({"status": "updated"}), 200
    return jsonify({"error": "datos incompletos"}), 400

@app.route('/api/weather/alert', methods=['POST'])
def receive_weather_alert():
    data = request.get_json()
    city = data.get('city')
    temp = data.get('temperature')
    
    if not city:
        return jsonify({"error": "Falta parámetro city"}), 400
        
    logger.warning(f"❄️ ALERTA CLIMÁTICA RECIBIDA para ciudad: {city} ({temp}ºC)")

    # 1. Buscar en BD qué CPs están en esa ciudad
    affected_cps = get_cps_by_location(city)
    
    if not affected_cps:
        logger.info(f"ℹ️ No hay CPs registrados en {city}. No se requiere acción.")
        return jsonify({"status": "Alerta recibida, sin CPs afectados"}), 200

    logger.info(f"🚨 CPs afectados en {city}: {affected_cps}")
    
    # 2. Notificación Dashboard
    NOTIFICATIONS.append({
        "type": "danger",
        "title": f"❄️ OLA DE FRÍO EN {city.upper()}",
        "message": f"Temperatura: {temp}ºC. Deteniendo {len(affected_cps)} puntos de carga.",
        "time": datetime.now().strftime("%H:%M:%S")
    })

    # 3. Enviar comando FAILURE por Kafka para CADA CP encontrado
    sent_count = 0
    if producer:
        for cp_id in affected_cps:
            message = {
                'cp_id': cp_id,
                'command': 'FAILURE', 
                'reason': f'ALERTA_CLIMA: {temp}ºC en {city}',
                'temperature': temp,
                'source': 'api_weather',
                'timestamp': datetime.now().isoformat()
            }
            try:
                producer.send(TOPIC_COMMANDS, message)
                sent_count += 1
            except Exception as e:
                logger.error(f"Error enviando Kafka para CP {cp_id}: {e}")
        
        producer.flush()
        return jsonify({"status": f"Avería enviada a {sent_count} CPs en {city}"}), 200

    return jsonify({"error": "Kafka no disponible"}), 500

@app.route('/api/weather/clear', methods=['POST'])
def receive_weather_clear():
    data = request.get_json()
    city = data.get('city')
    temp = data.get('temperature')
    
    if not city:
        return jsonify({"error": "Falta parámetro city"}), 400

    logger.info(f"☀️ CLIMA RESTABLECIDO en ciudad: {city}")

    affected_cps = get_cps_by_location(city)
    
    if not affected_cps:
        return jsonify({"status": "Clima OK, sin CPs afectados"}), 200

    NOTIFICATIONS.append({
        "type": "success",
        "title": f"☀️ CLIMA OK EN {city.upper()}",
        "message": f"Restableciendo servicio en {len(affected_cps)} puntos.",
        "time": datetime.now().strftime("%H:%M:%S")
    })

    sent_count = 0
    if producer:
        for cp_id in affected_cps:
            message = {
                'cp_id': cp_id,
                'command': 'RESUME',
                'reason': f'CLIMA_OK: {temp}ºC en {city}',
                'source': 'api_weather',
                'timestamp': datetime.now().isoformat()
            }
            try:
                producer.send(TOPIC_COMMANDS, message)
                sent_count += 1
            except: pass
        
        producer.flush()
        return jsonify({"status": f"Reanudación enviada a {sent_count} CPs"}), 200

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
        'source_ip': request.remote_addr, 
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
        'source_ip': request.remote_addr,
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