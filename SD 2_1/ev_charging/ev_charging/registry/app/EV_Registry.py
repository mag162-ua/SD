# EV_Registry.py (MODIFICADO PARA USAR POSTGRESQL)

import os
import secrets
from flask import Flask, request, jsonify
# Necesario para la conexión a PostgreSQL
import psycopg2 
from psycopg2 import sql 
from psycopg2 import OperationalError

# -------------------------------------------------------------------------
# CONFIGURACIÓN DE LA BASE DE DATOS
# -------------------------------------------------------------------------

# Lee las variables de entorno para la conexión
DB_HOST = os.environ.get("DB_HOST", "postgres") # Usamos el nombre del servicio Docker
DB_NAME = os.environ.get("DB_NAME", "ev_db")
DB_USER = os.environ.get("DB_USER", "user")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "password")
DB_PORT = os.environ.get("DB_PORT", "5432")

app = Flask(__name__)

# -------------------------------------------------------------------------
# FUNCIONES DE CONEXIÓN Y SETUP DE SQL
# -------------------------------------------------------------------------

def connect_db():
    """Establece y devuelve la conexión a la base de datos."""
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        return conn
    except OperationalError as e:
        print(f"[DB ERROR] No se pudo conectar a la base de datos: {e}")
        return None

def create_table_if_not_exists():
    """Crea la tabla de puntos de carga si no existe."""
    conn = connect_db()
    if not conn:
        return False
        
    try:
        with conn: # Usa 'with' para asegurar el commit o rollback
            with conn.cursor() as cur:
                # Definición de la tabla para los puntos de carga
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS charging_points (
                    cp_id VARCHAR(50) PRIMARY KEY,
                    secret_key VARCHAR(100) NOT NULL,
                    location VARCHAR(100) NOT NULL,
                    price_per_kwh DECIMAL(10, 2) NOT NULL,
                    status VARCHAR(20) NOT NULL DEFAULT 'DESCONECTADO',
                    current_consumption DECIMAL(10, 2) DEFAULT 0.0,
                    current_amount DECIMAL(10, 2) DEFAULT 0.0,
                    driver_id VARCHAR(50),
                    last_heartbeat TIMESTAMP,
                    total_energy_supplied DECIMAL(10, 2) DEFAULT 0.0,
                    total_revenue DECIMAL(10, 2) DEFAULT 0.0,
                    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_supply_message TIMESTAMP,
                    supply_ending BOOLEAN DEFAULT FALSE,
                    supply_ended_time TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """)
        print("[DB] Tabla 'charging_points' verificada/creada con éxito.")
        return True
    except Exception as e:
        print(f"[DB ERROR] Fallo al crear la tabla: {e}")
        return False


# -------------------------------------------------------------------------
# FUNCIONES DE PERSISTENCIA (Sustituyen a load_db y save_db)
# -------------------------------------------------------------------------
#from psycopg2 import sql
# ... otras importaciones ...

from psycopg2 import sql, extras
# Asegúrate de importar extras si usas fetchone() con diccionario

def register_cp_in_db(cp_id, secret_key, location, price_per_kwh):
    """
    Versión compatible con todas las versiones de PostgreSQL.
    """
    conn = connect_db()
    if not conn:
        print("[DB ERROR] No hay conexión a la base de datos")
        return None

    try:
        with conn:
            with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
                # Primero verificar si existe
                cur.execute("SELECT cp_id FROM charging_points WHERE cp_id = %s", (cp_id,))
                exists = cur.fetchone() is not None
                
                if exists:
                    # UPDATE si existe
                    update_query = """
                        UPDATE charging_points 
                        SET secret_key = %s, 
                            updated_at = CURRENT_TIMESTAMP
                        WHERE cp_id = %s
                        RETURNING *
                    """
                    cur.execute(update_query, (secret_key, cp_id))
                else:
                    # INSERT si no existe
                    insert_query = """
                        INSERT INTO charging_points 
                        (cp_id, secret_key, location, price_per_kwh)
                        VALUES (%s, %s, %s, %s)
                        RETURNING *
                    """
                    cur.execute(insert_query, (cp_id, secret_key, location, price_per_kwh))
                
                result = cur.fetchone()
                return result
                
    except Exception as e:
        print(f"[DB ERROR] Fallo al registrar CP {cp_id}: {e}")
        import traceback
        traceback.print_exc()
        return None
        
# Nota: Si Central necesita obtener la lista completa, implementaría un SELECT *
# Aquí solo se necesita la función de registro para el endpoint /register

# -------------------------------------------------------------------------
# ENDPOINTS DE LA API (Flask)
# -------------------------------------------------------------------------

@app.route('/register', methods=['POST'])
def register_cp():
    cp_data = request.get_json()
    cp_id = cp_data.get('id')
    location = cp_data.get('location', 'Descaonocida')
    price_per_kwh = cp_data.get('price_per_kwh', 0.20)

    if not cp_id :
        return jsonify({"error": "Missing 'id'"}), 400

    secret_key = secrets.token_hex(16)
    
    cp_data_from_db = register_cp_in_db(cp_id, secret_key, location, price_per_kwh)

    if cp_data_from_db:
        # Devuelve TODOS los datos de la fila (incluida la clave secreta)
        return jsonify(cp_data_from_db), 201
    else:
        # Esto ocurre si register_cp_in_db devolvió None
        return jsonify({"error": "DB registration failed"}), 500


@app.route('/health', methods=['GET'])
def health_check():
    """Endpoint de salud y conexión a la BD."""
    if create_table_if_not_exists():
        return jsonify({"status": "Registry OK", "db_status": "Connected"}), 200
    return jsonify({"status": "Registry OK", "db_status": "DOWN"}), 503


if __name__ == '__main__':
    # Intentar asegurar que la tabla existe antes de iniciar el servidor
    create_table_if_not_exists()
    
    # Asume que el Registry necesita certificados (SSL adhoc)
    app.run(host='0.0.0.0', port=8080, ssl_context='adhoc')