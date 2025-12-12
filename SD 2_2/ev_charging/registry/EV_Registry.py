import os
import secrets
import requests
from flask import Flask, request, jsonify

# -------------------------------------------------------------------------
# CONFIGURACIÓN Y LECTURA DE VARIABLES DE ENTORNO
# -------------------------------------------------------------------------
app = Flask(__name__)

# CRÍTICO: La URL del servicio remoto que gestiona el archivo JSON.
# Se lee de la variable de entorno establecida en docker-compose.yml.
# Ejemplo de valor esperado (Máquina B): "http://192.168.1.50:8090/api/data"
DB_SERVICE_URL = os.getenv("DB_SERVICE_URL")

if not DB_SERVICE_URL:
    # Mensaje de error si la variable no se configuró correctamente en docker-compose
    print("[FATAL] ERROR: La variable de entorno DB_SERVICE_URL no está configurada.")
    exit(1)

# -------------------------------------------------------------------------
# FUNCIONES AUXILIARES (ACCESO A LA BD REMOTA)
# -------------------------------------------------------------------------

def load_db():
    """
    Lee el contenido completo del JSON de la Base de Datos Remota mediante GET.
    """
    try:
        # Petición GET a la URL configurada
        response = requests.get(DB_SERVICE_URL)
        response.raise_for_status() # Lanza HTTPError para códigos 4xx/5xx
        
        # Devuelve el JSON deserializado (diccionario Python)
        return response.json()
        
    except requests.exceptions.RequestException as e:
        # En caso de fallo de conexión o error HTTP
        print(f"[!] ERROR: Falló la conexión a la BD Remota ({DB_SERVICE_URL}). Detalles: {e}")
        # Retorna una estructura vacía para evitar fallos catastróficos, aunque 
        # la operación de registro probablemente fallará después.
        return {"charging_points": {}}

def save_db(data):
    """
    Guarda (sobrescribe) el diccionario completo en la Base de Datos Remota
    mediante una petición PUT.
    """
    try:
        # Petición PUT para actualizar el recurso remoto con el nuevo JSON
        response = requests.put(DB_SERVICE_URL, json=data)
        response.raise_for_status()
        
        print("[*] Datos guardados remotamente con éxito.")
        return True
        
    except requests.exceptions.RequestException as e:
        print(f"[!] ERROR: No se pudo guardar la BD en el servicio remoto: {e}")
        return False

# -------------------------------------------------------------------------
# ENDPOINTS DEL API REST DEL REGISTRY
# -------------------------------------------------------------------------

@app.route('/register', methods=['POST'])
def register_cp():
    """
    ENDPOINT: /register
    Maneja el alta de un Punto de Carga (CP).
    Cumple el requisito de NO autenticar al CP, solo registrarlo.
    """
    req_data = request.get_json()
    
    if not req_data or 'id' not in req_data:
        return jsonify({"error": "Faltan datos. Se requiere 'id' y 'socket_ip'."}), 400

    cp_id = req_data['id']
    cp_socket = req_data.get('socket_ip', 'unknown') 

    # --- GENERACIÓN DE CLAVE (Requisito de Seguridad) ---
    # Genera una clave simétrica única que será la credencial de ese CP
    # para cifrar mensajes al Central, y que la Central usará para descifrar.
    secret_key = secrets.token_hex(16) # Clave de 32 caracteres hexadecimales

    # 1. Cargamos el estado actual de la BD remota
    '''
    db_data = load_db()
    
    # 2. Nos aseguramos de la estructura básica
    if "charging_points" not in db_data:
        db_data["charging_points"] = {}
        
    # 3. Almacenamos el nuevo CP con su clave secreta
    db_data["charging_points"][cp_id] = {
        "socket_ip": cp_socket,
        "status": "REGISTERED",
        "secret_key": secret_key 
    }
    
    # 4. Guardamos los cambios de vuelta en el servicio remoto
    if not save_db(db_data):
        return jsonify({"error": "Fallo al persistir el registro en la BD remota."}), 500
    '''
    print(f"[+] CP Registrado/Actualizado: {cp_id}")

    # 5. Devolvemos la clave al CP
    return jsonify({
        "message": "Registro exitoso",
        "cp_id": cp_id,
        "secret_key": secret_key 
    }), 201


@app.route('/unregister/<cp_id>', methods=['DELETE'])
def unregister_cp(cp_id):
    """
    ENDPOINT: /unregister/<cp_id>
    Maneja la baja de un Punto de Carga.
    """
    # 1. Cargamos datos de la BD remota
    db_data = load_db()
    
    # 2. Verificamos y eliminamos el CP
    if "charging_points" in db_data and cp_id in db_data["charging_points"]:
        del db_data["charging_points"][cp_id]
        
        # 3. Guardamos los cambios
        if not save_db(db_data):
            return jsonify({"error": "Fallo al persistir la baja en la BD remota."}), 500

        print(f"[-] CP Dado de baja: {cp_id}")
        return jsonify({"message": f"CP {cp_id} eliminado correctamente"}), 200
    else:
        return jsonify({"error": "CP no encontrado o BD no disponible"}), 404

@app.route('/health', methods=['GET'])
def health_check():
    """Endpoint simple para verificar que el servicio está vivo."""
    return jsonify({"status": "EV_Registry Online"}), 200

# -------------------------------------------------------------------------
# ARRANQUE DEL SERVIDOR
# -------------------------------------------------------------------------
if __name__ == '__main__':
    # El puerto interno del contenedor. Se mapea a 5001 en el host por Docker Compose.
    PORT = 8080
    
    print(f"[*] EV_Registry escuchando en puerto {PORT} (HTTPS)")
    print(f"[*] Accediendo a BD remota configurada: {DB_SERVICE_URL}")

    # Requisito de Seguridad (Canal Seguro - HTTPS)
    # 'adhoc' genera un certificado SSL temporal para pruebas en desarrollo.
    # En producción, se usaría un certificado real (cert.pem, key.pem).
    app.run(host='0.0.0.0', port=PORT, ssl_context='adhoc', debug=True)