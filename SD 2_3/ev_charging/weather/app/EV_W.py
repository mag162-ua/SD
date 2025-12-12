import time
import os
import requests
import json

# --- CONFIGURACIÓN ---
# URL interna de Docker para hablar con la API (servicio: api_central)
CENTRAL_API_URL = os.getenv("CENTRAL_URL", "http://api_central:8090/api/weather")
OPENWEATHER_API_KEY = os.getenv("OW_API_KEY", "TU_API_KEY_AQUI") 

# Mapeo manual de CPs a Ciudades (Según PDF Pag 6)
CP_LOCATIONS = {
    "1": "Oslo,NO",    # Frío para probar alertas
    "2": "Seville,ES", # Calor
    "3": "Madrid,ES"
}

def get_temperature(city_name):
    """Consulta OpenWeatherMap y devuelve temperatura en Celsius"""
    if OPENWEATHER_API_KEY == "TU_API_KEY_AQUI":
        print("⚠️ ERROR: Falta API KEY de OpenWeather")
        return None

    url = f"http://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={OPENWEATHER_API_KEY}&units=metric"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        temp = data['main']['temp']
        return temp
    except Exception as e:
        print(f"❌ Error consultando clima para {city_name}: {e}")
        return None

def notify_central(cp_id, city, temp, alert_type):
    """
    Envía aviso a la API Central (que lo pasará a Kafka).
    alert_type: 'alert' (parar) o 'clear' (restaurar)
    """
    endpoint = f"{CENTRAL_API_URL}/{alert_type}"
    payload = {
        "cp_id": cp_id,
        "temperature": temp,
        "city": city
    }
    
    try:
        print(f"📡 Enviando {alert_type.upper()} a API Central para {cp_id} ({temp}ºC)...")
        res = requests.post(endpoint, json=payload, timeout=5)
        if res.status_code == 200:
            print("✅ Notificación recibida por API.")
        else:
            print(f"⚠️ API respondió: {res.status_code}")
    except Exception as e:
        print(f"❌ Error contactando con API Central ({endpoint}): {e}")

def main():
    print(f"☀️ EV_W (Weather Office) Iniciado.")
    print(f"🎯 Apuntando a API Central: {CENTRAL_API_URL}")
    print(f"📍 Monitorizando {len(CP_LOCATIONS)} localizaciones.")
    
    # Estado local para no enviar alertas repetidas continuamente
    # { "CP_ID": True/False } -> True significa que hay alerta activa
    active_alerts = {}

    while True:
        print("\n--- Consultando Clima ---")
        
        for cp_id, city in CP_LOCATIONS.items():
            temp = get_temperature(city)
            
            if temp is None:
                continue

            print(f"📍 CP {cp_id} en {city}: {temp}ºC")

            # Lógica PDF: Si temp < 0 -> Alerta (STOP)
            if temp < 8:
                # Si no había alerta previa, notificamos
                if not active_alerts.get(cp_id, False):
                    print(f"❄️ ALERTA: Baja temperatura detectada!")
                    notify_central(cp_id, city, temp, "alert")
                    active_alerts[cp_id] = True
            else:
                # Si temp >= 0 y HABÍA alerta previa, mandamos 'clear' (RESUME)
                if active_alerts.get(cp_id, False):
                    print(f"☀️ NORMALIZADO: Temperatura segura.")
                    notify_central(cp_id, city, temp, "clear")
                    active_alerts[cp_id] = False

        # Pausa para no saturar la API gratuita (cada 10s está bien)
        time.sleep(10)

if __name__ == "__main__":
    main()