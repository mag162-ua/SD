import time
import os
import requests
import json

# CONFIGURACIÓN
OPENWEATHER_API_KEY = os.getenv("OW_API_KEY", "d77ed952764846a75a86e7ecf11df223") # ¡Pon tu Key aquí o en var de entorno!
CENTRAL_API_URL = os.getenv("CENTRAL_URL", "http://localhost:8090/api/weather")

# Mapeo manual de CPs a Ciudades
# "indicar manualmente las ciudades donde se encuentran los CP"
# Hay que literalmente poner el id y la ciudad de cada CP que se vaya a registrar
CP_LOCATIONS = {
    # "ID_DEL_CP": "Ciudad,CodigoPais", (la coma para añadir más)
    "1": "Oslo,NO"
}

def get_temperature(city_name):
    """Consulta OpenWeatherMap y devuelve temperatura en Celsius"""
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={OPENWEATHER_API_KEY}&units=metric"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        temp = data['main']['temp']
        return temp
    except Exception as e:
        print(f"Error consultando clima para {city_name}: {e}")
        return None

def notify_central(cp_id, city, temp, alert_type):
    """
    Envía aviso a la Central.
    alert_type: 'alert' (parar) o 'clear' (restaurar)
    """
    endpoint = f"{CENTRAL_API_URL}/{alert_type}"
    payload = {
        "cp_id": cp_id,
        "temperature": temp,
        "city": city
    }
    
    try:
        print(f"Enviando {alert_type.upper()} a Central para {cp_id} ({temp}ºC)...")
        requests.post(endpoint, json=payload, timeout=2)
    except Exception as e:
        print(f"Error contactando con Central: {e}")

def main():
    print(f"☀️ EV_W (Weather Office) Iniciado.")
    print(f"📍 Monitorizando {len(CP_LOCATIONS)} localizaciones.")
    
    # Estado local para no spammear a la central si el estado no cambia
    # Diccionario: { "CP01": True } (True = Alerta activa)
    active_alerts = {}

    while True:
        print("\n--- Consultando OpenWeather ---")
        
        for cp_id, city in CP_LOCATIONS.items():
            temp = get_temperature(city)
            
            if temp is None:
                continue

            print(f"📍 {cp_id} en {city}: {temp}ºC")

            # Lógica del PDF: Si temp < 0 -> Alerta
            if temp < 0:
                # Si no había alerta previa, notificamos
                if not active_alerts.get(cp_id, False):
                    print(f"⚠️ ALERTA: Baja temperatura en {city} para {cp_id}")
                    notify_central(cp_id, city, temp, "alert")
                    active_alerts[cp_id] = True
            else:
                # Si temp >= 0 y HABÍA alerta previa, mandamos 'clear'
                if active_alerts.get(cp_id, False):
                    print(f"✅ NORMALIZADO: Temperatura segura en {city} para {cp_id}")
                    notify_central(cp_id, city, temp, "clear")
                    active_alerts[cp_id] = False

        # "Cada 4 segundos" (PDF pag 6)
        time.sleep(4)

if __name__ == "__main__":
    if OPENWEATHER_API_KEY == "TU_API_KEY_AQUI":
        print("ERROR: Configura tu OPENWEATHER_API_KEY en el código o variables de entorno.")
    else:
        main()