import time
import os
import requests
import json
import threading
import sys

# --- CONFIGURACIÓN ---
CENTRAL_API_URL = os.getenv("CENTRAL_URL", "http://api_central:8090/api/weather")
CONFIG_FILE = "weather_config.json"
DEFAULT_API_KEY = os.getenv("OW_API_KEY", "")

# NUEVA ESTRUCTURA: Solo lista de ciudades
config_data = {
    "api_key": DEFAULT_API_KEY,
    "cities": [
        "Oslo,NO",
        "Madrid,ES",
        "Tokyo,JP"
    ]
}

running = True
monitoring_active = True

def load_config():
    global config_data
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                saved_data = json.load(f)
                # Migración automática si detecta formato antiguo
                if "locations" in saved_data:
                    print("⚠️ Formato antiguo detectado. Migrando a lista de ciudades...")
                    config_data["cities"] = list(saved_data["locations"].values())
                    if "api_key" in saved_data: config_data["api_key"] = saved_data["api_key"]
                else:
                    config_data = saved_data
            print("✅ Configuración cargada.")
        except Exception as e:
            print(f"⚠️ Error cargando config: {e}")
    else:
        save_config()

def save_config():
    try:
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config_data, f, indent=4)
        print("💾 Configuración guardada.")
    except Exception as e:
        print(f"❌ Error guardando: {e}")

def get_temperature(city_name):
    api_key = config_data.get("api_key")
    if not api_key: return None
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={api_key}&units=metric"
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        return response.json()['main']['temp']
    except Exception:
        return None

def send_temp_update(city, temp):
    endpoint = f"{CENTRAL_API_URL}/update"
    try:
        requests.post(endpoint, json={"city": city, "temperature": temp}, timeout=2)
    except: pass # Ignorar errores de actualización rutinaria

def notify_central(city, temp, alert_type):
    """Envía solo la ciudad y temperatura, sin CP ID"""
    endpoint = f"{CENTRAL_API_URL}/{alert_type}"
    payload = {"city": city, "temperature": temp}
    try:
        requests.post(endpoint, json=payload, timeout=5)
    except Exception as e:
        print(f"❌ Error conectando con Central: {e}")

def weather_loop():
    active_alerts = {} # Registro local de alertas por ciudad

    print("🚀 Hilo de monitoreo de CIUDADES iniciado.")
    
    while running:
        if monitoring_active:
            cities = list(config_data.get("cities", []))
            
            if not config_data.get("api_key"):
                print("⚠️  Falta API KEY.")
            elif not cities:
                print("⚠️  No hay ciudades configuradas.")
            else:
                print(f"\n--- ☁️ Consultando Clima en {len(cities)} Ciudades ---")
                
                for city in cities:
                    if not running: break
                    
                    temp = get_temperature(city)
                    if temp is None: continue

                    print(f"🏙️  {city}: {temp}ºC")

                    send_temp_update(city, temp)

                    # ALERTA SI BAJA DE 0 GRADOS (O el umbral que prefieras)
                    if temp < 0:
                        if city not in active_alerts:
                            print(f"❄️  ALERTA ENVIADA: {city} bajo cero")
                            notify_central(city, temp, "alert")
                            active_alerts[city] = True
                    else:
                        if city in active_alerts:
                            print(f"☀️  RESTABLECIDO: {city}")
                            notify_central(city, temp, "clear")
                            del active_alerts[city]
        
        for _ in range(15):
            if not running: break
            time.sleep(1)

def show_menu():
    print("\n" + "="*40)
    print(" 🕹️  WEATHER SERVICE (MODO CIUDADES) ")
    print("="*40)
    print("1. Ver ciudades vigiladas")
    print("2. Añadir ciudad")
    print("3. Eliminar ciudad")
    print("4. Cambiar API KEY")
    print("5. Pausar Logs")
    print("6. Salir")
    print("="*40)

def interactive_menu():
    global running, monitoring_active
    while running:
        time.sleep(0.5)
        if not monitoring_active: show_menu()
        
        if monitoring_active:
            print("\n[Enter para menú]")
            opcion = input("Cmd > ").strip()
            if not opcion:
                monitoring_active = False
                continue
        else:
            opcion = input("Seleccione: ").strip()

        if opcion == '1':
            print(f"\nAPI KEY: {config_data.get('api_key')}")
            print("Ciudades:")
            for c in config_data.get("cities", []): print(f" - {c}")
            input("[Enter]")

        elif opcion == '2':
            city = input("Ciudad,Codigo (ej: Berlin,DE): ").strip()
            if city and city not in config_data["cities"]:
                config_data["cities"].append(city)
                save_config()
                print("✅ Ciudad añadida.")

        elif opcion == '3':
            city = input("Ciudad a borrar: ").strip()
            if city in config_data["cities"]:
                config_data["cities"].remove(city)
                save_config()
                print("🗑️ Ciudad eliminada.")

        elif opcion == '4':
            config_data['api_key'] = input("Nueva API KEY: ").strip()
            save_config()

        elif opcion == '5':
            monitoring_active = not monitoring_active

        elif opcion == '6':
            running = False
            break

if __name__ == "__main__":
    load_config()
    t = threading.Thread(target=weather_loop)
    t.start()
    try: interactive_menu()
    except: running = False
    t.join()