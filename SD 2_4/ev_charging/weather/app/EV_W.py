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

# Estructura de datos en memoria (protegida por el Global Interpreter Lock de Python para ops simples)
config_data = {
    "api_key": DEFAULT_API_KEY,
    "locations": {
        "1": "Oslo,NO",
        "2": "Seville,ES",
        "3": "Madrid,ES"
    }
}

running = True  # Control del hilo de fondo
monitoring_active = True # Pausar/Reanudar logs en pantalla

def load_config():
    """Carga la configuración desde el archivo JSON o crea uno por defecto"""
    global config_data
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                config_data = json.load(f)
            print("✅ Configuración cargada desde archivo.")
        except Exception as e:
            print(f"⚠️ Error cargando config, usando valores por defecto: {e}")
    else:
        print("ℹ️ Creando archivo de configuración por defecto...")
        save_config()

def save_config():
    """Guarda la configuración actual en el archivo JSON"""
    try:
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config_data, f, indent=4)
        print("💾 Configuración guardada.")
    except Exception as e:
        print(f"❌ Error guardando configuración: {e}")

def get_temperature(city_name):
    """Consulta OpenWeatherMap"""
    api_key = config_data.get("api_key")
    if not api_key or api_key == "TU_API_KEY_AQUI":
        return None

    url = f"http://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={api_key}&units=metric"
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        data = response.json()
        return data['main']['temp']
    except Exception as e:
        # Solo imprimimos error si el monitoreo está activo para no ensuciar el menú
        if monitoring_active: 
            print(f"❌ Error API Clima ({city_name}): {e}")
        return None

def notify_central(cp_id, city, temp, alert_type):
    """Envía aviso a la API Central"""
    endpoint = f"{CENTRAL_API_URL}/{alert_type}"
    payload = {"cp_id": cp_id, "temperature": temp, "city": city}
    try:
        requests.post(endpoint, json=payload, timeout=5)
    except Exception as e:
        print(f"❌ Error conectando con Central: {e}")

def weather_loop():
    """Hilo en segundo plano que comprueba el clima periódicamente"""
    active_alerts = {} # Estado local de alertas

    print("🚀 Hilo de monitoreo climático iniciado.")
    
    while running:
        if monitoring_active:
            # Iterar sobre una COPIA de las claves para evitar errores si se modifica el diccionario durante el loop
            locations = config_data["locations"].copy()
            
            if not config_data.get("api_key"):
                print("⚠️  AVISO: Falta API KEY. Configúrala en el menú.")
            
            elif not locations:
                print("⚠️  AVISO: No hay localizaciones configuradas.")

            else:
                print(f"\n--- ☁️ Consultando Clima ({len(locations)} CPs) ---")
                
                for cp_id, city in locations.items():
                    if not running: break
                    
                    temp = get_temperature(city)
                    if temp is None: continue

                    print(f"📍 CP {cp_id} [{city}]: {temp}ºC")

                    # Lógica de Alerta
                    if temp < 8:
                        if not active_alerts.get(cp_id, False):
                            print(f"❄️  ALERTA ENVIADA: CP {cp_id}")
                            notify_central(cp_id, city, temp, "alert")
                            active_alerts[cp_id] = True
                    else:
                        if active_alerts.get(cp_id, False):
                            print(f"☀️  RESTABLECIDO: CP {cp_id}")
                            notify_central(cp_id, city, temp, "clear")
                            active_alerts[cp_id] = False
        
        # Esperar 15 segundos antes de la siguiente vuelta
        for _ in range(15):
            if not running: break
            time.sleep(1)

def show_menu():
    """Muestra el menú interactivo"""
    print("\n" + "="*40)
    print(" 🕹️  CONTROL DE CLIMA (EV_W) ")
    print("="*40)
    print("1. Ver configuración actual")
    print("2. Añadir/Modificar localización")
    print("3. Eliminar localización")
    print("4. Cambiar API KEY")
    print("5. Pausar/Reanudar Logs de Monitoreo")
    print("6. Salir")
    print("="*40)

def interactive_menu():
    """Hilo principal para gestionar la entrada del usuario"""
    global running, monitoring_active
    
    while running:
        # Pequeña pausa para que los logs del otro hilo no rompan el input visualmente
        time.sleep(0.5) 
        
        # Si el monitoreo está activo, el menú se imprime entre logs. 
        # Si está pausado, se ve limpio.
        if not monitoring_active:
            show_menu()
            opcion = input("Seleccione opción: ").strip()
        else:
            # Modo "comando oculto" mientras salen logs
            print("\n[Presione Enter para ver menú o escriba comando (1-6)]")
            opcion = input("Cmd > ").strip()
            if not opcion:
                monitoring_active = False # Pausamos logs para ver el menú tranquilos
                continue

        if opcion == '1':
            print("\n--- 📋 CONFIGURACIÓN ACTUAL ---")
            print(f"API KEY: {config_data['api_key']}")
            print("Localizaciones:")
            for k, v in config_data['locations'].items():
                print(f"  - CP {k}: {v}")
            input("[Enter para continuar]")

        elif opcion == '2':
            cp = input("Ingrese ID del CP: ").strip()
            city = input("Ingrese Ciudad,CodigoPais (ej: Madrid,ES): ").strip()
            if cp and city:
                config_data['locations'][cp] = city
                save_config()
                print(f"✅ CP {cp} asignado a {city}")
            else:
                print("❌ Datos inválidos.")

        elif opcion == '3':
            cp = input("Ingrese ID del CP a eliminar: ").strip()
            if cp in config_data['locations']:
                del config_data['locations'][cp]
                save_config()
                print(f"🗑️ CP {cp} eliminado.")
            else:
                print("❌ ID no encontrado.")

        elif opcion == '4':
            key = input("Nueva API KEY: ").strip()
            if key:
                config_data['api_key'] = key
                save_config()
                print("✅ API Key actualizada.")

        elif opcion == '5':
            monitoring_active = not monitoring_active
            estado = "RESUMIDO" if monitoring_active else "PAUSADO"
            print(f"⏯️  Monitoreo en pantalla {estado} (El proceso sigue corriendo de fondo)")

        elif opcion == '6':
            print("👋 Cerrando Weather Service...")
            running = False
            break

if __name__ == "__main__":
    # 1. Cargar configuración persistente
    load_config()

    # 2. Iniciar hilo de monitoreo (Daemon=False para esperar que termine limpio)
    t_weather = threading.Thread(target=weather_loop)
    t_weather.start()

    # 3. Ejecutar menú en hilo principal
    try:
        interactive_menu()
    except KeyboardInterrupt:
        running = False
        print("\nDeteniendo...")
    
    t_weather.join()
    print("Sistema detenido.")