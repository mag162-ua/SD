# -*- coding: utf-8 -*-

import time
import sys
import socket
import threading
import enum
import json
import os
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

class MENSAJES_CP_M(enum.Enum):
    STATUS_E = "STATUS_E"
    STATUS_OK = "STATUS_OK" 
    STATUS_KO = "STATUS_KO"
    SOL_SUMINISTRO = 'SUPPLY_APPROVE'
    SUMINISTRAR = "SUPPLY_AUTHORIZED"
    SUMINISTRANDO = "ENERGY_FLOW"
    SOL_PARAR = 'STOP'
    PARAR = "STOP_CONFIRMED"
    ERROR_COMM = "ERROR_COMM"
    ERROR_KAFKA = "ERROR_KAFKA"

class EV_CP_E:
    PUERTO_BASE = 6000

    def __init__(self, IP_PUERTO_BROKER, PUERTO):
        self.ID = None
        self.IP_BROKER, self.PUERTO_BROKER = IP_PUERTO_BROKER.split(':')
        self.IP_E = "0.0.0.0"
        self.PUERTO_E = PUERTO
        self.socket_monitor = None
        self.IP_M = None
        self.estado = MENSAJES_CP_M.STATUS_KO.value
        self.producer = None
        self.consumer = None
        self.suministrar_actvio = False
        self.parar_suministro = threading.Event()
        self.espera_respuesta_menu = threading.Event()
        self.total_kwh_suministrados = 0.0
        print(f"Engine inicializado con IP_BROKER: {self.IP_BROKER}, PUERTO_BROKER: {self.PUERTO_BROKER}")

    def abrir_socket(self):
        print("Abriendo monitor...")
        while True:
            self.socket_monitor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket_monitor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            try:
                self.socket_monitor.bind((self.IP_E, self.PUERTO_E))
                self.socket_monitor.listen(5)
                print(f"Socket abierto en {self.IP_E}:{self.PUERTO_E}")
                EV_CP_E.PUERTO_BASE += 1
                return True
            
            except OSError as e:
                print(f"Error al abrir el socket: {e}. Reintentando...")
                self.socket_monitor.close()
                self.PUERTO_E += 1
                EV_CP_E.PUERTO_BASE += 1

            except Exception as e:
                print(f"Error al abrir el socket: {e}")
                self.socket_monitor.close()
                return False

    def abrir_kafka(self):
        print("Abriendo conexión Kafka REAL...")
        try:
            # CORREGIDO: Usar JSON serialization
            self.producer = KafkaProducer(
                bootstrap_servers=[f"{self.IP_BROKER}:{self.PUERTO_BROKER}"],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),  # CORREGIDO
                retries=3,
                acks='all'
            )
            
            # CORREGIDO: Consumer para control_commands (donde la central envía comandos)
            self.consumer = KafkaConsumer(
                'control_commands',  # Topic donde escucha comandos de la central
                bootstrap_servers=[f"{self.IP_BROKER}:{self.PUERTO_BROKER}"],
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id=f'engine_{self.ID}_group' if self.ID else 'engine_temp_group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),  # CORREGIDO
                consumer_timeout_ms=1000
            )
            
            print("✅ Conexión Kafka REAL establecida correctamente.")
            return True
        
        except Exception as e:
            print(f"❌ Error al abrir la conexión Kafka REAL: {e}")
            if self.producer:
                self.producer.close()
            if self.consumer:
                self.consumer.close()
            return False

    def escuchar_monitor(self):
        while True:
            conexion_monitor = None
            try:
                conexion_monitor, self.IP_M = self.socket_monitor.accept()
                
                mensaje = conexion_monitor.recv(1024).decode('utf-8').strip()
                print(f"📨 Mensaje recibido del monitor: {mensaje}")
                
                if mensaje:
                    if self.ID is None:
                        self.ID = mensaje.split('#')[1]
                        print(f"🆔 ID del engine asignado: {self.ID}")
                        self.estado = MENSAJES_CP_M.STATUS_OK.value
                        self.cargar_estado()
                    
                    if mensaje == MENSAJES_CP_M.STATUS_E.value + f"#{self.ID}":
                        respuesta = self.estado
                        conexion_monitor.sendall(respuesta.encode())
                        print(f"📤 Respuesta enviada al monitor: {respuesta}")

            except socket.error as e:
                if e.errno == 9:
                    print(f"🛑 Hilo Monitor detenido. Socket principal cerrado.")
                    break
                else:
                    print(f"Error de socket al escuchar el monitor: {e}")

            except Exception as e:
                print(f"Error al escuchar el monitor: {e}")

            finally:
                if conexion_monitor:
                    conexion_monitor.close()

    def escuchar_central(self):
        if self.consumer is None:
            print("❌ Consumidor Kafka no está inicializado.")
            return

        print(f"🔍 Escuchando mensajes de la central en topic 'control_commands'...")
        
        try:
            for mensaje in self.consumer:
                mensaje_valor = mensaje.value
                print(f"📨 Mensaje recibido de la central: {mensaje_valor}")
                
                cp_id = mensaje_valor.get('cp_id')
                command = mensaje_valor.get('command')  # Cambiado de 'type' a 'command'
                
                if cp_id == self.ID:
                    if command == "START" or command == "RESUME":
                        print("▶️ Comando de inicio recibido de la central.")
                        if self.comprobar_si_suministrar():
                            print("✅ Suministro iniciado por comando de la central")
                        else:
                            print("❌ No se pudo iniciar suministro")
                            
                    elif command == "STOP":
                        print("⏹️ Comando de parada recibido de la central.")
                        if self.comprobar_si_parar():
                            print("✅ Suministro detenido por comando de la central")
                        else:
                            print("❌ No se pudo detener suministro")

        except Exception as e:
            print(f"❌ Error al escuchar la central: {e}")

    def comprobar_si_suministrar(self):
        if self.estado == MENSAJES_CP_M.STATUS_OK.value and not self.suministrar_actvio:
            self.suministrar_actvio = True
            self.total_kwh_suministrados = 0.0
            self.parar_suministro.clear()
            
            # CORREGIDO: Enviar a supply_response con formato correcto
            mensaje = {
                'cp_id': self.ID, 
                'approve': True, 
                'reason': 'Suministro iniciado',
                'timestamp': datetime.now().isoformat(),
                'type': 'SUPPLY_APPROVED'
            }
            
            print(f"📤 Enviando confirmación a supply_response: {mensaje}")
            self.producer.send('supply_response', mensaje)  # CORREGIDO: Topic correcto
            self.producer.flush()
            
            suministrar_thread = threading.Thread(target=self.suministrar_energia, daemon=True)
            suministrar_thread.start()
            return True
        else:
            # Enviar rechazo si no se puede iniciar
            mensaje = {
                'cp_id': self.ID, 
                'approve': False, 
                'reason': 'No se puede iniciar suministro - Estado incorrecto',
                'timestamp': datetime.now().isoformat(),
                'type': 'SUPPLY_REJECTED'
            }
            print(f"📤 Enviando rechazo a supply_response: {mensaje}")
            self.producer.send('supply_response', mensaje)
            self.producer.flush()
            return False

    def comprobar_si_parar(self):
        if self.suministrar_actvio:
            # CORREGIDO: Enviar confirmación de parada
            respuesta = {
                'cp_id': self.ID, 
                'approve': True, 
                'reason': 'Parado por solicitud',
                'timestamp': datetime.now().isoformat(),
                'type': 'SUPPLY_STOPPED'
            }
            print(f"📤 Enviando confirmación de parada a supply_response: {respuesta}")
            self.producer.send('supply_response', respuesta)
            self.producer.flush()
            self.parar_suministro.set()
            return True
        else:
            return False

    def suministrar_energia(self):
        print("⚡ Suministro de energía iniciado.")
        
        while not self.parar_suministro.is_set():
            self.total_kwh_suministrados += 0.1
            
            # CORREGIDO: Enviar datos de suministro a supply_flow
            mensaje = {
                'cp_id': self.ID, 
                'driver_id': "ANONIMO", 
                'kwh': round(self.total_kwh_suministrados, 2),
                'timestamp': datetime.now().isoformat(),
                'type': 'ENERGY_FLOW'
            }
            
            print(f"📤 Enviando datos de suministro a supply_flow: {mensaje}")
            self.producer.send('supply_flow', mensaje)  # CORREGIDO: Topic correcto
            self.producer.flush()
            
            self.guardar_estado()
            self.parar_suministro.wait(1)
        
        print("🔌 Hilo de suministro detenido")
        self.suministrar_actvio = False
        self.total_kwh_suministrados = 0.0

    # ... (el resto de los métodos se mantienen igual, solo cambian los prints)
    def mostrar_menu(self):
        self.espera_respuesta_menu.set()
        while True:
            os.system('cls' if os.name == 'nt' else 'clear')
            
            if self.ID is None:
                print(f"{self.IP_E}:{self.PUERTO_E} A la espera de conexión con un monitor...")
            else:
                print(f"\n--- Menú del Engine {self.ID} : {self.IP_E}:{self.PUERTO_E}---")
                print("1. Mostrar estado actual")
                if self.estado == MENSAJES_CP_M.STATUS_OK.value:
                    print("2. Notificar avería")
                else:
                    print("2. Notificar restablecimiento")
                if self.suministrar_actvio:
                    print("3. Parar suministro de energía")
                else:
                    print("3. Suministrar energía")
                print("4. Salir")
                print("-----------------------------------")
                if self.suministrar_actvio:
                    print("⚠️  Suministro de energía ACTIVO ⚠️ ")
                    print(f"Total kWh suministrados hasta ahora: {self.total_kwh_suministrados:.2f} kWh")
                    print("-----------------------------------")
                print("Seleccione una opción: ")
                if not self.espera_respuesta_menu.is_set():
                    response_menu_thread = threading.Thread(target=self.responder_menu, daemon=True)
                    response_menu_thread.start()

            time.sleep(1)

    def responder_menu(self):
        self.espera_respuesta_menu.clear()
        try:
            switch = input().strip()

            if switch == "1":
                estado = "🟢 ACTIVADO" if self.estado == MENSAJES_CP_M.STATUS_OK.value else "🔴 AVERIADO"
                print(f"Estado actual: {estado}, Suministro activo: {self.suministrar_actvio}, Total kWh suministrados: {self.total_kwh_suministrados:.2f} kWh")
            
            elif switch == "2":
                if self.estado == MENSAJES_CP_M.STATUS_OK.value:
                    print(f"🔴 CP {self.ID} averiado")
                    self.estado = MENSAJES_CP_M.STATUS_KO.value
                else:
                    print(f"🟢 CP {self.ID} reparado")
                    self.estado = MENSAJES_CP_M.STATUS_OK.value

            elif switch == "3":
                if self.suministrar_actvio:
                    if self.comprobar_si_parar():
                        print("⏹️ DETENIENDO EL SUMINISTRO: El punto de carga terminará de suministrar")
                    else:
                        print("❌ Error deteniendo suministro")
                else:
                    if not self.comprobar_si_suministrar():
                        print("❌ IMPOSIBLE_SUMINISTRAR: El punto de carga se encuentra averiado")

            elif switch == "4":
                if self.total_kwh_suministrados != 0.0:
                    self.guardar_estado()
                if self.producer:
                    self.producer.close()
                if self.consumer:
                    self.consumer.close()
                if self.socket_monitor:
                    self.socket_monitor.close()
                print("🔴 CERRANDO SISTEMA")
                os._exit(0)
            elif switch:
                print("❌ Comando desconocido")
            
        except Exception as e:
            print(f"❌ Error en menú: {e}")
        finally:
            self.espera_respuesta_menu.set()

    def run(self):
        print("🚀 Engine corriendo...")
        if self.abrir_socket() and self.abrir_kafka():
            print("✅ Monitor y Kafka iniciados correctamente")

            listener_thread_m = threading.Thread(target=self.escuchar_monitor, daemon=True)
            listener_thread_m.start()

            listener_thread_c = threading.Thread(target=self.escuchar_central, daemon=True)
            listener_thread_c.start()

            self.mostrar_menu()

    def guardar_estado(self):
        estado_info = {
            "ID": self.ID,
            "Total_kWh_Suministrados": self.total_kwh_suministrados
        }
        with open(f"estado_engine_{self.ID}.json", "w") as archivo:
            json.dump(estado_info, archivo, indent=4)
        print(f"💾 Estado del engine guardado en estado_engine_{self.ID}.json")

    def cargar_estado(self):
        if os.path.exists(f"estado_engine_{self.ID}.json"):
            try:
                with open(f"estado_engine_{self.ID}.json", "r") as archivo:
                    estado_info = json.load(archivo)
                    self.total_kwh_suministrados = estado_info.get("Total_kWh_Suministrados", 0.0)
                    self.parar_suministro.clear()
                    self.suministrar_actvio = True
                    print(f"📂 Estado del engine cargado: Total kWh suministrados = {self.total_kwh_suministrados} kWh")
                    suministrar_thread = threading.Thread(target=self.suministrar_energia, daemon=True)
                    suministrar_thread.start()
                    os.remove(f"estado_engine_{self.ID}.json")
            except Exception as e:
                print(f"❌ [ERROR RESILIENCIA] Error al cargar el estado: {e}. Iniciando desde 0.")
                self.total_kwh_suministrados = 0.0

if __name__ == "__main__":
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print("Uso: python ev_cp_engine.py <IP_BROKER:PUERTO_BROKER> <PUERTO_ENGINE/OPCIONAL>")
        sys.exit(1)
    
    puerto_engine = int(sys.argv[2]) if len(sys.argv) == 3 else EV_CP_E.PUERTO_BASE
    engine = EV_CP_E(sys.argv[1], puerto_engine)

    try:
        engine.run()
    except KeyboardInterrupt:
        print("🔴 Engine detenido. Ctrl+C detectado. Saliendo...")
        if engine.total_kwh_suministrados != 0.0:
            engine.guardar_estado()
        if engine.producer:
            engine.producer.close()
        if engine.consumer:
            engine.consumer.close()
        if engine.socket_monitor:
            engine.socket_monitor.close()
        os._exit(0)
