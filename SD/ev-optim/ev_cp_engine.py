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

class MENSAJES_CP_M(enum.Enum):
    STATUS_E = "STATUS_E"
    STATUS_OK = "STATUS_OK"
    STATUS_KO = "STATUS_KO"
    SOL_SUMINISTRO = 'SUPPLY_APPROVED'
    SUMINISTRAR = "supply_response"
    SUMINISTRANDO = "supply_flow"
    SOL_PARAR = 'STOP'
    PARAR = "stop"
    ERROR_COMM = "ERROR_COMM"
    ERROR_KAFKA = "ERROR_KAFKA"

class EV_CP_E:

    PUERTO_BASE = 6000
    TOPICO_ACCION = "supply_flow"
    TOPICO_SUMINISTRO = "supply_response"
    TOPICO_CONTROL = "control_commands"
    TIMEOUT = 2

    def __init__(self, IP_PUERTO_BROKER, PUERTO):
        self.id = None
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
        print("Abriendo conexión Kafka...")
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=[f"{self.IP_BROKER}:{self.PUERTO_BROKER}"], 
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )

            self.consumer = KafkaConsumer(
                EV_CP_E.TOPICO_CONTROL,
                'engine_tickets',
                bootstrap_servers=[f"{self.IP_BROKER}:{self.PUERTO_BROKER}"],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id=f"cp_{self.id}_group",
                value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None
            )

            print(f"✅ Conexión Kafka abierta - Grupo: cp_{self.id}_group")
            return True
        except Exception as e:
            print(f"❌ Error abriendo Kafka: {e}")
            return False

    def escuchar_monitor(self):
        while True:
            conexion_monitor = None
            try:
                conexion_monitor, self.IP_M = self.socket_monitor.accept()
                
                mensaje = conexion_monitor.recv(1024).decode('utf-8').strip()
                if mensaje:
                    if self.id is None:
                        self.id = mensaje.split('#')[1]
                        print(f"ID del engine asignado: {self.id}")
                        self.estado = MENSAJES_CP_M.STATUS_OK.value
                        self.cargar_estado()
                    if mensaje == MENSAJES_CP_M.STATUS_E.value+f"#{self.id}":
                        respuesta = self.estado
                        conexion_monitor.sendall(respuesta.encode())

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
        """Escucha comandos de la Central Y tickets - VERSIÓN MEJORADA"""
        if self.consumer is None:
            print("Consumidor Kafka no está inicializado.")
            return

        print(f"🔍 Escuchando mensajes de la central...")
        
        try:
            for mensaje in self.consumer:
                try:
                    mensaje_valor = mensaje.value
                    print(f"📨 Mensaje recibido: {mensaje_valor}")
                    
                    if isinstance(mensaje_valor, str):
                        try:
                            mensaje_valor = json.loads(mensaje_valor)
                        except json.JSONDecodeError:
                            continue
                    
                    if not isinstance(mensaje_valor, dict):
                        continue
                    
                    cp_id = mensaje_valor.get('cp_id')
                    if not cp_id or str(cp_id) != str(self.id):
                        print(f"📭 Mensaje para CP {cp_id}, este es CP {self.id} - IGNORADO")
                        continue
                    
                    print(f"🎯 MENSAJE PARA ESTE CP {self.id} - PROCESANDO")
                    
                    message_type = mensaje_valor.get('type')
                    if message_type == 'CHARGING_TICKET':
                        print(f"🎫 TICKET RECIBIDO - Transacción: {mensaje_valor.get('ticket_id')}")
                        self.mostrar_ticket(mensaje_valor)
                        continue 
                    
                    command = mensaje_valor.get('command')
                    print(f"⚡ Comando: {command}")
                    
                    if command == 'START':
                        print("🚀 INICIANDO SUMINISTRO...")
                        if not self.suministrar_actvio:
                            self.iniciar_suministro()
                        else:
                            print("ℹ️ Ya estaba suministrando")
                            
                    elif command == 'STOP':
                        print("🛑 DETENIENDO SUMINISTRO...")
                        if self.suministrar_actvio:
                            self.detener_suministro()
                        else:
                            print("ℹ️ Ya estaba detenido")
                            
                except Exception as e:
                    print(f"❌ Error procesando mensaje: {e}")
                        
        except Exception as e:
            print(f"❌ Error en escuchar_central: {e}")

    def iniciar_suministro(self):
        if self.suministrar_actvio:
            return
            
        self.suministrar_actvio = True
        self.parar_suministro.clear()
        self.total_kwh_suministrados = 0.0

        suministro_thread = threading.Thread(target=self.suministrar_energia, daemon=True)
        suministro_thread.start()
        print("✅ Hilo de suministro iniciado")

    def detener_suministro(self):
        if not self.suministrar_actvio:
            return
        self.parar_suministro.set()
        time.sleep(1)
        self.suministrar_actvio = False
        print("✅ Suministro detenido completamente")

    def suministrar_energia(self):
        print("⚡ Suministro de energía iniciado.")
        
        while not self.parar_suministro.is_set():
            self.total_kwh_suministrados += 0.1
            mensaje = {
                'cp_id': self.id, 
                'driver_id': "MANUAL", 
                'kwh': round(self.total_kwh_suministrados, 1),
                'timestamp': datetime.now().isoformat(),
                'reason': 'SUPPLY_FLOW'
            }
            self.producer.send(EV_CP_E.TOPICO_ACCION, json.dumps(mensaje))
            self.producer.flush()
            
            print(f"⚡ Suministrando... {self.total_kwh_suministrados:.1f}kWh")
            self.parar_suministro.wait(1)
        
        print("🔌 Hilo de suministro detenido y finalizado limpiamente.")
        self.suministrar_actvio = False
        
        mensaje_final = {
            'cp_id': self.id, 
            'kwh': round(self.total_kwh_suministrados, 1), 
            'reason': 'SUPPLY_ENDED'
        }
        self.producer.send(EV_CP_E.TOPICO_ACCION, json.dumps(mensaje_final))
        self.producer.flush()
        self.total_kwh_suministrados = 0.0

    def mostrar_menu(self):
        self.espera_respuesta_menu.set()
        while True:

            os.system('cls' if os.name == 'nt' else 'clear')
            
            if self.id is None :
                print(f"{self.IP_E}:{self.PUERTO_E} A la espera de conexión con un monitor...")
            else:
                print(f"\n--- Menú del Engine {self.id} : {self.IP_E}:{self.PUERTO_E}---")
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
                response_menu_thread = threading.Thread(target=self.responder_menu, daemon=True)
                response_menu_thread.start()

            time.sleep(1)

            
    def responder_menu(self):
        self.espera_respuesta_menu.clear()
        switch = input().strip()

        if switch == "1":
            estado = ""
            if self.estado == MENSAJES_CP_M.STATUS_OK.value:
                estado = "🟢 ACTIVADO"
            else:
                estado = "🔴 AVERIADO"
            print(f"Estado actual: {estado}, Suministro activo: {self.suministrar_actvio}, Total kWh suministrados: {self.total_kwh_suministrados:.2f} kWh")
        
        elif switch == "2":
            if self.estado == MENSAJES_CP_M.STATUS_OK.value:
                print(f"CP {self.id} averiado")
                self.estado = MENSAJES_CP_M.STATUS_KO.value
                # Si está suministrando, parar
                if self.suministrar_actvio:
                    self.detener_suministro()
            else:
                print(f"CP {self.id} reparado")
                self.estado = MENSAJES_CP_M.STATUS_OK.value

        elif switch == "3":
            if self.estado == MENSAJES_CP_M.STATUS_OK.value and not self.suministrar_actvio:
                # SOLICITAR INICIO de suministro a la Central
                print("🚀 Solicitando inicio de suministro a la central...")
                mensaje_inicio = {
                    'cp_id': self.id,  # CORRECCIÓN: Incluir siempre el cp_id
                    'type': 'SUPPLY_REQUEST',
                    'driver_id': 'MANUAL_ENGINE',
                    'timestamp': datetime.now().isoformat(),
                    'reason': 'MANUAL_START'
                }
                self.producer.send(EV_CP_E.TOPICO_SUMINISTRO, json.dumps(mensaje_inicio))
                self.producer.flush()
                print("✅ Solicitud de inicio enviada a la central")
                
            elif self.estado == MENSAJES_CP_M.STATUS_OK.value and self.suministrar_actvio:
                # DETENER suministro
                print("🛑 Enviando solicitud de PARADA a la central...")
                mensaje_stop = {
                    'cp_id': self.id,  # CORRECCIÓN: Incluir siempre el cp_id
                    'type': 'STOP_SUPPLY',
                    'reason': 'MANUAL_STOP_ENGINE',
                    'timestamp': datetime.now().isoformat()
                }
                self.producer.send(EV_CP_E.TOPICO_SUMINISTRO, json.dumps(mensaje_stop))
                self.producer.flush()
                
                # También parar localmente el hilo de suministro
                self.detener_suministro()
                print("✅ Solicitud de parada enviada a la central y suministro detenido localmente")
                
            else:
                print("IMPOSIBLE_SUMINISTRAR: El punto de carga se encuentra averiado")

        elif switch == "4":
            if self.total_kwh_suministrados != 0.0:
                self.guardar_estado()
            if self.producer:
                self.producer.close()
            if self.consumer:
                self.consumer.close()
            if self.socket_monitor:
                self.socket_monitor.close()
            print("CERRADA DE SISTEMA")
            os._exit(0)
        elif switch:
            print("Comando desconocido")
        
        self.espera_respuesta_menu.set()

    def run(self):
        print("Engine corriendo...")
        if self.abrir_socket():
            print("Monitor abierto correctamente.")

            # Iniciar hilo del monitor para recibir el ID
            listener_thread_m = threading.Thread(target=self.escuchar_monitor, daemon=True)
            listener_thread_m.start()

            # 🔁 Esperar a que el monitor asigne el ID antes de abrir Kafka
            while self.id is None:
                time.sleep(0.1)

            # ✅ Ahora que el ID está asignado, abrir Kafka con group_id único
            if self.abrir_kafka():
                listener_thread_c = threading.Thread(target=self.escuchar_central, daemon=True)
                listener_thread_c.start()

                self.mostrar_menu()


    def guardar_estado(self):
        estado_info = {
            "ID": self.id,
            "Total_kWh_Suministrados": self.total_kwh_suministrados
        }
        with open(f"estado_engine_{self.id}.json", "w") as archivo:
            json.dump(estado_info, archivo, indent=4)
        print(f"Estado del engine guardado en estado_engine_{self.id}.json")

    def cargar_estado(self):
        if os.path.exists(f"estado_engine_{self.id}.json"):
            try:
                with open(f"estado_engine_{self.id}.json", "r") as archivo:
                    estado_info = json.load(archivo)
                    self.total_kwh_suministrados = estado_info.get("Total_kWh_Suministrados", 0.0)
                    self.parar_suministro.clear()
                    self.suministrar_actvio = True
                    print(f"Estado del engine cargado: Total kWh suministrados = {self.total_kwh_suministrados} kWh")
                    suministrar_thread = threading.Thread(target=self.suministrar_energia, daemon=True)
                    suministrar_thread.start()
                    os.remove(f"estado_engine_{self.id}.json")
            except Exception as e:
                print(f"[ERROR RESILIENCIA] Error al cargar el estado: {e}. Iniciando desde 0.")
                self.total_kwh_suministrados = 0.0

    def mostrar_ticket(self, ticket_data: dict):
        """Muestra el ticket de carga de forma visual y clara"""
        print("\n" + "="*60)
        print("🎫 TICKET DE CARGA - RESUMEN DE TRANSACCIÓN")
        print("="*60)
        
        # Información básica
        print(f"🔌 Punto de Carga: {ticket_data.get('cp_id', 'N/A')}")
        print(f"📋 ID Transacción: {ticket_data.get('ticket_id', 'N/A')}")
        print(f"🕐 Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("-"*60)
        
        # Detalles de energía y costo
        energia = ticket_data.get('energy_consumed', 0)
        precio_kwh = ticket_data.get('price_per_kwh', 0)
        importe_total = ticket_data.get('amount', 0)
        
        print(f"⚡ Energía Consumida: {energia:.2f} kWh")
        print(f"💰 Precio por kWh: €{precio_kwh:.3f}")
        print(f"💵 Importe Total: €{importe_total:.2f}")
        print("-"*60)
        
        # Tiempos de carga
        start_time = ticket_data.get('start_time', '')
        end_time = ticket_data.get('end_time', '')
        
        if start_time and end_time:
            try:
                # Formatear tiempos para mejor legibilidad
                start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
                duracion = end_dt - start_dt
                
                print(f"⏱️  Duración de carga: {duracion.total_seconds() / 60:.1f} minutos")
                print(f"🟢 Inicio: {start_dt.strftime('%H:%M:%S')}")
                print(f"🔴 Fin: {end_dt.strftime('%H:%M:%S')}")
            except Exception as e:
                print(f"⏱️  Inicio: {start_time}")
                print(f"🔴 Fin: {end_time}")
        
        # Resumen final
        print("-"*60)
        print("✅ CARGA COMPLETADA EXITOSAMENTE")
        print("="*60)
        print("\n")
    
if __name__ == "__main__":
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print("Uso: python ev_cp_monitor.py <IP_BROKER:PUERTO_BROKER> <PUERTO_ENGINE/OPCIONAL>")
        sys.exit(1)
    
    puerto_engine = int(sys.argv[2]) if len(sys.argv) == 3 else EV_CP_E.PUERTO_BASE
    engine = EV_CP_E(sys.argv[1], puerto_engine)

    try:
        engine.run()
    except KeyboardInterrupt:
        print("Engine detenido. Ctrl+C detectado. Saliendo...")
        if engine.total_kwh_suministrados != 0.0:
            engine.guardar_estado()
        if engine.producer:
            engine.producer.close()
        if engine.consumer:
            engine.consumer.close()
        if engine.socket_monitor:
            engine.socket_monitor.close()
        os._exit(0)