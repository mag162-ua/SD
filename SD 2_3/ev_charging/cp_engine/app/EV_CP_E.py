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
if os.name != 'nt':
    import select
    import tty
    import termios
else:
    import msvcrt

class MENSAJES_CP_M(enum.Enum):
    STATUS_E = "STATUS_E"
    STATUS_OK = "STATUS_OK"
    STATUS_KO = "STATUS_KO"
    TICKET_RECIBIDO = 'CHARGING_TICKET'
    SOL_SUMINISTRO = 'SUPPLY_REQUEST'
    SUMINISTRAR = "START"
    SUMINISTRANDO = "SUPPLY_FLOW"
    SOL_PARAR = 'STOP_SUPPLY'
    PARAR = "STOP"

class Ticket: #CONFIGURAR SEGUN NECESIDADES
    
    def __init__(self, cp_id, type, ticket_id, energy_consumed, amount, price_per_kwh, start_time, end_time, timestamp, tiempo_pantalla):
        self.cp_id = cp_id
        self.type = type
        self.ticket_id = ticket_id
        self.energy_consumed = energy_consumed
        self.amount = amount
        self.price_per_kwh = price_per_kwh
        self.start_time = start_time
        self.end_time = end_time
        self.timestamp = timestamp
        self.tiempo_pantalla = tiempo_pantalla
        self.my_ip = self.get_local_ip()
    
    def get_local_ip(self):
        """Obtiene la IP real de la máquina (útil en Docker)"""
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            # No se conecta realmente, solo busca la ruta de salida
            s.connect(('10.255.255.255', 1))
            IP = s.getsockname()[0]
        except Exception:
            IP = '127.0.0.1'
        finally:
            s.close()
        return IP

    def mostrar_ticket(self):
        SEPARADOR_FIN = "=" * 80
        SEPARADOR_MEDIO = "-" * 80
        
        # --- Título ---
        print(f"\n{SEPARADOR_FIN}")
        print(f"| {'***** TICKET DE CARGA FINALIZADA *****':^78} |")
        print(f"| {self.type.upper():^78} |")
        print(f"{SEPARADOR_FIN}")
        
        # --- Detalles de la Sesión ---
        print(f"| {'Punto de Carga ID: ':<30} {self.cp_id:^47} |")
        print(f"| {'Ticket ID: ':<30} {self.ticket_id:^47} |")
        print(SEPARADOR_MEDIO)
        
        # --- Consumo y Costo ---
        # Usamos f-strings para formatear los números a dos decimales y alinearlos.
        energia_str = f"{self.energy_consumed:,.2f} kWh"
        importe_str = f"{self.amount:,.2f} €"
        precio_str = f"{self.price_per_kwh:,.3f} €/kWh"
        
        print(f"| {'ENERGÍA CONSUMIDA: ':<30} {energia_str:>47} |")
        print(f"| {'IMPORTE TOTAL: ':<30} {importe_str:>47} |")
        print(f"| {'Precio por kWh: ':<30} {precio_str:>47} |")
        print(SEPARADOR_MEDIO)
        
        # --- Tiempos ---
        print(f"| {'Inicio de Carga: ':<30} {self.start_time:^47} |")
        print(f"| {'Fin de Carga: ':<30} {self.end_time:^47} |")
        print(f"| {'Timestamp de Emisión: ':<30} {self.timestamp:^47} |")
        
        # --- Final ---
        print(f"{SEPARADOR_FIN}\n")
        
        # Reducir el contador para que se oculte después de unos segundos
        self.tiempo_pantalla -= 1

class EV_CP_E:

    PUERTO_BASE = 6000
    TOPICO_ACCION = "supply_flow"
    TOPICO_SUMINISTRO = "supply_response"
    TOPICO_CONTROL = "control_commands"
    TOPICO_TICKETS = "engine_tickets"
    RUTA_FICHERO = "/app/estado_engine/"
    

    def __init__(self, IP_PUERTO_BROKER, PUERTO):
        self.ID = None # Identificador del engine
        self.IP_BROKER, self.PUERTO_BROKER = IP_PUERTO_BROKER.split(':') # Dirección IP y puerto del broker Kafka
        self.IP_E = "0.0.0.0" # Escuchar en todas las interfaces
        self.PUERTO_E = PUERTO # Puerto del engine
        self.socket_monitor = None # Socket para la comunicación con el monitor
        self.IP_M = None # Dirección IP del monitor conectado
        self.estado = MENSAJES_CP_M.STATUS_KO.value # Estado inicial del engine
        self.producer = None # Productor Kafka
        self.consumer = None # Consumidor Kafka
        self.suministrar_actvio = False # Estado de suministro
        self.parar_suministro = threading.Event() # Evento para parar el suministro
        self.total_kwh_suministrados = 0.0 # Contador de kWh suministrados
        self.ticket_actual = None # Ticket actual en pantalla
        self.averiado = False # Estado de avería
        self.my_ip = self.get_local_ip()
        print(f"Engine inicializado con IP_BROKER: {self.IP_BROKER}, PUERTO_BROKER: {self.PUERTO_BROKER}")

    def get_local_ip(self):
        """Obtiene la IP local del contenedor/máquina"""
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            # No se conecta realmente, solo busca la ruta
            s.connect(('10.255.255.255', 1))
            IP = s.getsockname()[0]
        except Exception:
            IP = '127.0.0.1'
        finally:
            s.close()
        return IP

    def abrir_socket(self): #Intento de abrir socket para el monitor
        print("Abriendo monitor...")
        while True:
            self.socket_monitor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket_monitor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            try:
                self.socket_monitor.bind((self.IP_E, self.PUERTO_E))
                self.socket_monitor.listen(5)
                print(f"Socket abierto en {self.IP_E}:{self.PUERTO_E}")
                #EV_CP_E.PUERTO_BASE += 1
                return True
            
            except OSError as e: #Si el puerto está en uso, probar con el siguiente
                print(f"Error al abrir el socket: {e}. Reintentando...")
                self.socket_monitor.close()
                #self.PUERTO_E += 1
                #EV_CP_E.PUERTO_BASE += 1

            except Exception as e:
                print(f"Error al abrir el socket: {e}")
                self.socket_monitor.close()
                return False

    def abrir_kafka(self): #Intento de abrir conexión Kafka
        print("Abriendo conexión Kafka...")
        try:
            # Configurar el productor y consumidor con group_id único basado en el ID del engine
            self.producer = KafkaProducer( 
                bootstrap_servers=[f"{self.IP_BROKER}:{self.PUERTO_BROKER}"], 
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            self.consumer = KafkaConsumer(
                EV_CP_E.TOPICO_CONTROL,
                EV_CP_E.TOPICO_TICKETS, 
                bootstrap_servers=[f"{self.IP_BROKER}:{self.PUERTO_BROKER}"], 
                auto_offset_reset='latest', 
                enable_auto_commit=True, 
                group_id=f'engine_{self.ID}_group', 
                value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None
            )
            
            print("Conexión Kafka abierta correctamente.")
            return True
        
        except Exception as e:
            print(f"Error al abrir la conexión Kafka: {e}")
            if (hasattr(self, 'producer') and self.producer) or (hasattr(self, 'consumer') and self.consumer): 
                self.producer.close()
                self.consumer.close()
            return False

    def escuchar_monitor(self):
        self.socket_monitor.settimeout(None) # Quitar timeout para mantener conexión abierta o usar uno largo
        print(f"👂 Esperando conexión del Monitor en {self.IP_E}:{self.PUERTO_E}...")
        
        while True:
            conexion_monitor = None
            try:
                # Aceptar conexión (Bloqueante)
                conexion_monitor, self.IP_M = self.socket_monitor.accept()
                print(f"✅ Monitor conectado desde {self.IP_M}")
                
                with conexion_monitor: # Context manager asegura cierre limpio si hay error
                    while True: # Bucle de comunicación persistente con ESTE monitor
                        try:
                            # Recibir datos
                            data = conexion_monitor.recv(1024)
                            
                            if not data: 
                                print("⚠️ Monitor desconectado (Socket cerrado por el cliente).")
                                break # Salir del bucle interno, volver a accept()
                                
                            mensaje = data.decode('utf-8').strip()
                            
                            if mensaje:
                                if self.ID is None and '#' in mensaje:
                                    # Formato esperado mensaje inicial: STATUS_E#ID_CP
                                    self.ID = mensaje.split('#')[1]
                                    print(f"🆔 ID del engine asignado: {self.ID}")
                                    self.estado = MENSAJES_CP_M.STATUS_OK.value

                                if MENSAJES_CP_M.STATUS_E.value in mensaje:
                                    # Si no está averiado por comando manual, responder OK
                                    if not self.averiado:
                                        self.estado = MENSAJES_CP_M.STATUS_OK.value
                                    
                                    # Responder al monitor
                                    respuesta = self.estado
                                    conexion_monitor.sendall(respuesta.encode())
                                    
                        except ConnectionResetError:
                            print("⚠️ Conexión reseteada por el Monitor.")
                            break
                        except Exception as e:
                            print(f"❌ Error leyendo del socket: {e}")
                            break
                            
            except Exception as e:
                print(f"❌ Error aceptando conexión: {e}")
                time.sleep(1) # Esperar un poco antes de reintentar accept

    def escuchar_central(self):
        #Escucha comandos de la Central
        if self.consumer is None:
            print("Consumidor Kafka no está inicializado.")
            return

        print(f"🔍 Escuchando mensajes de la central en topic: {EV_CP_E.TOPICO_CONTROL}")
        
        try:
            for mensaje in self.consumer: #Bucle infinito para escuchar mensajes
                try:
                    mensaje_valor = mensaje.value 
                    print(f"📨 Mensaje recibido de la central: {mensaje_valor}")
                    
                    if isinstance(mensaje_valor, str): # Asegurarse de que el mensaje es un diccionario
                        try:
                            mensaje_valor = json.loads(mensaje_valor)
                        except json.JSONDecodeError:
                            print("❌ Error: Mensaje no es JSON válido y se descartará.")
                            continue # Pasar al siguiente mensaje
                    
                    if not isinstance(mensaje_valor, dict):
                        continue

                    cp_id = mensaje_valor.get('cp_id') # Obtener el ID del CP del mensaje

                    if not cp_id or str(cp_id) != str(self.ID): # Ignorar mensajes no dirigidos a este CP
                        print(f"📭 Mensaje para CP {cp_id}, este es CP {self.ID} - IGNORADO")
                        continue

                    if cp_id == self.ID: # Procesar solo si el mensaje es para este CP
                        print(f"🎯 Mensaje para este CP {self.ID}")

                        command = mensaje_valor.get('command')
                        message_type = mensaje_valor.get('type')
                        
                        if message_type == MENSAJES_CP_M.TICKET_RECIBIDO.value: #Si es un ticket recibido
                            print(f"🎫 TICKET RECIBIDO - Transacción: {mensaje_valor.get('ticket_id')}")
                            # Crear objeto Ticket
                            self.ticket_actual = Ticket(
                                cp_id=mensaje_valor.get('cp_id'),
                                type=mensaje_valor.get('type'),
                                ticket_id=mensaje_valor.get('ticket_id'),
                                energy_consumed=mensaje_valor.get('energy_consumed'),
                                amount=mensaje_valor.get('amount'),
                                price_per_kwh=mensaje_valor.get('price_per_kwh'),
                                start_time=mensaje_valor.get('start_time'),
                                end_time=mensaje_valor.get('end_time'),
                                timestamp=mensaje_valor.get('timestamp'),
                                tiempo_pantalla=30
                            )
                            continue

                        if command == MENSAJES_CP_M.SUMINISTRAR.value: #Si el comando es suministrar
                            print("🚀 Comando START recibido - Iniciando suministro...")
                            if not self.suministrar_actvio: # Si no está suministrando, iniciar
                                self.iniciar_suministro()
                                
                        elif command == MENSAJES_CP_M.PARAR.value: #Si el comando es parar
                            print("🛑 Comando STOP recibido - Deteniendo suministro...")
                            if self.suministrar_actvio: # Si está suministrando, detener
                                self.detener_suministro()
                                
                except Exception as e:
                    print(f"❌ Error procesando mensaje: {e}")
                    
        except Exception as e:
            print(f"❌ Error en escuchar_central: {e}")

    def iniciar_suministro(self):
        """Inicia el suministro de energía"""
        if self.suministrar_actvio: # Ya está suministrando
            return
        
        self.ticket_actual = None  # Limpiar ticket actual al iniciar suministro   
        self.suministrar_actvio = True
        self.parar_suministro.clear()

        # Iniciar hilo de suministro
        suministro_thread = threading.Thread(target=self.suministrar_energia, daemon=True) 
        suministro_thread.start()
        print("✅ Hilo de suministro iniciado")

    def detener_suministro(self):
        """Detiene el suministro de energía"""
        if not self.suministrar_actvio: # Ya está detenido
            return

        self.parar_suministro.set()
        self.suministrar_actvio = False
        self.total_kwh_suministrados = 0.0

        # Eliminar archivo de estado si existe
        if os.path.exists(f"{EV_CP_E.RUTA_FICHERO}estado_engine_{self.ID}.json"):
            os.remove(f"{EV_CP_E.RUTA_FICHERO}estado_engine_{self.ID}.json")
        print("✅ Suministro detenido completamente")

    def suministrar_energia(self):
        """Hilo principal de suministro de energía - CORREGIDO"""
        print("⚡ Suministro de energía iniciado.")
        
        try: # Bucle de suministro
            while not self.parar_suministro.is_set(): # Mientras no se haya solicitado parar
                self.total_kwh_suministrados += 0.1
                self.guardar_estado() # Guardar estado tras cada incremento
                
                # Enviar datos de suministro a la Central
                mensaje = {
                    'cp_id': self.ID, 
                    'driver_id': "MANUAL", 
                    'kwh': round(self.total_kwh_suministrados, 1),
                    'timestamp': datetime.now().isoformat(),
                    'reason': MENSAJES_CP_M.SUMINISTRANDO.value,
                    'source_ip': self.my_ip
                }
                
                # Enviar mensaje a la central 
                self.producer.send(EV_CP_E.TOPICO_ACCION, mensaje)
                self.producer.flush()
                
                print(f"⚡ Suministrando... {self.total_kwh_suministrados:.1f}kWh")
                time.sleep(1) 
            
            # CÓDIGO DE SALIDA LIMPIA (si self.parar_suministro.set() fue llamado)
            print("🔌 Hilo de suministro detenido y finalizado limpiamente.")
            self.suministrar_actvio = False
            
            # Enviar mensaje final de parada
            mensaje_final = {
                'cp_id': self.ID, 
                'kwh': round(self.total_kwh_suministrados, 1), 
                'reason': 'SUPPLY_ENDED'
            }
            # Enviar mensaje_final a la central
            self.producer.send(EV_CP_E.TOPICO_ACCION, mensaje_final)
            self.producer.flush()

            # Resetear contadores
            self.total_kwh_suministrados = 0.0

        except Exception as e:
            
            print(f"❌ [ERROR CRÍTICO HILO SUMINISTRO] El suministro falló inmediatamente: {e}", file=sys.stderr)
            # Limpiar estado local para no mostrarlo como activo en el menú
            self.suministrar_actvio = False
            self.total_kwh_suministrados = 0.0

    def mostrar_menu(self): #Menú interactivo del Engine
        while True:

            os.system('cls' if os.name == 'nt' else 'clear') #Limpiar pantalla
            
            print(f"\n--- Menú del Engine {self.ID} : {self.IP_E}:{self.PUERTO_E}---")
            print("1. Mostrar estado actual") #Mostrar estado actual

            if self.estado == MENSAJES_CP_M.STATUS_OK.value: #Si está OK o KO
                print("2. Notificar avería")
            else:
                print("2. Notificar restablecimiento")

            if self.suministrar_actvio: #Si está suministrando o no
                print("3. Parar suministro de energía")
            else:
                print("3. Suministrar energía")

            print("4. Salir") #Salir del programa
            print("-----------------------------------")

            if self.suministrar_actvio: #Mostrar estado de suministro
                print("⚠️  Suministro de energía ACTIVO ⚠️ ")
                print(f"Total kWh suministrados hasta ahora: {self.total_kwh_suministrados:.2f} kWh")
                print("-----------------------------------")

            if self.ticket_actual and self.ticket_actual.tiempo_pantalla > 0: #Mostrar ticket si existe y tiempo pantalla > 0
                self.ticket_actual.mostrar_ticket()

            print("Seleccione una opción: ", end='')

            respuesta = None # Esperar entrada sin bloquear
            if os.name == 'nt': # Solo intentar usar msvcrt en Windows
                if msvcrt.kbhit():
                    respuesta = msvcrt.getch().decode()
            else: # En sistemas Unix, usar select
                if select.select([sys.stdin], [], [], 0) == ([sys.stdin], [], []):
                    # Si hay datos, leemos la línea completa.
                    respuesta = sys.stdin.readline().strip()
            self.responder_menu(respuesta)
            time.sleep(1)

            
    def responder_menu(self, switch):

        if switch == "1": #Mostrar estado actual
            estado = ""
            if self.estado == MENSAJES_CP_M.STATUS_OK.value:
                estado = "🟢 ACTIVADO"
            else:
                estado = "🔴 AVERIADO"
            print(f"Estado actual: {estado}, Suministro activo: {self.suministrar_actvio}, Total kWh suministrados: {self.total_kwh_suministrados:.2f} kWh")
        
        elif switch == "2": #Notificar avería o restablecimiento
            if self.estado == MENSAJES_CP_M.STATUS_OK.value:
                print(f"CP {self.ID} averiado")
                self.estado = MENSAJES_CP_M.STATUS_KO.value
                self.averiado = True
                # Si está suministrando, parar
                if self.suministrar_actvio:
                    self.detener_suministro()

            else:
                print(f"CP {self.ID} reparado")
                try:
                    mensaje_resume = {
                        'cp_id': self.ID,
                        'command': 'RESUME',  # Este comando fuerza ACTIVADO en Central
                        'reason': 'MANUAL_REPAIR_ENGINE',
                        'source': 'engine_manual',
                        'timestamp': datetime.now().isoformat()
                    }
                    # Usamos el tópico de control (control_commands)
                    self.producer.send(EV_CP_E.TOPICO_CONTROL, mensaje_resume)
                    self.producer.flush()
                    print("✅ Notificación de reparación enviada a la Central")
                except Exception as e:
                    print(f"❌ Error notificando reparación: {e}")
                self.averiado = False

        elif switch == "3": #Suministrar o parar suministro
            if self.estado == MENSAJES_CP_M.STATUS_OK.value and not self.suministrar_actvio: # Si está OK y no está suministrando
                # SOLICITAR INICIO de suministro a la Central
                print("🚀 Solicitando inicio de suministro a la central...")
                mensaje_inicio = {
                    'cp_id': self.ID,
                    'type': MENSAJES_CP_M.SOL_SUMINISTRO.value,
                    'driver_id': 'MANUAL_ENGINE',
                    'timestamp': datetime.now().isoformat(),
                    'reason': 'MANUAL_START',
                    'source_ip': self.my_ip
                }
                self.producer.send(EV_CP_E.TOPICO_SUMINISTRO, json.dumps(mensaje_inicio))
                self.producer.flush()
                print("✅ Solicitud de inicio enviada a la central")
                
            elif self.estado == MENSAJES_CP_M.STATUS_OK.value and self.suministrar_actvio: # Si está OK y está suministrando
                # DETENER suministro - ENVIAR MENSAJE DE STOP CORRECTO
                print("🛑 Enviando solicitud de PARADA a la central...")
                mensaje_stop = {
                    'cp_id': self.ID,
                    'type': MENSAJES_CP_M.SOL_PARAR.value,
                    'reason': 'MANUAL_STOP_ENGINE',
                    'timestamp': datetime.now().isoformat(),
                    'source_ip': self.my_ip
                }
                self.producer.send(EV_CP_E.TOPICO_SUMINISTRO, json.dumps(mensaje_stop))
                self.producer.flush()
                
                # También parar localmente el hilo de suministro
                self.detener_suministro()
                print("✅ Solicitud de parada enviada a la central y suministro detenido localmente")
                
            else: # Si está averiado
                print("IMPOSIBLE_SUMINISTRAR: El punto de carga se encuentra averiado")

        elif switch == "4": #Salir del programa
            self.limpiar_y_salir()

        elif switch:
            print("Comando desconocido")

    def run(self):
        print("Engine corriendo...")
        
        if self.abrir_socket():
            print("Monitor abierto correctamente.")

            # Iniciar hilo del monitor para recibir el ID y estado
            listener_thread_m = threading.Thread(target=self.escuchar_monitor, daemon=True)
            listener_thread_m.start()

            # 🔁 Esperar a que el monitor asigne el ID antes de abrir Kafka
            while self.ID is None:
                os.system('cls' if os.name == 'nt' else 'clear')
                print(f"⏳ Engine {self.IP_E}:{self.PUERTO_E} sperando asignación de ID desde el monitor...")
                time.sleep(1)

            # Ahora que el ID está asignado, abrir Kafka con group_id único
            if self.abrir_kafka():
                self.cargar_estado()
                listener_thread_c = threading.Thread(target=self.escuchar_central, daemon=True)
                listener_thread_c.start()

                self.mostrar_menu()
            
    def guardar_estado(self): #Guardar estado actual en fichero JSON
        estado_info = {
            "ID": self.ID,
            "Total_kWh_Suministrados": self.total_kwh_suministrados
        }
        with open(f"{EV_CP_E.RUTA_FICHERO}estado_engine_{self.ID}.json", "w") as archivo:
            json.dump(estado_info, archivo, indent=4)
        print(f"Estado del engine guardado en estado_engine_{self.ID}.json")

    def cargar_estado(self): #Cargar estado desde fichero JSON si existe
        if os.path.exists(f"{EV_CP_E.RUTA_FICHERO}estado_engine_{self.ID}.json"):
            try:
                with open(f"{EV_CP_E.RUTA_FICHERO}estado_engine_{self.ID}.json", "r") as archivo:
                    estado_info = json.load(archivo)
                    self.total_kwh_suministrados = estado_info.get("Total_kWh_Suministrados", 0.0)
                    self.parar_suministro.clear()
                    self.suministrar_actvio = True
                    print(f"Estado del engine cargado: Total kWh suministrados = {self.total_kwh_suministrados} kWh")
                    suministrar_thread = threading.Thread(target=self.suministrar_energia, daemon=True)
                    suministrar_thread.start()
                    os.remove(f"{EV_CP_E.RUTA_FICHERO}estado_engine_{self.ID}.json")

            except Exception as e:
                print(f"Error al cargar el estado: {e}. Iniciando desde 0.")
                self.total_kwh_suministrados = 0.0

    def limpiar_y_salir(self): #Limpiar recursos y salir del programa
        print("\nIniciando limpieza de recursos...")
        if self.ID and self.total_kwh_suministrados != 0.0:
            self.guardar_estado()
            time.sleep(1)

        if self.suministrar_actvio:
            #self.parar_suministro.set() 
            time.sleep(1.5)

        try: #Cerrar conexiones Kafka y socket
            if hasattr(self, 'producer') and self.producer:
                self.producer.close()
            if hasattr(self, 'consumer') and self.consumer:
                self.consumer.close()
            # CRUCIAL: Cerrar el socket detiene el bloqueo en .accept() de escuchar_monitor
            if hasattr(self, 'socket_monitor') and self.socket_monitor:
                self.socket_monitor.close() 

        except Exception as e:
            print(f"[Engine Shutdown] Error al cerrar recursos: {e}")

        print("Proceso terminado.")
        sys.exit(0)
    
if __name__ == "__main__":
    if len(sys.argv) < 2 or len(sys.argv) > 3: #Comprobar argumentos
        print("Uso: python ev_cp_monitor.py <IP_BROKER:PUERTO_BROKER> <PUERTO_ENGINE/OPCIONAL>")
        sys.exit(1)
    
    puerto_engine = int(sys.argv[2]) if len(sys.argv) == 3 else EV_CP_E.PUERTO_BASE
    engine = EV_CP_E(sys.argv[1], puerto_engine)
    
    try:
        engine.run()

    except KeyboardInterrupt: #Captura de Ctrl+C para salir limpiamente
        engine.limpiar_y_salir()
        os._exit(0)

    except Exception as e:
        # Este bloque sigue siendo crucial para manejar otros errores inesperados.
        print(f"[ERROR CRÍTICO] Excepción no controlada: {e}")
        engine.limpiar_y_salir()
        