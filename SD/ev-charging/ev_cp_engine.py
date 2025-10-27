# -*- coding: utf-8 -*-

import time
import sys
import socket                                   # Importa el módulo 'socket' para la comunicación en red (ej. conexiones TCP).
import threading                                # Necesario para usar la funcionalidad de hilos
import enum                                     # Necesario para definir enumeraciones
import json                                     # Necesario para manejar datos en formato JSON
import os                                       # Necesario para operaciones del sistema (ej. verificar existencia de archivos)
from datetime import datetime                                     # Necesario para manejar tiempos y retrasos
from kafka import KafkaConsumer, KafkaProducer  # Importamos las librerías de Kafka

class MENSAJES_CP_M(enum.Enum): #ID ocupa 4 caracteres
    STATUS_E = "STATUS_E" # ST_EN#ID
    STATUS_OK = "STATUS_OK" #ST_OK#ID
    STATUS_KO = "STATUS_KO" #ST_KO#ID
    SOL_SUMINISTRO = 'SUPPLY_APPROVE'
    SUMINISTRAR = "supply_response" # SU_AU#ID
    SUMINISTRANDO = "supply_flow" # SU_IN#ID#ANONIMO#KWH#TIMESTAMP
    SOL_PARAR = 'STOP'
    PARAR = "stop_response" # ST_OP#ID
    ERROR_COMM = "ERROR_COMM" # ER_CO#ID
    ERROR_KAFKA = "ERROR_KAFKA" # ER_KA#ID


class EV_CP_E:

    PUERTO_BASE = 6000 # Atributo statico para el puerto base
    TOPICO_ACCION = "supply_flow" # Atributo statico para el tópico
    TOPICO_SUMINISTRO = "supply_response" # Atributo statico para el tópico

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
        # Aquí iría la lógica para abrir el socket del monitor.
        while True: ### CAMBIAR POR FOR LOOP CON MAX REINTENTOS 
            self.socket_monitor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket_monitor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            try:
                self.socket_monitor.bind((self.IP_E, self.PUERTO_E))
                self.socket_monitor.listen(5)
                print(f"Socket abierto en {self.IP_E}:{self.PUERTO_E}")
                EV_CP_E.PUERTO_BASE += 1  # Incrementa el puerto base para el próximo monitor
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
        # Aquí iría la lógica para abrir la conexión Kafka.
        try:
            self.producer = KafkaProducer(bootstrap_servers=[f"{self.IP_BROKER}:{self.PUERTO_BROKER}"], value_serializer=lambda v: str(v).encode('utf-8'))
            self.consumer = KafkaConsumer(EV_CP_E.TOPICO_ACCION, bootstrap_servers=[f"{self.IP_BROKER}:{self.PUERTO_BROKER}"], auto_offset_reset='latest', enable_auto_commit=True, group_id=f'engine_{self.ID}_group', value_deserializer=lambda x: x.decode('utf-8'))
            print("Conexión Kafka abierta correctamente.")
            return True
        
        except Exception as e:
            print(f"Error al abrir la conexión Kafka: {e}")
            if (hasattr(self, 'producer') and self.producer) or (hasattr(self, 'consumer') and self.consumer): 
                self.producer.close()
                self.consumer.close()
            return False

    def escuchar_monitor(self):
        
        while True:
            conexion_monitor = None
            try:
                conexion_monitor, self.IP_M = self.socket_monitor.accept()
                '''
                if conexion_monitor is None:
                    print("Socket de la central no está inicializado.")
                    return 
                '''
                
                mensaje = conexion_monitor.recv(1024).decode('utf-8').strip()
                #print(f"Mensaje recibido del monitor: {mensaje}")
                if mensaje:
                    # Aquí iría la lógica para procesar el mensaje recibido del monitor.
                    if self.ID is None:
                        self.ID = mensaje.split('#')[1]  # Asignar ID del monitor
                        #print(f"ID del engine asignado: {self.ID}")
                        self.estado = MENSAJES_CP_M.STATUS_OK.value
                        self.cargar_estado()  # Cargar estado previo si existe
                    if mensaje == MENSAJES_CP_M.STATUS_E.value+f"#{self.ID}":
                        respuesta = self.estado
                        conexion_monitor.sendall(respuesta.encode())
                        #print(f"Respuesta enviada al monitor: {respuesta}")

            except socket.error as e:
                # Si el error es 9 (Bad file descriptor), salimos del bucle.
                if e.errno == 9:
                    print(f"🛑 Hilo Monitor detenido. Socket principal cerrado.")
                    break # Detiene el bucle while True del hilo.
                else:
                    print(f"Error de socket al escuchar el monitor: {e}")

            except Exception as e:
                print(f"Error al escuchar el monitor: {e}")

            finally:
                if conexion_monitor: # Cerrar el socket si se llegó a crear el socket
                    conexion_monitor.close()

    def escuchar_central(self):

        if self.consumer is None:
            print("Consumidor Kafka no está inicializado.")
            return

        print(f"Escuchando mensajes de la central...")
        # Aquí iría la lógica para escuchar mensajes de la central.
        #while True:
        try:
            for mensaje in self.consumer:
                #mensaje_valor = mensaje.value
                mensaje_valor = json.loads(mensaje.value)
                print(f"Mensaje recibido de la central: {mensaje_valor}")
                # Aquí iría la lógica para procesar el mensaje recibido de la central.
                cp_id = mensaje_valor.get('cp_id')
                type = mensaje_valor.get('type')
                #if mensaje_valor == MENSAJES_CP_M.SUMINISTRAR.value+f"#{self.ID}":
                if cp_id == self.ID:
                    if type == MENSAJES_CP_M.SOL_SUMINISTRO.value:
                        print("Suministro autorizado por la central.")
                        '''
                        if not self.suministrar_actvio:
                            print("Iniciando suministro...")
                            self.suministrar_actvio = True
                            self.parar_suministro.clear()  # Señal para iniciar el suministro
                            self.total_kwh_suministrados = 0
                            suministrar_thread = threading.Thread(target=self.suministrar_energia, daemon=True)
                            respuesta =  {'cp_id': self.ID, 'approve': True, 'reason': 'Suministro iniciado'}
                            self.producer.send(EV_CP_E.TOPICO_SUMINISTRO, json.dumps(respuesta))
                            self.producer.flush()
                            suministrar_thread.start()
                        else:
                            print("El suministro ya está activo.")
                            #self.producer.send(EV_CP_E.TOPICO_SUMINISTRO, MENSAJES_CP_M.SUMINISTRAR.value+f"#{self.ID}#{False}")
                            respuesta =  {'cp_id': self.ID, 'approve': False, 'reason': 'Suministro ya iniciado'}
                            self.producer.send(EV_CP_E.TOPICO_SUMINISTRO, json.dumps(respuesta))
                            self.producer.flush()
                        '''
                        if self.comprobar_si_suministrar():
                            print("Iniciando suministro...")
                        else:
                            print("El suministro ya está activo.")
                            respuesta =  {'cp_id': self.ID, 'approve': False, 'reason': 'Suministro ya iniciado'}
                            self.producer.send(EV_CP_E.TOPICO_SUMINISTRO, json.dumps(respuesta))
                            self.producer.flush()

                    elif type == MENSAJES_CP_M.SOL_PARAR.value:
                        '''
                        if self.suministrar_actvio:
                            print("Suministro detenido por la central.")
                            #self.producer.send(EV_CP_E.TOPICO_SUMINISTRO, MENSAJES_CP_M.PARAR.value+f"#{self.ID}#YA_PARADO")
                            respuesta = {'cp_id': self.ID, 'approve': True, 'reason': 'Parado'}  # importante: reason 'stop' para que central lo procese correctamente
                            self.producer.send(EV_CP_E.TOPICO_SUMINISTRO, json.dumps(respuesta))
                            self.producer.flush()
                            self.parar_suministro.set()  # Señal para detener el suministro
                        else:
                            print("El suministro ya está detenido.")
                            #self.producer.send(EV_CP_E.TOPICO_SUMINISTRO, MENSAJES_CP_M.PARAR.value+f"#{self.ID}#YA_PARADO")
                            respuesta = {'cp_id': self.ID, 'approve': True, 'reason': 'Ya parado'}  # importante: reason 'stop' para que central lo procese correctamente ######################################################################3 PREGUNTAR SI MANDAR COMO TRUE O COMO FALSE, YA QUE EN AMBOS CASOS ESTARÏA PARADO
                            self.producer.send(EV_CP_E.TOPICO_SUMINISTRO, json.dumps(respuesta))
                            self.producer.flush()
                        '''
                        if self.comprobar_si_parar():
                            print("Suministro detenido por la central.")
                        else:
                            print("El suministro ya está detenido.")


        except Exception as e:
            print(f"Error al escuchar la central: {e}")

    def comprobar_si_suministrar(self):
        #if self.estado == MENSAJES_CP_M.STATUS_OK.value or not self.suministrar_actvio:
        
        mensaje = {'reason':MENSAJES_CP_M.SUMINISTRAR.value, 'cp_id': self.ID, 'approve':True}
        self.producer.send(EV_CP_E.TOPICO_SUMINISTRO, json.dumps(mensaje))
        self.producer.flush()
        for mensaje in self.consumer:
            respuesta = json.loads(mensaje.value)
            
            if cp_id == self.ID:
                if type == MENSAJES_CP_M.SOL_SUMINISTRO.value:
                    self.suministrar_actvio = True
                    self.total_kwh_suministrados = 0.0
                    self.parar_suministro.clear()
                    #self.suministrar_energia()
                    suministrar_thread = threading.Thread(target=self.suministrar_energia, daemon=True)
                    suministrar_thread.start()
                    while self.suministrar_actvio:
                        time.sleep(1)
                    return True
                return False

    def comprobar_si_parar(self):
        if self.suministrar_actvio:
            respuesta = {'cp_id': self.ID, 'approve': True, 'reason': 'Parado'}  # importante: reason 'stop' para que central lo procese correctamente
            self.producer.send(EV_CP_E.TOPICO_SUMINISTRO, json.dumps(respuesta))
            self.producer.flush()
            self.parar_suministro.set()  # Señal para detener el suministro
            return True
        else:
            return False

    def suministrar_energia(self):
        print("Suministro de energía iniciado.")
        #canidadatos_kwh = 0.0
        while not self.parar_suministro.is_set():
            # Lógica para suministrar energía
            #canidadatos_kwh += 0.1  # Simulación de suministro de energía
            self.total_kwh_suministrados += 0.1
            #print(f"Suministrando energía... Total kWh suministrados: {self.total_kwh_suministrados:.2f} kWh")
            self.guardar_estado()
            #self.producer.send(EV_CP_E.TOPICO_SUMINISTRO, MENSAJES_CP_M.SUMINISTRANDO.value+f"#{self.ID}#ANONIMO#{self.total_kwh_suministrados:.2f}#{datetime.now().strftime("%Y%m%d_%H%M%S")}")
            mensaje = {'reason': MENSAJES_CP_M.SUMINISTRANDO.value, 'cp_id': self.ID, 'driver_id':"ANONIMO", 'kwh': self.total_kwh_suministrados, 'timestamp':datetime.now().strftime("%Y%m%d_%H%M%S")}
            print(f"Mensaje enviado a la central {json.dumps(mensaje)}")
            self.producer.send(EV_CP_E.TOPICO_SUMINISTRO, json.dumps(mensaje))################################################################# Cmbiar mensahe
            self.producer.flush()
            self.parar_suministro.wait(1)
        
        print("🔌 Hilo de suministro detenido y finalizado limpiamente.")
        self.suministrar_actvio = False
        self.total_kwh_suministrados = 0.0

    def mostrar_menu(self):
        self.espera_respuesta_menu.set()
        while True:

            os.system('cls' if os.name == 'nt' else 'clear')
            
            if self.ID is None :
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
                #if not self.espera_respuesta_menu.is_set():
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
                print(f"CP {self.ID} averiado")
                self.estado = MENSAJES_CP_M.STATUS_KO.value
            else:
                print(f"CP {self.ID} reparado")
                self.estado = MENSAJES_CP_M.STATUS_OK.value

        elif switch == "3":
            if self.comprobar_si_parar():
                print("DETENIENDO EL SUMINISTRO: El punto de carga terminará de suministrar")
            else:
                #if not self.comprobar_si_suministrar():
                    #print("IMPOSIBLE_SUMINISTRAR: El punto de carga se encuentra averiado")
                if self.estado == MENSAJES_CP_M.STATUS_OK.value or not self.suministrar_actvio:
                    suministro_thread = threading.Thread(target=self.comprobar_si_suministrar, daemon=True)
                    suministro_thread.start()
                else:
                    print("IMPOSIBLE_SUMINISTRAR: El punto de carga se encuentra averiado")

        elif switch == "4":
            if engine.total_kwh_suministrados != 0.0:
                engine.guardar_estado()
            engine.producer.close()
            engine.consumer.close()
            engine.socket_monitor.close()
            print("CERRADA DE SISTEMA")
            os._exit(0)
        elif switch:
            print("Comando desconocido")
        
        self.espera_respuesta_menu.set()

    def run(self):
        print("Engine corriendo...")
        # Aquí iría la lógica principal del engine.
        if self.abrir_socket() and self.abrir_kafka():
            print("Monitor abierto correctamente.")
            #conexion_monitor, self.IP_M = self.server_socket.accept()

            listener_thread_m = threading.Thread(target=self.escuchar_monitor, daemon=True)
            listener_thread_m.start()

            listener_thread_c = threading.Thread(target=self.escuchar_central, daemon=True)
            listener_thread_c.start()

            #menu_thread = threading.Thread(target=self.mostrar_menu, daemon=True)
            #menu_thread.start()

            self.mostrar_menu()

            #while True:
                #time.sleep(1) 

    def guardar_estado(self):
        estado_info = {
            "ID": self.ID,
            "Total_kWh_Suministrados": self.total_kwh_suministrados
        }
        with open(f"estado_engine_{self.ID}.json", "w") as archivo:
            json.dump(estado_info, archivo, indent=4)
        print(f"Estado del engine guardado en estado_engine_{self.ID}.json")

    def cargar_estado(self):
        if os.path.exists(f"estado_engine_{self.ID}.json"):
            try:
                with open(f"estado_engine_{self.ID}.json", "r") as archivo:
                    estado_info = json.load(archivo)
                    self.total_kwh_suministrados = estado_info.get("Total_kWh_Suministrados", 0.0)
                    self.parar_suministro.clear()  # Asegurarse de que el suministro no esté detenido al cargar el estado
                    self.suministrar_actvio = True  # Asegurarse de que el suministro no esté activo al cargar el estado
                    print(f"Estado del engine cargado: Total kWh suministrados = {self.total_kwh_suministrados} kWh")
                    suministrar_thread = threading.Thread(target=self.suministrar_energia, daemon=True)
                    suministrar_thread.start()
                    os.remove(f"estado_engine_{self.ID}.json")  # Eliminar el archivo después de cargar el estado
            except Exception as e:
                print(f"[ERROR RESILIENCIA] Error al cargar el estado: {e}. Iniciando desde 0.")
                self.total_kwh_suministrados = 0.0
    
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
        engine.producer.close()
        engine.consumer.close()
        engine.socket_monitor.close()
        os._exit(0)
        
