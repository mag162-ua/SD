# EV_CP_M.py
import sys                     # Importa el módulo 'sys' para acceder a variables y funciones específicas del intérprete (ej. argumentos de línea de comandos).
import time                    # Importa el módulo 'time' para funciones relacionadas con el tiempo (ej. pausas con time.sleep()).
import threading               # Importa el módulo 'threading' para ejecutar tareas concurrentemente (ej. el consumidor de Kafka en un hilo separado).
import socket                  # Importa el módulo 'socket' para la comunicación en red (ej. conexiones TCP).
import threading               # Necesario para usar la funcionalidad de hilos
import enum                    # Necesario para definir enumeraciones
from faker import Faker        # Importamos la librería Faker

TIMEOUT = 4  # Tiempo de espera para las conexiones en segundos

class MENSAJES_CP_M(enum.Enum):
    REGISTER_CP = "REGISTER_CP"
    REGISTER_OK = "REGISTER_OK"
    REGISTER_KO = "REGISTER_KO"
    STATUS_E = "STATUS_E"
    STATUS_OK = "STATUS_OK"
    STATUS_KO = "STATUS_KO"
    OK_CP = "CP_OK"
    KO_CP = "CP_KO"
    ERROR_COMM = "ERROR_COMM"
    ERROR_REG = "ERROR#CP_no_registrado#SOLICITAR_REGISTRO"

class EV_CP_M:
    def __init__(self, IP_PUERTO_E, IP_PUERTO_C, ID, LOCALIZACION, KWH):
        self.IP_E, self.PUERTO_E = IP_PUERTO_E.split(':')      # Dirección IP y puerto del emulador EV
        self.IP_C, self.PUERTO_C = IP_PUERTO_C.split(':')      # Dirección IP y puerto del emulador CP
        self.ID = ID                        # Identificador del monitor 
        self.localizacion = LOCALIZACION    # Variable para almacenar la localización del monitor
        self.kwh = KWH                      # Variable para almacenar los kWh del monitor
        self.connect_engine = False          # Estado inicial del engine
        self.socket_central = None          # Socket para la comunicación con la central
        print(f"Monitor {self.ID} inicializado con IP_PUERTO_E: {IP_PUERTO_E}, IP_PUERTO_C: {IP_PUERTO_C}")

    def enviar_mensaje_socket_transitiva(self, IP, PUERTO,mensaje): # Comunicación socket transitiva, envio y cierre de socket
        print(f"Enviando mensaje transitiva a {IP}:{PUERTO} : {mensaje}")
        try: # Intentar enviar el mensaje y recibir la respuesta
            socket_t = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            socket_t.settimeout(TIMEOUT)
            socket_t.connect((IP, int(PUERTO)))
            socket_t.sendall(mensaje.encode())

            respuesta = socket_t.recv(1024).decode('utf-8').strip()
            if not respuesta:
                raise Exception("No hubo respuesta / conexión finalizada")
            return respuesta
        
        except Exception as e:
            print(f"Error {e} envio a IP: {IP}, PUERTO: {PUERTO}, MENSAJE: {mensaje}")
            return MENSAJES_CP_M.ERROR_COMM.value
        
        finally: # Asegurarse de cerrar el socket
            socket_t.close()
    
    def enviar_mensaje_socket_persistente(self, IP, PUERTO,mensaje): # Comunicación socket persistente, mantiene el socket abierto
        print(f"Enviando mensaje persistente a {IP}:{PUERTO} : {mensaje}")
        try: # Intentar enviar el mensaje y recibir la respuesta
            if self.socket_central is None: # Crear y conectar el socket si no está ya conectado
                self.socket_central = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.socket_central.connect((IP, int(PUERTO)))
            
            self.socket_central.sendall(mensaje.encode())
            respuesta = self.socket_central.recv(1024).decode('utf-8').strip()
            return respuesta
        
        except Exception as e: # Manejar errores y cerrar el socket si hay un problema
            print(f"Error {e} envio persistente a IP: {IP}, PUERTO: {PUERTO}, MENSAJE: {mensaje}")
            if self.socket_central:
                self.socket_central.close()
                self.socket_central = None
            return MENSAJES_CP_M.ERROR_COMM.value

    def registrarse_central(self): # Registro del monitor en la central
        print(f"Tratando de registrarse en la central en {self.IP_C}:{self.PUERTO_C}...")
        
        # Enviar mensaje de registro a la central
        respuesta = self.enviar_mensaje_socket_persistente(self.IP_C, self.PUERTO_C, MENSAJES_CP_M.REGISTER_CP.value+f"#{self.ID}#{self.localizacion}#{self.kwh}")
        
        # Procesar la respuesta de la central
        if respuesta == MENSAJES_CP_M.REGISTER_OK.value:
            print(f"Monitor {self.ID} registrado exitosamente en la central.")
            return True
        
        elif respuesta == "ERROR_COMM" or respuesta == MENSAJES_CP_M.REGISTER_KO.value:
            print(f"Error al registrar el monitor {self.ID} en la central: {respuesta}")
            return False
    
    def escuchar_central(self): # Escuchar mensajes de la central
        print(f"Escuchando mensajes de la central en {self.IP_C}:{self.PUERTO_C}...")

        if self.socket_central is None: # Verificar que el socket esté inicializado
            print("Socket de la central no está inicializado.")
            return

        while True: # Bucle infinito para escuchar mensajes
            try:
                mensaje = self.socket_central.recv(1024).decode('utf-8').strip()
                if mensaje: # Procesar el mensaje recibido
                    print(f"Monitor {self.ID} recibió mensaje de la central: {mensaje}")
                    if mensaje == MENSAJES_CP_M.ERROR_REG.value+f"#{self.ID}": # Solicitud de re-registro
                        print(f"Monitor {self.ID} suministrando energía...")
                        self.registrarse_central()

                else: 
                    print("Conexión cerrada por la central.")
                    self.socket_central.close()
                    self.socket_central = None
                    break
            
            except Exception as e:
                print(f"Error al recibir mensaje de la central: {e}")
                break
         
    def comprobar_estado_engine(self): # Comprobar el estado del engine periódicamente
        print(f"Comprobando estado del engine {self.ID}...")

        while True: # Bucle infinito para comprobar el estado
            respuesta = self.enviar_mensaje_socket_transitiva(self.IP_E, self.PUERTO_E, MENSAJES_CP_M.STATUS_E.value+f"#{self.ID}") #Mensaje de estado al engine

            # Procesar la respuesta del engine
            if respuesta == MENSAJES_CP_M.STATUS_OK.value: #Respuesta exitosa del engine
                print(f"Monitor {self.ID} recibió estado OK del engine.")
                self.enviar_mensaje_socket_transitiva(self.IP_C, self.PUERTO_C, MENSAJES_CP_M.OK_CP.value+f"#{self.ID}") #Notificación de restablecimiento a la central
                self.connect_engine = True

            elif respuesta == MENSAJES_CP_M.ERROR_COMM.value or respuesta == MENSAJES_CP_M.STATUS_KO.value: #Respuesta de error del engine
                print(f"Monitor {self.ID} recibió estado ERROR del engine: {respuesta}")
                self.enviar_mensaje_socket_transitiva(self.IP_C, self.PUERTO_C, MENSAJES_CP_M.KO_CP.value+f"#{self.ID}") #Notificación de fallo a la central
                print("Reintentando conexión al engine...")
                self.connect_engine = False
            
            time.sleep(1) # Esperar antes de la siguiente comprobación

    def run(self): 
        print(f"Monitor {self.ID} corriendo...")

        if self.registrarse_central():

            print(f"Monitor {self.ID} activo. Iniciando hilos concurrentes.")
            
             # HILO 1: Comprobar estado del Engine
            check_thread = threading.Thread(target=self.comprobar_estado_engine, daemon=True)
            check_thread.start()
            
            # HILO 2: Escucha de la Central
            listener_thread = threading.Thread(target=self.escuchar_central, daemon=True)
            listener_thread.start()
            

            self.comprobar_estado_engine()

            while True:
                time.sleep(1) 
        else:
            print("Fallo en el registro inicial. El Monitor se cerrará.")
        

if __name__ == "__main__":
    if len(sys.argv) != 4: # Comprobar argumentos
        print("Uso: python ev_cp_monitor.py <IP_ENGINE:PUERTO_ENGINE> <IP_CENTRAL:PUERTO_CENTRAL> <ID>")
        sys.exit(1)
    
    faker = Faker()
    monitor = EV_CP_M(sys.argv[1], sys.argv[2], sys.argv[3], faker.city(), float(faker.random_number(digits=2, fix_len=True))/100)

    try:
        monitor.run()
    except KeyboardInterrupt:
        print("Monitor detenido por el usuario.")
        sys.exit(0)
