# EV_CP_M.py
import os                      # Importa el módulo 'os' para interactuar con el sistema operativo (ej. variables de entorno).
import sys                     # Importa el módulo 'sys' para acceder a variables y funciones específicas del intérprete (ej. argumentos de línea de comandos).
import time                    # Importa el módulo 'time' para funciones relacionadas con el tiempo (ej. pausas con time.sleep()).
import threading               # Importa el módulo 'threading' para ejecutar tareas concurrentemente (ej. el consumidor de Kafka en un hilo separado).
import socket                  # Importa el módulo 'socket' para la comunicación en red (ej. conexiones TCP).
import threading               # Necesario para usar la funcionalidad de hilos
import enum                    # Necesario para definir enumeraciones
from faker import Faker        # Importamos la librería Faker
import random
import requests
if os.name != 'nt':
    import select
    import tty
    import termios
else:
    import msvcrt

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

    REGISTRY_URL=os.getenv('REGISTRY_URL', "https://registry:8080")  # URL del registro desde variable de entorno
    RUTA_CIUDADES = "/app/ciudades/"  # Ruta de las ciudades, se asume que está en el contenedor

    def __init__(self, IP_PUERTO_E, IP_PUERTO_C, ID, LOCALIZACION, KWH):
        self.IP_E, self.PUERTO_E = IP_PUERTO_E.split(':')      # Dirección IP y puerto del emulador EV
        self.IP_C, self.PUERTO_C = IP_PUERTO_C.split(':')      # Dirección IP y puerto del emulador CP
        self.ID = ID                        # Identificador del monitor 
        self.localizacion = LOCALIZACION    # Variable para almacenar la localización del monitor
        self.kwh = KWH                      # Variable para almacenar los kWh del monitor
        self.connect_engine = False          # Estado inicial del engine
        self.socket_central = None          # Socket para la comunicación con la central
        self.autorizado = False             # Estado de autorización inicial
        self.clave_acceso = ""             # Clave secreta para la comunicación segura
        print(f"Monitor {self.ID} inicializado con IP_PUERTO_E: {IP_PUERTO_E}, IP_PUERTO_C: {IP_PUERTO_C}")
    
    def ciudad():
        if os.path.exists(f"{EV_CP_M.RUTA_CIUDADES}ciudades.txt"):
            try:
                with open(f"{EV_CP_M.RUTA_CIUDADES}ciudades.txt", "r") as archivo:
                    ciudades = [linea.strip() for linea in archivo if linea.strip()]  # Leer ciudades del archivo
                    if not ciudades:
                        print("No se encontraron ciudades en el archivo.")
                        return "Desconocida"
                    ciudad = random.choice(ciudades)  # Selecciona una ciudad aleatoria
                    print(f"Ciudad seleccionada: {ciudad}")
                    return ciudad
            except Exception as e:
                print(f"Error al cargar las ciudades: {e}")
                return "Desconocida"
        else:
            print(f"Archivo de ciudades no encontrado en {EV_CP_M.RUTA_CIUDADES}ciudades.json")
            return "Desconocida"

    def dar_de_alta(self):
        # Lógica para obtener la clave de acceso del registro
        if not EV_CP_M.REGISTRY_URL:
                print("ERROR: La variable de entorno REGISTRY_URL no está configurada.")
                return None
        
        registry_endpoint = f"{EV_CP_M.REGISTRY_URL}/register"  
        datos = {
            "id": self.ID,
            "socket_ip": os.getenv('HOSTNAME'),
        }

        try:

            print(f"Intentando registrar el CP {self.ID} en el Registry {EV_CP_M.REGISTRY_URL}...")

            response = requests.post(registry_endpoint, json=datos, timeout=TIMEOUT, verify=False)
            response.raise_for_status()

            data = response.json()
            secret_key = data.get('secret_key') #### PREGUNTAR AL PROFESOR SI ESTO VA AQUI O EN OTRO SITIO

            if secret_key:
                print(f"Clave secreta obtenida del Registry para {self.ID}.")
                self.clave_acceso = secret_key
                return True
            else:
                print("Error: El Registry no devolvió la 'secret_key' en la respuesta.")
                return False

        except requests.exceptions.RequestException as e:
            print(f"ERROR: Fallo de comunicación con el Registry ({registry_endpoint}): {e}")
            return False 

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
                    #ERROR#CP_no_registrado#SOLICITAR_REGISTRO#{cp_id}
                    #ERROR_REG = "ERROR#CP_no_registrado#SOLICITAR_REGISTRO"
                    if mensaje == MENSAJES_CP_M.ERROR_REG.value+f"#{self.ID}": # Solicitud de re-registro
                        print(f"Monitor {self.ID} vuelve a registrarse...")
                        if self.dar_de_alta():
                            self.registrarse_central()
                        

                else: 
                    print("Conexión cerrada por la central.")
                    self.socket_central.close()
                    self.socket_central = None
                    break
            
            except Exception as e:
                print(f"Error al recibir mensaje de la central: {e}")
                break
         
    def comprobar_estado_engine(self): 
        print(f"🔍 Iniciando comprobación de estado del Engine {self.ID}...")
        
        socket_engine = None

        while True:
            try:
                # 1. Intentar conectar si no hay socket
                if socket_engine is None:
                    try:
                        print(f"🔌 Conectando al Engine en {self.IP_E}:{self.PUERTO_E}...")
                        socket_engine = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        socket_engine.settimeout(5) # Timeout razonable
                        socket_engine.connect((self.IP_E, int(self.PUERTO_E)))
                        print("✅ Conectado al Engine.")
                    except Exception as e:
                        print(f"❌ Error de conexión con Engine: {e}")
                        socket_engine = None
                        self.enviar_mensaje_socket_transitiva(self.IP_C, self.PUERTO_C, MENSAJES_CP_M.KO_CP.value+f"#{self.ID}")
                        time.sleep(2)
                        continue # Reintentar en el siguiente ciclo

                # 2. Enviar petición de estado
                msg = MENSAJES_CP_M.STATUS_E.value + f"#{self.ID}"
                socket_engine.sendall(msg.encode())

                # 3. Recibir respuesta
                respuesta = socket_engine.recv(1024).decode('utf-8').strip()

                if not respuesta:
                    raise ConnectionResetError("Conexión cerrada por el Engine")

                # 4. Procesar respuesta
                if respuesta == MENSAJES_CP_M.STATUS_OK.value:
                    # Solo notificar a central si antes estaba desconectado/error (para no saturar central)
                    #if not self.connect_engine: 
                    self.enviar_mensaje_socket_transitiva(self.IP_C, self.PUERTO_C, MENSAJES_CP_M.OK_CP.value+f"#{self.ID}")
                    self.connect_engine = True
                    # Opcional: print solo para debug, comentar para producción
                    # print(f"Estado Engine OK") 

                elif respuesta == MENSAJES_CP_M.STATUS_KO.value:
                    print(f"🚨 Engine reporta AVERÍA")
                    self.enviar_mensaje_socket_transitiva(self.IP_C, self.PUERTO_C, MENSAJES_CP_M.KO_CP.value+f"#{self.ID}")
                    self.connect_engine = True # Hay conexión, pero el engine dice que está KO

            except (socket.timeout, socket.error, ConnectionResetError, BrokenPipeError) as e:
                print(f"⚠️ Pérdida de conexión con Engine: {e}")
                if socket_engine:
                    socket_engine.close()
                socket_engine = None
                self.connect_engine = False
                # Notificar a la central que el CP no responde
                self.enviar_mensaje_socket_transitiva(self.IP_C, self.PUERTO_C, MENSAJES_CP_M.KO_CP.value+f"#{self.ID}")
            
            except Exception as e:
                print(f"❌ Error inesperado en monitor: {e}")
                if socket_engine:
                    socket_engine.close() 
                socket_engine = None

            # Espera entre comprobaciones (Heartbeat)
            time.sleep(1)


    #def enviar_heartbeat_central(self):
        # 4 segundos es seguro, ya que el timeout de la Central es de 10s
    #    HEARTBEAT_INTERVAL = 4 
        
    #    print(f"💓 Heartbeat a Central iniciado (cada {HEARTBEAT_INTERVAL}s)...")
        
    #    while True:
            # El mensaje de estado a la Central es CP_OK#<ID>
    #        msg = MENSAJES_CP_M.STATUS_OK.value + f"#{self.ID}"
            
    #        try:
                # Usar la función transaccional para la Central (abrir, enviar, cerrar socket)
                # Esto es correcto, ya que la Central está diseñada para manejar conexiones rápidas y transitorias.
    #            self.enviar_mensaje_socket_transitiva(self.IP_C, self.PUERTO_C, msg)
    #        except Exception as e:
                # Si falla el heartbeat, no es crítico, pero se registra
    #            print(f"❌ Error enviando Heartbeat a Central: {e}")
            
    #        time.sleep(HEARTBEAT_INTERVAL)

    def mostrar_menu(self): #Menú interactivo del Engine
        while True:

            os.system('cls' if os.name == 'nt' else 'clear') #Limpiar pantalla
            
            print(f"\n--- Menú del monitor {self.ID} - {self.ciudad} - {self.kwh}€/KWh---")
            if self.autorizado:
                print("1. Dar de baja el CP") #Opción para desautorizar
            else:
                print("1. Dar de alta el CP") #Opción para autorizar

            print("2. Salir") #Salir del menú
            print("---------------------------")

            respuesta = None # Esperar entrada sin bloquear
            if os.name == 'nt': # Solo intentar usar msvcrt en Windows
                if msvcrt.kbhit():
                    respuesta = msvcrt.getch().decode()
            else: # En sistemas Unix, usar select
                if select.select([sys.stdin], [], [], 0) == ([sys.stdin], [], []):
                    # Si hay datos, leemos la línea completa.
                    respuesta = sys.stdin.readline().strip()
            self.responder_menu(respuesta)
            time.sleep(3)
    
    def responder_menu(self, respuesta): # Responder a la entrada del menú
        if respuesta == '1':
            if self.autorizado:
                print(f"CP {self.ID} dado de baja...")
                self.autorizado = False
            else:
                print(f"CP {self.ID} dado de alta...")
                # LLAMAR A REGISTRY PARA DAR DE ALTA EL CP
                if self.dar_de_alta():
                    if self.registrarse_central():
                        self.autorizado = True
                        
                        listener_thread = threading.Thread(target=self.escuchar_central, daemon=True)
                        listener_thread.start()

        elif respuesta == '2':
            print("Saliendo del menú...")
            self.limpiar_y_salir()
            return
        else:
            if respuesta is not None:
                print("Opción no válida. Intente de nuevo.")

    def limpiar_y_salir(self): #Limpiar recursos y salir del programa
        print("\nIniciando limpieza de recursos...")
        try: # Cerrar el socket de la central
            if hasattr(self, 'socket_central') and self.socket_central:
                self.socket_central.close()
        except Exception as e:
            print(f"[Monitor Shutdown] Error al cerrar recursos: {e}")

        print("Proceso terminado.")
        sys.exit(0)

    def run(self): 
        print(f"Monitor {self.ID} corriendo...")
        '''
        if self.registrarse_central():
            print(f"Monitor {self.ID} activo. Iniciando hilos concurrentes.")
            
            # HILO 1: Comprobar estado del Engine
            # Nota: comprobar_estado_engine tiene un while True, así que el hilo vivirá por siempre
            check_thread = threading.Thread(target=self.comprobar_estado_engine, daemon=True)
            check_thread.start()
            
            # HILO 2: Escucha de la Central
            listener_thread = threading.Thread(target=self.escuchar_central, daemon=True)
            listener_thread.start()
            
            #heartbeat_thread = threading.Thread(target=self.enviar_heartbeat_central, daemon=True)
            #heartbeat_thread.start()

            # Bucle principal para mantener el programa vivo
            while True:
                time.sleep(1) 
        else:
            print("Fallo en el registro inicial. El Monitor se cerrará.")
        '''
        check_thread = threading.Thread(target=self.comprobar_estado_engine, daemon=True)
        check_thread.start()

        #menu_thread = threading.Thread(target=self.mostrar_menu, daemon=True)
        #menu_thread.start()

        while True:
            self.mostrar_menu()
            #time.sleep(1)

if __name__ == "__main__":
    if len(sys.argv) != 4: # Comprobar argumentos
        print("Uso: python ev_cp_monitor.py <IP_ENGINE:PUERTO_ENGINE> <IP_CENTRAL:PUERTO_CENTRAL> <ID>")
        sys.exit(1)
    
    #faker = Faker()
    monitor = EV_CP_M(sys.argv[1], sys.argv[2], sys.argv[3], EV_CP_M.ciudad(), round(random.random(), 2))

    try:
        monitor.run()
    except KeyboardInterrupt:
        print("Monitor detenido por el usuario.")
        sys.exit(0)
