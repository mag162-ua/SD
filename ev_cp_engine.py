from kafka import KafkaConsumer, KafkaProducer
import time
import sys
import socket                  # Importa el módulo 'socket' para la comunicación en red (ej. conexiones TCP).
import threading               # Necesario para usar la funcionalidad de hilos

class EV_CP_E:

    PUERTO_BASE = 5000 # Atributo statico para el puerto base

    def __init__(self, IP_PUERTO_BROKER):
        self.ID = None
        self.IP_BROKER, self.PUERTO_BROKER = IP_PUERTO_BROKER.split(':')
        self.IP_E = "0.0.0.0"
        self.PUERTO_E = EV_CP_E.PUERTO_BASE
        self.suministrar = False
        self.socket_monitor = None
        self.IP_M = None
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
                print(f"Error al abrir el socket: {e}. Reintentando en 5 segundos...")
                self.socket_monitor.close()
                self.PUERTO_E += 1
                EV_CP_E.PUERTO_BASE += 1

            except Exception as e:
                print(f"Error al abrir el socket: {e}")
                self.socket_monitor.close()
                return False

    def escuchar_monitor(self, conexion_monitor):

        if conexion_monitor is None:
            print("Socket de la central no está inicializado.")
            return 
        while True:
            
        #try:
            mensaje = self.socket_monitor.recv(1024).decode('utf-8').strip()
            print(f"Mensaje recibido del monitor: {mensaje}")


    def run(self):
        print("Engine corriendo...")
        # Aquí iría la lógica principal del engine.
        if self.abrir_socket():
            print("Monitor abierto correctamente.")
            while True:
                conexion_monitor, self.IP_M = self.server_socket.accept()

                check_thread = threading.Thread(target=self.escuchar_monitor, daemon=True, args=(con))
                check_thread.start()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python ev_cp_monitor.py <IP_BROKER:PUERTO_BROKER>")
        sys.exit(1)

    engine = EV_CP_E(sys.argv[1])

    try:
        engine.run()
    except KeyboardInterrupt:
        print("Engine detenido. Ctrl+C detectado. Saliendo...")
        sys.exit(0)
        