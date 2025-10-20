#!/usr/bin/env python3
# ev_central.py

import os
import sys
import socket
import threading
import json
import time
import logging
from datetime import datetime
from typing import Dict, List, Optional

# Configuración de logging MEJORADA para Docker
def setup_logging():
    """Configuración robusta de logging con soporte para Docker"""
    # Usar variable de entorno o valor por defecto
    log_dir = os.getenv('LOG_DIR', '/app/logs')
    
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
        print(f"📁 Directorio de logs creado: {log_dir}")
    
    # Formato de logs
    log_format = '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
    
    # Configurar logger principal
    logger = logging.getLogger('EV_Central')
    logger.setLevel(logging.DEBUG)
    
    # Evitar logs duplicados
    if logger.hasHandlers():
        logger.handlers.clear()
    
    # Handler para archivo
    log_file = os.path.join(log_dir, 'central.log')
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(log_format)
    file_handler.setFormatter(file_formatter)
    
    # Handler para consola
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    
    # Agregar handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    print(f"✅ Logging configurado - Archivo: {log_file}")
    return logger

# Inicializar logger
logger = setup_logging()

class ChargingPoint:
    """Clase que representa un punto de carga"""
    
    def __init__(self, cp_id: str, location: str, price_per_kwh: float):
        self.cp_id = cp_id
        self.location = location
        self.price_per_kwh = price_per_kwh
        self.status = "DESCONECTADO"
        self.current_consumption = 0.0
        self.current_amount = 0.0
        self.driver_id = None
        self.last_heartbeat = None
        self.socket_connection = None
        self.total_energy_supplied = 0.0
        self.total_revenue = 0.0
        self.registration_date = datetime.now().isoformat()
    
    def parse(self):
        """Convierte el CP a diccionario para serialización"""
        return {
            'cp_id': self.cp_id,
            'location': self.location,
            'price_per_kwh': self.price_per_kwh,
            'status': self.status,
            'current_consumption': self.current_consumption,
            'current_amount': self.current_amount,
            'driver_id': self.driver_id,
            'last_heartbeat': self.last_heartbeat.isoformat() if self.last_heartbeat else None,
            'total_energy_supplied': self.total_energy_supplied,
            'total_revenue': self.total_revenue,
            'registration_date': self.registration_date
        }
    
    @classmethod
    def unparse(cls, data: dict):
        """Crea un ChargingPoint desde un diccionario"""
        cp = cls(data['cp_id'], data['location'], data['price_per_kwh'])
        cp.status = data['status']
        cp.current_consumption = data['current_consumption']
        cp.current_amount = data['current_amount']
        cp.driver_id = data['driver_id']
        cp.total_energy_supplied = data.get('total_energy_supplied', 0.0)
        cp.total_revenue = data.get('total_revenue', 0.0)
        cp.registration_date = data.get('registration_date', datetime.now().isoformat())
        
        if data['last_heartbeat']:
            cp.last_heartbeat = datetime.fromisoformat(data['last_heartbeat'])
        
        return cp

class DatabaseManager:
    """Gestor de base de datos con persistencia en archivo JSON"""
    
    def __init__(self, data_file: str = None):
        # Usar variable de entorno o valor por defecto
        if data_file is None:
            data_file = os.getenv('DATA_FILE', '/app/ev_central_data.json')
        
        self.data_file = data_file
        self.charging_points: Dict[str, ChargingPoint] = {}
        self.drivers = set()
        self.transactions = []
        
        # Crear directorio del archivo de datos si no existe
        data_dir = os.path.dirname(self.data_file)
        if data_dir and not os.path.exists(data_dir):
            os.makedirs(data_dir)
            logger.info(f"📁 Directorio de datos creado: {data_dir}")
        
        # Cargar datos al iniciar
        self.load_data()
    
    def load_data(self):
        """Carga los datos desde el archivo JSON"""
        try:
            if os.path.exists(self.data_file):
                with open(self.data_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Cargar puntos de carga
                for cp_data in data.get('charging_points', []):
                    cp = ChargingPoint.unparse(cp_data)
                    self.charging_points[cp.cp_id] = cp
                
                # Cargar conductores
                self.drivers = set(data.get('drivers', []))
                
                # Cargar transacciones
                self.transactions = data.get('transactions', [])
                
                logger.info(f"✅ Datos cargados desde {self.data_file}: {len(self.charging_points)} CPs, {len(self.drivers)} conductores")
            else:
                logger.info("📁 Archivo de datos no encontrado. Se iniciará con datos vacíos.")
                self.initialize_sample_data()
                
        except Exception as e:
            logger.error(f"❌ Error cargando datos: {e}", exc_info=True)
            self.initialize_sample_data()
    
    def save_data(self):
        """Guarda los datos en el archivo JSON"""
        try:
            data = {
                'charging_points': [],
                'drivers': list(self.drivers),
                'transactions': self.transactions,
                'last_save': datetime.now().isoformat()
            }
            
            for cp in self.charging_points.values():
                cp_data = cp.parse()
                data['charging_points'].append(cp_data)
            
            with open(self.data_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"💾 Datos guardados en {self.data_file}")
            
        except Exception as e:
            logger.error(f"❌ Error guardando datos: {e}", exc_info=True)
    
    def initialize_sample_data(self):
        """Inicializa datos de ejemplo si no hay archivo"""
        sample_cps = [
            ChargingPoint("CP001", "Plaza Mayor", 0.45),
            ChargingPoint("CP002", "Avenida Central", 0.42),
            ChargingPoint("CP003", "Estación Norte", 0.48)
        ]
        
        for cp in sample_cps:
            self.add_charging_point(cp)
        
        logger.info("🔧 Datos de ejemplo inicializados")
    
    def add_charging_point(self, cp: ChargingPoint):
        self.charging_points[cp.cp_id] = cp
        logger.info(f"➕ Punto de carga {cp.cp_id} agregado a la BD")
        self.save_data()
    
    def get_charging_point(self, cp_id: str) -> Optional[ChargingPoint]:
        return self.charging_points.get(cp_id)
    
    def get_all_cps(self) -> List[dict]:
        return [cp.parse() for cp in self.charging_points.values()]
    
    def register_driver(self, driver_id: str):
        self.drivers.add(driver_id)
        logger.info(f"👤 Conductor {driver_id} registrado")
        self.save_data()
    
    def add_transaction(self, transaction_data: dict):
        """Añade una transacción al historial"""
        transaction_data['timestamp'] = datetime.now().isoformat()
        self.transactions.append(transaction_data)
    
    def backup_data(self, backup_file: str = None):
        """Crea una copia de seguridad de los datos"""
        if backup_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_dir = os.getenv('BACKUP_DIR', '/app/backups')
            if not os.path.exists(backup_dir):
                os.makedirs(backup_dir)
            backup_file = os.path.join(backup_dir, f"ev_central_backup_{timestamp}.json")
        
        try:
            self.save_data()
            import shutil
            shutil.copy2(self.data_file, backup_file)
            logger.info(f"📦 Copia de seguridad creada: {backup_file}")
            return True
        except Exception as e:
            logger.error(f"❌ Error creando copia de seguridad: {e}", exc_info=True)
            return False

class SimpleKafkaManager:
    """Simulador de Kafka para desarrollo"""
    
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self.topics = {
            'cp_registrations': [],
            'cp_status_updates': [],
            'central_responses': [],
            'driver_requests': [],
            'driver_responses': [],
            'central_updates': [],
            'control_commands': [],
            'supply_flow': [],
        }
        logger.info(f"🔌 Kafka simulado en {bootstrap_servers}")
    
    def send_message(self, topic: str, message: dict):
        """Envía mensaje a topic simulado"""
        if topic in self.topics:
            self.topics[topic].append({
                'timestamp': datetime.now(),
                'message': message
            })
            logger.debug(f"📤 Mensaje simulado enviado a {topic}: {message}")
        else:
            logger.warning(f"⚠️ Topic {topic} no existe")
    
    def get_messages(self, topic: str, consumer_group: str = None):
        """Obtiene mensajes de un topic"""
        if topic in self.topics:
            messages = self.topics[topic].copy()
            self.topics[topic].clear()
            if messages:
                logger.debug(f"📥 Mensajes consumidos de {topic}: {len(messages)}")
            return messages
        return []
    
    def peek_messages(self, topic: str):
        """Mira los mensajes sin consumirlos"""
        if topic in self.topics:
            return self.topics[topic].copy()
        return []

class SocketServer:
    """Servidor de sockets para comunicación directa"""
    
    def __init__(self, host: str, port: int, central):
        self.host = host
        self.port = port
        self.central = central
        self.socket = None
        self.running = False
    
    def start(self):
        """Inicia el servidor de sockets"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.socket.bind((self.host, self.port))
            self.socket.listen(5)
            self.running = True
            
            logger.info(f"🔌 Servidor socket iniciado en {self.host}:{self.port}")
            
            while self.running:
                client_socket, address = self.socket.accept()
                client_thread = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket, address)
                )
                client_thread.daemon = True
                client_thread.start()
                
        except Exception as e:
            logger.error(f"❌ Error en servidor socket: {e}", exc_info=True)
    
    def handle_client(self, client_socket, address):
        """Maneja conexiones de clientes"""
        try:
            logger.info(f"🔗 Conexión establecida desde {address}")
            
            while True:
                data = client_socket.recv(1024).decode('utf-8')
                if not data:
                    break
                
                self.process_message(data, client_socket)
                
        except Exception as e:
            logger.error(f"❌ Error manejando cliente {address}: {e}", exc_info=True)
        finally:
            client_socket.close()
            logger.info(f"🔌 Conexión cerrada con {address}")
    
    def process_message(self, message: str, client_socket):
        """Procesa mensajes recibidos por socket"""
        try:
            if message.startswith('REGISTER_CP'):
                parts = message.split('#')
                self.handle_cp_registration(parts[1:], client_socket)
            elif message.startswith('CP_OK'):
                parts = message.split('#')
                self.handle_cp_ok(parts[1:])
            elif message.startswith('CP_KO'):
                parts = message.split('#')
                self.handle_cp_failure(parts[1:], client_socket)
            else:
                logger.warning(f"⚠️ Mensaje desconocido: {message}")
            
        except Exception as e:
            logger.error(f"❌ Error procesando mensaje: {e}", exc_info=True)
    
    def handle_cp_registration(self, params: List[str], client_socket):
        """Maneja registro de puntos de carga"""
        if len(params) < 3:
            logger.error("❌ Parámetros insuficientes para registro")
            return
            
        cp_id = params[0]
        location = params[1]
        price = float(params[2])
        
        cp = ChargingPoint(cp_id, location, price)
        cp.socket_connection = client_socket
        cp.last_heartbeat = datetime.now()
        
        self.central.database.add_charging_point(cp)
        self.central.update_cp_status(cp_id, "DESCONECTADO")
        self.central.database.save_data()
        
        response = "REGISTER_OK"
        client_socket.send(response.encode('utf-8'))
        
        logger.info(f"✅ Punto de carga {cp_id} registrado correctamente")
    
    def handle_cp_ok(self, params: List[str]):
        """Maneja ok de puntos de carga"""
        if params:
            cp_id = params[0]
            cp = self.central.database.get_charging_point(cp_id)
            if cp:
                cp.last_heartbeat = datetime.now()
                if cp.status == "DESCONECTADO":
                    self.central.update_cp_status(cp_id, "ACTIVADO")
                    logger.info(f"🔄 CP {cp_id} reactivado - Estado cambiado a ACTIVADO")
                elif cp.status == "AVERIADO":
                    self.central.update_cp_status(cp_id, "ACTIVADO")
                    logger.info(f"🔄 CP {cp_id} reactivado - Estado anterior AVERIADO cambiado a ACTIVADO")
                
                logger.debug(f"💓 Heartbeat recibido de {cp_id}")
            
    def handle_cp_failure(self, params: List[str], client_socket):
        """Maneja mensajes de avería CP_KO"""
        if not params:
            logger.error("❌ Parámetros insuficientes para mensaje CP_KO")
            return
            
        cp_id = params[0]
        reason = params[1] if len(params) > 1 else "Avería no especificada"
        
        logger.warning(f"⚠️ CP {cp_id} reporta avería: {reason}")
        
        cp = self.central.database.get_charging_point(cp_id)
        if cp:
            was_supplying = cp.status == "SUMINISTRANDO"
            driver_id = cp.driver_id
            
            self.central.update_cp_status(cp_id, "AVERIADO")
            
            if was_supplying and driver_id:
                self.central.record_failed_transaction(cp, reason, driver_id)
                logger.info(f"🔌 Suministro interrumpido para conductor {driver_id} por avería en CP {cp_id}")
            
            response = f"CP_KO_ACK#{cp_id}"
            client_socket.send(response.encode('utf-8'))
            
            logger.info(f"🔴 CP {cp_id} puesto en estado AVERIADO")
        else:
            logger.error(f"❌ CP {cp_id} no encontrado para mensaje CP_KO")
            response = f"ERROR#CP_no_encontrado"
            client_socket.send(response.encode('utf-8'))

class EVCentral:
    """Clase principal del sistema central con persistencia"""
    
    def __init__(self, socket_host: str, socket_port: int, kafka_servers: str, data_file: str = None):
        self.database = DatabaseManager(data_file)
        self.kafka_manager = SimpleKafkaManager(kafka_servers)
        self.socket_server = SocketServer(socket_host, socket_port, self)
        self.auto_save_interval = 300
        self.running = False
        
        self.setup_signal_handlers()
        self.start_kafka_consumer()
    
    def setup_signal_handlers(self):
        """Configura manejadores para señales de terminación"""
        import signal
        try:
            signal.signal(signal.SIGINT, self.graceful_shutdown)
            signal.signal(signal.SIGTERM, self.graceful_shutdown)
        except AttributeError:
            pass
    
    def graceful_shutdown(self, signum=None, frame=None):
        """Cierre graceful guardando todos los datos"""
        logger.info("🛑 Iniciando apagado graceful...")
        self.running = False
        self.database.save_data()
        logger.info("💾 Datos guardados correctamente")
        self.database.backup_data()
        logger.info("✅ EV_Central apagado correctamente")
        sys.exit(0)
    
    def start(self):
        """Inicia todos los servicios del sistema central"""
        logger.info("🚀 Iniciando EV_Central...")
        self.running = True
        
        socket_thread = threading.Thread(target=self.socket_server.start)
        socket_thread.daemon = True
        socket_thread.start()
        
        heartbeat_thread = threading.Thread(target=self.monitor_heartbeats)
        heartbeat_thread.daemon = True
        heartbeat_thread.start()
        
        autosave_thread = threading.Thread(target=self.auto_save)
        autosave_thread.daemon = True
        autosave_thread.start()
        
        self.show_control_panel()
        self.handle_console_input()
        
        logger.info("✅ EV_Central iniciado correctamente")
    
    def show_control_panel(self):
        """Muestra el panel de control en consola"""
        print("\n" + "="*80)
        print("EV_Central - Panel de Control")
        print("="*80)
        print("Estados: 🟢 ACTIVADO | 🟠 PARADO | 🔵 SUMINISTRANDO | 🔴 AVERIADO | ⚫ DESCONECTADO")
        print("="*80)
        
        def update_panel():
            while self.running:
                self.display_status()
                time.sleep(5)
        
        panel_thread = threading.Thread(target=update_panel)
        panel_thread.daemon = True
        panel_thread.start()
    
    def display_status(self):
        """Muestra el estado actual del sistema"""
        os.system('cls' if os.name == 'nt' else 'clear')
        print("\n" + "="*80)
        print("EV_Central - Estado del Sistema")
        print("="*80)
        print(f"{'ID':<10} {'Ubicación':<20} {'Estado':<15} {'Caudal':<10} {'Importe':<10} {'Conductor'}")
        print("-"*80)
        
        for cp in self.database.charging_points.values():
            status_icon = {
                "ACTIVADO": "🟢 ACTIVADO",
                "PARADO": "🟠 PARADO", 
                "SUMINISTRANDO": "🔵 SUMINISTRANDO",
                "AVERIADO": "🔴 AVERIADO",
                "DESCONECTADO": "⚫ DESCONECTADO"
            }.get(cp.status, "⚫ DESCONECTADO")
            
            if cp.status == "SUMINISTRANDO" and cp.current_consumption > 0:
                caudal = f"{cp.current_consumption:.1f}kW"
            else:
                caudal = "-"
                
            importe = f"€{cp.current_amount:.2f}" if cp.current_amount > 0 else "-"
            conductor = cp.driver_id if cp.driver_id else "-"
            
            print(f"{cp.cp_id:<10} {cp.location:<20} {status_icon:<15} {caudal:<10} {importe:<10} {conductor}")
        
        print("="*80)
        print("Comandos disponibles:")
        print("  stop <cp_id>    - Parar un punto de carga")
        print("  resume <cp_id>  - Reanudar un punto de carga")  
        print("  status         - Mostrar estado actual")
        print("  stats          - Mostrar estadísticas")
        print("  save           - Guardar datos manualmente")
        print("  backup         - Crear copia de seguridad")
        print("  quit           - Salir")
        print("="*80)
    
    def handle_console_input(self):
        """Maneja la entrada por consola"""
        while self.running:
            try:
                command = input("\nComando: ").strip().lower()
                
                if command == 'quit' or command == 'exit':
                    self.graceful_shutdown()
                elif command == 'status':
                    self.display_status()
                elif command == 'save':
                    self.database.save_data()
                    print("✅ Datos guardados manualmente")
                elif command == 'backup':
                    if self.database.backup_data():
                        print("✅ Copia de seguridad creada")
                    else:
                        print("❌ Error creando copia de seguridad")
                elif command == 'stats':
                    self.show_statistics()
                elif command.startswith('stop '):
                    cp_id = command[5:].strip().upper()
                    self.send_control_command(cp_id, "STOP")
                elif command.startswith('resume '):
                    cp_id = command[7:].strip().upper()
                    self.send_control_command(cp_id, "RESUME")
                else:
                    print("❌ Comando no reconocido. Use 'stop <cp_id>', 'resume <cp_id>', 'status', 'stats', 'save', 'backup' o 'quit'")
                    
            except Exception as e:
                logger.error(f"❌ Error procesando comando: {e}", exc_info=True)
    
    def show_statistics(self):
        """Muestra estadísticas del sistema"""
        total_cps = len(self.database.charging_points)
        active_cps = sum(1 for cp in self.database.charging_points.values() 
                        if cp.status in ["ACTIVADO", "SUMINISTRANDO"])
        supplying_cps = sum(1 for cp in self.database.charging_points.values() 
                           if cp.status == "SUMINISTRANDO")
        total_energy = sum(cp.total_energy_supplied for cp in self.database.charging_points.values())
        total_revenue = sum(cp.total_revenue for cp in self.database.charging_points.values())
        
        print("\n" + "="*50)
        print("ESTADÍSTICAS DEL SISTEMA")
        print("="*50)
        print(f"Total puntos de carga: {total_cps}")
        print(f"Puntos activos: {active_cps}")
        print(f"Puntos suministrando: {supplying_cps}")
        print(f"Energía total suministrada: {total_energy:.2f} kWh")
        print(f"Ingresos totales: €{total_revenue:.2f}")
        print(f"Conductores registrados: {len(self.database.drivers)}")
        print(f"Transacciones registradas: {len(self.database.transactions)}")
        print("="*50)
    
    def auto_save(self):
        """Guarda los datos automáticamente cada cierto tiempo"""
        while self.running:
            time.sleep(self.auto_save_interval)
            try:
                self.database.save_data()
                logger.debug("💾 Guardado automático realizado")
            except Exception as e:
                logger.error(f"❌ Error en guardado automático: {e}", exc_info=True)
    
    def monitor_heartbeats(self):
        """Monitoriza los heartbeats de los CPs"""
        while self.running:
            time.sleep(30)
            
            current_time = datetime.now()
            for cp in self.database.charging_points.values():
                if cp.last_heartbeat:
                    time_diff = (current_time - cp.last_heartbeat).total_seconds()
                    if time_diff > 60:
                        if cp.status != "DESCONECTADO" and cp.status != "PARADO":
                            self.update_cp_status(cp.cp_id, "DESCONECTADO")
                            logger.warning(f"🔴 CP {cp.cp_id} desconectado - Sin heartbeat")
                        elif cp.status == "PARADO":
                            logger.debug(f"🟠 CP {cp.cp_id} está PARADO intencionalmente - Ignorando falta de heartbeat")
    
    def update_cp_status(self, cp_id: str, status: str, consumption: float = 0.0, 
                        amount: float = 0.0, driver_id: str = None):
        """Actualiza el estado de un punto de carga"""
        cp = self.database.get_charging_point(cp_id)
        if cp:
            if status == "AVERIADO" and cp.status == "SUMINISTRANDO":
                affected_driver = cp.driver_id
                cp.current_consumption = 0.0
                cp.current_amount = 0.0
                cp.driver_id = None
                logger.warning(f"🔌 Suministro cortado para conductor {affected_driver} - CP {cp_id} en avería")
            
            elif cp.status == "SUMINISTRANDO" and status != "SUMINISTRANDO" and status != "AVERIADO" and cp.current_amount > 0:
                self.record_transaction(cp, "COMPLETED")
            
            cp.status = status
            cp.current_consumption = consumption
            cp.current_amount = amount
            cp.driver_id = driver_id
            
            if status == "SUMINISTRANDO" and consumption > 0:
                cp.total_energy_supplied += consumption / 3600
                cp.total_revenue += amount
            
            self.kafka_manager.send_message('central_updates', cp.parse())
            logger.info(f"🔄 Estado actualizado - CP: {cp_id}, Estado: {status}")
    
    def record_transaction(self, cp: ChargingPoint, status: str):
        """Registra una transacción completada"""
        transaction = {
            'transaction_id': f"TXN{int(time.time())}",
            'cp_id': cp.cp_id,
            'driver_id': cp.driver_id,
            'energy_consumed': cp.current_consumption,
            'amount': cp.current_amount,
            'price_per_kwh': cp.price_per_kwh,
            'status': status,
            'start_time': cp.last_heartbeat.isoformat() if cp.last_heartbeat else datetime.now().isoformat(),
            'end_time': datetime.now().isoformat()
        }
        
        self.database.add_transaction(transaction)
        logger.info(f"💰 Transacción registrada: {transaction['transaction_id']}")

    def record_failed_transaction(self, cp: ChargingPoint, failure_reason: str, driver_id: str):
        """Registra una transacción fallida por avería"""
        transaction = {
            'transaction_id': f"TXN_FAIL_{int(time.time())}",
            'cp_id': cp.cp_id,
            'driver_id': driver_id,
            'energy_consumed': cp.current_consumption,
            'amount': cp.current_amount,
            'price_per_kwh': cp.price_per_kwh,
            'status': 'FAILED',
            'failure_reason': failure_reason,
            'start_time': cp.last_heartbeat.isoformat() if cp.last_heartbeat else datetime.now().isoformat(),
            'end_time': datetime.now().isoformat()
        }
        
        self.database.add_transaction(transaction)
        logger.info(f"❌ Transacción fallida registrada: {transaction['transaction_id']} - Razón: {failure_reason}")
        
        cp.current_consumption = 0.0
        cp.current_amount = 0.0
        cp.driver_id = None
    
    def send_control_command(self, cp_id: str, command: str):
        """Envía comandos de control a CPs via Kafka"""
        cp = self.database.get_charging_point(cp_id)
        if cp:
            logger.info(f"🔄 Enviando comando {command} a CP {cp_id} via Kafka")
            
            control_message = {
                'cp_id': cp_id,
                'command': command,
                'timestamp': datetime.now().isoformat(),
                'source': 'central'
            }
            
            self.kafka_manager.send_message('control_commands', control_message)
            logger.debug(f"📤 Mensaje Kafka enviado a control_commands: {control_message}")
            
            if command == "STOP":
                self.update_cp_status(cp_id, "PARADO")
                logger.info(f"⏹️ CP {cp_id} puesto en estado PARADO")
                print(f"✅ Punto de carga {cp_id} parado")
            elif command == "RESUME":
                self.update_cp_status(cp_id, "ACTIVADO") 
                logger.info(f"▶️ CP {cp_id} puesto en estado ACTIVADO")
                print(f"✅ Punto de carga {cp_id} reanudado")
                
        else:
            logger.error(f"❌ CP {cp_id} no encontrado para enviar comando")
            print(f"❌ Punto de carga {cp_id} no encontrado")
            
    def start_kafka_consumer(self):
        """Inicia el consumidor de Kafka en un hilo separado"""
        kafka_thread = threading.Thread(
            target=self.kafka_consumer_loop,
            daemon=True
        )
        kafka_thread.start()
        logger.info("📥 Consumidor de Kafka iniciado")
    
    def kafka_consumer_loop(self):
        """Loop principal para consumir mensajes de Kafka"""
        while self.running:
            try:
                logger.debug("🔄 Ciclo de consumidor Kafka iniciado")
                
                # Consumir mensajes de diferentes topics
                messages = self.kafka_manager.get_messages('cp_registrations')
                for msg_data in messages:
                    self.process_kafka_registration(msg_data['message'])
                
                status_messages = self.kafka_manager.get_messages('cp_status_updates')
                for msg_data in status_messages:
                    self.process_kafka_status_update(msg_data['message'])
                
                driver_messages = self.kafka_manager.get_messages('driver_requests')
                for msg_data in driver_messages:
                    self.process_driver_request(msg_data['message'])
                
                flow_messages = self.kafka_manager.get_messages('supply_flow')
                for msg_data in flow_messages:
                    self.process_supply_flow(msg_data['message'])
                
                response_messages = self.kafka_manager.get_messages('central_responses')
                for msg_data in response_messages:
                    self.process_central_response(msg_data['message'])
                
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"❌ Error en consumidor Kafka: {e}", exc_info=True)
                time.sleep(5)
    
    def process_central_response(self, message: dict):
        """Procesa respuestas de los CPs a comandos"""
        try:
            cp_id = message.get('cp_id')
            command = message.get('command')
            status = message.get('status')
            
            logger.debug(f"📨 Respuesta recibida de CP {cp_id}: {command} -> {status}")
            
            if status == 'EXECUTED':
                logger.info(f"✅ CP {cp_id} confirmó ejecución de comando: {command}")
            elif status == 'ERROR':
                logger.error(f"❌ CP {cp_id} reportó error en comando: {command}")
            else:
                logger.warning(f"⚠️ Respuesta desconocida de CP {cp_id}: {status}")
                
        except Exception as e:
            logger.error(f"❌ Error procesando respuesta de CP: {e}", exc_info=True)

    def process_supply_flow(self, message: dict):
        """Procesa mensajes de caudal de suministro desde el Engine"""
        try:
            cp_id = message.get('cp_id')
            driver_id = message.get('driver_id')
            flow_rate = message.get('flow_rate')
            energy_delivered = message.get('energy_delivered')
            current_amount = message.get('current_amount')
            timestamp = message.get('timestamp')
            
            if not cp_id or flow_rate is None:
                logger.error("❌ Parámetros insuficientes en mensaje de caudal")
                return
            
            self.update_cp_status(
                cp_id=cp_id,
                status="SUMINISTRANDO",
                consumption=flow_rate,
                amount=current_amount,
                driver_id=driver_id
            )
            
            logger.debug(f"⚡ Caudal actualizado - CP: {cp_id}, Flujo: {flow_rate} kW, Importe: €{current_amount:.2f}")
            
            if driver_id:
                self.send_flow_update_to_driver(
                    driver_id=driver_id,
                    cp_id=cp_id,
                    flow_rate=flow_rate,
                    energy_delivered=energy_delivered,
                    current_amount=current_amount,
                    timestamp=timestamp
                )
                
        except Exception as e:
            logger.error(f"❌ Error procesando mensaje de caudal: {e}", exc_info=True)

    def send_flow_update_to_driver(self, driver_id: str, cp_id: str, flow_rate: float, 
                                  energy_delivered: float, current_amount: float, timestamp: str = None):
        """Envía actualizaciones de caudal al conductor"""
        try:
            cp = self.database.get_charging_point(cp_id)
            if not cp:
                logger.error(f"❌ CP {cp_id} no encontrado para enviar actualización a driver {driver_id}")
                return
            
            flow_message = {
                'driver_id': driver_id,
                'cp_id': cp_id,
                'type': 'FLOW_UPDATE',
                'flow_rate': flow_rate,
                'energy_delivered': energy_delivered,
                'current_amount': current_amount,
                'total_amount': cp.total_revenue,
                'location': cp.location,
                'price_per_kwh': cp.price_per_kwh,
                'timestamp': timestamp or datetime.now().isoformat()
            }
            
            self.kafka_manager.send_message('driver_responses', flow_message)
            logger.debug(f"📤 Actualización de caudal enviada a driver {driver_id} - Flujo: {flow_rate} kW")
            
        except Exception as e:
            logger.error(f"❌ Error enviando actualización de caudal a driver {driver_id}: {e}", exc_info=True)
    
    def process_kafka_registration(self, message: dict):
        """Procesa mensajes de registro de CPs via Kafka"""
        try:
            operation = message.get('operation')
            
            if operation == 'REGISTER_CP':
                self.handle_kafka_cp_registration(message)
            elif operation == 'HEARTBEAT':
                self.handle_kafka_heartbeat(message)
            else:
                logger.warning(f"⚠️ Operación Kafka desconocida: {operation}")
                
        except Exception as e:
            logger.error(f"❌ Error procesando registro Kafka: {e}", exc_info=True)
    
    def handle_kafka_cp_registration(self, message: dict):
        """Maneja registro de CP via Kafka"""
        cp_id = message.get('cp_id')
        location = message.get('location')
        price_per_kwh = message.get('price_per_kwh', 0.45)
        
        existing_cp = self.database.get_charging_point(cp_id)
        if existing_cp:
            existing_cp.location = location
            existing_cp.price_per_kwh = price_per_kwh
            existing_cp.last_heartbeat = datetime.now()
            if existing_cp.status == "DESCONECTADO":
                self.update_cp_status(cp_id, "ACTIVADO")
            logger.info(f"🔄 CP {cp_id} actualizado via Kafka")
        else:
            cp = ChargingPoint(cp_id, location, price_per_kwh)
            cp.last_heartbeat = datetime.now()
            self.database.add_charging_point(cp)
            self.update_cp_status(cp_id, "ACTIVADO")
            logger.info(f"➕ CP {cp_id} registrado via Kafka - Ubicación: {location}")
        
        self.kafka_manager.send_message('central_responses', {
            'operation': 'REGISTRATION_CONFIRMED',
            'cp_id': cp_id,
            'status': 'SUCCESS',
            'timestamp': datetime.now().isoformat()
        })
    
    def handle_kafka_heartbeat(self, message: dict):
        """Maneja heartbeat de CP via Kafka"""
        cp_id = message.get('cp_id')
        cp = self.database.get_charging_point(cp_id)
        if cp:
            cp.last_heartbeat = datetime.now()
            logger.debug(f"💓 Heartbeat recibido via Kafka de {cp_id}")
    
    def process_kafka_status_update(self, message: dict):
        """Procesa actualizaciones de estado via Kafka"""
        try:
            cp_id = message.get('cp_id')
            status = message.get('status')
            consumption = message.get('consumption', 0.0)
            amount = message.get('amount', 0.0)
            driver_id = message.get('driver_id')
            
            self.update_cp_status(cp_id, status, consumption, amount, driver_id)
            logger.info(f"🔄 Estado actualizado via Kafka - CP: {cp_id}, Estado: {status}")
            
        except Exception as e:
            logger.error(f"❌ Error procesando actualización de estado Kafka: {e}", exc_info=True)

    def process_driver_request(self, message: dict):
        """Procesa peticiones de suministro desde app de conductor via Kafka"""
        try:
            driver_id = message.get('driver_id')
            cp_id = message.get('cp_id')
            request_type = message.get('type', 'SUPPLY_REQUEST')
            
            if not driver_id or not cp_id:
                logger.error("❌ Parámetros insuficientes en petición de conductor")
                return
            
            logger.info(f"📨 Petición de conductor recibida - Driver: {driver_id}, CP: {cp_id}")
            
            if request_type == 'SUPPLY_REQUEST':
                self.handle_driver_supply_request(driver_id, cp_id)
            elif request_type == 'STATUS_QUERY':
                self.handle_driver_status_query(driver_id, cp_id)
            else:
                logger.warning(f"⚠️ Tipo de petición desconocido: {request_type}")
                
        except Exception as e:
            logger.error(f"❌ Error procesando petición de conductor: {e}", exc_info=True)
    
    def handle_driver_supply_request(self, driver_id: str, cp_id: str):
        """Maneja solicitud de suministro desde conductor"""
        if driver_id not in self.database.drivers:
            self.database.register_driver(driver_id)
        
        cp = self.database.get_charging_point(cp_id)
        if cp and cp.status == "ACTIVADO":
            if self.authorize_cp_supply(cp_id, driver_id):
                self.kafka_manager.send_message('driver_responses', {
                    'driver_id': driver_id,
                    'cp_id': cp_id,
                    'status': 'AUTHORIZED',
                    'message': 'Suministro autorizado - Puede proceder con la carga',
                    'location': cp.location,
                    'price_per_kwh': cp.price_per_kwh,
                    'timestamp': datetime.now().isoformat()
                })
                logger.info(f"✅ Suministro autorizado - Driver: {driver_id}, CP: {cp_id}")
            else:
                self.kafka_manager.send_message('driver_responses', {
                    'driver_id': driver_id,
                    'cp_id': cp_id,
                    'status': 'ERROR',
                    'message': 'Error de comunicación con el punto de carga',
                    'timestamp': datetime.now().isoformat()
                })
        else:
            cp_status = cp.status if cp else "NO_ENCONTRADO"
            self.kafka_manager.send_message('driver_responses', {
                'driver_id': driver_id,
                'cp_id': cp_id,
                'status': 'DENIED',
                'message': f'Punto de carga no disponible - Estado: {cp_status}',
                'timestamp': datetime.now().isoformat()
            })
            logger.warning(f"❌ Suministro denegado - Driver: {driver_id}, CP: {cp_id} - Estado: {cp_status}")
    
    def handle_driver_status_query(self, driver_id: str, cp_id: str = None):
        """Maneja consulta de estado desde conductor"""
        if cp_id:
            cp = self.database.get_charging_point(cp_id)
            if cp:
                self.kafka_manager.send_message('driver_responses', {
                    'driver_id': driver_id,
                    'cp_id': cp_id,
                    'type': 'STATUS_RESPONSE',
                    'status': cp.status,
                    'location': cp.location,
                    'price_per_kwh': cp.price_per_kwh,
                    'current_consumption': cp.current_consumption,
                    'current_amount': cp.current_amount,
                    'driver_id': cp.driver_id,
                    'timestamp': datetime.now().isoformat()
                })
            else:
                self.kafka_manager.send_message('driver_responses', {
                    'driver_id': driver_id,
                    'cp_id': cp_id,
                    'type': 'STATUS_RESPONSE',
                    'status': 'NOT_FOUND',
                    'message': 'Punto de carga no encontrado',
                    'timestamp': datetime.now().isoformat()
                })
        else:
            all_cps = self.database.get_all_cps()
            self.kafka_manager.send_message('driver_responses', {
                'driver_id': driver_id,
                'type': 'ALL_STATUS_RESPONSE',
                'charging_points': all_cps,
                'timestamp': datetime.now().isoformat()
            })
    
    def authorize_cp_supply(self, cp_id: str, driver_id: str) -> bool:
        """Autoriza suministro en el CP via socket"""
        try:
            cp = self.database.get_charging_point(cp_id)
            if not cp or not cp.socket_connection:
                logger.error(f"❌ No hay conexión socket con CP {cp_id}")
                return False
            
            auth_message = f"SUMINISTRO_AUTORIZADO#{driver_id}"
            cp.socket_connection.send(auth_message.encode('utf-8'))
            
            self.update_cp_status(cp_id, "SUMINISTRANDO", driver_id=driver_id)
            logger.info(f"✅ CP {cp_id} confirmó autorización para driver {driver_id}")
            return True
        except Exception as e:
            logger.error(f"❌ Error autorizando suministro en CP {cp_id}: {e}", exc_info=True)
            return False

def main():
    """Función principal"""
    
    # Configuración con variables de entorno
    socket_host = os.getenv('SOCKET_HOST', '0.0.0.0')
    socket_port = int(os.getenv('SOCKET_PORT', '5000'))
    kafka_servers = os.getenv('KAFKA_SERVERS', 'localhost:9092')
    data_file = os.getenv('DATA_FILE', None)
    
    # Usar parámetros de línea de comandos si se proporcionan
    if len(sys.argv) > 1:
        socket_port = int(sys.argv[1])
    if len(sys.argv) > 2:
        kafka_servers = sys.argv[2]
    if len(sys.argv) > 3:
        data_file = sys.argv[3]
    
    print("=" * 60)
    print("EV_Central - Sistema de Gestión de Carga EV")
    print("Curso 25/26 - Sistemas Distribuidos")
    print("CON PERSISTENCIA DE DATOS - VERSIÓN COMPLETA")
    print("=" * 60)
    print(f"Socket: {socket_host}:{socket_port}")
    print(f"Kafka: {kafka_servers} (simulado)")
    print(f"Archivo de datos: {data_file or 'valor por defecto'}")
    print(f"Directorio de logs: {os.getenv('LOG_DIR', '/app/logs')}")
    print("=" * 60)
    
    try:
        central = EVCentral(socket_host, socket_port, kafka_servers, data_file)
        central.start()
        
    except Exception as e:
        logger.error(f"❌ Error iniciando EV_Central: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
