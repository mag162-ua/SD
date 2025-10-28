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
            data_file = os.getenv('DATA_FILE', '/app/data/ev_central_data.json')
        
        self.data_file = data_file
        self.charging_points: Dict[str, ChargingPoint] = {}
        self.drivers = set()
        self.transactions = []
        
        # Crear directorio del archivo de datos si no existe
        data_dir = os.path.dirname(self.data_file)
        if data_dir and not os.path.exists(data_dir):
            os.makedirs(data_dir)
            logger.info(f"📁 Directorio de datos creado: {data_dir}")
        
        # Solo cargar datos existentes, sin inicializar datos de ejemplo
        data_loaded = self.load_data()
        
        if not data_loaded:
            logger.info("🚀 Sistema iniciado sin datos previos. Esperando registro de puntos de carga...")
    
    def load_data(self):
        """Carga los datos desde el archivo JSON o backup más reciente"""
        try:
            # Primero intentar cargar el archivo principal
            if os.path.exists(self.data_file):
                logger.info(f"📁 Intentando cargar datos desde {self.data_file}")
                data = self._load_json_file(self.data_file)
                if data is not None:
                    self._process_loaded_data(data)
                    logger.info(f"✅ Datos cargados desde {self.data_file}: {len(self.charging_points)} CPs, {len(self.drivers)} conductores")
                    return True
            
            # Si el archivo principal no existe o está corrupto, buscar backups
            logger.info("🔍 Buscando backups disponibles...")
            backup_file = self._find_latest_backup()
            if backup_file:
                logger.info(f"🔄 Cargando desde backup: {backup_file}")
                backup_data = self._load_json_file(backup_file)
                if backup_data is not None:
                    self._process_loaded_data(backup_data)
                    # Restaurar el backup como archivo principal
                    self._restore_from_backup(backup_file)
                    logger.info(f"✅ Datos cargados desde backup: {backup_file}")
                    return True
            
            # No hay datos existentes - esto es normal al iniciar por primera vez
            logger.info("📭 No se encontraron datos existentes ni backups. Sistema iniciado sin datos.")
            return False
                    
        except Exception as e:
            logger.error(f"❌ Error cargando datos: {e}", exc_info=True)
            return False

    def _load_json_file(self, file_path: str) -> Optional[dict]:
        """Carga un archivo JSON de forma segura"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Validar estructura básica
            if not isinstance(data, dict):
                logger.error(f"❌ Estructura inválida en {file_path}")
                return None
                
            return data
            
        except json.JSONDecodeError as e:
            logger.error(f"❌ JSON corrupto en {file_path}: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ Error leyendo {file_path}: {e}")
            return None

    def _process_loaded_data(self, data: dict):
        """Procesa los datos cargados del JSON"""
        # Cargar puntos de carga
        self.charging_points.clear()
        for cp_data in data.get('charging_points', []):
            try:
                cp = ChargingPoint.unparse(cp_data)
                self.charging_points[cp.cp_id] = cp
            except Exception as e:
                logger.error(f"❌ Error cargando CP {cp_data.get('cp_id', 'desconocido')}: {e}")
        
        # Cargar conductores
        self.drivers = set(data.get('drivers', []))
        
        # Cargar transacciones
        self.transactions = data.get('transactions', [])
        
        logger.info(f"📊 Datos procesados: {len(self.charging_points)} CPs, {len(self.drivers)} conductores, {len(self.transactions)} transacciones")

    def _find_latest_backup(self) -> Optional[str]:
        """Encuentra el backup más reciente"""
        try:
            backup_dir = os.getenv('BACKUP_DIR', '/app/backups')
            if not os.path.exists(backup_dir):
                logger.warning(f"📁 Directorio de backups no encontrado: {backup_dir}")
                return None
            
            # Buscar archivos de backup
            backup_files = []
            for filename in os.listdir(backup_dir):
                if filename.startswith('ev_central_backup_') and filename.endswith('.json'):
                    file_path = os.path.join(backup_dir, filename)
                    if os.path.isfile(file_path):
                        backup_files.append(file_path)
            
            if not backup_files:
                logger.info("📁 No se encontraron archivos de backup")
                return None
            
            # Ordenar por fecha de modificación (más reciente primero)
            backup_files.sort(key=os.path.getmtime, reverse=True)
            latest_backup = backup_files[0]
            
            logger.info(f"📦 Backup más reciente encontrado: {latest_backup}")
            return latest_backup
            
        except Exception as e:
            logger.error(f"❌ Error buscando backups: {e}")
            return None

    def _restore_from_backup(self, backup_file: str):
        """Restaura el archivo principal desde un backup"""
        try:
            import shutil
            shutil.copy2(backup_file, self.data_file)
            logger.info(f"🔄 Backup restaurado como archivo principal: {backup_file} -> {self.data_file}")
        except Exception as e:
            logger.error(f"❌ Error restaurando backup: {e}")
    
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
    
    def add_charging_point(self, cp: ChargingPoint) -> bool:
        """Añade un punto de carga Y guarda automáticamente. Retorna True si fue exitoso."""
        # Verificar si el CP ya existe
        if cp.cp_id in self.charging_points:
            existing_cp = self.charging_points[cp.cp_id]
            logger.warning(f"⚠️ Intento de registrar CP duplicado: {cp.cp_id} - "
                          f"Estado actual: {existing_cp.status}, "
                          f"Ubicación: {existing_cp.location}")
            return False
        
        self.charging_points[cp.cp_id] = cp
        logger.info(f"➕ Punto de carga {cp.cp_id} registrado - Ubicación: {cp.location}, Precio: €{cp.price_per_kwh}/kWh")
        self.save_data()
        return True
    
    def cp_exists(self, cp_id: str) -> bool:
        """Verifica si un CP ya existe en la base de datos"""
        return cp_id in self.charging_points
    
    def get_charging_point(self, cp_id: str) -> Optional[ChargingPoint]:
        return self.charging_points.get(cp_id)
    
    def get_all_cps(self) -> List[dict]:
        return [cp.parse() for cp in self.charging_points.values()]
    
    def register_driver(self, driver_id: str):
        """Registra un conductor Y guarda automáticamente"""
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

import json
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

class RealKafkaManager:
    """Gestor de Kafka real para producción"""
    
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.consumers = {}
        
        try:
            # Inicializar producer
            self.producer = KafkaProducer(
                bootstrap_servers=[self.bootstrap_servers],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=3,
                acks='all'
            )
            
            logger.info(f"🔌 Kafka REAL conectado en {bootstrap_servers}")
            
        except Exception as e:
            logger.error(f"❌ Error conectando a Kafka: {e}")
            raise
    
    def send_message(self, topic: str, message: dict):
        """Envía mensaje a topic real de Kafka"""
        try:
            if self.producer:
                future = self.producer.send(topic, message)
                self.producer.flush()
                logger.debug(f"📤 Mensaje REAL enviado a {topic}: {message}")
            else:
                logger.error("❌ Producer de Kafka no inicializado")
                
        except KafkaError as e:
            logger.error(f"❌ Error enviando mensaje a Kafka: {e}")
        except Exception as e:
            logger.error(f"❌ Error inesperado enviando mensaje: {e}")
    
    def get_messages(self, topic: str, consumer_group: str = None):
        """Obtiene mensajes de un topic real de Kafka - CORREGIDO"""
        try:
            logger.info(f"🔍 get_messages llamado para topic: {topic}")
            
            if topic not in self.consumers:
                logger.info(f"🆕 Creando nuevo consumer para: {topic}")
                group_id = consumer_group or f"central_{topic}_group"
                self.consumers[topic] = KafkaConsumer(
                    topic,
                    bootstrap_servers=[self.bootstrap_servers],
                    auto_offset_reset='earliest',
                    enable_auto_commit=True,
                    auto_commit_interval_ms=1000,
                    group_id=group_id,
                    value_deserializer=lambda x: x.decode('utf-8') if x else None,
                    consumer_timeout_ms=1000
                )
                logger.info(f"✅ Consumer creado para {topic}, group: {group_id}")
            
            consumer = self.consumers[topic]
            messages = []
            
            logger.info(f"📥 Polling mensajes de {topic}...")
            try:
                records = consumer.poll(timeout_ms=2000)
            except Exception as e:
                if "KafkaConsumer is closed" in str(e):
                    logger.warning(f"🔄 Consumer cerrado, recreando para {topic}")
                    del self.consumers[topic]
                    return self.get_messages(topic, consumer_group)
                else:
                    raise e
            
            logger.info(f"📊 Poll result: {len(records)} partitions con mensajes")
            
            for topic_partition, message_batch in records.items():
                logger.info(f"   📦 Partition {topic_partition}: {len(message_batch)} mensajes")
                for message in message_batch:
                    if message.value:
                        try:
                            # PARSEAR MANUALMENTE el JSON
                            if isinstance(message.value, str):
                                message_dict = json.loads(message.value)
                            else:
                                message_dict = json.loads(message.value.decode('utf-8'))
                            
                            messages.append({
                                'timestamp': datetime.fromtimestamp(message.timestamp / 1000),
                                'message': message_dict
                            })
                            logger.info(f"   📨 Mensaje recibido (TIPO: {type(message_dict)}): {message_dict}")
                        except json.JSONDecodeError as e:
                            logger.error(f"❌ Error parseando JSON: {e} - Mensaje: {message.value}")
                        except Exception as e:
                            logger.error(f"❌ Error procesando mensaje: {e} - Mensaje: {message.value}")
            
            if messages:
                logger.info(f"✅ {len(messages)} mensajes REALES consumidos de {topic}")
            else:
                logger.info(f"📭 No hay mensajes en {topic}")
            
            return messages
            
        except Exception as e:
            logger.error(f"❌ Error en get_messages para {topic}: {e}", exc_info=True)
            # Si el consumer está cerrado, eliminarlo para recrearlo en el próximo intento
            if topic in self.consumers and "closed" in str(e).lower():
                logger.warning(f"🗑️ Eliminando consumer cerrado para {topic}")
                del self.consumers[topic]
            return []
    
    def close(self):
        """Cierra las conexiones de Kafka"""
        try:
            if self.producer:
                self.producer.close()
            
            for consumer in self.consumers.values():
                consumer.close()
                
            logger.info("🔌 Conexiones de Kafka cerradas")
        except Exception as e:
            logger.error(f"❌ Error cerrando conexiones Kafka: {e}")

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
                self.handle_cp_ok(parts[1:],client_socket)
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
            response = "ERROR#Parámetros_insuficientes"
            client_socket.send(response.encode('utf-8'))
            return
            
        cp_id = params[0]
        location = params[1]
        price = float(params[2])
        
        # ================================================================
        # 🎯 CORRECCIÓN: Condición lógica corregida - usar 'and' en lugar de 'or'
        # ================================================================
        if self.central.database.cp_exists(cp_id) and self.central.database.get_charging_point(cp_id).status not in ["AVERIADO", "DESCONECTADO"]:
            logger.warning(f"⚠️ Intento de registrar CP duplicado via socket: {cp_id}")
            response = f"ERROR#CP_ya_registrado#{cp_id}"
            client_socket.send(response.encode('utf-8'))
            return
        
        cp = ChargingPoint(cp_id, location, price)
        cp.socket_connection = client_socket
        cp.last_heartbeat = datetime.now()
        
        # Intentar agregar el CP (con validación duplicada por seguridad)
        if self.central.database.add_charging_point(cp):
            self.central.update_cp_status(cp_id, "DESCONECTADO")
            response = "REGISTER_OK"
            client_socket.send(response.encode('utf-8'))
            logger.info(f"✅ Punto de carga {cp_id} registrado correctamente via socket")
        else:
            # Esto no debería pasar porque ya verificamos, pero por seguridad
            response = f"ERROR#CP_ya_registrado#{cp_id}"
            client_socket.send(response.encode('utf-8'))
            logger.error(f"❌ Error inesperado registrando CP {cp_id}")
    
    def handle_cp_ok(self, params: List[str], client_socket):
        """Maneja ok de puntos de carga y envía confirmación"""
        if params:
            cp_id = params[0]
            cp = self.central.database.get_charging_point(cp_id)
            if not cp:
                logger.warning(f"⚠️ CP {cp_id} no registrado - Solicitando registro")
                response = f"ERROR#CP_no_registrado#SOLICITAR_REGISTRO#{cp_id}"
                client_socket.send(response.encode('utf-8'))
                logger.info(f"📋 Solicitando registro a CP no registrado: {cp_id}")
                return
                
            if cp:
                cp.last_heartbeat = datetime.now()
            
                status_changed = False
                if cp.status == "DESCONECTADO":
                    self.central.update_cp_status(cp_id, "ACTIVADO")
                    logger.info(f"🔄 CP {cp_id} reactivado - Estado cambiado a ACTIVADO")
                    status_changed = True
                elif cp.status == "AVERIADO":
                    self.central.update_cp_status(cp_id, "ACTIVADO")
                    logger.info(f"🔄 CP {cp_id} reactivado - Estado anterior AVERIADO cambiado a ACTIVADO")
                    status_changed = True
                
                response = f"CP_OK_ACK#{cp_id}"
                if status_changed:
                    response += "#ESTADO_ACTUALIZADO"
                
                client_socket.send(response.encode('utf-8'))
                logger.debug(f"💓 Heartbeat recibido de {cp_id} - Confirmación enviada")
                
            else:
                # CP no encontrado - enviar error
                response = f"ERROR#CP_no_encontrado#{cp_id}"
                client_socket.send(response.encode('utf-8'))
                logger.error(f"❌ CP {cp_id} no encontrado para mensaje CP_OK")

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
        
        # Usar Kafka REAL en lugar del simulado
        self.kafka_manager = RealKafkaManager(kafka_servers)
        
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
        
        # Cerrar conexiones Kafka
        if hasattr(self, 'kafka_manager'):
            self.kafka_manager.close()
        
        self.database.save_data()
        logger.info("💾 Datos guardados correctamente")
        self.database.backup_data()
        logger.info("✅ EV_Central apagado correctamente")
        sys.exit(0)

    def test_kafka_connection(self):
        """Test para verificar la conexión a Kafka"""
        try:
            logger.info("🧪 Probando conexión Kafka...")
            
            # Enviar mensaje de prueba
            test_message = {
                'test': True,
                'timestamp': datetime.now().isoformat(),
                'message': 'Test de conexión Kafka'
            }
            
            self.kafka_manager.send_message('test_topic', test_message)
            logger.info("✅ Mensaje de prueba enviado a Kafka")
            
            # Intentar consumir (puede que no haya mensajes, pero verifica la conexión)
            test_messages = self.kafka_manager.get_messages('test_topic', 'test_group')
            logger.info(f"📥 Test - Mensajes recibidos: {len(test_messages)}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Test de Kafka falló: {e}")
            return False

    def test_kafka_detailed(self):
        """Test detallado de Kafka"""
        try:
            logger.info("🧪 INICIANDO TEST KAFKA DETALLADO...")
            
            # 1. Verificar que podemos enviar
            test_msg = {
                'test': True,
                'timestamp': datetime.now().isoformat(),
                'source': 'central_test'
            }
            
            logger.info("📤 Enviando mensaje de test a supply_response...")
            self.kafka_manager.send_message('supply_response', test_msg)
            logger.info("✅ Mensaje enviado")
            
            # 2. Verificar que podemos recibir
            logger.info("📥 Intentando recibir mensajes...")
            messages = self.kafka_manager.get_messages('supply_response')
            logger.info(f"📦 Mensajes recibidos: {len(messages)}")
            
            # 3. Verificar conexión directa
            from kafka import KafkaConsumer
            logger.info("🔍 Probando conexión directa...")
            test_consumer = KafkaConsumer(
                'supply_response',
                bootstrap_servers=[self.kafka_manager.bootstrap_servers],
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                group_id='test_direct_group',
                consumer_timeout_ms=3000
            )
            
            available = list(test_consumer)
            logger.info(f"👀 Mensajes disponibles (directo): {len(available)}")
            test_consumer.close()
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Test Kafka falló: {e}", exc_info=True)
            return False

    def start(self):
        """Inicia todos los servicios del sistema central"""
        logger.info("🚀 Iniciando EV_Central...")
        self.running = True
        
        # Test de Kafka
        self.test_kafka_connection()
        self.test_kafka_detailed()
        
        # Iniciar todos los hilos
        socket_thread = threading.Thread(target=self.socket_server.start, daemon=True)
        socket_thread.start()
        
        heartbeat_thread = threading.Thread(target=self.monitor_heartbeats, daemon=True)
        heartbeat_thread.start()
        
        autosave_thread = threading.Thread(target=self.auto_save, daemon=True)
        autosave_thread.start()
        
        # Kafka consumer en hilo NO daemon
        self.start_kafka_consumer()
        
        # Console input en hilo separado
        console_thread = threading.Thread(target=self.handle_console_input, daemon=True)
        console_thread.start()
        
        self.show_control_panel()
        
        logger.info("✅ EV_Central iniciado correctamente")
        
        # Mantener hilo principal vivo
        try:
            while self.running:
                time.sleep(0.1)
        except KeyboardInterrupt:
            self.graceful_shutdown()

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
        """Muestra el estado actual del sistema con información detallada de suministro"""
        os.system('cls' if os.name == 'nt' else 'clear')
        print("\n" + "="*110)
        print("EV_Central - Estado del Sistema")
        print("="*110)
        
        print(f"{'ID':<8} {'Ubicación':<18} {'Estado':<14} {'Energía':<10} {'Precio':<8} {'Importe':<10} {'Conductor':<12} {'Detalles'}")
        print("-"*110)
        
        for cp in self.database.charging_points.values():
            status_icon = {
                "ACTIVADO": "🟢 ACTIVADO",
                "PARADO": "🟠 PARADO", 
                "SUMINISTRANDO": "🔵 SUMINISTRANDO",
                "AVERIADO": "🔴 AVERIADO",
                "DESCONECTADO": "⚫ DESCONECTADO"
            }.get(cp.status, "⚫ DESCONECTADO")
            
            precio = f"€{cp.price_per_kwh:.2f}"
            conductor = cp.driver_id if cp.driver_id else "-"
            
            if cp.status == "SUMINISTRANDO" and cp.total_energy_supplied > 0:
                # ⭐⭐ INFORMACIÓN DETALLADA PARA CPs SUMINISTRANDO
                energia = f"{cp.total_energy_supplied:.1f}kWh"
                importe_actual = f"€{cp.current_amount:.2f}"
                importe_total = f"€{cp.total_revenue:.2f}"
                caudal = f"{cp.current_consumption:.1f}kW" if cp.current_consumption > 0 else "0.0kW"
                
                detalles = f"Caudal: {caudal} | Sesión: {importe_actual}"
                
                print(f"{cp.cp_id:<8} {cp.location:<18} {status_icon:<14} {energia:<10} {precio:<8} {importe_total:<10} {conductor:<12} {detalles}")
                
            else:
                # Información básica para otros estados
                energia = f"{cp.total_energy_supplied:.1f}kWh" if cp.total_energy_supplied > 0 else "0.0kWh"
                importe_total = f"€{cp.total_revenue:.2f}" if cp.total_revenue > 0 else "€0.00"
                
                if cp.status == "ACTIVADO":
                    detalles = "🟢 Listo para cargar"
                elif cp.status == "PARADO":
                    detalles = "🟠 Parado manualmente"
                elif cp.status == "AVERIADO":
                    detalles = "🔴 En avería"
                else:
                    detalles = "⚫ Desconectado"
                
                print(f"{cp.cp_id:<8} {cp.location:<18} {status_icon:<14} {energia:<10} {precio:<8} {importe_total:<10} {conductor:<12} {detalles}")
        
        print("="*110)
        print("Comandos disponibles:")
        print("  stop <cp_id>    - Parar un punto de carga")
        print("  resume <cp_id>  - Reanudar un punto de carga")  
        print("  status         - Mostrar estado actual")
        print("  stats          - Mostrar estadísticas")
        print("  save           - Guardar datos manualmente")
        print("  backup         - Crear copia de seguridad")
        print("  quit           - Salir")
        print("="*110)

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
    
    # ================================================================
    # 🎯 NUEVO: Método mejorado para registrar transacciones con tickets
    # ================================================================
    def record_transaction(self, cp: ChargingPoint, status: str) -> dict:
        """Registra una transacción completada y retorna los datos"""
        transaction_id = f"TXN{int(time.time())}"
        transaction = {
            'transaction_id': transaction_id,
            'cp_id': cp.cp_id,
            'driver_id': cp.driver_id,
            'energy_consumed': cp.total_energy_supplied,
            'amount': cp.total_revenue,
            'price_per_kwh': cp.price_per_kwh,
            'status': status,
            'start_time': cp.last_heartbeat.isoformat() if cp.last_heartbeat else datetime.now().isoformat(),
            'end_time': datetime.now().isoformat(),
            'location': cp.location
        }
        
        self.database.add_transaction(transaction)
        logger.info(f"💰 Transacción registrada: {transaction_id}")
        
        return transaction

    # ================================================================
    # 🎯 NUEVO: Método para enviar tickets al driver
    # ================================================================
    def send_ticket_to_driver(self, cp: ChargingPoint, transaction_data: dict):
        """Envía ticket de transacción al conductor"""
        try:
            if not cp.driver_id:
                logger.warning(f"⚠️ No hay driver_id para enviar ticket del CP {cp.cp_id}")
                return
                
            ticket_message = {
                'type': 'TRANSACTION_TICKET',
                'ticket_id': transaction_data['transaction_id'],
                'driver_id': cp.driver_id,
                'cp_id': cp.cp_id,
                'location': cp.location,
                'energy_consumed_kwh': round(transaction_data['energy_consumed'], 2),
                'total_amount_eur': round(transaction_data['amount'], 2),
                'price_per_kwh': cp.price_per_kwh,
                'start_time': transaction_data['start_time'],
                'end_time': transaction_data['end_time'],
                'duration_minutes': self.calculate_duration_minutes(
                    transaction_data['start_time'], 
                    transaction_data['end_time']
                ),
                'status': 'COMPLETED',
                'timestamp': datetime.now().isoformat()
            }
            
            self.kafka_manager.send_message('driver_tickets', ticket_message)
            logger.info(f"🎫 Ticket enviado a driver {cp.driver_id} - Transacción: {transaction_data['transaction_id']}")
            
        except Exception as e:
            logger.error(f"❌ Error enviando ticket a driver {cp.driver_id}: {e}", exc_info=True)

    # ================================================================
    # 🎯 NUEVO: Método para enviar tickets al engine
    # ================================================================
    def send_ticket_to_engine(self, cp: ChargingPoint, transaction_data: dict):
        """Envía ticket de transacción al engine"""
        try:
            ticket_message = {
                'type': 'TRANSACTION_TICKET',
                'ticket_id': transaction_data['transaction_id'],
                'cp_id': cp.cp_id,
                'energy_consumed_kwh': round(transaction_data['energy_consumed'], 2),
                'total_amount_eur': round(transaction_data['amount'], 2),
                'price_per_kwh': cp.price_per_kwh,
                'start_time': transaction_data['start_time'],
                'end_time': transaction_data['end_time'],
                'duration_minutes': self.calculate_duration_minutes(
                    transaction_data['start_time'], 
                    transaction_data['end_time']
                ),
                'status': 'COMPLETED',
                'timestamp': datetime.now().isoformat()
            }
            
            self.kafka_manager.send_message('engine_tickets', ticket_message)
            logger.info(f"🎫 Ticket enviado a engine {cp.cp_id} - Transacción: {transaction_data['transaction_id']}")
            
        except Exception as e:
            logger.error(f"❌ Error enviando ticket a engine {cp.cp_id}: {e}", exc_info=True)

    # ================================================================
    # 🎯 NUEVO: Método auxiliar para calcular duración
    # ================================================================
    def calculate_duration_minutes(self, start_time_str: str, end_time_str: str) -> float:
        """Calcula la duración en minutos entre dos timestamps"""
        try:
            start_time = datetime.fromisoformat(start_time_str)
            end_time = datetime.fromisoformat(end_time_str)
            duration = end_time - start_time
            return round(duration.total_seconds() / 60, 2)
        except Exception as e:
            logger.error(f"❌ Error calculando duración: {e}")
            return 0.0

    # ================================================================
    # 🎯 MODIFICADO: Método mejorado para transacciones fallidas con tickets
    # ================================================================
    def record_failed_transaction(self, cp: ChargingPoint, failure_reason: str, driver_id: str):
        """Registra una transacción fallida por avería y envía tickets"""
        transaction_id = f"TXN_FAIL_{int(time.time())}"
        transaction = {
            'transaction_id': transaction_id,
            'cp_id': cp.cp_id,
            'driver_id': driver_id,
            'energy_consumed': cp.current_consumption,
            'amount': cp.current_amount,
            'price_per_kwh': cp.price_per_kwh,
            'status': 'FAILED',
            'failure_reason': failure_reason,
            'start_time': cp.last_heartbeat.isoformat() if cp.last_heartbeat else datetime.now().isoformat(),
            'end_time': datetime.now().isoformat(),
            'location': cp.location
        }
        
        self.database.add_transaction(transaction)
        logger.info(f"❌ Transacción fallida registrada: {transaction_id} - Razón: {failure_reason}")
        
        # Enviar tickets de transacción fallida
        if driver_id:
            self.send_failed_ticket_to_driver(cp, transaction, failure_reason)
        self.send_failed_ticket_to_engine(cp, transaction, failure_reason)
        
        self.update_cp_status(
            cp_id=cp.cp_id,
            status="AVERIADO",
            consumption=0.0,
            amount=0.0,
            driver_id=None
        )

    # ================================================================
    # 🎯 NUEVO: Método para tickets fallidos al driver
    # ================================================================
    def send_failed_ticket_to_driver(self, cp: ChargingPoint, transaction_data: dict, failure_reason: str):
        """Envía ticket de transacción fallida al conductor"""
        try:
            ticket_message = {
                'type': 'TRANSACTION_TICKET',
                'ticket_id': transaction_data['transaction_id'],
                'driver_id': cp.driver_id,
                'cp_id': cp.cp_id,
                'location': cp.location,
                'energy_consumed_kwh': round(transaction_data['energy_consumed'], 2),
                'total_amount_eur': round(transaction_data['amount'], 2),
                'price_per_kwh': cp.price_per_kwh,
                'start_time': transaction_data['start_time'],
                'end_time': transaction_data['end_time'],
                'duration_minutes': self.calculate_duration_minutes(
                    transaction_data['start_time'], 
                    transaction_data['end_time']
                ),
                'status': 'FAILED',
                'failure_reason': failure_reason,
                'timestamp': datetime.now().isoformat()
            }
            
            self.kafka_manager.send_message('driver_tickets', ticket_message)
            logger.info(f"🎫 Ticket FALLIDO enviado a driver {cp.driver_id}")
            
        except Exception as e:
            logger.error(f"❌ Error enviando ticket fallido a driver: {e}")
    
    def send_control_command(self, cp_id: str, command: str):
        """Envía comandos de control a CPs via Kafka - MEJORADO"""
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
            
            # CORRECCIÓN: Solo actualizar estado local para comandos manuales
            # Los comandos automáticos desde el Engine ya manejan su estado
            if command == "STOP":
                if cp.status == "SUMINISTRANDO":
                    self.update_cp_status(cp_id, "PARADO")
                    logger.info(f"⏹️ CP {cp_id} puesto en estado PARADO")
                print(f"✅ Comando STOP enviado a punto de carga {cp_id}")
            elif command == "START":
                if cp.status == "ACTIVADO":
                    self.update_cp_status(cp_id, "SUMINISTRANDO")
                    logger.info(f"▶️ CP {cp_id} puesto en estado SUMINISTRANDO")
                print(f"✅ Comando START enviado a punto de carga {cp_id}")
            elif command == "RESUME":
                self.update_cp_status(cp_id, "ACTIVADO") 
                logger.info(f"▶️ CP {cp_id} puesto en estado ACTIVADO")
                print(f"✅ Punto de carga {cp_id} reanudado")
                
        else:
            logger.error(f"❌ CP {cp_id} no encontrado para enviar comando")
            print(f"❌ Punto de carga {cp_id} no encontrado")
            
    def start_kafka_consumer(self):
        """Inicia el consumidor de Kafka en un hilo separado"""
        try:
            logger.info("🔄 Iniciando hilo del consumidor Kafka...")
            kafka_thread = threading.Thread(
                target=self.kafka_consumer_loop,
                daemon=True,
                name="KafkaConsumerThread"
            )
            kafka_thread.start()
            logger.info(f"✅ Consumidor de Kafka iniciado")
            
        except Exception as e:
            logger.error(f"❌ Error iniciando consumidor Kafka: {e}", exc_info=True)
    
    def kafka_consumer_loop(self):
        """Loop principal para consumir mensajes de Kafka - CORREGIDO"""
        logger.info("🚀 Kafka consumer loop iniciado correctamente")
        
        loop_count = 0
        while self.running:
            try:
                loop_count += 1
                logger.info(f"🔄 DENTRO DEL WHILE - Ciclo #{loop_count}")
                
                # ================================================================
                # 🎯 MODIFICADO: Agregar nuevos topics para tickets
                # ================================================================
                topics_to_check = ['driver_requests', 'supply_flow', 'supply_response', 'control_commands', 'driver_tickets', 'engine_tickets']
                
                for topic in topics_to_check:
                    try:
                        logger.info(f"🔍 Verificando topic: {topic}")
                        messages = self.kafka_manager.get_messages(topic)
                        logger.info(f"📦 Resultado para {topic}: {len(messages)} mensajes")
                        
                        if messages:
                            logger.info(f"🎯 MENSAJES ENCONTRADOS en {topic}: {len(messages)}")
                            for i, msg_data in enumerate(messages):
                                message_content = msg_data['message']
                                logger.info(f"   📝 Mensaje {i+1} (TIPO: {type(message_content)}): {message_content}")
                                
                                try:
                                    if topic == 'supply_response':
                                        self.process_supply_response(message_content)
                                    elif topic == 'supply_flow':
                                        self.process_supply_flow(message_content)
                                    elif topic == 'driver_requests':
                                        self.process_driver_request(message_content)
                                    # Los topics driver_tickets y engine_tickets son para enviar, no recibir
                                except Exception as e:
                                    logger.error(f"❌ Error procesando mensaje en {topic}: {e}")
                        
                        else:
                            logger.debug(f"📭 No hay mensajes en {topic}")
                            
                    except Exception as e:
                        logger.error(f"❌ Error en topic {topic}: {e}", exc_info=True)
                
                logger.info(f"💤 Esperando 2 segundos...")
                time.sleep(2)
                logger.info(f"✅ Ciclo #{loop_count} completado")
                
            except Exception as e:
                logger.error(f"💥 ERROR CRÍTICO en kafka_consumer_loop: {e}", exc_info=True)
                logger.info("💤 Esperando 5 segundos antes de reintentar...")
                time.sleep(5)
        
        logger.info("🛑 Kafka consumer loop detenido")

    # ================================================================
    # 🎯 MODIFICADO: Procesar supply_response con manejo de STOP_SUPPLY y tickets
    # ================================================================
    def process_supply_response(self, message):
        """Procesa respuestas de suministro desde el Engine - CON TICKETS"""
        logger.info(f"🎯 process_supply_response llamado con: {message} (TIPO: {type(message)})")
        try:
            # Manejar tanto diccionarios como strings
            if isinstance(message, str):
                try:
                    message = json.loads(message)
                    logger.info(f"🔄 Mensaje parseado de string a dict: {message}")
                except json.JSONDecodeError as e:
                    logger.error(f"❌ Error parseando mensaje string a JSON: {e}")
                    return
            elif not isinstance(message, dict):
                logger.error(f"❌ Mensaje no es ni string ni diccionario: {type(message)} - {message}")
                return

            # Ahora message debería ser un diccionario
            cp_id = message.get('cp_id')
            message_type = message.get('type')
            reason = message.get('reason', 'No especificado')

            if not cp_id:
                logger.error("❌ Mensaje de suministro sin CP_ID")
                return

            cp = self.database.get_charging_point(cp_id)
            if not cp:
                logger.error(f"❌ CP {cp_id} no encontrado para mensaje de suministro")
                return

            # CORRECCIÓN: Manejar los tipos de mensaje del Engine
            if message_type == 'SUPPLY_REQUEST':
                # El Engine solicita iniciar suministro
                if cp.status == "ACTIVADO" or cp.status == "SUMINISTRANDO":
                    # Autorizar suministro
                    driver_id = message.get('driver_id', 'MANUAL_ENGINE')
                    
                    # Enviar comando START al Engine
                    control_message = {
                        'cp_id': cp_id,
                        'command': 'START',
                        'driver_id': driver_id,
                        'timestamp': datetime.now().isoformat(),
                        'source': 'central'
                    }
                    self.kafka_manager.send_message('control_commands', control_message)
                    logger.info(f"✅ Suministro AUTORIZADO para CP {cp_id} - Comando START enviado")
                    
                    # Actualizar estado local
                    self.update_cp_status(
                        cp_id=cp_id,
                        status="SUMINISTRANDO",
                        driver_id=driver_id
                    )
                else:
                    logger.warning(f"❌ CP {cp_id} no está ACTIVADO o SUMINISTRANDO - Estado actual: {cp.status}")
                    
            elif message_type == 'STOP_SUPPLY':
                # ================================================================
                # 🎯 NUEVO: El Engine solicita parar suministro - GENERAR TICKETS
                # ================================================================
                if cp.status == "SUMINISTRANDO":
                    # 1. Registrar transacción completada
                    transaction_data = self.record_transaction(cp, "COMPLETED")
                    
                    # 2. Enviar ticket al Driver
                    self.send_ticket_to_driver(cp, transaction_data)
                    
                    # 3. Enviar ticket al Engine
                    self.send_ticket_to_engine(cp, transaction_data)
                    
                    # 4. Enviar comando STOP al Engine (confirmación)
                    control_message = {
                        'cp_id': cp_id,
                        'command': 'STOP',
                        'timestamp': datetime.now().isoformat(),
                        'source': 'central',
                        'transaction_id': transaction_data['transaction_id']
                    }
                    self.kafka_manager.send_message('control_commands', control_message)
                    logger.info(f"🛑 Suministro DETENIDO para CP {cp_id} - Ticket enviado")
                    
                    # 5. Actualizar estado local
                    self.update_cp_status(cp_id, "ACTIVADO")
                else:
                    logger.info(f"ℹ️ CP {cp_id} ya estaba detenido - Estado: {cp.status}")
                    
            else:
                logger.warning(f"⚠️ Tipo de mensaje no reconocido: {message_type}")
                
        except Exception as e:
            logger.error(f"❌ Error procesando respuesta de suministro: {e}", exc_info=True)
        
    def process_supply_flow(self, message):
        """Procesa mensajes de caudal de suministro desde el Engine - CORREGIDO"""
        logger.info(f"🎯 process_supply_flow llamado con: {message} (TIPO: {type(message)})")
        try:
            # Manejar tanto diccionarios como strings
            if isinstance(message, str):
                try:
                    message = json.loads(message)
                    logger.info(f"🔄 Mensaje parseado de string a dict: {message}")
                except json.JSONDecodeError as e:
                    logger.error(f"❌ Error parseando mensaje string a JSON: {e}")
                    return
            elif not isinstance(message, dict):
                logger.error(f"❌ Mensaje no es ni string ni diccionario: {type(message)} - {message}")
                return

            cp_id = message.get('cp_id')
            driver_id = message.get('driver_id', 'MANUAL')
            kwh = message.get('kwh', 0.0)
            reason = message.get('reason', '')
            
            if not cp_id:
                logger.error("❌ Parámetros insuficientes en mensaje de suministro")
                return
            
            cp = self.database.get_charging_point(cp_id)
            if not cp:
                logger.error(f"❌ CP {cp_id} no encontrado")
                return

            # CORRECCIÓN: Manejar diferentes razones del mensaje
            if reason == 'SUPPLY_ENDED':
                # Suministro finalizado
                if cp.status == "SUMINISTRANDO":
                    # Registrar transacción completada
                    if cp.current_amount > 0:
                        self.record_transaction(cp, "COMPLETED")
                    
                    self.update_cp_status(cp_id, "ACTIVADO")
                    logger.info(f"✅ Suministro FINALIZADO - CP: {cp_id}, Energía total: {kwh:.2f}kWh")
                return

            # Suministro en progreso
            # Calcular importe actual basado en el precio del CP
            current_amount = kwh * cp.price_per_kwh
            
            # Calcular caudal (kW) basado en incremento de energía
            # Asumimos 1 kWh por segundo para simplificar (en realidad sería menos)
            current_flow_rate = 1.0  # kW

            # Actualizar el estado del CP
            self.update_cp_status(
                cp_id=cp_id,
                status="SUMINISTRANDO",
                consumption=current_flow_rate,
                amount=current_amount,
                driver_id=driver_id
            )
            
            # Actualizar estadísticas acumuladas
            cp.total_energy_supplied = kwh
            cp.total_revenue = current_amount
            
            logger.info(f"⚡ Suministro ACTIVO - CP: {cp_id}, Energía: {kwh:.2f}kWh, Importe: €{current_amount:.2f}")
            
        except Exception as e:
            logger.error(f"❌ Error procesando mensaje de suministro: {e}", exc_info=True)

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

    def process_driver_request(self, message):
        """Procesa peticiones de suministro desde app de conductor via Kafka - CORREGIDO"""
        logger.info(f"🎯 process_driver_request llamado con: {message} (TIPO: {type(message)})")
        try:
            # CORRECCIÓN: Manejar tanto diccionarios como strings
            if isinstance(message, str):
                try:
                    message = json.loads(message)
                    logger.info(f"🔄 Mensaje parseado de string a dict: {message}")
                except json.JSONDecodeError as e:
                    logger.error(f"❌ Error parseando mensaje string a JSON: {e}")
                    return
            elif not isinstance(message, dict):
                logger.error(f"❌ Mensaje no es ni string ni diccionario: {type(message)} - {message}")
                return

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
        """Autoriza suministro en el CP via socket Y Kafka"""
        try:
            # Enviar comando START por Kafka
            control_message = {
                'cp_id': cp_id,
                'command': 'START',  # Comando para iniciar suministro
                'driver_id': driver_id,
                'timestamp': datetime.now().isoformat(),
                'source': 'central'
            }
            self.kafka_manager.send_message('control_commands', control_message)
            logger.info(f"📤 Comando START enviado a CP {cp_id} via Kafka")
            
            # También enviar por socket (backup)
            cp = self.database.get_charging_point(cp_id)
            if cp and cp.socket_connection:
                try:
                    auth_message = f"SUMINISTRO_AUTORIZADO#{driver_id}"
                    cp.socket_connection.send(auth_message.encode('utf-8'))
                    logger.info(f"📤 Comando enviado a CP {cp_id} via socket")
                except:
                    logger.warning(f"⚠️ No se pudo enviar comando via socket a CP {cp_id}")
            
            self.update_cp_status(cp_id, "SUMINISTRANDO", driver_id=driver_id)
            logger.info(f"✅ Suministro autorizado para CP {cp_id} - Conductor: {driver_id}")
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
    print(f"Kafka: {kafka_servers} (REAL)")
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
