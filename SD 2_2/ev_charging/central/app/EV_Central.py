#!/usr/bin/env python3
# ev_central.py

import os
import sys
import socket
import threading
import json
import time
import logging
from decimal import Decimal
from datetime import datetime,date
from typing import Dict, List, Optional
import psycopg2
from psycopg2 import pool, extras, OperationalError

# Configuración de logging MEJORADA para Docker
def setup_logging():
    """Configuración logging """
    # Usar variable de entorno o valor por defecto
    log_dir = os.getenv('LOG_DIR', '/app/logs')
    
    if not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)
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
        self.last_supply_message = None
        self.supply_ending = False  
        self.supply_ended_time = None
        self.secret_key = None
    
    def parse(self):
        """Convierte el CP a diccionario para serialización"""
        return {
            'cp_id': self.cp_id,
            'location': self.location,
            'price_per_kwh': float(self.price_per_kwh), # <--- AÑADIR float()
            'status': self.status,
            'current_consumption': float(self.current_consumption), # <--- AÑADIR float()
            'current_amount': float(self.current_amount), # <--- AÑADIR float()
            'driver_id': self.driver_id,
            'last_heartbeat': self.last_heartbeat.isoformat() if self.last_heartbeat else None,
            'total_energy_supplied': float(self.total_energy_supplied), # <--- AÑADIR float()
            'total_revenue': float(self.total_revenue), # <--- AÑADIR float()
            'registration_date': self.registration_date,
            'last_supply_message': self.last_supply_message.isoformat() if self.last_supply_message else None,
            'supply_ending': self.supply_ending, 
            'supply_ended_time': self.supply_ended_time.isoformat() if self.supply_ended_time else None
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
        cp.secret_key = data.get('secret_key', None)
        
        if data['last_heartbeat']:
            cp.last_heartbeat = datetime.fromisoformat(data['last_heartbeat'])
        
        # Control de suministro
        if data.get('last_supply_message'):
            cp.last_supply_message = datetime.fromisoformat(data['last_supply_message'])
        cp.supply_ending = data.get('supply_ending', False)
        if data.get('supply_ended_time'):
            cp.supply_ended_time = datetime.fromisoformat(data['supply_ended_time'])
        
        return cp

class DatabaseManager:
    """Gestor de base de datos PostgreSQL con persistencia"""
    
    def __init__(self):
        self.charging_points: Dict[str, ChargingPoint] = {}
        self.drivers = set()
        self.transactions = []
        
        # Configuración de conexión a PostgreSQL desde variables de entorno
        self.db_config = {
            'host': os.getenv('DB_HOST', 'postgres'),
            'database': os.getenv('DB_NAME', 'ev_db'),
            'user': os.getenv('DB_USER', 'user'),
            'password': os.getenv('DB_PASSWORD', 'password'),
            'port': os.getenv('DB_PORT', '5432')
        }
        
        # Pool de conexiones
        self.connection_pool = None
        
        # Intentar conectar con reintentos
        self.connect_with_retries(max_retries=15, retry_interval=3)
        
        # Inicializar base de datos
        self.init_database()
        
        # Cargar datos existentes
        data_loaded = self.load_data()
        
        if not data_loaded:
            logger.info("🚀 Sistema iniciado sin datos previos. Esperando registro de puntos de carga...")
    
    def connect_with_retries(self, max_retries=15, retry_interval=3):
        """Intenta conectar a PostgreSQL con reintentos"""
        logger.info("🔌 Conectando a PostgreSQL...")
        logger.info(f"📊 Configuración DB: host={self.db_config['host']}, db={self.db_config['database']}, user={self.db_config['user']}")
        
        for attempt in range(max_retries):
            try:
                logger.info(f"🔄 Intentando conectar a PostgreSQL (intento {attempt+1}/{max_retries})...")
                
                # Intentar conexión simple primero
                test_conn = psycopg2.connect(
                    host=self.db_config['host'],
                    database=self.db_config['database'],
                    user=self.db_config['user'],
                    password=self.db_config['password'],
                    port=self.db_config['port'],
                    connect_timeout=5
                )
                test_conn.close()
                
                # Crear pool de conexiones
                self.connection_pool = pool.SimpleConnectionPool(
                    1, 20,
                    host=self.db_config['host'],
                    database=self.db_config['database'],
                    user=self.db_config['user'],
                    password=self.db_config['password'],
                    port=self.db_config['port']
                )
                
                logger.info("✅ Conexión a PostgreSQL establecida correctamente")
                return True
                
            except OperationalError as e:
                if attempt < max_retries - 1:
                    logger.warning(f"⚠️ Error conectando a PostgreSQL: {e}. Reintentando en {retry_interval}s...")
                    time.sleep(retry_interval)
                else:
                    logger.error(f"❌ No se pudo conectar a PostgreSQL después de {max_retries} intentos")
                    # No lanzamos excepción, el sistema puede funcionar en modo memoria
                    logger.info("⚠️ El sistema funcionará en modo memoria hasta que PostgreSQL esté disponible")
                    return False
    
    def get_connection(self):
        """Obtiene una conexión del pool"""
        try:
            if not self.connection_pool:
                # Intentar reconectar si el pool no existe
                self.connect_with_retries(max_retries=3, retry_interval=1)
                if not self.connection_pool:
                    raise OperationalError("No hay conexión disponible a PostgreSQL")
            
            conn = self.connection_pool.getconn()
            return conn
            
        except Exception as e:
            logger.error(f"❌ Error obteniendo conexión: {e}")
            raise
    
    def return_connection(self, conn):
        """Devuelve una conexión al pool"""
        try:
            if self.connection_pool:
                self.connection_pool.putconn(conn)
        except Exception as e:
            logger.error(f"❌ Error devolviendo conexión: {e}")
    
    def init_database(self):
        """Inicializa la base de datos y crea las tablas si no existen"""
        if not self.connection_pool:
            logger.warning("⚠️ No se puede inicializar base de datos: PostgreSQL no disponible")
            return
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Crear tabla de puntos de carga
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS charging_points (
                    cp_id VARCHAR(50) PRIMARY KEY,
                    location VARCHAR(100) NOT NULL,
                    price_per_kwh DECIMAL(10, 2) NOT NULL,
                    status VARCHAR(20) NOT NULL DEFAULT 'DESCONECTADO',
                    current_consumption DECIMAL(10, 2) DEFAULT 0.0,
                    current_amount DECIMAL(10, 2) DEFAULT 0.0,
                    driver_id VARCHAR(50),
                    last_heartbeat TIMESTAMP,
                    total_energy_supplied DECIMAL(10, 2) DEFAULT 0.0,
                    total_revenue DECIMAL(10, 2) DEFAULT 0.0,
                    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_supply_message TIMESTAMP,
                    supply_ending BOOLEAN DEFAULT FALSE,
                    supply_ended_time TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Crear tabla de conductores
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS drivers (
                    driver_id VARCHAR(50) PRIMARY KEY,
                    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Crear tabla de transacciones
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    transaction_id VARCHAR(100) PRIMARY KEY,
                    cp_id VARCHAR(50) NOT NULL,
                    driver_id VARCHAR(50),
                    energy_consumed DECIMAL(10, 2),
                    amount DECIMAL(10, 2),
                    price_per_kwh DECIMAL(10, 2),
                    status VARCHAR(20),
                    failure_reason TEXT,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    location VARCHAR(100),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Crear tabla de backups (histórico)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS backups (
                    backup_id SERIAL PRIMARY KEY,
                    backup_type VARCHAR(50) NOT NULL,
                    description TEXT,
                    data JSONB NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Crear índices
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_cp_status ON charging_points(status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_cp_id ON transactions(cp_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_driver_id ON transactions(driver_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_backups_created_at ON backups(created_at)")
            
            conn.commit()
            cursor.close()
            self.return_connection(conn)
            
            logger.info("✅ Base de datos PostgreSQL inicializada correctamente")
            
        except Exception as e:
            logger.error(f"❌ Error inicializando base de datos: {e}", exc_info=True)
    
    def load_data(self):
        """Carga los datos desde la base de datos PostgreSQL"""
        if not self.connection_pool:
            logger.warning("⚠️ No se pueden cargar datos: PostgreSQL no disponible")
            return False
            
        try:
            logger.info("📁 Cargando datos desde PostgreSQL...")
            
            conn = self.get_connection()
            cursor = conn.cursor(cursor_factory=extras.RealDictCursor)
            
            # Cargar puntos de carga
            cursor.execute("SELECT * FROM charging_points")
            charging_points_data = cursor.fetchall()
            
            self.charging_points.clear()
            for cp_data in charging_points_data:
                try:
                    cp_dict = dict(cp_data)
                    # Convertir tipos especiales
                    cp_dict['last_heartbeat'] = cp_data['last_heartbeat'].isoformat() if cp_data['last_heartbeat'] else None
                    cp_dict['last_supply_message'] = cp_data['last_supply_message'].isoformat() if cp_data['last_supply_message'] else None
                    cp_dict['supply_ended_time'] = cp_data['supply_ended_time'].isoformat() if cp_data['supply_ended_time'] else None
                    cp_dict['registration_date'] = cp_data['registration_date'].isoformat() if cp_data['registration_date'] else datetime.now().isoformat()
                    
                    cp = ChargingPoint.unparse(cp_dict)
                    self.charging_points[cp.cp_id] = cp
                except Exception as e:
                    logger.error(f"❌ Error cargando CP {cp_data.get('cp_id', 'desconocido')}: {e}")
            
            # Cargar conductores
            cursor.execute("SELECT driver_id FROM drivers")
            drivers_data = cursor.fetchall()
            self.drivers = set(driver['driver_id'] for driver in drivers_data)
            
            # Cargar transacciones (solo para estadísticas, no para memoria)
            cursor.execute("SELECT COUNT(*) as count FROM transactions")
            transaction_count = cursor.fetchone()['count']
            self.transactions = []  # No cargamos todas las transacciones en memoria
            
            cursor.close()
            self.return_connection(conn)
            
            logger.info(f"✅ Datos cargados desde PostgreSQL: {len(self.charging_points)} CPs, {len(self.drivers)} conductores, {transaction_count} transacciones")
            return True
                    
        except Exception as e:
            logger.error(f"❌ Error cargando datos desde PostgreSQL: {e}", exc_info=True)
            return False
        
    def get_secret_key(self, cp_id: str, secret_key) -> Optional[str]:
        """Carga los datos desde la base de datos PostgreSQL"""
        if not self.connection_pool:
            logger.warning("⚠️ No se pueden cargar datos: PostgreSQL no disponible")
            return False
            
        try:
            logger.info("📁 Cargando datos desde PostgreSQL...")
            
            conn = self.get_connection()
            cursor = conn.cursor(cursor_factory=extras.RealDictCursor)
            
            # Cargar puntos de carga
            cursor.execute("SELECT secret_key FROM charging_points where cp_id=%s", (cp_id,))
            
            return secret_key == cursor.fetchone()['secret_key']
            
        except Exception as e:
            logger.error(f"❌ Error cargando datos desde PostgreSQL: {e}", exc_info=True)
            return False
    
    def save_charging_point(self, cp: ChargingPoint) -> bool:
        """Guarda un punto de carga en la base de datos"""
        if not self.connection_pool:
            logger.warning(f"⚠️ No se puede guardar CP {cp.cp_id}: PostgreSQL no disponible")
            return False
            
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Verificar si existe
            cursor.execute("SELECT cp_id FROM charging_points WHERE cp_id = %s", (cp.cp_id,))
            exists = cursor.fetchone() is not None
            
            if exists:
                # Actualizar
                cursor.execute("""
                    UPDATE charging_points SET
                        location = %s,
                        price_per_kwh = %s,
                        status = %s,
                        current_consumption = %s,
                        current_amount = %s,
                        driver_id = %s,
                        last_heartbeat = %s,
                        total_energy_supplied = %s,
                        total_revenue = %s,
                        last_supply_message = %s,
                        supply_ending = %s,
                        supply_ended_time = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE cp_id = %s
                """, (
                    cp.location, cp.price_per_kwh, cp.status,
                    cp.current_consumption, cp.current_amount,
                    cp.driver_id, cp.last_heartbeat,
                    cp.total_energy_supplied, cp.total_revenue,
                    cp.last_supply_message, cp.supply_ending,
                    cp.supply_ended_time, cp.cp_id
                ))
            else:
                # Insertar nuevo
                cursor.execute("""
                    INSERT INTO charging_points (
                        cp_id, location, price_per_kwh, status,
                        current_consumption, current_amount, driver_id,
                        last_heartbeat, total_energy_supplied, total_revenue,
                        registration_date, last_supply_message, supply_ending,
                        supply_ended_time
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    cp.cp_id, cp.location, cp.price_per_kwh, cp.status,
                    cp.current_consumption, cp.current_amount, cp.driver_id,
                    cp.last_heartbeat, cp.total_energy_supplied, cp.total_revenue,
                    cp.registration_date, cp.last_supply_message, cp.supply_ending,
                    cp.supply_ended_time
                ))
            
            conn.commit()
            cursor.close()
            self.return_connection(conn)
            
            logger.debug(f"💾 CP {cp.cp_id} guardado en PostgreSQL")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error guardando CP {cp.cp_id}: {e}")
            return False
    
    def save_data(self):
        """Guarda todos los datos en la base de datos"""
        if not self.connection_pool:
            logger.warning("⚠️ No se pueden guardar datos: PostgreSQL no disponible")
            return
            
        try:
            logger.debug("💾 Guardando datos en PostgreSQL...")
            
            # Guardar todos los puntos de carga
            saved_count = 0
            for cp in self.charging_points.values():
                if self.save_charging_point(cp):
                    saved_count += 1
            
            # Guardar conductores
            conn = self.get_connection()
            cursor = conn.cursor()
            
            for driver_id in self.drivers:
                cursor.execute("""
                    INSERT INTO drivers (driver_id) 
                    VALUES (%s) 
                    ON CONFLICT (driver_id) DO NOTHING
                """, (driver_id,))
            
            conn.commit()
            cursor.close()
            self.return_connection(conn)
            
            logger.info(f"💾 Datos guardados en PostgreSQL: {saved_count}/{len(self.charging_points)} CPs, {len(self.drivers)} conductores")
            
        except Exception as e:
            logger.error(f"❌ Error guardando datos en PostgreSQL: {e}", exc_info=True)
    
    def add_charging_point(self, cp: ChargingPoint) -> bool:
        """Añade un punto de carga Y guarda automáticamente"""
        # Verificar si CP ya existe
        existing_cp = self.charging_points.get(cp.cp_id)
        
        if existing_cp:
            # PERMITIR reemplazo SOLO si DESCONECTADO
            if existing_cp.status == "DESCONECTADO":
                logger.info(f"🔄 Reemplazando CP {cp.cp_id} en estado DESCONECTADO")
                self.charging_points[cp.cp_id] = cp
                self.save_charging_point(cp)
                return True
            else:
                # NO permitir cualquier otro estado
                logger.warning(f"🚫 Intento de registrar CP duplicado: {cp.cp_id} - "
                            f"Estado actual: {existing_cp.status}")
                return False
        
        # Si no existe o está DESCONECTADO entonces añadir 
        self.charging_points[cp.cp_id] = cp
        logger.info(f"➕ Punto de carga {cp.cp_id} registrado - Ubicación: {cp.location}, Precio: €{cp.price_per_kwh}/kWh")
        self.save_charging_point(cp)
        return True

    def get_charging_point(self, cp_id: str) -> Optional[ChargingPoint]:
        return self.charging_points.get(cp_id)
    
    def get_all_cps(self) -> List[dict]:
        return [cp.parse() for cp in self.charging_points.values()]
    
    def register_driver(self, driver_id: str):
        """Registra un conductor Y guarda automáticamente"""
        self.drivers.add(driver_id)
        logger.info(f"👤 Conductor {driver_id} registrado")
        
        if not self.connection_pool:
            logger.warning(f"⚠️ No se puede guardar conductor {driver_id}: PostgreSQL no disponible")
            return
            
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO drivers (driver_id) 
                VALUES (%s) 
                ON CONFLICT (driver_id) DO NOTHING
            """, (driver_id,))
            conn.commit()
            cursor.close()
            self.return_connection(conn)
        except Exception as e:
            logger.error(f"❌ Error registrando conductor {driver_id} en BD: {e}")
    
    def add_transaction(self, transaction_data: dict):
        """Añade una transacción al historial en PostgreSQL"""
        if not self.connection_pool:
            logger.warning(f"⚠️ No se puede guardar transacción: PostgreSQL no disponible")
            return False
            
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO transactions (
                    transaction_id, cp_id, driver_id, energy_consumed,
                    amount, price_per_kwh, status, failure_reason,
                    start_time, end_time, location
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (transaction_id) DO NOTHING
            """, (
                transaction_data.get('transaction_id'),
                transaction_data.get('cp_id'),
                transaction_data.get('driver_id'),
                transaction_data.get('energy_consumed'),
                transaction_data.get('amount'),
                transaction_data.get('price_per_kwh'),
                transaction_data.get('status'),
                transaction_data.get('failure_reason'),
                transaction_data.get('start_time'),
                transaction_data.get('end_time'),
                transaction_data.get('location')
            ))
            
            conn.commit()
            cursor.close()
            self.return_connection(conn)
            
            logger.debug(f"📝 Transacción añadida: {transaction_data.get('transaction_id')}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error añadiendo transacción: {e}")
            return False
    
    def backup_data(self, backup_type: str = "automatic", description: str = None):
        """Crea una copia de seguridad de los datos en PostgreSQL"""
        if not self.connection_pool:
            logger.warning("⚠️ No se puede crear backup: PostgreSQL no disponible")
            return False
            
        try:
            logger.info("📦 Creando backup en PostgreSQL...")
            
            # Preparar datos para el backup
            backup_data = {
                'charging_points': [cp.parse() for cp in self.charging_points.values()],
                'drivers': list(self.drivers),
                'timestamp': datetime.now().isoformat(),
                'backup_type': backup_type
            }
            
            if description:
                backup_data['description'] = description
            
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Insertar backup
            cursor.execute("""
                INSERT INTO backups (backup_type, description, data)
                VALUES (%s, %s, %s)
            """, (
                backup_type,
                description or f"Backup {backup_type} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                json.dumps(backup_data, ensure_ascii=False)
            ))
            
            conn.commit()
            cursor.close()
            self.return_connection(conn)
            
            logger.info(f"✅ Backup creado en PostgreSQL - Tipo: {backup_type}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error creando backup en PostgreSQL: {e}", exc_info=True)
            return False
    
    def restore_from_backup(self, backup_id: int = None):
        """Restaura datos desde un backup específico o el más reciente"""
        if not self.connection_pool:
            logger.warning("⚠️ No se puede restaurar backup: PostgreSQL no disponible")
            return False
            
        try:
            conn = self.get_connection()
            cursor = conn.cursor(cursor_factory=extras.RealDictCursor)
            
            if backup_id:
                cursor.execute("SELECT * FROM backups WHERE backup_id = %s", (backup_id,))
            else:
                cursor.execute("SELECT * FROM backups ORDER BY created_at DESC LIMIT 1")
            
            backup = cursor.fetchone()
            
            if not backup:
                logger.warning("📭 No se encontraron backups disponibles")
                return False
            
            logger.info(f"🔄 Restaurando desde backup ID {backup['backup_id']} - {backup['backup_type']}")
            
            backup_data = backup['data']
            
            # Limpiar datos actuales en memoria
            self.charging_points.clear()
            self.drivers.clear()
            
            # Restaurar puntos de carga
            for cp_data in backup_data.get('charging_points', []):
                try:
                    cp = ChargingPoint.unparse(cp_data)
                    self.charging_points[cp.cp_id] = cp
                    self.save_charging_point(cp)
                except Exception as e:
                    logger.error(f"❌ Error restaurando CP {cp_data.get('cp_id', 'desconocido')}: {e}")
            
            # Restaurar conductores
            for driver_id in backup_data.get('drivers', []):
                self.drivers.add(driver_id)
                cursor.execute("""
                    INSERT INTO drivers (driver_id) 
                    VALUES (%s) 
                    ON CONFLICT (driver_id) DO NOTHING
                """, (driver_id,))
            
            conn.commit()
            cursor.close()
            self.return_connection(conn)
            
            logger.info(f"✅ Datos restaurados desde backup - {len(self.charging_points)} CPs, {len(self.drivers)} conductores")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error restaurando desde backup: {e}", exc_info=True)
            return False
    
    def get_backup_list(self):
        """Obtiene lista de backups disponibles"""
        if not self.connection_pool:
            logger.warning("⚠️ No se pueden listar backups: PostgreSQL no disponible")
            return []
            
        try:
            conn = self.get_connection()
            cursor = conn.cursor(cursor_factory=extras.RealDictCursor)
            
            cursor.execute("""
                SELECT backup_id, backup_type, description, created_at
                FROM backups 
                ORDER BY created_at DESC
                LIMIT 20
            """)
            
            backups = cursor.fetchall()
            cursor.close()
            self.return_connection(conn)
            
            return backups
            
        except Exception as e:
            logger.error(f"❌ Error obteniendo lista de backups: {e}")
            return []

import json
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

class RealKafkaManager:
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.consumers = {}
        
        try:
            
            def custom_serializer(v):
                if isinstance(v, Decimal):
                    return float(v)
                if isinstance(v, (datetime, date)):
                    return v.isoformat()
                raise TypeError(f"Type {type(v)} not serializable")

            self.producer = KafkaProducer(
                bootstrap_servers=[self.bootstrap_servers],
                value_serializer=lambda v: json.dumps(v, default=custom_serializer).encode('utf-8'),
                retries=2,
                acks=1,
                linger_ms=5,
                batch_size=16384,
                buffer_memory=33554432,
                max_block_ms=5000
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
    
    def get_messages(self, topic: str, consumer_group: str):
        """Obtiene mensajes de un topic real de Kafka"""
        try:
            logger.debug(f"🔍 get_messages llamado para topic: {topic}, group: {consumer_group}")
            
            if topic not in self.consumers or consumer_group not in self.consumers[topic]:
                logger.debug(f"🆕 Creando nuevo consumer para: {topic}, group: {consumer_group}")
                consumer = KafkaConsumer(
                    topic,
                    bootstrap_servers=[self.bootstrap_servers],
                    auto_offset_reset='earliest',
                    enable_auto_commit=True,
                    auto_commit_interval_ms=500,
                    group_id=consumer_group,
                    value_deserializer=lambda x: x.decode('utf-8') if x else None,
                    consumer_timeout_ms=100,
                    max_poll_records=10,
                    session_timeout_ms=10000,
                    heartbeat_interval_ms=3000
                )
                if topic not in self.consumers:
                    self.consumers[topic] = {}
                self.consumers[topic][consumer_group] = consumer
                logger.debug(f"✅ Consumer creado para {topic}, group: {consumer_group}")
            
            consumer = self.consumers[topic][consumer_group]
            messages = []

            logger.debug(f"📥 Polling mensajes de {topic}...")
            try:
                records = consumer.poll(timeout_ms=500)
            except Exception as e:
                if "KafkaConsumer is closed" in str(e):
                    logger.warning(f"🔄 Consumer cerrado, recreando para {topic}, group: {consumer_group}")
                    del self.consumers[topic][consumer_group]
                    return self.get_messages(topic, consumer_group)
                else:
                    raise e

            for topic_partition, message_batch in records.items():
                for message in message_batch:
                    if message.value:
                        try:
                            message_dict = json.loads(message.value)
                            messages.append({
                                'timestamp': datetime.fromtimestamp(message.timestamp / 1000),
                                'message': message_dict
                            })
                            logger.debug(f"   📨 Mensaje recibido: {message_dict}")
                        except json.JSONDecodeError as e:
                            logger.error(f"❌ Error parseando JSON: {e}")
                        except Exception as e:
                            logger.error(f"❌ Error procesando mensaje: {e}")

            return messages

        except Exception as e:
            logger.error(f"❌ Error en get_messages para {topic}: {e}")
            if topic in self.consumers and consumer_group in self.consumers[topic] and "closed" in str(e).lower():
                logger.warning(f"🗑️ Eliminando consumer cerrado para {topic}, group: {consumer_group}")
                del self.consumers[topic][consumer_group]
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
            elif message.startswith('DEREGISTER_CP'):
                parts = message.split('#')
                self.handle_cp_deregistration(parts[1:], client_socket)
            else:
                logger.warning(f"⚠️ Mensaje desconocido: {message}")
            
        except Exception as e:
            logger.error(f"❌ Error procesando mensaje: {e}", exc_info=True)  

    def handle_cp_deregistration(self, params: List[str], client_socket):
        """Maneja deregistro de puntos de carga"""
        if len(params) < 1:
            logger.error("❌ Parámetros insuficientes para deregistro")
            response = "ERROR#Parámetros_insuficientes"
            client_socket.send(response.encode('utf-8'))
            return
            
        cp_id = params[0]
        
        if cp_id not in self.central.database.charging_points:
            response = f"ERROR#CP_no_registrado#{cp_id}"
            client_socket.send(response.encode('utf-8'))
            logger.warning(f"🚫 Intento de deregistro de CP no registrado: {cp_id}")
            return
        
        cp = self.central.database.charging_points[cp_id]
        
        if cp.status == "SUMINISTRANDO":
            logger.info(f"🔄 CP {cp_id} en SUMINISTRANDO - Finalizando suministro antes de eliminar")
            
            # 1. Cambiar a PARADO para generar ticket automáticamente
            self.central.update_cp_status(cp_id, "DESCONECTADO")
            logger.info(f"🎫 Ticket generado para CP {cp_id}")
            
            # Esperar un momento para que se procese el ticket
            time.sleep(0.5)
        
        del self.central.database.charging_points[cp_id]
        logger.info(f"🗑️ CP {cp_id} eliminado del diccionario en memoria")

        response = "DEREGISTER_OK"
        client_socket.send(response.encode('utf-8'))
        logger.info(f"✅ CP {cp_id} deregistrado correctamente via socket")

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
        secret_key = params[3]
        
        # Verificar si CP ya existe
        existing_cp = self.central.database.get_charging_point(cp_id)

        if self.central.database.get_secret_key(cp_id, secret_key) is False:
            logger.warning(f"🚫 CP {cp_id} intento de registro con clave secreta inválida")
            response = f"ERROR#Clave_secreta_inválida#{cp_id}"
            client_socket.send(response.encode('utf-8'))
            return
        
        if existing_cp:
            # PERMITIR reconexión SOLO si DESCONECTADO
            if existing_cp.status == "DESCONECTADO" or existing_cp.status == "AVERIADO":
                logger.info(f"🔄 CP {cp_id} reconectando - Estado anterior: DESCONECTADO")
                
                # Actualizar datos del CP existente
                existing_cp.location = location
                existing_cp.price_per_kwh = price
                existing_cp.socket_connection = client_socket
                existing_cp.last_heartbeat = datetime.now()
                existing_cp.status = "ACTIVADO"  # ACTIVADO al reconectar
                
                # Guardar cambios
                self.central.database.save_charging_point(existing_cp)
                
                # REGISTER_OK
                response = "REGISTER_OK"
                client_socket.send(response.encode('utf-8'))
                logger.info(f"✅ CP {cp_id} reconectado correctamente")
                return
                
            else:
                # NO permitir reconexión si el CP CUALQUIER OTRO estado
                logger.warning(f"🚫 CP {cp_id} ya registrado - Estado actual: {existing_cp.status} - No se permite reconexión")
                response = f"ERROR#CP_ya_registrado#{cp_id}#Estado:{existing_cp.status}"
                client_socket.send(response.encode('utf-8'))
                return
        
        # Si no existe, crear nuevo CP
        cp = ChargingPoint(cp_id, location, price)
        cp.socket_connection = client_socket
        cp.last_heartbeat = datetime.now()
        cp.status = "ACTIVADO"  # Estado inicial al registrar
        
        if self.central.database.add_charging_point(cp):
            response = "REGISTER_OK"
            client_socket.send(response.encode('utf-8'))
            logger.info(f"✅ Punto de carga {cp_id} registrado correctamente via socket")
        else:
            response = f"ERROR#CP_ya_registrado#{cp_id}"
            client_socket.send(response.encode('utf-8'))
            logger.error(f"❌ Error inesperado registrando CP {cp_id}")
    
    def handle_cp_ok(self, params: List[str], client_socket):
        """Maneja ok de puntos de carga"""
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
                # Actualizar conexión socket y heartbeat
                cp.socket_connection = client_socket
                cp.last_heartbeat = datetime.now()
            
                status_changed = False
                # SOLO cambiar si DESCONECTADO
                if cp.status == "DESCONECTADO": # or cp.status == "AVERIADO": #(cambiado por weather)
                    self.central.update_cp_status(cp_id, "ACTIVADO")
                    logger.info(f"🔄 CP {cp_id} reactivado - Estado cambiado a ACTIVADO")
                    status_changed = True
                
                # Enviar respuesta simple
                response = "CP_OK_ACK"
                client_socket.send(response.encode('utf-8'))
                logger.debug(f"💓 Heartbeat recibido de {cp_id} - Confirmación enviada")
                
            else:
                response = f"ERROR#CP_no_encontrado"
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
            
            # STOP si suministrando
            if was_supplying:
                logger.info(f"🛑 Enviando comando STOP a CP {cp_id} por avería")
                
                # Enviar STOP via Kafka
                control_message = {
                    'cp_id': cp_id,
                    'command': 'STOP',
                    'reason': f'AVERIA_CP: {reason}',
                    'timestamp': datetime.now().isoformat(),
                    'source': 'central'
                }
                self.central.kafka_manager.send_message('control_commands', control_message)
                
                # Registrar transacción fallida
                self.central.record_failed_transaction(cp, reason, driver_id)
                logger.info(f"🔌 Suministro interrumpido para conductor {driver_id} por avería en CP {cp_id}")
            
            # Actualizar estado a AVERIADO
            self.central.update_cp_status(cp_id, "AVERIADO")
            
            response = f"CP_KO_ACK#{cp_id}"
            client_socket.send(response.encode('utf-8'))
            
            logger.info(f"🔴 CP {cp_id} puesto en estado AVERIADO y suministro detenido")
        else:
            logger.error(f"❌ CP {cp_id} no encontrado para mensaje CP_KO")
            response = f"ERROR#CP_no_encontrado"
            client_socket.send(response.encode('utf-8'))

class EVCentral:
    """Clase principal del sistema central con persistencia en PostgreSQL"""
    
    def __init__(self, socket_host: str, socket_port: int, kafka_servers: str):
        # Ahora DatabaseManager no necesita data_file
        self.database = DatabaseManager()
        
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
        logger.info("💾 Datos guardados correctamente en PostgreSQL")
        self.database.backup_data("shutdown", "Backup antes del apagado")
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

            test_msg = {
                'test': True,
                'timestamp': datetime.now().isoformat(),
                'source': 'central_test'
            }

            logger.info("📤 Enviando mensaje de test a supply_response...")
            self.kafka_manager.send_message('supply_response', test_msg)
            logger.info("✅ Mensaje enviado")

            logger.info("📥 Intentando recibir mensajes...")
            messages = self.kafka_manager.get_messages('supply_response', consumer_group='test_kafka_detailed_group')
            logger.info(f"📦 Mensajes recibidos: {len(messages)}")

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

    def handle_supply_ended(self, cp_id: str, driver_id: str = None):
        """Maneja la finalización de suministro """
        logger.info(f"🛑 handle_supply_ended - CP: {cp_id}, Driver: {driver_id}")
        
        cp = self.database.get_charging_point(cp_id)
        if not cp:
            logger.error(f"❌ CP {cp_id} no encontrado en handle_supply_ended")
            return
        
        if cp.status == "SUMINISTRANDO":
            logger.info(f"🔄 Finalizando suministro para CP {cp_id}")
            
            # Registrar transacción
            transaction_data = self.record_transaction(cp, "COMPLETED")
            
            # Enviar tickets
            if transaction_data:
                self.send_ticket(cp_id, transaction_data)
            
            if cp.status != "PARADO":
                # Estado a ACTIVADO
                self.update_cp_status(cp_id, "ACTIVADO")
            
            # Reset flags de control
            cp.supply_ending = False
            cp.supply_ended_time = datetime.now()
            
            logger.info(f"✅ Suministro finalizado correctamente - CP {cp_id} ahora en estado ACTIVADO")
        else:
            logger.warning(f"⚠️ Intento de finalizar suministro en CP {cp_id} que no estaba SUMINISTRANDO (estado: {cp.status})")

    def handle_charging_failure(self, cp_id: str, failure_reason: str, driver_id: str = None):
        """Maneja fallos de carga durante el suministro """
        logger.warning(f"🚨 handle_charging_failure - CP: {cp_id}, Razón: {failure_reason}, Driver: {driver_id}")
        
        cp = self.database.get_charging_point(cp_id)
        if not cp:
            logger.error(f"❌ CP {cp_id} no encontrado en handle_charging_failure")
            return
        
        if cp.status == "SUMINISTRANDO":
            logger.info(f"🛑 Detectada carga fallida en CP {cp_id} - Razón: {failure_reason}")
            
            # REgistrar transacción fallida
            real_driver_id = driver_id or cp.driver_id
            if real_driver_id:
                transaction_data = self.record_failed_transaction(cp, failure_reason, real_driver_id)
                if transaction_data:
                    logger.info(f"❌ Transacción fallida registrada para CP {cp_id}")
            
            # Enviar fallo
            if real_driver_id and real_driver_id != "MANUAL":
                self.send_failure_notification_to_driver(real_driver_id, cp_id, failure_reason, 
                                                       transaction_data or {})
            
            # Enviar comando STOP
            control_message = {
                'cp_id': cp_id,
                'command': 'STOP',
                'reason': f'CARGA_FALLIDA: {failure_reason}',
                'timestamp': datetime.now().isoformat(),
                'source': 'central',
                'emergency': True
            }
            self.kafka_manager.send_message('control_commands', control_message)
            logger.info(f"🛑 Comando STOP de emergencia enviado a CP {cp_id}")
            
            # Estado a AVERIADO
            self.update_cp_status(cp_id, "AVERIADO")
            
            # Reset flags
            cp.supply_ending = False
            cp.supply_ended_time = datetime.now()
            
            logger.info(f"🔴 CP {cp_id} puesto en estado AVERIADO por carga fallida")
        else:
            logger.warning(f"⚠️ Carga fallida recibida para CP {cp_id} que no estaba SUMINISTRANDO (estado: {cp.status})")

    def start(self):
        """Inicia servicios del sistema central"""
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
        print("  backups        - Listar backups disponibles")
        print("  restore        - Restaurar último backup")
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
                    if self.database.backup_data("manual", "Backup manual desde consola"):
                        print("✅ Copia de seguridad creada")
                    else:
                        print("❌ Error creando copia de seguridad")
                elif command == 'backups':
                    self.show_backup_list()
                elif command == 'restore':
                    if self.database.restore_from_backup():
                        print("✅ Sistema restaurado desde backup")
                    else:
                        print("❌ Error restaurando desde backup")
                elif command == 'stats':
                    self.show_statistics()
                elif command.startswith('stop '):
                    cp_id = command[5:].strip().upper()
                    self.send_control_command(cp_id, "STOP")
                elif command.startswith('resume '):
                    cp_id = command[7:].strip().upper()
                    self.send_control_command(cp_id, "RESUME")
                else:
                    print("❌ Comando no reconocido. Use 'stop <cp_id>', 'resume <cp_id>', 'status', 'stats', 'save', 'backup', 'backups', 'restore' o 'quit'")
                    
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
    
    def show_backup_list(self):
        """Muestra la lista de backups disponibles"""
        backups = self.database.get_backup_list()
        
        if not backups:
            print("📭 No hay backups disponibles")
            return
        
        print("\n" + "="*80)
        print("BACKUPS DISPONIBLES")
        print("="*80)
        print(f"{'ID':<8} {'Tipo':<12} {'Fecha':<20} {'Descripción'}")
        print("-"*80)
        
        for backup in backups:
            backup_id = backup['backup_id']
            backup_type = backup['backup_type']
            created_at = backup['created_at'].strftime('%Y-%m-%d %H:%M:%S')
            description = backup.get('description', 'Sin descripción')
            
            print(f"{backup_id:<8} {backup_type:<12} {created_at:<20} {description}")
        
        print("="*80)
        print("Para restaurar un backup específico: restore <backup_id>")
        print("Para restaurar el último backup: restore")
        print("="*80)
    
    def auto_save(self):
        """Guarda los datos automáticamente cada cierto tiempo"""
        while self.running:
            time.sleep(self.auto_save_interval)
            try:
                self.database.save_data()
                logger.debug("💾 Guardado automático realizado en PostgreSQL")
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
    
    def update_cp_status(self, cp_id: str, status: str, consumption: float = 0.0, amount: float = 0.0, driver_id: str = None):
        """Actualiza el estado de un punto de carga"""
        cp = self.database.get_charging_point(cp_id)
        if not cp:
            logger.error(f"❌ CP {cp_id} no encontrado en update_cp_status")
            return
            
        if driver_id:
            cp.driver_id = driver_id
            logger.info(f"🔍 Driver ID actualizado: {driver_id} para CP {cp_id}")

        previous_status = cp.status

        if status == "SUMINISTRANDO" and previous_status != "SUMINISTRANDO":
            logger.info(f"🔄 Iniciando nuevo suministro - Reseteando contadores para CP {cp_id}")
            cp.supply_ended_time = None  
            cp.supply_ending = False
            cp.last_supply_message = None

        logger.info(f"🔍 CP: {cp_id}, Estado anterior: '{previous_status}', Nuevo estado: '{status}'")
        logger.info(f"🔍 Consumo: {consumption}, Importe: {amount}, Driver: {driver_id}")
        
        # Verificar si realmente hay un cambio de estado
        if previous_status == status and status != "SUMINISTRANDO": ########
            logger.info(f"🔍 Mismo estado, no hay cambio necesario")
            return

        transaction_data = None
        
        # Cuando termina un suministro
        if previous_status == "SUMINISTRANDO" and status != "SUMINISTRANDO" and status != "AVERIADO":
            logger.info(f"🔍 Finalizando suministro - registrando transacción")
            if float(cp.current_amount) > 0.01:
                transaction_data = self.record_transaction(cp, "COMPLETED")
                if transaction_data:
                    logger.info(f"💰 Transacción registrada para CP {cp_id}")
                    self.send_ticket(cp_id, transaction_data)
            else:
                logger.info(f"🔍 Sin importe significativo, no se registra transacción")
        
        # Averías
        if status == "AVERIADO" and previous_status == "SUMINISTRANDO":
            logger.info(f"🔍 Avería durante suministro - registrando transacción fallida")
            affected_driver = cp.driver_id
            if affected_driver and cp.current_amount > 0:
                self.record_failed_transaction(cp, "Avería del punto de carga", affected_driver)
            
            cp.current_consumption = 0.0
            cp.current_amount = 0.0
            cp.driver_id = None
        
        # ACTUALIZAR ESTADO
        logger.info(f"🔍 Aplicando cambio de estado: '{previous_status}' -> '{status}'")
        cp.status = status
        cp.current_consumption = consumption
        cp.current_amount = amount
        
        if driver_id:
            cp.driver_id = driver_id
        
        # Actualizar estadísticas si suministrando
        if status == "SUMINISTRANDO" and consumption > 0:
            energy_increment = consumption / 3600
            cp.total_energy_supplied = float(cp.total_energy_supplied) + float(energy_increment)
            cp.total_revenue = float(cp.total_revenue) + float(amount)
        
        # Guardar los cambios en base de datos
        logger.info(f"🔍 Guardando cambios en base de datos...")
        self.database.save_charging_point(cp)
        
        # Enviar actualización via Kafka
        logger.info(f"🔍 Enviando actualización via Kafka...")
        self.kafka_manager.send_message('central_updates', cp.parse())
        
        logger.info(f"✅ Estado actualizado - CP: {cp_id}, Estado: {cp.status}")
        logger.info(f"🔍 DEBUG update_cp_status FIN - Estado verificado: '{cp.status}'")
        
    def record_transaction(self, cp: ChargingPoint, status: str):
        """Registra una transacción completada"""
        try:
            transaction_id = f"TXN{int(time.time())}_{cp.cp_id}"
            
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
                'location': cp.location,
                'timestamp': datetime.now().isoformat()
            }
            
            if self.database.add_transaction(transaction):
                logger.info(f"💰 Transacción registrada: {transaction_id} - CP: {cp.cp_id}, Importe: €{cp.current_amount:.2f}")
                
                # Reset contadores después de registrar transacción
                cp.current_consumption = 0.0
                cp.current_amount = 0.0
                
                return transaction
            else:
                return None
                
        except Exception as e:
            logger.error(f"❌ Error registrando transacción: {e}", exc_info=True)
            return None

    def send_ticket(self, cp_id: str, transaction_data: dict):
        """Envía ticket al Engine Y al Driver """
        try:
            if not transaction_data:
                logger.warning(f"⚠️ No se puede enviar ticket: datos de transacción vacíos")
                return
            
            base_ticket = {
                'cp_id': cp_id,
                'type': 'CHARGING_TICKET',
                'ticket_id': transaction_data['transaction_id'],
                'energy_consumed': transaction_data['energy_consumed'],
                'amount': transaction_data['amount'],
                'price_per_kwh': transaction_data['price_per_kwh'],
                'start_time': transaction_data['start_time'],
                'end_time': transaction_data['end_time'],
                'timestamp': datetime.now().isoformat()
            }
            
            # Ticket para Engine
            engine_ticket = base_ticket.copy()
            engine_ticket['driver_id'] = transaction_data.get('driver_id', 'MANUAL')
            self.kafka_manager.send_message('engine_tickets', engine_ticket)
            logger.info(f"🎫 Ticket enviado a Engine - CP: {cp_id}, Transacción: {transaction_data['transaction_id']}")
            
            # Ticket para Driver
            driver_id = transaction_data.get('driver_id')
            if driver_id and driver_id != "MANUAL":
                driver_ticket = base_ticket.copy()
                driver_ticket['driver_id'] = driver_id
                driver_ticket['location'] = transaction_data.get('location', 'Desconocida')
                self.kafka_manager.send_message('driver_tickets', driver_ticket)
                logger.info(f"🎫 Ticket enviado a Driver {driver_id} - CP: {cp_id}")
                
        except Exception as e:
            logger.error(f"❌ Error enviando ticket: {e}", exc_info=True)

    def record_failed_transaction(self, cp: ChargingPoint, failure_reason: str, driver_id: str):
        """Registra transacción fallida por avería"""
        try:
            transaction_id = f"TXN_FAIL_{int(time.time())}_{cp.cp_id}"
            
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
                'location': cp.location,
                'timestamp': datetime.now().isoformat()
            }
            
            if self.database.add_transaction(transaction):
                logger.info(f"❌ Transacción fallida registrada: {transaction_id} - Razón: {failure_reason}")
                
                if driver_id and driver_id != "MANUAL":
                    self.send_failure_notification_to_driver(driver_id, cp.cp_id, failure_reason, transaction)
                else:
                    # Notificar solo al Engine si es suministro manual
                    self.send_failure_notification_to_engine(cp.cp_id, failure_reason, transaction)
                
                # Resetear contadores del CP
                cp.current_consumption = 0.0
                cp.current_amount = 0.0
                cp.driver_id = None
                
                return transaction
            else:
                return None
                
        except Exception as e:
            logger.error(f"❌ Error registrando transacción fallida: {e}", exc_info=True)
            return None

    def send_failure_notification_to_driver(self, driver_id: str, cp_id: str, failure_reason: str, transaction_data: dict):
        """Notifica al conductor sobre una avería durante el suministro"""
        try:
            failure_message = {
                'driver_id': driver_id,
                'type': 'CHARGING_FAILED',
                'cp_id': cp_id,
                'failure_reason': failure_reason,
                'energy_consumed': transaction_data.get('energy_consumed', 0),
                'amount_charged': transaction_data.get('amount', 0),
                'transaction_id': transaction_data.get('transaction_id'),
                'timestamp': datetime.now().isoformat()
            }
            
            self.kafka_manager.send_message('driver_responses', failure_message)
            logger.info(f"🚨 Notificación de avería enviada a conductor {driver_id}")
            
        except Exception as e:
            logger.error(f"❌ Error enviando notificación de fallo a conductor {driver_id}: {e}")

    def send_failure_notification_to_engine(self, cp_id: str, failure_reason: str, transaction_data: dict):
        """Notifica al Engine sobre una avería durante el suministro"""
        try:
            failure_message = {
                'cp_id': cp_id,
                'type': 'CHARGING_FAILED',
                'failure_reason': failure_reason,
                'energy_consumed': transaction_data.get('energy_consumed', 0),
                'amount_charged': transaction_data.get('amount', 0),
                'transaction_id': transaction_data.get('transaction_id'),
                'timestamp': datetime.now().isoformat()
            }
            
            self.kafka_manager.send_message('supply_response', failure_message)
            logger.info(f"🚨 Notificación de avería enviada a Engine {cp_id}")
            
        except Exception as e:
            logger.error(f"❌ Error enviando notificación de fallo a Engine {cp_id}: {e}")
    
    def process_control_command(self, message):
        """Procesa comandos externos (ej: desde API Weather)"""
        try:
            # Si el mensaje viene como string, parsearlo
            if isinstance(message, str):
                message = json.loads(message)
            
            command = message.get('command')
            cp_id = message.get('cp_id')
            reason = message.get('reason', 'Sin razón especificada')
            source = message.get('source', 'unknown')

            # Ignoramos los comandos enviados por la propia Central para evitar bucles
            if source == 'central':
                return

            logger.info(f"📨 Comando externo recibido: {command} para CP {cp_id} (Origen: {source})")

            cp = self.database.get_charging_point(cp_id)
            if not cp:
                logger.error(f"❌ CP {cp_id} no encontrado para comando externo")
                return

            if command == 'FAILURE':
                logger.warning(f"❄️ ALERTA RECIBIDA: Poniendo CP {cp_id} en AVERIADO. Razón: {reason}")
                # Esto actualiza la variable en memoria Y la Base de Datos PostgreSQL
                self.update_cp_status(cp_id, "AVERIADO")
                
                # Opcional: Si estaba cargando, forzar parada
                if cp.status == "SUMINISTRANDO":
                    self.handle_charging_failure(cp_id, reason)

            elif command == 'RESUME':
                logger.info(f"☀️ RESTABLECIMIENTO: Poniendo CP {cp_id} en ACTIVADO")
                self.update_cp_status(cp_id, "ACTIVADO")

            elif command == 'STOP':
                logger.info(f"🛑 STOP Remoto recibido para CP {cp_id}")
                
                # Si estaba suministrando, cerramos la sesión formalmente
                if cp.status == "SUMINISTRANDO":
                    logger.info(f"🛑 Cerrando sesión y generando tickets para CP {cp_id}...")
                    
                    # 1. Registrar Transacción (Calcula totales y cierra sesión DB)
                    transaction_data = self.record_transaction(cp, "COMPLETED")
                    
                    # 2. Enviar Tickets (Al Driver y al Engine)
                    if transaction_data:
                        self.send_ticket(cp_id, transaction_data)
                        logger.info(f"🎫 Tickets enviados por parada remota en CP {cp_id}")
                    
                    # 3. Cambiar estado a PARADO
                    self.update_cp_status(cp_id, "PARADO")
                    
                # Si solo estaba activado, pasamos a parado directamente
                else:
                    self.update_cp_status(cp_id, "PARADO")
                    logger.info(f"⏹️ CP {cp_id} puesto en estado PARADO")
                    
        except Exception as e:
            logger.error(f"❌ Error procesando comando externo: {e}", exc_info=True)

    def send_control_command(self, cp_id: str, command: str):
        """Envía comandos de control a CPs via Kafka """
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
            
            print(f"✅ Comando {command} enviado a punto de carga {cp_id}")
            # Actualización de estado
            if command == "STOP":
                # Si estaba suministrando, cerramos la sesión formalmente
                if cp.status == "SUMINISTRANDO":
                    logger.info(f"🛑 Parada Manual: Cerrando sesión y generando tickets para CP {cp_id}")
                    
                    # 1. Registrar Transacción (Esto pone a 0 los contadores del CP)
                    # Usamos status "COMPLETED" ya que es una parada controlada, no un fallo técnico
                    transaction_data = self.record_transaction(cp, "COMPLETED")
                    
                    # 2. Enviar Tickets (Al Driver y al Engine)
                    if transaction_data:
                        self.send_ticket(cp_id, transaction_data)
                        logger.info(f"🎫 Tickets enviados por parada manual en CP {cp_id}")
                    
                    # 3. Cambiar estado a PARADO
                    # Al haber llamado a record_transaction antes, el current_amount es 0.
                    # Esto evita que update_cp_status genere un ticket duplicado.
                    self.update_cp_status(cp_id, "PARADO")
                    
                # Si solo estaba activado, pasamos a parado directamente
                elif cp.status == "ACTIVADO":
                    self.update_cp_status(cp_id, "PARADO")
                    logger.info(f"⏹️ CP {cp_id} puesto en estado PARADO")
            elif command == "START":
                if cp.status == "ACTIVADO" and cp.status != "PARADO":
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
        """Loop principal para consumir mensajes de Kafka"""
        logger.info("🚀 Kafka consumer loop iniciado correctamente")

        instance_id = getattr(self, "id", "central")

        loop_count = 0
        while self.running:
            try:
                loop_count += 1
                if loop_count % 10 == 0:
                    logger.debug(f"🔄 Ciclo #{loop_count}")

                critical_topics = ['supply_flow', 'supply_response', 'driver_requests']
                normal_topics = ['control_commands', 'driver_tickets', 'engine_tickets']
                all_messages = {}

                # Procesar topics críticos
                for topic in critical_topics:
                    try:
                        group_id = f"{instance_id}_{topic}_group"
                        messages = self.kafka_manager.get_messages(topic, consumer_group=group_id)
                        if messages:
                            all_messages[topic] = messages
                            logger.debug(f"🎯 {len(messages)} mensajes en {topic}")
                    except Exception as e:
                        logger.error(f"❌ Error en topic crítico {topic}: {e}")

                # Procesar mensajes críticos
                for topic, messages in all_messages.items():
                    for msg_data in messages:
                        try:
                            message_content = msg_data['message']
                            if topic == 'supply_response':
                                self.process_supply_response(message_content)
                            elif topic == 'supply_flow':
                                self.process_supply_flow(message_content)
                            elif topic == 'driver_requests':
                                self.process_driver_request(message_content)
                        except Exception as e:
                            logger.error(f"❌ Error procesando mensaje en {topic}: {e}")

                # Procesar topics normales
                for topic in normal_topics:
                    try:
                        group_id = f"{instance_id}_{topic}_group"
                        messages = self.kafka_manager.get_messages(topic, consumer_group=group_id)
                        if messages:
                            logger.debug(f"📦 {len(messages)} mensajes en {topic}")
                            for msg_data in messages:
                                if topic == 'control_commands':
                                    self.process_control_command(msg_data['message'])
                    except Exception as e:
                        logger.error(f"❌ Error en topic normal {topic}: {e}")

                time.sleep(0.5)

            except Exception as e:
                logger.error(f"💥 ERROR CRÍTICO en kafka_consumer_loop: {e}")
                logger.info("💤 Esperando 1 segundo antes de reintentar...")
                time.sleep(1)

        logger.info("🛑 Kafka consumer loop detenido")

    def process_supply_response(self, message):
        """Procesa respuestas de suministro desde el Engine"""
        logger.info(f"🎯 process_supply_response llamado con: {message} (TIPO: {type(message)})")
        try:
            # Tanto diccionarios como strings
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
            # Tickets
            if message.get('type') == 'CHARGING_TICKET':
                logger.info(f"🎫 Ticket recibido del Engine - CP: {message.get('cp_id')}")
                return

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

            # Solicita iniciar suministro
            if message_type == 'SUPPLY_REQUEST':
                if cp.status == "ACTIVADO" or cp.status == "SUMINISTRANDO":
                    # Autorizar suministro
                    driver_id = message.get('driver_id', 'MANUAL_ENGINE')
                    
                    # Enviar START al Engine
                    control_message = {
                        'cp_id': cp_id,
                        'command': 'START',
                        'driver_id': driver_id,
                        'timestamp': datetime.now().isoformat(),
                        'source': 'central'
                    }
                    self.kafka_manager.send_message('control_commands', control_message)
                    logger.info(f"✅ Suministro AUTORIZADO para CP {cp_id} - Comando START enviado")
                    
                    # Actualizar estado
                    self.update_cp_status(
                        cp_id=cp_id,
                        status="SUMINISTRANDO",
                        driver_id=driver_id
                    )
                else:
                    logger.warning(f"❌ CP {cp_id} no está ACTIVADO o SUMINISTRANDO - Estado actual: {cp.status}")
                    
            elif message_type == 'STOP_SUPPLY':

                if cp.status == "SUMINISTRANDO":
                    # Registrar transacción completada
                    transaction_data = self.record_transaction(cp, "COMPLETED")
                    
                    # Enviar ticket al Engine
                    self.send_ticket(cp_id, transaction_data)
                    
                    # Enviar comando STOP al Engine
                    control_message = {
                        'cp_id': cp_id,
                        'command': 'STOP',
                        'timestamp': datetime.now().isoformat(),
                        'source': 'central',
                        'transaction_id': transaction_data['transaction_id']
                    }
                    self.kafka_manager.send_message('control_commands', control_message)
                    logger.info(f"🛑 Suministro DETENIDO para CP {cp_id} - Ticket enviado")
                    
                    # Actualizar estado
                    self.update_cp_status(cp_id, "ACTIVADO")
                else:
                    logger.info(f"ℹ️ CP {cp_id} ya estaba detenido - Estado: {cp.status}")
                    
            else:
                logger.warning(f"⚠️ Tipo de mensaje no reconocido: {message_type}")

        except Exception as e:
            logger.error(f"❌ Error procesando respuesta de suministro: {e}", exc_info=True)
    
    def process_supply_flow(self, message):
        """Procesa mensajes de caudal de suministro"""
        logger.info(f"🎯 process_supply_flow llamado con: {message}")
        try:
            if isinstance(message, str):
                message = json.loads(message)
            elif not isinstance(message, dict):
                return

            cp_id = message.get('cp_id')
            driver_id = message.get('driver_id', 'MANUAL')
            kwh = float(message.get('kwh', 0.0))
            reason = message.get('reason', '')
            
            if not cp_id:
                return
            
            cp = self.database.get_charging_point(cp_id)
            if not cp:
                return

            # Finalización suministro
            if reason == 'SUPPLY_ENDED':
                self.handle_supply_ended(cp_id, driver_id)
                return

            # Carga fallida
            if 'CARGA FALLIDA' in reason.upper() or 'FALLIDO' in reason.upper() or 'AVERÍA' in reason.upper():
                logger.warning(f"🚨 Detectada carga fallida en mensaje supply_flow - CP: {cp_id}, Razón: {reason}")
                self.handle_charging_failure(cp_id, reason, driver_id)
                return

            real_driver_id = cp.driver_id if cp.driver_id else driver_id

            price_float = float(cp.price_per_kwh)
            current_amount = kwh * price_float
            
            # Solo actualizaciones si es un conductor real
            if real_driver_id and real_driver_id != "MANUAL" and real_driver_id != "MANUAL_ENGINE":
                total_revenue_float = float(cp.total_revenue)
                self.send_flow_update_to_driver(
                    driver_id=real_driver_id,
                    cp_id=cp_id,
                    flow_rate=1.0,
                    energy_delivered=kwh,
                    current_amount=current_amount,
                    total_amount=total_revenue_float + current_amount,
                    location=cp.location,
                    price_per_kwh=cp.price_per_kwh,
                    timestamp=datetime.now().isoformat()
                )
                logger.info(f"📤 Enviando actualización a driver {real_driver_id} - Energía: {kwh:.2f}kWh")

            # Suministro en progreso
            cp.last_supply_message = datetime.now()
            
            current_flow_rate = 1.0
            
            if cp.status != "PARADO":
                self.update_cp_status(
                    cp_id=cp_id,
                    status="SUMINISTRANDO",
                    consumption=current_flow_rate,
                    amount=current_amount,
                    driver_id=real_driver_id
                )
                
                cp.total_energy_supplied = kwh
                cp.total_revenue = current_amount
            
            logger.info(f"⚡ Suministro ACTIVO - CP: {cp_id}, Energía: {kwh:.2f}kWh, Driver: {real_driver_id}")
            
        except Exception as e:
            logger.error(f"❌ Error procesando mensaje de suministro: {e}")
            
    def send_flow_update_to_driver(self, driver_id: str, cp_id: str, flow_rate: float, energy_delivered: float, current_amount: float, total_amount: float = None, location: str = None, price_per_kwh: float = None, timestamp: str = None):
        """Envía actualizaciones de caudal al conductor """
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
                'total_amount': total_amount or cp.total_revenue,
                'location': location or cp.location,
                'price_per_kwh': price_per_kwh or cp.price_per_kwh,
                'timestamp': timestamp or datetime.now().isoformat()
            }
            
            self.kafka_manager.send_message('driver_responses', flow_message)
            logger.debug(f"📤 Actualización de caudal enviada a driver {driver_id} - "
                    f"Energía: {energy_delivered:.2f}kWh, Importe: €{current_amount:.2f}")
            
        except Exception as e:
            logger.error(f"❌ Error enviando actualización de caudal a driver {driver_id}: {e}", exc_info=True)

    def process_driver_request(self, message):
        """Procesa peticiones de suministro desde app de conductor"""
        logger.info(f"🎯 process_driver_request llamado con: {message} (TIPO: {type(message)})")
        try:
            # Tanto diccionarios como strings
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
            
            # Validación 
            if not driver_id:
                logger.error("❌ Parámetros insuficientes: falta driver_id")
                return
                
            logger.info(f"📨 Petición de conductor recibida - Driver: {driver_id}, Tipo: {request_type}, CP: {cp_id}")
            
            if request_type == 'SUPPLY_REQUEST':
                if not cp_id:
                    logger.error("❌ Parámetros insuficientes: falta cp_id en SUPPLY_REQUEST")
                    return
                self.handle_driver_supply_request(driver_id, cp_id)
                
            elif request_type == 'STATUS_QUERY':
                logger.info(f"🔍 Procesando STATUS_QUERY para driver {driver_id}")
                self.handle_driver_status_query(driver_id, cp_id)
                
            elif request_type == 'CANCEL_SUPPLY':
                # Cancelación de suministro desde el driver
                logger.info(f"🛑 Procesando CANCEL_SUPPLY para driver {driver_id}, CP: {cp_id}")
                self.handle_driver_cancel_supply(driver_id, cp_id, message.get('reason', 'Cancelación del conductor'))
                self.update_cp_status(cp_id, "ACTIVADO")

            else:
                logger.warning(f"⚠️ Tipo de petición desconocido: {request_type}")
                    
        except Exception as e:
            logger.error(f"❌ Error procesando petición de conductor: {e}", exc_info=True)
    
    def handle_driver_cancel_supply(self, driver_id: str, cp_id: str, reason: str):
        """Maneja la cancelación de suministro solicitada por el driver"""
        try:
            logger.info(f"🛑 Driver {driver_id} solicita cancelar suministro en CP {cp_id} - Razón: {reason}")
            
            cp = self.database.get_charging_point(cp_id)
            
            # ENVIAR STOP AL ENGINE
            logger.info(f"🔄 Enviando comando STOP a Engine del CP {cp_id} por cancelación del driver")
            control_message = {
                'cp_id': cp_id,
                'command': 'STOP',
                'reason': f'CANCELACION_DRIVER: {reason}',
                'driver_id': driver_id,
                'timestamp': datetime.now().isoformat(),
                'source': 'central'
            }
            self.kafka_manager.send_message('control_commands', control_message)
            
            # Registrar transacción cancelada
            if cp.current_amount > 0.01:
                transaction_data = self.record_transaction(cp, "CANCELLED")
                if transaction_data:
                    logger.info(f"💰 Transacción cancelada registrada para CP {cp_id}")
                    # Enviar ticket cancelación
                    self.send_cancellation_ticket(cp_id, transaction_data, reason)
            
            # Actualizar estado del CP
            self.update_cp_status(cp_id, "ACTIVADO")
            
            logger.info(f"✅ Suministro cancelado exitosamente - Driver: {driver_id}, CP: {cp_id}")
        
        except Exception as e:
            logger.error(f"❌ Error enviando cancelación: {e}")
    
    def send_cancellation_ticket(self, cp_id: str, transaction_data: dict, reason: str):
        """Envía ticket de cancelación al driver"""
        try:
            cancellation_ticket = {
                'cp_id': cp_id,
                'type': 'CANCELLATION_TICKET',
                'ticket_id': transaction_data['transaction_id'],
                'energy_consumed': transaction_data['energy_consumed'],
                'amount': transaction_data['amount'],
                'price_per_kwh': transaction_data['price_per_kwh'],
                'start_time': transaction_data['start_time'],
                'end_time': transaction_data['end_time'],
                'cancellation_reason': reason,
                'timestamp': datetime.now().isoformat()
            }
            
            # Enviar al driver si es un conductor real
            driver_id = transaction_data.get('driver_id')
            if driver_id and driver_id != "MANUAL":
                cancellation_ticket['driver_id'] = driver_id
                self.kafka_manager.send_message('driver_tickets', cancellation_ticket)
                logger.info(f"🎫 Ticket de cancelación enviado a Driver {driver_id}")
            
            # También engine para registro
            self.kafka_manager.send_message('engine_tickets', cancellation_ticket)
            logger.info(f"🎫 Ticket de cancelación enviado a Engine - CP: {cp_id}")
            
        except Exception as e:
            logger.error(f"❌ Error enviando ticket de cancelación: {e}")
    
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
        """Autoriza suministro en el CP vía Kafka"""
        try:
            # PRIMERO el punto de carga 
            cp = self.database.get_charging_point(cp_id)
            if not cp:
                logger.error(f"❌ CP {cp_id} no encontrado para autorización")
                return False

            # Reset contadores
            cp.driver_id = driver_id
            cp.supply_ended_time = None
            cp.supply_ending = False
            cp.last_supply_message = None
            logger.info(f"🔄 Autorizando suministro - Contadores reseteados para CP {cp_id}")

            # Enviar START
            control_message = {
                'cp_id': cp_id,
                'command': 'START',
                'driver_id': driver_id,
                'timestamp': datetime.now().isoformat(),
                'source': 'central'
            }
            self.kafka_manager.send_message('control_commands', control_message)
            logger.info(f"📤 Comando START enviado a CP {cp_id} via Kafka")
            
            if cp.status != "PARADO":
                self.update_cp_status(cp_id, "SUMINISTRANDO", driver_id=driver_id)
                logger.info(f"✅ Suministro autorizado para CP {cp_id} - Conductor: {driver_id}")
                return True
            
        except Exception as e:
            logger.error(f"❌ Error autorizando suministro en CP {cp_id}: {e}", exc_info=True)
            return False

def main():
    """Función principal"""
    
    # Configuración param
    socket_host = os.getenv('SOCKET_HOST', '0.0.0.0')
    socket_port = int(os.getenv('SOCKET_PORT', '5000'))
    kafka_servers = os.getenv('KAFKA_SERVERS', 'kafka:9092')
    
    if len(sys.argv) > 1:
        socket_port = int(sys.argv[1])
    if len(sys.argv) > 2:
        kafka_servers = sys.argv[2]
    
    print("=" * 60)
    print("EV_Central - Sistema de Gestión de Carga EV")
    print("Curso 25/26 - Sistemas Distribuidos")
    print("CON PERSISTENCIA EN POSTGRESQL - VERSIÓN COMPLETA")
    print("=" * 60)
    print(f"Socket: {socket_host}:{socket_port}")
    print(f"Kafka: {kafka_servers} (REAL)")
    print(f"PostgreSQL: {os.getenv('DB_HOST', 'postgres')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'ev_db')}")
    print(f"Directorio de logs: {os.getenv('LOG_DIR', '/app/logs')}")
    print("=" * 60)
    
    try:
        # Ahora EVCentral no necesita el parámetro data_file
        central = EVCentral(socket_host, socket_port, kafka_servers)
        central.start()
        
    except Exception as e:
        logger.error(f"❌ Error iniciando EV_Central: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()