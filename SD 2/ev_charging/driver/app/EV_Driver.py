#!/usr/bin/env python3
# EV_Driver.py

import os
import sys
import json
import time
import threading
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

class EV_Driver:
    """Aplicación del conductor para solicitar suministros de carga CON RESILIENCIA"""
    
    def __init__(self, bootstrap_servers: str, driver_id: str):
        self.bootstrap_servers = bootstrap_servers
        self.driver_id = driver_id
        self.producer = None
        self.consumer = None
        self.available_cps = []
        self.current_request = None
        self.running = False
        
        # SISTEMA DE RESILIENCIA
        self.current_supply = None 
        self.supply_state_file = f"/app/data/driver_{driver_id}_state.json"
        self.supply_data_file = f"/app/data/driver_{driver_id}_supply.json"
        
        # Crear directorio si no existe
        os.makedirs(os.path.dirname(self.supply_state_file), exist_ok=True)
        
        self.setup_kafka()
        self.load_supply_state()  # Cargar estado al iniciar
        
    def setup_kafka(self):
        """Configura las conexiones Kafka"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=[self.bootstrap_servers],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=3
            )
            
            # Consumer para respuestas de la central
            self.consumer = KafkaConsumer(
                'driver_responses',
                'driver_tickets', 
                bootstrap_servers=[self.bootstrap_servers],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id=f'driver_{self.driver_id}_group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
                consumer_timeout_ms=5000
            )
            
            print(f"✅ Driver {self.driver_id} conectado a Kafka")
            
        except Exception as e:
            print(f"❌ Error conectando a Kafka: {e}")
            raise

    # MÉTODOS DE RESILIENCIA
    def save_supply_state(self):
        """Guarda el estado actual del suministro del driver"""
        try:
            state_data = {
                'driver_id': self.driver_id,
                'current_supply': self.current_supply,
                'last_update': datetime.now().isoformat(),
                'available_cps': self.available_cps,
                'timestamp': datetime.now().isoformat()
            }
            
            with open(self.supply_state_file, 'w', encoding='utf-8') as f:
                json.dump(state_data, f, indent=2, ensure_ascii=False)
            
            print(f"💾 Estado del driver guardado: {self.supply_state_file}")
            return True
            
        except Exception as e:
            print(f"❌ Error guardando estado del driver: {e}")
            return False

    def load_supply_state(self):
        """Carga el estado previo del suministro del driver"""
        try:
            if not os.path.exists(self.supply_state_file):
                print("📭 No se encontró estado previo del driver")
                return False
            
            with open(self.supply_state_file, 'r', encoding='utf-8') as f:
                state_data = json.load(f)
            
            # Cargar datos del estado
            self.current_supply = state_data.get('current_supply')
            self.available_cps = state_data.get('available_cps', [])
            
            print(f"✅ Estado del driver cargado - Suministro actual: {self.current_supply}")
            
            # Si había un suministro en curso, intentar recuperarlo
            if self.current_supply:
                print(f"🔄 Recuperando suministro interrumpido: {self.current_supply}")
                self.recover_interrupted_supply()
            
            return True
            
        except Exception as e:
            print(f"❌ Error cargando estado del driver: {e}")
            return False

    def save_supply_data(self, supply_data: dict):
        """Guarda los datos detallados del suministro actual"""
        try:
            supply_data['driver_id'] = self.driver_id
            supply_data['last_save'] = datetime.now().isoformat()
            
            with open(self.supply_data_file, 'w', encoding='utf-8') as f:
                json.dump(supply_data, f, indent=2, ensure_ascii=False)
            
            print(f"💾 Datos de suministro guardados: {self.supply_data_file}")
            return True
            
        except Exception as e:
            print(f"❌ Error guardando datos de suministro: {e}")
            return False

    def load_supply_data(self) -> dict:
        """Carga los datos detallados del suministro actual"""
        try:
            if not os.path.exists(self.supply_data_file):
                return {}
            
            with open(self.supply_data_file, 'r', encoding='utf-8') as f:
                return json.load(f)
            
        except Exception as e:
            print(f"❌ Error cargando datos de suministro: {e}")
            return {}

    def recover_interrupted_supply(self):
        """Recupera un suministro que fue interrumpido abruptamente"""
        try:
            if not self.current_supply:
                return False
            
            cp_id = self.current_supply.get('cp_id')
            print(f"🔄 Intentando recuperar suministro en CP {cp_id}...")
            
            # Cargar datos del suministro
            supply_data = self.load_supply_data()
            
            # Verificar si CP disponible
            cp_available = any(cp.get('cp_id') == cp_id for cp in self.available_cps)
            
            if not cp_available:
                print(f"❌ CP {cp_id} no está disponible para recuperación")
                self.clear_supply_state()
                return False
            
            # Consultar estado actual del CP
            status_message = {
                'driver_id': self.driver_id,
                'cp_id': cp_id,
                'type': 'STATUS_QUERY',
                'recovery_attempt': True,
                'timestamp': datetime.now().isoformat()
            }
            
            self.producer.send('driver_requests', status_message)
            self.producer.flush()
            print(f"📡 Consultando estado de CP {cp_id} para recuperación...")
            
            # Esperar respuesta de estado
            response = self.wait_for_response('STATUS_RESPONSE', timeout=10)
            
            if response and response.get('status') == 'SUMINISTRANDO':
                print(f"✅ Suministro todavía activo en CP {cp_id} - Recuperando...")
                return self.wait_for_supply_completion(cp_id, is_recovery=True)
            else:
                print(f"❌ Suministro en CP {cp_id} ya finalizó - Limpiando estado")
                self.clear_supply_state()
                return False
                
        except Exception as e:
            print(f"❌ Error recuperando suministro interrumpido: {e}")
            self.clear_supply_state()
            return False

    def clear_supply_state(self):
        """Limpia el estado del suministro actual"""
        try:
            self.current_supply = None
            
            # Eliminar archivos de estado
            if os.path.exists(self.supply_state_file):
                os.remove(self.supply_state_file)
            if os.path.exists(self.supply_data_file):
                os.remove(self.supply_data_file)
            
            print("🧹 Estado del suministro limpiado")
            return True
            
        except Exception as e:
            print(f"❌ Error limpiando estado del suministro: {e}")
            return False

    def update_current_supply(self, cp_id: str, action: str = "start"):
        """Actualiza el suministro actual"""
        try:
            if action == "start":
                self.current_supply = {
                    'cp_id': cp_id,
                    'start_time': datetime.now().isoformat(),
                    'driver_id': self.driver_id,
                    'status': 'IN_PROGRESS'
                }
            elif action == "end":
                self.current_supply = None
                self.clear_supply_state()
            
            # Guardar estado después de cada actualización
            self.save_supply_state()
            
        except Exception as e:
            print(f"❌ Error actualizando suministro actual: {e}")

    def get_available_cps(self):
        """Solicita a la central los puntos de carga disponibles"""
        try:
            print(f"\n📡 Solicitando CPs disponibles a la central...")
            
            request_message = {
                'driver_id': self.driver_id,
                'type': 'STATUS_QUERY',
                'timestamp': datetime.now().isoformat()
            }
            
            self.producer.send('driver_requests', request_message)
            self.producer.flush()
            print("✅ Solicitud de CPs enviada a la central")
            
            # Esperar respuesta
            response = self.wait_for_response('ALL_STATUS_RESPONSE', timeout=20)
            if response and 'charging_points' in response:
                self.available_cps = response['charging_points']
                # Guardar CPs disponibles en estado
                self.save_supply_state()
                self.display_available_cps()
                return True
            else:
                print("❌ No se recibieron datos de CPs disponibles")
                return False
                
        except Exception as e:
            print(f"❌ Error solicitando CPs disponibles: {e}")
            return False
    
    def display_available_cps(self):
        """Muestra los puntos de carga disponibles"""
        if not self.available_cps:
            print("❌ No hay puntos de carga disponibles")
            return
            
        print(f"\n{'='*80}")
        print("🔌 PUNTOS DE CARGA DISPONIBLES")
        print(f"{'='*80}")
        print(f"{'ID':<8} {'Ubicación':<20} {'Estado':<15} {'Precio/kWh':<12} {'Conductor':<12}")
        print(f"{'-'*80}")
        
        for cp in self.available_cps:
            try:
                # Estado
                status_icon = {
                    "ACTIVADO": "🟢 DISPONIBLE",
                    "SUMINISTRANDO": "🔵 OCUPADO", 
                    "PARADO": "🟡 PARADO",
                    "AVERIADO": "🔴 AVERIADO",
                    "DESCONECTADO": "⚫ DESCONECTADO"
                }.get(cp.get('status', 'DESCONECTADO'), "⚫ DESCONECTADO")
                
                # Precio 
                price_value = cp.get('price_per_kwh')
                precio = f"€{float(price_value):.3f}" if price_value is not None else "N/A"
                
                # Otros campos
                conductor = cp.get('driver_id') or '-'
                cp_id = cp.get('cp_id') or 'N/A'
                location = cp.get('location') or 'N/A'
                
                print(f"{cp_id:<8} {location:<20} {status_icon:<15} {precio:<12} {conductor:<12}")
                
            except Exception as e:
                print(f"⚠️ Error mostrando CP: {e}")
                continue
        
        print(f"{'='*80}")
    
    def request_supply(self, cp_id: str):
        """Solicita suministro en un punto de carga específico"""
        try:
            print(f"\n🚀 Preparando solicitud de suministro en CP {cp_id}...")
            
            # Guardar estado ANTES de iniciar
            self.update_current_supply(cp_id, "start")
            
            # PRIMERO PREGUNTAR SI ESTÁ CONECTADO
            print(f"\n🔌 CONEXIÓN REQUERIDA - CP {cp_id}")
            print("¿Ha conectado su vehículo al punto de carga? (s/n): ", end='')
            
            connected = self.wait_for_connection()
            
            if not connected:
                print("❌ Vehículo no conectado - Cancelando solicitud")
                self.clear_supply_state()  # 🆕 Limpiar estado si no se conecta
                return False
            
            # Guardar datos iniciales
            initial_data = {
                'cp_id': cp_id,
                'connection_confirmed': True,
                'request_time': datetime.now().isoformat(),
                'energy_delivered': 0.0,
                'current_amount': 0.0
            }
            self.save_supply_data(initial_data)
            
            print("✅ Vehículo conectado - Enviando solicitud de suministro a la central...")
            
            self.current_request = {
                'driver_id': self.driver_id,
                'cp_id': cp_id,
                'type': 'SUPPLY_REQUEST',
                'timestamp': datetime.now().isoformat()
            }
            
            # Enviar solicitud a la central
            self.producer.send('driver_requests', self.current_request)
            self.producer.flush()
            print(f"✅ Solicitud de suministro enviada para CP {cp_id}")
            
            # Esperar autorización
            return self.wait_for_authorization(cp_id)
            
        except Exception as e:
            print(f"❌ Error solicitando suministro: {e}")
            self.clear_supply_state()  # Limpiar estado, error
            return False
    
    def wait_for_authorization(self, cp_id: str):
        """Espera la respuesta de autorización de la central"""
        print(f"⏳ Esperando autorización de la central para CP {cp_id}...")
        
        start_time = time.time()
        timeout = 30
        
        while time.time() - start_time < timeout:
            try:
                # Buscar mensajes respuesta
                messages = self.consumer.poll(timeout_ms=2000)
                
                for topic_partition, message_batch in messages.items():
                    for message in message_batch:
                        if message.value:
                            response = message.value
                            
                            # Verificar si es respuesta para este driver y CP
                            if (response.get('driver_id') == self.driver_id and 
                                response.get('cp_id') == cp_id):
                                
                                status = response.get('status')
                                
                                if status == 'AUTHORIZED':
                                    print(f"✅ AUTORIZADO - Suministro autorizado en CP {cp_id}")
                                    print(f"📍 Ubicación: {response.get('location', 'N/A')}")
                                    print(f"💰 Precio: €{response.get('price_per_kwh', 0):.3f}/kWh")
                                    # DIRECTAMENTE INICIAMOS EL SUMINISTRO 
                                    return self.handle_authorized_supply(cp_id, response)
                                
                                elif status == 'DENIED':
                                    reason = response.get('message', 'Razón no especificada')
                                    print(f"❌ DENEGADO - {reason}")
                                    self.clear_supply_state()  # Limpiar denegación
                                    return False
                                
                                elif status == 'ERROR':
                                    print(f"❌ ERROR - {response.get('message', 'Error de comunicación')}")
                                    self.clear_supply_state()  # Limpiar error
                                    return False
                
                time.sleep(1)
     
            except Exception as e:
                print(f"❌ Error esperando autorización: {e}")
                continue
        
        print("❌ Timeout esperando autorización")
        self.clear_supply_state()  # Limpiar timeout
        return False
    
    def handle_authorized_supply(self, cp_id: str, auth_response: dict):
        """Maneja el suministro autorizado"""
        try:
            print("✅ Vehículo ya conectado - Iniciando suministro...")

            return self.wait_for_supply_completion(cp_id)
                
        except Exception as e:
            print(f"❌ Error durante el suministro autorizado: {e}")
            # NO limpiar estado aquí - permitir recuperación
            return False
    
    def wait_for_connection(self):
        """Espera a que el conductor confirme la conexión del vehículo"""
        try:
            response = input().strip().lower()
            return response in ['s', 'si', 'sí', 'y', 'yes']
        except Exception as e:
            print(f"\n❌ Error leyendo input (asumiendo NO): {e}")
            return False
    
    def wait_for_supply_completion(self, cp_id: str, is_recovery: bool = False):
        """Espera a que el suministro se complete"""
        print(f"\n⚡ INICIANDO SUMINISTRO - CP {cp_id}" + (" [RECUPERACIÓN]" if is_recovery else ""))
        print("=" * 60)
        
        start_time = time.time()
        timeout = 100  
        last_display_update = time.time()
        last_save = time.time()
        
        # Variables tracking del progreso
        last_energy = 0.0
        last_amount = 0.0
        session_start = time.time()
        
        # Estadísticas iniciales
        print(f"🕐 Hora de inicio: {datetime.now().strftime('%H:%M:%S')}")
        print(f"⏱️  Tiempo máximo de sesión: {timeout} segundos")
        print("=" * 60)
        
        while time.time() - start_time < timeout:
            try:
                messages = self.consumer.poll(timeout_ms=timeout)
                
                has_flow_update = False
                current_energy = last_energy
                current_amount = last_amount
                current_flow = 0.0
                
                for topic_partition, message_batch in messages.items():
                    for message in message_batch:
                        if message.value:
                            response = message.value
                            
                            # PROCESAR ACTUALIZACIONES DE FLUJO
                            if (response.get('driver_id') == self.driver_id and 
                                response.get('type') == 'FLOW_UPDATE'):
                                
                                has_flow_update = True
                                current_energy = response.get('energy_delivered', 0)
                                current_amount = response.get('current_amount', 0)
                                current_flow = response.get('flow_rate', 0)
                                total_amount = response.get('total_amount', 0)
                                
                                # Calcular estadísticas
                                elapsed_time = time.time() - session_start
                                if elapsed_time > 0:
                                    power_kw = current_flow
                                    avg_power = (current_energy / elapsed_time) * 3600 if elapsed_time > 0 else 0
                                    
                                    # MOSTRAR PROGRESO
                                    if time.time() - last_display_update >= 5:  # Actualizar cada 5 segundos
                                        self.display_progress(
                                            cp_id=cp_id,
                                            energy_delivered=current_energy,
                                            current_amount=current_amount,
                                            total_amount=total_amount,
                                            flow_rate=current_flow,
                                            power_kw=power_kw,
                                            avg_power=avg_power,
                                            elapsed_time=elapsed_time,
                                            session_start=session_start
                                        )
                                        last_display_update = time.time()
                                
                                # Guardar progreso cada 20 segundos
                                if time.time() - last_save > 20:
                                    supply_data = {
                                        'cp_id': cp_id,
                                        'energy_delivered': current_energy,
                                        'current_amount': current_amount,
                                        'total_amount': total_amount,
                                        'flow_rate': current_flow,
                                        'last_flow_update': datetime.now().isoformat(),
                                        'elapsed_time': elapsed_time
                                    }
                                    self.save_supply_data(supply_data)
                                    last_save = time.time()
                                
                                # Actualizar últimos valores
                                last_energy = current_energy
                                last_amount = current_amount
                                
                                continue
                            
                            # PROCESAR FINALIZACIÓN
                            if (response.get('driver_id') == self.driver_id and 
                                response.get('type') == 'CHARGING_TICKET'):
                                
                                print(f"\n{'='*60}")
                                print("🎫 CARGA COMPLETADA - RECIBIENDO TICKET...")
                                print(f"{'='*60}")
                                self.display_charging_ticket(response)
                                self.clear_supply_state()
                                return True
                            
                            # PROCESAR FALLOS
                            elif (response.get('driver_id') == self.driver_id and 
                                response.get('type') == 'CHARGING_FAILED'):
                                
                                print(f"\n{'='*60}")
                                print("❌ CARGA FALLIDA")
                                print(f"{'='*60}")
                                print(f"Razón: {response.get('failure_reason', 'Razón desconocida')}")
                                if response.get('energy_consumed', 0) > 0:
                                    print(f"Energía suministrada antes del fallo: {response.get('energy_consumed'):.2f} kWh")
                                self.clear_supply_state()
                                return False
                
                # MOSTRAR ESTADO DE ESPERA SI NO HAY ACTUALIZACIONES
                if not has_flow_update:
                    current_time = time.time()
                    if current_time - last_display_update >= 10:  # Mostrar cada 10 segundos sin updates
                        elapsed = current_time - start_time
                        remaining = timeout - elapsed
                        
                        print(f"\n📡 Esperando datos de carga...")
                        print(f"⏱️  Tiempo transcurrido: {int(elapsed)}s")
                        print(f"⏳ Tiempo restante: {int(remaining)}s")
                        if last_energy > 0:
                            print(f"⚡ Última energía registrada: {last_energy:.2f} kWh")
                            print(f"💰 Último importe registrado: €{last_amount:.2f}")
                        
                        last_display_update = current_time
                
                # Mostrar barra de progreso cada 30 segundos
                elapsed_total = time.time() - start_time
                if int(elapsed_total) % 30 == 0 and elapsed_total > 5:
                    progress_percent = min(100, (elapsed_total / timeout) * 100)
                    self.display_progress_bar(progress_percent, elapsed_total, timeout)
                    
                time.sleep(1)
                
            except Exception as e:
                print(f"❌ Error durante el suministro: {e}")
                time.sleep(2)
                continue
        
        # TIMEOUT
        self.cancel_supply(cp_id=cp_id)
        return self.wait_for_ticket_after_timeout(cp_id)

    def display_progress(self, cp_id: str, energy_delivered: float, current_amount: float, total_amount: float, flow_rate: float, power_kw: float, avg_power: float, elapsed_time: float, session_start: float):
        """Muestra el progreso detallado del suministro"""
        # Calcular estadísticas
        remaining_time = (total_amount - current_amount) / (avg_power / 3600) if avg_power > 0 else 0
        cost_per_minute = (current_amount / elapsed_time) * 60 if elapsed_time > 0 else 0
        
        # Limpiar pantalla o mostrar separador
        print(f"\n{'='*60}")
        print(f"⚡ PROGRESO EN TIEMPO REAL - CP {cp_id}")
        print(f"{'='*60}")
        
        # Información principal
        print(f"🔋 Energía suministrada: {energy_delivered:.2f} kWh")
        print(f"💰 Importe actual: €{current_amount:.2f}")
        print(f"📈 Caudal instantáneo: {flow_rate:.1f} kW")
        print(f"⚡ Potencia media: {avg_power:.1f} kW")
        
        # Estadísticas de tiempo
        elapsed_minutes = int(elapsed_time // 60)
        elapsed_seconds = int(elapsed_time % 60)
        print(f"🕐 Tiempo de carga: {elapsed_minutes:02d}:{elapsed_seconds:02d}")
        
        if remaining_time > 0 and remaining_time < 3600:  # Mostrar solo si razonable
            remaining_minutes = int(remaining_time // 60)
            remaining_seconds = int(remaining_time % 60)
            print(f"⏳ Tiempo estimado restante: {remaining_minutes:02d}:{remaining_seconds:02d}")
        
        # Costo por minuto
        if cost_per_minute > 0:
            print(f"💶 Costo por minuto: €{cost_per_minute:.3f}")
        
        # Barra de progreso simple
        progress = min(100, (energy_delivered / 50) * 100)
        bars = int(progress / 5)  # 20 barras total
        
        print(f"{'='*60}")

    def display_progress_bar(self, progress: float, elapsed: float, total: float):
        """Muestra una barra de progreso simple"""
        bars = 30
        filled = int((progress / 100) * bars)
        empty = bars - filled
        
        print(f"\n📦 Progreso general de sesión:")
        print(f"[{'█' * filled}{'░' * empty}] {progress:.1f}%")
        print(f"⏱️  {int(elapsed)}/{int(total)} segundos")
    
    def wait_for_ticket_after_timeout(self, cp_id: str):
        """Espera un ticket después de un timeout"""
        print("⏳ Esperando ticket final después del timeout...")
        
        ticket_timeout = 10  # Esperar máximo 10 segundos por el ticket
        
        start_time = time.time()
        while time.time() - start_time < ticket_timeout:
            try:
                messages = self.consumer.poll(timeout_ms=2000)
                
                for topic_partition, message_batch in messages.items():
                    for message in message_batch:
                        if message.value:
                            response = message.value
                            
                            # Procesar tickets
                            if (response.get('driver_id') == self.driver_id and 
                                response.get('type') == 'CHARGING_TICKET'):
                                
                                print(f"🎫 TICKET RECIBIDO via {topic_partition.topic} - Mostrando resumen...")
                                self.display_charging_ticket(response)
                                self.clear_supply_state()  # Limpiar estado al recibir ticket
                                return True
                            
                            # Procesar tickets de cancelación
                            if (response.get('driver_id') == self.driver_id and 
                                response.get('type') == 'CANCELLATION_TICKET'):
                                
                                print(f"🎫 TICKET DE CANCELACIÓN RECIBIDO - Mostrando resumen...")
                                self.display_cancellation_ticket(response)
                                self.clear_supply_state()  # Limpiar estado al recibir ticket
                                return True
            
                time.sleep(1)
            except Exception as e:
                print(f"❌ Error esperando ticket post-timeout: {e}")
                continue
        
        print("❌ No se recibió ticket después del timeout")
        return False
    
    def display_charging_ticket(self, ticket_data: dict):
        """Muestra el ticket de carga al conductor"""
        print(f"\n{'='*60}")
        print("🎫 TICKET DE CARGA - RESUMEN")
        print(f"{'='*60}")
        print(f"👤 Conductor: {ticket_data.get('driver_id', 'N/A')}")
        print(f"🔌 Punto de Carga: {ticket_data.get('cp_id', 'N/A')}")
        print(f"📍 Ubicación: {ticket_data.get('location', 'N/A')}")
        print(f"📋 ID Transacción: {ticket_data.get('ticket_id', 'N/A')}")
        print(f"{'-'*60}")
        print(f"⚡ Energía Consumida: {ticket_data.get('energy_consumed', 0):.2f} kWh")
        print(f"💰 Precio por kWh: €{ticket_data.get('price_per_kwh', 0):.3f}")
        print(f"💵 Importe Total: €{ticket_data.get('amount', 0):.2f}")
        print(f"{'-'*60}")
        print(f"✅ CARGA COMPLETADA EXITOSAMENTE")
        print(f"{'='*60}\n")
    
    def display_cancellation_ticket(self, ticket_data: dict):
        """Muestra el ticket de cancelación al conductor"""
        print(f"\n{'='*60}")
        print("🎫 TICKET DE CANCELACIÓN - RESUMEN")
        print(f"{'='*60}")
        print(f"👤 Conductor: {ticket_data.get('driver_id', 'N/A')}")
        print(f"🔌 Punto de Carga: {ticket_data.get('cp_id', 'N/A')}")
        print(f"📍 Ubicación: {ticket_data.get('location', 'N/A')}")
        print(f"📋 ID Transacción: {ticket_data.get('ticket_id', 'N/A')}")
        print(f"{'-'*60}")
        print(f"⚡ Energía Consumida: {ticket_data.get('energy_consumed', 0):.2f} kWh")
        print(f"💰 Precio por kWh: €{ticket_data.get('price_per_kwh', 0):.3f}")
        print(f"💵 Importe Total: €{ticket_data.get('amount', 0):.2f}")
        print(f"❌ Razón de Cancelación: {ticket_data.get('cancellation_reason', 'No especificada')}")
        print(f"{'-'*60}")
        print(f"🛑 CARGA CANCELADA")
        print(f"{'='*60}\n")
    
    def wait_for_response(self, expected_type: str, timeout: int = 10):
        """Espera una respuesta específica de la central"""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                messages = self.consumer.poll(timeout_ms=2000)
                
                for topic_partition, message_batch in messages.items():
                    for message in message_batch:
                        if message.value:
                            response = message.value
                            if (response.get('driver_id') == self.driver_id and 
                                response.get('type') == expected_type):
                                return response
                
                time.sleep(1)
                
            except Exception as e:
                print(f"❌ Error esperando respuesta: {e}")
                continue
        
        return None
    
    def cancel_supply(self, cp_id: str):
        """Cancela un suministro en curso"""
        try:
            cancel_message = {
                'driver_id': self.driver_id,
                'cp_id': cp_id,
                'type': 'CANCEL_SUPPLY',
                'reason': 'Cancelación manual del conductor',
                'timestamp': datetime.now().isoformat()
            }
            
            self.producer.send('driver_requests', cancel_message)
            self.producer.flush()
            print(f"✅ Solicitud de cancelación enviada para CP {cp_id}")
            
        except Exception as e:
            print(f"❌ Error cancelando suministro: {e}")
    
    def process_service_file(self, file_path: str):
        """Procesa un archivo con múltiples servicios"""
        try:
            if not os.path.exists(file_path):
                print(f"❌ Archivo no encontrado: {file_path}")
                return
            
            with open(file_path, 'r', encoding='utf-8') as f:
                services = json.load(f)
            
            if not isinstance(services, list):
                print("❌ El archivo debe contener una lista de servicios")
                return
            
            print(f"\n📁 Procesando {len(services)} servicios del archivo: {file_path}")
            
            for i, service in enumerate(services, 1):
                print(f"\n{'='*50}")
                print(f"📦 PROCESANDO SERVICIO {i}/{len(services)}")
                print(f"{'='*50}")
                
                cp_id = service.get('cp_id')
                if not cp_id:
                    print("❌ Servicio sin CP_ID - Saltando...")
                    continue
                
                # Solicitar suministro
                success = self.request_supply(cp_id)
                
                # Esperar 4 segundos entre servicios
                if i < len(services):
                    print(f"\n⏳ Esperando 4 segundos antes del siguiente servicio...")
                    time.sleep(4)
                
            print(f"\n✅ Todos los servicios procesados")
            
        except json.JSONDecodeError:
            print("❌ Error: El archivo no tiene formato JSON válido")
        except Exception as e:
            print(f"❌ Error procesando archivo de servicios: {e}")

    def leer_archivo(self, archivo: str):
        """Lee un archivo de servicios y devuelve la lista de CPs"""

        if not os.path.exists(archivo):
                print(f"❌ Archivo no encontrado: {archivo}")
                return []
        
        try:
            with open(archivo, 'r', encoding='utf-8') as f:
                servicios = [
                    {'cp_id': cp_limpio} 
                    for linea in f
                    if (cp_limpio := linea.strip())
                ]

            return servicios
        except IOError as e:
            print(f"❌ Error de lectura/escritura en el archivo: {e}")
            return []
        except Exception as e:
            print(f"❌ Error leyendo archivo de servicios: {e}")
            return []
    
    def interactive_mode(self):
        """Modo interactivo para solicitar servicios manualmente"""
        print(f"\n Driver {self.driver_id}")
        print(f"{'='*50}")
        
        # Mostrar estado de recuperación si existe
        if self.current_supply:
            print(f"🔄 SUMINISTRO PENDIENTE: CP {self.current_supply.get('cp_id')}")
            print("   Use la opción 1 para ver CPs y recuperar automáticamente")
            print(f"{'='*50}")
        
        while True:
            print("\nOpciones disponibles:")
            print("1. Ver CPs disponibles")
            print("2. Solicitar suministro")
            print("3. Suministro automático")
            print("4. Limpiar estado pendiente")
            print("5. Salir")
            print("\nSeleccione una opción: ", end='')
            
            try:
                option = input().strip()
                
                if option == '1':
                    self.get_available_cps()
                    
                elif option == '2':
                    if not self.available_cps:
                        print("❌ Primero debe obtener los CPs disponibles (opción 1)")
                        continue
                    
                    print("\nIngrese el ID del CP para suministro: ", end='')
                    cp_id = input().strip().upper()
                    
                    # Verificar CP existe en la lista disponible
                    cp_exists = any(cp.get('cp_id') == cp_id for cp in self.available_cps)
                    if not cp_exists:
                        print(f"❌ CP {cp_id} no encontrado en la lista de disponibles")
                        continue
                    
                    self.request_supply(cp_id)
                    time.sleep(4)  # Espera entre solicitudes
                    
                elif option == '3':
                    self.get_available_cps()

                    archivo = input("Nombre del archivo de suministro: ").strip()
                    archivo = "/app/suministros_driver/" + archivo
                    suministros = self.leer_archivo(archivo)
                    if not suministros:
                        print("❌ No se encontraron suministros en el archivo")
                        continue
                    else:
                        print("\n🚀 Iniciando suministro automático en todos los CPs disponibles...")
                        for suministro in suministros:
                            cp_id = suministro.get('cp_id')
                            cp_exists = any(cp.get('cp_id') == cp_id for cp in self.available_cps)
                            if cp_exists:
                                print(f"\n{'-'*40}")
                                print(f"🔄 Solicitando suministro en CP {cp_id}")
                                self.request_supply(cp_id)
                                print(f"{'-'*40}\n")
                                time.sleep(4)  # Espera entre solicitudes
                            else:
                                print(f"❌ CP {cp_id} no está disponible actualmente")
                                time.sleep(4)  # Espera entre solicitudes
                                
                elif option == '4':  # Limpiar estado pendiente
                    if self.current_supply:
                        print("🧹 Limpiando estado de suministro pendiente...")
                        self.clear_supply_state()
                        print("✅ Estado limpiado correctamente")
                    else:
                        print("ℹ️ No hay estado pendiente que limpiar")
                        
                elif option == '5':
                    print("👋 Saliendo...")
                    break
                    
                else:
                    print("❌ Opción no válida")
                    
            except KeyboardInterrupt:
                print("\n👋 Saliendo...")
                break
            except Exception as e:
                print(f"❌ Error: {e}")
    
    def close(self):
        """Cierra las conexiones - CON RESILIENCIA"""
        try:
            # Guardar estado antes de cerrar
            if self.current_supply:
                print("💾 Guardando estado del suministro antes de cerrar...")
                self.save_supply_state()
            
            if self.producer:
                self.producer.close()
            if self.consumer:
                self.consumer.close()
            print("✅ Conexiones cerradas")
        except Exception as e:
            print(f"❌ Error cerrando conexiones: {e}")

def main():
    """Función principal"""
    if len(sys.argv) < 3:
        print("Uso: python EV_Driver.py <IP:PUERTO_KAFKA> <DRIVER_ID> [ARCHIVO_SERVICIOS]")
        print("Ejemplo: python EV_Driver.py localhost:9092 DRIVER_001 servicios.json")
        sys.exit(1)
    
    bootstrap_servers = sys.argv[1]
    driver_id = sys.argv[2]
    service_file = sys.argv[3] if len(sys.argv) > 3 else None
    
    print(f"{'='*60}")
    print("🚗 EV_Driver")
    print(f"{'='*60}")
    print(f"👤 Driver ID: {driver_id}")
    print(f"🔌 Kafka: {bootstrap_servers}")
    print(f"💾 Sistema de resiliencia")
    print(f"{'='*60}")
    
    try:
        driver = EV_Driver(bootstrap_servers, driver_id)
        
        if service_file:
            # Modo archivo
            driver.process_service_file(service_file)
        else:
            # Modo interactivo
            driver.interactive_mode()
        
        driver.close()
        
    except Exception as e:
        print(f"❌ Error iniciando EV_Driver: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()