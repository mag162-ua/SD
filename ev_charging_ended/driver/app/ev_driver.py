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
    """Aplicación del conductor para solicitar suministros de carga"""
    
    def __init__(self, bootstrap_servers: str, driver_id: str):
        self.bootstrap_servers = bootstrap_servers
        self.driver_id = driver_id
        self.producer = None
        self.consumer = None
        self.available_cps = []
        self.current_request = None
        self.running = False
        
        # Inicializar Kafka
        self.setup_kafka()
        
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
                
                # Precio (validado)
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
        """Solicita suministro en un punto de carga específico - MODIFICADO: Primero pregunta conexión"""
        try:
            print(f"\n🚀 Preparando solicitud de suministro en CP {cp_id}...")
            
            # 🆕 PRIMERO PREGUNTAR SI ESTÁ CONECTADO ANTES DE ENVIAR LA SOLICITUD
            print(f"\n🔌 CONEXIÓN REQUERIDA - CP {cp_id}")
            print("¿Ha conectado su vehículo al punto de carga? (s/n): ", end='')
            
            connected = self.wait_for_connection()
            
            if not connected:
                print("❌ Vehículo no conectado - Cancelando solicitud")
                return False
            
            # 🆕 SOLO ENVIAR LA SOLICITUD SI EL CONDUCTOR CONFIRMA LA CONEXIÓN
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
            
            # Esperar respuesta de autorización
            return self.wait_for_authorization(cp_id)
            
        except Exception as e:
            print(f"❌ Error solicitando suministro: {e}")
            return False
    
    def wait_for_authorization(self, cp_id: str):
        """Espera la respuesta de autorización de la central"""
        print(f"⏳ Esperando autorización de la central para CP {cp_id}...")
        
        start_time = time.time()
        timeout = 30  # 30 segundos de timeout
        
        while time.time() - start_time < timeout:
            try:
                # Buscar mensajes de respuesta
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
                                    # 🆕 AHORA DIRECTAMENTE INICIAMOS EL SUMINISTRO (ya está conectado)
                                    return self.handle_authorized_supply(cp_id, response)
                                
                                elif status == 'DENIED':
                                    reason = response.get('message', 'Razón no especificada')
                                    print(f"❌ DENEGADO - {reason}")
                                    return False
                                
                                elif status == 'ERROR':
                                    print(f"❌ ERROR - {response.get('message', 'Error de comunicación')}")
                                    return False
                
                time.sleep(1)
     
            except Exception as e:
                print(f"❌ Error esperando autorización: {e}")
                continue
        
        print("❌ Timeout esperando autorización")
        return False
    
    def handle_authorized_supply(self, cp_id: str, auth_response: dict):
        """Maneja el suministro autorizado - MODIFICADO: Ya no pregunta conexión"""
        try:
            print("✅ Vehículo ya conectado - Iniciando suministro...")
            
            # El suministro ahora es manejado por la central y el CP
            # Esperamos a que termine
            return self.wait_for_supply_completion(cp_id)
                
        except Exception as e:
            print(f"❌ Error durante el suministro autorizado: {e}")
            return False
    
    def wait_for_connection(self):
        """Espera a que el conductor confirme la conexión del vehículo"""
        try:
            # En una aplicación real, esto sería una interfaz gráfica
            # Por ahora usamos input para simular
            response = input().strip().lower()
            return response in ['s', 'si', 'sí', 'y', 'yes']
        except Exception as e:
            print(f"\n❌ Error leyendo input (asumiendo NO): {e}")
            return False
    
    def wait_for_supply_completion(self, cp_id: str):
        """Espera a que el suministro se complete - CON FLUJO EN TIEMPO REAL"""
        print(f"⚡ Suministro en progreso - CP {cp_id}")
        print("Esperando finalización...")
        
        start_time = time.time()
        timeout = 100  # 100 segundos máximo
        last_update = time.time()
        
        while time.time() - start_time < timeout:
            try:
                messages = self.consumer.poll(timeout_ms=5000)
                
                for topic_partition, message_batch in messages.items():
                    for message in message_batch:
                        if message.value:
                            response = message.value
                            
                            # 🎯 NUEVO: Procesar actualizaciones de flujo en tiempo real
                            if (response.get('driver_id') == self.driver_id and 
                                response.get('type') == 'FLOW_UPDATE'):
                                
                                flow_rate = response.get('flow_rate', 0)
                                energy_delivered = response.get('energy_delivered', 0)
                                current_amount = response.get('current_amount', 0)
                                total_amount = response.get('total_amount', 0)
                                
                                # Mostrar actualización cada 10 segundos para no saturar
                                if time.time() - last_update > 10:
                                    print(f"⚡ Cargando... Energía: {energy_delivered:.2f} kWh | "
                                        f"Caudal: {flow_rate:.1f} kW | Importe actual: €{current_amount:.2f}")
                                    last_update = time.time()
                                
                                continue
                            
                            # 🎯 Procesar tickets del topic específico
                            if (response.get('driver_id') == self.driver_id and 
                                response.get('type') == 'CHARGING_TICKET'):
                                
                                print(f"🎫 TICKET RECIBIDO via {topic_partition.topic} - Mostrando resumen...")
                                self.display_charging_ticket(response)
                                return True
                            
                            # Verificar notificaciones de fallo
                            elif (response.get('driver_id') == self.driver_id and 
                                response.get('type') == 'CHARGING_FAILED'):
                                
                                print(f"❌ CARGA FALLIDA - {response.get('failure_reason', 'Razón desconocida')}")
                                if response.get('energy_consumed', 0) > 0:
                                    print(f"⚡ Energía suministrada antes del fallo: {response.get('energy_consumed')} kWh")
                                return False
                
                # Mostrar mensaje de espera cada 30 segundos
                elapsed = time.time() - start_time
                if int(elapsed) % 30 == 0 and elapsed > 1:
                    print(f"⏳ Carga en progreso... {int(elapsed)}/{timeout} segundos")
                    
                time.sleep(2)
                
            except Exception as e:
                print(f"❌ Error durante el suministro: {e}")
                continue
        
        print("❌ Timeout esperando finalización del suministro")
        # 🆕 ENVIAR CANCELACIÓN POR TIMEOUT
        self.producer.send('driver_requests', {
            'driver_id': self.driver_id,
            'cp_id': cp_id,
            'type': 'CANCEL_SUPPLY',
            'reason': 'TIMEOUT_ESPERA_FINALIZACION',
            'timestamp': datetime.now().isoformat()
        })
        self.producer.flush()
        print("✅ Cancelación por timeout enviada a la central")
        
        # Esperar un poco por si llega el ticket
        return self.wait_for_ticket_after_timeout(cp_id)
    
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
                                return True
                            
                            # Procesar tickets de cancelación
                            if (response.get('driver_id') == self.driver_id and 
                                response.get('type') == 'CANCELLATION_TICKET'):
                                
                                print(f"🎫 TICKET DE CANCELACIÓN RECIBIDO - Mostrando resumen...")
                                self.display_cancellation_ticket(response)
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
                    if (cp_limpio := linea.strip())  # Uso del operador Walrus (Python 3.8+)
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
        print(f"\n🎮 MODO INTERACTIVO - Driver {self.driver_id}")
        print(f"{'='*50}")
        
        while True:
            print("\nOpciones disponibles:")
            print("1. Ver CPs disponibles")
            print("2. Solicitar suministro")
            print("3. Suministro automático")
            print("4. Salir")
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
                    
                    # Verificar que el CP existe en la lista disponible
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
                                
                elif option == '4':
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
        """Cierra las conexiones"""
        try:
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
    print("🚗 EV_Driver - Aplicación del Conductor")
    print(f"{'='*60}")
    print(f"👤 Driver ID: {driver_id}")
    print(f"🔌 Kafka: {bootstrap_servers}")
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