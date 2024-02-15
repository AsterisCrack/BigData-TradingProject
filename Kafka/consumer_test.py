# -*- coding: utf-8 -*-

from kafka import KafkaConsumer
import json

# Configuración del servidor Kafka
bootstrap_servers = 'worker01:9092'
topic_name = 'industrials_g7'

# Crear un consumidor Kafka
consumer = KafkaConsumer(topic_name,
                         bootstrap_servers=bootstrap_servers,
                         auto_offset_reset='earliest',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Escuchar continuamente los mensajes del topic
for message in consumer:
    # Procesar el mensaje
    print(f"Mensaje recibido: {message.value}")
