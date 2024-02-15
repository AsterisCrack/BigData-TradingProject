# -*- coding: utf-8 -*-
from kafka import KafkaProducer
import json
import os
import time

# Configuración del servidor Kafka
bootstrap_servers = 'worker01:9092'
topic_name = 'industrials_g7'

# Crear un productor Kafka
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Directorio donde se encuentra el archivo JSON
json_directory = '/home/alumnos/arq_big_data/abd18/SPRINT2/'

while True:
    for filename in os.listdir(json_directory):
        if filename.endswith('.json'):
            with open(os.path.join(json_directory, filename)) as json_file:
                data = json.load(json_file)
                # Enviar el JSON al topic de Kafka
                producer.send(topic_name, value=data)
                print(f"JSON enviado al topic '{topic_name}'")
            # Esperar un segundo antes de enviar el siguiente JSON
            time.sleep(1)
