# -*- coding: utf-8 -*-
from kafka import KafkaProducer
import json
from bson import json_util
import os, sys
import time
from dotenv import load_dotenv

load_dotenv()
# Configuracion del servidor Kafka
bootstrap_servers = os.getenv('BOOTSTRAP_SERVERS')
topic_name = os.getenv('TOPIC_NAME')

# Crear un productor Kafka
#producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
#                         value_serializer=lambda v: json.dumps(v, default=json_util.default).encode('utf-8'))
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
def send_data_to_kafka(data):
    # Enviar el JSON al topic de Kafka
    producer.send(topic_name, value=data.encode('utf-8'))
    
    producer.flush()
    