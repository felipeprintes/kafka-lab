from kafka import KafkaProducer
from datetime import datetime
from time import sleep
import random
import json
import time

# Variables
TOPICO = 'meu-topico'

# Create an instance of the kafka producer
producer = KafkaProducer(bootstrap_servers = 'localhost:9092',
                                              value_serializer = lambda v: str(v).encode('utf-8'))

print(("Ctrl+c to stop"))

while True:
    idade_aleatoria = random.randint(1,40)
    frase = "Eu tenho {idade_aleatoria} anos".format(idade_aleatoria=idade_aleatoria)
    dado = {"frase":frase}
    time.sleep(5)
    producer.send(TOPICO, value=dado)