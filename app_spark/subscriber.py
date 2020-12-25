from pyspark.sql import SparkSession
from pyspark import RDD
from kafka import KafkaConsumer
import json
import ast


## Variables config ##
BROKER = 'localhost:9092'
TOPIC = 'meu-topico'
## End variables config ##

## Creating spark session
spark = SparkSession \
    .builder \
    .appName("Consumer and write data") \
    .getOrCreate()

consumer = KafkaConsumer(TOPIC, bootstrap_servers=BROKER)

for msg in consumer:
    dado = msg.value.decode('utf-8')
    #spark.readStream(dado)
    print(dado)


"""
spark = SparkSession \
    .builder \
    .appName('kafka') \
    .getOrCreate()

# default for startingOffsets is "latest", but "earliest" allows rewind for missed alerts    
dsraw = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "host:9092") \
  .option("subscribe", "meu-topico") \
    .load()
  #.option("startingOffsets", "earliest") \
"""