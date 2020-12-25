from pyspark.sql import SparkSession
from pyspark import RDD
from kafka import KafkaProducer
from datetime import datetime
from time import sleep
import json

## Variables ##
TOPICO = 'meu-topico'
## End variable ##


# Creating spark session
spark = SparkSession \
    .builder \
    .appName("write data in kafka topic") \
    .getOrCreate()

# csv path data
path_data = '/home/felipe/Área de Trabalho/jupydocker/demo/src/data/raw_data.csv'

# write raw parquet data
path_raw_data = '/home/felipe/kafka-lab/s3/raw_data/2020'

# read csv for testing
df = spark.read.csv(path_data, header=True, sep=',')

# print for testing
print(df.printSchema())
print(df.show())
print(df)
print(type(df))
print(df.rdd.collect())

#for row in df.rdd.collect():
#    print('--- row ---')
#    print(row)

# Create an instance of the kafka producer

producer = KafkaProducer(bootstrap_servers = 'localhost:9092',
                        value_serializer=lambda v: str(v).encode('utf-8'))

# write in topic
for row in df.rdd.collect():
    dado = row.asDict()
    producer.send(TOPICO, dado)
    producer.flush()

#producer.close()