import pyspark
from pyspark import RDD
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import ast


#Spark context details
sc = SparkContext(appName="PythonSparkStreamingKafka")
#ssc = SQLContext(sc)
ssc = StreamingContext(sc,2)
sq = SQLContext(sc)

spark = SparkSession \
    .builder \
    .appName("SSKafka") \
    .getOrCreate()

#Creating Kafka direct stream
dks = KafkaUtils.createDirectStream(ssc, ["meu-topico"], {"metadata.broker.list":"localhost:9092"})

#counts = dks.pprint()
print("############ AHHHHHHHHHHHHH ##############")
counts = dks.map(lambda x: ast.literal_eval(x[1])).pprint()
"""
if counts is not None:
    df = sq.createDataFrame([counts]).collect()
    #print("Data Frame")
    #print(df)
    #print(type(df))
    df.write.parquet('teste')
#print(store_s3)
"""


#Starting Spark context
ssc.start()
ssc.awaitTermination()