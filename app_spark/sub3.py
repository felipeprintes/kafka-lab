from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
import ast

spark = SparkSession \
    .builder \
    .appName("SSKafka") \
    .getOrCreate()
    
# default for startingOffsets is "latest", but "earliest" allows rewind for missed alerts   

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "meu-topico") \
  .option("startingOffsets", "earliest") \
  .load()

ds = df.selectExpr("CAST(value AS STRING)")
print(df.selectExpr("CAST(value AS STRING)"))
print(type(df))
print(type(ds))
print(ds)

words = lines.select(
        # explode turns each item in an array into a separate row
        explode(
            split(lines.value, ' ')
        ).alias('word')
    )

query = ds\
        .writeStream\
        .outputMode('complete')\
        .format('console')\
        .start()

query.awaitTermination()


#.option("startingOffsets", "earliest") \
"""
rawQuery = df \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "meu-topico") \
        .option("startingOffsets", "earliest") \
        .queryName("qraw")\
        .format("memory")\
        .start()


raw = spark.sql("select * from qraw")
print(raw.show())
"""

"""
df.writeStream \
  .format("parquet") \
  .option("startingOffsets", "earliest") \
  .option("path", "/teste") \
  .option("checkpointLocation", "/home/felipe/kafka-lab/app_spark/checkpoint") \
  .start()
"""

"""
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "meu-topico") \
  .option("startingOffsets", "earliest") \
  .load()

words = df.select(
   explode(
       split(df.value, " ")
   ).alias("word")
)

# Generate running word count
wordCounts = words.groupBy("word").count()

# Start running the query that prints the running counts to the console
query = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()

#df2 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
#df2 = df.groupBy("value").count()
#query = df2.writeStream.outputMode("complete").format("console").start()
#query.awaitTermination()
#print("########################### OLAAAAAAAAAAAAAAAA #############################")
#print("query result {}".format(query))
"""


"""
rawQuery = df2 \
        .writeStream \
        .queryName("qraw")\
        .format("memory")\
        .trigger(processingTime='60 seconds') \
        .start()

raw = spark.sql("select * from qraw")
raw.show()
"""