import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.functions import col
from pyspark.sql.functions import from_json
import ast

spark = SparkSession\
    .builder\
    .appName("StructuredNetworkWordCount")\
    .getOrCreate()

df = spark\
  .readStream\
  .format("kafka")\
  .option("kafka.bootstrap.servers", "localhost:9092")\
  .option("subscribe", "meu-topico")\
  .load()
#df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
df = df.selectExpr("CAST(value AS STRING)")

print("##################### AHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH ####################")
print(df)

schema = StructType().add("frase", StringType())
df = df.select(from_json(col("value").cast("string"), schema).alias("parsed"))
df2 = df.select("parsed.frase")


# value schema => {"frase": "Eu tenho X anos"}
#schema = StructType().add("value", StringType())
#result_query = df.select(\
#    from_json(col('value').cast('string'),schema))

#query = df2\
#    .writeStream\
#    .format('console')\
#    .trigger(processingTime='30 seconds')\
#    .start()
#query.awaitTermination()



## TIRAR COMENTÁRIO

query = df2\
    .writeStream\
    .outputMode("append")\
    .format('parquet')\
    .option("checkpointLocation","/home/felipe/kafka-lab/checkpoint_dir")\
    .option('path','/home/felipe/kafka-lab/teste/2020')\
    .trigger(processingTime='30 seconds')\
    .start()\
    .awaitTermination()



#col('key').cast('string'),
#df.write.parquet("/home/felipe/kafka-lab/app_spark/teste/data")


#.option("startingOffsets", "earliest") \

"""
words = df.select(
    # explode turns each item in an array into a separate row
    explode(
        split(df.value, ' ')
    ).alias('word')
)

wordCounts = words.groupBy('word').count()
"""

#query = df\
#    .writeStream\
#    .format('console')\
#    .start()

#print(query)


#funciona!
#query = df\
#    .writeStream\
#    .format('memory')\
#    .option("parquet.block.size", 1024)\
#    .queryName('raw')\
#    .start()
    

#raw = query.sql("select * from raw")
#print(raw.show())

"""
query = df\
    .writeStream\
    .outputMode("append")\
    .format('parquet')\
    .option("parquet.block.size", 1024)\
    .option("checkpointLocation","/home/felipe/kafka-lab/checkpoint_dir")\
    .option('path','/home/felipe/kafka-lab/teste')\
    .start()\
    .awaitTermination()
"""

"""
block_sz = 1024 #1KB 
val query = movieFlattenedDf\
    writeStream\
    .outputMode("append")\
    .format("parquet")\
    .queryName("movies")\ 	
    .option("parquet.block.size", block_sz)\
    .option("checkpointLocation", "src/main/resources/chkpoint_dir")\
    .start("src/main/resources/output")\
    .awaitTermination()
"""


#query.awaitTermination()

#rawQuery = df \
#        .writeStream \
#        .queryName("qraw")\
#        .format("memory")\
#        .start()

#raw = spark.sql("select * from qraw")
#raw.show()

#raw.awaitTermination()
"""
lines = spark\
    .readStream\
    .format('kafka')\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "novo-topico")\
    .load()

words = lines.select(
    # explode turns each item in an array into a separate row
    explode(
        split(lines.value, ' ')
    ).alias('word')
)

# Generate running word count
wordCounts = words.groupBy('word').count()

query = wordCounts\
    .writeStream\
    .outputMode('complete')\
    .format('console')\
    .start()

query.awaitTermination()
"""