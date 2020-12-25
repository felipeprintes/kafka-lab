from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.functions import col
from pyspark.sql.functions import from_json

spark = SparkSession\
    .builder\
    .appName('teste')\
    .getOrCreate()

schema = StructType().add("frase", StringType()).add("content", StringType())

schema2 = StructType().add("content", MapType(StringType(), StringType()))

df_raw = spark.read.parquet("/home/felipe/kafka-lab/teste/*")

df = df_raw.select(from_json(col("value"), schema).alias("parsed_value"))
df.select("parsed_value.content", "parsed_value.frase").show()

structureSchema = StructType([
        StructField('name', StructType([
             StructField('content', StringType(), True),
             StructField('frase', StringType(), True)
             ]))
         ])




schema = StructType([
    StructField('content', StringType(), True),
    StructField('frase', StringType(), True),
])

schema = StructType([
    StructField("frase", StringType(), True)
])


df_raw.select("content", split(col("value"), ": ").getItem(0).alias("parsed")).show(20, False)


df_raw.withColumn("content", split(col("value"), ": ").getItem(0)).withColumn("frase", split(col("value"), ": ").getItem(1)).show(20, False)
