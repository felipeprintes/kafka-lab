from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql.types import FloatType

# Creating spark session
spark = SparkSession \
    .builder \
    .appName("segundo spark") \
    .getOrCreate()

df = spark.read.parquet("parquet_data2/data/2020/*")
print(df.show())