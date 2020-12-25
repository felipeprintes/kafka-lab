from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql.types import FloatType

# Creating spark session
spark = SparkSession \
    .builder \
    .appName("primeiro spark") \
    .getOrCreate()

# csv path
path_data = '/home/felipe/Área de Trabalho/jupydocker/demo/src/data/raw_data.csv'

# parquet-data
path_write = '/home/felipe/kafka-lab/parquet_data2/data/2020/12'

# reading csv file
df = spark.read.csv(path_data, header=True, sep=',')

# print raw data schema
print(df.printSchema())

# converting string to integer
df = df.withColumn("A", df["A"].cast(IntegerType()))
df = df.withColumn("B", df["B"].cast(IntegerType()))

# print transform data -> transform string into integer
print(df.printSchema())

# creating a temporary view
temp_table_df = df.createOrReplaceTempView("raw_table")

# query to read just even number
query_even_number = """
    select * from raw_table where A%2=0;
"""

df2 = spark.sql(query_even_number)
#print(df2.collect())
print(df2.show())
#print(df2)

df2.write.parquet(path_write)