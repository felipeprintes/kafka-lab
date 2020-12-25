from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import split

spark = SparkSession\
    .builder\
    .appName("refinamento dos dados")\
    .getOrCreate()

path = "/home/felipe/kafka-lab/teste/"
path_ref_data = "/home/felipe/kafka-lab/s3/ref_data/2020"

df = spark.read.parquet("{path}/2020/*".format(path=path))
print(" ######### RESULTADO DO DF #########")
print(df.show())

#df2 = df.createOrReplaceTempView("idade")

df2 = df.withColumn("idade", split(df.frase, " ").getItem(2))
df_temp_view = df2.createOrReplaceTempView("frase_idade")

query = """
    select frase from frase_idade where idade%2=0;
"""

df3 = spark.sql(query)
print(" ######### RESULTADO DO DF3 #########")
print(df3.show())

df3.write.parquet(path_ref_data)