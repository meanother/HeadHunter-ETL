from os import environ
import findspark
environ['SPARK_HOME'] = '/usr/local/spark'
findspark.init()
import pandas
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, create_map, lit, sum, split, explode
from pyspark.sql.functions import to_timestamp, from_unixtime


from .consumer_read import ReadStreamClass



sc = SparkContext("local", "first app")
sqlContext = SQLContext(sc)

read = ReadStreamClass()
data = read.fetch_data_from_topic()

temp_df = sqlContext.read.json(data)
print(temp_df.show())