import logging
from os import environ

import psycopg2
from config import timer_logger
from consumer_read import ReadStreamClass
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql.functions import col

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)s %(funcName)s %(process)d:%(processName)s [%(levelname)s] %(message)s')

logger = logging.getLogger('TransformDataModule')


class TransformAndWrite:

    def __init__(self):
        self.sc = SparkContext("local", "first app")
        self.sqlContext = SparkSession(self.sc)

    @timer_logger
    def initialize(self):
        import findspark
        environ['SPARK_HOME'] = '/usr/local/spark'
        # findspark.add_packages('org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5')
        findspark.add_jars('/usr/local/spark/jars/postgresql-42.2.14.jar')
        # findspark.add_jars('postgresql-42.2.14.jar')
        findspark.init()

    @timer_logger
    def connection(self):
        conn = psycopg2.connect(
            host='77.244.65.15',
            port='58642',
            database='parsing_db',
            user='semenov',
            password='12345',
        )
        return conn

    @timer_logger
    def load_to_df(self, json_file):
        temp_df = self.sqlContext.read.json(json_file)
        df = temp_df.select(
            col('id').cast(types.IntegerType()),
            col('name'),
            col('has_test'),
            col('published_at').cast(types.DateType()),
            col('created_at').cast(types.DateType()),
            col('url'),
            col('area.name').alias('area_name'),
            col('salary.from').alias('salary_from'),
            col('salary.to').alias('salary_to'),
            col('salary.currency').alias('salary_currency'),
            col('address.street').alias('address.street'),
            col('address.building').alias('address_building'),
            col('address.raw').alias('address_raw'),
            col('address.metro.station_name').alias('metro_name'),
            col('employer.id').alias('employer_id').cast(types.IntegerType()),
            col('employer.name').alias('employer_name'),
            col('snippet.requirement').alias('snippet_requirement'),
            col('snippet.responsibility').alias('snippet_responsibility'),
            col('contacts.name').alias('contacts_name'),
            col('contacts.email').alias('contacts_email'))
        return df

    @timer_logger
    def write_to_db(self, df):
        collect = df.write.format('jdbc').options(
            url='jdbc:postgresql://77.244.65.15:58642/parsing_db',
            driver='org.postgresql.Driver',
            dbtable='adhoc_parser.etl_hh',
            user='semenov',
            password='12345').mode('append').save()
        return collect

    @timer_logger
    def insert_with_psycopg2(self, conn, df):
        seq = [tuple(x) for x in df.collect()]

        record_array = ','.join(['%s'] * len(seq))

        querry = "insert into adhoc_parser.etl_hh (id, name, has_test, published_at, created_at, url, \
          area_name, salary_from, salary_to, salary_currency, address.street, address_building, address_raw,\
          metro_name, employer_id, employer_name, snippet_requirement, snippet_responsibility, \
          contacts_name, contacts_email) values {}".format(record_array)
        cur = conn.cursor()

        cur.execute(querry, seq)


test = ReadStreamClass()
insert = TransformAndWrite()
insert.initialize()
while True:
    # test.fetch_data_from_topic()
    data = test.upload_to_json_file(test.fetch_data_from_topic())
    x = insert.load_to_df(data)
    insert.write_to_db(x)
# connect = insert.connection()
# insert.insert_with_psycopg2(connect, x)


# sc = SparkContext("local", "first app")
# sqlContext = SQLContext(sc)
# sqlContext = SparkSession(sc)


# temp_df = sqlContext.read.json(data)
# temp_df.show(5)
# temp_df.printSchema()


# renamed_df.show(5)
# print(dir(renamed_df))
# renamed_df.printSchema()
# renamed_df.write.format('jdbc').options(
#     url='jdbc:postgresql://77.244.65.15:58642/parsing_db',
#     driver='org.postgresql.Driver',
#     dbtable='adhoc_parser.etl_hh',
#     user='semenov',
#     password='12345').mode('append').save()


