import json

from kafka import KafkaProducer, KafkaConsumer
from run import (
    DATABASE_PASS,
    DATABASE_LOGIN,
    DATABASE_ADDRESS,
    DATABASE_NAME,
    BOOTSTRAP_SERVER,
    TOPIC_NAME,
    INTERVAL,
)
from sqlalchemy import create_engine

producer = KafkaProducer(bootstrap_servers=[BOOTSTRAP_SERVER],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                         compression_type='gzip')

# query_date = str(date.today() - timedelta(days=INTERVAL))


DB = 'postgresql'
engine = create_engine(f'{DB}://{DATABASE_LOGIN}:{DATABASE_PASS}@{DATABASE_ADDRESS}/{DATABASE_NAME}')

# sql = 'select report_date, count(*) from adhoc_parser.audit_yandex_bot group by report_date order by 1 desc'
# sql = 'select * from adhoc_parser.audit_yandex_bot where report_date = date(now()) -1 limit 2'
# sql = f'select * from adhoc_parser.audit_yandex_bot where report_date = date(now()) -{INTERVAL} limit 2'
sql = 'select * from adhoc_parser.audit_yandex_bot where report_date = date(now()) -{} limit 2'.format(INTERVAL)

data = engine.execute(sql).fetchall()

for i in data:
    collect = {
        'id': i[0],
        'url': i[1][:10],
        'report_date': str(i[2]),
        'parse_date': str(i[3]),
        'has_error': i[4],
        'time_load': i[5],
    }

    print(collect)
#
#     producer.send(topic=TOPIC_NAME, value=collect, key=b'in_process', headers=[('header_key', b'in_process')])
# producer.flush()
# print(end_date)


from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext

'''
conf = SparkConf().setMaster('spark://manjaro:7077')
conf.setAppName('Yandex-app')

regex = '(?:(?:https?|ftp):\/\/)?[\w/\-?=%.]+\.[\w/\-?=%.]+'

# Creates spark session with JDBC JAR
spark = SparkSession.builder \
    .appName('stack_overflow') \
    .master('spark://manjaro:7077') \
    .config('spark.jars', '/usr/local/spark/jars/postgresql-42.2.14.jar') \
    .getOrCreate()

#    .option("query", 'select * from adhoc_parser.audit_yandex_bot where report_date = date(now()) -{} limit 2'.format(INTERVAL)) \
#   .option("query", 'select * from adhoc_parser.audit_yandex_bot where report_date = date(now()) -{} limit 2000'.format(INTERVAL)) \

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://77.244.65.15:58642/parsing_db") \
    .option("query", 'select * from adhoc_parser.audit_yandex_bot where report_date = date(now()) -{} limit 500'.format(INTERVAL)) \
    .option("user", "semenov") \
    .option("password", DATABASE_PASS) \
    .load()

# print(df)
df.show()
df.printSchema()


def write_to_db(df):
    df.write.format('jdbc').options(
        url='jdbc:postgresql://77.244.65.15:58642/parsing_db',
        driver='org.postgresql.Driver',
        dbtable='adhoc_parser.tested_df',
        user='semenov',
        password=DATABASE_PASS).mode('append').save()


result = df.withColumn('URLX', regexp_extract(col('url'), regex, 0))
x = result.select('URLX').limit(20).toPandas()
print(x)
# write_to_db(result)
dx = result \
    .groupBy('URLX') \
    .agg(count('*').alias('cnt')) \
    .orderBy('cnt', ascending=False) \
    .show()

# df.write.format('parquet').option('compressin', 'gzip').save('qwe.parquet')
'''

#     .set('packages', 'org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,org.apache.kafka:kafka-clients:0.10.1.0:2.4.5') \
#    .set('spark.jars', '/usr/local/spark/jars/spark-streaming-kafka-0-10-assembly_2.10-2.1.0.jar') \

import os

# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5'

# spark-sql-kafka-0-10_2.12-2.4.6.jar

# OLD     .set('spark.jars', '/usr/local/spark/jars/spark-sql-kafka-0-10_2.11-2.1.0.jar') \
# '/usr/local/spark/jars/spark-sql-kafka-0-10_2.11-2.4.5.jar


#    .set('spark.jars', '/usr/local/spark/jars/spark-sql-kafka-0-10_2.12-2.4.5.jar') \
#    .set('spark.jars.packages', 'org.apache.kafka:kafka-clients:0.10.1.0:2.4.5') \
#     .set('spark.jars', '/usr/local/spark/jars/postgresql-42.2.14.jar,/usr/local/spark/jars/spark-sql-kafka-0-10_2.12-2.4.5.jar') \
#    # .set('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.5') \
#    .set('spark.jars', '/usr/local/spark/jars/spark-sql-kafka-0-10_2.12-2.4.5.jar') \
#     .set('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.5') \

# spark-sql-kafka-0-10_2.11-2.4.5.jar
# manjaro
conf = SparkConf().setMaster('spark://35.230.42.114:7077')\
    .setAppName('Yandex-app') \
    .set("spark.shuffle.service.enabled", "false") \
    .set("spark.dynamicAllocation.enabled", "false") \
    .set('spark.jars', '/usr/local/spark/jars/postgresql-42.2.14.jar') \
    .set('spark.jars', '/usr/local/spark/jars/spark-sql-kafka-0-10_2.11-2.4.5.jar') \
    .set('spark.jars', '/usr/local/spark/jars/kafka-clients-0.10.2.2.jar') \
    .set('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5') \
    .set('spark.driver.maxResultSize', '2g')\
    .set('spark.driver.port', "20002")\
    .set("spark.driver.bindAddress", "0.0.0.0")\
    .set("spark.blockManager.port", "6060")\
    .set('spark.driver.memory', '4g')\
    .set('spark.executor.memory', '1g')\
    .set('spark.num.executors', '2')\
    .set('spark.executor.cores', '2')

sc = SparkContext.getOrCreate(conf=conf)
SP = SparkSession(sc)
print(sc.version)

# Привет Аа

df1 = SP.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://77.244.65.15:58642/parsing_db") \
    .option("query", 'select * from adhoc_parser.audit_yandex_bot where report_date = date(now()) -{} limit 50'.format(INTERVAL)) \
    .option("user", "semenov") \
    .option("password", DATABASE_PASS) \
    .load()

# print(df)
df1.show()
df1.printSchema()

# 0Oo lLiI
#
# dataframe = SP.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER) \
#     .option("subscribe", TOPIC_NAME) \
#     .option("startingOffsets", "earliest") \
#     .load()
#
# #    .option("failOnDataLoss", "false") \
# x = dataframe.selectExpr("CAST(value AS STRING)")
# print(x)
# print(x.printSchema())
# print('``````````````````````')
# print(dataframe)
# print(type(dataframe))
#
# print('444444444444444444444444444')
#
# qqq = dataframe.writeStream \
#     .outputMode('append') \
#     .format('console') \
#     .start()
# # qqq.awaitTermination()
# print('444444444444444444444444444')

# schema_json = dataframe.schema.json()
# print(schema_json)
# schema_json_prototype = StructType.fromJson(json.loads(schema_json))
# print(schema_json_prototype)

print('-------------')
#report_date, has_403, id, waste_time, parse_time, url
print(df1)
print(type(df1))


# json_df = df1.toJSON(use_unicode=True)
# print(json_df)
# results = json.loads(df1.toJSON().collect())
# results = df1.toJSON().map(lambda j: json.loads(j)).collect()
# print(results)

json_df = df1.write.format('json')
# print(json_df.json('data/'))

#
# ds = dataframe.selectExpr("to_json(struct(*)) AS value")
# print(ds)


# qqq = ds.writeStream \
#     .outputMode('append') \
#     .format('console') \
#     .start()

# ds = df1 \
#     .selectExpr("to_json(struct(*)) AS value") \
#     .writeStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER) \
#     .option("topic", TOPIC_NAME) \
#     .start()
from pyspark.sql.functions import to_json, struct, lit
'''
stream_df = df1.select(to_json(struct([df1[x] for x in df1.columns])).alias("value")).withColumn('key', lit('process'))
stream_df.show()
stream_df.printSchema()

# WORK WRITE TOPIC
#stream_df.write.format("kafka").option("kafka.bootstrap.servers", BOOTSTRAP_SERVER).option("topic", TOPIC_NAME).save()
'''



'''
from confluent_kafka import Consumer

print('------ READ TOPIC --------')

c = Consumer({
    'bootstrap.servers': BOOTSTRAP_SERVER,
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

c.subscribe([TOPIC_NAME])

while True:
    msg = c.poll(5.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    print('Received message: {}'.format(msg.value().decode('utf-8')))

c.close()
'''
# consumer = KafkaConsumer(
#     bootstrap_servers=[BOOTSTRAP_SERVER],
#     auto_offset_reset='earliest',
#     max_in_flight_requests_per_connection=50,
#     max_poll_records=5,
#     enable_auto_commit=True,
#     group_id='group-1',
#     value_deserializer=lambda x: json.loads(x.decode('utf-8')))
#
# consumer.subscribe([TOPIC_NAME])
#
# print('------ READ TOPIC --------')
# for msg in consumer:
#     print(msg.value)
#     print('----------------------')

#
# print(ds)
#
# # sc = SparkContext(conf=conf)
# sp = SparkSession.builder.config(conf=conf).getOrCreate()
# print(sp)
# x = sp.sparkContext.getConf().getAll()
# print(x)
