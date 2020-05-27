import logging
from collections import defaultdict
from json import loads
from time import sleep
import pandas as pd
from bs4 import BeautifulSoup as bs
from kafka import KafkaConsumer
# from pandas.io.json import json_normalize
from pandas import json_normalize
import sqlalchemy
from sqlalchemy.types import String, INT, Boolean, DateTime, TEXT

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s [%(levelname)s] %(message)s')
# logger = logging.getLogger(__name__)
logger = logging.getLogger('ReadDataModule')

consumer = KafkaConsumer(
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='headhunter_group',
    value_deserializer=lambda x: loads(x.decode('utf-8')))

consumer.subscribe(['HeadHunterETL'])


class ReadStreamClass:

    def fetch_data_from_topic(self, consumer):
        json_array = []
        total = 0
        for msg in consumer:
            message = msg.value
            offset = msg.offset
            headers = msg.headers
            key = msg.key
            if key != b'final_message':
                json_array.append(message)
                # logger.warning(type(message))
                total += 1
            else:
                logger.info(f'fetch message with params: offset={offset}, headers={headers}, key={key} and append json array')
                logger.info('have a last messsage, go to collect json array')
                logger.warning(f'total message collected: {str(total)}, length={str(len(json_array))}')
                return json_array

    def make_row_data(self, item):
        logger.info('start to normalize data')
        dataframe = json_normalize(item)
        # logger.info(dataframe)

        # data.to_csv('qweqwe.csv')

        df = dataframe[['id',
                        'name',
                        'has_test',
                        'published_at',
                        'created_at',
                        'url',
                        'area.name',
                        'salary.from',
                        'salary.to',
                        'salary.currency',
                        'salary.gross',
                        'address.city',
                        'address.street',
                        'address.building',
                        'address.raw',
                        'address.metro.station_name',
                        'employer.id',
                        'employer.name',
                        'snippet.requirement',
                        'snippet.responsibility',
                        'contacts.name',
                        'contacts.email'
                        ]]
        logger.info('rename columns in dataframe')
        renamed_df = df.rename(columns={
            'area.name': 'area_name',
            'salary.from': 'salary_from',
            'salary.to': 'salary_to',
            'salary.currency': 'salary_currency',
            'address.street': 'address.street',
            'address.building': 'address_building',
            'address.raw': 'address_raw',
            'address.metro.station_name': 'metro_name',
            'employer.id': 'employer_id',
            'employer.name': 'employer_name',
            'snippet.requirement': 'snippet_requirement',
            'snippet.responsibility': 'snippet_responsibility',
            'contacts.name': 'contacts_name',
            'contacts.email': 'contacts_email',
        })

        def clean_data(row):
            # TODO change try to regex
            try:
                soup = bs(row, 'html.parser')
                return soup.text
            except TypeError:
                return None

        renamed_df['snippet_requirement'] = renamed_df['snippet_requirement'].apply(lambda x: clean_data(x))
        renamed_df['snippet_responsibility'] = renamed_df['snippet_responsibility'].apply(lambda x: clean_data(x))
        logger.info('clean snippet_requirement and snippet_responsibility')
        renamed_df['published_at'] = pd.to_datetime(renamed_df['published_at'])
        renamed_df['created_at'] = pd.to_datetime(renamed_df['created_at'])
        logger.info('finished normalize dataframe')
        return renamed_df

    def insert_data(self, dataframe):
        logger.info('create engine and connect to database')
        engine = sqlalchemy.create_engine('postgresql://semenov:12345@77.244.65.15:58642/parsing_db')
        logger.info('prepare to insert data')
        dataframe.to_sql('etl_hh', schema='adhoc_parser', con=engine, if_exists='append', index=False,
                         dtype={
                             'id': INT(),
                             'name': String(255),
                             'has_test': Boolean(),
                             'published_at': DateTime(),
                             'created_at': DateTime(),
                             'url': String(255),
                             'area_name': String(255),
                             'salary_from': INT(),
                             'salary_to': INT(),
                             'salary_currency': String(10),
                             'salary.gross': Boolean(),
                             'address.city': String(255),
                             'address.street': String(255),
                             'address_building': String(255),
                             'address_raw': String(500),
                             'metro_name': String(255),
                             'employer_id': INT(),
                             'employer_name': String(255),
                             'snippet_requirement': TEXT(),
                             'snippet_responsibility': TEXT(),
                             'contacts_name': String(255),
                             'contacts_email': String(255),
                         })
        logger.warning('data are inserted now')
        total_rows = engine.execute('select count(*) from adhoc_parser.etl_hh').fetchall()[0][0]
        logger.warning('-------------------------------------')
        logger.warning(f'Total row in table adhoc_parser.etl_hh is {str(total_rows)}')
        logger.warning('-------------------------------------')

example = ReadStreamClass()
# example.fetch_data_from_topic(consumer=consumer)

while True:
    data = example.fetch_data_from_topic(consumer=consumer)
    array = example.make_row_data(data)
    example.insert_data(array)
    sleep(5)
