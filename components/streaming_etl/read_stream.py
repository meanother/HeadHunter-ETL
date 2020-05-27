import logging
from json import loads
from time import sleep
import pandas as pd
import sqlalchemy
from bs4 import BeautifulSoup as bs
from kafka import KafkaConsumer
from pandas import json_normalize
from sqlalchemy.types import String, INT, Boolean, DateTime, TEXT

logger = logging.basicConfig(
    format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:' +
           '%(levelname)s:%(process)d:%(message)s',
    level=logging.INFO
)

consumer = KafkaConsumer(
    # 'HeadHunterETL',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='headhunter_group',
    value_deserializer=lambda x: loads(x.decode('utf-8')))

consumer.subscribe(['HeadHunterETL'])

json_array = []
for msg in consumer:
    message = msg.value
    offset = msg.offset
    key = msg.key
    # yield message
    # collection.insert_one(message)
    # logging.debug(f'message: {message} was been readed..')
    logging.info(f'message: {message} was been readed..')
    logging.info(f'offset: {offset}')
    logging.info(f'key: {key}')
    logging.warning('-----------------------')
    sleep(2)

    '''
    json_array.append(message)
    # logging.warning(f'message: {message} was been readed..')
    # logging.error(f'message: {message} was been readed..')
    consumer.close()
dataframe = json_normalize(json_array)
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
renamed_df['published_at'] = pd.to_datetime(renamed_df['published_at'])
renamed_df['created_at'] = pd.to_datetime(renamed_df['created_at'])
logging.info('Create final dataframe is done')
engine = sqlalchemy.create_engine('postgresql://semenov:12345@77.244.65.15:58642/parsing_db')
logging.info('prepare to insert')
renamed_df.to_sql('etl_hh', schema='adhoc_parser', con=engine, if_exists='append', index=False,
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
logging.info('data is inserted')
'''
