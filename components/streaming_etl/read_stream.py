import logging
from json import loads
from time import sleep

import pandas as pd
import sqlalchemy
from bs4 import BeautifulSoup as bs
from kafka import KafkaConsumer
# from components.config import log_file
# from pandas.io.json import json_normalize
from pandas import json_normalize
from sqlalchemy.types import String, INT, Boolean, DateTime, TEXT



class ReadStreamClass:

    # logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s [%(levelname)s] %(message)s')
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(funcName)s %(process)d:%(processName)s [%(levelname)s] %(message)s')

    # logger = logging.getLogger(__name__)
    logger = logging.getLogger('ReadDataModule')

    # bootstrap_servers = 'localhost:9092'
    bootstrap_servers = '35.230.42.114:9092'

    consumer = KafkaConsumer(
        bootstrap_servers=[bootstrap_servers],
        auto_offset_reset='earliest',
        # enable_auto_commit=True,
        group_id='headhunter_group',
        value_deserializer=lambda x: loads(x.decode('utf-8')))

    consumer.subscribe(['HeadHunterETL'])

    def fetch_data_from_topic(self):
        json_array = []
        total = 0
        for msg in self.consumer:
            message = msg.value
            offset = msg.offset
            headers = msg.headers
            key = msg.key
            self.logger.info(message)
            # self.consumer.commit()
            if key != b'final_message':
                json_array.append(message)
                # logger.warning(type(message))
                total += 1
            else:
                self.logger.info(
                    f'fetch message with params: offset={offset}, headers={headers}, key={key} and append json array')
                self.logger.info('have a last messsage, go to collect json array')
                self.consumer.commit()
                self.logger.warning(f'total message collected: {str(total)}, length={str(len(json_array))}')
                return json_array



# test = ReadStreamClass()
# test.fetch_data_from_topic()

# #
# def read_data():
#     read = ReadStreamClass()
#     read.fetch_data_from_topic()
# while True:
#     read_data()

