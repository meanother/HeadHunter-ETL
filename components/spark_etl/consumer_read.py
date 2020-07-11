import logging
from json import loads
import os
import json
from datetime import datetime
from config import timer_logger
from kafka import KafkaConsumer


class ReadStreamClass:
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(name)s %(funcName)s %(process)d:%(processName)s [%(levelname)s] %(message)s')

    logger = logging.getLogger(__name__)
    # logger = logging.getLogger('ReadDataModule')

    # bootstrap_servers = 'localhost:9092'
    bootstrap_servers = '35.230.42.114:9092'

    consumer = KafkaConsumer(
        bootstrap_servers=[bootstrap_servers],
        auto_offset_reset='earliest',
        # enable_auto_commit=True,
        group_id='headhunter_group',
        max_in_flight_requests_per_connection=50,
        # auto_commit_interval_ms=5000,
        value_deserializer=lambda x: loads(x.decode('utf-8')))

    consumer.subscribe(['HeadHunterETL'])

    @timer_logger
    def fetch_data_from_topic(self):
        json_array = []
        total = 0
        self.logger.info('start to fetch messages')
        for msg in self.consumer:
            message = msg.value
            offset = msg.offset
            headers = msg.headers
            key = msg.key
            # self.logger.info(message)
            # self.consumer.commit()
            if key != b'final_message':
                json_array.append(message)
                total += 1
            else:
                self.logger.info(
                    f'fetch message with params: offset={offset}, headers={headers}, key={key} and append json array')
                self.logger.info('have a last messsage, go to collect json array')
                self.consumer.commit()
                self.logger.warning(f'total message collected: {str(total)}, length={str(len(json_array))}')
                self.consumer.commit()
                return json_array

    @timer_logger
    def upload_to_json_file(self, array):
        name_pattern = datetime.now().strftime('%d-%m-%Y__%H-%M-%S')
        with open(f'headhuner_{name_pattern}.json', 'w') as file:
            json.dump(array, file)
            file_path = os.path.abspath(file.name)
            return file_path

# test = ReadStreamClass()
# # test.fetch_data_from_topic()
# test.upload_to_json_file(test.fetch_data_from_topic())

