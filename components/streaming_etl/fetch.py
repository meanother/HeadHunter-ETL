import logging
import traceback
from json import dumps
from time import sleep
import requests
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'),
                         compression_type='gzip')

my_topic = 'HeadHunterETL'

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s [%(levelname)s] %(message)s')
# logger = logging.getLogger(__name__)
logger = logging.getLogger('FetchDataModule')


class FetchHHVacancy:

    def __init__(self, text):
        self.headers = {'User-Agent': 'ETL Pipeline exmaple@mail.com'}
        self.main_url = 'https://api.hh.ru/vacancies/'
        self.text = text
        self.per_page = 100
        self.page = 1
        logger.warning(f'START FETCH DATA WITH KEY: {self.text}')


    def get_total_number_of_vanacy(self):
        logger.info('Started to get total number of vacancy list')
        url = f'{self.main_url}?text="{self.text}"&per_page={self.per_page}&page={self.page}'
        logger.info(f'requests get: {url}')
        req = requests.get(url, headers=self.headers)
        page = req.json()['pages']
        logger.info(f'Complete get pages, result: {page}')
        # print(req.json()['pages'])
        return page

    def fetch_all_results(self):
        array = []

        def fetch_single_result(page):
            url = f'{self.main_url}?text="{self.text}"&per_page={self.per_page}&page={page}'
            logger.info(f'Get single page: {url}')
            req = requests.get(url, headers=self.headers)
            result = req.json()['items']
            # array.append(result)
            logger.info('start to append list result')
            for i in result:
                array.append(i)
                # logger.info(f'array is appended item: {i}')
                # print(i)

        count = self.get_total_number_of_vanacy()
        logger.info(f'total number of pages: {count}')
        logger.info(f'start get all pages')
        for i in range(count):  # TODO need to rollback
            # for i in range(3):
            fetch_single_result(i)
        # print(len(array))
        return array

    def send_to_topic(self, array):
        # array = [1, 2, 3]
        # for i in array:
        #     producer.send(topic=my_topic, value=f'text{i}', key=b'KEY', headers=[('HEADERS', b'HEADER_VALUE')])
        # producer.send(topic=my_topic, value=f'FINISH', key=b'END', headers=[('HEADERS', b'END')])
        #
        # producer.flush()
        #
        logger.info(f'start to send message to kafka-topic: {my_topic}')
        for item in array:
            try:
                # future = producer.send(topic=my_topic, value=item)
                producer.send(topic=my_topic, value=item, key=b'in_process', headers=[('header_key', b'in_process')])
                # record_metadata = future.get(timeout=10)
                # print('--> The message has been sent to a topic: \
                #         {}, partition: {}, offset: {}' \
                #       .format(record_metadata.topic.strip(),
                #               record_metadata.partition,
                #               record_metadata.offset))
            except:
                logger.error(traceback.format_exc())
            # finally:
        logger.warning('producer flush!')
        producer.flush()
        producer.send(topic=my_topic, value=None, key=b'final_message', headers=[('header_key', b'final_message')])
        producer.flush()
        logger.info('send last message and flush producer')



example = FetchHHVacancy('python')
for i in range(20):
    example.send_to_topic(example.fetch_all_results())
    sleep(10)
