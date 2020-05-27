from json import dumps

import requests
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'),
                         compression_type='gzip')

my_topic = 'HeadHunterETL'


class FetchHHVacancy:

    def __init__(self, text):
        self.headers = {'User-Agent': 'ETL Pipeline exmaple@mail.com'}
        self.main_url = 'https://api.hh.ru/vacancies/'
        self.text = text
        self.per_page = 100
        self.page = 1

    def get_total_number_of_vanacy(self):
        url = f'{self.main_url}?text="{self.text}"&per_page={self.per_page}&page={self.page}'
        req = requests.get(url, headers=self.headers)
        page = req.json()['pages']
        # print(req.json()['pages'])
        return page

    def fetch_all_results(self):
        array = []

        def fetch_single_result(page):
            url = f'{self.main_url}?text="{self.text}"&per_page={self.per_page}&page={page}'
            req = requests.get(url, headers=self.headers)
            result = req.json()['items']
            # array.append(result)
            for i in result:
                array.append(i)
                # print(i)

        count = self.get_total_number_of_vanacy()
        for i in range(count):  # TODO need to rollback
            # for i in range(3):
            fetch_single_result(i)
        # print(len(array))
        return array

    def send_to_topic(self, array):
        for item in array:
            try:
                future = producer.send(topic=my_topic, value=item)
                record_metadata = future.get(timeout=10)

                print('--> The message has been sent to a topic: \
                        {}, partition: {}, offset: {}' \
                      .format(record_metadata.topic,
                              record_metadata.partition,
                              record_metadata.offset))

            except Exception as e:
                print('--> It seems an Error occurred: {}'.format(e))

            finally:
                producer.flush()
        producer.send(topic=my_topic, value='kill')

example = FetchHHVacancy('python')
example.send_to_topic(example.fetch_all_results())
