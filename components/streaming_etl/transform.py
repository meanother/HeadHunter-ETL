import logging

import pandas as pd
from bs4 import BeautifulSoup as bs
from pandas import json_normalize
# from components.config import log_file


class TransformData:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s [%(levelname)s] %(message)s')
    # logger = logging.getLogger(__name__)
    logger = logging.getLogger('TransformDataModule')

    # def __init__(self, vacansy_json):
    #     self.vacansy_count = 0
    #     self.vacansy_json = vacansy_json

    def construct_dataframe(self):
        pass

    def make_row_data(self, item):
        self.logger.info('start to normalize data')
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
        self.logger.info('rename columns in dataframe')
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
        self.logger.info('clean snippet_requirement and snippet_responsibility')
        renamed_df['published_at'] = pd.to_datetime(renamed_df['published_at'])
        renamed_df['created_at'] = pd.to_datetime(renamed_df['created_at'])
        self.logger.info('finished normalize dataframe')
        return renamed_df
