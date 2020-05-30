import sqlalchemy
import logging
from sqlalchemy.types import String, INT, Boolean, DateTime, TEXT
# from components.config import log_file

class Database:

    # logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s [%(levelname)s] %(message)s')
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(funcName)s %(process)d:%(processName)s [%(levelname)s] %(message)s')

    # logger = logging.getLogger(__name__)
    logger = logging.getLogger('TransformDataModule')

    def __init__(self, Config):
        self.database_url = Config.database_url
        self.database_login = Config.database_login
        self.database_password = Config.database_password
        self.database_name = Config.database_name
        self.database_type = Config.database_type


    def insert_data(self, dataframe):
        self.logger.info('create engine and connect to database')
        engine = sqlalchemy.create_engine(f'{self.database_type}://{self.database_login}:{self.database_password}@{self.database_url}/{self.database_name}')
        self.logger.info('prepare to insert data')
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
        self.logger.warning('data are inserted now')
        # total_rows = engine.execute('select count(*) from adhoc_parser.etl_hh').fetchall()[0][0]
        # self.logger.warning('-------------------------------------')
        # self.logger.warning(f'Total row in table adhoc_parser.etl_hh is {str(total_rows)}')
        # self.logger.warning('-------------------------------------')