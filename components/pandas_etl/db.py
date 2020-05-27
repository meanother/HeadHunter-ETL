import sqlalchemy
from sqlalchemy.types import String, INT, Boolean, DateTime, TEXT


class Database:

    def __init__(self):
        pass

    def insert_data(self, dataframe):
        engine = sqlalchemy.create_engine('postgresql://semenov:12345@77.244.65.15:58642/parsing_db')
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
