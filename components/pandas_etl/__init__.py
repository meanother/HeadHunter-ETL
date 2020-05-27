import logging
# from config import Config
from hhfetch import FetchHHVacancy
from transform import TransformData
from db import Database


def main():
    first_step = fetch_headhunter()
    second_step = clean_raw_data(first_step)
    last_step = get_insert(second_step)


def fetch_headhunter():
    print('Fetch data from hh.ru')
    rest = FetchHHVacancy('')
    fetch_data = rest.fetch_all_results()
    return fetch_data


def clean_raw_data(data):
    print('Transforming data')
    transform = TransformData()
    prepare_data = transform.make_row_data(data)
    return prepare_data


def get_insert(dataframe):
    print("Upload data")
    database = Database()
    final = database.insert_data(dataframe)
    return final


main()
