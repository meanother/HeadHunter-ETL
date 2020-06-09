from database import Database
from fetch import FetchHHVacancy
from read_stream import ReadStreamClass
from transform import TransformData
from config import Config
# from components.streaming_etl.config import Config
# from .config import Config


def main():
    read = read_data()
    transform = transform_data(read)
    make_insert(transform)


def fetch_data(name):
    fetch = FetchHHVacancy(name)
    write = fetch.send_to_topic(fetch.fetch_all_results())
    return write


def read_data():
    read = ReadStreamClass()
    raw_data = read.fetch_data_from_topic()
    return raw_data


def transform_data(data):
    transform = TransformData()
    transformed_data = transform.make_row_data(data)
    return transformed_data


def make_insert(dataframe):
    database = Database(Config)
    final = database.insert_data(dataframe)
    return final


# array = ['java', 'python', 'sql', 'oracle', 'frontend', 'data engineer', 'bigdata']
# array = ['sql', 'bigdata']
array = ['python']
for i in array:
    fetch_data(i)
while True:
    main()
