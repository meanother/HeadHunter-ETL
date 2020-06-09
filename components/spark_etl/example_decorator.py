# import os
# import sys
# MODULE_NAME = 'streaming_etl'
# sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))+os.sep+MODULE_NAME)
# try:
#     from components.streaming_etl.fetch import FetchHHVacancy
# except:
#     from fetch import FetchHHVacancy

import time
from functools import wraps
from time import sleep


def timer(func):
    @wraps(func)
    def wrapper_function(*args):
        start = time.time()
        sleep(2)
        print('sleeping 2 sec')
        print(time.time() - start)
        return func(*args)

    return wrapper_function


def example_logger(func):
    import logging
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(name)s %(funcName)s %(process)d:%(processName)s [%(levelname)s] %(message)s')
    log = logging.getLogger(__name__)

    @wraps(func)
    def wrapper(*args, **kwargs):
        log.info('example INFO log message')
        return func(*args, **kwargs)

    return wrapper


@timer
@example_logger
def check(string):
    print(f'this is a string: {string}')


check('qqqq')
