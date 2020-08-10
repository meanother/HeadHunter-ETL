import logging
import os

DATABASE_ADDRESS = os.getenv('ura_db_address')
DATABASE_LOGIN = os.getenv('ura_db_login')
DATABASE_PASS = os.getenv('ura_db_pass')
DATABASE_NAME = os.getenv('ura_db_name')

BOOTSTRAP_SERVER = '35.230.42.114:9092'
TOPIC_NAME = 'yandex-bot'
INTERVAL = str(1)

LOG_FORMATTER = '%(asctime)s %(name)s %(funcName)s %(process)d:%(processName)s [%(levelname)s] %(message)s'

logging.basicConfig(level=logging.INFO,
                    format=LOG_FORMATTER)
logger = logging.getLogger(__name__)
