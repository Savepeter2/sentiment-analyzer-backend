import logging
import os
import sys
import configparser
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from configs.logger_config import error_log_file_path, log_file_path, keyword_log_file_path
config = configparser.RawConfigParser()

logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

error_logger = logging.getLogger("error_logger")
error_logger.setLevel(logging.ERROR)
error_handler = logging.FileHandler(error_log_file_path)
error_logger.addHandler(error_handler)

logger_handler = logging.FileHandler(log_file_path)
logger = logging.getLogger("logger")
logger.addHandler(logger_handler)
logger.setLevel(logging.INFO)

keyword_logger_handler = logging.FileHandler(keyword_log_file_path)
keyword_logger = logging.getLogger("keyword_logger")
keyword_logger.addHandler(keyword_logger_handler)
keyword_logger.setLevel(logging.INFO)

config.read('secrets.ini')


ACCESS_KEY_ID = config['AWS_CREDENTIALS']['ACCESS_KEY_ID']
SECRET_ACCESS_KEY = config['AWS_CREDENTIALS']['SECRET_ACCESS_KEY']
REDSHIFT_HOST = config['AWS_CREDENTIALS']['REDSHIFT_HOST']
REDSHIFT_PORT = config['AWS_CREDENTIALS']['REDSHIFT_PORT']
REDSHIFT_DATABASE = config['AWS_CREDENTIALS']['REDSHIFT_DATABASE']
REDSHIFT_PASSWORD = config['AWS_CREDENTIALS']["REDSHIFT_PASSWORD"]
REDSHIFT_USERNAME = config['AWS_CREDENTIALS']['REDSHIFT_USERNAME']
AWS_REGION = config['AWS_CREDENTIALS']["AWS_REGION"]
USER_DIM_TABLE = config['REDSHIFT_TABLES']['USER_DIM_TABLE_NAME']
DATE_DIM_TABLE = config['REDSHIFT_TABLES']['DATE_DIM_TABLE_NAME']
TOPIC_DIM_TABLE = config['REDSHIFT_TABLES']['TOPIC_DIM_TABLE_NAME']
FIRM_DIM_TABLE = config['REDSHIFT_TABLES']['FIRM_DIM_TABLE_NAME']
FACT_TWEETS_TABLE = config['REDSHIFT_TABLES']['FACT_TWEETS_TABLE_NAME']

