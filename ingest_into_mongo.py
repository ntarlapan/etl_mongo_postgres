import csv
from datetime import datetime
from io import TextIOWrapper
import logging
import time
import zipfile
from zipfile import ZipFile

from utils import get_env_variable

from pymongo import MongoClient
from pymongo.errors import BulkWriteError

logging.basicConfig(filename='etl_app.log',
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.DEBUG)

logger = logging.getLogger('loading_csv_to_mongo')
logger.info("Running ETL Service")


MONGO_DATABASE = get_env_variable('MONGO_DATABASE')
MONGO_HOST = get_env_variable('MONGO_HOST')
MONGO_PORT = int(get_env_variable('MONGO_PORT'))

DATE_COLS_ORDERS = ['created_at', 'date_tz', 'updated_at', 'fulfillment_date_tz']
DATE_COLS_USERS = ['created_at', 'updated_at']
MAX_RECORD_NUMBER = 10 ** 3  # todo make this number configurable from config

# create client to mongo
client = MongoClient(MONGO_HOST, MONGO_PORT)

# create a new database:
db = client[MONGO_DATABASE]

# read the data from the csv files

# todo replace these hardcoded paths with paths from environment/config
order_path = '../data/orders_202002181303.csv'
user_path = '../data/users_202002181303.csv'

# database for users
orders = db['orders']

# first delete then ingest the orders
orders.delete_many({})
logger.info('deleted old orders')

# database for users
users = db['users']
users.delete_many({})
logger.info('deleted old users')


# TODO parse the dates
def parse_record_dates(record, date_columns):
    """
    For the keys in record that are in date_columns, parse the values to datetime
    """
    parsed_dict = {}
    for key, value in record.items():
        if key in ['id_']:
            continue
        elif key in date_columns:
            try:
                parsed_dict[key] = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                # TODO include a logger here
                # logger.lo
                parsed_dict[key] = None
        else:
            parsed_dict[key] = value

    return parsed_dict


def load_csv_to_mongo(collection, csv_file_path, date_columns, max_batch_size=MAX_RECORD_NUMBER):
    """
    load csv into the specified collection.
    """
    with open(csv_file_path, 'r') as file:
        # TODO adjust ingestion to take memory into consideration. Use some number of rows only
        # TODO: parse All the numeric values.
        csv_reader = csv.DictReader(file)
        record_list = []
        cur_index = 0
        for record in csv_reader:
            cur_index +=1
            parsed_record = parse_record_dates(record, date_columns)
            # record['updated_at'] = datetime.strptime(record['updated_at'],
            #                                          '%Y-%m-%d %H:%M:%S')
            record_list.append(parsed_record)
            if len(record_list) >= max_batch_size:
                try:
                    new_result = collection.insert_many(record_list)
                except BulkWriteError as exc:
                    logger.warning(exc.details)
                logger.info(f'inserted: {len(new_result.inserted_ids)} rows; total rows inserted {cur_index}')
                # empty the batch:
                record_list = []
        # insert remaining rows may (not enough to gather MAX_RECORD_NUMBER)
        new_result = collection.insert_many(record_list)
        logger.info(f'inserted: {len(new_result.inserted_ids)} rows; total rows inserted {cur_index}')


logger.info('loading orders')
load_csv_to_mongo(orders, order_path, DATE_COLS_ORDERS)
logger.info('loading users')
load_csv_to_mongo(users, user_path, DATE_COLS_USERS)
