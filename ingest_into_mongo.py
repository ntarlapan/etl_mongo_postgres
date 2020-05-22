import csv
from datetime import datetime
from io import TextIOWrapper
import logging
from zipfile import ZipFile

from pymongo import MongoClient
from pymongo.errors import BulkWriteError

from utils import get_env_variable, load_env_variables_from_file

# configurate the logger
logging.basicConfig(filename='etl_app.log',
                    filemode='a',
                    format='%(asctime)s,%(msecs)d | %(name)s | %(levelname)s | %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

logger = logging.getLogger('loading_csv_to_mongo')
logger.info("Running Loadind from CSV to Mongo")

load_env_variables_from_file(env_file='.env')

# the path to zip file
DATA_ZIP_PATH = get_env_variable('DATA_ZIP_PATH')

# the env variable for mongo
MONGO_DATABASE = get_env_variable('MONGO_DATABASE')
MONGO_HOST = get_env_variable('MONGO_HOST')
MONGO_PORT = int(get_env_variable('MONGO_PORT'))


DATE_COLS = {
    'order': ['created_at', 'date_tz', 'updated_at', 'fulfillment_date_tz'],
    'user': ['created_at', 'updated_at']
}

# if the maximum rows per batch is set in env, use it, otherwise set a default value
try:
    MAX_RECORD_NUMBER = get_env_variable('MAX_RECORD_NUMBER')
except KeyError:
    MAX_RECORD_NUMBER = 10 ** 3

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
                parsed_dict[key] = None
                logger.debug(f'Could not parse value {value} of {key} for record {record}')
        else:
            parsed_dict[key] = value

    return parsed_dict


def load_csv_to_mongo(collection, csv_dictreader_iterator, date_columns, max_batch_size=MAX_RECORD_NUMBER):
    """
    load csv into the specified collection.
    """
    # with open(csv_file_path, 'r') as file:
    #     # TODO: parse All the numeric values.
    # csv_reader = csv.DictReader(file)
    record_list = []
    cur_index = 0
    for record in csv_dictreader_iterator:
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

def open_zipped_csv(acrchived_file_name, destination_collection, record_type):
    with zf.open(acrchived_file_name, 'r') as infile:
        csv_read_iterator = csv.DictReader(TextIOWrapper(infile, 'utf-8'))
        load_csv_to_mongo(orders, csv_read_iterator, DATE_COLS[record_type])



if __name__ == '__main__':
    # create client to mongo
    client = MongoClient(MONGO_HOST, MONGO_PORT)
    # create a new database:
    db = client[MONGO_DATABASE]
    # database for users
    orders = db['orders']
    # first delete then ingest the orders
    orders.delete_many({})
    logger.info('deleted old orders')
    # reset collection users
    users = db['users']
    users.delete_many({})
    logger.info('deleted old users')

    # open the directory and load the csv
    with ZipFile(DATA_ZIP_PATH) as zf:
        file_list = zf.namelist()

        order_files = [file_path for file_path in file_list if 'order' in file_path]
        user_files = [file_path for file_path in file_list if 'user' in file_path]

        logger.info('loading orders')
        for order_file in order_files:
            open_zipped_csv(order_file, orders, 'order')
        logger.info('loading users')
        for user_file in user_files:
            open_zipped_csv(user_file, users, 'user')

    logger.info(f'Successful loading of csv files from zip {DATA_ZIP_PATH}')