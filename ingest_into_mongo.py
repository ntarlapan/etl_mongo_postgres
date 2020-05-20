import csv
from datetime import datetime
import logging
import time
import zipfile

from pymongo import MongoClient
from pymongo.errors import BulkWriteError

# TODO set the logging to a file : data_loading.log

DATE_COLS_ORDERS = ['created_at', 'date_tz', 'updated_at', 'fulfillment_date_tz']
DATE_COLS_USERS = ['created_at', 'updated_at']
MAX_RECORD_NUMBER = 10 ** 6  # todo make this number configurable from config

# create client to mongo
client = MongoClient('localhost', 27017)

# create a new database:
db = client['pymongo_test']

# write one example
# posts = db.posts

# new_result = posts.insert_many([post_1, post_2, post_3])
# print('Multiple posts: {0}'.format(new_result.inserted_ids))

# read the data from the csv files

# todo replace these hardcoded paths with paths from environment/config
order_path = '../data/orders_202002181303.csv'
user_path = '../data/users_202002181303.csv'

today = datetime.utcnow()

# database for users
orders = db['orders']

# first delete then ingest the orders
orders.delete_many({})
print('deleted old orders')

# database for users
users = db['users']
users.delete_many({})
print('deleted old users')


# TODO parse the dates
def parse_record_dates(record, date_columns):
    """

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

    """
    with open(csv_file_path, 'r') as file:
        # TODO adjust ingestion to take memory into consideration. Use some number of rows only
        # TODO: parse All the numeric values.
        csv_reader = csv.DictReader(file)
        record_list = []
        # import pdb; pdb.set_trace()
        for record in csv_reader:
            parsed_record = parse_record_dates(record, date_columns)
            # record['updated_at'] = datetime.strptime(record['updated_at'],
            #                                          '%Y-%m-%d %H:%M:%S')
            record_list.append(parsed_record)
            if len(record_list) > max_batch_size:
                try:
                    new_result = collection.insert_many(record_list)
                except BulkWriteError as exc:
                    print(exc.details)
                print('inserted: {0}'.format(len(new_result.inserted_ids)))
                # print('sleeping...')
                # time.sleep(2)
        # insert remaining rows may (not enough to gather MAX_RECORD_NUMBER)
        new_result = collection.insert_many(record_list)
        print('inserted: {0}'.format(len(new_result.inserted_ids)))


print('loading orders')
load_csv_to_mongo(orders, order_path, DATE_COLS_ORDERS)
print('loading users')
load_csv_to_mongo(users, user_path, DATE_COLS_USERS)
