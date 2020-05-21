from datetime import datetime
import logging
import os

from flask import Flask
from flask_crontab import Crontab
from flask_sqlalchemy import SQLAlchemy
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.sql.expression import text
from pymongo import MongoClient

# from .db_statements import STMT_CREATE_ORDERS_USERS, STMT_UPSERT_POSTGRES
from db_statements import *
from utils import database_exists, get_env_variable

logging.basicConfig(filename='etl_app.log',
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.DEBUG)

logger = logging.getLogger('etl_service')
logger.info("Running ETL Service")

DEFAULT_MAX_DATE = datetime(2020, 1, 1, 0, 0, 0)
DEFAULT_MAX_ROWS = 1000
BLANK_USER_DICT = {'user_first_name': None,
                   'user_last_name': None, 'user_merchant_id': None,
                   'user_phone_number': None, 'user_created_at': None, 'user_updated_at': None}

# Config the postgres connection


# the values of those depend on your setup
POSTGRES_URL = get_env_variable("POSTGRES_URL")
POSTGRES_USER = get_env_variable("POSTGRES_USER")
POSTGRES_PW = get_env_variable("POSTGRES_PW")
POSTGRES_DB = get_env_variable("POSTGRES_DB")

app = Flask(__name__)
crontab = Crontab(app)
# db_postgres = SQLAlchemy(app)

database_connection_url = f'postgresql://{POSTGRES_USER}:{POSTGRES_PW}@{POSTGRES_URL}/{POSTGRES_DB}'
engine = create_engine(database_connection_url, echo=False)

try:
    engine.connect()
except sqlalchemy.exc.OperationalError:
    logger.warning(f'Could not connect to the database {POSTGRES_DB}')
    logger.info('Trying to connect without using the database name')
    database_connection_url = f'postgresql://{POSTGRES_USER}:{POSTGRES_PW}@{POSTGRES_URL}/'
    engine = create_engine(database_connection_url, echo=False)
    # if database does not exist create it:
    if not database_exists(engine, POSTGRES_DB):
        logger.info(f'The database {POSTGRES_DB} does not exist, re-creating the database')
        with engine.connect() as connection:
            with connection.execution_options(isolation_level='AUTOCOMMIT'):
                connection.execute(f'CREATE DATABASE {POSTGRES_DB}')
            # reconnect to the target database:
            database_connection_url = f'postgresql://{POSTGRES_USER}:{POSTGRES_PW}@{POSTGRES_URL}/{POSTGRES_DB}'
            engine = create_engine(database_connection_url, echo=False)

# create the table in postgres (if it does not exist)
with engine.connect() as connection:
    result = connection.execute(STMT_CREATE_ORDERS_USERS)

# here query the postgres database:

STMT_MAX_DATE = """SELECT MAX(updated_at) from ORDER_USERS;"""

with engine.connect() as connection:
    max_date = connection.execute(STMT_MAX_DATE).scalar()

if max_date is None:
    max_date = DEFAULT_MAX_DATE
    is_first_synchronization = True
    logger.info('Running the synchronization for the first time!')
else:
    is_first_synchronization = False
    logger.info(f'Running the synchronization with existing data, max_date = {max_date}!')

# configs mongodb
MONGO_COLLECTION = 'pymongo_test'

# connect to mongodb, read the data
# establish the connection
client = MongoClient('localhost', 27017)
# TODO use "with client" context manager to automatically close the connection.


db_mongo = client[MONGO_COLLECTION]
users = db_mongo['users']
orders = db_mongo['orders']


@crontab.job(minute="*/5")
def synch_postgres_with_mongo():
    current_row_batch = []
    if is_first_synchronization:
        # on the first run take all orders up to the date of the first synchronization
        query = {'updated_at': {'$lte': max_date}}
    else:
        # on subsequent runs, take all orders that have been updated more recently than last synch
        query = {'updated_at': {'$gte': max_date}}

    updated_orders = orders.find(query).sort('updated_at')

    # Transform - iterate through orders
    for updated_order_record in updated_orders:
        # for each order select the corresponding user
        user_id = updated_order_record['user_id']
        user_record = users.find_one({'user_id': user_id})
        # connect the dicts, get rid of the '_id' key
        # TODO what if there is no such user in the users collection
        if user_record is not None:
            for key, value in user_record.items():
                if key in ['id_', 'user_id']:
                    continue
                updated_order_record['user_' + key] = value
        else:
            for key, value in BLANK_USER_DICT.items():
                updated_order_record[key] = value
        current_row_batch.append(updated_order_record)
        if len(current_row_batch) >= DEFAULT_MAX_ROWS:
            # update the records in the postgres database
            with engine.connect() as connection:
                connection.execute(text(STMT_UPSERT_POSTGRES), current_row_batch)
            logger.debug(f'inserted {len(current_row_batch)} rows in Postgres')
            # empty the batch:
            current_row_batch = []
    with engine.connect() as connection:
        connection.execute(text(STMT_UPSERT_POSTGRES), current_row_batch)
    logger.debug(f'inserted {len(current_row_batch)} rows in Postgres')

    logger.info(f'synchronization performed successfully at {datetime.now()}')


if __name__ == '__main__':
    synch_postgres_with_mongo()
