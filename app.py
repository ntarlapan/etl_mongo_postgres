import datetime
import os

from flask import Flask
from flask_crontab import Crontab
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine
from sqlalchemy.sql.expression import text
from pymongo import MongoClient

from db_statements import STMT_CREATE_ORDERS_USERS, STMT_UPSERT_POSTGRES

DEFAULT_MAX_DATE = datetime.datetime(2020, 1, 1, 0, 0, 0)
DEFAULT_MAX_ROWS = 1 #1000
BLANK_USER_DICT = {'user_first_name': None,
'user_last_name': None, 'user_merchant_id': None,
'user_phone_number': None, 'user_created_at': None, 'user_updated_at':None}

# Config the postgres connection

def get_env_variable(name):
    try:
        return os.environ[name]
    except KeyError:
        message = "Expected environment variable '{}' not set.".format(name)
        raise Exception(message)


# the values of those depend on your setup
POSTGRES_URL = get_env_variable("POSTGRES_URL")
POSTGRES_USER = get_env_variable("POSTGRES_USER")
POSTGRES_PW = get_env_variable("POSTGRES_PW")
POSTGRES_DB = get_env_variable("POSTGRES_DB")

DB_URL = 'postgresql+psycopg2://{user}:{pw}@{url}/{db}'.format(user=POSTGRES_USER, pw=POSTGRES_PW, url=POSTGRES_URL,
                                                               db=POSTGRES_DB)

app = Flask(__name__)
crontab = Crontab(app)

app.config['SQLALCHEMY_DATABASE_URI'] = DB_URL
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False  # silence the deprecation warning

# db_postgres = SQLAlchemy(app)

INSTALLED_DATABASE_URL = 'postgresql://postgres:uGAn7agA@localhost:5432/go_parrot'
engine = create_engine(INSTALLED_DATABASE_URL, echo=True)

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
else:
    is_first_synchronization = False

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
    # if received a value from postgres - use it
    # for filtering the results from mongo
    # TODO It is necessary to include a limit on the number of records to fetch, like 1000
    if is_first_synchronization:
        # on the first run take all orders up to the date of the first synchronization
        query = {'updated_at': {'$lte': max_date}}  # TODO order the rows by date!!!
    else:
        # on subsequent runs, take all orders that have been updated more recently than last synch
        query = {'updated_at': {'$gte': max_date}} # TODO order the rows by date!!!

    updated_orders = orders.find(query).sort('updated_at')


    # TODO should I maybe transform some values like dates. Not sure - check.
    # Transform - iterate through orders
    # import pdb; pdb.set_trace()
    for updated_order_record in updated_orders:
        # for each order select the corresponding user
        user_id = updated_order_record['user_id']
        user_record = users.find_one({'user_id': user_id })
        # connect the dicts, get rid of the '_id' key
        # TODO what if there is no such user in the users collection
        # import pdb; pdb.set_trace()
        if user_record is not None:
            for key, value in user_record.items():
                if key in ['id_', 'user_id']:
                    continue
                updated_order_record['user' + key] = value
        else:
            for key, value in BLANK_USER_DICT.items():
                updated_order_record[key] = value
        import pdb; pdb.set_trace()
        current_row_batch.append(updated_order_record)
        if len(current_row_batch) > DEFAULT_MAX_ROWS:
            # update the records in the postgres database
            with engine.connect() as connection:
                connection.execute(text(STMT_UPSERT_POSTGRES), current_row_batch)
            # empty the batch:
            current_row_batch = []

    print('synchronization performed successfully for timedate {}')


if __name__ == '__main__':
    synch_postgres_with_mongo()
