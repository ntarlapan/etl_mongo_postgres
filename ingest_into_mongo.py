import csv
from datetime import datetime
import zipfile

from pymongo import MongoClient


# create client to mongo
client = MongoClient('localhost', 27017)

# create a new database:
db = client['pymongo_test']


# write one example
# posts = db.posts

# new_result = posts.insert_many([post_1, post_2, post_3])
# print('Multiple posts: {0}'.format(new_result.inserted_ids))

# read the data from the csv files

order_path = 'orders_202002181303.csv'
user_path = 'users_202002181303.csv'

today = datetime.utcnow()

# database for users
orders = db['orders']

#first delete then ingest the orders
orders.delete_many( {} )
print('deleted old orders')

with open(order_path, 'r') as file:
    csv_reader = csv.DictReader(file)
    order_list = []
    for record in csv_reader:
        record['updated_at'] = datetime.strptime(record['updated_at'],
                                                 '%Y-%m-%d %H:%M:%S')
        order_list.append(dict(record))
    
    new_result = orders.insert_many(order_list)

    print('Multiple orders: {0}'.format(len(new_result.inserted_ids)))

# database for users
users = db['users']
users.delete_many( {} )
print('deleted old users')


# first delete then ingest the users

with open(user_path, 'r') as file:
    csv_reader = csv.DictReader(file)
    new_result = users.insert_many(csv_reader)

    print('Multiple users: {0}'.format(len(new_result.inserted_ids)))