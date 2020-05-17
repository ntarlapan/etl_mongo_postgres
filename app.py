import datetime

from pymongo import MongoClient



#configs mongodb



# connect to mongodb, read the data

#establish the connection
client = MongoClient('localhost', 27017)

db_mongo = client['pymongo_test']
users = db_mongo['users']
orders = db_mongo['orders']

# before extracting the max_date from Postgres
max_date = None 

# here query the postgres database:

# if received a value from postgres - use it 
# for filtering the results from mongo
if max_date is None:
    query = { }
else:
    query = { 'updated_at': {'$gte': max_date} }
updated_orders = orders.find( query )

print(f'len(updated_orders) = {len(list(updated_orders))}')


# transform

# iterate through orders

# for each order select the corresponding user

# connect the dicts, get rid of the '_id' key

# update the records in the postgres database

