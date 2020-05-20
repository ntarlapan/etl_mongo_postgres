STMT_CREATE_ORDERS_USERS = '''CREATE TABLE IF NOT EXISTS order_users (

id INT PRIMARY KEY,
created_at timestamp without time zone,
date_tz timestamp without time zone,
item_count INT,
order_id VARCHAR(31) UNIQUE,
receive_method VARCHAR(63),
status VARCHAR(31),
store_id VARCHAR(63),
subtotal numeric(10, 4),
tax_percentage numeric (10, 7),
total numeric(10, 4),
total_discount numeric(10, 4),
total_gratuity numeric(10, 4),
total_tax numeric(10, 4),
updated_at timestamp without time zone,
user_id varchar(16),
fulfillment_date_tz timestamp without time zone,
user_first_name VARCHAR(32) DEFAULT NULL,
user_last_name VARCHAR(50) DEFAULT NULL,
user_merchant_id VARCHAR(15) DEFAULT NULL,
user_phone_number VARCHAR(31) DEFAULT NULL,
user_created_at timestamp without time zone DEFAULT NULL,
user_updated_at timestamp without time zone DEFAULT NULL
)
'''

STMT_UPSERT_POSTGRES = """
INSERT INTO order_users (
id ,created_at , date_tz, item_count, order_id, receive_method,
status , store_id , subtotal, tax_percentage, total,
total_discount, total_gratuity, total_tax, updated_at,
user_id, fulfillment_date_tz, user_first_name,
user_last_name, user_merchant_id, user_phone_number, user_created_at, user_updated_at
) 
VALUES (
:id, :created_at , :date_tz, :item_count, :order_id, :receive_method,
:status, :store_id , :subtotal, :tax_percentage, :total,
:total_discount, :total_gratuity, :total_tax, :updated_at,
:user_id, :fulfillment_date_tz, :user_first_name,
:user_last_name, :user_merchant_id, :user_phone_number, :user_created_at, :user_updated_at
)
ON CONFLICT ON CONSTRAINT order_users_order_id_key
DO
UPDATE
SET
id = EXCLUDED.id,
created_at = EXCLUDED.created_at, 
date_tz = EXCLUDED.date_tz, 
item_count = EXCLUDED.item_count, 
receive_method EXCLUDED.receive_method,
status = EXCLUDED.status, 
store_id = EXCLUDED.store_id,
subtotal = EXCLUDED.subtotal, 
tax_percentage = EXCLUDED.tax_percentage, 
total = EXCLUDED.total,
total_discount = EXCLUDED.total_discount, 
total_gratuity = EXCLUDED.total_gratuity, 
total_tax = EXCLUDED.total_tax, 
updated_at = EXCLUDED.updated_at,
user_id = EXCLUDED.user_id, 
fulfillment_date_tz = EXCLUDED.fulfillment_date_tz, 
user_first_name = EXCLUDED.user_first_name,
user_last_name = EXCLUDED.user_last_name, 
user_merchant_id = EXCLUDED.user_merchant_id, 
user_phone_number = EXCLUDED.user_phone_number,
user_created_at = EXCLUDED.user_created_at, 
user_updated_at = EXCLUDED.user_updated_at

WHERE order_users.updated_at = EXCLUDED.updated_at
"""