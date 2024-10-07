CREATE DATABASE ecommerce;

CREATE TABLE geolocation(
    geolocation_zip_code_prefix BIGINT NOT NULL ,
    geolocation_lat FLOAT ,
    geolocation_lng FLOAT ,
    geolocation_city VARCHAR(50) ,
    geolocation_state VARCHAR(2) 
);

COPY geolocation
FROM 'C:\Users\public\olist_geolocation_dataset.csv'
DELIMITER ','
CSV HEADER;

CREATE TABLE customers (
    customer_id VARCHAR(32) PRIMARY KEY NOT NULL ,
    customer_unique_id VARCHAR(32) NOT NULL ,
    customer_zip_code_prefix BIGINT ,
    customer_city VARCHAR(32) ,
    customer_state VARCHAR(2) 
);

COPY customers
FROM 'C:\Users\public\olist_customers_dataset.csv'
DELIMITER ','
CSV HEADER;

CREATE TABLE orders (
    order_id VARCHAR(32) PRIMARY KEY NOT NULL ,
    customer_id VARCHAR(32) REFERENCES customers (customer_id) ,
    order_status VARCHAR(11) ,
    order_purchase_timestamp TIMESTAMP ,
    order_approved_at TIMESTAMP ,
    order_delivered_carrier_date TIMESTAMP ,
    order_delivered_customer_date TIMESTAMP ,
    order_estimated_delivery_date TIMESTAMP
);

COPY orders
FROM 'C:\Users\public\olist_orders_dataset.csv'
DELIMITER ','
CSV HEADER;

CREATE TABLE reviews (
    review_id VARCHAR(32) NOT NULL ,
    order_id VARCHAR(32) REFERENCES orders (order_id) ,
    review_score SMALLINT ,
    review_comment_title VARCHAR,
    review_comment_message VARCHAR,
    review_creation_date TIMESTAMP ,
    review_answer_timestamp TIMESTAMP
);

COPY reviews
FROM 'C:\Users\public\olist_order_reviews_dataset.csv'
DELIMITER ','
CSV HEADER ENCODING 'LATIN1';
-- deleted PRIMARY KEY CONSTRAINT review_id

CREATE TABLE payments (
    order_id VARCHAR(32) REFERENCES orders (order_id) ,
    payment_sequential SMALLINT ,
    payment_type VARCHAR(15) ,
    payment_installments SMALLINT ,
    payment_value REAL
);

COPY payments
FROM 'C:\Users\public\olist_order_payments_dataset.csv'
DELIMITER ','
CSV HEADER;

CREATE TABLE sellers (
    seller_id VARCHAR(32) PRIMARY KEY NOT NULL ,
    seller_zip_code_prefix BIGINT ,
    seller_city VARCHAR(50) ,
    seller_state VARCHAR(2)
);

COPY sellers
FROM 'C:\Users\public\olist_sellers_dataset.csv'
DELIMITER ','
CSV HEADER;

CREATE TABLE products (
    product_id VARCHAR(32) PRIMARY KEY NOT NULL ,
    product_category_name VARCHAR(50) ,
    product_name_lenght FLOAT ,
    product_description_lenght FLOAT ,
    product_photos_qty FLOAT ,
    product_weight_g FLOAT ,
    product_length_cm FLOAT ,
    product_height_cm FLOAT ,
    product_width_cm FLOAT
);

COPY products
FROM 'C:\Users\public\olist_products_dataset.csv'
DELIMITER ','
CSV HEADER;

CREATE TABLE order_items (
    order_id VARCHAR(32) REFERENCES orders (order_id) ,
    order_item_id INT NOT NULL ,
    product_id VARCHAR(32) REFERENCES products (product_id) ,
    seller_id VARCHAR(32) REFERENCES sellers (seller_id) ,
    shipping_limit_date TIMESTAMP ,
    price FLOAT ,
    freight_value FLOAT
);

COPY order_items
FROM 'C:\Users\public\olist_order_items_dataset.csv'
DELIMITER ','
CSV HEADER;
-- deleted PRIMARY KEY CONSTRAINT order_item_id

