\c oltp;

CREATE TABLE customers (
id SERIAL PRIMARY KEY,
name TEXT,
country TEXT,
updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE products (
id SERIAL PRIMARY KEY,
name TEXT,
groupname TEXT,
price NUMERIC,
updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE sales (
id SERIAL PRIMARY KEY,
customer_id INT,
product_id INT,
qty INT,
updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

\c mrr;

CREATE TABLE mrr_dim_customers (
id INT,
name TEXT,
country TEXT,
updated_at TIMESTAMP
);

CREATE TABLE mrr_dim_products (
id INT,
name TEXT,
groupname TEXT,
price NUMERIC,
updated_at TIMESTAMP
);

CREATE TABLE mrr_fact_sales (
id INT,
customer_id INT,
product_id INT,
qty INT,
updated_at TIMESTAMP
);

\c stg;

CREATE TABLE stg_dim_customers (
id INT,
name TEXT,
country TEXT,
updated_at TIMESTAMP
);

CREATE TABLE stg_dim_products (
id INT,
name TEXT,
groupname TEXT,
price NUMERIC,
updated_at TIMESTAMP
);

CREATE TABLE stg_fact_sales (
id INT,
customer_id INT,
product_id INT,
qty INT,
updated_at TIMESTAMP
);

\c dwh;

CREATE TABLE dim_customers (
customer_key SERIAL PRIMARY KEY,
id INT,
name TEXT,
country TEXT
);

CREATE TABLE dim_products (
product_key SERIAL PRIMARY KEY,
id INT,
name TEXT,
groupname TEXT,
price NUMERIC
);

CREATE TABLE fact_sales (
sales_key SERIAL PRIMARY KEY,
customer_id INT,
product_id INT,
qty INT,
updated_at TIMESTAMP
);

\c dwh;

CREATE TABLE high_water_mark (
table_name TEXT PRIMARY KEY,
last_loaded TIMESTAMP
);

INSERT INTO high_water_mark VALUES
('customers', '1900-01-01'),
('products', '1900-01-01'),
('sales', '1900-01-01');

\c dwh;

CREATE TABLE etl_logs (
id SERIAL PRIMARY KEY,
process_name TEXT,
status TEXT,
start_time TIMESTAMP,
end_time TIMESTAMP,
message TEXT
);

\c oltp;

INSERT INTO customers (name, country) VALUES
('Alice', 'USA'),
('Bob', 'Germany'),
('Charlie', 'Netherlands');

INSERT INTO products (name, groupname, price) VALUES
('Laptop', 'Electronics', 1000),
('Phone', 'Electronics', 500),
('Table', 'Furniture', 200);

INSERT INTO sales (customer_id, product_id, qty) VALUES
(1, 1, 1),
(2, 2, 2),
(3, 3, 1);
