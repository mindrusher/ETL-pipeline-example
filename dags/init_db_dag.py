from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
from faker import Faker
import random

fake = Faker()


def escape_sql_string(s):
    return str(s).replace("'", "''")


def execute_sql(db_name, sql):
    conn = psycopg2.connect(
        host="postgres",
        database=db_name,
        user="airflow",
        password="airflow"
    )

    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(sql)
    cursor.close()
    conn.close()


def init_oltp():
    """Инициализация источника данных"""

    sql = """
    CREATE TABLE IF NOT EXISTS customers (
        id SERIAL PRIMARY KEY,
        name TEXT,
        country TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS products (
        id SERIAL PRIMARY KEY,
        name TEXT,
        groupname TEXT,
        price NUMERIC,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS sales (
        id SERIAL PRIMARY KEY,
        customer_id INT,
        product_id INT,
        qty INT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    execute_sql("oltp", sql)

    # тестовые данные, закинуть Faker в контейнер
    customers = []
    for i in range(100):
        name = escape_sql_string(fake.name())
        country = escape_sql_string(fake.country())
        updated_at = datetime.now() - timedelta(days=random.randint(0, 365))
        customers.append(f"('{name}', '{country}', '{updated_at}')")
    
    insert_customers = f"""
        INSERT INTO customers (name, country, updated_at) VALUES
        {', '.join(customers)}
        ON CONFLICT DO NOTHING;
    """
    execute_sql("oltp", insert_customers)
    
    product_groups = ['Electronics', 'Furniture', 'Clothing', 'Books', 'Sports', 'Toys', 
                      'Food', 'Automotive', 'Health', 'Beauty']
    products = []
    used_names = set()
    
    for i in range(60):
        while True:
            product_name = f"{fake.word().capitalize()} {fake.word().capitalize()}"
            if product_name not in used_names:
                used_names.add(product_name)
                break
        
        groupname = random.choice(product_groups)
        price = round(random.uniform(10, 2000), 2)
        updated_at = datetime.now() - timedelta(days=random.randint(0, 180))
        products.append(f"('{product_name}', '{groupname}', {price}, '{updated_at}')")
    
    batch_size = 20
    for i in range(0, len(products), batch_size):
        batch = products[i:i+batch_size]
        insert_products = f"""
            INSERT INTO products (name, groupname, price, updated_at) VALUES
            {', '.join(batch)}
            ON CONFLICT DO NOTHING;
        """
        execute_sql("oltp", insert_products)
    
    sales = []

    for i in range(250):
        customer_id = random.randint(1, 100)
        product_id = random.randint(1, 60)
        qty = random.randint(1, 10)

        if random.random() < 0.1:
            qty = random.randint(20, 100)
        
        days_ago = random.randint(0, 90)
        hours_ago = random.randint(0, 23)
        minutes_ago = random.randint(0, 59)
        updated_at = datetime.now() - timedelta(days=days_ago, hours=hours_ago, minutes=minutes_ago)
        
        sales.append(f"({customer_id}, {product_id}, {qty}, '{updated_at}')")
    
    batch_size = 50
    for i in range(0, len(sales), batch_size):
        batch = sales[i:i+batch_size]
        insert_sales = f"""
            INSERT INTO sales (customer_id, product_id, qty, updated_at) VALUES
            {', '.join(batch)}
            ON CONFLICT DO NOTHING;
        """
        execute_sql("oltp", insert_sales)

    # INSERT INTO customers (name, country) VALUES
    # ('Alice', 'USA'),
    # ('Bob', 'Germany'),
    # ('Charlie', 'Netherlands')
    # ON CONFLICT DO NOTHING;

    # INSERT INTO products (name, groupname, price) VALUES
    # ('Laptop', 'Electronics', 1000),
    # ('Phone', 'Electronics', 500),
    # ('Table', 'Furniture', 200)
    # ON CONFLICT DO NOTHING;

    # INSERT INTO sales (customer_id, product_id, qty) VALUES
    # (1, 1, 1),
    # (2, 2, 2),
    # (3, 3, 1)
    # ON CONFLICT DO NOTHING;
    # """

    

def init_hwm():
    """Инициализация high water mark """
    sql = """
        CREATE TABLE IF NOT EXISTS high_water_mark (
            table_name TEXT PRIMARY KEY,
            last_loaded TIMESTAMP
        );

        INSERT INTO high_water_mark VALUES
        ('customers', '1900-01-01'),
        ('products', '1900-01-01'),
        ('sales', '1900-01-01'),
        ('fact_sales', '1900-01-01')
        ON CONFLICT DO NOTHING;

        CREATE TABLE IF NOT EXISTS etl_logs (
            id SERIAL PRIMARY KEY,
            process_name TEXT,
            status TEXT,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            message TEXT
        );
    """

    execute_sql("dwh", sql)


def init_mrr():
    sql = """
    CREATE TABLE IF NOT EXISTS mrr_dim_customers (
        id INT PRIMARY KEY,
        name TEXT,
        country TEXT,
        updated_at TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS mrr_dim_products (
        id INT PRIMARY KEY,
        name TEXT,
        groupname TEXT,
        price NUMERIC,
        updated_at TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS mrr_fact_sales (
        id INT PRIMARY KEY,
        customer_id INT,
        product_id INT,
        qty INT,
        updated_at TIMESTAMP
    );
    """
    execute_sql("mrr", sql)


def init_stg():
    sql = """
    CREATE TABLE IF NOT EXISTS stg_dim_customers (
        id INT PRIMARY KEY,
        name TEXT,
        country TEXT,
        updated_at TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS stg_dim_products (
        id INT PRIMARY KEY,
        name TEXT,
        groupname TEXT,
        price NUMERIC,
        updated_at TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS stg_fact_sales (
        id INT PRIMARY KEY,
        customer_id INT,
        product_id INT,
        qty INT,
        updated_at TIMESTAMP
    );
    """
    execute_sql("stg", sql)


def init_dwh():
    sql = """
    CREATE TABLE IF NOT EXISTS dim_customers (
        customer_key SERIAL PRIMARY KEY,
        id INT UNIQUE,
        name TEXT,
        country TEXT
    );

    CREATE TABLE IF NOT EXISTS dim_products (
        product_key SERIAL PRIMARY KEY,
        id INT UNIQUE,
        name TEXT,
        groupname TEXT,
        price NUMERIC
    );

    CREATE TABLE IF NOT EXISTS fact_sales (
        sales_key SERIAL PRIMARY KEY,
        customer_id INT,
        product_id INT,
        qty INT,
        updated_at TIMESTAMP,
        UNIQUE (customer_id, product_id, updated_at)
    );
    """
    execute_sql("dwh", sql)


def create_view():
    sql = """
    CREATE OR REPLACE VIEW sales_dashboard AS
    SELECT
        c.country,
        p.groupname,
        p.name AS product_name,
        SUM(f.qty) AS total_qty,
        SUM(f.qty * p.price) AS revenue,
    COUNT(*) AS total_orders
    FROM fact_sales f
    JOIN dim_customers c ON f.customer_id = c.id
    JOIN dim_products p ON f.product_id = p.id
    GROUP BY c.country, p.groupname, p.name;
    """
    execute_sql("dwh", sql)


with DAG(
        dag_id="init_db_dag",
        start_date=datetime(2024, 1, 1),
        schedule_interval=None,
        catchup=False,
    ) as dag:


    t1 = PythonOperator(
        task_id="init_oltp",
        python_callable=init_oltp
    )

    t2 = PythonOperator(
        task_id="init_hwm",
        python_callable=init_hwm
    )

    t3 = PythonOperator(
        task_id="init_mrr",
        python_callable=init_mrr
    )

    t4 = PythonOperator(
        task_id="init_stg",
        python_callable=init_stg
    )

    t5 = PythonOperator(
        task_id="init_dwh",
        python_callable=init_dwh
    )

    t1 >> t2 >> t3 >> t4 >> t5
