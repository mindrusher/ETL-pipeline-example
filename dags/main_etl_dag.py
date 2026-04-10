from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models.baseoperator import cross_downstream
from datetime import datetime
import psycopg2

# ---------- CONFIG ----------

DB_CONFIG = {
    "host": "postgres",
    "user": "airflow",
    "password": "airflow"
}

# ---------- UTILS ----------

def get_connection(db):
    return psycopg2.connect(database=db, **DB_CONFIG)


def log_status(process, status, message=""):
    conn = get_connection("dwh")
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO etl_logs (process_name, status, start_time, end_time, message)
        VALUES (%s, %s, NOW(), NOW(), %s)
    """, (process, status, message))

    conn.commit()
    conn.close()


def get_high_water_mark(table_name):
    conn = get_connection("dwh")
    cursor = conn.cursor()

    cursor.execute(
        "SELECT last_loaded FROM high_water_mark WHERE table_name = %s",
        (table_name,)
    )
    result = cursor.fetchone()

    conn.close()
    return result[0]


def update_high_water_mark(table_name, value):
    conn = get_connection("dwh")
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE high_water_mark
        SET last_loaded = %s
        WHERE table_name = %s
    """, (value, table_name))

    conn.commit()
    conn.close()

# ---------- OLTP → MRR ----------

def extract_table(table_name, source_table, target_table):
    oltp_conn = get_connection("oltp")
    mrr_conn = get_connection("mrr")

    try:
        oltp_cursor = oltp_conn.cursor()
        mrr_cursor = mrr_conn.cursor()

        last_loaded = get_high_water_mark(table_name)

        oltp_cursor.execute(
            f"SELECT * FROM {source_table} WHERE updated_at > %s",
            (last_loaded,)
        )
        rows = oltp_cursor.fetchall()

        if not rows:
            log_status(table_name, "SUCCESS", "No new data")
            return

        columns = [desc[0] for desc in oltp_cursor.description]
        col_names = ",".join(columns)
        placeholders = ",".join(["%s"] * len(columns))

        update_clause = ",".join(
            [f"{col}=EXCLUDED.{col}" for col in columns if col != "id"]
        )

        query = f"""
            INSERT INTO {target_table} ({col_names})
            VALUES ({placeholders})
            ON CONFLICT (id) DO UPDATE
            SET {update_clause}
        """

        mrr_cursor.executemany(query, rows)
        mrr_conn.commit()

        max_ts = max(r[-1] for r in rows)
        update_high_water_mark(table_name, max_ts)

        log_status(table_name, "SUCCESS", f"{len(rows)} rows upserted")

    except Exception as e:
        mrr_conn.rollback()
        log_status(table_name, "FAILED", str(e))
        raise

    finally:
        oltp_conn.close()
        mrr_conn.close()

# ---------- MRR → STG ----------

def mrr_to_stg(table_name, source_table, target_table):
    mrr_conn = get_connection("mrr")
    stg_conn = get_connection("stg")

    try:
        mrr_cursor = mrr_conn.cursor()
        stg_cursor = stg_conn.cursor()

        stg_cursor.execute(f"TRUNCATE TABLE {target_table}")

        mrr_cursor.execute(f"SELECT * FROM {source_table}")
        rows = mrr_cursor.fetchall()

        if not rows:
            log_status(table_name + "_stg", "SUCCESS", "No data")
            return

        placeholders = ",".join(["%s"] * len(rows[0]))
        insert_query = f"INSERT INTO {target_table} VALUES ({placeholders})"

        stg_cursor.executemany(insert_query, rows)
        stg_conn.commit()

        log_status(table_name + "_stg", "SUCCESS", f"{len(rows)} rows")

    except Exception as e:
        stg_conn.rollback()
        log_status(table_name + "_stg", "FAILED", str(e))
        raise

    finally:
        mrr_conn.close()
        stg_conn.close()

# ---------- STG → DWH ----------

def load_dim_customers():
    stg_conn = get_connection("stg")
    dwh_conn = get_connection("dwh")

    try:
        stg_cursor = stg_conn.cursor()
        dwh_cursor = dwh_conn.cursor()

        stg_cursor.execute("SELECT DISTINCT id, name, country FROM stg_dim_customers")
        rows = stg_cursor.fetchall()

        if not rows:
            return

        insert_query = """
            INSERT INTO dim_customers (id, name, country)
            VALUES (%s, %s, %s)
            ON CONFLICT (id) DO NOTHING
        """

        dwh_cursor.executemany(insert_query, rows)
        dwh_conn.commit()

    finally:
        stg_conn.close()
        dwh_conn.close()


def load_dim_products():
    stg_conn = get_connection("stg")
    dwh_conn = get_connection("dwh")

    try:
        stg_cursor = stg_conn.cursor()
        dwh_cursor = dwh_conn.cursor()

        stg_cursor.execute("""
            SELECT DISTINCT id, name, groupname, price FROM stg_dim_products
        """)
        rows = stg_cursor.fetchall()

        if not rows:
            return

        insert_query = """
            INSERT INTO dim_products (id, name, groupname, price)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
        """

        dwh_cursor.executemany(insert_query, rows)
        dwh_conn.commit()

    finally:
        stg_conn.close()
        dwh_conn.close()


def load_fact_sales():
    stg_conn = get_connection("stg")
    dwh_conn = get_connection("dwh")

    try:
        stg_cursor = stg_conn.cursor()
        dwh_cursor = dwh_conn.cursor()

        last_loaded = get_high_water_mark("fact_sales")

        stg_cursor.execute("""
            SELECT customer_id, product_id, qty, updated_at
            FROM stg_fact_sales
            WHERE updated_at > %s
        """, (last_loaded,))

        rows = stg_cursor.fetchall()

        if not rows:
            log_status("fact_sales", "SUCCESS", "No new data")
            return

        insert_query = """
            INSERT INTO fact_sales (customer_id, product_id, qty, updated_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """

        dwh_cursor.executemany(insert_query, rows)
        dwh_conn.commit()

        max_ts = max(r[3] for r in rows)
        update_high_water_mark("fact_sales", max_ts)

        log_status("fact_sales", "SUCCESS", f"{len(rows)} rows loaded")

    except Exception as e:
        dwh_conn.rollback()
        log_status("fact_sales", "FAILED", str(e))
        raise

    finally:
        stg_conn.close()
        dwh_conn.close()


# ---------- DAG ----------

with DAG(
        dag_id="main_etl_dag",
        start_date=datetime(2024, 1, 1),
        schedule_interval="@daily",
        catchup=False,
        max_active_runs=1,
    ) as dag:

    # OLTP → MRR
    load_customers = PythonOperator(
        task_id="load_customers",
        python_callable=lambda: extract_table("customers", "customers", "mrr_dim_customers")
    )

    load_products = PythonOperator(
        task_id="load_products",
        python_callable=lambda: extract_table("products", "products", "mrr_dim_products")
    )

    load_sales = PythonOperator(
        task_id="load_sales",
        python_callable=lambda: extract_table("sales", "sales", "mrr_fact_sales")
    )

    # MRR → STG
    stg_customers = PythonOperator(
        task_id="stg_customers",
        python_callable=lambda: mrr_to_stg("customers", "mrr_dim_customers", "stg_dim_customers")
    )

    stg_products = PythonOperator(
        task_id="stg_products",
        python_callable=lambda: mrr_to_stg("products", "mrr_dim_products", "stg_dim_products")
    )

    stg_sales = PythonOperator(
        task_id="stg_sales",
        python_callable=lambda: mrr_to_stg("sales", "mrr_fact_sales", "stg_fact_sales")
    )

    # STG → DWH
    dim_customers = PythonOperator(
        task_id="dim_customers",
        python_callable=load_dim_customers
    )

    dim_products = PythonOperator(
        task_id="dim_products",
        python_callable=load_dim_products
    )

    fact_sales = PythonOperator(
        task_id="fact_sales",
        python_callable=load_fact_sales
    )

    backup = BashOperator(
        task_id="backup",
        bash_command="""
        bash /opt/airflow/scripts/backup.sh
        """
    )

    # PIPELINE
    cross_downstream(
        [load_customers, load_products, load_sales],
        [stg_customers, stg_products, stg_sales]
    )
    cross_downstream(
        [stg_customers, stg_products], [dim_customers, dim_products]
    )
    stg_sales >> fact_sales >> backup
