import pandas as pd
import psycopg2

def connect_to_db():
    print("Connecting to the PostgresSQL database...")
    try:
        conn = psycopg2.connect(
            host="db",
            port=5432,
            database="database",
            user="postgres",
            password="123456"
        )
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        raise

def create_table_query(conn):
    print("Creating table if it does not exist...")
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS staging;
            CREATE TABLE IF NOT EXISTS staging.online_sales_data (
                id SERIAL PRIMARY KEY,
                invoice_no VARCHAR,
                stock_code VARCHAR,
                description VARCHAR,
                quantity INTEGER,
                invoice_date TIMESTAMP,
                unit_price FLOAT,
                customer_id FLOAT,
                country VARCHAR,
                discount FLOAT,
                payment_method VARCHAR,
                shipping_cost FLOAT,
                category VARCHAR,
                sales_channel VARCHAR,
                return_status VARCHAR,
                shipment_provider VARCHAR,
                warehouse_location VARCHAR,
                order_priority VARCHAR
            );
        """)
        conn.commit()
        print("Table created successfully.")
    except Exception as e:
        print(f"Error creating table: {e}")
        raise

df = pd.read_csv(r'data/raw/online_sales_dataset.csv')

def load_data_to_db(df, conn):
    print("Loading online sales data into the database...")
    try:
        cursor = conn.cursor()
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO staging.online_sales_data (
                    invoice_no, stock_code, description, quantity, invoice_date, unit_price,
                    customer_id, country, discount, payment_method, shipping_cost, category,
                    sales_channel, return_status, shipment_provider, warehouse_location, order_priority
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, tuple(row))
        conn.commit()
        print("Data loaded successfully.")
    except Exception as e:
        print(f"Error loading data into the database: {e}")
        raise

def main():
    try:
        conn = connect_to_db()
        create_table_query(conn)
        load_data_to_db(df, conn)
    except Exception as e:
        print(f"An error occurred during execution: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            print("Database connection closed.")