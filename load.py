# import necessary libraries
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os
from pathlib import Path

# define root dir
PROJECT_ROOT = Path(__file__).resolve().parent

RAW_DATA_DIR = PROJECT_ROOT / "raw_data"
CLEANED_DATA_DIR = PROJECT_ROOT / "cleaned_data"

dim_business = pd.read_csv(CLEANED_DATA_DIR / 'dim_business.csv')
dim_flour = pd.read_csv(CLEANED_DATA_DIR / 'dim_flour.csv')
dim_rider = pd.read_csv(CLEANED_DATA_DIR / 'dim_rider.csv')
fact_orders = pd.read_csv(CLEANED_DATA_DIR / 'fact_orders.csv')

print("Cleaned csvs loaded successfully")

# define connection variables
db_name = 'flour4four'
db_user = 'postgres'
db_password = 'password'
db_host = 'localhost'
db_port = '5432'

def get_connection():
    connection = psycopg2.connect(
        dbname = db_name,
        user = db_user,
        password = db_password,
        host = db_host,
        port = db_port
    )

    return connection

conn = get_connection()

# create the tables
def create_tables():
    conn = get_connection()
    cursor = conn.cursor()
    create_tables_query = """
                CREATE SCHEMA IF NOT EXISTS flour4four;

                DROP TABLE IF EXISTS flour4four.fact_orders CASCADE;
                DROP TABLE IF EXISTS flour4four.dim_business CASCADE;
                DROP TABLE IF EXISTS flour4four.dim_flour CASCADE;
                DROP TABLE IF EXISTS flour4four.dim_rider CASCADE;

                CREATE TABLE flour4four.dim_business (
                    business_id       VARCHAR PRIMARY KEY,
                    business_name     VARCHAR NOT NULL,
                    business_type     VARCHAR,
                    business_address  VARCHAR,
                    contact_name      VARCHAR NOT NULL,
                    contact_phone     VARCHAR NOT NULL
                );

                CREATE TABLE flour4four.dim_flour (
                    flour_type_id SERIAL PRIMARY KEY,
                    flour_type VARCHAR
                );

                CREATE TABLE flour4four.dim_rider (
                    rider_id SERIAL PRIMARY KEY,
                    rider_name VARCHAR NOT NULL,
                    rider_phone VARCHAR NOT NULL
                );

                CREATE TABLE flour4four.fact_orders (
                    order_id       VARCHAR PRIMARY KEY,
                    order_date     DATE NOT NULL,
                    delivery_date  DATE NOT NULL,
                    business_id    VARCHAR NOT NULL REFERENCES flour4four.dim_business(business_id),
                    rider_id    INT NOT NULL REFERENCES flour4four.dim_rider(rider_id),
                    flour_type_id     INT NOT NULL REFERENCES flour4four.dim_flour(flour_type_id),
                    quantity_bags  INT NOT NULL,
                    price_per_bag   NUMERIC NOT NULL,
                    total_amount   NUMERIC NOT NULL,
                    payment_method VARCHAR NOT NULL,
                    order_status   VARCHAR NOT NULL
                );
    """
    cursor.execute(create_tables_query)
    conn.commit()
    cursor.close()
    conn.close()
    print("Tables created successfully.")

create_tables()

# create to_sql engine
engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

# load to sql 
dim_business.to_sql(
    "dim_business", engine,
    schema="flour4four",
    if_exists="append", index=False
)

dim_flour.to_sql(
    "dim_flour", engine,
    schema="flour4four",
    if_exists="append", index=False
)

dim_rider.to_sql(
    "dim_rider", engine,
    schema="flour4four",
    if_exists="append", index=False
)

fact_orders.to_sql(
    "fact_orders", engine,
    schema="flour4four",
    if_exists="append", index=False
)

print('Data Successfully loaded to Postgres')