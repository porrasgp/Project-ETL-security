import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime
from cryptography.fernet import Fernet
from sqlalchemy.exc import SQLAlchemyError


# Function to encrypt data
def encrypt_data(data, key):
    cipher_suite = Fernet(key)
    cipher_text = cipher_suite.encrypt(data.encode())
    return cipher_text

# Function to load CSV file into a Pandas DataFrame
def load_csv(file_path):
    return pd.read_csv(file_path, sep=';', encoding='latin-1')

# Function to transform DataFrame into two DataFrames
def transform_data(df):
    df_null = df[df['CustomerID'].isnull()]
    df_not_null = df[df['CustomerID'].notnull()]
    return df_null, df_not_null

# Function to create a MySQL database engine
def create_mysql_engine(username, password, host, port, database):
    return create_engine(f'mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database}')

# Function to create a PostgreSQL database engine
def create_postgres_engine(username, password, host, port, database):
    return create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}')

# Function to create a PostgreSQL schema if it doesn't exist
def create_postgres_schema(engine, schema_name):
    with engine.connect() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS {schema_name}'))

# Function to load data into MySQL database
def load_data_to_mysql(engine, df_not_null, encryption_key):
    connection = engine.raw_connection()
    cursor = connection.cursor()

    try:
        # Create a copy of the DataFrame to avoid SettingWithCopyWarning
        df_copy = df_not_null.copy()

        # Apply encryption to the 'CustomerID' column in the copy
        df_copy['CustomerID'] = df_copy['CustomerID'].apply(lambda x: encrypt_data(str(x), encryption_key))

        # Convert DataFrame to SQL
        df_copy.to_sql('invoices', con=engine, if_exists='replace', index=False, method='multi', chunksize=500)

        # Commit the transaction
        connection.commit()
        print("Data loaded successfully to MySQL.")
    except SQLAlchemyError as e:
        # Rollback the transaction in case of an error
        connection.rollback()
        print(f"Error loading data to MySQL: {e}")
    finally:
        # Close the cursor and connection
        cursor.close()
        connection.close()

# Function to load data into PostgreSQL database
def load_data_to_postgres(engine, df_null, schema_name):
    df_null.to_sql('invoices', con=engine, if_exists='replace', index=False, schema=schema_name)

# Function to execute a PostgreSQL query and print the results
def execute_postgres_query(engine, query):
    with engine.connect() as db_conn:
        result = db_conn.execute(text(query))
        rows = result.fetchall()
        for row in rows:
            print(row)

# Function to execute a MySQL query and print the results
def execute_mysql_query(engine, query):
    with engine.connect() as db_conn:
        result = db_conn.execute(text(query))
        rows = result.fetchall()
        for row in rows:
            print(row)

# Function to execute a query and log the operation
def execute_query_and_log(engine, query, database_name):
    start_time = datetime.now()
    try:
        with engine.connect() as connection:
            result = connection.execute(text(query))
            print(f"Query executed successfully on {database_name} database.")
    except Exception as e:
        print(f"Error executing query on {database_name} database: {e}")
    finally:
        finish_time = datetime.now()
        time_difference = finish_time - start_time
        print(f"Start Time: {start_time}")
        print(f"Finish Time: {finish_time}")
        print(f"Time Difference: {time_difference}")

def main():
    # Step 0: Setting environment variables
    load_dotenv()

    # Access PostgreSQL environment variables
    postgres_username = os.getenv("POSTGRES_USERNAME")
    postgres_password = os.getenv("POSTGRES_PASSWORD")
    postgres_host = os.getenv("POSTGRES_HOST")
    postgres_port = os.getenv("POSTGRES_PORT", default="5432")
    postgres_database = os.getenv("POSTGRES_DATABASE")

    # Convert the port to an integer
    postgres_port = int(postgres_port)

    # Access MySQL environment variables
    mysql_username = os.getenv("MYSQL_USERNAME")
    mysql_password = os.getenv("MYSQL_PASSWORD")
    mysql_host = os.getenv("MYSQL_HOST")
    mysql_port = os.getenv("MYSQL_PORT", default="3306")
    mysql_database = os.getenv("MYSQL_DATABASE")

    # Convert the port to an integer
    mysql_port = int(mysql_port)

    # Step 1: Load CSV into DataFrame
    csv_path = r'.\data\ecommerce.csv'
    df = load_csv(csv_path)

    # Step 2: Transform - Create Two DataFrames
    df_null, df_not_null = transform_data(df)

    # Step 3: Loading the data to PostgreSQL and MySQL
    # Encryption key for data
    encryption_key = Fernet.generate_key()

    # Create SQLAlchemy Engine for MySQL
    mysql_engine = create_mysql_engine(mysql_username, mysql_password, mysql_host, mysql_port, mysql_database)
    load_data_to_mysql(mysql_engine, df_not_null, encryption_key)

    # Create SQLAlchemy Engine for PostgreSQL
    postgres_engine = create_postgres_engine(postgres_username, postgres_password, postgres_host, postgres_port, postgres_database)
    # Specify the schema name you want to use
    schema_name = 'invoices'
    create_postgres_schema(postgres_engine, schema_name)
    load_data_to_postgres(postgres_engine, df_null, schema_name)

    # Example usage of the engines
    mysql_query = 'SELECT * FROM `eCommerce`.`invoices` LIMIT 5'
    print("These are the results for MySQL")
    execute_mysql_query(mysql_engine, mysql_query)
    execute_query_and_log(mysql_engine, mysql_query, "MySQL")  # Add this line for logging

    postgres_query = 'SELECT * FROM invoices.invoices LIMIT 5'
    print("<-------------------------------------------------->")
    print("These are the results for PostgreSQL")
    execute_postgres_query(postgres_engine, postgres_query)
    execute_query_and_log(postgres_engine, postgres_query, "PostgreSQL")  # Add this line for logging

if __name__ == "__main__":
    main()
