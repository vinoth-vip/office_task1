import pandas as pd
import psycopg2
from sqlalchemy import create_engine


# Function to read database connection details from a text file
def get_db_details(file_path):
    with open(file_path, 'r') as file:
        db_details = {}
        for line in file:
            key, value = line.strip().split('=')
            db_details[key] = value
    return db_details


# Function to insert data into PostgreSQL
def insert_data_from_csv(csv_file, db_details_file):
    # Read the database connection details
    db_details = get_db_details(db_details_file)

    # Create the SQLAlchemy engine for PostgreSQL
    engine = create_engine(
        f'postgresql+psycopg2://{db_details["user"]}:{db_details["password"]}@{db_details["host"]}:{db_details["port"]}/{db_details["database"]}')

    # Load CSV data into DataFrame
    df = pd.read_csv(csv_file)

    # Define the table name and schema
    table_name = 'cbase.employee'

    # Use pandas to insert data into the PostgreSQL table
    with engine.connect() as connection:
        # Start a transaction
        with connection.begin():
            df.to_sql(table_name, con=engine, schema='cbase', if_exists='append', index=False)

    print(f"Data successfully inserted into the table {table_name}")


# File paths
csv_file = 'employee_data.csv'  # CSV file containing data
db_details_file = 'db_details.txt'  # File with database connection details

# Insert data from CSV to PostgreSQL
insert_data_from_csv(csv_file, db_details_file)
