# import pandas as pd
# import psycopg2
# import xml.etree.ElementTree as ET
#
# # Function to read DB details from text file
# def get_db_details(file_path):
#     with open(file_path, 'r') as file:
#         db_details = {}
#         for line in file:
#             key, value = line.strip().split('=')
#             db_details[key] = value
#     return db_details
#
# # Function to read SQL query from XML file
# def get_query_from_xml(file_path):
#     tree = ET.parse(file_path)
#     root = tree.getroot()
#     query = root.text.strip()
#     return query
#
# # Function to insert data from CSV to PostgreSQL
# def insert_data_from_csv_to_postgres(csv_file, db_details_file, query_file):
#     # Get DB details
#     db_details = get_db_details(db_details_file)
#
#     # Get SQL query from XML
#     insert_query = get_query_from_xml(query_file)
#
#     # Connect to PostgreSQL database
#     conn = psycopg2.connect(
#         host=db_details['host'],
#         database=db_details['database'],
#         user=db_details['user'],
#         password=db_details['password'],
#         port=db_details['port']
#     )
#
#     # Create a cursor object
#     cur = conn.cursor()
#
#     try:
#         # Load CSV data into DataFrame
#         df = pd.read_csv(csv_file)
#
#         # Convert DataFrame to list of tuples
#         data_tuples = list(df.itertuples(index=False, name=None))
#
#         # Execute the query to insert data
#         cur.executemany(insert_query, data_tuples)
#
#         # Commit the transaction
#         conn.commit()
#
#         print(f"Data successfully inserted into the database from {csv_file}")
#
#     except Exception as e:
#         print(f"Error: {e}")
#         conn.rollback()
#
#     finally:
#         # Close cursor and connection
#         cur.close()
#         conn.close()
#
# # File paths
# csv_file = 'emplData.csv'    # CSV file containing data
# db_details_file = 'db_details.txt'
# query_file = 'insert_query.xml'
#
# # Insert data from CSV to PostgreSQL
# insert_data_from_csv_to_postgres(csv_file, db_details_file, query_file)

# -----------------------------------------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------------------------


import pandas as pd
import psycopg2
import xml.etree.ElementTree as ET


# Function to read DB details from text file
def get_db_details(file_path):
    with open(file_path, 'r') as file:
        db_details = {}
        for line in file:
            key, value = line.strip().split('=')
            db_details[key] = value
    return db_details


# Function to read SQL query from XML file
def get_query_from_xml(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()
    query = root.text.strip()
    return query


# Function to insert data into PostgreSQL
def insert_data_to_postgres(data_tuples, insert_query, conn):
    try:
        # Create a cursor object
        cur = conn.cursor()

        # Execute the query to insert data
        cur.executemany(insert_query, data_tuples)

        # Commit the transaction
        conn.commit()

        print("Data successfully inserted into the database.")

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()

    finally:
        # Close cursor
        cur.close()


# Function to process the CSV file and insert data into PostgreSQL
def process_and_insert_data(csv_file, db_details_file, query_file):
    # Get DB details
    db_details = get_db_details(db_details_file)

    # Get SQL query from XML
    insert_query = get_query_from_xml(query_file)

    # Connect to PostgreSQL database
    conn = psycopg2.connect(
        host=db_details['host'],
        database=db_details['database'],
        user=db_details['user'],
        password=db_details['password'],
        port=db_details['port']
    )

    try:
        # Load CSV data into DataFrame
        df = pd.read_csv(csv_file)

        # Convert DataFrame to list of tuples
        data_tuples = list(df.itertuples(index=False, name=None))

        # Insert data into PostgreSQL
        insert_data_to_postgres(data_tuples, insert_query, conn)

    finally:
        # Close connection
        conn.close()


# File paths
csv_file = 'emplData.csv'  # CSV file containing data
db_details_file = 'db_details.txt'
query_file = 'insert_query.xml'

# Process and insert data
process_and_insert_data(csv_file, db_details_file, query_file)
