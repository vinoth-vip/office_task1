import psycopg2
import pandas as pd
import numpy as np
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

# Function to fetch data from PostgreSQL and save to CSV
def extract_data_to_csv(db_details_file, query_file, output_csv):

    db_details = get_db_details(db_details_file)

    query = get_query_from_xml(query_file)

    conn = psycopg2.connect(
        host=db_details['host'],
        database=db_details['database'],
        user=db_details['user'],
        password=db_details['password'],
        port=db_details['port']
    )

    try:
        # Execute the query and load the data into a pandas dataframe
        df = pd.read_sql_query(query, conn)

        # Replace any NaN values with zeros using numpy
        df = df.replace({np.nan: 0})

        # Save the dataframe to a CSV file
        df.to_csv(output_csv, index=False)
        print(f"Data successfully extracted and saved to {output_csv}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

# Specify the file paths
db_details_file = 'db_details.txt'
query_file = 'query.xml'
output_csv = 'employee_data.csv'

# Extract the data and save to CSV
extract_data_to_csv(db_details_file, query_file, output_csv)
