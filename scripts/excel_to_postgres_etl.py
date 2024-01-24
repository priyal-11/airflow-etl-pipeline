import os
import pandas as pd
from sqlalchemy import create_engine
import logging

# Configure professional logging to provide structured, time-stamped output.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# --- Configuration ---
# Load sensitive credentials and database configuration from environment variables for security.
DB_PASSWORD = os.environ.get('PGPASS')
DB_USER = os.environ.get('PGUID')
DB_HOST = "localhost"
DB_NAME = "AdventureWorks"
DB_PORT = "5432"

# --- File Paths ---
# Dynamically construct the path to the 'data' directory relative to the script's location.
# This makes the script portable and runnable on any machine.
try:
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
    DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
except NameError:
    # Handle cases where __file__ is not defined (e.g., in an interactive session).
    DATA_DIR = 'data' 
    logging.warning("__file__ is not defined. Using relative 'data' path.")

def extract_from_excel(data_path):
    """
    Reads all Excel files from a specified directory into a list of DataFrames.

    Args:
        data_path (str): The path to the directory containing Excel files.

    Returns:
        list: A list of dictionaries, where each dictionary contains the
              table_name (from the filename) and its corresponding DataFrame.
    """
    all_data = []
    logging.info(f"Extracting data from directory: '{data_path}'")
    for filename in os.listdir(data_path):
        if filename.endswith(".xlsx"):
            file_path = os.path.join(data_path, filename)
            table_name = os.path.splitext(filename)[0]
            try:
                df = pd.read_excel(file_path)
                all_data.append({'table_name': table_name, 'data': df})
                logging.info(f"Successfully extracted {len(df)} rows from '{filename}'.")
            except Exception as e:
                logging.error(f"Failed to extract data from '{filename}': {e}")
    return all_data

def transform(dataframes_list):
    """
    Applies basic data cleaning and transformation to a list of DataFrames.

    Args:
        dataframes_list (list): The list of dictionaries containing table_name and data.

    Returns:
        list: The list of dictionaries with transformed DataFrames.
    """
    transformed_data = []
    for item in dataframes_list:
        df = item['data'].copy()
        table_name = item['table_name']
        logging.info(f"Applying transformations to '{table_name}'...")
        
        # Standardize column names: convert to lowercase and replace spaces with underscores.
        df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
        
        # Remove rows that are entirely empty.
        df.dropna(how='all', inplace=True)
        
        item['data'] = df
        transformed_data.append(item)
        logging.info(f"Transformations for '{table_name}' complete.")
    return transformed_data

def load_to_postgres(dataframes_list):
    """
    Loads a list of transformed DataFrames into a PostgreSQL database.
    Each DataFrame is loaded into a new table prefixed with 'stg_'.

    Args:
        dataframes_list (list): The list of dictionaries with transformed data.
    """
    connection_str = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    engine = create_engine(connection_str)
    
    for item in dataframes_list:
        df = item['data']
        table_name = f"stg_{item['table_name']}"
        try:
            logging.info(f"Loading {len(df)} rows into '{table_name}'...")
            # Load data, replacing the table if it already exists.
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            logging.info(f"Successfully loaded data into '{table_name}'.")
        except Exception as e:
            logging.error(f"Failed to load data into '{table_name}': {e}")

def main():
    """Orchestrates the main ETL workflow: Extract, Transform, and Load."""
    logging.info("Starting ETL process...")
    
    # 1. Extract
    extracted_data = extract_from_excel(DATA_DIR)
    
    if extracted_data:
        # 2. Transform
        transformed_data = transform(extracted_data)
        
        # 3. Load
        load_to_postgres(transformed_data)
        
        logging.info("ETL process finished successfully.")
    else:
        logging.warning("No data found to process. ETL process terminated.")

# Entry point for the script.
# This block ensures the main() function is called only when the script is executed directly.
if __name__ == "__main__":
    main()