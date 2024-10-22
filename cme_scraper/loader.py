



import json
import pandas as pd
import mysql.connector
from mysql.connector import Error

def load_config(config_path='config.json'):
    """
    Load MySQL configuration from JSON file
    """
    try:
        with open(config_path) as f:
            config = json.load(f)
        return config
    except FileNotFoundError:
        raise Exception(f"Configuration file not found at {config_path}")
    except json.JSONDecodeError:
        raise Exception("Invalid JSON format in configuration file")

def get_mysql_connection(config):
    """
    Establish MySQL connection using configuration
    """
    try:
        connection = mysql.connector.connect(
            host=config['host'],
            user=config['user'],
            password=config['password'],
            database=config['database']
        )
        if connection.is_connected():
            print("Successfully connected to MySQL database")
            return connection
    except Error as e:
        raise Exception(f"Error connecting to MySQL database: {e}")

def df_to_mysql(df, table_name, connection, if_exists='replace'):
    """
    Store pandas DataFrame to MySQL table
    
    Parameters:
    df: pandas DataFrame to store
    table_name: name of the target table
    connection: MySQL connection object
    if_exists: 'replace' or 'append' - what to do if table exists
    """
    try:
        cursor = connection.cursor()

        # Generate CREATE TABLE statement
        columns = []
        for col, dtype in df.dtypes.items():
            # Map pandas datatypes to MySQL datatypes
            if 'int' in str(dtype):
                mysql_type = 'INT'
            elif 'float' in str(dtype):
                mysql_type = 'DOUBLE'
            elif 'datetime' in str(dtype):
                mysql_type = 'DATETIME'
            elif 'bool' in str(dtype):
                mysql_type = 'BOOLEAN'
            else:
                mysql_type = 'VARCHAR(255)'
            columns.append(f"`{col}` {mysql_type}")

        # Drop table if if_exists='replace'
        if if_exists == 'replace':
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

        # Create table
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            {', '.join(columns)}
        )
        """
        cursor.execute(create_table_query)

        # Prepare insert statement
        columns = ', '.join([f"`{col}`" for col in df.columns])
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        # Convert DataFrame records to list of tuples
        values = [tuple(row) for row in df.values]

        # Execute batch insert
        cursor.executemany(insert_query, values)
        connection.commit()

        print(f"Successfully stored {len(df)} records in table '{table_name}'")

    except Error as e:
        connection.rollback()
        raise Exception(f"Error storing DataFrame in MySQL: {e}")
    finally:
        cursor.close()

def main():
    try:
        # Load configuration
        config = load_config('config.json')
        
        # Create sample DataFrame (replace with your actual data)
        sample_df = pd.DataFrame({
            'name': ['John Doe', 'Jane Smith', 'Bob Johnson'],
            'age': [30, 25, 35],
            'salary': [60000.50, 55000.75, 75000.25],
            'department': ['IT', 'HR', 'Finance'],
            'active': [True, True, False]
        })
        
        # Connect to MySQL
        connection = get_mysql_connection(config)
        
        # Store DataFrame
        df_to_mysql(sample_df, 'employees', connection, if_exists='replace')
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'connection' in locals() and connection.is_connected():
            connection.close()
            print("MySQL connection closed")


def load_data(final_df):
    # Save to CSV (optional)
    final_df.to_csv('processed_commodity_data.csv', index=False)
    print("Data processing completed successfully!")

if __name__ == "__main__":
    main()






# import pandas as pd
# from sqlalchemy import create_engine
# import urllib

# def save_dataframe_to_sql(df, table_name, server='your_server', database='your_db', username='your_username', password='your_password', schema='dbo'):
#     """
#     Save a pandas DataFrame to SQL Server.
    
#     Parameters:
#     -----------
#     df : pandas.DataFrame
#         The DataFrame to be stored
#     table_name : str
#         Name of the table to create/update in SQL Server
#     server : str
#         SQL Server instance name
#     database : str
#         Database name
#     username : str
#         SQL Server username
#     password : str
#         SQL Server password
#     schema : str
#         SQL Server schema (default is 'dbo')
#     """
#     try:
#         # Create connection parameters
#         params = urllib.parse.quote_plus(
#             'DRIVER={SQL Server};'
#             f'SERVER={server};'
#             f'DATABASE={database};'
#             f'UID={username};'
#             f'PWD={password}'
#         )

#         # Create SQLAlchemy engine
#         engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}')

#         # Save DataFrame to SQL Server
#         df.to_sql(
#             name=table_name,
#             con=engine,
#             schema=schema,
#             if_exists='replace',  # Options: 'fail', 'replace', 'append'
#             index=False,
#             chunksize=1000  # Adjust based on your data size
#         )
        
#         print(f"Successfully saved DataFrame to {database}.{schema}.{table_name}")
        
#     except Exception as e:
#         print(f"Error saving DataFrame to SQL Server: {str(e)}")
    
#     finally:
#         # Close the connection
#         if 'engine' in locals():
#             engine.dispose()

# # Example usage:
# if __name__ == "__main__":
#     # Create sample DataFrame
#     sample_df = pd.DataFrame({
#         'name': ['John', 'Alice', 'Bob'],
#         'age': [25, 30, 35],
#         'city': ['New York', 'London', 'Paris']
#     })
    
#     # Save to SQL Server
#     save_dataframe_to_sql(
#         df=sample_df,
#         table_name='employees',
#         server='your_server',
#         database='your_db',
#         username='your_username',
#         password='your_password'
#     )