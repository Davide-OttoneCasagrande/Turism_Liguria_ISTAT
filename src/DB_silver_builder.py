import pandas as pd
import psycopg2
import REST_handler as H_Rest
import ISTAT_API_Request_data as ISTAT_var
import DB_bronze_builder as bronze
from dotenv import load_dotenv
from os import getenv
load_dotenv()


def sql_query_from_file(sql_filePath) -> pd.DataFrame:
    """
        execute SQL script using SQL file
        Args:
            sql_filePath (str): the path to the SQL script file
            
        Returns:
           pd.DataFrame (optional) : if the script is a SELECT the dataframe of the selected table
    """
    
    with open(sql_filePath, 'r') as file:   # Read the SQL file
        sql_query = file.read()
    df=bronze.db_execute_SQL_Query(sql_query)
    return df


def db_execute_SQL_Query(sql_query: str):
    """
    Execute SQL query and return results.
    
    Args:
        sql_query (str): SQL statement.
        
    Returns:
        pandas.DataFrame: Query results as a DataFrame for SELECT queries, 
                          or None for non-SELECT queries.
    """
    print(f"querying DB ... \n{sql_query}")
    
    conn = None
    cursor = None
    df = None
    
    try:
        conn = bronze.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(sql_query)
        
        if sql_query.strip().upper().startswith('SELECT'):
            #results = cursor.fetchall()
            df = pd.read_sql(sql_query, conn)
        else:
            conn.commit()

        return df
        
    except Exception as e:
        print(f"Database error: {e}")
        if conn:
            conn.rollback()
        raise
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    SQL_FilePath = "src\\data\\select_location_hierarchy.sql"